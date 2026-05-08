"""
Celery tasks for post-call processing.

This is the main background processing pipeline. Every completed interaction
with a long transcript ends up here.

The task runs five steps sequentially:
    1. Wait 45s, try to fetch recording from Exotel → upload to S3
    2. Run full LLM analysis on the transcript
    3. Write result to interaction_metadata (dashboard cache)
    4. Trigger signal jobs (downstream actions: WhatsApp, callbacks, etc.)
    5. Update lead stage

A few things worth understanding before you start changing things:

WHY CELERY + REDIS?
  We needed a task queue and Celery was already in the stack. Redis was already
  in the stack. It worked fine at 1K calls/day. At 100K calls/campaign the cracks
  show: broker restarts lose tasks, queue depth is invisible, and there's no way
  to see which step a given interaction is stuck on.

WHY ONE QUEUE?
  Originally there was only one customer. One queue was fine. We never revisited
  it when the platform became multi-customer. Now a campaign for Customer A can
  fill the queue and delay Customer B's results by hours.

WHY DOES RECORDING BLOCK ANALYSIS?
  It shouldn't. Recording upload and LLM analysis are completely independent —
  the LLM reads the transcript, not the audio file. But they're sequential here
  because that's how the task was originally written and nobody had a reason to
  split them until the 45-second sleep became a visible SLA problem.

  Think about what "run them in parallel" would require at the infrastructure level.
"""

import asyncio
import logging
from datetime import datetime
from typing import Any, Dict

from src.tasks.celery_app import celery_app
from src.services.post_call_processor import PostCallProcessor, PostCallContext
from src.services.recording import fetch_and_upload_recording
from src.services.signal_jobs import trigger_signal_jobs, update_lead_stage
from src.services.retry_queue import retry_queue
from src.services.metrics import metrics_tracker

logger = logging.getLogger(__name__)


@celery_app.task(
    name="process_interaction_end_background_task",
    bind=True,
    max_retries=3,
    default_retry_delay=60,  # Fixed 60s — no exponential backoff
    acks_late=True,           # Task only acked after completion, not on receipt.
                              # This means a worker crash causes redelivery — good.
                              # But "redelivery" goes to the back of the queue,
                              # which at 100K depth means hours of extra wait.
    queue="postcall_processing",
)
def process_interaction_end_background_task(self, payload: Dict[str, Any]):
    """
    Main Celery task. Called for every long-transcript interaction.

    Celery workers are synchronous by default, so we spin up an event loop
    per task to run the async processing code. This means each Celery worker
    process handles one interaction at a time — no concurrency within a worker.

    At 100K interactions/campaign with ~3,500ms LLM latency per call:
        100,000 × 3.5s = 350,000 worker-seconds needed
        With 10 workers: ~9.7 hours to drain the queue

    If your campaign window is 8 hours, you're already behind before you start.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    try:
        loop.run_until_complete(_process_interaction(self, payload))
    except Exception as e:
        logger.exception(
            "celery_task_failed",
            extra={
                "interaction_id": payload.get("interaction_id"),
                "error": str(e),
                "attempt": self.request.retries,
            },
        )
        # Failed tasks earlier went into PostCallRetryQueue (Redis) AND Celery retries.
        # Two retry mechanisms that don't know about each other. 
        #Removing the Celery retry for now to prevent race conditions and extra workloads.
        loop.run_until_complete(
            retry_queue.enqueue_retry(
                interaction_id=payload["interaction_id"],
                error=str(e),
                payload=payload,
            )
        )
        # raise self.retry(exc=e)
    finally:
        loop.close()


async def _process_interaction(task, payload: Dict[str, Any]):
    interaction_id = payload["interaction_id"]

    await metrics_tracker.track_processing_started(interaction_id)

    ctx = PostCallContext(
        interaction_id=interaction_id,
        session_id=payload["session_id"],
        lead_id=payload["lead_id"],
        campaign_id=payload["campaign_id"],
        customer_id=payload["customer_id"],
        agent_id=payload["agent_id"],
        call_sid=payload.get("call_sid", ""),
        transcript_text=payload.get("transcript_text", ""),
        conversation_data=payload.get("conversation_data", {}),
        additional_data=payload.get("additional_data", {}),
        ended_at=datetime.fromisoformat(payload["ended_at"]),
        exotel_account_id=payload.get("exotel_account_id"),
    )

    # Create the instance for post call processing and parallelize  
    # the recording and LLM calls,since both are independent, 
    # time constrained and have chances of getting retried.
    #
    # Additionally, when the context for recording and s3 upload task is 
    # in-waiting, the celery task and LLM processing will not sit idle,
    # This will efficiently improve token usage across all customers and
    # campaigns. 

    processor = PostCallProcessor()

    # ── Step 1: Recording ─────────────────────────────────────────────────────
    # Blocks here for few seconds seconds waiting for Exotel to make the recording
    # available. Meanwhile, the LLM analysis (step 2) will also start asynchronously,
    # since it has zero dependency on the recording.
    #
    # Recording step will wait in steps of 10,15,15,20,30,30 seconds or as defined in 
    # the config. This way, if the file has arrived in 30 seconds, there will not be 
    # much overhead of wait for this task to end. And even if the wait is 2 minutes,
    # it will still process the recording.
    
    recording_task = asyncio.create_task(
        fetch_and_upload_recording(
            interaction_id=ctx.interaction_id,
            call_sid=ctx.call_sid,
            exotel_account_id=ctx.exotel_account_id or "",
        )
    )


    # ── Step 2: LLM analysis (Concurrent)──────────────────────────────────────────────────
    # Full analysis on every call. 1,500 tokens average. Now with pre-screening on 
    # token availability. If the tokens are not available, the system will retry after
    # some time based on the interaction priority
    #
    # The LLM rate limit (settings.LLM_CUSTOMER_TOKEN_BUDGETS) is checked before
    # the actual LLM call. If we're over the limit, the controlled waits and retries
    # till the tokens are replenished enough to process the query.
    
    llm_task = asyncio.create_task(
        processor.process_post_call(
            ctx,
            single_prompt=True,
        )
    )
    recording_s3_key, llm_result = await asyncio.gather(
        recording_task,
        llm_task,
        return_exceptions=True,
    )

    if recording_s3_key is None:
        # If recording_s3_key is None, flag it as an exception
        logger.exception(
            "recording_pipeline_failed",
            extra={
                "interaction_id": interaction_id,
                "error": "Recording upload error"
            },
        )

    if isinstance(llm_result,Exception):
        raise llm_result
    await metrics_tracker.track_processing_completed(
        interaction_id, llm_result.tokens_used, llm_result.latency_ms
    )

    # ── Step 3: Signal jobs ───────────────────────────────────────────────────
    # Downstream actions: send a WhatsApp follow-up, book a callback slot,
    # push to the customer's CRM. These depend on knowing the analysis result.
    #
    # If this raises, we log a warning and continue — the lead stage still
    # updates. But the downstream action (WhatsApp, callback, CRM push) is lost.
    try:
        await trigger_signal_jobs(
            interaction_id=ctx.interaction_id,
            session_id=ctx.session_id,
            campaign_id=ctx.campaign_id,
            analysis_result=llm_result.raw_response,
        )
    except Exception as e:
        logger.warning("signal_jobs_failed", extra={"error": str(e)})

    # ── Step 4: Lead stage update ─────────────────────────────────────────────
    # Updates the lead's stage in the leads table based on call_stage.
    # e.g., "rebook_confirmed" → lead moves to "booked" stage.
    # Same fire-and-forget risk as signal_jobs above.
    try:
        await update_lead_stage(
            lead_id=ctx.lead_id,
            interaction_id=ctx.interaction_id,
            call_stage=llm_result.call_stage,
        )
    except Exception as e:
        logger.warning("lead_stage_update_failed", extra={"error": str(e)})
