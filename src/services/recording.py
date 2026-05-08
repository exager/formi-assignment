"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

  The URL is fetched via:
      GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
  Returns 200 + recording_url if ready, 404 if not yet available.

Current approach:
  Wait 45 seconds. Try once. If it's not there, give up silently.

This means:
  - Recordings ready in 10s: we waste 35 seconds of wall time
  - Recordings ready in 60s: we miss them entirely, no retry, no alert
  - We have no idea how many recordings we're silently missing

The Exotel API is poll-friendly — they don't rate-limit the status endpoint.
The information needed to fix this is already available: try, check, sleep
a bit, try again. How many times and with what interval is worth thinking about.

Note: recording upload and LLM analysis are completely independent. The LLM
reads the transcript text, not the audio. There's no reason they have to run
sequentially. What would need to change for them to run in parallel?
"""

import asyncio
import logging
from typing import Optional
import httpx
from src.models.enums import RecordingFetchStatus
from src.config import settings
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass(slots=True)
class RecordingFetchResult:
    status: RecordingFetchStatus
    recording_url: str | None = None

async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """
    Attempt to fetch the Exotel recording and upload it to S3.

    Current implementation: sleep 45s, try once, return None on failure.
    Failure is logged at DEBUG level — effectively invisible in production
    where the log level is INFO.

    Returns the S3 key on success, None on failure or timeout.
    """

    # This sleep blocks the entire Celery task. While we're sleeping here,
    # the LLM quota is sitting idle, the analysis hasn't started, and the
    # dashboard still shows "processing" for what might be a confirmed rebook
    # that the sales team is waiting to act on.
    attempt = 0
    for time_delay in settings.RECORDING_WAIT_SECONDS:
        attempt += 1
        await asyncio.sleep(time_delay)

        try:
            recording_result = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

            if recording_result.status.value == RecordingFetchStatus.READY.value:
                # Found the recording, upload it to the URL
                s3_key = await _upload_to_s3(recording_result.recording_url, interaction_id)
                logger.info(
                    "recording_upload_successful",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "recording_retry_count": attempt-1,
                        "recording_failure_reason": None,
                    },
                )
                return s3_key
            
            if recording_result.status.value == RecordingFetchStatus.NOT_READY.value:
                # Not available after the time delay. We wait for the other time-gaps. 
                # And with proper logging of these events, this will be useful further in
                # telemetry and data validations...
                logger.debug(
                    "recording_retry_scheduled",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "attempt": attempt,
                        "waited_seconds": time_delay,
                    },
                )

            else:
                # This is for some failure (httpx or any other) from Exotel's side
                logger.exception(
                    "recording_fetch_error",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "recording_retry_count": attempt-1,
                        "recording_failure_reason": "Failure at upstream while getting the recording details",
                        },
                )

        except Exception as e:
            # Exception is caught here and swallowed. The caller (Celery task)
            # doesn't know whether this succeeded, failed, or was skipped.
            # It logs at ERROR level, which is at least visible — but there's
            # no retry path and no way to replay just the recording upload later.
            logger.error(
                "recording_upload_error",
                extra={"interaction_id": interaction_id, "error": str(e)},
            )
            return None
        
    logger.error(
        "recording_fetch_exhausted",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "attempts": attempt,
        },
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> RecordingFetchResult:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready.
    The 404 case (not yet ready) and the genuine error case (call had no
    recording, e.g., call was never connected) look the same from here —
    both return None. A retry loop would want to handle these differently.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(url)
            if resp.status_code == 200:
                data = resp.json()
                return RecordingFetchResult(status=RecordingFetchStatus.READY,recording_url=data.get("recording_url"))
            #Try after some wait period
            if resp.status_code ==404:
                return RecordingFetchResult(status=RecordingFetchStatus.NOT_READY,recording_url=None)
    
    except httpx.TimeoutException:
        # Timeouts return NOT_READY for still processing files(just for the case when timeouts happen since file is not fully uploaded by exotel)
        return RecordingFetchResult(status=RecordingFetchStatus.NOT_READY, recording_url=None)

    except httpx.HTTPError:
        # This is failure from Exotel's end
        return RecordingFetchResult(status=RecordingFetchStatus.FAILED,recording_url=None)


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3

    The interaction's recording_s3_key column gets updated after this succeeds.
    If this crashes after the upload but before the DB write, the file is in S3
    but the interaction row doesn't know about it. Currently no reconciliation job.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
