# Post-Call Processing Pipeline — Design Document

**Author:** Sahil Singh
**Date:** 08-05-2026

---

## 1. Assumptions

1. The post-call webhook (`POST /session/{sid}/interaction/{id}/end`) must remain low latency and respond within the telephony provider timeout window (~5 seconds). Heavy processing should remain asynchronous.
2. LLM APIs enforce hard limits on both requests-per-minute (RPM) and tokens-per-minute (TPM). TPM is treated as the primary limiting factor since transcript lengths vary significantly between calls.
3. Not all customers using the platform have equal priority or throughput requirements. Enterprise customers may reserve guaranteed LLM capacity, while lower-tier customers may use shared overflow capacity.
4. Calls are bursty in nature. During campaign runs, tens of thousands of calls may complete within a short time window, causing sudden spikes in post-call processing demand.
5. Interaction records in the database are treated as the source of truth for post-call processing state and auditability.
6. Short or low-information calls do not require full LLM analysis. The existing short transcript logic is directionally correct but currently placed inconsistently across layers.
7. Recording availability is eventually consistent. Exotel recordings may become available anywhere between 10–90 seconds after call completion.
8. Downstream systems (CRM pushes, signal jobs, lead stage updates) are eventually consistent and do not need to block the webhook response.
9. Failures are expected at scale (429s, worker crashes, delayed recordings, Redis restarts, transient DB/network failures) and must be recoverable without losing interaction state.
10. Local execution and testing must remain possible using docker-compose with mocked infrastructure and no real LLM credentials.

---

## 2. Problem Diagnosis

The current system works correctly at low throughput but fails under bursty, multi-customer workloads due to the absence of centralized execution control and durable processing state.

### 1. No rate-limit awareness

The system fires LLM requests immediately as calls complete without considering provider TPM/RPM limits. Under campaign-scale bursts (e.g., 100K calls), this creates large waves of 429 responses, retry amplification, queue growth, and eventual backlog collapse.

The current circuit breaker attempts to react to overload, but:
- it measures requests-per-minute instead of tokens-per-minute,
- it reacts after requests are already in-flight,
- and it freezes dialling entirely instead of applying gradual backpressure.

### 2. No customer-level isolation

All customers share the same Celery queue and the same processing path. A large campaign for one customer can fully consume queue capacity and delay processing for all other customers.

There is no notion of:
- reserved token budgets,
- fair scheduling,
- customer prioritization,
- or overflow handling.

### 3. Sequential blocking pipeline

Recording fetch and LLM analysis are independent operations but currently execute sequentially.

The pipeline blocks for a fixed 45-second sleep before attempting recording retrieval. During this period:
- workers remain idle,
- LLM capacity is unused,
- and urgent interactions are unnecessarily delayed.

### 4. Fragile task durability

The current design uses:
- Celery retries,
- plus a separate Redis retry queue.

These systems are unaware of each other and can both retry the same interaction, causing duplicate execution.

Additionally:
- Redis acts as both broker and retry persistence layer,
- Retry state is not durable,
- Tasks can be permanently lost during Redis failures or worker crashes.

### 5. Incorrect execution ordering

Signal jobs and lead stage updates are triggered before LLM analysis is complete in some execution paths. Downstream systems may therefore receive incomplete or empty analysis payloads.

### 6. Recording pipeline is unreliable:
The system uses a fixed asyncio.sleep(45s) delay to fetch recordings. If recordings are delayed beyond this window, they are silently skipped with no retry or alerting.

### 6. Inconsistent business logic placement

Short transcript detection exists in multiple layers (API and model) instead of being centralized in the processing pipeline. This creates inconsistent behaviour during retries and reprocessing.

### 8. Lack of observability

The system does not maintain a durable, queryable execution trail for an interaction.

Current limitations include:
- no correlation IDs,
- no processing-stage visibility,
- no queue wait visibility,
- no token accounting aggregation,
- and insufficient structured failure logging.

This makes debugging production failures difficult and prevents accurate customer-level cost attribution.

---

## 3. Architecture Overview

The redesigned system introduces a centralized, rate-limit-aware scheduling layer between task ingestion and LLM execution.

The primary architectural change is separating:
- ingestion,
- scheduling,
- and execution.

This allows the system to:
- enforce token budgets,
- isolate customers,
- prioritize urgent workloads,
- and apply natural backpressure without freezing the dialler.

### High-Level Flow

```text
POST /interaction/end
        │
        ▼
Persist interaction state
        │
        ▼
Enqueue durable post-call task
        │
        ▼
Worker pulls interaction
        │
        ├── Recording Poller (async retry/backoff)
        │
        └── LLM Scheduler
                │
                ├── Global token bucket
                ├── Per-customer token budgets
                ├── Priority-aware scheduling
                └── Deferred queue
                        │
                        ▼
                 LLM Execution
                        │
                        ▼
            interaction_metadata update
                        │
                        ▼
      Signal jobs / CRM / Lead stage updates
                        │
                        ▼
               Structured audit logs
```

### Key design decisions

1. Interaction remains the source of truth
- Processing state is stored against the interaction record itself.
2. LLM access is centralized
- Workers no longer call the LLM directly.
- All requests pass through a scheduler responsible for token allocation and admission control.
3. Backpressure replaces binary freezing
- Instead of stopping dialling completely, constrained capacity naturally slows processing throughput through queue buildup and scheduling.
4. Recording fetch and LLM analysis are decoupled
- They execute independently and no longer block each other.
5. Customer isolation is enforced at the scheduler layer
- One customer's workload cannot consume another customer's reserved allocation.
6. Retry handling is consolidated
- The system avoids overlapping retry mechanisms and ensures retries remain idempotent.

---

## 4. Rate Limit Management

The redesigned system introduces a centralized LLM scheduler responsible for enforcing token-aware admission control before any LLM request is executed.

The scheduler acts as the control plane for all LLM consumption.

### Why token-based limiting instead of request-based limiting

The existing circuit breaker measures requests-per-minute (RPM), but transcript sizes vary significantly between calls.

A short interaction may consume a few hundred tokens, while a long conversation may consume several thousand. Because provider enforcement primarily occurs at the tokens-per-minute (TPM) level, TPM becomes the primary scheduling constraint.

### Scheduler responsibilities

Before executing an LLM request, the scheduler evaluates:

1. Global token availability
2. Per-customer token allocation
3. Current queue backlog
4. Interaction urgency/priority

Only requests that satisfy all constraints are admitted immediately.

All others are deferred into a scheduling queue instead of being failed.

### Scheduler vs CircuitBreaker

The scheduler becomes the primary admission-control mechanism.

The existing circuit breaker is retained as a lightweight telemetry and emergency safety layer, but proactive token admission control now occurs before requests are sent to the LLM provider.

This shifts overload handling from:
- reactive request failure handling,
to:
- proactive capacity-aware scheduling.

### Token accounting

Each interaction carries:
- estimated token usage before execution,
- and actual token usage after execution.

Actual token usage is extracted from the provider response (`usage.total_tokens`) and persisted for:
- billing,
- debugging,
- and customer-level budget tracking.

### Scheduling model

The scheduler maintains:

- A global token bucket representing total provider TPM capacity.
- Per-customer token buckets representing reserved customer allocations.
- A shared overflow pool for unused capacity.

Requests are evaluated in the following order:

1. Check customer allocation availability
2. Check global token availability
3. Check interaction priority
4. Admit or defer

### Deferred execution

When capacity is unavailable:
- interactions are queued,
- not rejected.

Deferred tasks remain durable and retry automatically once capacity becomes available.

This avoids retry storms caused by provider 429 responses.

### Backpressure model

The redesigned system removes the binary circuit breaker model.

Instead of freezing dialling entirely:
- queue depth,
- token utilization,
- and scheduler backlog

become natural backpressure signals.

As capacity becomes constrained:
- processing latency increases gradually,
- while the system continues operating safely within provider limits.

---

## 5. Per-Customer Token Budgeting

The redesigned system introduces token-aware customer isolation at the scheduler layer.

Instead of allowing all interactions to compete for a single global queue, the scheduler allocates token capacity across customers using a hybrid reservation + shared-pool model.

### Capacity Allocation Model

The platform maintains:

- A global token-per-minute (TPM) limit based on the configured LLM provider quota.
- Reserved per-customer token budgets.
- A shared overflow pool for unused capacity redistribution.

Example:

- Global TPM capacity: 100,000 TPM
- Customer A reserved: 40,000 TPM
- Customer B reserved: 20,000 TPM
- Shared overflow pool: 40,000 TPM

Each customer therefore receives guaranteed baseline throughput while still allowing unused capacity to be reused efficiently.

### How capacity is allocated

Each incoming interaction is tagged with:
- `customer_id`
- `estimated_token_usage`
- `campaign_id`
- `interaction_priority`

Before executing an LLM request, the scheduler checks:

1. Whether the customer has remaining reserved capacity.
2. Whether shared overflow capacity is available.
3. Whether global provider TPM capacity is still available.

Only interactions satisfying these conditions are admitted immediately.

Otherwise, they are deferred into the scheduling queue.

### Guarantees provided to customers

Customers with reserved budgets receive guaranteed minimum throughput even during large campaign spikes from other customers.

For example:
- Customer A exhausting the queue with 50K interactions cannot consume Customer B's reserved token allocation.

This prevents cross-customer starvation and improves predictability for enterprise customers.

### What happens when a customer exceeds their budget

If a customer exceeds their reserved allocation:

1. The scheduler first attempts to use shared overflow capacity.
2. If overflow capacity is unavailable, the interaction is deferred.

Interactions are not rejected or dropped.

Deferred interactions remain durable and are retried automatically once token capacity becomes available.

### Handling unused headroom

Unused customer allocations are temporarily added to a shared overflow pool.

This allows:
- higher overall utilization,
- better throughput during uneven workloads,
- and reduced idle LLM capacity.

However, overflow borrowing is opportunistic and revocable:
- reserved customer allocations always take precedence when reclaimed.

### Token accounting

Each request tracks:
- estimated tokens before execution,
- actual tokens after execution.

Actual usage is persisted per interaction and attributed to:
- `customer_id`,
- `campaign_id`,
- `interaction_id`.

This supports:
- billing,
- debugging,
- and future quota enforcement.

---

## 6. Differentiated Processing

Not all interactions require the same processing urgency.

Certain outcomes are operationally time-sensitive:
- confirmed bookings,
- callback requests,
- escalation requests,
- or highly interested leads.

Other interactions provide little immediate business value:
- short calls,
- disconnected calls,
- wrong numbers,
- or uninterested leads.

The redesigned system introduces differentiated processing to ensure limited LLM capacity is used where it provides the highest operational value.

### Processing Categories

Interactions are divided into three categories:

| Category | Behaviour |
|----------|------------|
| Skip | No LLM processing |
| High Priority | Immediate scheduling preference |
| Normal Priority | Deferred when capacity is constrained |

### Skip path

Very short or low-information interactions bypass LLM analysis entirely.

Examples:
- immediate hangups,
- disconnected calls,
- calls with fewer than 4 transcript turns.

This preserves LLM capacity for meaningful interactions.

The skip decision is centralized in the worker pipeline rather than duplicated across API and retry layers.

### High-priority interactions

Interactions likely to require immediate operational action are prioritized within the scheduler.

Examples:
- confirmed demo bookings,
- callback requests,
- payment confirmations,
- escalation language.

These interactions are scheduled ahead of normal-priority interactions when token capacity becomes constrained.

### Classification mechanism

The initial implementation uses lightweight rule-based classification.

The classifier evaluates:
- transcript length,
- keywords/phrases,
- and optional customer-provided metadata.

Examples:
- "booked"
- "confirmed"
- "call me tomorrow"
- "interested"

Rule-based classification was chosen because:
- it is deterministic,
- inexpensive,
- easy to explain operationally,
- and implementable within the constraints of the assignment.

The design intentionally avoids requiring an additional ML model or pre-LLM classification stage.

### Customer overrides

The system also supports optional business-configured overrides.

Examples:
- always prioritize VIP campaigns,
- always prioritize callback campaigns,
- or force immediate processing for specific customer workflows.

This allows operational teams to influence prioritization without requiring deployment changes.

### Why differentiated processing matters

Without differentiated scheduling:
- low-value interactions consume the same scarce token capacity as high-value interactions.

Under heavy burst traffic, this significantly increases operational latency for business-critical calls.

Differentiated processing ensures:
- urgent interactions are processed first,
- low-value interactions are deferred safely,
- and total token capacity is utilized more efficiently.

---

## 7. Recording Pipeline

The current implementation blocks the entire post-call pipeline for a fixed 45-second delay before attempting recording retrieval.

This creates multiple operational problems:
- workers remain idle unnecessarily,
- LLM analysis is delayed even though it does not depend on recordings,
- recordings arriving after 45 seconds are silently lost,
- and failures are not observable.

The redesigned system replaces the fixed-delay approach with an asynchronous polling and retry pipeline.

### Design Goals

The new recording pipeline is designed to:

1. Avoid blocking LLM processing.
2. Retry safely until the recording becomes available.
3. Make failures observable and replayable.
4. Maintain durable state for debugging and recovery.

### Decoupled execution

Recording retrieval and LLM analysis now execute independently and concurrently using asyncio.gather(...).

This avoids idle worker time during recording polling and improves throughput during large campaign bursts.

Instead of executing sequentially:

```text
recording fetch → LLM analysis
```

they execute in parallel:

```text
recording fetch
      │
      ├────> independent async execution
      │
      ▼
LLM analysis
```

This removes unnecessary pipeline latency and prevents recording delays from blocking business-critical analysis.

### Polling strategy

The recording service polls Exotel periodically until:
- the recording becomes available,
- or the retry limit is exceeded.

The scheduler uses exponential backoff between retries. Example retry intervals:

```text
10s → 15s → 15s → 20s → 30 → 30s
```

This approach:
- reduces unnecessary API traffic,
- handles Exotel-side delays gracefully,
- and avoids fixed waiting windows.

### Failure handling

Recording failures are no longer silent.

Each interaction maintains explicit recording state:
- pending
- uploaded
- failed

Failures produce:
- structured error logs,
- retry metadata,
- and durable interaction state updates.

Example failure causes:
- recording never became available,
- Exotel API timeout,
- S3 upload failure,
- invalid recording URL.

### Operational visibility

Instead of relying on DEBUG logs or Redis inspection, an on-call engineer investigating a missing recording can now query:
- `interaction_id`
- `recording_status`
- `retry_count`
- `last_attempt_at`
- `failure_reason`

Example structured log:
```json
{
  "interaction_id": "...",
  "stage": "recording_fetch",
  "status": "failed",
  "retry_count": 5,
  "failure_reason": "recording_not_available"
}
```

---


## 8. Reliability & Durability

The redesigned system improves reliability by introducing durable processing state, centralized retry orchestration, and structured recovery visibility.

The primary design goal is:

> Prevent silent interaction loss and make all failures observable and replayable.
> No interaction should be permanently lost due to worker crashes, Redis failures, retries, or transient infrastructure problems.

The current implementation improves recoverability significantly compared to the original system, while still acknowledging some remaining infrastructure limitations.

### Interaction as source of truth

The interaction record becomes the durable execution source of truth for post-call processing.

Instead of relying entirely on:
- in-memory async execution,
- transient Celery worker state,
- or untracked retries,

the processing lifecycle is persisted inside `interaction_metadata`.

Each interaction stores execution metadata such as:
- processing_status
- recording_status
- retry_count
- processing timestamps
- last_error

This allows operators to inspect and recover interaction state even after worker failures or delayed retries.

### Durable execution flow

The webhook layer performs only lightweight ingestion responsibilities:
1. persist interaction state,
2. enqueue asynchronous work,
3. return immediately.

All expensive operations execute asynchronously inside workers.

This prevents long-running post-call analysis from blocking webhook responsiveness and reduces the risk of request timeout failures.

### Centralized retry orchestration

The original implementation contained overlapping retry mechanisms:
- Celery retries,
- and a separate Redis retry queue.

These systems could retry the same interaction independently, creating duplicate processing risk and inconsistent retry visibility.

The redesigned system consolidates retry ownership into a single retry orchestration layer.

Failed interactions are:
- persisted into the retry queue,
- replayed through a retry poller,
- retried with exponential backoff,
- and logged with structured retry metadata.

This improves retry visibility and operational traceability while reducing conflicting retry execution paths.

### Worker crash recovery

Celery tasks are configured with:
- `acks_late=True`
- `task_reject_on_worker_lost=True`

This ensures tasks are acknowledged only after successful execution.

If a worker crashes mid-processing:
- the task is returned to the broker,
- replayed automatically,
- and resumed using persisted interaction state.

Additionally, interactions entering `PROCESSING` state persist lifecycle metadata such as:
- processing_started_at
- processing_status

Retries detecting abandoned processing states emit structured recovery logs, improving visibility into interrupted executions.

### Retry visibility

Retries are treated as observable operational events rather than silent background behaviour.

The system logs:
- retry scheduling,
- retry replay,
- retry exhaustion,
- and recovery attempts

using structured log events containing:
- interaction_id
- correlation_id
- retry_count
- failure_reason
- processing stage

This improves debuggability during high-volume campaign failures.

### Current durability limitations

The current implementation still retains some dependence on Redis durability.

Specifically:
- Celery broker state,
- retry queue state,
- and retry scheduling metadata

are Redis-backed.

As a result, a Redis outage or restart can still affect retry durability.

The redesign reduces dependence on transient execution state by persisting interaction lifecycle metadata inside the database, but retry orchestration itself is not yet fully durable.

A production implementation would likely:
- move retry orchestration to Kafka/SQS/DB-backed queues,
- centralize scheduler coordination in Redis or another distributed store,
- and introduce dead-letter replay workflows.

### Backpressure instead of failure amplification

The redesigned scheduler introduces proactive admission control before requests reach the LLM provider.

Instead of:
- overloading the provider,
- receiving large waves of 429 responses,
- and amplifying retries,

the scheduler:
- evaluates token availability,
- defers excess work,
- and gradually slows throughput under load.

This converts overload behaviour from:
- cascading failure amplification,
into:
- controlled throughput degradation with retry visibility.

---

## 9. Auditability & Observability

The redesigned system introduces structured, correlation-aware observability across the entire post-call processing pipeline.

The primary goal is:

> Every interaction should be traceable end-to-end across asynchronous workers, retries, recording fetches, scheduler decisions, and downstream updates.

This significantly improves operational debugging, replayability, and customer-level auditability.

### End-to-end traceability

Every interaction is assigned a `correlation_id` at webhook ingress.

This correlation ID propagates through:
- Celery task execution,
- retry queue orchestration,
- recording polling,
- scheduler decisions,
- LLM execution,
- downstream signal jobs,
- and lead stage updates.

This allows operators to trace the complete lifecycle of an interaction across multiple asynchronous systems using a single identifier.

### Durable interaction lifecycle visibility

Each interaction persists processing lifecycle metadata inside `interaction_metadata`.

This includes:
- processing_status
- llm_status
- recording_status
- retry_count
- processing timestamps
- failure metadata
- last_error

Example lifecycle transitions:

```text
PENDING
    ↓
PROCESSING
    ↓
COMPLETED / FAILED
```

This allows debugging even after:
- worker restarts,
- delayed retries,
- or transient infrastructure failures.

### Structured logging

All operational events emit structured logs. Every log event includes:
- interaction_id
- correlation_id
- customer_id
- campaign_id
- processing stage
- retry metadata (if applicable)
- error information (for failure events)

Example processing stages:

```json
{
  "interaction_id": "123",
  "correlation_id": "abc-xyz",
  "customer_id": "cust_001",
  "campaign_id": "cmp_001",
  "stage": "recording_fetch",
  "status": "failed",
  "retry_count": 3,
  "error": "recording_not_available"
}
```

### Retry observability

Retries are treated as explicit operational events rather than silent background behaviour.

The system emits structured logs for:
- retry scheduling,
- retry replay,
- retry exhaustion,
- and interrupted processing recovery.

Each retry event includes:
- interaction_id,
- correlation_id,
- retry_count,
- failure_reason,
- and processing stage.

This allows operators to:
- identify retry storms,
- inspect permanently failing interactions,
- trace replay attempts,
- and manually recover failed workloads when necessary.

### Queue visibility

The redesigned system improves operational visibility into asynchronous workload pressure.

The system tracks:
- retry queue depth,
- deferred interaction counts,
- scheduler backlog,
- and token utilization pressure.

These metrics help operators identify:
- overload conditions,
- provider throttling pressure,
- uneven customer workload distribution,
- and growing retry backlog conditions before they become outages.

### Alert conditions

The following events are treated as alertable operational conditions:

| Condition | Reason |
|---|---|
| Retry exhaustion | Interaction exceeded maximum retry attempts and requires manual investigation or replay |
| Recording upload failure | Recording could not be fetched from Exotel or failed during upload/storage operations |
| Large scheduler backlog | LLM token capacity saturation or sustained queue pressure |
| Repeated provider throttling (429 responses) | Indicates sustained provider-side rate limiting despite scheduler admission control and retry backoff |
| Worker recovery events | Indicates interrupted task execution that was replayed after worker loss due to `acks_late=True` and `task_reject_on_worker_lost=True` |
| Excessive retry queue growth | Persistent downstream failures, provider instability, or prolonged overload conditions |
| Interactions stuck in PROCESSING state | Possible abandoned execution caused by worker interruption or repeated retry failures |

### Debugging a failed interaction after several days

To debug a failed interaction, an operator can:

1. Query the interaction using `interaction_id`
2. Inspect durable processing metadata stored in `interaction_metadata`
3. Search structured logs using `correlation_id`
4. Review retry attempts and failure history
5. Inspect scheduler admission and defer decisions
6. Determine whether the interaction:
   - failed permanently,
   - exhausted retries,
   - was replayed successfully,
   - or remains deferred awaiting capacity

This provides significantly stronger operational visibility compared to the original implementation, where failures could silently disappear across asynchronous execution boundaries.

---

## 10. Data Model

The current implementation intentionally minimizes schema churn and reuses the existing `interaction_metadata` JSONB column for workflow state persistence.

This allowed the redesign to:
- avoid large migration risk,
- remain backward compatible,
- and rapidly introduce durable processing state without restructuring core interaction models.

The following workflow metadata is now persisted inside `interaction_metadata`:

- processing_status
- recording_status
- llm_status
- retry_count
- processing timestamps
- last_error
- token usage metadata

Example structure:

```json
{
  "processing_status": "processing",
  "recording_status": "uploaded",
  "llm_status": "completed",
  "retry_count": 1,
  "tokens_used": 1320,
  "processing_started_at": "2026-05-08T10:00:00Z",
  "processing_completed_at": "2026-05-08T10:00:12Z",
  "last_error": null
}
```

Why JSONB was reused

Using JSONB allowed:
- incremental rollout,
- flexible workflow evolution,
- and reduced migration complexity during rapid iteration.

This was particularly useful because workflow state requirements were still evolving during redesign.

### Recommended production schema evolution

For long-term scalability and observability, several fields would likely be promoted into dedicated indexed columns or tables.

Example future schema additions:
```sql
ALTER TABLE interactions
ADD COLUMN processing_status VARCHAR(50),
ADD COLUMN recording_status VARCHAR(50),
ADD COLUMN llm_status VARCHAR(50),
ADD COLUMN retry_count INTEGER DEFAULT 0,
ADD COLUMN correlation_id UUID,
ADD COLUMN last_error TEXT,
ADD COLUMN processing_started_at TIMESTAMP,
ADD COLUMN processing_completed_at TIMESTAMP;

CREATE INDEX idx_interactions_processing_status
ON interactions(processing_status);

CREATE INDEX idx_interactions_correlation_id
ON interactions(correlation_id);
```

And for high-scale production observability, retry history and lifecycle events would likely move into a dedicated append-only audit table.

Example:
```sql
CREATE TABLE interaction_processing_events (
    id UUID PRIMARY KEY,
    interaction_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    stage VARCHAR(100),
    status VARCHAR(50),
    error_message TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);
```

This would provide:
- immutable execution history,
- replay visibility,
- operational debugging support,
- and customer-level audit trails.

---

## 11. Security

The post-call processing pipeline handles multiple categories of sensitive data, including:
- customer PII (phone numbers, emails, names),
- call transcripts,
- call recordings,
- extracted entities,
- and customer-specific operational metadata.

### Data in transit

All communication between services should occur over TLS-secured channels, including:
- webhook ingestion,
- Exotel API calls,
- LLM provider requests,
- Redis communication,
- and object storage uploads.

This prevents interception of transcripts, recordings, and customer metadata across network boundaries.

### Data at rest

Sensitive data should remain encrypted at rest:
- recordings stored in encrypted S3 buckets,
- database storage protected using disk-level encryption,
- and Redis deployments restricted to private network boundaries.

Access to recordings and transcripts should follow least-privilege access policies.

### Logging & observability safety

Structured logging intentionally avoids dumping:
- raw transcript payloads,
- full provider responses,
- or sensitive customer PII.

Operational logs contain:
- correlation identifiers,
- processing metadata,
- retry information,
- and failure context,

while minimizing exposure of customer conversation data.

### Operational isolation

Customer attribution is enforced throughout the pipeline using:
- customer_id,
- campaign_id,
- and correlation_id.

This improves:
- auditability,
- tenant isolation,
- and token usage traceability for billing and debugging purposes.

### Production hardening considerations

A production deployment would additionally introduce:
- secret rotation for provider credentials,
- IAM-scoped storage access,
- retention policies for recordings and transcripts,
- audit logging for privileged access,
- and centralized secrets management.

---

## 12. API Interface

The external API contract was intentionally kept unchanged:

```http
POST /session/{session_id}/interaction/{interaction_id}/end
```

The redesign focused on improving:
- internal execution reliability,
- asynchronous orchestration,
- retry visibility,
- rate-limit management,
- and operational durability,

without forcing upstream clients or telephony integrations to change behaviour.

### Why the contract was preserved

The webhook endpoint already represented the correct domain boundary:
- a call has ended,
- interaction state must be persisted,
- and post-call processing should begin asynchronously.

The primary problems were not API-shape problems; they were:
- execution orchestration problems,
- retry coordination problems,
- durability gaps,
- and missing rate-limit awareness.

Changing the endpoint contract would not meaningfully solve those architectural issues.

### Behavioural changes without contract changes

Although the external interface remained stable, the internal processing semantics changed significantly.

Key behavioural changes include:
- all interactions now flow through the asynchronous processing pipeline,
- short-call gating moved into worker processing,
- recording retrieval and LLM analysis execute independently,
- retries are centrally orchestrated,
- and downstream actions execute only after durable processing completion.

This preserves backward compatibility while improving operational correctness.

### Potential future API evolution

A future production system could optionally introduce:
- explicit webhook acknowledgment IDs,
- replay endpoints,
- interaction processing status endpoints,
- or customer-facing observability APIs.

However, these were intentionally excluded from the current redesign to keep the scope focused on reliability and processing architecture rather than external API expansion.

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What Was Chosen Instead |
|---|---|---|
| Kafka-based retry orchestration | Strong durability and replay guarantees | Too operationally heavy for the assignment scope. A lightweight Redis-backed retry queue with replay polling was sufficient for the current redesign. |
| Distributed token bucket using Redis | Accurate global TPM coordination across workers | Added distributed coordination complexity. Current implementation uses process-local token buckets with documented limitations. |
| Celery-only retries | Simpler retry implementation | Weak retry visibility and limited replay control. A centralized retry queue provided better observability and retry orchestration. |
| ML-based interaction prioritization | Better prioritization accuracy | Rule-based prioritization was deterministic, cheaper, and easier to operationalize within the assignment timeline. |
| Dedicated recording-processing workers | Better workload isolation | Added orchestration complexity. Parallel async execution (`asyncio.gather`) was sufficient for current requirements. |
| Fully normalized workflow tables | Stronger relational modeling and indexing | Existing JSONB metadata allowed faster iteration, lower migration risk, and flexible workflow evolution. |
| Hard circuit-breaker freeze logic | Simpler overload protection | Binary freezing causes operational disruption. Scheduler-based backpressure provided more graceful degradation under load. |

---

## 14. Known Weaknesses

1. The current LLM scheduler is process-local. Each Celery worker maintains independent in-memory token buckets, which means global TPM enforcement is not perfectly coordinated across multiple workers. A production implementation should centralize token accounting using Redis or another distributed coordination layer.

2. Retry durability still depends on Redis. A Redis outage or restart can affect:
  - Celery broker state,
  - retry queue state,
  - and retry scheduling metadata.

  A production system would likely move retry orchestration to Kafka, SQS, or a database-backed queue.

3. `dequeue_ready()` is not fully atomic. Multiple retry pollers could theoretically dequeue and replay the same interaction simultaneously under race conditions. A production implementation should use Redis sorted sets, Lua scripts, or atomic queue primitives.

4. The retry poller implementation is intentionally lightweight and process-driven. Under very large workloads, retry orchestration would likely need:
  - dedicated workers,
  - distributed scheduling,
  - and dead-letter replay tooling.

5. Idempotency protections are only partially implemented. Some downstream operations could still theoretically execute more than once during replay or worker recovery scenarios. A production implementation would introduce stronger idempotency guarantees using persistent execution markers or deduplication keys.

6. The current implementation uses JSONB metadata for workflow state persistence. While flexible, this reduces:
  - queryability,
  - indexing efficiency,
  - and relational enforcement

  compared to fully normalized workflow tables.

7. The scheduler currently estimates token usage before execution using heuristics. Actual token usage may differ from estimates, which can temporarily affect scheduling accuracy under burst traffic.

8. Retry replay ordering is not strictly guaranteed. Interactions pushed back into the retry queue may not preserve perfect temporal ordering during repeated polling cycles.

9. Recording retrieval still depends on external provider availability and eventual consistency guarantees from Exotel. Extremely delayed or missing recordings may still require manual operational intervention.

10. Structured logging is implemented at the application layer but is not yet integrated with centralized observability tooling such as:
  - Prometheus,
  - Grafana,
  - OpenTelemetry,
  - or distributed tracing systems.

11. The current implementation does not include dead-letter queue (DLQ) infrastructure for permanently failed interactions. Retry exhaustion is observable, but replay remains operationally manual.

12. Customer prioritization is currently rule-based and configuration-driven. More sophisticated prioritization models could incorporate:
  - customer SLAs,
  - campaign urgency,
  - or predictive business-value scoring.

13. The retry queue currently operates independently from scheduler backlog pressure. A production implementation would likely unify:
  - retry orchestration,
  - scheduler pressure,
  - and queue admission policies

  into a single coordinated control plane.

14. The current design focuses primarily on reliability and orchestration correctness. It does not yet include:
  - autoscaling policies,
  - workload-aware worker scaling,
  - or infrastructure cost optimization strategies.

16. Security hardening is intentionally lightweight for local development simplicity. Production systems would require:
  - stronger IAM isolation,
  - secret rotation,
  - audit access controls,
  - retention policies,
  - and stricter tenant isolation guarantees.

17. The current implementation improves worker crash recovery using:
  - `acks_late=True`
  - and `task_reject_on_worker_lost=True`

  but does not yet implement true checkpoint-based resumability for partially completed workflows.

18. Queue visibility currently relies primarily on structured logging and lightweight metrics. A production system would require:
  - centralized dashboards,
  - alert routing,
  - SLO tracking,
  - and historical workload analytics.

---

## 15. What I Would Do With More Time

1. Move scheduler token accounting into Redis or another distributed coordination layer to enforce globally consistent TPM limits across multiple workers.

2. Replace the lightweight retry poller with a dedicated distributed retry orchestration system supporting:
   - atomic dequeue,
   - delayed scheduling,
   - dead-letter queues,
   - and replay tooling.

3. Introduce proper dead-letter queue (DLQ) handling for permanently failed interactions instead of relying on operational replay through logs.

4. Add centralized observability tooling using:
   - Prometheus,
   - Grafana,
   - OpenTelemetry

5. Add richer operational dashboards for:
   - scheduler backlog,
   - retry pressure,
   - token utilization,
   - worker throughput,
   - and customer-level queue visibility.

6. Add adaptive token estimation and dynamic scheduling heuristics using historical interaction patterns and actual provider token usage.

7. Replace lightweight rule-based prioritization with configurable SLA-aware scheduling and customer policy controls.

8. Improve retry durability by reducing Redis dependence and introducing database-backed or streaming-based retry persistence.

9. Add integration and chaos testing for:
   - worker crashes,
   - Redis restarts,
   - provider throttling,
   - and partial infrastructure failures.

10. Improve recording orchestration by separating recording ingestion into an independently scalable worker pool under very high campaign concurrency.

11. Normalize frequently queried workflow metadata into indexed relational columns or dedicated workflow tables for improved analytics and operational querying.

12. Add customer-facing replay and processing visibility APIs for operational support and debugging workflows.
