# Post-Call Processing Pipeline — Design Document

**Author:** [Your Name]
**Date:** [Date]

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

Recording retrieval and LLM analysis are treated as independent tasks.

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
5s → 10s → 20s → 40s → 60s
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

The redesigned system treats durability as a first-class concern.

The primary design goal is:

> No interaction should be permanently lost due to worker crashes, Redis failures, retries, or transient infrastructure problems.

### Interaction as source of truth

The interaction record becomes the durable execution source of truth.

Instead of relying on:
- in-memory async tasks,
- Redis-only retry queues,
- or Celery task state,

the processing lifecycle is persisted directly against the interaction.

Each interaction stores explicit processing state such as:
- processing_status
- recording_status
- llm_status
- retry_count
- last_error

This allows the system to recover processing progress even after infrastructure restarts.

### Durable execution flow

The webhook layer only:
1. persists interaction state,
2. and enqueues durable work.

All business logic executes inside workers.

This removes fire-and-forget execution paths such as `asyncio.create_task(...)`, which currently lose tasks during process crashes.

### Consolidated retry handling

The existing design contains two independent retry systems:
- Celery retries,
- and a Redis retry queue.

These systems can both retry the same interaction, causing duplicate execution.

The redesign removes the separate Redis retry queue and standardizes retry handling through a single retry path.

Retries become:
- centralized,
- observable,
- and idempotent.

### Idempotent processing

Workers check interaction processing state before executing expensive operations.

Examples:
- completed LLM analysis is not re-run,
- completed recording uploads are not duplicated,
- downstream actions are not triggered twice.

This prevents duplicate processing during retries or worker redelivery.

### Failure recovery

Failures are categorized into:
- retryable failures,
- and terminal failures.

Examples:
- 429 rate limits → retryable
- transient DB/network failures → retryable
- invalid payload structure → terminal

Retryable failures are deferred automatically using scheduler-aware retry logic.

Terminal failures:
- update durable interaction state,
- emit structured alerts,
- and remain replayable later.

### Removal of Redis-only durability

The previous retry design depended entirely on Redis:
- broker queue,
- retry queue,
- retry counters.

This created a shared failure domain where Redis outages could lose both primary and retry execution paths simultaneously.
The redesign reduces dependence on ephemeral Redis state and stores execution progress durably in the database.

### Natural backpressure instead of failure amplification

The current design amplifies overload:
- provider 429s trigger retries,
- retries increase queue depth,
- queue growth increases latency,
- and the system destabilizes further.

The scheduler prevents this by enforcing admission control before requests are sent to the LLM provider.

Interactions are queued before overload occurs instead of failing after overload has already started.

This converts overload from:
- failure amplification,
into:
- controlled throughput degradation.

---

## 9. Auditability & Observability

_How would you debug a specific failed interaction 3 days after the fact?_

### What you log (and what fields every log event includes)

### Alert conditions

---

## 10. Data Model

_Schema changes required. Show the SQL._

```sql
-- Your schema additions/changes here
```

---

## 11. Security

_What data in this system is sensitive? How do you protect it at rest and in transit?_

---

## 12. API Interface

_Did you change the API contract (`POST /session/.../end`)? If yes, explain why. If no, explain why you kept it._

---

## 13. Trade-offs & Alternatives Considered

| Option | Why Considered | Why Rejected / What You Chose Instead |
|--------|---------------|--------------------------------------|
| ... | ... | ... |

---

## 14. Known Weaknesses

_What are the gaps in your design? What would you address next?_

---

## 15. What I Would Do With More Time

_Specific, prioritised list — not a generic wishlist._

1. ...
2. ...
