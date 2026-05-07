# Post-Call Processing Pipeline — Design Document

**Author:** [Your Name]
**Date:** [Date]

---

## 1. Assumptions

_State every assumption you made about the business, system, or environment. Be specific. These will be discussed in the follow-up._

1. Not all calls are equally important, some outcomes (e.g., confirmed bookings) require near real-time processing, while others (e.g., short calls, uninterested leads) can be deferred.
2. LLM providers enforce strict rate limits (requests/min and tokens/min), and exceeding them results in 429 errors without guaranteed retry success.
3. Customers may have different priorities or pre-allocated LLM budgets, implying fairness and isolation requirements.
4. The system must handle burst traffic (e.g., 100K calls completing within a short window).
5. Post-call processing is asynchronous and does not need to block the webhook response.
6. Interaction is the source of truth for all post-call processing state.
7. Failures (LLM, recording, CRM push) are expected and must be retried with visibility.
---

## 2. Problem Diagnosis

_Before designing anything: what is actually broken, and why does it break at scale? In your own words._

1. No rate limit awareness:
   The system sends LLM requests as soon as calls complete, without accounting for provider limits (requests/min, tokens/min). Under burst load (e.g., 100K calls), this results in widespread 429 errors, cascading retries, and system instability.

2. No prioritization or scheduling:
   All interactions are treated equally and pushed into a single queue. There is no distinction between high-value calls (e.g., confirmed bookings) and low-value calls, leading to inefficient use of limited LLM capacity.

3. Fragile async execution:
   Critical tasks (signal jobs, lead stage updates) are triggered using asyncio.create_task in the API layer. These are not durable and are lost on process restarts, violating reliability guarantees.

4. Incorrect execution order:
   Downstream actions (signal jobs) are triggered before LLM analysis completes, resulting in empty or incorrect payloads being sent to dependent systems.

5. Recording pipeline is unreliable:
   The system uses a fixed asyncio.sleep(45s) delay to fetch recordings. If recordings are delayed beyond this window, they are silently skipped with no retry or alerting.

6. Lack of durable task tracking:
   Task execution relies on Celery + Redis without a persistent source of truth. If Redis or workers restart, in-flight tasks are lost with no recovery mechanism.

7. Inconsistent business logic placement:
   Logic such as "short transcript detection" exists in multiple places (API and model), leading to inconsistencies during retries and reprocessing.

8. Poor observability:
   There is no end-to-end traceability for an interaction. Logs lack correlation IDs and do not capture processing stages, making debugging difficult.
---

## 3. Architecture Overview

_End-to-end flow from call-end webhook to completed analysis. Include a diagram._

```
[Your architecture diagram — ASCII or Mermaid]
```

### Key design decisions

1. Move all business logic out of the API layer into a worker-based pipeline.
2. Introduce a rate limit–aware scheduler to control LLM usage.
3. Use Interaction as the source of truth for processing state.
4. Replace fire-and-forget async calls with durable, state-driven execution.
5. Separate ingestion (API) from processing (workers).

---

## 4. Rate Limit Management

_This is the primary problem. How does your system respect LLM rate limits across 100K calls?_

### How you track rate limit usage

### How you decide what to process now vs. defer

### What happens when the limit is hit (recovery, not crash)

---

## 5. Per-Customer Token Budgeting

_If total capacity is N tokens/min and K customers are active simultaneously:_

- How do you allocate capacity across customers?
- What guarantees does a customer with a pre-allocated budget receive?
- What happens when a customer exceeds their budget?
- What happens to unallocated headroom?

---

## 6. Differentiated Processing

_Some call outcomes are time-sensitive. Some can wait. How do you determine which is which?_

_What mechanism do you use — is it a classification step, a flag set by the business, something else? Justify your choice._

---

## 7. Recording Pipeline

_Replacement for `asyncio.sleep(45s)`. How does it work? What does a failure look like to the on-call engineer?_

---

## 8. Reliability & Durability

_How do you ensure no analysis result is permanently lost?_

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
