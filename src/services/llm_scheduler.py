"""
Centralized LLM scheduler responsible for:
- Global token-per-minute enforcement
- Per-customer token budgeting
- Admission control before LLM execution
- Natural backpressure instead of binary circuit breaking
- Structured audit logging

This scheduler intentionally acts as an admission controller rather than
just a request-per-minute limiter.

Why?
LLM providers enforce limits primarily on TOKENS per minute, not just
requests per minute. Transcript sizes vary significantly across calls,
making request-count limiting insufficient.

Design goals:
- Prevent provider-side 429 amplification
- Enforce customer isolation
- Avoid retry storms
- Keep implementation simple enough for local execution/testing
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, Dict

from src.config import settings

logger = logging.getLogger(__name__)


class InteractionPriority(str, Enum):
    """Priority assigned to post-call processing requests."""

    HIGH = "high"
    NORMAL = "normal"


@dataclass(slots=True)
class LLMRequest:
    """
    Represents a schedulable LLM request.

    estimated_tokens:
        Estimated token consumption BEFORE execution.
        Used for admission control.
    """

    interaction_id: str
    customer_id: str
    campaign_id: str
    estimated_tokens: int
    priority: InteractionPriority


class TokenBucket:
    """
    Simple token bucket implementation.

    Tokens refill continuously over time.

    Example:
        capacity = 90_000 TPM
        refill_rate = 1_500 tokens/sec
    """

    def __init__(self, capacity: int, refill_rate_per_second: float):
        self.capacity = float(capacity)
        self.tokens = float(capacity)
        self.refill_rate_per_second = refill_rate_per_second
        self.last_refill_time = time.monotonic()
        self._lock = asyncio.Lock()

    async def try_consume(self, token_count: int) -> bool:
        """
        Attempt to consume tokens from the bucket.

        Returns:
            True if tokens were successfully consumed.
            False if insufficient capacity exists.
        """
        async with self._lock:
            self._refill()

            if self.tokens >= token_count:
                self.tokens -= token_count
                return True

            return False

    async def available_tokens(self) -> int:
        """Return currently available tokens."""
        async with self._lock:
            self._refill()
            return int(self.tokens)

    def _refill(self) -> None:
        """Refill tokens based on elapsed wall-clock time."""
        now = time.monotonic()
        elapsed_seconds = now - self.last_refill_time

        refill_amount = elapsed_seconds * self.refill_rate_per_second

        self.tokens = min(self.capacity, self.tokens + refill_amount)
        self.last_refill_time = now


class LLMScheduler:
    """
    Centralized scheduler controlling ALL outbound LLM execution.

    Responsibilities:
    - Enforce global provider TPM limits
    - Enforce per-customer TPM budgets
    - Apply admission control BEFORE requests are sent
    - Defer requests instead of triggering provider 429s
    - Emit structured scheduler telemetry
    """

    def __init__(self) -> None:
        global_tpm = settings.LLM_GLOBAL_TPM

        self.global_bucket = TokenBucket(
            capacity=global_tpm,
            refill_rate_per_second=global_tpm / 60,
        )

        self.customer_buckets: Dict[str, TokenBucket] = {}

        for customer_id, budget in settings.LLM_CUSTOMER_TOKEN_BUDGETS.items():
            self.customer_buckets[customer_id] = TokenBucket(
                capacity=budget,
                refill_rate_per_second=budget / 60,
            )

        self.retry_interval_seconds = (
            settings.LLM_SCHEDULER_RETRY_INTERVAL_SECONDS
        )

    async def schedule_and_execute(
        self,
        request: LLMRequest,
        execute_fn: Callable[[], Awaitable[T]],
    ) -> T:
        """
        Schedule and execute an LLM request.

        Flow:
            1. Check customer budget
            2. Check global provider capacity
            3. Admit request if capacity exists
            4. Otherwise defer via retry loop

        This intentionally DEFERs instead of failing.
        Deferred execution creates natural backpressure without generating
        provider-side 429 retry storms.
        """

        customer_bucket = self._get_customer_bucket(request.customer_id)

        while True:
            customer_allowed = await customer_bucket.try_consume(
                request.estimated_tokens
            )

            if not customer_allowed:
                logger.warning(
                    "customer_budget_exhausted",
                    extra={
                        "interaction_id": request.interaction_id,
                        "customer_id": request.customer_id,
                        "campaign_id": request.campaign_id,
                        "estimated_tokens": request.estimated_tokens,
                        "priority": request.priority.value,
                    },
                )

                await asyncio.sleep(self.retry_interval_seconds)
                continue

            global_allowed = await self.global_bucket.try_consume(
                request.estimated_tokens
            )

            if not global_allowed:
                logger.warning(
                    "global_tpm_exhausted",
                    extra={
                        "interaction_id": request.interaction_id,
                        "customer_id": request.customer_id,
                        "campaign_id": request.campaign_id,
                        "estimated_tokens": request.estimated_tokens,
                        "priority": request.priority.value,
                    },
                )

                await asyncio.sleep(self.retry_interval_seconds)
                continue

            logger.info(
                "llm_request_admitted",
                extra={
                    "interaction_id": request.interaction_id,
                    "customer_id": request.customer_id,
                    "campaign_id": request.campaign_id,
                    "estimated_tokens": request.estimated_tokens,
                    "priority": request.priority.value,
                },
            )

            return await execute_fn()

    def _get_customer_bucket(self, customer_id: str) -> TokenBucket:
        """
        Return the bucket for a customer.

        Customers without explicit configuration receive the default budget.
        """
        if customer_id not in self.customer_buckets:
            default_budget = settings.LLM_DEFAULT_CUSTOMER_TPM

            self.customer_buckets[customer_id] = TokenBucket(
                capacity=default_budget,
                refill_rate_per_second=default_budget / 60,
            )

        return self.customer_buckets[customer_id]


llm_scheduler = LLMScheduler()