"""The streaming core: provider fallback, two deadlines, budget reserve/settle.

Keep `Event` and `explain_stream` exactly as named; the public and hidden tests import them."""
import asyncio
import time
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass, field

from app.budget import DailyBudget, actual_cost_usd, estimate_cost_usd
from app.providers import LLMProvider, ProviderError


@dataclass(frozen=True)
class Event:
    kind: str                       # "meta" | "token" | "done" | "error"
    data: dict = field(default_factory=dict)


async def explain_stream(prompt: str, providers: Sequence[LLMProvider], *, tenant: str,
                         budget: DailyBudget, ttft_timeout_s: float, total_timeout_s: float,
                         max_output_tokens: int) -> AsyncIterator[Event]:
    """Yield Event("meta", ...), Event("token", ...)* and a final Event("done" | "error", ...).

    Rules, in order (see README.md for the full statement):
    - Try providers in the given order. Before each, reserve its estimate; skip an
      unaffordable provider without calling it.

    - Wait at most ttft_timeout_s (never past the overall deadline) for the first token.
      On timeout, ProviderError, or an empty stream before the first token: settle the
      reservation at 0 and try the next provider.

    - On the first token, emit "meta" {"provider": name}, then "token" {"text": ...} per token.
    - After the first token, never switch provider: a ProviderError now is a fatal
      "error" {"code": "upstream_interrupted", "partial": True}.

    - The whole call never runs past total_timeout_s from the start; past the deadline, emit
      "error" {"code": "deadline_exceeded", "partial": <any token sent yet>}.

    - On success, settle at the actual cost and emit
      "done" {"provider", "output_tokens", "cost_usd"}.

    - Every affordable provider failed before its first token: "error" {"code": "all_providers_failed"}.
      No provider was affordable: "error" {"code": "budget_exhausted"}.

    - Whatever happens, including the caller closing this generator early, close the
      provider's stream (aclose()) and settle its reservation at the actual cost of the
      tokens it produced.
      
    """
    deadline = time.monotonic() + total_timeout_s
    any_affordable = False

    for provider in providers:
        if time.monotonic() >= deadline:
            yield Event("error", {"code": "deadline_exceeded", "partial": False})
            return

        reserved = estimate_cost_usd(provider, prompt, max_output_tokens)
        if not budget.try_reserve(tenant, reserved):
            continue

        any_affordable = True
        stream = provider.stream(prompt, max_output_tokens)
        produced = 0
        settled = False

        def settle_now(cost: float | None = None) -> float:
            nonlocal settled
            if cost is None:
                cost = 0.0 if produced == 0 else actual_cost_usd(provider, prompt, produced)
            if not settled:
                budget.settle(tenant, reserved, cost)
                settled = True
            return cost

        try:
            left = deadline - time.monotonic()
            if left <= 0:
                settle_now()
                yield Event("error", {"code": "deadline_exceeded", "partial": False})
                return

            wait = min(ttft_timeout_s, left)
            try:
                first = await asyncio.wait_for(anext(stream), timeout=wait)
            except TimeoutError:
                if wait >= left or time.monotonic() >= deadline:
                    settle_now()
                    yield Event("error", {"code": "deadline_exceeded", "partial": False})
                    return
                continue
            except (StopAsyncIteration, ProviderError):
                continue

            produced = 1
            yield Event("meta", {"provider": provider.name})
            yield Event("token", {"text": first})

            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    settle_now()
                    yield Event("error", {"code": "deadline_exceeded", "partial": True})
                    return
                try:
                    token = await asyncio.wait_for(anext(stream), timeout=left)
                except StopAsyncIteration:
                    cost = settle_now()
                    yield Event("done", {
                        "provider": provider.name,
                        "output_tokens": produced,
                        "cost_usd": cost,
                    })
                    return
                except TimeoutError:
                    settle_now()
                    yield Event("error", {"code": "deadline_exceeded", "partial": True})
                    return
                except ProviderError:
                    settle_now()
                    yield Event("error", {"code": "upstream_interrupted", "partial": True})
                    return
                produced += 1
                yield Event("token", {"text": token})
        finally:
            try:
                try:
                    await stream.aclose()
                except Exception:
                    pass
            finally:
                settle_now()

    if not any_affordable:
        yield Event("error", {"code": "budget_exhausted"})
    else:
        yield Event("error", {"code": "all_providers_failed"})
