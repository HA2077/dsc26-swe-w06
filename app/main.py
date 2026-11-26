"""The HTTP layer around explain_stream.

Keep `StreamSettings` and `create_app` exactly as named; the public and hidden tests import
them."""
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Annotated, Literal

from fastapi import Depends, FastAPI, Header, Request
from fastapi.responses import JSONResponse
from fastapi.sse import EventSourceResponse, ServerSentEvent
from pydantic import BaseModel, Field

from app.budget import DailyBudget, estimate_cost_usd
from app.providers import LLMProvider, build_prompt
from app.stream import explain_stream


@dataclass
class StreamSettings:
    ttft_timeout_s: float = 2.0
    total_timeout_s: float = 20.0
    max_output_tokens: int = 400


class ClauseRequest(BaseModel):
    clause_text: str = Field(min_length=1, max_length=8000)
    language: Literal["ar", "en"]


class _BudgetExhausted(Exception):
    pass


def create_app(providers: Sequence[LLMProvider], budget: DailyBudget,
               settings: StreamSettings | None = None) -> FastAPI:
    """Return a NEW FastAPI app exposing POST /v1/clauses/explain.

    - Header X-Tenant-ID (required, 1-64 chars). Body {"clause_text": 1-8000 chars,
      "language": "ar" | "en"}. Build the prompt with app.providers.build_prompt.

    - Before streaming: invalid input is 422; if the tenant cannot afford even the
      cheapest provider's worst-case estimate, 429 application/problem+json with type
      /problems/budget-exhausted. Providers are not called in either case.

    - Otherwise, a text/event-stream response (use fastapi.sse.EventSourceResponse and
      ServerSentEvent) whose events are explain_stream's events, named meta, token, done
      and error, each event's data as JSON.
      
    """
    settings = settings if settings is not None else StreamSettings()
    app = FastAPI()

    @app.exception_handler(_BudgetExhausted)
    async def _budget_exhausted_handler(request: Request, exc: _BudgetExhausted) -> JSONResponse:
        return JSONResponse(
            status_code=429,
            media_type="application/problem+json",
            content={
                "type": "/problems/budget-exhausted",
                "title": "Budget exhausted",
                "status": 429,
            },
        )

    def _prepare(
        body: ClauseRequest,
        tenant: Annotated[str, Header(alias="X-Tenant-ID", min_length=1, max_length=64)],
    ) -> tuple[str, str]:
        prompt = build_prompt(body.clause_text, body.language)
        cheapest = min(
            (estimate_cost_usd(p, prompt, settings.max_output_tokens) for p in providers),
            default=None,
        )
        if cheapest is None or budget.remaining(tenant) < cheapest:
            raise _BudgetExhausted
        return prompt, tenant

    @app.post("/v1/clauses/explain", response_class=EventSourceResponse)
    async def explain(
        prep: Annotated[tuple[str, str], Depends(_prepare)],
    ) -> AsyncIterator[ServerSentEvent]:
        prompt, tenant = prep
        async for ev in explain_stream(
            prompt,
            providers,
            tenant=tenant,
            budget=budget,
            ttft_timeout_s=settings.ttft_timeout_s,
            total_timeout_s=settings.total_timeout_s,
            max_output_tokens=settings.max_output_tokens,
        ):
            yield ServerSentEvent(event=ev.kind, data=ev.data)

    return app
