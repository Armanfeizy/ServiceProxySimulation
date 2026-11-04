"""Concrete adapters."""

from __future__ import annotations

from collections.abc import Mapping

import orjson

from gateway.providers.base import (
    CanonicalRequest,
    CanonicalResult,
    ProviderAdapter,
    UpstreamRequest,
    parse_retry_after,
    register_adapter,
)
from gateway.resilience.outcomes import Outcome, classify_status


@register_adapter("generic_json")
class GenericJsonAdapter(ProviderAdapter):
    """JSON in, JSON out, failure signalled by HTTP status.

    The starter-kit providers all expose the same `POST /v1/process` contract, so one
    adapter serves A, B and C; a provider with a different shape gets its own class.
    """

    def build_request(self, request: CanonicalRequest, attempt: int) -> UpstreamRequest:
        headers = {
            "content-type": "application/json",
            "x-request-id": request.trace_id,
            "x-attempt": str(attempt),
            **self.config.headers,  # credentials / static extras from config
        }
        if request.idempotency_key and self.config.supports_idempotency_key:
            # Lets the provider deduplicate, which makes read-timeouts safe to retry.
            headers["idempotency-key"] = request.idempotency_key
        return UpstreamRequest(content=request.raw_body, headers=headers)

    def parse_response(
        self, status_code: int, headers: Mapping[str, str], content: bytes
    ) -> CanonicalResult:
        outcome = classify_status(status_code)
        try:
            data = orjson.loads(content) if content else None
        except orjson.JSONDecodeError:
            if outcome is Outcome.SUCCESS:
                # A 2xx we cannot read is not a success; we cannot tell what happened.
                return CanonicalResult(Outcome.PROTOCOL_ERROR, status_code, None)
            data = {"raw": content[:2048].decode("utf-8", "replace")}
        retry_after = parse_retry_after(headers.get("retry-after")) if outcome is Outcome.RATE_LIMITED else 0
        return CanonicalResult(outcome, status_code, data, retry_after)
