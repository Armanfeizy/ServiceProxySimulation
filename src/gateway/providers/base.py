"""Provider adapter contract.

The gateway speaks one canonical shape to callers; each adapter translates it to and
from one provider's wire format — URL, auth, extra fields, response shape, and what
that provider's errors mean. Adding provider D = one adapter (if its format differs)
plus one config block in providers.toml.
"""

from __future__ import annotations

import email.utils
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from gateway.resilience.outcomes import Outcome
from gateway.settings import ProviderConfig


@dataclass(frozen=True, slots=True)
class CanonicalRequest:
    trace_id: str
    tenant: str
    body: dict[str, Any]
    raw_body: bytes
    idempotency_key: str | None


@dataclass(frozen=True, slots=True)
class UpstreamRequest:
    content: bytes
    headers: dict[str, str]


@dataclass(frozen=True, slots=True)
class CanonicalResult:
    outcome: Outcome
    status_code: int
    data: Any                  # normalized provider payload (JSON-able) or error detail
    retry_after_ms: int = 0


class ProviderAdapter(ABC):
    kind: ClassVar[str]

    def __init__(self, config: ProviderConfig) -> None:
        self.config = config

    @abstractmethod
    def build_request(self, request: CanonicalRequest, attempt: int) -> UpstreamRequest:
        """Map the canonical request to this provider's payload, creds and headers."""

    @abstractmethod
    def parse_response(
        self, status_code: int, headers: Mapping[str, str], content: bytes
    ) -> CanonicalResult:
        """Map the provider's response to a canonical result, classifying failures."""


ADAPTERS: dict[str, type[ProviderAdapter]] = {}


def register_adapter(kind: str) -> Callable[[type[ProviderAdapter]], type[ProviderAdapter]]:
    def decorator(cls: type[ProviderAdapter]) -> type[ProviderAdapter]:
        cls.kind = kind
        ADAPTERS[kind] = cls
        return cls

    return decorator


def build_adapter(config: ProviderConfig) -> ProviderAdapter:
    try:
        return ADAPTERS[config.adapter](config)
    except KeyError:
        known = ", ".join(sorted(ADAPTERS))
        raise ValueError(f"provider {config.name}: unknown adapter {config.adapter!r} ({known})") from None


def parse_retry_after(value: str | None, *, now: float | None = None) -> int:
    """Retry-After as milliseconds. Accepts delta-seconds or an HTTP-date."""
    if not value:
        return 0
    value = value.strip()
    try:
        return max(0, int(float(value) * 1000))
    except ValueError:
        pass
    try:
        when = email.utils.parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        return 0
    return max(0, int((when - (now if now is not None else time.time())) * 1000))
