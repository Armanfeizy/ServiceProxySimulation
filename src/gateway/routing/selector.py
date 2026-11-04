"""Provider selection: power-of-two-choices over eligible providers.

Deterministic "pick the best" makes every replica stampede the provider that looks
best right now (herding). Sampling two at random and taking the better one keeps load
spread while still strongly preferring healthy, fast providers (Finagle, Envoy).
"""

from __future__ import annotations

import random
from dataclasses import dataclass

from gateway.providers.registry import ProviderRegistry, ProviderRuntime
from gateway.resilience.breaker import BreakerState
from gateway.resilience.coordinator import Coordinator, now_ms


@dataclass(frozen=True, slots=True)
class Selection:
    provider: ProviderRuntime | None
    is_probe: bool = False
    # When nothing is available: why, and when the caller may usefully retry.
    reason: str = ""
    retry_after_ms: int = 0


def p2c_order(candidates: list[ProviderRuntime], rng: random.Random) -> list[ProviderRuntime]:
    """P2C winner first, then the rest best-first as fallbacks (for rate-limit denials)."""
    if len(candidates) <= 1:
        return candidates
    a, b = rng.sample(candidates, 2)
    first = a if a.score() <= b.score() else b
    rest = sorted((c for c in candidates if c is not first), key=ProviderRuntime.score)
    return [first, *rest]


class Selector:
    def __init__(
        self, registry: ProviderRegistry, coordinator: Coordinator, rng: random.Random | None = None
    ) -> None:
        self._registry = registry
        self._coordinator = coordinator
        self._rng = rng or random.Random()

    async def select(self, exclude: set[str]) -> Selection:
        now = now_ms()
        coordinator = self._coordinator
        eligible: list[ProviderRuntime] = []
        saw_open = saw_full = False
        for runtime in self._registry.all():
            if runtime.name in exclude:
                continue
            if not coordinator.is_eligible(runtime.health, now):
                saw_open = True
                continue
            if not runtime.has_room():
                saw_full = True
                continue
            eligible.append(runtime)

        min_retry_after: int | None = None
        for runtime in p2c_order(eligible, self._rng):
            is_probe = coordinator.breaker_state(runtime.health) is BreakerState.HALF_OPEN
            if is_probe and not await coordinator.try_acquire_probe(runtime.health):
                saw_open = True  # another request (maybe another replica) is probing it
                continue
            # Take the bulkhead slot *before* awaiting Redis so concurrent selections
            # cannot overfill it while we wait.
            if not runtime.acquire_slot():
                if is_probe:
                    coordinator.release_probe(runtime.health)
                saw_full = True
                continue
            allowed, retry_after = await coordinator.acquire_token(runtime.health)
            if allowed:
                return Selection(runtime, is_probe=is_probe)
            runtime.release_slot()
            if is_probe:
                coordinator.release_probe(runtime.health)
            min_retry_after = retry_after if min_retry_after is None else min(min_retry_after, retry_after)

        if min_retry_after is not None:
            return Selection(None, reason="rate_limited", retry_after_ms=min_retry_after)
        if saw_full:
            return Selection(None, reason="bulkhead_full", retry_after_ms=1000)
        if saw_open:
            return Selection(None, reason="circuit_open", retry_after_ms=1000)
        return Selection(None, reason="no_provider", retry_after_ms=1000)
