"""Event-loop lag: the replica's own CPU-saturation signal.

A proxy's in-flight count is dominated by slow upstream waits, so it cannot tell "many
requests parked on a slow provider" (fine) from "the loop has no CPU left" (not fine).
Loop lag can: when the loop runs late, every timer and every response read runs late.

Two uses:
  * admission sheds a growing share of new requests as lag passes the threshold,
  * timeouts that complete while lagging are not blamed on the provider (otherwise a
    saturated replica opens every breaker — a self-inflicted cascade).
"""

from __future__ import annotations

import asyncio
import contextlib
import random
import time

from gateway.observability import metrics


class LoopLagMonitor:
    def __init__(self, threshold_ms: float, interval_s: float = 0.05, alpha: float = 0.3) -> None:
        self.threshold_ms = threshold_ms
        self._interval_s = interval_s
        self._alpha = alpha
        self.lag_ms = 0.0
        self._task: asyncio.Task[None] | None = None
        self._rng = random.Random()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run(), name="loop-lag-monitor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        while True:
            started = time.monotonic()
            await asyncio.sleep(self._interval_s)
            lag = max(0.0, (time.monotonic() - started - self._interval_s) * 1000.0)
            self.lag_ms = self._alpha * lag + (1 - self._alpha) * self.lag_ms
            metrics.EVENT_LOOP_LAG.set(self.lag_ms / 1000.0)

    @property
    def lagging(self) -> bool:
        return self.lag_ms > self.threshold_ms

    def shed_probability(self) -> float:
        """0 below the threshold, rising linearly to 1 at twice the threshold."""
        if self.lag_ms <= self.threshold_ms:
            return 0.0
        return min(1.0, (self.lag_ms - self.threshold_ms) / self.threshold_ms)

    def should_shed(self) -> bool:
        p = self.shed_probability()
        return p > 0.0 and self._rng.random() < p
