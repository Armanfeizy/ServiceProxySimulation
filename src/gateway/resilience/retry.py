"""Retry budget (Google SRE book, "Handling Overload").

Retries are capped at `ratio` of recent requests plus a small per-second floor. Without
it, an upstream brownout multiplies our own load at exactly the wrong moment — the
textbook way one degraded dependency cascades into an outage.

Kept per replica on purpose: nginx balances requests evenly, so each replica sees a
representative slice of traffic and the fleet-wide ratio is the same, without adding
Redis round-trips to every request.
"""

from __future__ import annotations

from collections import deque


class RetryBudget:
    __slots__ = ("_min_per_s", "_ratio", "_requests", "_retries", "_window_s")

    def __init__(self, ratio: float, min_per_s: float, window_s: int) -> None:
        self._ratio = ratio
        self._min_per_s = min_per_s
        self._window_s = window_s
        # deque of [second, count]; at most window_s entries each.
        self._requests: deque[list[int]] = deque()
        self._retries: deque[list[int]] = deque()

    def _bump(self, series: deque[list[int]], now_s: int) -> None:
        if series and series[-1][0] == now_s:
            series[-1][1] += 1
        else:
            series.append([now_s, 1])

    def _sum(self, series: deque[list[int]], now_s: int) -> int:
        horizon = now_s - self._window_s
        while series and series[0][0] <= horizon:
            series.popleft()
        return sum(count for _, count in series)

    def record_request(self, now_s: int) -> None:
        self._bump(self._requests, now_s)

    def try_spend(self, now_s: int) -> bool:
        allowance = self._ratio * self._sum(self._requests, now_s) + self._min_per_s * self._window_s
        if self._sum(self._retries, now_s) >= allowance:
            return False
        self._bump(self._retries, now_s)
        return True
