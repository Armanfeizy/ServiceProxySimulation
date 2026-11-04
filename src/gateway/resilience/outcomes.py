"""Classify an upstream attempt by what the failure *proves* about the provider.

The retry question is never "did it fail?" but "could the provider have processed it?".
A request that provably never reached (or was explicitly refused by) the provider is
safe to send elsewhere. A request that may be mid-processing is only safe to replay if
the operation is idempotent or the provider deduplicates on our Idempotency-Key.
"""

from __future__ import annotations

from enum import StrEnum


class Outcome(StrEnum):
    SUCCESS = "success"
    CLIENT_ERROR = "client_error"      # provider says *our request* is invalid (4xx)
    RATE_LIMITED = "rate_limited"      # 429: explicitly refused, not processed
    CONNECT_ERROR = "connect_error"    # never arrived (refused / DNS / connect timeout)
    REJECTED = "rejected"              # 502/503/408: refused at the edge, not processed
    SERVER_ERROR = "server_error"      # 500/504/other 5xx after send: unknown
    TIMEOUT = "timeout"                # read timeout / our deadline: unknown
    PROTOCOL_ERROR = "protocol_error"  # broken response / reset mid-flight: unknown


# Failures that prove the provider did NOT process the request.
SAFE_TO_RETRY: frozenset[Outcome] = frozenset(
    {Outcome.RATE_LIMITED, Outcome.CONNECT_ERROR, Outcome.REJECTED}
)
# Failures that prove nothing: the provider may have processed the request.
OUTCOME_UNKNOWN: frozenset[Outcome] = frozenset(
    {Outcome.SERVER_ERROR, Outcome.TIMEOUT, Outcome.PROTOCOL_ERROR}
)
# What counts against provider health. 429 is excluded on purpose: it means "slow down",
# not "unhealthy" — AIMD handles it. CLIENT_ERROR is the caller's fault.
BREAKER_FAILURES: frozenset[Outcome] = frozenset(
    {Outcome.CONNECT_ERROR, Outcome.REJECTED} | OUTCOME_UNKNOWN
)

_REJECTED_STATUSES = frozenset({408, 502, 503})


def classify_status(status_code: int) -> Outcome:
    if status_code < 400:
        return Outcome.SUCCESS
    if status_code == 429:
        return Outcome.RATE_LIMITED
    if status_code in _REJECTED_STATUSES:
        return Outcome.REJECTED
    if status_code >= 500:
        return Outcome.SERVER_ERROR
    return Outcome.CLIENT_ERROR


def is_retryable(outcome: Outcome, *, replay_safe: bool) -> bool:
    """`replay_safe`: the op is idempotent, or the provider honours our Idempotency-Key."""
    if outcome in SAFE_TO_RETRY:
        return True
    if outcome in OUTCOME_UNKNOWN:
        return replay_safe
    return False
