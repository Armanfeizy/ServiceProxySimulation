"""Runtime configuration.

Everything is environment-driven (12-factor). Provider definitions live in a TOML file
(`PROVIDERS_FILE`) whose values may reference the environment with `${VAR:-default}`.
"""

from __future__ import annotations

import os
import re
import tomllib
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, Field, ValidationInfo, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_REF = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")


def expand_env(value: str) -> str:
    """Expand `${VAR}` / `${VAR:-default}` references (shell-style, no nesting)."""

    def repl(match: re.Match[str]) -> str:
        name, default = match.group("name"), match.group("default")
        env = os.environ.get(name)
        if env:
            return env
        if default is not None:
            return default
        raise ValueError(f"environment variable {name} is required by providers config")

    return _ENV_REF.sub(repl, value)


class ProviderConfig(BaseModel):
    name: str
    adapter: str = "generic_json"
    url: str
    expected_latency_ms: float = Field(gt=0)
    connect_timeout_ms: int = Field(gt=0)
    timeout_ms: int = Field(gt=0)
    rate_limit_rps: float = Field(gt=0)
    rate_limit_min_rps: float = Field(gt=0)
    rate_limit_burst: int = Field(gt=0)
    max_concurrency: int = Field(gt=0)
    supports_idempotency_key: bool = False
    headers: dict[str, str] = Field(default_factory=dict)

    @field_validator("rate_limit_min_rps")
    @classmethod
    def _min_below_max(cls, v: float, info: ValidationInfo) -> float:
        max_rps = info.data.get("rate_limit_rps")
        if max_rps is not None and v > max_rps:
            raise ValueError("rate_limit_min_rps must not exceed rate_limit_rps")
        return v


def _expand_tree(node: object) -> object:
    if isinstance(node, str):
        return expand_env(node)
    if isinstance(node, dict):
        return {k: _expand_tree(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_expand_tree(v) for v in node]
    return node


def load_providers(path: Path) -> list[ProviderConfig]:
    with path.open("rb") as fh:
        raw = tomllib.load(fh)
    blocks = _expand_tree(raw.get("providers", {}))
    assert isinstance(blocks, dict)
    providers = [ProviderConfig(name=name, **block) for name, block in blocks.items()]
    if not providers:
        raise ValueError(f"no providers defined in {path}")
    return providers


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "gateway"
    log_level: str = "INFO"
    providers_file: Path = Path("config/providers.toml")

    # --- request path -------------------------------------------------------------
    request_deadline_ms: int = 10_000          # total budget per caller request
    min_attempt_ms: int = 150                  # don't start an attempt with less left
    max_attempts: int = 3
    max_body_bytes: int = 256 * 1024
    max_inflight: int = 4_000                  # per replica; above -> 503 (load shedding)
    # CPU starvation (own load or a busy host): shed progressively from this event-loop
    # lag, everything at 2x. Set relative to the latency SLO: with 1-10s upstreams, 200ms
    # of lag costs callers little, while seconds of lag would time healthy calls out.
    max_loop_lag_ms: float = 200.0
    shed_retry_after_s: int = 1
    operation_idempotent: bool = False         # is /v1/process safe to replay blindly?

    # --- inbound admission --------------------------------------------------------
    inbound_rate_rps: float = 20_000           # per API key, global across replicas
    inbound_burst: int = 40_000
    idempotency_ttl_s: int = 24 * 3600

    # --- redis / coordination -----------------------------------------------------
    redis_url: str = "redis://redis:6379/0"
    redis_timeout_ms: int = 100                # request-path ops fail fast -> degrade-open
    redis_pool_size: int = 256
    state_refresh_ms: int = 200                # breaker/AIMD local cache refresh period

    # --- resilience ---------------------------------------------------------------
    ewma_alpha: float = 0.2
    breaker_window_s: int = 10
    breaker_failure_ratio: float = 0.5
    breaker_min_volume: int = 20
    breaker_cooldown_ms: int = 5_000
    aimd_decrease_factor: float = 0.5
    aimd_increase_ratio: float = 0.05          # +5% of configured rps per clean window
    aimd_window_ms: int = 1_000
    retry_budget_ratio: float = 0.1            # retries <= 10% of requests ...
    retry_budget_min_per_s: float = 5.0        # ... plus a small floor for quiet periods
    retry_budget_window_s: int = 10
    upstream_proxy: str | None = None          # explicit; env proxies are ignored

    # --- audit pipeline -----------------------------------------------------------
    audit_enabled: bool = True
    audit_buffer_size: int = 50_000
    audit_capture_bodies: bool = True
    audit_max_body_bytes: int = 16 * 1024
    kafka_bootstrap: str = "kafka:9092"
    audit_topic: str = "audit.requests"
    audit_dlq_topic: str = "audit.requests.dlq"
    kafka_linger_ms: int = 50
    consumer_group: str = "audit-writer"
    consumer_batch_size: int = 1_000
    postgres_dsn: str = "postgresql://gateway:gateway@postgres:5432/gateway"
    mongo_url: str = "mongodb://mongo:27017"
    mongo_db: str = "gateway"
    audit_retention_days: int = 7

    # --- observability ------------------------------------------------------------
    otel_enabled: bool = True
    otel_endpoint: str = "http://jaeger:4318"
    otel_sample_ratio: float = 0.05

    def providers(self) -> list[ProviderConfig]:
        return load_providers(self.providers_file)


@lru_cache
def get_settings() -> Settings:
    return Settings()
