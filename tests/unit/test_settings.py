from __future__ import annotations

from pathlib import Path

import pytest

from gateway.settings import expand_env, load_providers


def test_expand_env_default_and_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GW_TEST_URL", raising=False)
    assert expand_env("${GW_TEST_URL:-http://mock}") == "http://mock"
    monkeypatch.setenv("GW_TEST_URL", "http://real")
    assert expand_env("${GW_TEST_URL:-http://mock}/v1") == "http://real/v1"


def test_expand_env_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("GW_TEST_REQUIRED", raising=False)
    with pytest.raises(ValueError):
        expand_env("${GW_TEST_REQUIRED}")


def test_shipped_providers_file_loads(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROVIDER_A_URL", "http://92.114.51.251:8001/v1/process")
    providers = {p.name: p for p in load_providers(Path("config/providers.toml"))}
    assert set(providers) == {"A", "B", "C"}
    assert providers["A"].url == "http://92.114.51.251:8001/v1/process"
    assert providers["B"].url == "http://mock-b:8000/v1/process"
    assert providers["A"].rate_limit_rps == 200.0
