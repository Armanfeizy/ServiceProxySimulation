"""FastAPI application factory (one event loop per process, N replicas behind nginx)."""

from __future__ import annotations

import gc
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from gateway.api import ops, process
from gateway.api.container import Gateway
from gateway.api.middleware import RequestContextMiddleware
from gateway.observability.logging import setup_logging
from gateway.observability.tracing import setup_tracing, shutdown_tracing
from gateway.settings import Settings, get_settings


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    setup_logging(settings.log_level, settings.service_name)
    setup_tracing(
        enabled=settings.otel_enabled,
        service=settings.service_name,
        endpoint=settings.otel_endpoint,
        sample_ratio=settings.otel_sample_ratio,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        gateway = await Gateway.start(settings)
        app.state.gw = gateway
        # Everything allocated so far lives for the whole process (modules, clients,
        # config). Freezing it keeps full GC passes from rescanning ~100k objects while
        # thousands of in-flight request coroutines are alive: shorter loop stalls.
        gc.collect()
        gc.freeze()
        try:
            yield
        finally:
            await gateway.stop()
            shutdown_tracing()

    app = FastAPI(
        title="Multi-provider API gateway",
        version="1.0.0",
        lifespan=lifespan,
        default_response_class=ORJSONResponse,
    )
    app.add_middleware(RequestContextMiddleware)
    app.include_router(process.router)
    app.include_router(ops.router)
    return app
