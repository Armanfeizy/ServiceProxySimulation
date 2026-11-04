# One image for every gateway process: api, audit-consumer, celery worker and beat.
FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app/src

# Optional PyPI mirror (e.g. an internal Nexus); empty means pypi.org.
ARG PIP_MIRROR=""
ARG PIP_MIRROR_HOST=""

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir \
        ${PIP_MIRROR:+--index-url "$PIP_MIRROR"} ${PIP_MIRROR_HOST:+--trusted-host "$PIP_MIRROR_HOST"} \
        -r requirements.txt

COPY config ./config
COPY src ./src

RUN useradd --system --uid 10001 gateway
USER gateway

EXPOSE 8000
# One event loop per container; scale with `docker compose up --scale api=N`.
CMD ["uvicorn", "gateway.api.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000", \
     "--loop", "uvloop", "--http", "httptools", \
     "--no-access-log", "--backlog", "8192", "--timeout-keep-alive", "75"]
