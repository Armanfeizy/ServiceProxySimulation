FROM python:3.13-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
ARG PIP_MIRROR=""
ARG PIP_MIRROR_HOST=""
RUN pip install --no-cache-dir \
        ${PIP_MIRROR:+--index-url "$PIP_MIRROR"} ${PIP_MIRROR_HOST:+--trusted-host "$PIP_MIRROR_HOST"} \
        "uvicorn[standard]>=0.32"
WORKDIR /app
COPY provider.py .
RUN useradd --system --uid 10001 mock
USER mock
EXPOSE 8000
CMD ["uvicorn", "provider:app", "--host", "0.0.0.0", "--port", "8000", \
     "--no-access-log", "--loop", "uvloop", "--http", "httptools", "--backlog", "4096"]
