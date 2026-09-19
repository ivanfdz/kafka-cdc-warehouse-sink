# Build the wheel in one stage, ship only the runtime in the next, so neither
# build tooling nor the source tree ends up in the published image.
FROM python:3.12-slim AS builder

ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir build==1.2.2.post1 \
    && python -m build --wheel --outdir /dist


FROM python:3.12-slim AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Non-root by default. The Kubernetes manifests expect uid 10001.
RUN groupadd --gid 10001 sink \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin sink

WORKDIR /app

# Pinned dependencies first so the layer is reused across code changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY --from=builder /dist/*.whl /tmp/
RUN pip install --no-cache-dir --no-deps /tmp/*.whl && rm -f /tmp/*.whl

# Needed by the sample producer, not by the sink itself.
COPY schemas ./schemas
COPY scripts ./scripts

USER 10001:10001

CMD ["python", "-m", "cdc_sink"]
