# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.11.18 AS uv

FROM python:3.13-slim-bookworm AS dependencies

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

FROM python:3.13-slim-bookworm AS runtime

ARG PUID=1000
ARG PGID=1000

# PyOgg uses native Vorbis libraries; FFmpeg handles validation and conversion.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates ffmpeg libvorbisfile3 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid "${PGID}" spotisync \
    && useradd --uid "${PUID}" --gid "${PGID}" --create-home spotisync \
    && mkdir -p /data /music \
    && chown "${PUID}:${PGID}" /data /music

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    HOME=/home/spotisync \
    SPOTISYNC_STATE_DIR=/data \
    SPOTISYNC_MUSIC_DIR=/music \
    SPOTISYNC_OAUTH_BIND=0.0.0.0

WORKDIR /app
COPY --from=dependencies /app/.venv /app/.venv
COPY spotisync ./spotisync

# Docker initializes a new named volume with this directory's ownership.
VOLUME ["/data"]
USER ${PUID}:${PGID}
EXPOSE 8000 4381
CMD ["uvicorn", "spotisync.web:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
