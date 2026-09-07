FROM ghcr.io/astral-sh/uv:0.9.30 AS uv
FROM rust:1.91.1-bookworm

RUN apt-get update -q \
    && apt-get install -y -q --no-install-recommends \
        ca-certificates \
        memcached \
        procps \
        python3 \
        python3-venv \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /usr/local/bin/

ENV UV_NO_CONFIG=1 \
    UV_DEFAULT_INDEX=https://pypi.org/simple \
    CARGO_INCREMENTAL=0

WORKDIR /work
