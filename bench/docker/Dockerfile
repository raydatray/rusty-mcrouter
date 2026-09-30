FROM ghcr.io/astral-sh/uv:0.9.30@sha256:538e0b39736e7feae937a65983e49d2ab75e1559d35041f9878b7b7e51de91e4 AS uv
FROM rust:1.98.1-bookworm@sha256:93ce27a88655056a51dbdd8f5f2d7ddc071c7b0070fb288a37b5a285fc83971e

RUN apt-get update -q \
    && apt-get install -y -q --no-install-recommends \
        ca-certificates \
        memcached \
        procps \
        python3 \
        python3-venv \
        shellcheck \
        util-linux \
    && rm -rf /var/lib/apt/lists/*

RUN rustup component add clippy rustfmt

COPY --from=uv /uv /uvx /usr/local/bin/

ENV UV_NO_CONFIG=1 \
    UV_DEFAULT_INDEX=https://pypi.org/simple \
    CARGO_INCREMENTAL=0

WORKDIR /work
