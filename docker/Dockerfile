FROM python:3.12-slim AS base

ARG TARGETARCH
ARG WEBCLAW_VERSION=0.6.14

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io ca-certificates curl wget jq \
    && python -m pip install --no-cache-dir playwright \
    && playwright install-deps chromium \
    && playwright install chromium \
    && chmod -R a+rX /ms-playwright \
    && rm -rf /var/lib/apt/lists/*

RUN set -eux; \
    arch="${TARGETARCH:-$(dpkg --print-architecture)}"; \
    case "$arch" in \
      amd64) asset="webclaw-v${WEBCLAW_VERSION}-x86_64-unknown-linux-musl.tar.gz"; checksum="05dad459aa45b6a9f8ddeba834a0e6c809c9778d89ddd2fc932265af7bfcfe8a" ;; \
      arm64) asset="webclaw-v${WEBCLAW_VERSION}-aarch64-unknown-linux-musl.tar.gz"; checksum="a8f5cfea9525ce08b8c58b8bbc725b608bed941a83007be3ac98519e34d60777" ;; \
      *) echo "Unsupported WebClaw architecture: $arch" >&2; exit 1 ;; \
    esac; \
    url="https://github.com/0xMassi/webclaw/releases/download/v${WEBCLAW_VERSION}/${asset}"; \
    curl -fsSL "$url" -o /tmp/webclaw.tar.gz; \
    echo "$checksum  /tmp/webclaw.tar.gz" | sha256sum -c -; \
    mkdir -p /tmp/webclaw-unpack; \
    tar -xzf /tmp/webclaw.tar.gz -C /tmp/webclaw-unpack; \
    binary="$(find /tmp/webclaw-unpack -type f -name webclaw -print -quit)"; \
    test -n "$binary"; \
    install -m 0755 "$binary" /usr/local/bin/webclaw; \
    rm -rf /tmp/webclaw.tar.gz /tmp/webclaw-unpack

COPY pyproject.toml README.md /app/
COPY src /app/src

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir .

FROM base AS worker

# The launcher invokes: python -m octopal.workers.entrypoint <spec>
# Keeping ENTRYPOINT empty to allow explicit command in DockerLauncher.
