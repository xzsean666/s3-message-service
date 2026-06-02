# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.95
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --release --locked && \
    cp /app/target/release/s3-message-service /usr/local/bin/s3-message-service

FROM debian:bookworm-slim AS runtime

RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/* && \
    groupadd --gid 10001 s3-message-service && \
    useradd --uid 10001 --gid s3-message-service \
        --home-dir /var/lib/s3-message-service \
        --shell /usr/sbin/nologin \
        s3-message-service && \
    mkdir -p /var/lib/s3-message-service && \
    chown -R s3-message-service:s3-message-service /var/lib/s3-message-service

COPY --from=builder /usr/local/bin/s3-message-service /usr/local/bin/s3-message-service

ENV S3MS_HTTP_ADDR=0.0.0.0:8080 \
    S3MS_STORAGE_PROVIDER=filesystem \
    S3MS_FILESYSTEM_ROOT=/var/lib/s3-message-service

WORKDIR /var/lib/s3-message-service
USER s3-message-service:s3-message-service

EXPOSE 8080
VOLUME ["/var/lib/s3-message-service"]

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS http://127.0.0.1:8080/healthz >/dev/null || exit 1

ENTRYPOINT ["s3-message-service"]
