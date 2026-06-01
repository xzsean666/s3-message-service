# Build And Usage Guide

Current step: Step 4 - Implementation

The repository now contains a Rust implementation of Async Messaging Service.
The implementation uses a provider-neutral object storage port and a local
filesystem adapter for development and tests. S3-compatible adapters can be
added behind the same port without changing application or domain modules.

## Current Repository Usage

Read these files in order:

1. `Agent.md`
2. `docs/ARCHITECTURE.md`
3. `docs/SPEC.md`
4. `docs/EXTERNAL_DOCS.md`
5. `docs/nextsession.md`

## Documentation Verification

Use these commands to inspect the current repository:

```bash
git status --short
find . -maxdepth 3 -type f | sort
```

## Runtime Decision

The selected runtime is Rust.

Reasons:

- Produces a small stateless service binary.
- Provides explicit domain, storage, and HTTP boundaries with strong typing.
- Uses Tokio and Axum for the HTTP service while keeping object-storage behavior
  behind a provider-neutral port.
- Can add S3-compatible SDK adapters later behind the existing storage port.

## Build, Test, And Run

Run all tests:

```bash
cargo test
```

Build the service:

```bash
cargo build
```

Run locally with filesystem object storage:

```bash
S3MS_STORAGE_PROVIDER=filesystem \
S3MS_FILESYSTEM_ROOT=.s3-message-data \
S3MS_HTTP_ADDR=:8080 \
cargo run
```

Health check:

```bash
curl http://localhost:8080/healthz
```

Run locally with Backblaze B2 S3-compatible storage:

```bash
set -a
. ./.env.test
set +a
S3MS_STORAGE_PROVIDER=b2 cargo run
```

Run the real B2 end-to-end test:

```bash
cargo test --test b2_e2e -- --ignored --nocapture
```

The B2 e2e test is ignored by default because it writes to a real bucket. It
loads `.env.test`, creates a unique object namespace, covers storage operations,
messages, mailboxes, threads, read state, attachments, broadcasts, and then
cleans up that namespace.

The B2 adapter emulates create-if-absent writes with `HEAD` before `PUT` because
the tested B2 S3-compatible endpoint does not accept the `If-None-Match: *`
conditional write header.

## Development Workflow

Follow the mandatory step order from `Agent.md`.

1. Architecture design.
2. Documentation.
3. Context handoff.
4. Implementation only after explicit approval.

After each major step:

```bash
git add .
git commit -m "feat: <describe current step>"
```

Do not push unless explicitly requested.

## Local Development Shape

Current source layout:

```text
src/
  application.rs
  config.rs
  cursors.rs
  domain.rs
  error.rs
  httpapi.rs
  ids.rs
  keys.rs
  lib.rs
  main.rs
  storage/
    mod.rs
    localfs.rs
```

The local filesystem adapter stores objects as files under
`S3MS_FILESYSTEM_ROOT`. It is intended for development and tests only.

## Configuration Inputs

Configuration should be centralized and explicit.

Expected settings:

- `S3MS_STORAGE_PROVIDER`: currently `filesystem`.
- `S3MS_FILESYSTEM_ROOT`: local object root, default `.s3-message-data`.
- `S3MS_OBJECT_NAMESPACE`: optional key namespace prefix.
- `S3MS_HTTP_ADDR`: HTTP listen address, default `:8080`.
- `S3MS_MAX_PAGE_SIZE`: maximum API page size, default `100`.
- `S3MS_READ_LOOKBACK_MINUTES`: max cursor windows for newest-first reads,
  default `43200`.
- `B2_BUCKET_NAME`: Backblaze B2 bucket name when `S3MS_STORAGE_PROVIDER=b2`.
- `B2_APPLICATION_KEY_ID`: Backblaze B2 application key identifier.
- `B2_APPLICATION_KEY`: Backblaze B2 application key secret.
- `B2_S3_ENDPOINT`: optional S3 endpoint override. When omitted, the service
  calls B2 authorization and uses the returned `s3ApiUrl`.
- `B2_S3_REGION`: optional region override. When omitted, the region is inferred
  from the B2 S3 endpoint host.
- `B2_TEST_PREFIX`: optional object prefix base for the real e2e test.

Secrets must not be committed.

Future S3-compatible adapters should also accept:

- Bucket name.
- Endpoint URL.
- Region.
- Access key identifier.
- Secret access key.
- Force path-style addressing flag when needed.
- Conditional write capability flag.
- Multipart upload threshold.

## Test Strategy

Tests should be added incrementally:

- Unit tests for object key builder.
- Unit tests for identifier and normalization behavior.
- Unit tests for message, mailbox, thread, broadcast, state, and attachment
  use cases.
- Contract tests for the storage port.
- Adapter tests against local S3-compatible storage when an S3 adapter is added.
- Provider compatibility notes for AWS S3, Cloudflare R2, Backblaze B2, and
  self-hosted storage.

## Deployment Notes

The service remains stateless except for object storage, so it can later run as:

- HTTP service.
- Background worker.
- Serverless function.
- Internal library used by another service.

The selected deployment mode must not change the storage model.
