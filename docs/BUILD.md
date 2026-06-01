# Build And Usage Guide

Current step: Step 4 - Implementation

The repository now contains a Go implementation of Async Messaging Service.
The first implementation uses a provider-neutral object storage port and a local
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

The selected runtime is Go.

Reasons:

- Produces a small stateless service binary.
- Uses the standard library for HTTP, JSON, filesystem-backed local storage, and
  tests.
- Keeps module boundaries explicit and easy to audit.
- Can add S3-compatible SDK adapters later behind the existing storage port.

## Build, Test, And Run

Run all tests:

```bash
go test ./...
```

Build the service:

```bash
go build ./cmd/s3-message-service
```

Run locally with filesystem object storage:

```bash
S3MS_STORAGE_PROVIDER=filesystem \
S3MS_FILESYSTEM_ROOT=.s3-message-data \
S3MS_HTTP_ADDR=:8080 \
go run ./cmd/s3-message-service
```

Health check:

```bash
curl http://localhost:8080/healthz
```

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
cmd/s3-message-service/
internal/
  application/
  config/
  core/
    cursors/
    ids/
    keys/
  domain/
  httpapi/
  storage/
    localfs/
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
