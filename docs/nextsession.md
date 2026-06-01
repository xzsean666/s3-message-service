# Next Session Handoff

Current step: Step 4 - Implementation

Last updated: 2026-06-01

## Current Progress

The repository now contains a Go implementation of Async Messaging Service.
The service is runnable as an HTTP API and persists all durable data through a
provider-neutral object storage port. The current adapter is filesystem-backed
for local development and tests.

Completed commits before this implementation:

- `fe969b4` - `feat: add architecture design docs`
- `2baa796` - `feat: add messaging service documentation`
- `5a34cbd` - `feat: add next session handoff`
- `499724e` - `feat: refine architecture design`

## Implemented Runtime And Layout

Runtime: Go.

Main layout:

- `cmd/s3-message-service` - service executable.
- `internal/application` - use cases for messages, mailboxes, threads,
  broadcasts, read state, attachments, idempotency, and operation records.
- `internal/config` - environment-based runtime configuration.
- `internal/core/cursors` - opaque prefix-window cursor encoding.
- `internal/core/ids` - UUIDv7 identifier generation.
- `internal/core/keys` - centralized object key builder and normalization.
- `internal/domain` - stored JSON object types.
- `internal/httpapi` - HTTP entrypoint.
- `internal/storage` - provider-neutral object store port.
- `internal/storage/localfs` - filesystem object store adapter for local use.

## Implemented Capabilities

- Send actor messages.
- Store immutable message bodies.
- Write direct lookup references for identifier-only reads.
- Write sender `sent` and recipient `inbox` mailbox references.
- Create and list thread references.
- List mailbox pages with prefix-window cursors and reverse-time feed keys.
- Mark messages and threads as read with append-only state events and current
  state projection.
- Create attachment metadata and lookup references.
- Send and retrieve broadcasts with audience descriptors.
- Use caller-scoped idempotency keys for retry-safe write operations.
- Record operation start, step, and completion objects.
- Run an HTTP API with health check.
- Run unit tests for key builder, cursor encoding, local storage, application
  behavior, and HTTP message flow.

## Build And Run

Run tests:

```bash
go test ./...
```

Run locally:

```bash
S3MS_STORAGE_PROVIDER=filesystem \
S3MS_FILESYSTEM_ROOT=.s3-message-data \
S3MS_HTTP_ADDR=:8080 \
go run ./cmd/s3-message-service
```

## Remaining Follow-Up Work

1. Add an S3-compatible SDK adapter behind `internal/storage.ObjectStore`.
2. Add MinIO integration tests for the future S3 adapter.
3. Add provider capability matrix for AWS S3, Cloudflare R2, Backblaze B2, and
   MinIO.
4. Add list-broadcast read use case that merges all/tag audiences with actor
   context.
5. Add lifecycle event use cases for correction, soft deletion, and moderation
   status when product requirements are known.
6. Add structured logging and request identifiers around HTTP handlers.
7. Add API contract examples or OpenAPI after the endpoint shape stabilizes.

## Risks And Unknowns

- Current storage adapter is filesystem-only. The core is provider-neutral, but
  production object storage still needs an S3-compatible adapter.
- Authorization remains intentionally out of scope. Callers must be trusted by
  deployment configuration.
- Current state projection uses overwrite semantics as an optimization. The
  append-only state event remains the source of truth.
- Operation repair records exist, but no background repair worker has been
  implemented yet.
