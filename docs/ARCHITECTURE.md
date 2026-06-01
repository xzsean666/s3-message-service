# Architecture Design

Current step: Step 1 - Architecture Design

This document defines the initial architecture for Async Messaging Service.
It intentionally contains no implementation code.

## Overall Architecture

Async Messaging Service is an independent asynchronous messaging capability
service. It persists all durable data in object storage and avoids any database
dependency. The system is not an instant messaging platform; it is a low-coupling
message persistence and organization layer for other systems.

The architecture is layered:

1. Entry points receive API or worker requests and convert transport data into
   explicit application commands.
2. Application use cases validate commands and coordinate message, mailbox,
   thread, broadcast, state, and attachment modules.
3. Domain modules produce immutable message objects and append-only reference
   objects.
4. Storage ports expose provider-neutral object operations.
5. Storage adapters implement provider-specific behavior for S3-compatible
   storage systems.

Normal read paths are prefix-based. Normal write paths create immutable objects
or append new reference/state objects. Updates are represented by new objects
instead of mutating message bodies.

## Implemented Directory Structure

```text
Agent.md
Cargo.toml
docs/
  ARCHITECTURE.md
  SPEC.md
  BUILD.md
  EXTERNAL_DOCS.md
  nextsession.md
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
    b2.rs
    localfs.rs
```

The current implementation uses a Rust `src/` layout. The architecture still
follows the same module boundaries: HTTP entrypoint, application use cases, core
cursor/key/identifier helpers, domain types, and a provider-neutral storage port
with filesystem and Backblaze B2 S3-compatible adapters.

## Module Breakdown

| Module | Purpose | Input | Output | Dependencies |
| --- | --- | --- | --- | --- |
| Entry Points | Accept external requests and call application use cases. | Transport request, caller-provided actor context, payload. | Response DTO or error. | Application use cases only. |
| Application Use Cases | Execute caller-visible workflows and define write order, retry, partial failure, and repair behavior. | Validated command, actor context, idempotency key, cursor. | Operation result, response DTO, next cursor, or recoverable error. | Domain modules, operation module, cursor module, storage port. |
| Configuration | Centralize bucket, provider, endpoint, and feature capability settings. | Environment or config file. | Typed runtime configuration. | No domain modules. |
| Identifier Module | Generate time-sortable identifiers for messages, threads, refs, and attachments. | Entity kind and current time. | Unique identifier string. | Standard UUIDv7 or compatible generator. |
| Cursor Module | Encode and decode prefix-window cursors for mailbox, thread, broadcast, and state reads. | Access path, direction, time window, last object key, page size. | Opaque external cursor and internal prefix scan plan. | Object key builder and time formatting helpers. |
| Validation Module | Validate command shape and domain constraints. | Application command. | Validated command or explicit validation error. | Types and constants only. |
| Message Module | Create immutable message records and resolve message reads. | Sender, recipients, message type, payload, attachments, optional thread. | Message object and message identifier. | Identifier, serialization, storage port, key builder. |
| Mailbox Module | Maintain per-actor inbox and sent references. | Actor identifier, message identifier, direction, timestamp. | Mailbox reference object. | Storage port, key builder. |
| Thread Module | Maintain thread metadata and message references. | Thread identifier or reply target, message identifier, parent relation. | Thread object and thread reference objects. | Storage port, key builder, message module contracts. |
| Broadcast Module | Store broadcast messages and audience descriptors without duplicating bodies. | Broadcast command, audience type, optional explicit targets. | Broadcast object and optional mailbox references. | Storage port, key builder, mailbox module. |
| State Module | Store read state and per-thread state independently from message bodies. | Actor identifier, message/thread identifier, read position. | State object. | Storage port, key builder. |
| Attachment Module | Store attachment metadata and object references independently from messages. | Upload metadata, object key, content type, size, checksum. | Attachment metadata object and storage object reference. | Storage port, key builder. |
| Operation Module | Store idempotency mappings, operation steps, and final outcomes for multi-object writes. | Caller scope, idempotency key, operation identifier, write step status. | Operation status, deduplicated result, repair input. | Storage port, key builder, serialization. |
| Storage Port | Define provider-neutral object operations. | Object key, object bytes, list prefix, metadata, write preconditions. | Object bytes, list page, write result. | None. |
| Storage Adapter | Implement object operations for a specific provider. | Storage port request and provider configuration. | Provider-neutral result. | Provider SDK. |
| Object Key Builder | Own all object key and prefix formats. | Entity identifiers, actor identifiers, timestamps. | Object key string or prefix string. | Identifier and time formatting helpers. |
| Serialization Module | Serialize and parse object payloads. | Typed domain object. | JSON bytes or typed object. | Types only. |

## Data Flow

### Direct Or Multi-Recipient Message

1. Entry point receives a send-message command from an external system.
2. Validation confirms sender, recipients, message type, payload shape, and
   optional idempotency key.
3. Operation module resolves or creates an operation record for retry-safe
   execution.
4. Attachment module records attachment metadata if attachments are present.
5. Identifier module creates a message identifier and optional thread identifier.
6. Message module writes one immutable message object under `messages/`.
7. Message module writes or returns a direct lookup reference so future
   identifier-only reads do not require bucket scans.
8. Mailbox module writes references into sender `sent` and recipient `inbox`
   prefixes.
9. Thread module writes thread references when the message belongs to a thread.
10. Operation module records the final outcome or a recoverable partial failure.
11. Use case returns identifiers, storage references, and operation status.

### Mailbox Read

1. Entry point receives actor identifier, mailbox direction, and cursor.
2. Cursor module turns the cursor into one or more actor-specific prefix windows.
3. Mailbox module lists reference objects by prefix window and last object key.
4. Message module fetches message bodies referenced by the page.
5. State module may fetch read state for the actor and thread/message scope.
6. Use case returns message summaries, a next prefix-window cursor, and
   read-state metadata.

### Thread Read

1. Entry point receives thread identifier, cursor, and optional actor context.
2. Cursor module turns the cursor into thread-specific prefix windows.
3. Thread module lists thread references by prefix window and last object key.
4. Message module fetches referenced immutable message bodies.
5. State module fetches actor-specific thread state if requested.
6. Use case returns ordered thread messages, state, and next cursor.

### Broadcast Message

1. Broadcast module writes one immutable broadcast object.
2. Broadcast module writes a direct lookup reference for identifier-only reads.
3. For explicit audiences, mailbox references may be written for each target.
4. For all-audience and tag-audience broadcasts, the service stores an audience
   descriptor and relies on external systems to supply actor context and tag
   membership during read or fan-out workflows.
5. Read paths list broadcast prefixes relevant to the actor context and merge
   them with mailbox references.

### Read State

1. Entry point receives actor identifier and message or thread read position.
2. Operation module resolves idempotency when the caller supplies a retry key.
3. State module writes a new state event object under an actor-specific state
   prefix.
4. State module may update a current-state projection object when that feature
   is enabled.
5. Message objects remain unchanged.

### Attachment Handling

1. Attachment module receives attachment metadata and upload result references.
2. Large file upload should use provider-native multipart upload when supported.
3. Attachment module writes a direct lookup reference when later reads accept
   only an attachment identifier.
4. Message objects store attachment identifiers and object references, not file
   bytes.

## Operational Models

### Prefix-Window Cursor Model

Cursors are service-owned read plans, not just provider continuation tokens.
They may include provider continuation data internally, but the external cursor
must remain stable across provider adapters.

A cursor contains:

- Access path kind, such as mailbox, thread, broadcast, or state.
- Owner storage identifier, such as actor, thread, or audience scope.
- Direction, such as newest-first or oldest-first.
- Current prefix window.
- Last object key read inside that prefix window.
- Page size and cursor schema version.

Prefix windows use hierarchical time segments. The `01 -> 11 -> 22` concept is
represented as progressively narrower prefixes, for example:

```text
mailboxes/{actorStorageId}/inbox/year=2026/month=06/day=01/
mailboxes/{actorStorageId}/inbox/year=2026/month=06/day=01/hour=11/
mailboxes/{actorStorageId}/inbox/year=2026/month=06/day=01/hour=11/minute=22/
```

The same idea can be represented as a compact sortable time segment if that is
cleaner for an adapter or key builder, for example:

```text
mailboxes/{actorStorageId}/inbox/t=202606011122/
```

This lets the cursor move through small, deterministic object ranges without a
full-bucket scan. The cursor module is responsible for choosing the next prefix
window when the current one is exhausted.

Mailbox and broadcast feeds should optimize for newest-first reads. Because
S3-compatible list APIs are normally lexicographic and forward-only, feed
reference keys should use either:

- A reverse-time sortable key inside each prefix window, so normal list order
  returns newest objects first.
- Or a cursor scan plan that enumerates very small finite time windows from
  newest to oldest and buffers only that bounded window before returning a page.

The preferred design is reverse-time sortable reference keys for mailbox and
broadcast feeds, plus chronological keys for thread reads where oldest-first is
usually natural. Cursor tokens should never require a global list operation.

Reference object keys should follow this shape after implementation refinement:

```text
mailboxes/{actorStorageId}/inbox/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/{feedSortKey}_{createdAtUtc}_{messageId}_{referenceId}.json
mailboxes/{actorStorageId}/sent/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/{feedSortKey}_{createdAtUtc}_{messageId}_{referenceId}.json
threads/{threadId}/messages/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/{threadSortKey}_{createdAtUtc}_{messageId}_{referenceId}.json
broadcast/audiences/{audienceType}/year=YYYY/month=MM/day=DD/hour=HH/minute=mm/{feedSortKey}_{broadcastId}.json
```

For feed reads, `feedSortKey` should be a fixed-width reverse timestamp such as
`maxTimestampMs - createdAtEpochMs`, followed by identifiers for uniqueness.
For thread reads, `threadSortKey` should preserve chronological order. The
cursor stores both the current prefix window and the last full object key, so it
can continue inside a minute-level window or advance to the next window.

### Direct Lookup Index Model

Any API that accepts only an entity identifier must have a scan-free lookup path.
Date-bucketed object keys are good for writes and range reads, but they are not
enough for identifier-only reads.

The message module should therefore create a small immutable lookup reference
such as:

```text
messages/by-id/{messageId}.json
```

The lookup reference stores the canonical message object key, created timestamp,
schema version, and checksum when available. The same pattern applies to
attachments, broadcasts, and other entities when an API accepts only an
identifier. Access-path references may also carry canonical object keys so
normal reads avoid extra lookup calls.

### Multi-Object Write And Recovery Model

The service cannot provide strong cross-object transactions with object storage
alone. It must instead make multi-object writes idempotent, observable, and
repairable.

For caller-visible write operations:

1. The caller should provide an idempotency key scoped to the trusted caller.
2. Operation module maps the idempotency key to one operation identifier with a
   create-if-absent write when supported.
3. Use case writes immutable source objects first, then lookup references, then
   mailbox, thread, broadcast, or state references.
4. Each write step records enough information to retry safely.
5. The final outcome is written as an immutable operation result object.
6. Retried calls with the same idempotency key return the existing final outcome
   or continue repair if the operation is incomplete.

Partial write failure is an explicit architecture state. A failed operation
returns an operation identifier, written object keys when safe to expose, and a
retryable error category. Repair workers scan operation prefixes, not business
data prefixes or the whole bucket.

Operation objects should live under explicit operational prefixes, for example:

```text
operations/idempotency/{callerStorageId}/{idempotencyKey}.json
operations/by-id/{operationId}/started.json
operations/by-id/{operationId}/steps/{stepId}.json
operations/by-id/{operationId}/completed.json
```

These objects are durable coordination records, not a separate database.

### Read State Projection Model

Read state remains separate from message bodies. The append-only state event log
is the source of truth, but high-frequency reads need a bounded way to find the
current state.

The state module should support two read modes:

- Event-log mode: use prefix-window cursors to search recent state events for a
  target message or thread. This is simplest and suitable for low volume.
- Current-projection mode: write a stable current-state object after the
  append-only event. This object is an optimization, not the source of truth.

If current-projection mode is enabled, readers must still compare timestamps or
read positions when merging state records so out-of-order retries do not move a
state backwards.

### Broadcast Audience Read Contract

All-audience and tag-audience broadcasts are not copied per actor. They require
an explicit caller-supplied audience context during read or fan-out.

The actor context for broadcast reads should include:

- Actor identifier.
- Normalized tag identifiers known by the trusted caller.
- Optional tag membership version or evaluation timestamp.
- Maximum broadcast age or explicit read window when the caller wants bounded
  reads.

Broadcast descriptors should include audience type, normalized audience keys,
effective timestamp, optional expiration timestamp, and schema version. Read
paths merge explicit mailbox references, all-audience broadcasts, and supplied
tag-audience broadcasts by message or broadcast identifier to avoid duplicates.

### Trusted Boundary And Observability

The service does not own login or authorization, but it still needs a clear
trusted integration boundary. Entry points must receive a caller context that
identifies the trusted upstream system and the actor identifiers that upstream
has already authorized.

Every caller-visible operation should carry:

- Request identifier.
- Operation identifier.
- Caller system identifier.
- Actor identifier when applicable.
- Storage provider name.
- Object keys written or read.
- Error category and provider error details when safe to expose internally.

This makes object-storage failures, partial writes, retries, and repair work
traceable without introducing a database.

### Message Lifecycle Events

Message bodies remain immutable. Corrections, soft deletion, moderation flags,
workflow status, or other lifecycle changes must be represented as separate
event objects or state objects. Read use cases are responsible for merging
message bodies with lifecycle events according to explicit rules.

## Key Design Decisions

### Object Storage Is The Only Persistent Layer

All durable state is represented as objects. Prefix design is therefore part of
the data model, not an implementation detail.

### Immutable Message Bodies

Messages are write-once. Correction, deletion, read state, and workflow state
must be represented separately.

### Append-Only References

Mailbox, thread, and broadcast read paths use small reference objects pointing
to message bodies. This keeps message data deduplicated and makes each access
path explicit.

### Provider-Neutral Core

Domain and application modules depend only on storage ports. Provider-specific
SDKs, endpoints, consistency notes, and unsupported APIs stay inside adapters.

### Conditional Writes Where Supported

Adapters should support write preconditions such as "create only if absent"
when the provider supports them. This reduces accidental overwrites while keeping
the core provider-neutral.

### Idempotent Caller-Visible Writes

Caller-visible write use cases should accept idempotency keys. Idempotency is
the main protection against duplicate messages, duplicate references, and
ambiguous retry behavior after timeouts.

### Time-Sortable Identifiers

Message, reference, and attachment identifiers should be time-sortable. UUIDv7
is preferred because it is standardized by RFC 9562 and preserves chronological
ordering without a central database sequence.

### Prefixes Follow Access Paths

Keys are organized around reads that the service must support:

- Per-actor inbox.
- Per-actor sent messages.
- Per-thread message lists.
- Broadcast lists by audience scope.
- Per-actor state.
- Direct message lookup by identifier.

Prefix design also owns cursor design. Cursor pagination should advance through
known access-path prefixes and time windows rather than relying on broad
discovery.

### Direct Identifier Lookup Must Be Indexed

If an API accepts only an identifier, the architecture must provide a direct
lookup object for that identifier. Date-bucketed body storage alone is not
sufficient because it would require scanning to find the object key.

### External Actor Ownership

The service accepts actor identifiers from trusted callers but does not manage
actors, permissions, tags, sessions, or online state.

### Repair Over Strong Transactions

The system intentionally avoids a database-backed transaction coordinator.
Correctness for multi-object writes comes from immutable source objects,
idempotency records, create-if-absent writes, explicit operation outcomes, and
repair workflows.

## Initial Optimization Notes From Current Provider Docs

- Use object prefixes and delimiters intentionally for listing and pagination.
- Use prefix-window cursors for mailbox, thread, broadcast, and state reads.
- Use conditional writes where available to protect immutable objects.
- Use idempotency keys and operation records for retry-safe multi-object writes.
- Add direct lookup references for identifier-only reads.
- Use multipart upload for large attachments.
- Keep provider-specific compatibility checks inside adapters.
- Treat Cloudflare R2 custom-domain caching as separate from object-store
  consistency; cached reads can be stale even when the bucket is strongly
  consistent.
