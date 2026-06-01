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

## Planned Directory Structure

```text
Agent.md
docs/
  ARCHITECTURE.md
  SPEC.md
  BUILD.md
  EXTERNAL_DOCS.md
  nextsession.md
src/
  entrypoints/
    http/
    workers/
  config/
  core/
    identifiers/
    serialization/
    validation/
  modules/
    messages/
    mailboxes/
    threads/
    broadcasts/
    states/
    attachments/
  storage/
    ports/
    adapters/
    keys/
  shared/
    errors/
    result/
  types/
tests/
scripts/
```

This is a target structure for future implementation. The current repository
contains documentation only.

## Module Breakdown

| Module | Purpose | Input | Output | Dependencies |
| --- | --- | --- | --- | --- |
| Entry Points | Accept external requests and call application use cases. | Transport request, caller-provided actor context, payload. | Response DTO or error. | Application use cases only. |
| Configuration | Centralize bucket, provider, endpoint, and feature capability settings. | Environment or config file. | Typed runtime configuration. | No domain modules. |
| Identifier Module | Generate time-sortable identifiers for messages, threads, refs, and attachments. | Entity kind and current time. | Unique identifier string. | Standard UUIDv7 or compatible generator. |
| Validation Module | Validate command shape and domain constraints. | Application command. | Validated command or explicit validation error. | Types and constants only. |
| Message Module | Create immutable message records and resolve message reads. | Sender, recipients, message type, payload, attachments, optional thread. | Message object and message identifier. | Identifier, serialization, storage port, key builder. |
| Mailbox Module | Maintain per-actor inbox and sent references. | Actor identifier, message identifier, direction, timestamp. | Mailbox reference object. | Storage port, key builder. |
| Thread Module | Maintain thread metadata and message references. | Thread identifier or reply target, message identifier, parent relation. | Thread object and thread reference objects. | Storage port, key builder, message module contracts. |
| Broadcast Module | Store broadcast messages and audience descriptors without duplicating bodies. | Broadcast command, audience type, optional explicit targets. | Broadcast object and optional mailbox references. | Storage port, key builder, mailbox module. |
| State Module | Store read state and per-thread state independently from message bodies. | Actor identifier, message/thread identifier, read position. | State object. | Storage port, key builder. |
| Attachment Module | Store attachment metadata and object references independently from messages. | Upload metadata, object key, content type, size, checksum. | Attachment metadata object and storage object reference. | Storage port, key builder. |
| Storage Port | Define provider-neutral object operations. | Object key, object bytes, list prefix, metadata, write preconditions. | Object bytes, list page, write result. | None. |
| Storage Adapter | Implement object operations for a specific provider. | Storage port request and provider configuration. | Provider-neutral result. | Provider SDK. |
| Object Key Builder | Own all object key and prefix formats. | Entity identifiers, actor identifiers, timestamps. | Object key string or prefix string. | Identifier and time formatting helpers. |
| Serialization Module | Serialize and parse object payloads. | Typed domain object. | JSON bytes or typed object. | Types only. |

## Data Flow

### Direct Or Multi-Recipient Message

1. Entry point receives a send-message command from an external system.
2. Validation confirms sender, recipients, message type, and payload shape.
3. Attachment module records attachment metadata if attachments are present.
4. Identifier module creates a message identifier and optional thread identifier.
5. Message module writes one immutable message object under `messages/`.
6. Mailbox module writes references into sender `sent` and recipient `inbox`
   prefixes.
7. Thread module writes thread references when the message belongs to a thread.
8. Use case returns identifiers and storage references.

### Mailbox Read

1. Entry point receives actor identifier, mailbox direction, and cursor.
2. Mailbox module lists reference objects by actor-specific prefix.
3. Message module fetches message bodies referenced by the page.
4. State module may fetch read state for the actor and thread/message scope.
5. Use case returns message summaries, cursor, and read-state metadata.

### Thread Read

1. Thread module lists thread references by thread prefix.
2. Message module fetches referenced immutable message bodies.
3. State module fetches actor-specific thread state if requested.
4. Use case returns ordered thread messages and state.

### Broadcast Message

1. Broadcast module writes one immutable broadcast object.
2. For explicit audiences, mailbox references may be written for each target.
3. For all-audience and tag-audience broadcasts, the service stores an audience
   descriptor and relies on external systems to supply actor context and tag
   membership during read or fan-out workflows.
4. Read paths list broadcast prefixes relevant to the actor context and merge
   them with mailbox references.

### Read State

1. Entry point receives actor identifier and message or thread read position.
2. State module writes a new state object under an actor-specific state prefix.
3. Message objects remain unchanged.

### Attachment Handling

1. Attachment module receives attachment metadata and upload result references.
2. Large file upload should use provider-native multipart upload when supported.
3. Message objects store attachment identifiers and object references, not file
   bytes.

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

### External Actor Ownership

The service accepts actor identifiers from trusted callers but does not manage
actors, permissions, tags, sessions, or online state.

## Initial Optimization Notes From Current Provider Docs

- Use object prefixes and delimiters intentionally for listing and pagination.
- Use conditional writes where available to protect immutable objects.
- Use multipart upload for large attachments.
- Keep provider-specific compatibility checks inside adapters.
- Treat Cloudflare R2 custom-domain caching as separate from object-store
  consistency; cached reads can be stale even when the bucket is strongly
  consistent.
