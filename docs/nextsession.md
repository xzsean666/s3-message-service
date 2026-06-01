# Next Session Handoff

Current step: Step 3 - Context Handoff

Last updated: 2026-06-01

## Current Progress

The repository is initialized and currently contains documentation only.
No implementation code has been written.

Completed commits:

- `fe969b4` - `feat: add architecture design docs`
- `2baa796` - `feat: add messaging service documentation`

## Architecture Summary

Async Messaging Service is an independent asynchronous messaging capability
service.

Core architecture:

- Object storage is the only persistent layer.
- Messages are immutable after creation.
- Mailboxes, threads, broadcasts, states, and attachments are organized by
  object prefixes.
- Mailbox and thread reads use small reference objects pointing to immutable
  message bodies.
- Broadcast message bodies are stored once and referenced or resolved through
  audience descriptors.
- Read state is stored independently from messages.
- Provider-specific logic stays behind storage adapters.
- External systems own actors, authorization, tag membership, and business
  workflows.

Primary planned modules:

- Entry points.
- Configuration.
- Identifier generation.
- Validation.
- Message module.
- Mailbox module.
- Thread module.
- Broadcast module.
- State module.
- Attachment module.
- Storage port.
- Storage adapters.
- Object key builder.
- Serialization.

## Completed Parts

- Root `Agent.md` normalized for future AI agent behavior.
- `docs/ARCHITECTURE.md` created with architecture, module breakdown, data flow,
  and key design decisions.
- `docs/SPEC.md` created with system boundaries, storage model, domain object
  requirements, conceptual API surface, provider capability requirements, and
  non-goals.
- `docs/BUILD.md` created with current documentation-only usage and future build
  workflow.
- `docs/EXTERNAL_DOCS.md` created with official provider and standards links.

## Pending Tasks

1. Ask the user whether Step 4 implementation is approved.
2. Choose implementation runtime before writing code.
3. Update `docs/BUILD.md` with the selected runtime and commands.
4. Define provider capability matrix for AWS S3, Cloudflare R2, Backblaze B2,
   and MinIO.
5. Implement centralized configuration.
6. Implement storage port contracts.
7. Implement object key builder.
8. Implement identifier and identifier parsing utilities.
9. Implement serialization and schema versioning.
10. Implement message use cases.
11. Implement mailbox reference use cases.
12. Implement thread use cases.
13. Implement broadcast use cases.
14. Implement state use cases.
15. Implement attachment metadata and upload workflow.
16. Add unit and contract tests incrementally.
17. Add local S3-compatible integration testing.

## Next Actions

Recommended next session flow:

1. Read `Agent.md`.
2. Read `docs/ARCHITECTURE.md`.
3. Read `docs/SPEC.md`.
4. Read `docs/EXTERNAL_DOCS.md`.
5. Confirm whether the user wants Step 4 implementation.
6. If approved, choose TypeScript or Go explicitly before creating source code.
7. Start with configuration, storage port, and object key builder because later
   modules depend on them.

## Risks And Unknowns

- Runtime is not selected.
- API transport is not selected.
- Exact deployment target is not selected.
- Provider-specific conditional write behavior must be verified during adapter
  implementation.
- Newest-first mailbox pagination needs careful design because object storage
  listing is naturally prefix and lexicographic based.
- Tag broadcast semantics depend on an external actor or tag system that has not
  been specified.
- Authorization is out of scope, but callers still need a trusted integration
  boundary.
- Read-state compaction or materialized current state may be needed later for
  high-volume actors, but V1 should remain append-only.

## Implementation Guard

Do not write code until the user explicitly approves Step 4.

If implementation begins, preserve these constraints:

- No database dependency.
- No hidden provider-specific behavior in domain modules.
- No full-bucket scans for normal reads.
- No mutable message body updates.
- No user, login, authorization, tag-management, push, search, or real-time
  presence features inside this service.
