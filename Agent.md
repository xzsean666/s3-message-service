# AI Agent Guide

Last updated: 2026-06-01

This file defines the operating rules for AI agents working on this repository.
The project is an object-storage-first async messaging service. Optimize every
change for AI comprehension, stable context handoff, and incremental buildability.

## Mandatory Execution Protocol

Before starting any major step, the agent must state:

1. The current step name.
2. What the step will produce.
3. That implementation code will not be written until Step 4 is explicitly approved.

Follow the step order strictly:

1. Step 1 - Architecture Design.
   Produce architecture only. Do not write implementation code.
2. Step 2 - Documentation.
   Produce or update `docs/SPEC.md`, `docs/BUILD.md`, and related docs.
3. Step 3 - Context Handoff.
   Produce or update `docs/nextsession.md`.
4. Step 4 - Implementation.
   Write code only after explicit user approval.

If the agent detects premature coding, poor modularization, or rising complexity,
it must stop and refactor the plan before continuing.

## Git Workflow

After each major step:

```bash
git add .
git commit -m "feat: <describe current step>"
```

Do not push unless the user explicitly asks.

## Project Boundary

The service provides async messaging capability for other systems.

The service owns:

- Message creation and retrieval.
- Message organization by mailbox, thread, and broadcast.
- Immutable message persistence.
- Read state persistence.
- Attachment metadata and object storage references.
- Storage provider abstraction.

The service does not own:

- User accounts.
- Login or authorization.
- Actor lifecycle.
- Tag membership management.
- Online state.
- Push delivery.
- Full-text search.
- Real-time chat transport.

External systems are responsible for actor identity, authorization decisions,
business workflows, and tag membership.

## Architecture Principles

- Split modules by cognitive load, not by line count.
- Make each module understandable in isolation.
- Keep one responsibility per module.
- Prefer composition over inheritance.
- Avoid hidden dependencies, global mutable state, and implicit configuration.
- Use explicit names; avoid abbreviations such as `cfg`, `tmp`, or `svc`.
- Keep data flow visible from entry point to storage operation.
- Treat object storage prefixes as query paths, not cosmetic folders.
- Treat messages as immutable after creation.
- Store status changes as separate objects.
- Keep provider-specific behavior behind storage adapters.

## Documentation Map

- `docs/ARCHITECTURE.md` - system architecture, module boundaries, and data flow.
- `docs/SPEC.md` - product and storage behavior specification.
- `docs/BUILD.md` - build, usage, and development instructions.
- `docs/EXTERNAL_DOCS.md` - latest official docs for external providers and standards.
- `docs/nextsession.md` - current progress and next-session handoff.

## Implementation Guardrails

- Do not introduce a database unless the user changes the project goal.
- Do not make provider-specific calls from domain modules.
- Do not scatter object key construction across modules.
- Do not overwrite immutable message objects.
- Do not require full-bucket scans for normal reads.
- Do not duplicate broadcast message bodies per recipient.
- Do not mix authentication or authorization into this service.
- Do not create "god" utility files.

## Current Implementation Directory Shape

The current Step 4 implementation is Rust and uses this shape:

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
    b2.rs
    localfs.rs
```
