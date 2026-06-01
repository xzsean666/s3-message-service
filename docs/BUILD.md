# Build And Usage Guide

Current step: Step 2 - Documentation

The repository is currently documentation-only. Implementation has not been
approved yet, so there are no application build, install, or test commands.

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

## Future Runtime Decision

No runtime has been selected yet. Choose the runtime before Step 4 begins.

Recommended evaluation criteria:

- Clear object-storage SDK support.
- Strong type definitions.
- Simple test setup.
- Easy local development.
- AI-readable module boundaries.

Reasonable candidates:

- TypeScript with the AWS SDK v3 for broad S3-compatible provider support.
- Go with an S3-compatible client for a small deployable binary.

The final choice should be documented here before implementation starts.

## Future Local Development Shape

When implementation is approved, add:

- Dependency manifest for the selected runtime.
- Local configuration example.
- Unit test command.
- Storage adapter integration test command.
- Formatter and linter command.
- Minimal local object-storage setup, likely MinIO or an equivalent
  S3-compatible service.

## Future Configuration Inputs

Configuration should be centralized and explicit.

Expected settings:

- Provider name.
- Bucket name.
- Endpoint URL.
- Region.
- Access key identifier.
- Secret access key.
- Force path-style addressing flag when needed.
- Conditional write capability flag.
- Multipart upload threshold.
- Maximum list page size.
- Object key namespace prefix for environment isolation.

Secrets must not be committed.

## Future Test Strategy

Tests should be added incrementally:

- Unit tests for object key builder.
- Unit tests for identifier and normalization behavior.
- Unit tests for message, mailbox, thread, broadcast, state, and attachment
  use cases.
- Contract tests for the storage port.
- Adapter tests against local S3-compatible storage.
- Provider compatibility notes for AWS S3, Cloudflare R2, Backblaze B2, and
  self-hosted storage.

## Future Deployment Notes

Deployment is intentionally undefined until runtime selection. The service
should remain stateless except for object storage, so it can later run as:

- HTTP service.
- Background worker.
- Serverless function.
- Internal library used by another service.

The selected deployment mode must not change the storage model.
