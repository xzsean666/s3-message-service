# Prebuilt Binary

This directory is used as Docker build context input for `Dockerfile.prebuilt`
and `Dockerfile.prebuilt.cn`.

Generate the binary with:

```bash
scripts/build-prebuilt-binary.sh
```

By default the script builds with `Dockerfile.prebuilt-builder` and
`rust:1.95-bookworm` so the generated Linux glibc binary matches the Debian
bookworm runtime images. To build directly with the host Rust toolchain instead:

```bash
S3MS_PREBUILT_BUILD_MODE=host scripts/build-prebuilt-binary.sh
```

Override the builder image with `S3MS_RUST_IMAGE` when a mirror is needed.

The generated binary is intentionally ignored by git.
