#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

binary_name="s3-message-service"
output_path="${S3MS_PREBUILT_BINARY:-deploy/prebuilt/s3-message-service}"
cargo_target_dir="${CARGO_TARGET_DIR:-target}"
build_mode="${S3MS_PREBUILT_BUILD_MODE:-docker}"
rust_image="${S3MS_RUST_IMAGE:-rust:1.95-bookworm}"
cargo_args=(build --release --locked)

if [[ -n "${S3MS_CARGO_TARGET:-}" ]]; then
  cargo_args+=(--target "$S3MS_CARGO_TARGET")
  built_binary="$cargo_target_dir/$S3MS_CARGO_TARGET/release/$binary_name"
else
  built_binary="$cargo_target_dir/release/$binary_name"
fi

if [[ "$build_mode" == "docker" ]]; then
  if [[ -n "${S3MS_DOCKER:-}" ]]; then
    read -r -a docker_command <<< "$S3MS_DOCKER"
  elif docker version >/dev/null 2>&1; then
    docker_command=(docker)
  elif sudo -n docker version >/dev/null 2>&1; then
    docker_command=(sudo -n docker)
  else
    printf 'Docker is required for S3MS_PREBUILT_BUILD_MODE=docker.\n' >&2
    printf 'Set S3MS_PREBUILT_BUILD_MODE=host to use the host Rust toolchain instead.\n' >&2
    exit 1
  fi

  docker_output_dir="$(mktemp -d)"
  cleanup() {
    rm -rf "$docker_output_dir"
  }
  trap cleanup EXIT

  docker_build_args=(
    build
    -f Dockerfile.prebuilt-builder
    --target export
    --output "type=local,dest=$docker_output_dir"
    --build-arg "RUST_IMAGE=$rust_image"
  )
  if [[ -n "${S3MS_CARGO_TARGET:-}" ]]; then
    docker_build_args+=(--build-arg "S3MS_CARGO_TARGET=$S3MS_CARGO_TARGET")
  fi
  docker_build_args+=(.)

  "${docker_command[@]}" "${docker_build_args[@]}"
  built_binary="$docker_output_dir/s3-message-service"
elif [[ "$build_mode" == "host" ]]; then
  cargo "${cargo_args[@]}"
else
  printf 'Unsupported S3MS_PREBUILT_BUILD_MODE: %s\n' "$build_mode" >&2
  printf 'Use "docker" or "host".\n' >&2
  exit 1
fi

install -d -m 0755 "$(dirname "$output_path")"
install -m 0755 "$built_binary" "$output_path"

printf 'Built prebuilt Docker binary: %s\n' "$output_path"
