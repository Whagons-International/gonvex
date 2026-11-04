#!/bin/bash
# Builds the lab's Linux runtime binaries inside rust:1.94-bookworm (the
# production image toolchain), so they run in debian:bookworm-slim on any
# Docker host, including Docker Desktop on macOS. Cargo's registry and target
# live on the named volumes gvsq-cargo-registry and gvsq-cargo-target-<checkout>
# (one per source path: cargo's mtime fingerprints mix up checkouts that share a
# target mounted at the same /src); remove
# them with `docker volume rm` when the lab is no longer needed.
set -euo pipefail
repo=${LOAD_GONVEX_REPO:-$(cd "$(dirname "$0")/../.." && pwd)}
lab=$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
case "$lab" in /|"$repo"|"$repo"/*) echo 'LOAD_LAB_ROOT must be a directory outside the checkout' >&2; exit 2;; esac
bin=${LOAD_BIN_DIR:?set LOAD_BIN_DIR}
mkdir -p "$lab" "$bin"
until ${LOAD_HEADROOM_CHECK:-true} "$lab"; do sleep 30; done
docker run --rm \
  -v "$repo:/src:ro" -v "$bin:/out" \
  -v gvsq-cargo-registry:/usr/local/cargo/registry \
  -v "gvsq-cargo-target-$(printf %s "$repo" | cksum | cut -d' ' -f1)":/target \
  -e CARGO_TARGET_DIR=/target -e CARGO_BUILD_JOBS="${LOAD_BUILD_JOBS:-6}" -e LOAD_FEATURES="${LOAD_FEATURES:-}" \
  -w /src/rust rust:1.94-bookworm bash -euc '
    cargo build --locked --profile local-load \
      --config "profile.local-load.inherits=\"release\"" \
      --config "profile.local-load.debug-assertions=true" \
      --features "gonvex-runtime/test-firebase-auth-emulator ${LOAD_FEATURES:-}" \
      -p gonvex-runtime -p gonvex-module-host -p gonvex-sandbox-worker
    for binary in gonvex-runtime gonvex-module-host gonvex-sandbox-worker; do
      install -m 755 "/target/local-load/$binary" "/out/$binary.new"
      mv "/out/$binary.new" "/out/$binary"
    done
    sha256sum /out/gonvex-*'
