#!/bin/bash
set -euo pipefail
repo=${LOAD_GONVEX_REPO:-$(cd "$(dirname "$0")/../.." && pwd)}
lab=$(realpath -m "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
case "$lab" in /|"$repo"|"$repo"/*) echo 'LOAD_LAB_ROOT must be a directory outside the checkout' >&2; exit 2;; esac
mkdir -p "$lab" "${LOAD_BIN_DIR:?set a revision-specific LOAD_BIN_DIR}"
until ${LOAD_HEADROOM_CHECK:-true} "$lab"; do sleep 30; done
revision=$(git -C "$repo" rev-parse HEAD)
# Separate targets prevent Cargo fresh-artifact root links selecting another checkout.
export CARGO_TARGET_DIR="${LOAD_CARGO_TARGET_DIR:-$lab/target-$revision}" CARGO_BUILD_JOBS=${LOAD_BUILD_JOBS:-2}
cargo build --locked --profile local-load --manifest-path "$repo/rust/Cargo.toml" \
  --config 'profile.local-load.inherits="release"' \
  --config 'profile.local-load.debug-assertions=true' \
  --features gonvex-runtime/test-firebase-auth-emulator \
  -p gonvex-runtime -p gonvex-module-host -p gonvex-sandbox-worker
for binary in gonvex-runtime gonvex-module-host gonvex-sandbox-worker; do
  install -m 755 "$CARGO_TARGET_DIR/local-load/$binary" "$LOAD_BIN_DIR/$binary.new"
  mv "$LOAD_BIN_DIR/$binary.new" "$LOAD_BIN_DIR/$binary"
done
sha256sum "$LOAD_BIN_DIR"/gonvex-*
