#!/bin/sh
# Rebuilds src/pbkdf2.wasm from the pinned crates (offline when they are cached).
set -eu
cd "$(dirname "$0")"
cargo build --release --target wasm32-wasip1 --offline 2>/dev/null || cargo build --release --target wasm32-wasip1
cp target/wasm32-wasip1/release/gonvex_control_pbkdf2.wasm ../../src/pbkdf2.wasm
echo "wrote cloudflare/control/src/pbkdf2.wasm ($(wc -c < ../../src/pbkdf2.wasm) bytes)"
