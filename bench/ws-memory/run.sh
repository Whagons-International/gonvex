#!/usr/bin/env bash
# Per-connection WebSocket memory micro-driver. See docs/performance/websocket-memory.md.
# usage: run.sh <ws_memory binary> <out dir> [modes] [deflate] [counts]
set -euo pipefail
BIN=${1:?binary}; OUT=${2:?out dir}
MODES=${3:-"transport runtime"}; DEFLATES=${4:-"off on"}; COUNTS=${5:-"1000 5000"}
SIZES=${SIZES:-2048,65536}; MEMORY_MAX=${MEMORY_MAX:-2G}
mkdir -p "$OUT"
ulimit -n 65536
for mode in $MODES; do for deflate in $DEFLATES; do for n in $COUNTS; do
  tag="$mode-$deflate-$n"; portfile="$OUT/$tag.port"; rm -f "$portfile" "$portfile.token"
  # The server runs in its own cgroup so memory.stat (anon, sock, slab) is per process.
  systemd-run --user --quiet --scope -p MemoryMax="$MEMORY_MAX" -p MemorySwapMax=0 --unit "ws-mem-$tag-$$" -- \
    "$BIN" server --mode "$mode" --compression on --port-file "$portfile" --data "$OUT/data-$tag" --sndbuf "${SNDBUF:-0}" --rcvbuf "${RCVBUF:-0}" \
    2>"$OUT/$tag.server.log" &
  server=$!
  for _ in $(seq 200); do [ -s "$portfile" ] && break; sleep 0.1; done
  port=$(cat "$portfile")
  "$BIN" client --port "$port" --server-pid "$server" --mode "$mode" --deflate "$deflate" --n "$n" \
    --sizes "$SIZES" --stall "${STALL:-0}" --label "$tag" --token-file "$portfile.token" 2>"$OUT/$tag.client.log" | tee "$OUT/$tag.txt"
  kill "$server"; wait "$server" 2>/dev/null || true
  rm -rf "$OUT/data-$tag"
done; done; done
