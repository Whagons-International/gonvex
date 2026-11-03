#!/bin/sh
# Real SDK workers on a separate host. Node 26 runs the checked-in TypeScript.
set -eu
: "${LOAD_RESULT:?set a fresh absolute output path}"
: "${LOAD_STATE:?set the copied fixture path}"
: "${WHAGONS_CHECKOUT:?set the pinned app/SDK checkout}"
bench=${LOAD_HARNESS_ROOT:-$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)}
node=${LOAD_NODE_BIN:-node}
shards=${LOAD_SHARDS:-4}
case "$shards" in ''|*[!0-9]*|0) echo 'Invalid LOAD_SHARDS' >&2;exit 2;; esac
export LOAD_SHARDS="$shards"
case "$LOAD_RESULT" in /*) ;; *) echo 'LOAD_RESULT must be absolute' >&2;exit 2;; esac
for path in "$LOAD_RESULT" "$LOAD_RESULT.ready-0" "$LOAD_RESULT.shard-0"; do
 [ ! -e "$path" ] || { echo 'Refusing to overwrite a prior run' >&2;exit 2; }
done
pids=
monitor=
pinger=
cleanup() { kill -TERM $pids ${monitor:-} ${pinger:-} 2>/dev/null || true; }
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# LOAD_SCRIPT=paths.ts runs one single-path scenario process instead of
# run.ts shards; it writes LOAD_RESULT itself.
script=${LOAD_SCRIPT:-run.ts}
case "$script" in run.ts) ;; paths.ts) shards=1 ;; *) echo 'Invalid LOAD_SCRIPT' >&2;exit 2;; esac
k=0
if [ "$script" = paths.ts ]; then
 "$node" "$bench/paths.ts" > "$LOAD_RESULT.shard-0.log" 2>&1 &
 pids="$!"
 k=$shards
fi
while [ "$k" -lt "$shards" ]; do
 LOAD_RESULT_ROOT="$LOAD_RESULT" LOAD_SHARD="$k" LOAD_RESULT="$LOAD_RESULT.shard-$k" \
  "$node" "$bench/run.ts" > "$LOAD_RESULT.shard-$k.log" 2>&1 &
 pids="$pids $!"
 k=$((k+1))
done
printf '%s\n' "$pids" > "$LOAD_RESULT.pids"
if [ "$(uname -s)" = Darwin ]; then
 python3 "$bench/mac-generator-monitor.py" "$LOAD_RESULT" > "$LOAD_RESULT.monitor.log" 2>&1 &
 monitor=$!
fi
if [ -n "${LOAD_RTT_HOST:-}" ]; then
 ping -i 1 "$LOAD_RTT_HOST" > "$LOAD_RESULT.rtt.log" 2>&1 &
 pinger=$!
fi
status=0
for pid in $pids; do wait "$pid" || status=$?;done
pids=
kill -TERM ${monitor:-} ${pinger:-} 2>/dev/null || true
[ -z "$monitor" ] || wait "$monitor" || true
trap - EXIT INT TERM
[ "$script" = paths.ts ] || "$node" "$bench/merge-shards.mjs" "$LOAD_RESULT" "$shards" > "$LOAD_RESULT.merge.log" 2>&1
printf '%s\n' "$status" > "$LOAD_RESULT.exit"
exit "$status"
