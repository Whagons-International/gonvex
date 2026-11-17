#!/bin/sh
# Runs LOAD_SHARDS generator processes in this container and merges their raw
# samples into LOAD_RESULT. With one shard it is the plain generator.
set -eu
shards=${LOAD_SHARDS:-1}
run() { exec "$LOAD_DEPS_ROOT/.bin/tsx" /harness/run.ts "$@"; }
if [ "$shards" -le 1 ] || [ "${1:-}" = prepare ]; then run "$@"; fi
pids=
k=0
while [ "$k" -lt "$shards" ]; do
  LOAD_SHARD=$k LOAD_RESULT="$LOAD_RESULT.shard-$k" "$LOAD_DEPS_ROOT/.bin/tsx" /harness/run.ts "$@" > "$LOAD_RESULT.shard-$k.log" 2>&1 &
  pids="$pids $!"
  k=$((k+1))
done
trap 'kill -TERM $pids 2>/dev/null || true' TERM INT
status=0
for pid in $pids; do wait "$pid" || status=$?; done
node /harness/merge-shards.mjs "$LOAD_RESULT" "$shards"
exit "$status"
