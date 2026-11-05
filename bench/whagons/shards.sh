#!/bin/sh
# Runs LOAD_SHARDS generator processes in this container and merges their raw
# samples into LOAD_RESULT. With one shard it is the plain generator.
set -eu
shards=${LOAD_SHARDS:-1}
# Empty optional Compose values retain run.ts's original defaults.
[ -n "${LOAD_OPEN_LOOP:-}" ] || unset LOAD_OPEN_LOOP
[ -n "${LOAD_RELOADS_PER_USER_HOUR:-}" ] || unset LOAD_RELOADS_PER_USER_HOUR
export LOAD_RESULT_ROOT="${LOAD_RESULT_ROOT:-$LOAD_RESULT}"
node=${LOAD_NODE_BIN:-node}
loader="$LOAD_DEPS_ROOT/tsx/dist/loader.mjs"
run() { exec "$node" --import "$loader" /harness/run.ts "$@"; }
if [ "$shards" -le 1 ] || [ "${1:-}" = prepare ]; then run "$@"; fi
pids=
k=0
while [ "$k" -lt "$shards" ]; do
  LOAD_SHARD=$k LOAD_RESULT="$LOAD_RESULT.shard-$k" "$node" --import "$loader" /harness/run.ts "$@" > "$LOAD_RESULT.shard-$k.log" 2>&1 &
  pids="$pids $!"
  k=$((k+1))
done
# The TSX CLI forwards signals through an intermediate process. Start the real
# Node workers directly so each stop handler can durably account for its work.
manifest="$LOAD_RESULT.workers.json"
"$node" --input-type=module -e 'import{writeFileSync}from"node:fs";const[out,base,...pids]=process.argv.slice(1);writeFileSync(out,JSON.stringify(pids.map((pid,k)=>({pid:Number(pid),partial:base+".shard-"+k+".partial.json"}))));' "$manifest" "$LOAD_RESULT" $pids
stop() {
 trap - TERM INT
 "$node" /harness/stop-workers.mjs "$manifest" "$LOAD_RESULT.stop.json" || true
 "$node" /harness/merge-shards.mjs "$LOAD_RESULT" "$shards" || true
 exit 143
}
trap stop TERM INT
status=0
for pid in $pids; do wait "$pid" || status=$?; done
node /harness/merge-shards.mjs "$LOAD_RESULT" "$shards"
exit "$status"
