#!/usr/bin/env bash
# Platform phase 2, after redeploying with the persisted lease. Needs S4_TOKEN and BASE.
#   2a (non-destructive, concurrent): lease-expiry re-run (the phase 1 stall runs
#       used the first, memory-only lease, which never expired) and large
#       downloads streamed through a 128 MB isolate, hashed, no upload.
#   2b (destructive, one group at a time so an isolate kill is attributable):
#       Whagons' buffered responseBytes, then an OOM kill of attempt 1 at 90 s
#       with two bystander Actions on the same runner.
set -u
run() { local name=$1; shift; node scripts/scenario.mjs --base "$BASE" --poll 5000 --timeout 1800 --out "results/raw/p2-$name.json" "$@" > "results/raw/p2-$name.log" 2>&1; echo "$(date +%T) $name exit=$?"; }
if [ "${PART:-a}" = a ]; then
  for r in workflow queue alarm; do run "stall-$r" --runner $r --preset stall --settle 60 & done
  for r in workflow alarm; do run "dl512hash-$r" --runner $r --preset dl512hash & done
  run dl1024hash-workflow --runner workflow --preset dl1024hash &
  wait
else
  run dl32buf-workflow --runner workflow --preset dl32buf
  for r in workflow queue alarm; do run "dl64buf-$r" --runner $r --preset dl64buf; done
  for r in workflow queue alarm; do
    run "oom-$r" --runner $r --preset crash &
    run "bystander1-$r" --runner $r --preset agent3 &
    run "bystander2-$r" --runner $r --preset agent3 &
    wait
  done
fi
