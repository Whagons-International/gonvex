#!/usr/bin/env bash
# Platform phase 1: long, non-destructive runs, all concurrent (LEASE_MS=300000).
# Needs S4_TOKEN and BASE. Do not redeploy the runner while this runs.
set -u
run() { local name=$1; shift; node scripts/scenario.mjs --base "$BASE" --poll 10000 --timeout 3000 --out "results/raw/p1-$name.json" "$@" > "results/raw/p1-$name.log" 2>&1; echo "$(date +%T) $name exit=$?"; }
for r in workflow queue alarm alarm-await; do run "agent30-$r" --runner $r --preset agent30 & done
for r in workflow queue alarm; do
  run "sse5-$r" --runner $r --preset sse5 &
  run "slow800-$r" --runner $r --preset slow800 &
  run "poll80-$r" --runner $r --preset poll80 &
  run "dl64-$r" --runner $r --preset dl64 --replay &
  run "dl512-$r" --runner $r --preset dl512 &
  run "stall-$r" --runner $r --preset stall --settle 30 &
done
run "handoff-lost-workflow" --runner workflow --preset quick --skip-handoff &
run "dup-handoff-queue" --runner queue --preset sse30s --redispatch-at 5 --settle 90 &
run "dup-handoff-workflow" --runner workflow --preset sse30s --redispatch-at 5 --settle 90 &
run "tenant-crash-workflow" --runner workflow --preset agent3 --crash-tenant-at 60 &
run "kill-runner-alarm" --runner alarm --preset agent3 --kill-runner-at 60 &
wait
