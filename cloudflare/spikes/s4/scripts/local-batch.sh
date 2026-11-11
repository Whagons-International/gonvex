#!/usr/bin/env bash
# Local workerd batch (wrangler dev, LEASE_MS=60000). Platform limits are not
# enforced locally; this checks the protocol, not the limits.
set -u
B=${BASE:-http://127.0.0.1:18787}
run() { local name=$1; shift; node scripts/scenario.mjs --base "$B" --poll 3000 --out "results/raw/local-$name.json" "$@" > "results/raw/local-$name.log" 2>&1; echo "$name exit=$?"; }
for r in queue workflow alarm; do
  run "dl64-$r" --runner $r --preset dl64 &
  run "dl64buf-$r" --runner $r --preset dl64buf &
  run "stall-$r" --runner $r --preset stallShort --settle 20 &
  run "crash-$r" --runner $r --spec '{"steps":[{"kind":"agent","seconds":60,"turnSeconds":10,"pauseSeconds":5}],"crash":{"onAttempt":1,"afterSeconds":20,"how":"throw"}}' &
done
run "handoff-lost-workflow" --runner workflow --preset quick --skip-handoff &
run "dup-handoff-queue" --runner queue --preset sse30s --redispatch-at 3 --settle 60 &
run "dl512-workflow" --runner workflow --preset dl512 &
wait
