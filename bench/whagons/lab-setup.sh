#!/bin/bash
# Builds a fresh SQLite load lab: stack up, app setup, fill to target sizes,
# synthetic users, staffing and a functional verify. Destroys only this lab's
# runtime data. Usage: LOAD_LAB_ROOT=... WHAGONS_CHECKOUT=... lab-setup.sh [users]
set -euo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
lab=$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
repo=$(cd "$(dirname "$0")/../.." && pwd)
case "$lab" in /|"$repo"|"$repo"/*) echo 'LOAD_LAB_ROOT must be a directory outside the checkout' >&2; exit 2;; esac
users=${1:-100}
compose=(docker compose -f "$bench/compose.yaml")
headroom() { until ${LOAD_HEADROOM_CHECK:-true} "$lab"; do sleep 30; done; }
"${compose[@]}" --profile load down --remove-orphans >/dev/null 2>&1 || true
docker volume rm gvsq-state >/dev/null 2>&1 || true
rm -rf "$lab/auth-export" "$lab/auth-golden" "$lab/fixture.json" "$lab/fixture.json.metrics.jsonl"
headroom
"${compose[@]}" up -d --wait --wait-timeout 240
"${compose[@]}" run --name gvsq-generator --rm --no-deps generator setup > "$lab/setup.log" 2>&1
"${compose[@]}" stop -t 20 runtime >/dev/null
"${compose[@]}" run --rm --no-deps tools fill.py /lab/fixture.json > "$lab/fill.log"
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime >/dev/null
"${compose[@]}" run --name gvsq-generator --rm --no-deps -e LOAD_USERS="$users" generator prepare > "$lab/prepare.log" 2>&1
"${compose[@]}" stop -t 20 runtime >/dev/null
"${compose[@]}" run --rm --no-deps tools staff.py /lab/fixture.json > "$lab/staff.log"
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime >/dev/null
"${compose[@]}" run --name gvsq-generator --rm --no-deps generator verify > "$lab/verify.log" 2>&1
grep -c '"ok":true' "$lab/verify.log"
# Freeze the emulator's accounts while it runs (export-on-exit never fires:
# SIGTERM stops npx before the emulator exports). Every later start imports
# this copy, so a teardown cannot leave auth out of step with tenant members.
"${compose[@]}" exec -T auth npx --offline --yes firebase-tools@15.11.0 emulators:export /state/auth-golden \
  --project demo-whagons-e2e --force > "$lab/auth-export.log" 2>&1
test -d "$lab/auth-golden"
