#!/bin/bash
# Builds a fresh SQLite load lab: stack up, app setup, fill to target sizes,
# synthetic users, staffing and a functional verify. Destroys only this lab's
# runtime data. Usage: LOAD_LAB_ROOT=... WHAGONS_CHECKOUT=... lab-setup.sh [users]
set -euo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
lab=$(realpath -m "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
case "$lab" in /mnt/data/*) ;; *) echo 'LOAD_LAB_ROOT must be on /mnt/data' >&2; exit 2;; esac
users=${1:-100}
compose=(docker compose -f "$bench/compose.yaml")
headroom() { until /home/gabriel/.local/bin/build-headroom "$lab"; do sleep 30; done; }
"${compose[@]}" --profile load down --remove-orphans >/dev/null 2>&1 || true
rm -rf "$lab/runtime" "$lab/auth-export" "$lab/fixture.json" "$lab/fixture.json.metrics.jsonl"
mkdir -p "$lab/runtime"
headroom
"${compose[@]}" up -d --wait --wait-timeout 240
"${compose[@]}" run --name gvsq-generator --rm --no-deps generator setup > "$lab/setup.log" 2>&1
"${compose[@]}" stop -t 20 runtime >/dev/null
python3 "$bench/fill.py" "$lab/fixture.json" > "$lab/fill.log"
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime >/dev/null
"${compose[@]}" run --name gvsq-generator --rm --no-deps -e LOAD_USERS="$users" generator prepare > "$lab/prepare.log" 2>&1
"${compose[@]}" stop -t 20 runtime >/dev/null
python3 "$bench/staff.py" "$lab/fixture.json" > "$lab/staff.log"
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime >/dev/null
"${compose[@]}" run --name gvsq-generator --rm --no-deps generator verify > "$lab/verify.log" 2>&1
grep -c '"ok":true' "$lab/verify.log"
