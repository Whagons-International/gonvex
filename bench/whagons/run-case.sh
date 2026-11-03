#!/bin/bash
# One measured load case on the SQLite lab. Artifacts and build caches belong
# outside the checkout. The runtime owns its SQLite files while running, so task state
# is reset with the runtime stopped, then the runtime is started fresh.
set -euo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
lab=$(realpath -m "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
repo=$(cd "$(dirname "$0")/../.." && pwd)
case "$lab" in /|"$repo"|"$repo"/*) echo 'LOAD_LAB_ROOT must be a directory outside the checkout' >&2; exit 2;; esac
label=${LOAD_LABEL:?set a unique LOAD_LABEL}
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
mkdir -p "$lab/$label"
[ ! -e "$lab/$label-summary.json" ] || { echo 'Refusing to overwrite a measured case' >&2;exit 2; }
headroom() { until ${LOAD_HEADROOM_CHECK:-true} "$lab"; do sleep 30;done; }
compose=(docker compose -f "$bench/compose.yaml")
export LOAD_LAB_ROOT="$lab"
headroom
"${compose[@]}" stop -t 20 runtime > "$lab/$label/runtime-stop.log" 2>&1 || true
python3 "$bench/reset.py" "$lab/fixture.json" > "$lab/$label/reset.log"
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime > "$lab/$label/runtime-start.log" 2>&1
for name in runtime auth; do
 [ "$(docker inspect "gvsq-$name" --format '{{index .Config.Labels "com.docker.compose.project"}}')" = gonvex-sqlite-load ] || exit 2
done
[ "$(docker network inspect gonvex-sqlite-load_isolated --format '{{.Internal}}')" = true ] || exit 2
durability=$(docker inspect gvsq-runtime --format '{{range .Config.Env}}{{println .}}{{end}}' | sed -n 's/^GONVEX_SQLITE_SYNCHRONOUS=//p')
case "$durability" in full|normal) ;; *) echo 'Durability preflight failed' >&2;exit 2;; esac
printf '%s\n' "$durability" > "$lab/$label/durability"
python3 "$bench/monitor.py" "$lab/$label" & monitor=$!
finish() {
 kill "$monitor" 2>/dev/null || true
 wait "$monitor" 2>/dev/null || true
 if [ "$(docker inspect gvsq-generator --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null || true)" = gonvex-sqlite-load ]; then docker stop -t 2 gvsq-generator >/dev/null 2>&1 || true;fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
headroom
status=0
[ ! -e "$lab/$label/STOPPED_MEMORY_PRESSURE" ] || exit 125
"${compose[@]}" run --name gvsq-generator --rm --no-deps generator > "$lab/$label/generator.log" 2>&1 || status=$?
finish
trap - EXIT INT TERM
printf '%s\n' "$status" > "$lab/$label.exit"
python3 "$bench/summarize.py" "$lab" "$label" > "$lab/$label-summary.json"
exit "$status"
