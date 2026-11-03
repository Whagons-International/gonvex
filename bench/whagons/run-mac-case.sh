#!/bin/bash
set -euo pipefail
export LOAD_CONTAINER_PREFIX=gvmac-
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH
bench=$(cd "$(dirname "$0")" && pwd)
export LOAD_LAB_ROOT=${LOAD_LAB_ROOT:-$HOME/loadtest/gvmac-state}
export LOAD_NPM_CACHE=${LOAD_NPM_CACHE:-$HOME/loadtest/gvmac-npm}
label=${LOAD_LABEL:?unique case label required}
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
mkdir -p "$LOAD_LAB_ROOT/$label" "$LOAD_LAB_ROOT/runtime"
[ ! -e "$LOAD_LAB_ROOT/$label-summary.json" ] || exit 2
compose=(docker compose -f "$bench/compose.mac.yaml")
for name in pg auth; do
 [ "$(docker inspect "gvmac-$name" --format '{{index .Config.Labels "com.docker.compose.project"}}')" = gvmac-load ] || exit 2
done
[ "$(docker network inspect gvmac-load_isolated --format '{{.Internal}}')" = true ] || exit 2
"${compose[@]}" up -d --no-deps --force-recreate --wait --wait-timeout 180 runtime > "$LOAD_LAB_ROOT/$label/runtime-start.log" 2>&1
settings=$(docker exec gvmac-pg psql -X -U postgres -At -c 'SHOW fsync;SHOW synchronous_commit;SHOW max_connections')
[ "$settings" = $'on\non\n100' ] || exit 2
[ "$(docker exec gvmac-pg psql -X -U postgres -At -c "select count(*) from pg_stat_activity where application_name like 'gvmac-monitor-%'")" = 0 ] || exit 2
python3 "$bench/reset.py" "$LOAD_LAB_ROOT/fixture.json" > "$LOAD_LAB_ROOT/$label/reset.log"
python3 "$bench/monitor.mac.py" "$LOAD_LAB_ROOT/$label" & monitor=$!
finish() { kill "$monitor" 2>/dev/null || true; wait "$monitor" 2>/dev/null || true; }
trap finish EXIT
status=0
"${compose[@]}" run --name gvmac-generator --rm --no-deps generator > "$LOAD_LAB_ROOT/$label/generator.log" 2>&1 || status=$?
finish
trap - EXIT
python3 "$bench/summarize.py" "$LOAD_LAB_ROOT" "$label" > "$LOAD_LAB_ROOT/$label-summary.json"
cat "$LOAD_LAB_ROOT/$label-summary.json"

printf '%s\n' "$status" > "$LOAD_LAB_ROOT/$label.exit"
exit "$status"
