#!/bin/bash
# Invoke through coach-resources. Artifacts and build caches belong on /mnt/data.
set -euo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
lab=$(realpath -m "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
case "$lab" in /mnt/data/*) ;; *) echo 'LOAD_LAB_ROOT must be on /mnt/data' >&2;exit 2;; esac
label=${LOAD_LABEL:?set a unique LOAD_LABEL}
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
mkdir -p "$lab/$label"
[ ! -e "$lab/$label-summary.json" ] || { echo 'Refusing to overwrite a measured case' >&2;exit 2; }
headroom() { /home/gabriel/.local/bin/build-headroom "$lab"; }
compose=(docker compose -f "$bench/compose.yaml")
headroom
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime > "$lab/$label/runtime-start.log" 2>&1
for name in pg runtime auth; do
 [ "$(docker inspect "gvload-$name" --format '{{index .Config.Labels "com.docker.compose.project"}}')" = gonvex-local-load ] || exit 2
done
[ "$(docker network inspect gonvex-local-load_isolated --format '{{.Internal}}')" = true ] || exit 2
settings=$(docker exec gvload-pg psql -X -U postgres -At -c 'SHOW fsync;SHOW synchronous_commit;SHOW max_connections')
[ "$settings" = $'on\non\n100' ] || { echo 'Durability/connection preflight failed' >&2;exit 2; }
samplers=$(docker exec gvload-pg psql -X -U postgres -At -c "select count(*) from pg_stat_activity where application_name like 'gvload-monitor-%'")
[ "$samplers" = 0 ] || { echo 'A sampler is already running' >&2;exit 2; }
headroom
python3 "$bench/reset.py" "$lab/fixture.json" > "$lab/$label/reset.log"
python3 "$bench/monitor.py" "$lab/$label" & monitor=$!
finish() {
 kill "$monitor" 2>/dev/null || true
 wait "$monitor" 2>/dev/null || true
 if [ "$(docker inspect gvload-generator --format '{{index .Config.Labels "com.docker.compose.project"}}' 2>/dev/null || true)" = gonvex-local-load ]; then docker stop -t 2 gvload-generator >/dev/null 2>&1 || true;fi
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
headroom
status=0
if [ -e "$lab/$label/STOPPED_MEMORY_PRESSURE" ]; then
 status=125
else
 "${compose[@]}" run --name gvload-generator --rm --no-deps generator > "$lab/$label/generator.log" 2>&1 || status=$?
fi
finish
trap - EXIT INT TERM
printf '%s\n' "$status" > "$lab/$label.exit"
python3 "$bench/summarize.py" "$lab" "$label" > "$lab/$label-summary.json"
exit "$status"
