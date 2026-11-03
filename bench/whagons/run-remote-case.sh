#!/bin/bash
# One measured case with the generator on a remote host (the MacBook) and the
# runtime in this host's lab. The runtime keeps its 1 vCPU / 2 GB container and
# loopback-only listeners: remote-proxy.py exposes them as Unix sockets and an
# SSH reverse tunnel makes them loopback ports on the generator host.
#
#   source $LAB/env.sh
#   LOAD_LABEL=thin-200 LOAD_USERS=200 LOAD_CLIENT=thin LOAD_SDK_USERS=8 bench/whagons/run-remote-case.sh
#   LOAD_LABEL=path-hb-50 LOAD_SCRIPT=paths.ts LOAD_SCENARIO=heartbeat LOAD_RATE=50 LOAD_USERS=200 bench/whagons/run-remote-case.sh
#
# Data: if $LOAD_LAB_ROOT/baseline-data exists (see LOAD_SAVE_BASELINE=true),
# every case starts from an exact copy of it; otherwise reset.py runs.
# Forwarded to the generator: LOAD_CLIENT LOAD_SDK_USERS LOAD_ORACLE_CLIENT
# LOAD_OPEN_LOOP LOAD_NATIVE_ROWS LOAD_THIN_ACK_DELAY_MS LOAD_USERS LOAD_SECONDS
# LOAD_RAMP_SECONDS LOAD_SHARDS LOAD_ALL_TENANTS_ACTIVE LOAD_SCRIPT LOAD_SCENARIO
# LOAD_RATE LOAD_DWELL_MS LOAD_PREPARED_USERS LOAD_MAX_INFLIGHT LOAD_PATH_MOUNTS.
set -euo pipefail
bench=$(cd "$(dirname "$0")" && pwd)
lab=$(python3 -c 'import os,sys;print(os.path.realpath(sys.argv[1]))' "${LOAD_LAB_ROOT:?set LOAD_LAB_ROOT}")
label=${LOAD_LABEL:?set a unique LOAD_LABEL}
[[ "$label" =~ ^[a-zA-Z0-9_-]+$ ]] || exit 2
[ ! -e "$lab/$label-summary.json" ] || { echo 'Refusing to overwrite a measured case' >&2; exit 2; }
remote=${LOAD_REMOTE:-mac} rroot=${LOAD_REMOTE_ROOT:?set LOAD_REMOTE_ROOT (generator host directory)}
rport=${LOAD_REMOTE_RUNTIME_PORT:-18080} aport=${LOAD_REMOTE_AUTH_PORT:-18099}
prefix=${LOAD_PREFIX:-gvsq} project=${LOAD_COMPOSE_PROJECT:-gonvex-sqlite-load}
compose=(docker compose -f "$bench/compose.yaml" ${LOAD_COMPOSE_OVERRIDE:+-f "$LOAD_COMPOSE_OVERRIDE"})
export LOAD_LAB_ROOT="$lab"
out="$lab/$label"; mkdir -p "$out" "$lab/proxy"
headroom() { until ${LOAD_HEADROOM_CHECK:-true} "$lab"; do sleep 30; done; }
busy=$(ssh "$remote" 'pgrep -fl "[r]un.ts|[p]aths.ts|[r]emote-shards" || true')
[ -z "$busy" ] || { echo "Generator host busy: $busy" >&2; exit 75; }
headroom
git -C "$bench" rev-parse HEAD > "$out/commit"
sha256sum "$LOAD_BIN_DIR"/gonvex-* > "$out/binaries.sha256"
"${compose[@]}" stop -t 20 runtime > "$out/runtime-stop.log" 2>&1 || true
if [ -d "$lab/baseline-data" ]; then
 "${compose[@]}" run --rm --no-deps --entrypoint sh tools -c 'rm -rf /lab/runtime/data && cp -a /lab/baseline-data /lab/runtime/data' > "$out/reset.log" 2>&1
else
 "${compose[@]}" run --rm --no-deps tools reset.py /lab/fixture.json > "$out/reset.log" 2>&1
fi
"${compose[@]}" up -d --no-deps --wait --wait-timeout 180 runtime > "$out/runtime-start.log" 2>&1
"${compose[@]}" --profile remote up -d --no-deps remote-proxy > "$out/proxy-start.log" 2>&1
for _ in $(seq 50); do [ -S "$lab/proxy/runtime.sock" ] && [ -S "$lab/proxy/auth.sock" ] && break; sleep 0.2; done
[ "$(docker inspect "$prefix-runtime" --format '{{index .Config.Labels "com.docker.compose.project"}}')" = "$project" ] || exit 2
docker inspect "$prefix-runtime" --format '{{.HostConfig.NanoCpus}} {{.HostConfig.Memory}}' > "$out/runtime-limits"
docker inspect "$prefix-runtime" --format '{{range .Config.Env}}{{println .}}{{end}}' | grep '^GONVEX_' > "$out/runtime-env"
tunnel=gvperf-tunnel-$label
systemd-run --user --quiet --unit="$tunnel" --collect -- ssh -N -o ExitOnForwardFailure=yes -o ServerAliveInterval=10 \
 -R "127.0.0.1:$rport:$lab/proxy/runtime.sock" -R "127.0.0.1:$aport:$lab/proxy/auth.sock" "$remote"
tunnel_cg=/sys/fs/cgroup$(systemctl --user show -p ControlGroup --value "$tunnel.service")
proxy_cg=/sys/fs/cgroup$(sed -n 's/^0:://p' "/proc/$(docker inspect "$prefix-remote-proxy" --format '{{.State.Pid}}')/cgroup")
"${compose[@]}" run -d --rm --no-deps --name "$prefix-monitor" monitor "/lab/$label" "$proxy_cg" "$tunnel_cg" > /dev/null
# /dev/metrics and /dev/profile snapshots every 10 s from this side (independent of the generator).
pid=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["projectId"])' "$lab/fixture.json")
( while :; do
   m=$(curl -s --max-time 5 --unix-socket "$lab/proxy/runtime.sock" -H 'authorization: Bearer local-load-admin' "http://127.0.0.1/dev/metrics?project=$pid" || true)
   [ -z "$m" ] || printf '{"at":%s,"metrics":%s}\n' "$(date +%s%3N)" "$m" >> "$out/dev-metrics.jsonl"
   # /dev/profile dumps (perf/profiler); pick the two that bracket the plateau
   # and diff them with bench/profiler/top.py --delta.
   mkdir -p "$out/profile"
   curl -sf --max-time 5 --unix-socket "$lab/proxy/runtime.sock" -H 'authorization: Bearer local-load-admin' \
    -o "$out/profile/$(date +%s%3N).json" http://127.0.0.1/dev/profile || true
   sleep 10
  done ) & poller=$!
finish() {
 kill "$poller" 2>/dev/null || true
 [ -z "${perfjob:-}" ] || wait "$perfjob" 2>/dev/null || true
 docker stop -t 5 "$prefix-monitor" >/dev/null 2>&1 || true
 systemctl --user stop "$tunnel.service" >/dev/null 2>&1 || true
}
trap finish EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
for _ in $(seq 50); do ssh "$remote" "curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:$rport/healthz" 2>/dev/null | grep -q 200 && break; sleep 0.2; done
# Ship this commit's harness and the lab fixture to the generator host.
rh="$rroot/harness-$(git -C "$bench" rev-parse --short HEAD)$( [ -z "$(git -C "$bench" status --porcelain -- .)" ] || echo -dirty)"
tar -C "$bench" --exclude ./results -cf - . | ssh "$remote" "rm -rf $rh && mkdir -p $rh && tar -C $rh -xf -"
scp -q "$lab/fixture.json" "$remote:$rroot/fixture.json"
rresult="$rroot/results/$label.json"
fwd=""
for v in LOAD_CLIENT LOAD_SDK_USERS LOAD_ORACLE_CLIENT LOAD_OPEN_LOOP LOAD_NATIVE_ROWS LOAD_THIN_ACK_DELAY_MS LOAD_USERS LOAD_SECONDS \
 LOAD_RAMP_SECONDS LOAD_SHARDS LOAD_ALL_TENANTS_ACTIVE LOAD_SCRIPT LOAD_SCENARIO LOAD_RATE LOAD_DWELL_MS LOAD_PREPARED_USERS LOAD_MAX_INFLIGHT LOAD_PATH_MOUNTS; do
 [ -z "${!v:-}" ] || fwd="$fwd $v=$(printf %q "${!v}")"
done
# On-CPU then off-CPU flamegraphs of the runtime, 30 s each, starting
# LOAD_PERF_AT seconds after the generator starts (default: ramp + 45 s, in the
# plateau). LOAD_PERF=0 skips them.
if [ "${LOAD_PERF:-1}" != 0 ] && command -v perf >/dev/null; then
 rpid=$(docker inspect "$prefix-runtime" --format '{{.State.Pid}}')
 perf buildid-cache -a "$LOAD_BIN_DIR/gonvex-runtime" 2>/dev/null || true
 ( sleep "${LOAD_PERF_AT:-$(( ${LOAD_RAMP_SECONDS:-60} + 45 ))}"
   "$bench/../profiler/perf-oncpu.sh" "$rpid" 30 "$out/oncpu.svg" > "$out/perf-oncpu.log" 2>&1
   "$bench/../profiler/offcpu.sh" "$rpid" 30 "$out/offcpu.svg" > "$out/perf-offcpu.log" 2>&1 ) & perfjob=$!
fi
status=0
ssh "$remote" "cd $rroot && env WHAGONS_CHECKOUT=$rroot/app LOAD_STATE=$rroot/fixture.json LOAD_ADMIN_KEY=local-load-admin \
 LOAD_URL=http://127.0.0.1:$rport LOAD_AUTH_URL=http://127.0.0.1:$aport LOAD_RESULT=$rresult \
 LOAD_NODE_BIN=$rroot/tools/node-v26.13.0-darwin-arm64/bin/node LOAD_HARNESS_ROOT=$rh LOAD_RTT_HOST=${LOAD_RTT_HOST:-100.85.148.8} \
 PATH=$rroot/tools/node-v26.13.0-darwin-arm64/bin:/usr/bin:/bin:/usr/sbin:/sbin $fwd sh $rh/remote-shards.sh" > "$out/generator.log" 2>&1 || status=$?
finish
trap - EXIT INT TERM
ssh "$remote" "cd $rroot/results && tar -cf - $label.json*" | tar -C "$lab" -xf - || true
# summarize.py reads <label>.json (+ .shard-0.metrics.jsonl) beside the case directory.
printf '%s\n' "$status" > "$lab/$label.exit"
LOAD_CONTAINER_PREFIX="$prefix-" python3 "$bench/summarize.py" "$lab" "$label" > "$lab/$label-summary.json" || true
exit "$status"
