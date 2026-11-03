#!/bin/sh
# Real SDK workers on a separate host. Node 26 runs the checked-in TypeScript.
set -eu
: "${LOAD_RESULT:?set a fresh absolute output path}"
: "${LOAD_STATE:?set the copied fixture path}"
: "${WHAGONS_CHECKOUT:?set the pinned app/SDK checkout}"
bench=${LOAD_HARNESS_ROOT:-$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)}
node=${LOAD_NODE_BIN:-node}
shards=${LOAD_SHARDS:-4}
case "$shards" in ''|*[!0-9]*|0) echo 'Invalid LOAD_SHARDS' >&2;exit 2;; esac
export LOAD_SHARDS="$shards"
case "$LOAD_RESULT" in /*) ;; *) echo 'LOAD_RESULT must be absolute' >&2;exit 2;; esac
for path in "$LOAD_RESULT" "$LOAD_RESULT.ready-0" "$LOAD_RESULT.shard-0"; do
 [ ! -e "$path" ] || { echo 'Refusing to overwrite a prior run' >&2;exit 2; }
done
pids=
monitor=
pinger=
tunnels=
cleanup() { kill -TERM $pids ${monitor:-} ${pinger:-} $tunnels 2>/dev/null || true; }
# LOAD_TUNNEL_HOST: one SSH tunnel per shard opened from this host (ssh -L to the lab's
# Unix sockets), each with its own loopback ports. A single reverse tunnel ends in one
# sshd-session here, which macOS caps at 256 open files (~240 sockets); the arch3 sshd
# side allows 1024 per session, so keep users per shard below ~900.
tunnel() {
 [ -n "${LOAD_TUNNEL_HOST:-}" ] || return 0
 tp=$(( ${LOAD_TUNNEL_PORT:-18200} + 2*$1 )); ta=$((tp+1))
 ( ulimit -n 65536 2>/dev/null || ulimit -n 10240; exec ssh -N -o BatchMode=yes -o ExitOnForwardFailure=yes -o ServerAliveInterval=10 \
   -L "127.0.0.1:$tp:${LOAD_TUNNEL_RUNTIME_SOCK:?}" -L "127.0.0.1:$ta:${LOAD_TUNNEL_AUTH_SOCK:?}" "$LOAD_TUNNEL_HOST" ) > "$LOAD_RESULT.tunnel-$1.log" 2>&1 &
 tunnels="$tunnels $!"
 n=0
 until curl -s -o /dev/null --max-time 2 "http://127.0.0.1:$tp/healthz"; do
  n=$((n+1)); [ "$n" -lt 100 ] || { echo "tunnel $1 did not come up" >&2; exit 3; }; sleep 0.2
 done
 export LOAD_URL="http://127.0.0.1:$tp" LOAD_AUTH_URL="http://127.0.0.1:$ta"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# LOAD_SCRIPT=paths.ts runs one single-path scenario process instead of
# run.ts shards; it writes LOAD_RESULT itself.
script=${LOAD_SCRIPT:-run.ts}
case "$script" in run.ts) ;; paths.ts) shards=1 ;; *) echo 'Invalid LOAD_SCRIPT' >&2;exit 2;; esac
k=0
if [ "$script" = paths.ts ]; then
 tunnel 0
 "$node" "$bench/paths.ts" > "$LOAD_RESULT.shard-0.log" 2>&1 &
 pids="$!"
 k=$shards
fi
while [ "$k" -lt "$shards" ]; do
 tunnel "$k"
 LOAD_RESULT_ROOT="$LOAD_RESULT" LOAD_SHARD="$k" LOAD_RESULT="$LOAD_RESULT.shard-$k" \
  "$node" "$bench/run.ts" > "$LOAD_RESULT.shard-$k.log" 2>&1 &
 pids="$pids $!"
 k=$((k+1))
done
printf '%s\n' "$pids" > "$LOAD_RESULT.pids"
if [ "$(uname -s)" = Darwin ]; then
 python3 "$bench/mac-generator-monitor.py" "$LOAD_RESULT" > "$LOAD_RESULT.monitor.log" 2>&1 &
 monitor=$!
fi
if [ -n "${LOAD_RTT_HOST:-}" ]; then
 ping -i 1 "$LOAD_RTT_HOST" > "$LOAD_RESULT.rtt.log" 2>&1 &
 pinger=$!
fi
status=0
for pid in $pids; do wait "$pid" || status=$?;done
pids=
kill -TERM ${monitor:-} ${pinger:-} $tunnels 2>/dev/null || true
[ -z "$monitor" ] || wait "$monitor" || true
trap - EXIT INT TERM
[ "$script" = paths.ts ] || "$node" "$bench/merge-shards.mjs" "$LOAD_RESULT" "$shards" > "$LOAD_RESULT.merge.log" 2>&1
printf '%s\n' "$status" > "$LOAD_RESULT.exit"
exit "$status"
