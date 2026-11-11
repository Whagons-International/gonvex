#!/usr/bin/env bash
# Dead-peer detection on local workerd (0016 G0-6), with no root: everything
# runs in an unprivileged user + network namespace, where nft can drop the
# victim's packets in both directions (a half-open TCP connection: no FIN, no
# RST), as a client whose network dies does.
#
#   cloudflare/kernel/presence/bench/run-dead-peer.sh [tcp_retries2]
#
# Prints the driver's JSON line. An optional tcp_retries2 sets the namespace's
# TCP retransmission limit (Linux default 15); the platform's edge uses its own.
set -euo pipefail
if [[ "${1:-}" != "--inside" ]]; then exec unshare -rn "$0" --inside "${1:-}"; fi
retries="${2:-}"
here="$(cd "$(dirname "$0")" && pwd)"
cf="$(cd "$here/../../.." && pwd)"
ip link set lo up
if [[ -n "$retries" ]]; then echo "$retries" > /proc/sys/net/ipv4/tcp_retries2; fi
echo "tcp_retries2=$(cat /proc/sys/net/ipv4/tcp_retries2)" >&2
port=$((20000 + RANDOM % 20000))
key="dead-peer-$RANDOM$RANDOM"
state="$(mktemp -d "${TMPDIR:-/tmp}/dead-peer.XXXXXX")"
log="$state/wrangler.log"
(cd "$cf" && WRANGLER_SEND_METRICS=false CI=1 exec node_modules/.bin/wrangler dev --ip 127.0.0.1 --port "$port" \
  --persist-to "$state" --var "GONVEX_STUB_ADMIN_KEY:$key" --show-interactive-dev-session=false >"$log" 2>&1) &
wrangler=$!
trap 'kill $wrangler 2>/dev/null; pkill -P $wrangler 2>/dev/null; rm -rf "$state"' EXIT
for _ in $(seq 1 300); do curl -fsS "http://127.0.0.1:$port/" >/dev/null 2>&1 && break; sleep 0.2; done
echo "listeners on $port:" >&2; ss -ltnp "sport = :$port" >&2 || true
coproc DRIVER { node "$here/dead-peer.mjs" "http://127.0.0.1:$port" "$key" 2>"$state/driver.err"; }
read -r ready victim <&"${DRIVER[0]}"
[[ "$ready" == "READY" ]] || { echo "driver: $ready $victim" >&2; cat "$state/driver.err" >&2; exit 1; }
# Drop every packet of the victim's server-side connection, both directions.
if [[ -z "${DEAD_PEER_NODROP:-}" ]]; then
nft add table inet deadpeer
nft add chain inet deadpeer out '{ type filter hook output priority 0; }'
nft add rule inet deadpeer out tcp sport "$victim" drop
nft add rule inet deadpeer out tcp dport "$victim" drop
fi
echo "DROPPED" >&"${DRIVER[1]}"
if [[ -n "${DEAD_PEER_TRACE:-}" ]]; then
  ( for _ in $(seq 1 200); do echo "--- $(date +%T)"; ss -tnoi "( sport = :$victim or dport = :$victim )"; sleep 5; done ) >"$DEAD_PEER_TRACE" 2>&1 &
fi
cat <&"${DRIVER[0]}"
cat "$state/driver.err" >&2
if [[ -n "${DEAD_PEER_TRACE:-}" ]]; then cp "$log" "$DEAD_PEER_TRACE.wrangler"; fi
