#!/usr/bin/env bash
# Platform phase 3: redeploy the runner Worker 60 s into four running Actions.
# Run under `whagons-dev credentials exec full-cloudflare-access --via env --prefix CF_`.
# Usage: redeploy-midrun.sh BASE UPSTREAM BEARER_FILE (a file holding the S4 bearer token).
set -u
cd "$(dirname "$0")/.."
BASE=$1 UPSTREAM=$2; S4_TOKEN=$(cat "$3"); export S4_TOKEN
run() { local name=$1; shift; node scripts/scenario.mjs --base "$BASE" --poll 5000 --timeout 1500 --out "results/raw/p3-$name.json" "$@" > "results/raw/p3-$name.log" 2>&1; echo "$(date +%T) $name exit=$?"; }
for r in workflow queue alarm alarm-await; do run "redeploy-$r" --runner $r --preset agent3 & done
sleep 60
echo "$(date +%T) redeploying runner"
CLOUDFLARE_API_TOKEN="$CF_APITOKEN" CLOUDFLARE_ACCOUNT_ID="$CF_ACCOUNTID" pnpm exec wrangler deploy -c wrangler.runner.jsonc --var "UPSTREAM_URL:$UPSTREAM" 2>&1 | grep -E 'Version ID|Uploaded'
echo "$(date +%T) redeployed"
wait
