#!/bin/sh
# Local run of both builds: loader on :8798, baked on :8799. Rebuilds .build first, so stop any
# earlier run of this script first (a rebuild deletes the directories wrangler dev is serving).
#   ./scripts/dev.sh <gonvex checkout with built packages>
set -eu
cd "$(dirname "$0")/.."
node scripts/prepare.mjs --built "${1:-../../..}"
for v in loader baked; do echo "ADMIN_KEY=local-spike-key" > .build/$v/.dev.vars; done
(cd .build/loader && ../../node_modules/.bin/wrangler dev --port 8798 --inspector-port 9298 > ../loader-dev.log 2>&1) &
(cd .build/baked && ../../node_modules/.bin/wrangler dev --port 8799 --inspector-port 9299 > ../baked-dev.log 2>&1) &
wait
