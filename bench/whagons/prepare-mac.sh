#!/bin/bash
set -euo pipefail
export LOAD_CONTAINER_PREFIX=gvmac-
export PATH=/opt/homebrew/bin:/usr/local/bin:$PATH
export LOAD_LAB_ROOT=$HOME/loadtest/gvmac-state LOAD_NPM_CACHE=$HOME/loadtest/gvmac-npm
cd "$HOME/loadtest/gvmac-gonvex"
python3 bench/whagons/fill.py "$LOAD_LAB_ROOT/fixture.json"
LOAD_USERS=256 docker compose -f bench/whagons/compose.mac.yaml run --rm --no-deps --name gvmac-generator generator prepare
python3 bench/whagons/staff.py "$LOAD_LAB_ROOT/fixture.json"
docker exec gvmac-pg psql -X -U postgres -c 'CHECKPOINT; SHOW fsync; SHOW synchronous_commit; SHOW max_connections;'
docker compose -f bench/whagons/compose.mac.yaml run --rm --no-deps --name gvmac-generator generator verifyUpdates
