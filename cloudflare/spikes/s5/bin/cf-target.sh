#!/bin/sh
# Point the drivers at the deployed Workers. The admin key is read from S5_KEY_FILE; it is never printed.
#   S5_KEY_FILE=<file> ./bin/cf-target.sh node test/probes.mjs load
set -eu
: "${S5_KEY_FILE:?set S5_KEY_FILE to the admin key file}"
S5_KEY=$(cat "$S5_KEY_FILE")
S5_BASE=${S5_BASE:-https://gonvex-cf-dev-s5.whagons-international.workers.dev}
S5_BAKED_BASE=${S5_BAKED_BASE:-https://gonvex-cf-dev-s5-baked.whagons-international.workers.dev}
export S5_KEY S5_BASE S5_BAKED_BASE
exec "$@"
