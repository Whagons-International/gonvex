#!/bin/sh
# Runs a command with the development Cloudflare account, from the credential
# file `whagons-dev credentials exec full-cloudflare-access` provides. Never
# prints the values. Usage:
#   whagons-dev credentials exec full-cloudflare-access -- sh scripts/with-cloudflare.sh <command...>
set -eu
CLOUDFLARE_ACCOUNT_ID=$(python3 -I -c 'import json,os;print(json.load(open(os.environ["WHAGONS_CREDENTIAL_FILE"]))["accountId"])')
CLOUDFLARE_API_TOKEN=$(python3 -I -c 'import json,os;print(json.load(open(os.environ["WHAGONS_CREDENTIAL_FILE"]))["apiToken"])')
export CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_API_TOKEN
export WRANGLER_SEND_METRICS=false
exec "$@"
