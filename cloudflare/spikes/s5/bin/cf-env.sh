#!/bin/sh
# Run a command with the Cloudflare development account from the credential file
# that `whagons-dev credentials exec full-cloudflare-access` provides. Prints no values.
#   whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh npx wrangler deploy
set -eu
: "${WHAGONS_CREDENTIAL_FILE:?run under whagons-dev credentials exec}"
CLOUDFLARE_ACCOUNT_ID=$(jq -r .accountId "$WHAGONS_CREDENTIAL_FILE")
CLOUDFLARE_API_TOKEN=$(jq -r .apiToken "$WHAGONS_CREDENTIAL_FILE")
export CLOUDFLARE_ACCOUNT_ID CLOUDFLARE_API_TOKEN
exec "$@"
