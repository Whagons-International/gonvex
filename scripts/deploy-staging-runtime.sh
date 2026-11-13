#!/usr/bin/env bash
# Deploy a prebuilt runtime image to the staging Coolify app. Nothing compiles on
# the server. Images come from .github/workflows/runtime-image.yml (or
# scripts/build-runtime-image.sh) as ghcr.io/whagons-international/gonvex-runtime:<sha>.
#
#   scripts/deploy-staging-runtime.sh <full-sha> [--digest sha256:...]
#   scripts/deploy-staging-runtime.sh --rollback      # the image before the live one
#   scripts/deploy-staging-runtime.sh --status        # what is live
#   scripts/deploy-staging-runtime.sh --dry-run <sha> # resolve and print, change nothing
#
# How it works (see docs/ops/runtime-image-deploys.md):
# the Coolify app keeps its Dockerfile build pack, volume, domains, labels, env and
# health check. Its git source is the orphan branch `runtime-image/staging`, whose
# only file is Dockerfile.runtime = `FROM ghcr.io/...:<sha>@sha256:<digest>`.
# A deploy commits that one line to the branch, points the app's git_commit_sha
# at the commit and deploys: Coolify clones one file and the "build" is a pull.
# The branch history is the deploy log; --rollback re-commits the previous image.
# The app uses consistent container names, so Coolify stops the old container
# before starting the new one (one process owns /data/.gonvex.lock).
#
# Credentials: COOLIFY_API_KEY/COOLIFY_BASE_URL, GHCR_READ_TOKEN and GH_TOKEN from
# the environment (CI); otherwise `whagons-dev credentials exec` (coolify-whagons,
# ghcr-gonvex-runtime-read) and the local gh login. Values are never printed.
set -euo pipefail

APP_UUID="${STAGING_RUNTIME_APP_UUID:-qijlnydkhsixirfc0ms6rwy8}"
IMAGE="${RUNTIME_IMAGE:-ghcr.io/whagons-international/gonvex-runtime}"
REPO_PATH="${IMAGE#ghcr.io/}"
GH_REPO="${GH_REPO:-Whagons-International/gonvex}"
DEPLOY_BRANCH="${STAGING_RUNTIME_BRANCH:-runtime-image/staging}"
DEPLOY_FILE="Dockerfile.runtime" # the app's dockerfile_location, unchanged
HEALTH_URL="${STAGING_RUNTIME_HEALTH_URL:-https://gonvex-stg.whagons.com/healthz}"
DEPLOY_TIMEOUT="${DEPLOY_TIMEOUT_SECONDS:-1200}"

usage() { sed -n '2,10p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-2}"; }

# Re-exec with the Coolify credential injected when it is not in the environment.
if [ -z "${COOLIFY_API_KEY:-}" ] || [ -z "${COOLIFY_BASE_URL:-}" ]; then
  command -v whagons-dev >/dev/null || { echo "COOLIFY_API_KEY/COOLIFY_BASE_URL unset and whagons-dev missing" >&2; exit 1; }
  exec whagons-dev credentials exec coolify-whagons --via env --prefix COOLIFY -- "$0" "$@"
fi

mode=deploy; sha=""; digest=""; dry_run=0
while [ $# -gt 0 ]; do
  case "$1" in
    --rollback) mode=rollback ;;
    --status) mode=status ;;
    --dry-run) dry_run=1 ;;
    --digest) digest="${2:?--digest needs a value}"; shift ;;
    -h|--help) usage 0 ;;
    -*) echo "unknown flag $1" >&2; usage ;;
    *) sha="$1" ;;
  esac
  shift
done

coolify() { # method path [json]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" -H "Authorization: Bearer $COOLIFY_API_KEY" -H 'content-type: application/json' \
      --data "$body" "$COOLIFY_BASE_URL$path"
  else
    curl -fsS -X "$method" -H "Authorization: Bearer $COOLIFY_API_KEY" "$COOLIFY_BASE_URL$path"
  fi
}

# Registry lookup: tag -> digest with a read-only token, in its own credential
# child so the token never shares a process with the Coolify key.
REGISTRY_LOOKUP='
  set -eu
  repo="$1"; tag="$2"; pat="${GHCR_READ_TOKEN:-${GHCR_READ_VALUE:-}}"
  [ -n "$pat" ] || { echo "no GHCR read token" >&2; exit 1; }
  token="$(curl -fsS -u "gonvex-deploy:$pat" "https://ghcr.io/token?service=ghcr.io&scope=repository:$repo:pull" | jq -r .token)"
  curl -sS -o /dev/null -D - -I -H "Authorization: Bearer $token" \
    -H "Accept: application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.oci.image.index.v1+json" \
    "https://ghcr.io/v2/$repo/manifests/$tag" | tr -d "\r" | awk "tolower(\$1)==\"docker-content-digest:\" {print \$2}"
'
resolve_digest() { # sha -> sha256:...
  if [ -n "${GHCR_READ_TOKEN:-}" ]; then
    sh -c "$REGISTRY_LOOKUP" lookup "$REPO_PATH" "$1"
  elif command -v whagons-dev >/dev/null; then
    whagons-dev credentials exec ghcr-gonvex-runtime-read --via env --prefix GHCR_READ -- \
      sh -c "$REGISTRY_LOOKUP" lookup "$REPO_PATH" "$1"
  else
    echo "no GHCR read token; pass --digest" >&2; return 1
  fi
}

branch_head() { # empty when the branch does not exist yet
  local h
  h="$(gh api "repos/$GH_REPO/git/ref/heads/$DEPLOY_BRANCH" --jq .object.sha 2>/dev/null || true)"
  [[ "$h" =~ ^[0-9a-f]{40}$ ]] && echo "$h" || true
}
file_at() { # commit -> Dockerfile content
  gh api "repos/$GH_REPO/contents/$DEPLOY_FILE?ref=$1" --jq .content | base64 -d
}
image_in() { sed -n 's/^FROM \([^ ]*\).*/\1/p' | head -1; }

app_json="$(coolify GET "/applications/$APP_UUID")"
app_branch="$(jq -r '.git_branch // ""' <<<"$app_json")"
app_commit="$(jq -r '.git_commit_sha // ""' <<<"$app_json")"
live_image=""
if [ "$app_branch" = "$DEPLOY_BRANCH" ] && [ -n "$app_commit" ]; then
  live_image="$(file_at "$app_commit" | image_in)"
fi

deployments() { coolify GET "/deployments/applications/$APP_UUID?take=20" | jq -c '(.deployments // .)[]'; }

if [ "$mode" = status ]; then
  echo "app:    $APP_UUID ($(jq -r .status <<<"$app_json"))"
  echo "source: $app_branch @ $app_commit"
  echo "image:  ${live_image:-<built from source on the server>}"
  echo "recent deployments:"
  deployments | jq -r '[.created_at, .status, .commit, .deployment_uuid] | @tsv' | head -6 || true
  exit 0
fi

busy="$(deployments | jq -r 'select(.status=="queued" or .status=="in_progress") | .deployment_uuid' | head -1)"
if [ -n "$busy" ]; then
  echo "deployment $busy is still queued or running; wait for it first" >&2
  exit 1
fi

parent="$(branch_head)"

if [ "$mode" = rollback ]; then
  [ -n "$live_image" ] || { echo "staging is not deploying from $DEPLOY_BRANCH; nothing to roll back to" >&2; exit 1; }
  # Walk the deploy branch from the live commit back to the first different image.
  target_ref=""
  for c in $(gh api "repos/$GH_REPO/commits?sha=$app_commit&per_page=30" --jq '.[].sha'); do
    img="$(file_at "$c" | image_in)"
    if [ -n "$img" ] && [ "$img" != "$live_image" ]; then target_ref="$img"; break; fi
  done
  [ -n "$target_ref" ] || { echo "no earlier image on $DEPLOY_BRANCH" >&2; exit 1; }
  sha="$(sed -n 's/.*:\([0-9a-f]\{40\}\)@.*/\1/p' <<<"$target_ref")"
  digest="${target_ref##*@}"
  echo "rolling back to ${sha:0:12}"
fi

[[ "$sha" =~ ^[0-9a-f]{40}$ ]] || { echo "need a full 40-character commit sha" >&2; usage; }
if [ -z "$digest" ]; then
  digest="$(resolve_digest "$sha")"
  [ -n "$digest" ] || { echo "no image $IMAGE:$sha in GHCR (has the Runtime image workflow finished?)" >&2; exit 1; }
fi
[[ "$digest" =~ ^sha256:[0-9a-f]{64}$ ]] || { echo "bad digest $digest" >&2; exit 1; }

ref="$IMAGE:$sha@$digest"
content="$(printf '# Staging runtime image. Written by scripts/deploy-staging-runtime.sh on the\n# gonvex repo; do not edit by hand. Built from commit %s.\nFROM %s\n' "$sha" "$ref")"
echo "live:   ${live_image:-<built from source: $app_branch @ ${app_commit:0:12}>}"
echo "target: $ref"
if [ "$dry_run" = 1 ]; then echo "dry run: nothing changed"; exit 0; fi

started=$(date +%s)

# 1. Commit the one-line Dockerfile to the deploy branch (orphan on first use).
tree="$(jq -n --arg p "$DEPLOY_FILE" --arg c "$content" '{tree:[{path:$p, mode:"100644", type:"blob", content:$c}]}' |
  gh api "repos/$GH_REPO/git/trees" --input - --jq .sha)"
message="staging runtime: $mode ${sha:0:12}"$'\n\n'"$ref"
if [ -n "$parent" ]; then
  commit="$(jq -n --arg m "$message" --arg t "$tree" --arg p "$parent" '{message:$m, tree:$t, parents:[$p]}' |
    gh api "repos/$GH_REPO/git/commits" --input - --jq .sha)"
  gh api -X PATCH "repos/$GH_REPO/git/refs/heads/$DEPLOY_BRANCH" -f sha="$commit" -F force=false >/dev/null
else
  commit="$(jq -n --arg m "$message" --arg t "$tree" '{message:$m, tree:$t, parents:[]}' |
    gh api "repos/$GH_REPO/git/commits" --input - --jq .sha)"
  gh api "repos/$GH_REPO/git/refs" -f ref="refs/heads/$DEPLOY_BRANCH" -f sha="$commit" >/dev/null
fi
echo "deploy commit $commit on $DEPLOY_BRANCH"

# 2. Point the app at it (branch is a no-op after the first switch).
coolify PATCH "/applications/$APP_UUID" "$(jq -n --arg b "$DEPLOY_BRANCH" --arg c "$commit" '{git_branch:$b, git_commit_sha:$c}')" >/dev/null
check="$(coolify GET "/applications/$APP_UUID")"
[ "$(jq -r .git_branch <<<"$check")" = "$DEPLOY_BRANCH" ] && [ "$(jq -r .git_commit_sha <<<"$check")" = "$commit" ] ||
  { echo "Coolify did not keep the new source commit" >&2; exit 1; }

# 3. Deploy and wait.
deploy_uuid="$(coolify POST "/deploy?uuid=$APP_UUID&force=false" | jq -r '.deployments[0].deployment_uuid // empty')"
[ -n "$deploy_uuid" ] || { echo "Coolify did not queue a deployment" >&2; exit 1; }
echo "deployment $deploy_uuid queued"
status=""
while :; do
  status="$(coolify GET "/deployments/$deploy_uuid" | jq -r .status)"
  case "$status" in
    finished) break ;;
    failed|cancelled*) echo "deployment $deploy_uuid $status; see Coolify logs" >&2; exit 1 ;;
  esac
  if [ $(( $(date +%s) - started )) -gt "$DEPLOY_TIMEOUT" ]; then
    echo "deployment $deploy_uuid still $status after ${DEPLOY_TIMEOUT}s" >&2; exit 1
  fi
  sleep 3
done

health=""
for _ in $(seq 1 30); do
  health="$(curl -fsS --max-time 5 "$HEALTH_URL" 2>/dev/null)" && break
  sleep 2
done
[ -n "$health" ] || { echo "deployment finished but $HEALTH_URL is not healthy" >&2; exit 1; }

# 4. Readiness stays OK while individual tenants failed provisioning (the
# runtime serves the others), so ask for operator diagnostics and refuse a
# partial outage. The admin key is read from the app's env and never printed.
admin_key="$(coolify GET "/applications/$APP_UUID/envs" |
  jq -r '[.[] | select(.key=="GONVEX_ADMIN_KEY" and (.is_preview|not)) | .value][0] // empty')"
[ -n "$admin_key" ] || { echo "GONVEX_ADMIN_KEY not found on $APP_UUID; cannot check tenant availability" >&2; exit 1; }
diagnostics="$(curl -fsS --max-time 10 -H @- "$HEALTH_URL" <<<"Authorization: Bearer $admin_key")" ||
  { echo "operator diagnostics at $HEALTH_URL failed" >&2; exit 1; }
unset admin_key
jq -e '.runtimeManifests' >/dev/null <<<"$diagnostics" ||
  { echo "operator diagnostics were not returned (admin key rejected?)" >&2; exit 1; }
if jq -e '.runtimeManifests | has("unavailableTenants") | not' >/dev/null <<<"$diagnostics"; then
  echo "note: this runtime does not report unavailable tenants"
elif jq -e '.runtimeManifests.unavailableTenants | length > 0' >/dev/null <<<"$diagnostics"; then
  echo "deployment is serving, but tenants are unavailable (roll back with --rollback):" >&2
  jq -r '.runtimeManifests.unavailableTenants | to_entries[] | "  \(.key): \(.value)"' <<<"$diagnostics" >&2
  exit 1
else
  echo "tenants: none unavailable"
fi
echo "live:    $ref"
echo "commit:  $commit ($DEPLOY_BRANCH)"
echo "healthz: $health"
echo "deploy took $(( $(date +%s) - started ))s"
