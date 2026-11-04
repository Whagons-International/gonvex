#!/usr/bin/env bash
# Manual fallback for .github/workflows/runtime-image.yml: build Dockerfile.runtime
# for one commit on this machine and push it to GHCR with the same tags.
#
#   scripts/build-runtime-image.sh <commit-ish> [--tag-branch NAME] [--no-push]
#
# Needs `docker login ghcr.io` with a token that has write:packages
# (e.g. `gh auth refresh -s write:packages && gh auth token | docker login ghcr.io -u <you> --password-stdin`).
#
# The build runs in a dedicated BuildKit container ("gonvex-runtime-image") whose
# cargo cache mounts persist between runs, so later builds only recompile what
# changed. Its memory and CPU are capped because the BuildKit container is
# outside any systemd/coach cgroup; set RUNTIME_IMAGE_BUILDER_MEMORY/_CPUS.
# On arch3 run it through coach-resources and build-headroom, e.g.
#   d=/home/gabriel/work/runtime-image-build; mkdir -p "$d" && build-headroom "$d" &&
#   coach-resources run --unit runtime-image-<sha> --build-dir "$d" --kind build -- scripts/build-runtime-image.sh <sha>
set -euo pipefail

IMAGE="${RUNTIME_IMAGE:-ghcr.io/whagons-international/gonvex-runtime}"
CACHE_REF="${RUNTIME_IMAGE_CACHE_REF:-$IMAGE:buildcache}"
BUILDER="${RUNTIME_IMAGE_BUILDER:-gonvex-runtime-image}"
MEMORY="${RUNTIME_IMAGE_BUILDER_MEMORY:-12g}"
CPUS="${RUNTIME_IMAGE_BUILDER_CPUS:-8}"

ref=""; branch=""; push=1
while [ $# -gt 0 ]; do
  case "$1" in
    --tag-branch) branch="${2:?}"; shift ;;
    --no-push) push=0 ;;
    -h|--help) sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) ref="$1" ;;
  esac
  shift
done
[ -n "$ref" ] || { echo "usage: $0 <commit-ish> [--tag-branch NAME] [--no-push]" >&2; exit 2; }

repo_root="$(git rev-parse --show-toplevel)"
git -C "$repo_root" fetch -q origin "$ref" 2>/dev/null || true
sha="$(git -C "$repo_root" rev-parse --verify "$ref^{commit}" 2>/dev/null || git -C "$repo_root" rev-parse --verify "FETCH_HEAD^{commit}")"

# Build from a clean export of exactly that commit, never the working tree.
src="$(mktemp -d "${TMPDIR:-/tmp}/gonvex-runtime-src.XXXXXX")"
trap 'rm -rf "$src"' EXIT
git -C "$repo_root" archive "$sha" | tar -x -C "$src"

if ! docker buildx inspect "$BUILDER" >/dev/null 2>&1; then
  docker buildx create --name "$BUILDER" --driver docker-container \
    --driver-opt "memory=$MEMORY" --driver-opt "cpu-quota=$((CPUS * 100000))" --driver-opt "cpu-period=100000" >/dev/null
fi

tags=(--tag "$IMAGE:$sha")
if [ -n "$branch" ]; then
  slug="$(printf '%s' "$branch" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9._-' '-' | cut -c1-120)"
  tags+=(--tag "$IMAGE:$slug")
fi
output=(--push)
[ "$push" = 1 ] || output=(--load)

meta="$src/.build-metadata.json"
started=$(date +%s)
docker buildx build --builder "$BUILDER" \
  --file "$src/Dockerfile.runtime" \
  --platform linux/amd64 \
  --build-arg GONVEX_DUCKDB=false \
  --label "org.opencontainers.image.source=https://github.com/Whagons-International/gonvex" \
  --label "org.opencontainers.image.revision=$sha" \
  --label "org.opencontainers.image.title=gonvex-runtime" \
  --provenance=false --sbom=false \
  --cache-from "type=registry,ref=$CACHE_REF" \
  --metadata-file "$meta" \
  "${tags[@]}" "${output[@]}" \
  "$src"
echo "built $sha in $(( $(date +%s) - started ))s"
if [ "$push" = 1 ]; then
  echo "digest: $IMAGE@$(jq -r '."containerimage.digest"' "$meta")"
  echo "deploy: scripts/deploy-staging-runtime.sh $sha"
fi
