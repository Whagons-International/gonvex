# Staging runtime: deploy prebuilt images

The staging runtime no longer compiles Rust on the shared Coolify server
(65.109.93.58, which also runs Go production). Images are built in GitHub
Actions and Coolify only pulls and restarts them.

## Pieces

| Piece | Where |
|---|---|
| Image build | `.github/workflows/runtime-image.yml`: buildx builds `Dockerfile.runtime` (`GONVEX_DUCKDB=false`) and pushes `ghcr.io/whagons-international/gonvex-runtime:<full sha>` plus a moving `:<branch-slug>` tag. It runs on pushes to `staging`, `deploy/**` and `perf/release-*`, and on manual dispatch (`ref` input = any commit). BuildKit layer cache in `:buildcache` (registry, `mode=max`). |
| Manual fallback build | `scripts/build-runtime-image.sh <sha>` (arch3 or any Docker host, needs `docker login ghcr.io` with `write:packages`). |
| Deploy / rollback | `scripts/deploy-staging-runtime.sh <sha>`, `--rollback`, `--status`. |
| Deploy log | orphan branch `runtime-image/staging`: one commit per deploy, a single `Dockerfile.runtime` = `FROM ghcr.io/...:<sha>@sha256:<digest>`. |
| Pull credential | vault `ghcr-gonvex-runtime-read` (classic PAT, `read:packages`), written to `/root/.docker/config.json` on 65.109.93.58 (`auths.ghcr.io` only). Coolify mounts that file into its build helper. |

The package stays private. Anonymous pulls are rejected.

## Deploy a commit

```bash
gh workflow run runtime-image.yml -f ref=<sha>   # if no image exists yet; ~5-9 min
scripts/deploy-staging-runtime.sh <sha>          # pull + stop-first restart + healthz
scripts/deploy-staging-runtime.sh --rollback     # previous image on runtime-image/staging
scripts/deploy-staging-runtime.sh --status
```

The deploy script commits the new `FROM` line to `runtime-image/staging` and
sets the Coolify app's `git_commit_sha` to that commit. It then triggers a
deploy, waits for Coolify, and checks `https://gonvex-stg.whagons.com/healthz`.
Images are referenced by digest, so a moved tag can't change what runs.

## What changed in Coolify (app `qijlnydkhsixirfc0ms6rwy8`)

Coolify 4.6.0 can't switch an existing git app to the Docker Image build pack
through the API. It only lets you edit an inline Dockerfile on apps created with
one. So the app keeps its Dockerfile build pack and only its git source moves:

| Field | Before | After |
|---|---|---|
| `git_branch` | `deploy/staging-preview-2026-11-04` | `runtime-image/staging` |
| `git_commit_sha` | `96c408c092279f3b0ce5b17edfcdeca97aef9ff1` | the deploy commit on `runtime-image/staging` |

Everything else stays as it was: build pack `dockerfile`, `dockerfile_location`
`/Dockerfile.runtime`, env vars (including `GONVEX_OFFLOAD_NICE=10` and
`GONVEX_SQLITE_READER_NICE=10`), the persistent volume
`qijlnydkhsixirfc0ms6rwy8-sqlite-data` at `/data`, domains
(`gonvex-stg.whagons.com`, `cvx-share.staging.whagons.com`), labels, the health
check (`/healthz`, 10 s interval, 30 s start period, 30 retries), resource
limits, `custom_docker_run_options`, and consistent container names. Consistent
names mean Coolify stops the old container before starting the new one, which
keeps a single owner of `/data/.gonvex.lock`.

Server-side change: `/root/.docker/config.json` created (it didn't exist before)
with a `ghcr.io` read credential.

## Undo

Back to compiling from source (the old behavior):

```bash
whagons-dev credentials exec coolify-whagons --via env --prefix COOLIFY -- sh -c '
  curl -fsS -X PATCH -H "Authorization: Bearer $COOLIFY_API_KEY" -H "content-type: application/json" \
    --data "{\"git_branch\":\"<source branch>\",\"git_commit_sha\":\"<source sha>\"}" \
    "$COOLIFY_BASE_URL/applications/qijlnydkhsixirfc0ms6rwy8"
  curl -fsS -H "Authorization: Bearer $COOLIFY_API_KEY" "$COOLIFY_BASE_URL/deploy?uuid=qijlnydkhsixirfc0ms6rwy8"'
```

The original values were `deploy/staging-preview-2026-11-04` @ `96c408c0...`.
If Coolify still has the image `qijlnydkhsixirfc0ms6rwy8:<source sha>` locally,
it skips the build and just restarts. Otherwise it does the old full compile.

To also remove the pull credential, delete `/root/.docker/config.json` on the
server. It held only the `ghcr.io` entry. Revoke the PAT on GitHub too.

## Measured (2026-11-04)

| | Time |
|---|---|
| Server Dockerfile deploy (before), 96c408c0 | 24 min (20:22-20:46 UTC), load ~50 |
| GH-hosted cold build, 96c408c0 (no cache) | cargo 4m24s, build step 5m00s, job ~6.5 min |
| GH-hosted, staging HEAD 0d7c8875 (cold) | build step 3m55s |
| GH-hosted, different commit 24055f50 (layer cache warm, Rust changed) | cargo 6m09s, job 8m47s |
| GH-hosted rebuild of the same commit (fully cached) | job 1m34s, identical digest |
| arch3 cold (8-CPU cap, other builds running) | 4m46s |

| arch3 warm (persisted cargo cache, 96c408c0 -> 24055f50) | 2m39s |

Deploys of prebuilt images to staging (`scripts/deploy-staging-runtime.sh`, end to end):

| Step | Script total | Inside Coolify | Container gap |
|---|---|---|---|
| Switch: 96c408c0 via image (first pull) | 70 s | 43 s | ~13 s |
| Rollback to 24055f50 (new layers to pull, server load ~34) | 136 s | 92 s | ~13 s |
| Forward with `--rollback` to 96c408c0 (image cached) | 100 s | 59 s | 13 s (21:18:17 stop -> 21:18:30 started) |

The script total adds the GitHub commit, Coolify queueing and waiting for a
public `healthz`.

`--rollback` walks `runtime-image/staging`. Right after the switch, an image that
predates the branch has to be named explicitly: `scripts/deploy-staging-runtime.sh <previous sha>`.

## Doing the same for other apps

See the PR description: Whagons web client (`jglqqx5tblw5fd0xncfsuf0i`) and bridge.
