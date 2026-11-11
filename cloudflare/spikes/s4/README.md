# S4 spike: long Actions on Cloudflare

Throwaway code for package S4. The report, numbers and verdict are in
[`docs/cloudflare/spikes/S4-long-actions.md`](../../../docs/cloudflare/spikes/S4-long-actions.md).

## What is here

| Path | What |
| --- | --- |
| `src/tenant.ts` | `TenantObject`: the outbox row, claim / renew / fail / complete, and the sweep alarm that recovers lost leases and lost hand-offs. |
| `src/action.ts` | The Action body (agent loop, SSE, download, slow request, poll) and `runAction`: claim, renew every 30 s, run, return the outcome. |
| `src/runner.ts` | Worker entry: runner A (Queue consumer), runner B (`ActionWorkflow`), the HTTP API the driver uses. |
| `src/runner-object.ts` | Runner C (`RunnerObject`): one Durable Object per Action, driven by its alarm (`heartbeat` or `await` mode). |
| `src/upstream.ts` | Test upstream Worker: `/sse`, `/slow`, `/download`, `/sink`, `/poll`. No paid API is called. |
| `scripts/scenario.mjs` | Driver: enqueues one Action, polls the tenant's event log, writes a summary JSON. |
| `scripts/*.sh` | The batches used for the report (local, platform phases 1-3). |
| `scripts/events.py` | Prints a result file's timeline. |
| `results/` | Summaries kept for the report (`raw/` is gitignored). |

## Run locally

```sh
cd cloudflare/spikes/s4
pnpm install                     # standalone pnpm root (own pnpm-workspace.yaml)
pnpm typecheck
pnpm exec wrangler dev -c wrangler.upstream.jsonc --port 18788 &
pnpm exec wrangler dev -c wrangler.runner.jsonc --port 18787 \
  --var LOCAL:1 --var LEASE_MS:60000 --var UPSTREAM_URL:http://127.0.0.1:18788 &
node scripts/scenario.mjs --base http://127.0.0.1:18787 --runner workflow --preset quick --replay
```

`LOCAL=1` turns the OOM crash into a thrown error; workerd under `wrangler dev`
does not enforce the 128 MB limit, the 15-minute limits or CPU limits.
Editing a source file reloads `wrangler dev` and kills in-flight work.

## Run on the dev account

Resources: Workers `gonvex-cf-dev-s4-runner` and `gonvex-cf-dev-s4-upstream`, Queue
`gonvex-cf-dev-s4-actions`, Workflow `gonvex-cf-dev-s4-action`. Credentials come only from
`whagons-dev credentials exec full-cloudflare-access` (map `apiToken` / `accountId` to
`CLOUDFLARE_API_TOKEN` / `CLOUDFLARE_ACCOUNT_ID`). Both Workers require a bearer token
(`S4_TOKEN` secret; generate one with `openssl rand -hex 24`).

```sh
pnpm exec wrangler queues create gonvex-cf-dev-s4-actions
pnpm exec wrangler deploy -c wrangler.upstream.jsonc
pnpm exec wrangler deploy -c wrangler.runner.jsonc \
  --var UPSTREAM_URL:https://gonvex-cf-dev-s4-upstream.<subdomain>.workers.dev
pnpm exec wrangler secret put S4_TOKEN -c wrangler.upstream.jsonc
pnpm exec wrangler secret put S4_TOKEN -c wrangler.runner.jsonc
S4_TOKEN=... BASE=https://gonvex-cf-dev-s4-runner.<subdomain>.workers.dev scripts/platform-phase1.sh
# phase 3 deploys, so it runs under the credentials wrapper and reads the bearer token from a file:
# whagons-dev credentials exec full-cloudflare-access --via env --prefix CF_ -- \
#   scripts/redeploy-midrun.sh "$BASE" "$UPSTREAM" /path/to/bearer-file
```

Do not redeploy the runner while a batch runs: the batches assume one code
version. Phase 3 measures a deploy mid-run on purpose (in-flight Actions
survived it; a busy tenant object kept the old code for tens of seconds).

Clean up (the Worker cannot be deleted while it is the queue's consumer):

```sh
pnpm exec wrangler queues consumer remove gonvex-cf-dev-s4-actions gonvex-cf-dev-s4-runner
pnpm exec wrangler queues delete gonvex-cf-dev-s4-actions
pnpm exec wrangler delete -c wrangler.runner.jsonc --force      # also drops both Durable Object namespaces
pnpm exec wrangler delete -c wrangler.upstream.jsonc --force
pnpm exec wrangler workflows delete gonvex-cf-dev-s4-action
```
