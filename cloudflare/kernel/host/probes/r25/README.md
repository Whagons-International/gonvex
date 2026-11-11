# K04 probe R25: a CPU-runaway Reducer

Answers 0012 R25 / 0016 G0-11: what one Reducer stuck in `for(;;){}` inside
`ctx.storage.transaction(async …)` does to the tenant object, its sockets, gateway sockets, queued calls,
colocated objects and committed data, and which per-call guard the platform offers. Report: `FINDINGS.md`.
Measurement code only; nothing here is imported by the kernel.

| File | What |
| --- | --- |
| `worker.js` | Worker + `TenantDO` (rows, hibernatable sockets, Reducers in one async storage transaction, clock and guard probes) + `GatewayDO` (G0-1 placement: holds sockets, calls the tenant by RPC) + an executor run through the Worker Loader (`LOADER`). Every route needs `?k=<ADMIN_KEY>`. |
| `wrangler.jsonc` | Config for `gonvex-cf-dev-k04-r25` (default CPU limit). `run.mjs` writes copies with `limits.cpu_ms` into `.results/`. |
| `run.mjs` | Driver: deploys, runs every experiment with Node `fetch`/`WebSocket` clients, writes raw JSON to `.results/` (git-ignored), deletes the Worker and checks the API returns 404 for it. |

## Run

From this directory, with `cloudflare/node_modules` installed
(`pnpm install --frozen-lockfile --filter @gonvex/cloudflare` from the repo root; wrangler 4.155.0):

```sh
# Local workerd (wrangler dev on :8826, started and stopped by the driver). The last step runs an
# endless loop; local workerd has no CPU limit, so it takes ~4 minutes to finish.
node run.mjs local

# Development account: deploys gonvex-cf-dev-k04-r25 three times (default, cpu_ms 10000, cpu_ms 2000),
# ~4 minutes, then deletes it. --keep skips the delete; --variants picks a subset.
whagons-dev credentials exec full-cloudflare-access -- node run.mjs platform
whagons-dev credentials exec full-cloudflare-access -- node run.mjs platform --variants 2000
# Follow-up set (default limit): loader vs in-object latency, runaway with reconnect attempts during the
# overrun, loader runaway without a loader CPU limit. Writes <run>-platform-followup.json.
whagons-dev credentials exec full-cloudflare-access -- node run.mjs platform --variants followup
```

Results: `.results/<run>-local.json`, `.results/<run>-platform.json` (tenant names, timings, close codes, error
texts, isolate ids; no credentials, no URLs). `platform-run.log` holds the driver output if redirected there.
The admin key is random per run and passed with `wrangler deploy --var`.

Costs per platform run: each runaway burns up to its CPU limit once (one per variant, two loader runaways, five in the
retry-storm step at the last variant's limit); ≤ 28 sockets open at once.
