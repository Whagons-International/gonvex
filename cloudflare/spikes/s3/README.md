# S3 socket spike

Report: `docs/cloudflare/spikes/S3-sockets.md`. This is a self-contained wrangler project outside the pnpm workspace,
like the S1 and S2 spikes. It is measurement code, not kernel code.

| Path | What |
|---|---|
| `src/worker.ts` | Worker + `TenantDO` + `GatewayDO`. `SocketHub` is the one delivery class both placements use. Admin ops under `/admin/t/<tenant>/<op>` and `/admin/g/<tenant>/<n>/<op>`, sockets at `/ws?t=&m=&c=&live=&gw=`. Every route needs `?k=<ADMIN_KEY>`. |
| `load/lib.mjs`, `load/fleet.mjs` | Socket fleet on worker threads, latency histograms (writer and receivers share one clock), control socket, admin client, local heap through the inspector. |
| `load/fanout.mjs` | Single commits, fan-out CPU probes, burst steps (`--bursts 10x20,30x60@256`), `--deflate`, `--gateways N`. |
| `load/cpu.mjs` | One tenant per delivery variant, for CPU per fan-out from analytics. |
| `load/memory.mjs` | Local V8 heap per socket count. Platform ballast probe (`--probe`). |
| `load/liveness.mjs`, `load/idle.mjs` | Auto-response pings, alarm sweep, silent sockets. 20-minute idle sockets. |
| `load/hibernate.mjs`, `load/attach.mjs` | Hibernation threshold, wake latency, eager or lazy rebuild, attachment limit. |
| `load/deploy.mjs` | Three deploys against 1,000 reconnecting sockets (calls `wrangler deploy`). |
| `load/big.mjs`, `load/big-threshold.mjs` | Message caps in both directions. |
| `load/hop.mjs` | Tenant → gateway RPC round trip. |
| `load/do-usage.mjs`, `load/requests.mjs`, `load/logs*.mjs`, `load/gql.mjs` | GraphQL Analytics and Workers Logs queries (run under `bin/cf-env.sh`). |
| `results/*.json` | Raw results (`-local` = local workerd, `-cf` = platform). They hold tenant names, timings and counts only. |

## Run

```sh
pnpm install                      # inside this directory (own pnpm-workspace.yaml)
# local
echo "ADMIN_KEY=local-spike-key" > .dev.vars && pnpm dev            # :8797
S3_LOCAL=1 S3_BASE=http://127.0.0.1:8797 node load/fanout.mjs --bursts 10x10
# platform (development account; names start with gonvex-cf-dev-)
whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh npx wrangler deploy
openssl rand -hex 24 | tr -d '\n' | whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh npx wrangler secret put ADMIN_KEY
S3_BASE=https://gonvex-cf-dev-s3.<subdomain>.workers.dev S3_KEY=<key> node load/fanout.mjs --gateways 8 --bursts 30x60
whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh node load/do-usage.mjs <fromISO> <toISO>
# clean up
whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh npx wrangler delete --force
```

Spike-only instrumentation that a kernel must not copy: the wake counter written to SQLite in the constructor, the
memory ballast, the `colo()` subrequest, and the `lastSweep` meta row.
