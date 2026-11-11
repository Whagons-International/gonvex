# S5 Worker Loader spike

Report: `docs/cloudflare/spikes/S5-worker-loader.md`. This is a self-contained wrangler project outside the
pnpm workspace, like S1–S4. It is measurement code, not kernel code.

## Layout

| Path | What |
|---|---|
| `src/loader.js` | The deployed Worker, in two builds. **loader** (`gonvex-cf-dev-s5`): `Tenant` object (sockets, generation switch, three execution modes), `Bundles` object (generation code store), `Gateway` object (G0-1 placement). **baked** (`gonvex-cf-dev-s5-baked`): the same file with the Whagons Query/Reducer bundle imported statically, the S2 / 0016 G0-2 baseline. |
| `src/gen.js` | Main module of one generation, loaded through `env.LOADER`. `Runner` (WorkerEntrypoint: dispatch here, host calls over RPC back into the tenant) and `TenantFacet` (Durable Object Facet: S2's host and the app together, on the facet's own SQLite). |
| `src/invoke-tx.js` | S2's one-transaction-per-invocation wrapper with a pluggable dispatch step. |
| `scripts/prepare.mjs` | Builds `.build/loader`, `.build/baked` and the generation uploads. The kernel modules are S2's `src/` files unchanged. |
| `scripts/dev.sh` | Both builds on local wrangler dev (:8798, :8799). |
| `split/` | Split-deploy probe: gateway objects in one script, tenant objects in another (`gonvex-cf-dev-s5-gw`, `gonvex-cf-dev-s5-tn`). |
| `test/workload.mjs` | S2's 39 steps per mode on fresh tenants, outcome and row-count comparison. |
| `test/roundtrip.mjs` | Committed Reducers, facet vs baked, timed by the front Worker. |
| `test/probes.mjs` | `load`, `hostbench`, `deadlock`, `gate`, `isolation`, `inflight`, `ballast`. |
| `test/txcalls.mjs` | Host-call RPC round trips under an open storage transaction, with and without `limits.cpuMs` (ruling 0022). |
| `test/sockets.mjs` | Generation switches with live sockets on the tenant; `--with-load`; `--deploy` for a Worker-version deploy. |
| `test/gateway-sockets.mjs` | Generation switches with sockets on gateway objects and on the tenant. |
| `test/runaway.mjs` | A spinning Reducer in a facet under `limits.cpuMs`, with sockets and queued calls. |
| `test/abort-*.mjs` | `facets.abort` behavior: matrix, sequences, abort during facet startup. |
| `test/split-deploy.mjs` | Tenant-script deploys under 500 gateway sockets. |
| `test/rss-local.mjs` | Local workerd RSS per loaded generation. |
| `test/logs.mjs` | Workers Logs events with a non-ok outcome. |
| `results/*.json` | Raw results (`-local` = local workerd, `-cf` = platform): tenant names, timings, counts, error texts. |

## Run

```sh
pnpm install                                   # wrangler 4.158.0 (workerd 1.20261104.1)
# The app bundle and manifest come from S2: node ../s2/scripts/build-artifact.mjs <whagons5-client 2457bb65f>,
# then copy module.reducers.js, functions.json and schema-subset.json from ../s2/.artifact into .artifact/.
node scripts/prepare.mjs --built <gonvex checkout with built packages>
# Platform (development account; names start with gonvex-cf-dev-)
for v in loader baked; do (cd .build/$v && whagons-dev credentials exec full-cloudflare-access -- ../../bin/cf-env.sh ../../node_modules/.bin/wrangler deploy); done
openssl rand -hex 24 | tr -d '\n' > <key file>     # then `wrangler secret put ADMIN_KEY < <key file>` in each build dir
S5_KEY_FILE=<key file> ./bin/cf-target.sh node -e 'import("./test/lib.mjs").then(async (l) => { await l.upload("tiny"); await l.upload("whagons"); })'
S5_KEY_FILE=<key file> ./bin/cf-target.sh node test/workload.mjs --modes facet,inline --tenants 8
S5_KEY_FILE=<key file> ./bin/cf-target.sh node test/probes.mjs load
# ... the other drivers the same way; sockets.mjs --deploy and split-deploy.mjs also need bin/cf-env.sh
# clean up
for w in gonvex-cf-dev-s5 gonvex-cf-dev-s5-baked gonvex-cf-dev-s5-gw gonvex-cf-dev-s5-tn; do whagons-dev credentials exec full-cloudflare-access -- ./bin/cf-env.sh npx wrangler delete --name $w --force; done
```

Spike-only instrumentation a kernel must not copy: the ballast probe, the admin key in a query string, the
`mark` route, per-call timing in responses, and the drain implemented as a promise set.
