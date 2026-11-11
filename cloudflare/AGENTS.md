# cloudflare/ — agent guide

The Gonvex backend on Cloudflare Workers + Durable Objects: one SQLite-backed Durable Object per tenant.
Read `docs/cloudflare/AGENT_BRIEF.md` and your entry in `docs/cloudflare/work-packages.json` first; this file
only covers the package mechanics. Branch `cf/<package id>-<slug>` from `cloudflare/integration`, open a
non-draft PR back to it with the package id in the title.

## Layout

| Path | What |
| --- | --- |
| `wrangler.jsonc` | Worker config: entry, compatibility date, Durable Object bindings and migrations. |
| `worker-configuration.d.ts` | Generated `Env` types. Rerun `pnpm types` after editing `wrangler.jsonc`; `pnpm typecheck` fails when stale. Never hand-edit. |
| `src/index.ts` | Worker entry: routes requests to objects, re-exports every Durable Object class. |
| `src/hello.ts` | P01 hello-world object (SQLite write + WebSocket echo). Template only; the tenant object replaces it. |
| `test/*.test.ts` | Vitest tests that run **inside workerd** (`@cloudflare/vitest-plugin`), with real Durable Objects and SQLite. |
| `control/` | K06 control object (Control Plane schema, `control.*`, auth, native OAuth, tenant routing), with its own `wrangler.jsonc`, tests and README. `pnpm test` and `pnpm typecheck` include it. |
| `scripts/deploy-preview.mjs` | Deploys/deletes preview Worker `gonvex-cf-dev-preview-<branch-slug>`. Node tests beside it. |
| `.github/workflows/cloudflare.yml` | PRs to `cloudflare/integration`: typecheck, tests, bundle size, preview deploy, delete on close. |

## Commands (run in `cloudflare/`)

```sh
pnpm install --filter @gonvex/cloudflare   # from the repo root; workerd comes with wrangler
pnpm typecheck        # wrangler types --check + tsc (strict)
pnpm test             # workerd tests + node tests; no account or network needed
pnpm types            # regenerate worker-configuration.d.ts
pnpm bundle           # dry-run deploy into dist/, prints upload size
pnpm dev              # local workerd on :8787

# Preview deploy on arch3 (prints the URL, smoke-tests GET /, never prints credentials)
whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy-preview.mjs deploy
whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy-preview.mjs delete
node scripts/deploy-preview.mjs name   # show the preview name for this branch
```

Delete every preview you deploy by hand once you have your numbers.

## Rules that bite here

- **Versions are pinned exactly.** `wrangler` and `@cloudflare/vitest-plugin` move in lockstep: the plugin pins
  one wrangler version, and they must match so tests and deploys use the same workerd. Upgrade both together and
  keep `compatibility_date` at or below the workerd date.
- **Durable Object classes are SQLite-backed**: add them under `new_sqlite_classes` in a new migration tag.
  Never edit or reorder an existing migration.
- **Resource names start with `gonvex-cf-dev-`.** Never touch other Workers, DNS, zones or R2 buckets in the account.
  Credentials come only through `whagons-dev credentials exec`; never print them.
- **Platform limits** (stop and report if one blocks you; do not design around it alone): 100 KB per SQL statement,
  100 bound parameters, 100 columns, 2 MB per row, no custom SQL functions or hooks, `BEGIN`/`SAVEPOINT` only through
  the storage transaction API, 128 MB per isolate, 30 s CPU per event, 10 GB per object, 32 MiB inbound WebSocket
  message, no server-sent protocol pings.
- **Do not add `cloudflare/` to any workflow that deploys `main` or `staging`.**

## Design rules (from whagons-lite `docs/DOD.md`)

- No SQL inside a loop over rows or members. Read by id list, once per batch.
- Work per change stays flat as member count grows: encode once per visibility class, send the same bytes to every
  socket in it.
- Every loop over rows works in pages and yields between pages.
- Nothing ephemeral is written to SQLite.
- Delivery is written against one interface with two placements (in the tenant object, or in a gateway object), so
  fan-out shards are a routing change.

## Every PR states its numbers

Bundle bytes (`pnpm bundle`, gzip too), rows read and written for the functions it touched (`SqlStorageCursor`
exposes `rowsRead` / `rowsWritten`; assert them in tests), and memory where it matters.
