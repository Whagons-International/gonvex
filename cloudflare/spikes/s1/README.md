# S1 schema spike

Self-contained wrangler project (P01's `cloudflare/` did not exist yet). One
SQLite-backed Durable Object class, `TenantSpike`, addressed by `?o=<name>`.
The report is `docs/cloudflare/spikes/S1-schema.md`; raw results are in
`results/` (names, counts and timings only, no row values).

Routes (all `POST`, JSON): `/exec` (statements, optional transaction and
pragmas), `/query`, `/insert` (one prepared shape, many rows), `/verify` (row
count per table, `foreign_key_check`, `quick_check`), `/wake`, `/abort`
(restart the object), `/alloc` (hold JS memory), `/stats`, `/reset`. The
deployed Worker requires the `x-s1-token` header (`wrangler secret put S1_TOKEN`)
because it holds a copy of tenant data.

## Run

```sh
pnpm install                       # wrangler 4.155.0, workerd 2026-10-23
pnpm dev                           # local workerd on :8787
export S1_TOKEN=...                # only for the deployed Worker

node scripts/probes.mjs <base> results/probes.json           # platform limits, FTS5, capture, regexp
node scripts/nesting.mjs <base>                              # CTE / view chain limits
node scripts/analyze.mjs <tenant.db> <migrations> scripts/runtime-prelude.sql results/analysis.json
node scripts/verify-workarounds.mjs <scratch copy of tenant.db> results/verify-workarounds.json
node scripts/load.mjs migrations <base> <object> scripts/runtime-prelude.sql <migrations> <tenant.db> [--raw] [--chunk N] [--continue]
node scripts/load.mjs snapshot <base> <object> <tenant.db> [--schema-only]
node scripts/capture.mjs <base> <object> [out.json] [--rewrite-only]
node scripts/reference-views.mjs <base> <object>
node scripts/wake.mjs <base> <object> [rounds] [out.json]
node scripts/headroom.mjs <base> <object>
```

Deploy with the development account only:

```sh
whagons-dev credentials exec full-cloudflare-access -- sh scripts/with-cloudflare.sh node_modules/.bin/wrangler deploy
```

`scripts/runtime-prelude.sql` is the static SQL the Rust runtime runs before
tenant migrations (`TENANT_IDENTITY_SQL`, `reference_integrity/schema.sql`,
`schema_options.sql`, the `gonvex_migrations` ledger) at
`0bf3ad13419d25d9370607e1470ae58da4c43235`. Migrations come from
whagons5-client `2457bb65f` (`migrations/` is identical on today's
`origin/staging`).

`scripts/sql.mjs` holds the statement splitter and the rewrites the report
proposes. They are spike code that proves a rewrite keeps results identical;
they are not a schema change.
