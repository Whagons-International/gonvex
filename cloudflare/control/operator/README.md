# Deploy and operator routes (K19)

`/dev/sync` and the operator HTTP surface of the Rust runtime (`CONTRACT.md` D), on the control object.
Reference: `rust/crates/runtime/src/operations.rs`, `operator_data.rs`, `dashboard.rs` and the dev routes of
`lib.rs` at `0bf3ad13`. Decisions: 0012 R1-R5, 0016 G0-2, 0022 §3, 0027/0028 §3, 0032, 0036.

| Path | What |
| --- | --- |
| `object.ts` | `OperatorControlObject extends ControlObject` (K06): `fetch` for `/dev/*` and `/healthz`, the operator sockets, RPC for the router (`devSync`, `rolloutReport`, `syncKeyAccepted`) and for tenant objects (`operatorDataChanged`), one alarm shared with K06's prune |
| `routes.ts` | Every operator route: account, tokens, notifications, projects, keys, members, invitations, env, auth providers, Google, auth accounts, memberships, tenants, manifest, metrics, logs, cache, storage files, errors, data browser |
| `auth.ts` | D.3: signed dashboard sessions (configured and database authority), native sessions, PATs with bounded grants, admin key, Local (kernel: only with `GONVEX_LOCAL_OPERATOR=true`), `can_access_project`, `can_manage_project`, `authorize_project_resource` |
| `sync.ts` | The `/dev/sync` coordinator: per-project lock (409), control-plane migrations with Rust's ledger, queued fail-fast rollout held open until done or the deadline, activation, `gonvex_runtime_manifests` |
| `sockets.ts` | `/dev/dashboard/ws` (dashboard.rs), `/dev/metrics/stream`, `/dev/logs/stream`, hibernatable, ticks on the alarm |
| `release.ts` | Release pieces in R2, the `ReleaseBuilder` interface, rollout messages, the HTTP builder client |
| `builder/` | The release builder: `manifest.ts` (modules.rs checks, canonical hash), `core.ts` (inspect, prepare with the K02 plan, Worker version build), `server.mjs` (the same core as an HTTP server: Container on the platform, Node locally), `Dockerfile` |
| `tenant/` | The tenant-object half: `port.ts` (`OperatorTenantPort`, `waitForRelease`, `rustTenantError`), `operator.ts` (`TenantOperator`: data browser, members, rollout step on K01/K02/K03), `rows.ts` (rows.rs and operator_data.rs helpers) |

The router side (`cloudflare/router/dev/`) streams `/dev/sync` bodies into R2, adds the `/dev` CORS layer,
consumes the rollout queue and deploys the Worker version. `router/dev/local/` is the local kernel.

## `/dev/sync`

```
CLI ──POST /dev/sync (45 MB)──▶ router ──stream──▶ R2 incoming/<id>.json
                                  │ devSync({incoming, headerProject, key}) (RPC, small)
                                  ▼
                           control object ──inspect / prepare / activate──▶ release builder (Container | Node)
                                  │                                           │ reads/writes R2 via the router's
                                  │ control migrations (own ledger)           │ /_gonvex/k19/blobs route
                                  │ queue: one tenant at a time ──▶ consumer ──▶ tenant.operatorApplyRelease (K02)
                                  │ ◀── rolloutReport ─────────────┘
                                  │ activate: release row, manifest row (one transaction)
                                  ▼
                               router ──deploy(version)──▶ builder ──▶ Worker version live (G0-2)
```

- **Why a builder.** The Whagons body is 44.7 MB (files 26.7 MB, bundles 14 MB, functions 1.6 MB): over the
  32 MiB RPC cap and the 2 MB row cap. The A01 transform over its history took 10.0 s and the whole
  prepare 19.6 s with a 207 MB heap in Node, which no object or Worker isolate can spend (0027 §1). The
  builder is one HTTP server; the platform runs it in a Container (0028 §3.1), local development and the
  end-to-end run start it as a Node process, tests run its core in-process.
- **Order and texts** are Rust's (D.2.2): body, project, key, module checks (`project "<id>" has an invalid
  module artifact: …`), control migrations, tenants in `tenant_id` order with the first failure answered as
  422 and nothing after it touched, install, crons, manifest, response
  `{ok, project, functionCount, schemaDefinition, runtimeReloaded}`.
- **Held open (R4).** The control object waits for the queue's per-tenant reports up to
  `GONVEX_SYNC_DEADLINE_MS` (10 minutes by default) and answers 503 after it.
- **Activation (G0-2).** The builder writes one module, `releases.js`, that imports every project's active
  Query/Reducer bundle wrapped with K04's prelude, plus its function contracts. The kernel imports it as
  `gonvex:releases`. A tenant object installs its project's release into a K04 `ModuleRegistry` on first use.
  The router, not the control object, puts the version live: a deploy restarts active objects, and the
  control object is the one holding the request.
- **Lock (R5).** A second sync of a project while one runs gets 409. The lock row expires with the deadline,
  so a reset control object does not keep it.

## Operator routes

All of D.1's operator rows, with Rust's statuses and `{error}` texts, run in the control object; data
browser rows go to the tenant object through `OperatorTenantPort`. Kernel differences are in note 0036.

## Run

```sh
cd cloudflare
pnpm vitest run --root router/dev          # 25 workerd tests: sync, rollout, routes, data, sockets
node router/dev/local/start.mjs            # the local kernel + builder on :8787
node router/dev/local/e2e.mjs --app <whagons5-client checkout> --work <dir> --keep   # done_when part 1
node router/dev/local/dashboard.mjs --report <dir>/report.json --playwright <dir with playwright>  # part 2
```
