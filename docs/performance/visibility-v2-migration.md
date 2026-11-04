# Whagons visibility: legacy (v1) plans → version 2 masks

Status: 2026-11-03. Gonvex branch `perf/vis-v2` (tests, bench, oracle);
Whagons branch `perf/visibility-v2` (the migration itself).

## Why

Whagons staging (`feat/sqlite-staging-deploy` ea04e91c, Coolify app
qijlnydkhsixirfc0ms6rwy8) logs on every module load:

> DEPRECATED legacy visibility manifest: data-table dependencies can cause SQL
> fan-out; migrate to version 2 masks

With one user, CPU spiked to ~190% and memory went 1.0 → 1.7 GB. The live
staging profile (runtime f3ef8ae1, cumulative from 23:53, including the
el-rey loads; `/home/gabriel/work/profile-capture/analysis.txt`) shows the
cost where the legacy rebuilds land: `delivery.visibility` 51.8 s over only
224 deliveries (p50 3.2 ms, p99 5.37 s, max 5.53 s), `delivery.queue_wait`
p99 2.0 s, and `snapshot.build` 322 s over 3,805 snapshots
(`snapshot.visibility` itself only 2.8 s). Every
optimization of the 5k-users program (compiled masks, per-class evaluation,
token routing, no per-client mirror) targets version 2 plans. Legacy plans use
the old evaluator: their sets are SQL over arbitrary tables, and any write to a
table a set reads re-resolves those sets in SQL for every subscribed member and
can rebuild the collection.

## Inventory

`bench/visibility-v2/inventory.py` over the staging manifest
(`git show ea04e91ce:gonvex/_generated/manifest.json`) and the migrated one:

- **All 340 plans are legacy** (no `version`), so the warning fires for the
  whole manifest, not a few tables.
- **96 plans have data-table dependencies** (a set reads a table that is not a
  declared permission table). They back **263 replica collections and 27
  live-query windows** (`tasks.grid`, `tasks.listWindow`, … read `tasks`).
- The other 244 read only permission tables (members, teams, spots,
  workspaces, roles, …) or nothing.

Most read data tables (legacy plans reading each):

| table | plans |
| --- | --- |
| `tasks` | 40 |
| `taskApprovalInstances` | 39 |
| `taskWorkspaceContexts` | 39 |
| `taskShares` | 37 |
| `taskUsers` | 37 |
| `workspaceViews` | 10 |
| `boards`, `relayCalls`, `workbenchFiles` | 6 each |
| `goals`, `scheduleBoards` | 5 each |

The reason is the task rule: 25 sets, 15 of them `SELECT _id FROM tasks JOIN …`
(spot gating, personal spaces, findings) plus grant sets over `taskUsers`,
`taskShares`, `taskApprovalInstances`, `taskWorkspaceContexts`. Every task
child (notes, logs, tags, users, findings, SLA, signatures, CRM, phone, …)
copies that rule, so each of those tables also depends on `tasks` and the
four grant tables. `workspaces`/`categories`/`spots`/`workspaceTeams` read
`tasks` for "a workspace/category/spot is visible if one of its tasks is".
Per-table detail (declaration version, dependencies, collections, live
queries, v2 set kinds and inheritance): `bench/visibility-v2/inventory.md`,
machine-readable `inventory.json`.

## Migration (Whagons)

A complete v2 migration already existed: Whagons #1623 ("express every replica
policy as v2 row masks"), reverted by #1679 because staging then ran a runtime
without the mask feed path, and restored on `fix/restore-v2-visibility-masks`
(68e136723) with the TS parity harness (legacy evaluator vs mask evaluator for
every plan, historical-commit behavior map). It was never merged into the
SQLite staging line.

The 340 legacy plans on `feat/sqlite-staging-deploy` are **byte-identical** to
that migration's frozen baseline (staging e218792177), so the reviewed masks
apply unchanged. `perf/visibility-v2` merges `fix/restore-v2-visibility-masks`
into ea04e91c: one source conflict (a test import), generated files rerun with
the branch's pinned `0.5.2-sqlite.0` CLI (`pnpm gonvex:types`). The generated
manifest now holds 340 version 2 plans, equal to
`gonvex/module/visibility-masks/deployedPlans.json`. Whagons checks:
`pnpm gonvex:v2:typecheck`, `pnpm test:gonvex:v2-modules` (315 files, 3,871
tests, including the TS parity suites) pass.

Runtime requirement (from the migration's own doc): Gonvex PRs 255, 273
and 277. The staging runtime line (and this branch's base) contains them; the
oracle below runs every v2 plan on this runtime.

## Oracle

Two runtime-level oracles, both on SQLite through the real code paths.

### 1. Fixture parity through two runtimes (`tests/visibility_v2_parity.rs`)

Two `Runtime`s over identical perf-fixture tenants (2,000 tasks, 78 members of
every kind, 104-spot tree, real Whagons schema and collection definitions);
one declares the legacy plans of the fixture's ten collections' tables
(`FixtureVisibility::Legacy`, `whagons_visibility_legacy.json`), the other the
v2 masks. Every member opens all ten collections in both. Then the same
writes commit to both, and after each one, for every member and collection:
v1 client state (deltas from the legacy evaluator) = v2 client state (deltas
from compiled masks) = fresh v1 SQL snapshot = fresh v2 SQL snapshot, rows and
values; digests verified; `fallback_row_checks == 0` on v2.

Writes: task edit / spot move / spot cleared / blank spot / workspace move /
soft delete / restore / insert / hard delete; finding created-by and
reported-by members; reported-by-team; assignment insert, soft and hard
delete; share to user and to team, revoke, soft delete; approval insert and
delete; workspace context to user and to team, revoke; member joins and
leaves a team; gains and loses spots; `see_all_spots` on/off; `tasks.viewAll`
granted and removed; role `system_admin`; workspace unlinked from and linked
to a team; spot subtree moved; tenant all-spots-member setting added and
removed; workspace becomes PERSONAL, personal member, personal context, task
moved into it, personal workspace deleted; tag added, tagged task hidden,
tag removed; presence beat; workspace preference; 24 mixed task edits.

**Result: identical.** 78 members × 10 collections, the open plus 73
writes: 5,673,531 visible rows compared four ways, no leaked, missing or stale
row, no digest mismatch, 0 fallback row checks on v2 (release build, 880 s).

    cargo test --release -p gonvex-runtime --features perf-fixture \
      --test visibility_v2_parity -- --nocapture
    # PARITY_TASKS / PARITY_MEMBERS change the shape (default 2000 / 78)

Its per-write counters (deterministic; v1 / v2; "rebuilds" = collections
forced to resync, each one a client reopen + SQL snapshot):

| write | v1 SQL | v1 rebuilds | v2 SQL | v2 rebuilds |
| --- | --- | --- | --- | --- |
| task description edit | 78 | 0 | 0 | 0 |
| task spot / workspace move, blank spot | 234 | 234 | 0–2 | 0 |
| task insert / hard delete | 312 | 312 | 0–2 | 0 |
| task soft delete / restore | 156 | 78 | 0 | 0 |
| finding created/reported by a member | 234 | 234 | 2 | 0 |
| assignment insert / delete | 80 | 80 | 3 | 0 / 3 |
| share to user or team / revoke | 234 | 234 | 3 | 0 / 3 |
| share soft delete | 234 | 234 | 27 | 27 |
| approval insert / delete | 4 | 4 | 3–4 | 0 / 4 |
| workspace context insert / revoke | 312 | 312 | 3 | 0 / 3 |
| member joins/leaves a team, gains/loses a spot | 3–4 | 3–4 | 3–4 | 3–4 |
| `see_all_spots` toggled | 234 | 234 | 3 | 3 |
| permissions / role changed | 315–318 | 315–318 | 10 | 10 |
| workspace ↔ team link, workspace becomes PERSONAL | 312 | 312 | 330 | 312 |
| spot subtree moved, tenant all-spots setting | 231 | 231 | 231 | 231 |
| personal context insert / personal workspace deleted | 312 | 234–312 | 234 | 234 |
| tag added / removed | 78 | 0 | 0 | 0 |
| presence beat, workspace preference | 0 | 0 | 0 | 0 |
| 24 mixed task edits | 0–234 | 0–234 | 0–2 | 0 |

The v2 rows that still rebuild everyone are tenant-wide permission-table
changes (`workspaceTeams`, `workspaces.type`, `spots.parentId`,
`globalSettings`, PERSONAL contexts through `workspaces`): admin
configuration, rare, and unanchored by design (the runtime conservatively
invalidates unanchored principal sets).

### 2. All 340 plans (`src/visibility_whagons_parity_tests.rs`)

The fixture covers ten tables. For every one of the 340 plans, a real store
(Control Plane + provisioned tenant with the schema of all 339 tables the
plans name, `whagons_schema_all.sql`) is seeded with random rows: keys from a
shared id pool (`k0..k23`, members `k0..k9`), every column drawn from the
literals the plans compare it with (statuses, kinds, PERSONAL, flags, null),
shared ids, blanks and nulls, so joins, grants, inheritance and flags all
fire. Members get random roles and permissions from the plans. For each plan,
member and seed: v1 SQL (`visibility::resolve` + the snapshot query
`replica::query_rows`), v1 interpreted (`row_matches` on every row), v2 SQL
(`ParentAclRegistry::ensure` + `resolve_with_parents` + snapshot query), v2
compiled (`member_visibility` on every row) must admit the same keys.

**Result: identical on all 340 plans.** Release run, 6 seeds × 30 rows per
table × 10 members: 610,800 row checks, 283,286 visible under both, zero
disagreements among the four evaluators (an earlier debug run with 3 seeds ×
24 rows: 244,380 checks, 114,387 visible, zero). `documents` and
`messageReactions` (the two plans with `inheritsAny`) fall back to the
interpreter in the compiled path, as documented in `visibility-masks.md`.

    cargo test --release -p gonvex-runtime --features perf-fixture --lib \
      visibility_whagons_parity -- --nocapture
    # WHAGONS_PARITY_SEEDS, WHAGONS_PARITY_ROWS, WHAGONS_PARITY_TABLES=a,b

Coverage and controls (printed by the test): 160 of 340 tables had a member
seeing a strict, non-empty subset; 58 are always fully visible (public
catalogs: statuses, tags, roles, …); 20 were never visible to anyone in the
random data (plugin-gated tables whose rule needs a conjunction the generator
rarely builds: goals*, market*, some people*/talent*/crm*, publicHolidays,
reportDocuments). Negative controls: the tasks mask with one of its seven
top-level branches removed is caught (v2 ≠ v1 somewhere) for 5 of 7
branches; branches 0 and 2 (personal-context owner / team) are not reached
by random data. Those are exercised by the fixture oracle's
personal-context writes, and every plan, including the never-visible ones, is
also covered by Whagons' TS parity suite (`visibility-masks/*.test.ts`,
legacy evaluator vs mask evaluator on hand-written fixtures). Treat the random
oracle as strong for the 160 tables with partial visibility and as a
consistency check (no false positives) for the rest.

## Cost: v1 vs v2

`benches/visibility_v2.rs`: the shared fixture (9,000 tasks, 78 members × 10
collections, 104-spot tree), once with the legacy plans and once with the v2
masks, same binary, arch3, 1 tokio worker, bench profile, 10 iterations per
write (`bench/visibility-v2/results/2026-11-03-arch3.jsonl`).

    cargo bench -p gonvex-runtime --features perf-fixture --bench visibility_v2

Each write is delivered to every connection the way the websocket loop does
it (per connection `apply_feed_event`, the work of the `delivery.visibility`
profiler stage; each subscription applied on its own so cost can be
attributed to a collection). Collections the runtime forces to resync
(`replica.needHashes`) are then reopened, as the SDK does, and timed per
connection.

**How this maps to staging's `delivery.visibility`.** On the staging runtime
(f3ef8ae1) a legacy dependency change re-runs the collection snapshot inline
inside `apply_feed_event` (`replica_snapshot` in the delivery loop), so it
lands in `delivery.visibility`. On this branch's runtime subscriptions hold
no rows: the same change emits `replica.needHashes` and the cost moves to the
client's reopen (`snapshot.*` stages). The column "per connection apply +
rebuild" adds each connection's apply and its reopens: that is what
staging's stage measures per connection. The total is the per-write sum over
the 78 connections; with one worker it serializes, which is what
`delivery.queue_wait` (p99 2.0 s on staging) shows.

| write (78 members) | v1: rebuilt collections | v1: per connection apply + rebuild, p50 / p99 / max | v1: total per write | v2: rebuilt | v2: per connection, p50 / p99 / max | v2: total per write |
| --- | --- | --- | --- | --- | --- | --- |
| task description edit | 78 (`tasks`) | 42.6 / 180 / 243 ms | 3,677 ms | 0 | 0.011 / 0.31 / 1.6 ms | 1.7 ms |
| task spot move | 234 (`tasks`, `taskUsers`, `taskTags`) | 111 / 262 / 485 ms | 8,535 ms | 0 | 0.024 / 0.31 / 0.71 ms | 3.0 ms |
| task insert | 312 (+ `workspaces.list`) | 116 / 306 / 746 ms | 9,427 ms | 0 | 0.020 / 0.11 / 0.18 ms | 2.1 ms |
| assignment (`taskUsers` insert) | 80 (`taskUsers` of everyone) | 36.7 / 79 / 141 ms | 2,895 ms | 0 | 0.017 / 0.88 / 3.8 ms | 2.5 ms |
| share to a user (`taskShares`) | 234 | 106 / 260 / 415 ms | 8,166 ms | 0 | 0.015 / 0.80 / 1.6 ms | 2.0 ms |
| approval instance insert | 4 | 0.08 / 89 / 136 ms | 110 ms | 0 | 0.014 / 0.95 / 1.3 ms | 2.2 ms |
| member joins a team | 4 | 0.09 / 82 / 102 ms | 92 ms | 4 | 0.07 / 63 / 83 ms | 75 ms |
| presence beat | 0 | 0.09 / 0.10 / 0.3 ms | 7.4 ms | 0 | 0.011 / 0.04 / 0.07 ms | 0.9 ms |

The apply step alone is small in both (v1 ~7 ms, v2 1–6 ms for all 78
connections); under v1 the rebuilds are the whole cost. v1 also interpreted
every change row (78–156 `fallback_row_checks` per write); v2 compiled all of
them (0). Visibility SQL statements per write (from the parity test,
deterministic, includes the rebuild snapshots): v1 78–318, v2 0–3, except
tenant-wide permission writes (see the parity table).

**Which v1 tables cause it** (per-collection attribution of the rebuilds):

- `tasks` (`tasks.recentWorkspaceTasks`): 15 of its 25 legacy sets select
  from `tasks` itself (spot gating at/below member spots, spotless/blank-spot,
  all-spots member/tenant, personal spaces, findings). Any task insert,
  delete or change of `spotId`/`workspaceId`/`createdVia`/... is a
  dependency change for every member; even a description edit forced a
  resync of every member's task collection (78 rebuilds).
- `taskUsers` and `taskTags`: their legacy plans copy the task rule, so they
  depend on `tasks` and the four grant tables and rebuild together with it
  (234 rebuilds for a spot move or a share).
- `taskShares`, `taskWorkspaceContexts`, `taskApprovalInstances`,
  `taskUsers` as grant sources: legacy grant sets are not routed to the named
  member, so one share or context insert rebuilds every member's
  task-derived collections; one assignment rebuilt `taskUsers` for all 78.
- `workspaces` (`workspaces.list`): "a workspace is visible if one of my
  tasks/contexts/approvals is in it" reads `tasks`, `taskWorkspaceContexts`
  and `taskApprovalInstances`, so task inserts/deletes rebuild it for everyone.

In the Whagons manifest the same pattern covers 96 plans (`inventory.md`):
every task child (notes, logs, findings, SLA states, signatures, CRM, phone,
acknowledgments, …) and `categories`, `spots`, `workspaceTeams`.

With v2 masks the same writes cause no rebuild and no SQL for anyone except
the members a grant names (in-memory grant patch) and genuine principal
changes (team/spot/role/flag of one member: rebuilds only that member, as in
v1). Tenant-wide permission-table changes (workspace↔team links, spot tree,
tenant settings, a workspace turning PERSONAL) still rebuild everyone in both
versions.

## Do compiled masks / per-class sharing / token routing apply?

Only with v2 plans; legacy plans get none of them (`member_visibility`
returns `Fallback`, every change row is interpreted per subscriber, and data
dependencies force rebuilds).

- **Compiled masks**: 338 of 340 migrated plans compile. `documents` and
  `messageReactions` use `inheritsAny`, which the compiler does not support
  yet; their rows are interpreted in memory (still no SQL on data writes).
  On the fixture's ten collections every v2 change row was compiled
  (`fallback_row_checks = 0`; v1 counted 78–156 fallback checks per member
  or presence write).
- **Per-class sharing**: applies to every compiled plan. On the fixture a task
  edit costs ~90 class/member evaluations for 78 members × 3 task-derived
  collections, not 234 per-subscriber evaluations.
- **Token routing** (`ClassScope::candidates`): applies to compiled plans
  without `inherits`; `taskUsers`/`taskTags` (inherit `tasks`) register their
  members as always-hit, as `visibility-masks.md` documents.
- **No per-client mirror**: subscriptions hold no rows on this runtime for both
  plan versions; under v1 that turns every dependency write into
  `replica.needHashes` + a client reopen, i.e. a full SQL snapshot per
  affected collection (see the cost section).

## Runtime extensions

None. All 340 legacy rules are expressed in the frozen v2 contract (principal,
grant and derived sets, `inherits` with `where`/`predicate`/`onMissing`,
`inheritsAny`), and both oracles found no disagreement. Follow-ups that are
performance, not expressiveness: compiled `inheritsAny` (2 plans), token
routing through inherited parents, and (separately) the global-existence
policy review the mask doc asks for (`templateForms`, `linkedSubmissions`,
centers) was done in Whagons #1623, not re-reviewed here.

## Not measured

- Live queries. Staging's `delivery.visibility` also includes
  `apply_live_query_feed_event` (the task grid and other windows over
  `tasks`: 27 live queries read v1-flagged tables). The fixture has no
  `taskGridRows`/search projections, so no live query is opened here; v1 live
  windows re-run SQL on dependency writes too, so the staging gap is larger
  than the replica-only numbers above.
- The staging runtime itself (f3ef8ae1). It re-runs v1 rebuild snapshots
  inline in `apply_feed_event`; this branch's runtime emits
  `replica.needHashes` and the client reopens. The "inline-equivalent"
  column (apply + that connection's reopens) models staging; reopens here are
  cold opens, while the SDK reopens with hashes (slightly cheaper transfer,
  same SQL).
- Real tenant data and size (el-rey); the fixture is 9,000 tasks / 78
  members. Cost scales with tasks × affected members.
- A real-tenant row-set comparison (the mask doc's independent sign-off):
  both oracles use fixture or random data.
- Memory (the 1.0 → 1.7 GB staging growth) was not measured here.
