# Visibility masks: permission checks without fan-out

Status: design, 2026-10-29. Implementation and behavior parity remain required.

## Problem

Visibility plans today are a small query language. A set may select any column
from any table through equality joins, and the feed treats every table a set
reads as a permission input. One write to such a table makes every connected
member re-run its sets in SQL and rebuild its collections. This is
subscription invalidation, the behavior Gonvex exists to avoid.

Observed on Whagons staging, 2026-10-29: two approvals pinned the runtime at
about 110% CPU for about 60 seconds. Ordinary task writes caused 50–80% bursts.
History:

- 2026-08-29, gonvex `8a73c779`: dependency-table invalidation in the first
  Rust replica runtime. Any insert or delete in a dependency table counts as a
  change for every member (`visibility.rs` `change_affects`). A dependency
  change rebuilds the collection (`replica.rs` `apply_feed_event`).
- 2026-09-30, client #967: spot gating was expressed as task-id sets selected
  from `tasks`, because the language had no member-flag operator. Every task
  write became a permission change for every member.
- 2026-10-19, gonvex #128: budgeted collections with materialized sets rebuild
  on any change to their own source table (`bounded_sql_visibility`).
- 2026-10-19 and 2026-10-23, client: more `tasks`-based sets (personal spaces,
  `ownedFindingTasks`).

Each change fixed a real correctness bug. None was checked for cost.

## Invariant

**V1. A permission check is a pure, linear, in-memory function of one row and
one principal.**

`visible(row, principal)` reads only the row's own columns and the member's
principal. It performs no I/O. Its cost is O(|row ACL columns| + |rule|).

**V2. A write never causes visibility SQL on a connection unless it changes
that connection's own principal.**

**V3. Principal construction reads only member-owned permission rows, and is
rebuilt only when one of them changes for that member.**

## Model

### Row ACL

Visibility reads only columns of the row itself:

- scalar ACL columns, for example `workspaceId`, `spotId`, `createdBy`,
  `reportedByUserId`, `reportedByTeamId`;
- grant arrays, for example `grantMemberIds` and `grantTeamIds` (text[]), for
  per-row grants such as assignment, approval, routing and sharing.

The application keeps grant arrays current inside the same Reducer transaction
as the grant itself, as the Reducer contract already requires for all
consequences of one intent.

Child tables (notes, attachments, logs) carry a copy of their parent's ACL
columns. Copies are kept current by the same Reducer as the parent's change, or
by a declared ACL inheritance the runtime maintains in that transaction. A child
rule never joins its parent.

### Principal

Computed when the session is admitted, and again only when invariant V3 says so:

- `member.id`, `account.id`, `role`, `permissions`;
- token sets: `teams`, `workspaces`, `spots` (assigned spots and their
  descendants, flattened), `personalWorkspaces`;
- flags, for example `seeAllSpots`.

The application declares how each token set is computed from member-owned
permission tables. Each declaration is anchored on `member.id`. Its source
tables are the principal inputs. They are permission tables, never data tables.

### Rule language

Operators allowed in a visibility rule:

| Operator | Meaning |
|---|---|
| `public`, `role`, `permission`, `flag` | principal-only checks |
| `eqPrincipal(column, key)` | `row.column = principal.key` |
| `inTokens(column, tokenSet)` | `row.column ∈ principal.tokenSet` |
| `overlaps(arrayColumn, tokenSet)` | `row.arrayColumn ∩ principal.tokenSet ≠ ∅` |
| `eq(column, literal)`, `isNull(column)` | row-only checks |
| `and`, `or`, `not` | composition |

Not representable: sets over tables, joins, subqueries, and references to
other rows. The manifest schema has no field for them, so a rule that needs them
fails code generation.

The same rule compiles to SQL for snapshots and windows, with token sets bound
as array parameters (`column = ANY($n)`, `array && $n`), and to the in-memory
evaluator for feed deltas. A shared test corpus checks that both always agree.

## Feed path

For each committed transaction and each connection:

1. **A principal input row changed.** If it names this member (its anchor
   column equals `member.id` in the old or new value), rebuild the principal
   from SQL for this member only. Then re-check the cached rows in memory and
   send inserts and deletes. Other connections skip the row.
2. **Any other row changed.** Evaluate `visible(old)` and `visible(new)` and
   send an insert, update or delete. No SQL.

Budgets and windows never force a rebuild on a source change. A budgeted
collection that receives a new visible row beyond its budget reports itself
truncated, as initial delivery already does.

Cost of a write: O(changed rows × connections) in-memory row checks, with zero
SQL, plus one principal rebuild for each member it names. A later optimization
can index connections by token (for example workspace → connections) to make it
sub-linear in connections. Correctness doesn't depend on that.

## Enforcement

Four independent layers. Any one of them blocks a regression.

1. **Schema (code generation).** The visibility manifest schema contains only
   the operators above. The CLI rejects unknown operators and any table or join
   reference. Principal declarations must have a `member.id` anchor and may name
   only tables declared `permission`. A table can't be both a `permission` table
   and a collection source with high write volume unless it is declared, and
   that declaration is reviewed in the rule registry.
2. **Code structure (runtime).** The feed visibility path takes no database
   handle. `visible(row, principal)` lives in a module with no access to
   `sqlx`, the pool or async runtimes. A CI check (cargo-deny or a crate
   boundary) fails if that module gains one. Only `principal::build` may query,
   and only through permission-table declarations.
3. **Proof (Verus).** `visible` and the feed decision function (`skip`,
   `row delta`, `rebuild principal`) are extracted from the production source,
   as in the call-budget and generation proofs. Properties:
   - `visible` is total and depends only on (row ACL, principal).
   - The decision returns `rebuild principal` only if a changed principal-input
     row names the member.
   - For any change set without principal inputs, every connection's decision
     is `skip` or `row delta`.
   Negative controls must fail: letting a non-anchored change rebuild, dropping
   the anchor check, letting a source change rebuild a budgeted collection.
   Register this as guarantee **VIS-1** in `verification/guarantees.md`.
4. **Fan-out budget (CI, deterministic).** A test opens 1,000 member
   connections with realistic collections over a fixture tenant and counts
   statements, not time:
   - one task insert, update or delete: 0 visibility statements, and at most
     changed rows × connections row checks;
   - one grant change naming k members: principal rebuilds on exactly those k
     connections;
   - one team-membership change for one member: 1 principal rebuild.
   Any increase fails CI. Add it to `docs/rules/registry.json` as
   `G1-visibility-no-fanout`, citing the 2026-10-29 incident.

## Migration

1. Gonvex: rule schema, principal builder, evaluator, SQL compiler, feed path,
   fan-out test, proof. The set and dependency path is removed, not kept beside
   the new one.
2. Whagons: add ACL columns and grant arrays with a backfill migration. Keep
   them current in the existing grant Reducers (assignment, approval, workspace
   context, share). Rewrite every visibility rule. A parity test proves each
   member sees exactly the same rows under the old and new rules, across every
   grant path and fixture tenant, before the old rules are deleted.
3. Land before production moves to the staging runtime.

## Required implementation decisions and acceptance gates

The implementation uses permission-only principal sets, feed-maintained grant
sets, and shared parent inheritance. These decisions supersede the grant-array
and copied-child-ACL suggestions above. Those suggestions describe an alternative,
not an application migration requirement.

- Declare permission tables explicitly. A principal set may read only these
  tables. Reject a set reading its own collection source or another data table
  during SDK validation and code generation, naming the set and recommending
  a principal set, grant set, or inheritance.
- Grant sets read one grant table and may join permission token sets. Maintain
  old/new row contributions with reference counts. Route changes only to the
  members named by those contributions, without SQL.
- Add `nonEmpty(set)` for flags. Compile principal tokens as SQL array
  parameters and use the same semantics in the in-memory evaluator.
- Inheritance resolves parent ACL columns through a tenant-shared index loaded
  lazily and maintained once per feed transaction. Validate acyclic inheritance
  and a bounded depth. Newly visible parents fetch children only for the
  affected connection. Parent deletion removes inherited access.
- Cache principals by tenant and member across connections. Deduplicate identical
  sets. Route permission changes using old and new anchor/reachable join values,
  including LEFT JOIN paths so missing links becoming present are observed.
  Column filtering must retain every column used by routing and predicates.
- Source-table writes must never force budgeted or progressive rebuilds. Live
  queries reconsider only visible rows matching their filter, or a changed
  principal. Preserve window ordering and truncation semantics.

### Deployment compatibility

The runtime must continue accepting existing manifests with a loud legacy-plan
 deprecation log. The new SDK and CLI reject forbidden legacy shapes. Deploying
 the runtime before the client must preserve current behavior. A separate small
 follow-up enables runtime rejection only after the migrated client is live.
 Do not alter the mobile compatibility bridge.

### Behavior preservation

Keep legacy and replacement plans loadable side by side from the same commit
until the independent real-tenant parity review signs off. Export an explicit
legacy plan map and document how to load it beside the generated mask manifest.
Do not delete or weaken existing visibility tests.

Inventory every visibility-changing client staging commit since 2026-09-27,
including the commits named in the task. Record each introduced access rule and
its proving test. Both PR descriptions must include that behavior table.
Tests must exercise old and new plans for every fixture row, plan, and member
role through SQL snapshots and in-memory deltas. Any disagreement fails the
build. Preserve behavioral assertions when replacing tests of set internals.

The deterministic runtime fan-out test must use real collections and count SQL
statements. A task write requires zero visibility statements; a grant naming k
members affects exactly those members; a membership change for one member
rebuilds one shared principal. Register the incident and budget in the rule
registry. A standalone decision-function simulation does not establish these
runtime budgets.

Both repositories require ready, linked PRs against staging, local targeted
validation, and explicit follow-ups for the legacy rejection flip and any
omitted Verus proof. Neither PR may be merged or deployed under this task.

## Frozen version 2 SDK and manifest contract

The following is the client migration target. Fields use camelCase in both
TypeScript and generated JSON. Existing unversioned plans remain the legacy
format during rollout. Version 2 runtime execution is still being implemented;
do not deploy a version 2 manifest before runtime integration is complete.

```ts
type MaskPlan = {
  version: 2;
  table: string;
  key: string;
  permissionTables: readonly string[];
  sets: Readonly<Record<string, {
    kind: "principal" | "grant";
    table: string;
    alias?: string;
    select: string;
    selectFrom?: string;
    joins: readonly {
      table: string;
      alias?: string;
      leftAlias?: string;
      leftColumn: string;
      rightColumn: string;
    }[];
    where: readonly {
      table: string;
      column: string;
      context?: "member.id" | "account.id";
      value?: { literal: JsonValue };
    }[];
  }>>;
  where: MaskExpression;
};
type MaskExpression =
  | { operator: "public" }
  | { operator: "role" | "permission"; value: string }
  | { operator: "eq"; column: string; value: { literal: JsonValue } }
  | { operator: "eqContext"; column: string; context: "member.id" | "account.id" }
  | { operator: "inSet"; column: string; set: string }
  | { operator: "nonEmpty"; set: string }
  | { operator: "inherits"; column: string; table: string; key: string }
  | { operator: "and" | "or"; children: readonly MaskExpression[] }
  | { operator: "not"; children: readonly [MaskExpression] };
```

`permissionTables` is an explicit per-plan declaration, copied from the app's
canonical permission-table list. Runtime principal sharing keys include the
canonical set declaration and tenant/member identity. Equivalent declarations
across plans share token values. Permission-table declarations must agree
across the module. A declaration never makes a table's rows public.

A `principal` set's base table and every joined table must be declared permission
tables. A `grant` set's base is its single grant source; every joined table must
be a declared permission table. Grant constraints must anchor on `member.id` or
`account.id`, directly or on a joined permission occurrence. The runtime derives
member routing and reachable join keys from these equality joins and anchors.
Principal sets without a member/account anchor are tenant-global and shared once
per tenant. Neither kind may read or join its plan's own source table.

`nonEmpty` tests the resolved set's cardinality, independently of the current
row. It has no `column` field. `inSet` compares a row column to set tokens.
`inherits` looks up `row[column]` in the declared parent table by `key`, and
applies that table's visibility plan to the shared parent ACL projection.
Missing/deleted parents and null foreign keys deny access. It is an expression,
so independent access grants can be combined with it using `or`; multiple
required parents can be combined with `and`. Parent `key` must equal the parent
plan's declared key. The module validator rejects inheritance cycles and chains
deeper than four edges. Four is a contract limit for this first implementation,
not a measured performance threshold.

The permission-table `where` constraints, join occurrence aliases, literal
wrapping, and column names retain existing semantics. There is no implicit
soft-delete filtering. Carry all existing `deletedAt`, `revokedAt`, status, and
personal-space constraints into the replacement declarations.

Example task mask and child plan, serialized exactly as JSON:

```json
{
  "version": 2,
  "table": "tasks",
  "key": "_id",
  "permissionTables": ["memberTeams", "workspaceTeams"],
  "sets": {
    "workspaces": {
      "kind": "principal", "table": "workspaceTeams", "select": "workspaceId",
      "joins": [{"table": "memberTeams", "leftColumn": "teamId", "rightColumn": "teamId"}],
      "where": [{"table": "memberTeams", "column": "memberId", "context": "member.id"}]
    },
    "assigned": {
      "kind": "grant", "table": "taskUsers", "select": "taskId", "joins": [],
      "where": [{"table": "taskUsers", "column": "userId", "context": "member.id"}]
    }
  },
  "where": {"operator": "or", "children": [
    {"operator": "inSet", "column": "workspaceId", "set": "workspaces"},
    {"operator": "inSet", "column": "_id", "set": "assigned"}
  ]}
}
```

```json
{
  "version": 2,
  "table": "taskNotes",
  "key": "_id",
  "permissionTables": ["memberTeams", "workspaceTeams"],
  "sets": {},
  "where": {"operator": "inherits", "column": "taskId", "table": "tasks", "key": "_id"}
}
```

These examples illustrate the wire format only. The client task mask must also
preserve spot, personal-space, creator, reporter, approval, context, and share
rules. A member flag uses, for example,
`{"operator":"nonEmpty","set":"seeAllSpots"}`, where `seeAllSpots` is a
permission-only principal set constrained to that flag's existing truth rule.
