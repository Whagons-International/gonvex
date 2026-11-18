# BUG-8 triage: API keys created in Settings → Integrations never appear in the list

Walkthrough 2026-11-17 (report `bugs/BUG-8-apikeys-list-empty.md`). Backends 6397e41d, 858f19e0 and afecea5e.
Tenants devcopy-stg1117-11, -04 and -12. Unmodified staging frontend 678bdc80. The creator in every case was
staging-smoke-admin, which was added to each copy through `POST /dev/internal/e2e/members`.

## Verdict

The kernel is at parity with Rust, and this PR changes no behavior. The app authorizes the write and the read with
two different permission checks, and the walkthrough's admin passes only the first:

| Step | Code (whagons5-client 678bdc80) | Permission check | Walkthrough admin |
| --- | --- | --- | --- |
| Create | `apiKeys.create` (Action) calls tool `apiKeys.createRecord` (Reducer), which calls `requireIntegrationAccess` | `hasSettingsPermission(ctx, "config.integrations")`: derived from the Member's roles (`roles`, `rolePermissions`, `global_role_names`) | passes (role `admin`) |
| List | `apiKeys.list`: eager `replicaCollection` on `apiKeys`, `excludeWhenSet: ["revokedAt"]` | `apiKeysVisibility.where = {operator: "permission", value: "config.integrations"}`: the Member's **direct** `members.permissions` JSON | fails (`{"e2e":true}`) |

Both runtimes evaluate the visibility atom `permission` against the stored `members.permissions` object, never
against role grants:

- Rust: `visibility.rs` `resolve_member` (`SELECT id, role, permissions FROM members WHERE account_id = $1`),
  `visibility_class.rs` `BitAtom::Permission => truthy(resolved.permissions.get(permission))`, and the SQL form
  `json_each(_gonvex_member.permissions) WHERE key = …`. No admin or owner bypass exists.
- Kernel: `visibility/principal.ts` `resolveMember`, `visibility/class.ts` (`atom.kind === "permission"`),
  `visibility/mask.ts` and `visibility/sql.ts` do the same.

Both runtimes' e2e seeding route writes `role='admin', permissions='{"e2e":true}'` (Rust `lib.rs:1315-1317`, kernel
`reducer/object.ts`). The keys are therefore stored with `isActive=1` and no `revokedAt`, and the `apiKeys.list`
snapshot for that Member is empty. Rust staging behaves the same way for this Member. The `replica.delta` and
`replica.ready` frames the client received are the correct empty window.

Real staging owners hold the grant directly. In the local staging copies (`staging-current/source-{1,2,3}.db`,
aggregates only), the owner of source-1 and source-2 has `members.permissions["config.integrations"] = true`. The owner
of source-3 and every active `member` row do not. Any role-admin or owner without that direct key sees the same empty
list on staging today.

## Candidates ruled out

- **An Action's write is missing from the change log.** The nested Reducer commit is captured and gets a revision like
  any Reducer write. New test: `kernel/assembly/test/composition.spec.ts`, "BUG-8 a row an Action writes …".
- **A different table, or a replica collection that was never registered.** The rows are in `apiKeys`, which is the
  table the collection reads, and the client got `replica.ready` for `apiKeys.list`.
- **`excludeWhenSet`, ordering or column decoding.** New test: `kernel/delivery/test/delivery.test.ts`, "BUG-8 …". It
  uses the exact 678bdc80 replica definition and visibility plan (v2, the same `permissionTables`):
  - With `{"e2e":true}` the snapshot is empty.
  - After a direct `config.integrations` grant, the stored row is delivered, a newly created key arrives as a delta,
    and a revoked key leaves the window.

## What would change the outcome (not kernel work)

- **App (whagons5-client, at cutover or in staging work):** make `apiKeysVisibility` and `webhooksVisibility` match the
  Reducer authorization. One option is a `permissionGrants` projection or a set built from roles. Another is to keep
  `members.permissions["config.integrations"]` in sync when a role grants it. Changing kernel semantics instead would
  break parity with Rust (brief rule 2) and would also widen `webhooks` and `workspaceResources`.
- **Dev walkthrough data:** to revoke the five synthetic `C3-SYNTH key (revoke)` keys from the UI, give the
  walkthrough admin a direct `config.integrations: true` on the dev copies. Otherwise, revoke the keys by id with an
  operator write. Neither was done here: no dev data was changed and no key value was read.
