# P03: tenant transfer (SQLite file ⇄ Durable Object)

Moves one tenant's SQLite file into a SQLite-backed Durable Object and back
out again, and proves both directions match by per-table content checksum.

```
source.db ──stage──▶ in.db ──push──▶ Durable Object ──pull──▶ raw.db ──stage──▶ out.db
           (runtime                (verbatim, chunked)                 (runtime
           provisioning +                                              provisioning +
           doc copy rules)                                             doc copy rules,
                                                                       installer accepts)
```

Two halves, one job each:

| Half | Code | Does |
|---|---|---|
| Semantics | `stage/` (Rust, `gonvex-tenant-stage`) | Builds the target file with the runtime's own `provision_tenant_database` (app tenant migrations loaded by `gonvex-admin`'s loader), copies rows under the rules of `docs/sqlite-storage.md` ("Moving existing data"), rebuilds the reference ledger with `reference_integrity::declare`, and checks that the installer accepts the result. |
| Transport | `src/`, `bin/` (Node + Worker) | Moves every table of a file verbatim into an empty object and back, inside the platform limits, with exact storage classes. |

The tool never translates DDL. The object's schema is the staged file's
`sqlite_schema`, which the runtime's provisioning created. Statements the
platform refuses are reported by name and are not rewritten (brief, rule 7).

## Rules applied (docs/sqlite-storage.md, "Moving existing data")

| Table | Handling |
|---|---|
| Application tables, `members`, `_gonvex_action_outbox`, `_gonvex_reducer_idempotency`, `_gonvex_control_plane_membership_outbox`, `_gonvex_files`, `_gonvex_record_*`, `_gonvex_identity_keys`, `_gonvex_interned_text`, `_gonvex_id_domains`, `_gonvex_table_options`, `_gonvex_schema_capabilities` | Copied row for row, rowids included. |
| `gonvex_migrations`, `_gonvex_sync_bootstrap` | Not copied: provisioning writes them. |
| `_gonvex_sync_changes`, `_gonvex_sync_transactions` | Not copied: history does not carry over. |
| `_gonvex_sync_clock` | `revision` kept, `retained_revision = revision`, new `epoch` (`--keep-epoch` keeps it). Every client cursor falls outside the retained window, so clients resync by snapshot. |
| `_gonvex_reference_*` | Rebuilt from the catalog by `reference_integrity::declare`. |
| `sqlite_stat1`, `sqlite_stat4` | Not copied. Planner statistics; the runtime runs ANALYZE when it opens the file. |
| `sqlite_sequence` | Kept for copied tables. |
| FTS5 shadow tables | Rebuilt from their external content (`'rebuild'`). |

Lazily created runtime tables are created through the runtime's own code when the source has them:
`ensure_reducer_idempotency` for `_gonvex_reducer_idempotency`, and the `ensure_table`
statements from `rust/crates/runtime/src/storage.rs` for `_gonvex_files`.

Triggers: the source is already SQLite, so every row a trigger derived
(projections, search documents) is already in the source. The stage step
suspends all triggers for the copy and restores them verbatim in the same
transaction. The object creates triggers only after its rows are in. Either
way, copied rows fire nothing, and each copied table equals its source
exactly.

## Platform limits

| Limit | How the transport stays inside it |
|---|---|
| 100 bound parameters | Multi-row `INSERT`s are split so each binds at most 100 values (`buildInserts`). Tables have at most 100 columns. |
| 100 KB per statement | Values always travel as bound parameters. Only placeholders are inlined, so statement text stays a few KB. |
| 2 MB per row | Rows are copied as they are. A row over the limit fails loudly. |
| CPU / memory per request | About 1.5 MB of rows per insert request, about 4 MB per read response, 400 DDL statements per schema request, 60 tables per checksum request. One request is one transaction. |
| Foreign keys | `PRAGMA foreign_keys = OFF` during the copy (rows arrive table by table, as in `export-sqlite`), then `foreign_key_check`, then enforcement goes back on. |

Exact values across JavaScript: every read asks SQLite for `typeof(x)` and an
exact form. An integer travels as decimal text and is cast back. A REAL that is
integral or infinite carries a tag. TEXT is read as bytes, so invalid UTF-8
survives. The checksum hashes (storage class, exact bytes) per value, so it is
identical whichever SQLite computed it (`src/codec.mjs`).

## Use

```sh
pnpm install --ignore-workspace          # in this directory
cargo build --release --manifest-path stage/Cargo.toml   # gonvex-tenant-stage

# local workerd
echo "TRANSFER_TOKEN=$(openssl rand -hex 24)" > .dev.vars
pnpm dev --port 8799 &
node bin/tenant-transfer.mjs run --migrations ../whagons/migrations --work /tmp/p03 \
  --stage-bin stage/target/release/gonvex-tenant-stage tenant_*.db

# development account (gonvex-cf-dev-p03)
whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy.mjs deploy ~/p03.vars
node bin/tenant-transfer.mjs run --url https://gonvex-cf-dev-p03.<subdomain>.workers.dev \
  --token-file ~/p03.vars ...
whagons-dev credentials exec full-cloudflare-access -- node scripts/deploy.mjs delete
```

`run` does stage → wipe → push → verify → pull → stage → verify for each file,
writes a JSON report (`--report`), wipes the object (`--keep-object` to keep
it), and deletes its intermediate files (`--keep-files` to keep them). Other
commands: `push`, `pull`, `verify`, `checksum`, `wipe`.

Every run verifies:

- source vs staged, staged vs object, and source vs object, over every copied table;
- object vs pulled file, pulled vs exported, and source vs exported (the full round trip);
- the object's schema equals the staged file's, except for the statements the platform refused;
- object `quick_check` per table, `foreign_key_check`, and FTS5 `integrity-check`;
- the exported file passes `integrity_check` and `foreign_key_check`, and running the installer again changes nothing.

## Tests

```sh
pnpm test            # codec, statement builder, plans, keyset paging (node:sqlite)
pnpm test:workerd    # generated fixture through a real object in local workerd, plus the limit probes
```
