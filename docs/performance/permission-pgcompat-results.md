# Permission stage 3: PostgreSQL schema and artifact compatibility

This task branch keeps v2 serving unchanged. The primitive path remains explicit,
local and default off. PostgreSQL codegen success is not qualification of a
converted SQLite module, a live feed, or the full Whagons permission mapping.

`e7624725d37d0c478b0b2bde1c894e5edca3aeeb` adds an explicit
`module.schemaDialect` (`sqlite` by default, `postgres` opt-in). PostgreSQL
migrations execute unchanged in PGlite 0.5.8, including real pgcrypto and pg_trgm.
Catalog introspection retains declared PG types, nullability, defaults and
ordered primary-key metadata. Composite keys are not silently reduced to one
column; automatic local collection synthesis rejects them. The original Whagons
baseline and all subsequent tenant migrations compile: 368 tables, 4,338 columns.
No application migration was rewritten, skipped after error, or installed on a
server. SQLite continues to use SQLite itself and a distinct schema cache key.

The earlier SDK artifacts are immutable. The whole SDK built from this SQLite
branch is unsuitable for a PostgreSQL application: its data/write helpers emit
SQLite SQL. Earlier app generator/typecheck successes using that SDK are
qualified and superseded for SQL semantic compatibility.

The new local extension is `0.5.2-permission-pgcompat.202611051051`, tar SHA256
`96936808cb60c7f4dda1355677ec96b9ecbbd92338a0f6f8f94caeee03f04fb5`.
Its baseline is the exact published `@gonvex/module-sdk@0.5.2-staging.31` tarball,
verified against the frozen app lock's SHA512 integrity. The reproducible
composer is `packages/gonvex/scripts/permission-pgcompat-artifact.py`.
The only changed existing files are:

- `package.json`: version value only; exports, dependencies and every other
  parsed manifest field remain equal to the published baseline.
- `dist/index.js`: the original bytes followed by a newline and one literal
  `permissionPrimitives` export.
- `dist/index.d.ts`: the original bytes followed by the runtime helper export
  and its type-only exports.

All other baseline dist, data/data-write, runtime-contract, README and map files
remain byte-identical. The compiled `permission-primitives.js/.d.ts` and available
maps come from the frozen e762 SDK build, not a callback/stub. The helper has no
imports or additional runtime dependencies. The receipt records exact before and
after hashes and appended text; index files are intentionally not described as
unchanged. The CLI is an explicit external entrypoint copied byte-for-byte from
the frozen e762 build. The app's installed patched staging.31 CLI identity and
all other lockfile dependency pins are preserved.

Private local artifact receipt:
`/mnt/data/work/gonvex-cli-schema/artifacts-pgcompat2/pairing.json`.
Invoke its `cliEntrypoint` explicitly; do not alias the installed CLI. The old
bd8720 pair retains its original dirty-build-then-matching-freeze provenance.
The e762 pair was built after freezing e762; the new extension composition does
not rewrite either old pair.

Verification on 2026-11-05, with direct command exits and asserted restrictive
6 GiB/200% CPU scopes, MemAvailable/IO PSI admission and actual /mnt headroom:

- Full CLI suite: 76 passed, zero failures, one external-artifact test skipped
  in the ordinary suite. That external-artifact PG test passed separately.
  All six dual-dialect tests passed, including the actual Whagons PG baseline.
- The external test compiles actual portable reducer bodies with both the
  original staging.31 CLI/SDK and the new CLI/extended PG SDK, then executes
  them on embedded PostgreSQL. SQL text, parameters, selected rows and mutation
  results match exactly: membership `ANY`, empty sets, JSONB containment,
  transformed/regex/null filters, JSONB aggregate reads, insert/update batches,
  predicate update and delete. It also verifies preserved package closure and
  exact export appends.
- A fresh source fixture from Whagons `814415ca0b060c7513eca3c7ab6db17fe15304ab`
  was regenerated with the original frozen staging.31 CLI/SDK. Actual generated
  `notifications.markAllRead` and `clearAllNotifications` were executed on PG
  against synthetic rows and compared with the new paired generation. SQL,
  parameters, results and final rows match; other-member/deleted rows stay
  unchanged. An older saved generated directory was discovered to contain
  SQLite helper SQL and was rejected as a PG baseline, not waived.
- The app was freshly regenerated with the new pair: 1,993 functions. All 342
  v2 policies and all 812 manifest replica definitions match the fresh original
  baseline exactly. This is metadata preservation, not all-user row parity.
- App mapping tests, declaration types, v2 types, 20 self-owner/v2 compatibility
  cases and architecture checks passed. Required app generators (Gonvex,
  agent-api, client contract, i18n, icons, mobile bindings, assistant catalog and
  compatibility inventory) were run from source. The two inventory regression
  cases retain exact nested source/negative/join/null paths and unordered gaps.
- Framework dashboard/template generators and docs typecheck passed. Runtime
  import cycles remain two cyclic edges against e5812dfb. App runtime cycles
  remain 62 SCCs against 814415ca0, with no new cyclic runtime edges.
- Rust source is unchanged by this compatibility commit. The earlier 84970d56
  stage-3 feature-on/off runtime and store checks remain applicable; no new Rust
  pass or performance measurement is inferred from these JavaScript checks.

Full policy coverage remains incomplete. The app inventory now retains every
exact v2 plan, each unsupported node's JSON path and input, full source-set DAG,
permission table declarations, every actual collection definition, and required
projection/order/filter/budget/authority oracles. No production route is marked
ready. Twenty self-owner table candidates have source preparation; this is not
40 serving routes. The 58 pure-public roots are a bounded next candidate only
with persisted-active-member admission parity; anonymous/developer/ephemeral
contexts remain explicitly unsupported. Correlated joins, scoped roles/defaults,
typed JSON flags, finite-depth parent/via rules and PERSONAL branch-local
subtraction still require executable normalizers and actual v2 row/message
oracles. Omitted ordering is not replaced with `_id` ordering. The full local
production-data differential is pending complete source/binder coverage and the
separate read-only dataset qualification; synthetic matches do not substitute.

Authority remains full signed policy/provider/source/dependency/projection plus
physical project/database/tenant/epoch, readiness, exact canonical admitted set,
index generation and permission-change serial. Every indexed row or principal
source mutation invalidates old authority conservatively. Piece retrieval and
manifest admission must check the same current authority before disclosure;
content hashes or unchanged member atoms alone are insufficient.

The superseded 1025 extension also appended a trailing newline to package.json.
The 1051 extension preserves even the original missing final newline: its
manifest bytes equal the published file with only the version token replaced.
The earlier 1025 tarball/receipt remain immutable; its SQL comparisons remain
valid but its strict manifest-byte qualification is superseded.
