# Indexed provider checkpoint: local synthetic coverage

This is an intermediate backend checkpoint on `perf/permission-full-provider`,
from `155988263856f4e3d78ce376814c8cd4264adc94`. It does not complete the Whagons
migration, enable a network route, or qualify production data. The existing v2
path remains the default. The private primitive path requires the feature,
explicit local opt-in, isolated fixture file identity, and explicit registration.
The app branch and the immutable 1051 SDK/CLI pair have no changes here.

## Authority and transactions

The compiler reads the actual leased module, complete supported policy shape,
collection definition, args/result/dependency metadata and physical source DDL.
A sealed certificate cannot be constructed from a receipt boolean or version.
Fresh validation compares those inputs again and checks physical database
instance, tenant/project/epoch, schema, native capture and permission triggers,
complete sources, current index generation, change serial, and read revision.
A validated scan retains the same mutable `TenantTransaction` and module lease;
its `parts()` supplies that transaction to the existing indexed window visitor.
No permission-to-replica import was added.

Legacy normalized mutation/seal helpers reject compiled scopes; their completion
receipts cannot construct or repair this source authority. Incomplete generations
force a full source/principal rebuild and a reset barrier.

Source triggers invalidate readiness before an action can read an old index.
The store updates row roots, principal atoms, old/new delta journals and readiness
on the writer after native feed clock assignment and before COMMIT/publication.
An in-transaction primitive read while dirty selects normal v2 fallback, whose
SQL sees the uncommitted source mutation. Rollback restores the prior index and
readiness. Ineligible domains commit through the original v2 writer; they do not
become a new user authorization denial. Repair after an ineligible gap starts a
new index generation and requires a resnapshot rather than inventing old diffs.

The exact permission set contains relevant principal inputs rather than one
permission per owned row. Content and authorization source changes also advance
serial/readiness, so unchanged principal atoms alone do not authorize old bytes.
The new source scope is separate from complete collection/query identity.

## Counts

The pinned source metadata fixture comes from Whagons `814415ca0b060c7513eca3c7ab6db17fe15304ab`:
342 policies, 812 replica definitions, 368 source-table catalogs. Its physical
schema and rows are explicitly synthetic. It is not an export or production
schema/wire qualification.

| Coverage | Policies | Replica definitions |
| --- | ---: | ---: |
| Supported typed policy normalization | 191 | 427 |
| Actual definition/certificate compilation (two relay policies have no replica definition) | 189 | 427 |
| Ordered indexed SQL window oracle | included above | 184 |
| Omitted-order paths selecting normal v2 before primitive publication | included above | 243 |
| Remaining unsupported policy/source families | 151 | 385 |
| Production-copy qualified | 0 | 0 |

The 184 ordered definitions compare exact row order, projected JSON, canonical
hashes, filters, configured limits and `ReplicaWindow` with the actual existing
v2 SQL scanner on the same SQLite snapshot. These initial eight-row fixtures do
not exercise every configured truncation boundary. The 243 unordered paths are
**not** counted as indexed window passes: v2 has no implicit key ordering, and a
new join plan cannot choose a different truncated prefix.

Supported policies are public, persisted-member/account/tenant TEXT context
identity, relevant permission/role atoms, AND/OR/NOT, and explicit NULL/TEXT row
literals. NULL and blank text stay distinct; no trimming/coercion is introduced.
The actual definition matrix includes 61 ordered public paths, 20 ordered
self-context paths, 60 ordered permission paths, 1 ordered role path and 42
ordered Boolean combinations. Admission is limited to supported persisted active
members. Ephemeral/developer-without-row cases retain normal v2.

## Regressions and query work

The runtime/store lifecycle regression covers owner reassignment, dirty writer
reads, exact removed/added candidates, rollback, non-TEXT eligibility failure,
canonical repair, and an unchanged module label with a changed projection.
The permission-domain regression preserves the existing SQL/interpreted
truthiness disagreement for `2`, `"TRUE"`, and `1.0`; the new provider becomes
ineligible rather than choosing a new v2 meaning. Canonical boolean/integer/
string/null cases recover. Unrelated noncanonical permission keys do not poison
a policy that does not depend on them.

An independent Boolean evaluator checks 128 randomized changed-atom plus row-edge
diffs, including negative-only changes and overlapping support. Deep decision
construction uses explicit stacks. Node/apply and reverse-cover expansion budgets
reject publication; normalized rebuilds use a savepoint so expansion failure can
leave the v2 source write intact and the new index ineligible.

A synthetic shared-negative diff exposes a planner regression in the first
experimental candidate query, then checks the corrected indexed seeks:

| Synthetic row roots | Before join-order fix, VM instructions | After | Candidate rows |
| --- | ---: | ---: | ---: |
| 1,000 | 14,054 | 1,036 | 1 |
| 10,000 | 131,054 | 1,036 | 1 |

These are SQLite statement VM counts, not a v2 timing or capacity comparison.
Reverse dependencies include a positive-cover atom in their index. The query
starts from changed atoms and the member's old/new covering atoms; it does not
walk a shared negative leaf through every unrelated member's root. Admission
changes can correctly involve the full finite universe. Negative-only universe
buckets and more complex full-family costs still need separate cost coverage.

Local direct-exit evidence is under `/mnt/data/work/gonvex-full-provider/`:
`provider-source-authority` (412 runtime passed, 8 ignored; 49 store passed),
`provider-full-off` (397 runtime passed, 8 ignored; 44 store passed),
`provider-retained-tx`, and `decision-covered-diffs-seeks` (5 kernel tests passed).
All heavy starts used MemAvailable/IO PSI gates, actual `/mnt` headroom, resource
admission, and verified aggregate 6 GiB / 200% CPU scope limits. A first on-disk
fixture attempt was cancelled (direct Cargo exit 101/SIGINT) because it performed
one synchronous transaction per fact; its corrected fixture batches writes.
The intermediate VM-growth assertion also failed before the join-order fix;
both failures remain in the local evidence. The first feature-off filtered store
command selected zero tests and is not counted as kernel verification.

## Work still required

The remaining roots are 64 OR policies, 63 AND policies, 19 inSet policies and
5 inherits policies. Their full source DAGs include 210 distinct set definitions
(77 grant, 61 principal, 72 derived), 47 physical source tables, correlated aliases
and joins, strict flags/JSON literals, scoped role/default projections, branch
local PERSONAL subtraction, finite ancestry, missing/hidden/null parent behavior,
and existential via links. These remain normal v2 until their compiler,
occurrence/reverse-dependency maintenance and actual-definition oracles exist.
No renamed v2 callback or per-user full-list authorization loop substitutes for
those adapters. Full source/projection/dependency authority and fresh delivery
serials remain required at the eventual transport boundary.

The all-user production-copy gate is separately blocked by the REG006 binding
and timestamp contract. No raw restore/export was read by this lane. Synthetic
passes and normal v2 fallback cases do not satisfy that gate. Full mapped policy,
all-user differential, source write/disk/memory/query cost, and actual transport
integration acceptance remain unfinished.

Strict full-check qualification: the installed toolchain reports a deprecation
in unchanged `module-host/session.rs`, existing feature-only dead code, and
additional existing Clippy lints. `clippy-baseline*.json` records byte-identical
base/current blobs for every affected file (the authorized 14-line wrapper is
the only addition to `replica_permission.rs`). The changed code's unused import,
large enum variants and modulo lint were fixed. `provider-clippy-final` passed
with only the recorded baseline lint classes allowed on the command line; no
shared lint policy or baseline source was edited. Full `cargo fmt --check`
likewise found five byte-identical baseline files; changed-file formatting
passed. These qualified checks are not described as a clean strict workspace
Clippy/format pass.

The permission serial is not a broad whole-source shape-validation stamp. Root
watches currently cover all physical root columns, but principal watches are
selective. No separate sealed ShapeValidation proof is supplied here; consumers
must retain whole-root/source shape validation. Fresh SQL count/digest checks
and per-piece/final authority barriers are not elided.

The subsequent checkpoint also checks full loaded module schema metadata under
unchanged generation/artifact labels, incomplete principal backfill followed by
a row-only mutation, and rejection/rollback of legacy apply/register/seal calls
against a compiled scope. Terminal decision IDs are reserved by `Builder::default`;
invalid atom IDs and oversized decision/cover construction cannot publish.
