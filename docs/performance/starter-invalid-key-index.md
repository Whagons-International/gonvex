# Native partial invalid-key index

The opt-in synthetic-fixture candidate reduces warm starter Rust CPU by **15.5%**
at the median of two matched optimized runs, from 121.80 to 102.90 ms. Cold
immutable-proof factory cases fall from 141.53/146.42 to 120.82/118.66 ms. The
original cheap-starter goal remains unmet: normal cold load is about 52 ms.
This is an execution change, with every fresh authorization proof retained.

Index-only source: `35885d15366ff3301ef0cb5b2a0bc5cd4838988a`, descendant of
accepted `1098dbf2d386b2018ad3d0c2ece69c1b467ca141`. The index-only executable is
frozen separately before admission-guard composition. Raw results and receipts:
`/mnt/data/work/gonvex-starter-key-index/results.json`.

## Execution and authority

`GONVEX_STARTER_KEY_INDEX_LOCAL=1` enables this experiment only with the existing
`starter-files` feature and actual private local fixture admission. It installs
no index through normal opens, offers, pieces, commit requests, provisioning or
held read transactions. The sole runtime installer is the explicit local source
writer registration, before backfill/seal. Invalid existing rows are flagged by
SQLite; unsupported source registration still fails and rolls back. Invalid
writes are allowed, with no new constraint, coercion, repair or RAISE trigger.

The index contains only keys and rowids for rows satisfying the **exact existing
predicate**, including rows outside the current principal/filter scope:

```sql
typeof("id") <> 'text' OR "id"='' OR instr("id",char(0))<>0
```

Every admission still executes the corresponding fresh `SELECT EXISTS(...)` in
its own authoritative transaction. There are **20 semantic checks**, now **20
fresh index checks**, rather than 20 full root scans. No expression rewrite,
permission memo, broad mutation token, skipped scan result, or B token is used.
SQLite automatically maintains the partial index on inserts/deletes/key/type
changes and transaction rollback. An unrelated field cannot change this predicate.
This relies on the same native index consistency assumptions as other SQLite
indexes; it does not relax file integrity checks.

The private name uses a distinct `_gonvex_starter_invalid_key_v1_` prefix and a
128-bit routing suffix derived from physical device/inode, table/key, exact
predicate and version. The name/hash grants no authority. Each use compares exact
actual DDL and table/key binding, index origin, non-unique/partial flags and both
`index_xinfo` rows (named ascending BINARY key and auxiliary rowid). Unsupported
layouts, including unproved layouts of WITHOUT ROWID tables, cannot use the hint.
Table/key identifiers pass the existing quoting rules; private names stay within
the same 63-byte limit. A name collision with
weaker/different DDL is rejected; no `IF NOT EXISTS` trust or automatic destructive
repair is used. Metadata errors fail to normal v2; missing/tampered indexes use
the original predicate with `NOT INDEXED`, and the old indexed registration no
longer matches. There is no client-supplied certificate or public constructor.

The verified descriptor, exact table DDL and index root page participate in the
private projection identity. A separate private registration lifetime guard pins
actual `PRAGMA main.schema_version` **after** all registration DDL/backfill/seal.
This avoids a recursive scope identity as installing scope watches changes the
schema cookie. The cookie is read again from the actual transaction at admission;
it is neither provider policy/scope nor a shape capability. Any DDL conservatively
retires that registration, including drop/recreate with the same name/DDL/root
page. Rebinding is an explicit source writer operation that reapplies/reseals the
normalized state and changes its serial; DDL alone does not claim to change the
provider serial, and is rejected through this private lifetime guard; the legacy read-only registration cannot
mint this guard. Changing index binding changes projection identity. Old artifact
proofs are not silently inherited across changed authority or DDL lifetime.

Native physical/schema checks use direct `PRAGMA main.database_list` and
`PRAGMA main.schema_version`. Ordinary tables named `pragma_database_list` or
`pragma_schema_version` cannot substitute their contents. The former is compared
with the existing actual local-path admission before installing or accepting an
index. Provider native table/column/watch/source checks remain untouched. The
full-provider owner confirmed that arbitrary indexes/global cookie are not part
of its authority and reserved no competing root-shape DDL; no provider/store/source
interface was changed.

Fresh normalized **SQL COUNT+DIGEST before offer**, every piece's fresh native
principal/source/permission serial plus ordered writer enqueue, and final normal-v2
SQL/XOR anchor are unchanged. A row moving out of scope with unchanged principal
atoms still denies old pieces before payload. Index validity is never row
permission authority, and a raw piece hash never grants GET access.

## Optimized measurements

Same synthetic module as 1098: 9,002 authorized tasks, one hidden task, 400-byte
extra bodies, exact projection/order/budgets, ready normalized source, same
C42/64 KiB workspace SDK. Warm server/OS, fresh Node SDK and Memory storage for
every operation; repeat 0 has a cold immutable artifact proof, repeats 1/2 have
warm proofs. Release opt3/thin LTO, aggregate 6 GiB / 200% CPU verified per unit,
memory/both I/O/headroom gates retained. Other lanes make host timings noisy;
none of this establishes one-vCPU capacity, browser performance or a latency SLO.

Primary runs disable allocation and schema tracing. Two complete run pairs:

| Index-only comparison | Rust CPU ms, cold / warm / warm | Node-observed wall ms, cold / warm / warm |
| --- | --- | --- |
| Accepted 1098, run 1 | 141.53 / 121.39 / 122.74 | 662.3 / 637.1 / 632.0 |
| Native index, run 1 | 120.82 / 102.66 / 103.14 | 625.1 / 606.4 / 613.6 |
| Accepted 1098, run 2 | 146.42 / 122.21 / 116.05 | 717.2 / 651.7 / 615.9 |
| Native index, run 2 | 118.66 / 104.57 / 100.83 | 613.9 / 628.2 / 615.9 |

The four warm Rust samples have medians 121.80 versus 102.90 ms. Wall time remains
noisy and one warm pair is equal; no guaranteed wall improvement is claimed.
Normal cold Rust CPU is 52.05/53.16 ms versus candidate 49–52 ms, substantially
below starter CPU. Node still hashes/hydrates all authorized rows; no SDK CPU
optimization is attributed to the index.

The exact native trace has zero accounting errors. For all 20 semantic checks:

| Work | Accepted full covering-PK path | Verified empty partial-index path |
| --- | --- | --- |
| Key-predicate VM instructions | 1,800,940 | 240 |
| Root FULLSCAN_STEP counter | 180,040 | 0 |
| Key-predicate scalar output rows | 20 | 20 |
| Additional index metadata | none | 40 top-level statements / 8,940 outer VM instructions |

The additional metadata includes direct physical PRAGMA and exact catalog/layout
verification. Its roughly 1.86–1.87 ms wall / fresh scans of sqlite_schema are
included in the full module results. Twenty additional fresh lifetime-cookie
PRAGMA reads are included in `authority.sources` and the module totals.
The metadata reports 1,940 FULLSCAN_STEPs
against the small schema relation, **not zero full scans everywhere**. Separate
actual-TX plans show the original covering primary-key index versus the private
covering partial index. Twelve alternating same-TX pairs, 20 checks each, measure
median process CPU 17.96 ms for forced original covering-PK execution versus
0.277 ms for verified-index lookups; this kernel excludes metadata admission,
which the module benchmark includes. Forcing the original PK after installation
prevents SQLite automatically selecting the new index for the control. Predicate
bytes and bool/error behavior are unchanged.

A separate negative lookup has 9,003 valid TEXT keys followed in PK order by
one invalid BLOB key (9,004 physical rows). Its native partial index has one
4,096-byte page/one cell. Twelve alternating pairs of 20 fresh reject checks
measure median CPU **18.276 ms → 0.303 ms**, VM **1,801,040 → 280**, and root
FULLSCAN_STEP **180,060 → 0**. Both predicates return true on every call; this
is a measured rejection, not an empty-index-only assumption. Trace errors are
zero in both controls.

The independent normalized membership query is byte-identical, its query plan
identical, and both executions return 9,002 rows with 1,377,000 sampled VM
instructions (1,000-instruction resolution). This extra SELECT is a separate
probe, excluded from primary/allocation timings; it does not replace the direct
worker's cancellation handler. No SQL proof was removed to get the improvement.

## Build, writes, disk and memory

Initial native index installation on the isolated 9,003-row/400-byte-body fixture
costs about 1.82 ms process CPU / 1.85 ms wall, including explicit installation
checks. The empty index is **one 4,096-byte page, zero cells**. It contains no good
row data. With all 9,003 keys made BLOBs, it is 68 pages / 278,528 bytes / 9,003
cells. Repair leaves one logical index page; freed pages can remain in the DB's
freelist/file until normal maintenance. These are dbstat page bytes, not JS/RSS.

Repeated six alternating native write pairs measure bulk updates of 9,003 rows
with/without this index under savepoints. No permission/capture triggers are in
this isolated write kernel; it measures incremental index cost, not total
application mutation latency. Key changes incur extra CPU. The raw result gives
medians for unchanged body, valid key retarget, TEXT→BLOB and BLOB→TEXT repair.

A separate actual-commit experiment starts equal private databases, truncates
initial WAL and disables autocheckpoint only for those fixtures. Actual WAL growth:

| Committed 9,003-row mutation | Without index | With index |
| --- | --- | --- |
| Unchanged body | 0 | 0 |
| Valid key retarget | 4,396,072 B | 4,396,072 B |
| Keys become BLOB | 4,412,520 B | 4,692,680 B |
| Keys repair to TEXT | 4,404,280 B | 4,692,680 B |

The initial committed DB grows by exactly 4,096 B with the empty index. Actual
commit CPU and wall samples are in the results; these single committed pairs
qualify the WAL measurement, not a stable fsync latency claim. Writes remain
transactional and invalid keys remain permitted. The partial index is not free
for write-heavy workloads.

Warm requested Rust allocations rise from roughly 130k / 27.63 MB to 132k /
27.83 MB due to fresh metadata/identity work. These counters exclude SQLite C and
Node; the index does not reduce allocations. Resident samples are roughly
96–106 MB, not peaks or evidence of an RSS reduction. Encoded pieces remain
103,417 B, raw NDJSON 4,021,989 B, wire 146,045 B versus normal 4,032,973 B;
artifact files remain about 138.2 kB (approximately 138,200 B).
The SDK stage estimator remains 56,483,844 B peak, releases to zero, and is not an
actual JS/RSS cap. Existing aggregate 64 MiB/max4 and codec workspace rules remain.

## Correctness and admission composition

Native oracle tests compare the original full predicate against fresh indexed
execution for initial bad rows, NULL/BLOB/empty/embedded-NUL/Unicode/malformed TEXT,
SQLite numeric TEXT affinity, hidden/outside-scope keys, same-length key changes,
valid→invalid→valid transitions, delete/retarget and rollback. Missing, weaker,
same-name/same-DDL recreated indexes and actual table DDL changes are tested.
Native WAL readers keep their old consistent snapshot, new readers see committed
invalidity; native backup/copy has consistent contents but a different physical
identity and cannot use the old index name as authority. Quoted reserved names
and missing-table error agreement are checked. Shadow metadata tables are tested.

Live fixture tests retire old offers/pieces before any payload after drop, weaker
replacement, same-DDL recreation or ALTER, permit explicit safe rebinds, and reject
same-length hidden BLOB keys before payload. Existing forged checksummed wrong-key
cache/preoffer-zero, unchanged-atoms row-scope revoke/prepiece-zero, corruption,
source serial and final normal-v2 XOR assertions are unchanged and pass with the
index enabled. Index-only runtime: **443 ON / 399 OFF**, 27 indexed-starter tests,
plus live SDK old-client/bootstrap/catchup/resume/retention/old-server/revoke cases.

After freezing that source/executable, only admission commit `39ce591e` was
consumed. Its isolated `1c94c8ac` ancestry was not applied: this branch already has
identical future fix `bc526fbd`. Generated conflicts were resolved by rebuilding
this branch's unchanged SDK/CLI source in a private canonical source mirror and
rerunning generators; no generated body was hand-merged. Dashboard artifact
`cab57fe878f4...` is reproduced; this branch's stale template bundle also refreshes
from its existing dueWhen source. These are generator inputs/outputs, never a
shipping SDK/PG compatibility/defer overlay replacement.

One composition issue was found and fixed: the admission snapshot probe and
starter tests each declared a global allocator. The snapshot allocator is now
selected only for perf-fixture without starter-files. Combined tests retain the
original starter System counting allocator; unsupported snapshot-probe allocation
fields print `None`, rather than false zero. Standalone admission allocation
measurements and production admission code are unchanged. The admission descendant is `eeed71e3`; the final test-only negative-kernel
checkpoint is `ac674b6a`. Combined checks pass **454 ON / 406 OFF**, **11 profiler**
tests, six SQL/cache/resume oracles, actual live Node bootstrap/resume/revoke
cases, **31 SDK / 18 thin-client** tests, docs metadata and formatting. Cycles
remain 2 → 2. The final combined primary measures cold **128.25 ms**, warm
**101.06/100.66 ms** Rust CPU versus normal cold **52.92 ms**; it is reported
separately from the causal index-only comparison. These numbers preserve the
remaining goal gap. Final narrow probes confirm unsupported allocation fields
are None, while standalone supported numeric output stays unchanged. Exact
descendant/artifact pins and all selected exit0 receipts are in the evidence file.

Full Whagons coverage, all browsers, real new-device/1,000-user performance and
shipping activation remain unqualified. No real data, integration, CI, PR,
staging or deployment was used.

## Remaining algorithmic cost

After this change, warm fresh offer SQL still costs about 42.6–42.9 ms and final
normal-v2 SQL about 21.3–22.1 ms, streaming 18,004 projected rows across both
mandatory proofs; cold artifact format validation adds about 18.2 ms. Other
native/source/module/serial checks remain. This explains why reducing shape
lookup work does not make starter cheaper than normal cold load.

A possible next execution experiment is a same-TX native canonical-row
COUNT+XOR aggregate over the **exact existing normalized projected SELECT**,
avoiding repeated transport/parsing of about 4 MB of JSON solely for equality
proofs. Every row/type/budget/error and exact hash/collision mode must still be
checked freshly. It needs a concrete registration/authority interface review and
byte-for-byte canonical SQL/hash oracles before implementation. No persisted or
client digest could authorize it. No equivalence or benefit is claimed here; no
such aggregate, proof memo or SQL elision has been implemented.
