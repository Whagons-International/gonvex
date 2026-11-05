# Principal-atom semijoin experiment

**Rejected: no demonstrated SQL execution benefit.** Accepted `73a72db9` is
preserved. The candidate was committed separately as `a6e2fbd3`, tested, and
reverted by `aca96df4`. The final `permissions/compile.rs` is byte-identical to
73. Only explicitly enabled test profiling remains; production SQL, policy
identities and all authority checks are unchanged.

The candidate changed only `Builder::node`'s `SlotAny` principal relation from a
JOIN against `_gonvex_permission_set_atoms` to `atom_id IN (SELECT ...)`, binding
the same actual scope, generation and principal. It retained the finite universe,
DISTINCT-backed views, final DISTINCT, schema types and binary comparisons. No
raw support table replaced a view. `Expr`, `CompiledSet`, `HasAtom`, source/store
maintenance and the full-provider semantic DAG were untouched.

## Measurements

Same synthetic module as 73: 9,002 authorized rows, one hidden row, 400-byte extra
bodies, identical projection/order/window, ready source index, fresh Node/SDK/Memory
storage per operation. The first starter proof is cold; later proofs are warm.
Both cases use the same C42 SDK/64 KiB codec workspace. Release opt3/thin-LTO,
actual aggregate 6 GiB / 2 CPU lane. Other lanes make host timings noisy; this is
not a capacity, browser-performance or full application result.

| Primary module run | Server CPU, ms (cold / warm / warm) | Node CPU, ms | Wall, ms |
| --- | --- | --- | --- |
| Immutable 73 binary | 152.2 / 128.4 / 137.0 | See raw results | 677.8 / 695.0 / 682.2 |
| JOIN with shared profiling hooks disabled | 156.1 / 133.0 / 134.8 | 718.6 / 797.9 / 693.9 | 678.5 / 716.0 / 626.9 |
| Candidate semijoin, same hooks disabled | 150.7 / 128.2 / 118.0 | 755.9 / 727.7 / 681.9 | 702.5 / 664.3 / 600.0 |

Those whole-operation changes are not attributable to SQL: unchanged Node work
also varied substantially. Separate allocation-profile offer-SQL timings were
45.41 / 42.49 / 43.08 ms for JOIN versus 50.99 / 46.55 / 40.15 ms for semijoin.
The query plan changed materialization/IN structure (35 versus 42 plan detail
records), but actual sampled VM instructions remained approximately **1,378,000**
for **9,002** output rows in both cases.

A second probe alternated JOIN/semijoin order for 12 pairs inside the **same read
transaction**, using the actual complete SELECT, identical typed bindings,
scope/generation/principal, projection, ordering and LIMIT. It streamed rows and
hashed ordered projected bytes; it did not accumulate another table or authorize
any network payload. All 24 queries returned 9,002 rows / 4,012,987 JSON bytes and
the same ordered-output SHA256:
`0e9ae6e083353f5ae95310919396f43a40ae8029e6a40a8c587891e7fcab4d82`.

| Paired SQL execution | Median process CPU | Sampled VM work |
| --- | --- | --- |
| Accepted JOIN | 51.7055 ms | 1,377,000–1,378,000 instructions |
| Candidate semijoin | 51.7779 ms | 1,377,000–1,378,000 instructions |

A second 12-pair run after restoring production JOIN measured 55.1902 ms JOIN
versus 54.9919 ms semijoin (about 0.36% faster), with the same sampled VM work.
The first run was about 0.14% slower at the median; individual pairs were noisy
and there was no consistent warm advantage. This does not justify retaining the
query or requiring a policy reinstall. It does not establish behavior at other
cardinalities, which were not measured.

VM sampling uses SQLite's progress handler at 1,000-instruction resolution on a
separate actual SQLx execution in the same transaction. It does **not** replace
the production direct reader's cancellation handler. Primary CPU/allocation cases
do not enable either extra SELECT probe. The paired probe includes SQLx row
transport and streaming hashing, unlike the production bounded-batch reader;
its comparison is controlled, but its absolute timing is not a production-stage
latency claim.

Rust allocation requests were effectively unchanged: warm JOIN about 130,172–
130,212 requests / 27.638–27.640 MB, semijoin about 130,197–130,204 / 27.637–
27.639 MB. Counters include all Rust process threads and exclude SQLite C/Node.
Resident samples were about 96.5–97.0 MB, not peaks or evidence of an RSS win.
Encoded pieces remain 103,417 bytes, raw NDJSON 4,021,989 bytes; files about
138,192–138,194 bytes differ only in opaque authority metadata. JSON wire bytes
remain 146,045 versus normal 4,032,973; Node makes 9,034 crypto calls, and the SDK
stage peaks at 56,483,844 estimated bytes then returns to zero. None of these byte
savings is a new CPU improvement from this experiment.

## Correctness and identity

The candidate's two new oracles passed, along with 19 permission/SQL tests and the
25 starter regressions. They covered duplicate grant/row provenance, alternate
and cyclic group paths, last-support removal, transaction rollback, scope and
generation isolation, NULL principal/schema rejection, BLOB versus integer/text,
case and NFC/NFD distinctions, overlap and branch-local subtraction.

The accepted single-slot SQL policy hash was independently frozen as
`c9cc51b5900a3519992e5b0dda0aa6d73130be0a2ce4326e7024785fa46c4895`.
The changed compiler produced a different SQL-derived hash; an old ready binding
was rejected, a rolled-back reinstall could not become ready, and a committed
fresh scope/backfill restored access. Different source authority bytes retained
different policy identities. No old artifact proof was inherited under a false
unchanged version. These candidate tests are preserved in `a6e2fbd3`; reverting
that entire experiment also removes its migration-only tests from the final tree.

Existing forged checksummed wrong-key cache / pre-piece unchanged-atoms row-revoke
assertions and current SQL/v2 source-guard oracles were not weakened. Fresh offer
SQL COUNT+DIGEST, every piece's fresh principal/source/serial writer barrier and
final normal-v2 SQL/XOR anchor remain mandatory. B retains the full shape scan;
the selective permission serial is not a broad shape-validation proof.

## Remaining cost and next proposal

This experiment leaves the accepted cost diagnosis intact: fresh normalized offer
SQL roughly 43–47 ms, final normal-v2 SQL roughly 22–25 ms, full shape checks around
23–25 ms and fresh source checks around 7 ms, plus the other native/module guards.
Both normalized plans do essentially the same VM work; syntax alone does not
remove the expensive work.

The next proposed experiment is to **split and measure fresh authoritative schema
work** before changing it: attribute `source_schema`'s local/physical admission,
actual DDL/column reads and full key-shape SELECT separately, with statement/VM
counts and query plans. If metadata round trips dominate, evaluate one combined
actual-TX metadata read for schema/guard/readiness fields, comparing every exact
value to the current implementation. Preserve the full key scan, fresh native
checks, all source dependencies and both membership SQL proofs. Do not validate
memoized fields against themselves or treat the selective source serial as a
whole-table mutation token. No benefit or implementation of that proposal is
claimed here.

The original cheap-starter CPU/latency goal remains unmet. Full Whagons/provider
coverage, shipping overlay, browser performance and the 1,000-user gate remain
unqualified; no customer data, staging, integration, PR, CI or deployment was used.
Raw measurements, exact plans, instruction counts, artifact pins, resource
receipts and final checks are in
`/mnt/data/work/gonvex-starter-semijoin/results.json`.
