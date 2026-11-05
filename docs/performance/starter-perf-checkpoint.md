# Starter performance checkpoint

The guarded starter is faster than accepted `3a984045`, but still costs more CPU
and time than a normal cold open on this local synthetic fixture. It saves wire
bytes. This is a default-off feature branch, with no integration, activation,
shipping-overlay composition, full Whagons qualification or 1,000-user result.

The branch preserves `3a984045` and contains separate changes:

| Change | Commit | Result |
| --- | --- | --- |
| Fixed profiling counters | `6fab6664` | Separate primary CPU runs from allocation/counter runs. |
| Bounded normalized SQL batches | `b42414c0` | Same SQL, binding types, transaction and window collector; binary bindings retain the old SQLx path. |
| A: private validated encoded artifact | `3c78a683` | Copied immutable encoded Arcs, exact context/identity checks, real-FD stamps, bounded singleflight/cache and retained-byte charges. Every offer still scans fresh SQL. |
| C: private stage hash reuse | `c5ca685f` | Frozen owned rows/offer, post-await mutation/owner checks, invalidation on every data delta; unchanged watermark alone can reuse hashes. |
| Fixed codec workspace | `42b872ca` | Another 64 KiB per pending stage, including tiny/empty stages; charges remain until asynchronous work settles. |
| Resident future bounds | `bc526fbd` | Three nested feed/live-query awaits boxed; websocket future 32,128 → 25,600 bytes, below the unchanged 28,672-byte optimized limit. |
| Browser runner hook | `b7d14a66` | Ignored-test runner override; existing real fixture, certificates, mutations and SQL/XOR oracles stay unchanged. |
| Optional browser decoder | `94bd1c15` (from `269dfbab`) | Structural DEFLATE output preflight before native allocation; separate opt-in export. |

B is **not implemented**. The compiled provider permission serial is selective,
and no separate broad shape-validation proof was supplied. All 20 full shape
checks remain in each measured starter operation. Nothing trusts a version string,
checksum, client digest, content hash, stable member atoms or cached authority in
place of the existing current authority/SQL checks.

## Matched optimized module measurements

Release opt3/thin-LTO, one 9,002-row visible window plus one hidden row, 400-byte
extra descriptions, two active native members sharing a team. Each operation
starts a fresh Node process, SDK and Memory storage. Both paths use the same
prepared source/module/index and warm OS. The first normal operation has a cold
server row-result cache; later normal operations have a warm cache. The first
starter proof factory is cold; subsequent encoded proofs are warm. This is one
collection, not the 78-client fixture or a full application load.

Primary runs disable allocation capture. Values below retain all three repeats;
separate allocation runs contain leaf timings and counters. Host activity and
cgroup scheduling make wall timings noisy; these are not 1-vCPU capacity figures.

| Starter case | Server process CPU, ms (cold / warm / warm) | Node CPU, ms | Wall, ms |
| --- | --- | --- | --- |
| Instrumented baseline, matched to exact optimized 3a | 173.0 / 173.0 / 174.4 | 875–903 in exact 3a run | 841.7 / 847.5 / 845.2 |
| Bounded batches | 148.7 / 146.4 / 146.5 | Same frozen SDK | 837.2 / 838.2 / 828.1 |
| A + batches, frozen original SDK | 138.5 / 122.8 / 120.6 | 872.1 / 856.3 / 858.5 | 828.7 / 799.8 / 803.5 |
| A + batches + C + fixed workspace | 147.5 / 120.7 / 121.8 | 696.3 / 698.2 / 696.1 | 658.6 / 634.8 / 631.6 |
| Normal open in the final matched run | 52.9 / 11.7 / 11.8 | 645.3 / 677.5 / 643.3 | 506.6 / 477.2 / 468.3 |

C changes actual WebCrypto calls from 18,036 to 9,034: 9,002 row hashes and 32
encoded/raw piece hashes remain. The normal client also makes 9,002 row-hash
calls. C's private memo is bound to one immutable stage/offer and hash format;
any data mutation forces a full recomputation. The server's authoritative final
digest and normal SQL/XOR anchor remain mandatory.

A cold proof reads 103,417 encoded bytes, versus 517,085 before A. Warm proof reuse
reads no new encoded bytes; current manifest/piece pathname metadata still must
match the actual validated read FDs. Removed/replaced/changed files deny retrieval.
New bytes never inherit a proof from stat metadata. The same-length, restored-mtime
mutation test verifies ctime invalidation and immutable old Arc bytes.

Application JSON wire bytes are about 146,045 with a starter versus 4,032,973 for
normal load. The uncompressed rows occupy 4,021,989 bytes, encoded pieces 103,417,
and complete files about 138,190–138,197 bytes including the authority-bearing
manifest. Background materialization wall time in the final allocation runs was
104.6–109.4 ms; it is not an isolated disk-write CPU/IO measurement. Earlier host
outliers included 1,222 ms operation wall time and 3,622 ms build wall time; no
build-speed or throughput claim follows from these timings.

Rust allocation requests fall from about 294,000 / 63.8 MB requested to 260,000 /
52.7 MB for a cold A factory and 130,000 / 27.6 MB for a warm proof. These counts
include all Rust test-process threads and exclude SQLite C and Node. Rust resident
samples after six operations were about 96–104 MB, not per-offer peaks. Node peak
RSS was about 163–168 MiB for these runs and includes its runtime/modules; there is
no demonstrated total RSS reduction.

The process artifact budget reserves before retaining encoded buffers, metadata,
factory scratch and outbound envelopes. Cold peak charge is about 15 MB, warm
retained proof 1.44 MB and warm proof plus offer peak 2.51 MB, within 64 MiB. The
SDK peak estimate is 56,483,844 bytes, including hash results, pending work and the
64 KiB workspace, and returns to zero. Estimates are not engine RSS hard limits.

## Remaining cost and next proposal

The final warm profile retains 18,004 SQL output rows: all 9,002 for the fresh
normalized count/digest before the offer and all 9,002 for the final normal-v2
anchor. Its full-shape scan count is 20. The current profile attributes roughly
47 ms to offer SQL, 22 ms to final SQL, 25 ms to shape checks and another 7 ms to
source authority checks. Canonical row handling already uses the existing raw-row
fast path: about 19 ms over both scans, with no parsed-row fallback in this fixture.

The saved query plan still materializes DISTINCT-backed normalized universe,
row-atom and principal-set views, then sorts the projected root rows. The next
concrete experiment is a **SQL execution change**, not a permission-proof memo:
compare the existing principal-atom JOIN against an atom-id semijoin bound to the
same scope/generation/principal, retaining the explicit finite-universe relation,
exact SQL count/digest, normal window collector and final v2 proof. Measure actual
plan/row work and CPU before keeping it. Preserve negative/overlap semantics and
SQL-derived policy-hash invalidation/reinstall; run the SQL/v2 rollback oracles and
both forged-file/revoke regressions unchanged. This experiment is not implemented
or claimed to be beneficial. No fresh SQL membership proof may be removed.

A later B proposal requires an independently minted, freshly checked broad
mutation proof covering every physical source/root mutation, schema/guard changes,
readiness/generation, rollback and copy/restore. The current provider serial is
not that proof. Full-provider integration remains owned by its separate lane.

## Verification and browser composition

Optimized runtime: **435 pass / 0 fail / 11 ignored** with starter enabled;
**399 / 0 / 8** off. Combined focused SDK: **60 pass**. Live Node websocket test
covers old-client, bootstrap, feature-off old-wire server and revoke-before-piece,
including retention bootstrap, final catchup and subsequent normal XOR resume with
zero base-row evaluation. Existing forged checksummed wrong-key cache and
unchanged-atoms row-scope revoke assertions were retained. Added tests cover
strict duplicate JSON keys, FD replacement/mutation, budget/last-owner release,
singleflight contention, eviction, warm fresh-SQL scans and asynchronous memo races.

The optional browser source was cherry-picked onto C42 with its 64 KiB workspace.
Its compiled stage and decoder SHA256 exactly match the browser owner's tested
composition. That composition passed 167 Node/real-Rust-encoder cases and 607
assertions in native Chrome 156; the real Rust fixture and durable
`IndexedDBEngineReplicaStorage` passed bootstrap, retention, XOR resume and
revoke-before-piece. The owner also ran 1,109 composed SDK tests plus two codec
tests. This branch reran 60 affected tests and the 167-case/607-assertion Node/Rust
oracle after composition. No duplicate browser session was started.

`IndexedDBLocalReplicaStorage` lacks atomic promotion and must keep starter
capability off. Only the existing guarded Engine storage is qualified here.
Native decoder availability is probed; no import activates the feature. No new
Node import, dependency or wire format was added. Other browser engines, browser
performance, shipping overlay, full Whagons/source coverage and the 1,000-user gate
remain unqualified.

Dashboard/template codegen, frozen lock verification and docs metadata/typecheck
passed. Runtime import cycles remain **2 → 2**, with no additions. Generator
reproduction uses the preserved CLI/resolved SDK; no CLI/module-SDK source or
shipping artifact was changed. No customer dataset was opened; access remains
false. No PR, CI run, deployment, integration merge or production activation.

Machine-readable measurements, artifact hashes, gate receipts and ownership:
`/mnt/data/work/gonvex-starter-perf/results.json`. Browser-owner evidence is
`/mnt/data/work/gonvex-starter-browser/capability-manifest.json`. The optimized
wrapper-enabled Rust test executable is pinned by SHA256
`bd490fdcdc6ce3bd8c9bdb5f859e4b72158ee6adcd4328fdebeb76ee02a858de`;
its exact Rust tree is `03c5d39a9ec2f8226c2cdfc9e9bffed72322d227`.
