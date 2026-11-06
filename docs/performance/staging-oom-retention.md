# Staging cold-open and retained-result fixes

This follow-up starts at staging merge `512e61186d7c4abda40ceca329136e10761f6307`.
It preserves that base's opening-cohort certification, native receipt fixtures,
historical SQLite verification and serialized WAL-cap fallback. The runtime
candidate was compiled at `3be7b65a81158d2058372d7845610e42f6c5e5ff`; later
documentation, regenerated manifest timestamps and the test-only assertion of
the retained fixture payload length do not change its production runtime code.

## Separate changes and overlap

| Change | Original source | Staging commit |
| --- | --- | --- |
| Bound nested feed futures | `bc526fbd` | `fb28b916` |
| Reject excess cold opens before cache single-flight | `39ce591e` | `1045d541` |
| Release completed results and their Reading owner on cancellation | `d0ed7a55` | `59940571` |
| Keep ownership measurements explicitly opt-in | `1e536760` | `16e14687` |
| Share validated principal payloads with COW mutation | `6ed873ff` | `3be7b65a` |

The base already boxes the quiet Live Query SQL rerun, with a newer relevance
comment. That file stays byte-identical to staging; only the remaining feed
boundaries are added. Generated conflicts were resolved by running codegen on
the merged source, using the projects configured in `gonvex.json`.

The principal patch shares permissions, direct bindings and sets from the
existing validated Core. Cache keys, invalidation, authority checks, SQL and
mask rules are unchanged. Mutation and override paths use `Arc::make_mut`.
Tests cover sibling isolation and actual cache eviction followed by last-lease
drop. The absent optional starter's constructor test file is not introduced.
Its adaptation belongs with that feature's later follow-up.

Optional timestamp `ec00c7fe` and finished default-off starter/key-index `6cbf3554`
remain preserved on `perf/integration-batch-20261105@1421c5f1` and
`perf/queue-backpressure-20261105@6ed873ff`. They are outside this focused patch.
No full-provider or heap implementation WIP is included.

## Measured causes and limits

The earlier 1421-derived ownership probe reproduced **516 snapshot waiters** as
258 cold preparations, 129 ordinary collection queries and 129 resume
candidates. The cold guard bounds cold preparations; it does not count every
path waiting for snapshot admission. Cancellation recovered all permits and a
validated resume still recovered without a new snapshot. Request/future
allocations in this specimen contained no snapshot rows. The old full run's
1.018 million refusals count repeated rejected attempts, not retained results;
the existing client retry wire contract and pacing remain unchanged.

The queue has separate request/prefix bookkeeping, offloaded work, completed
results and socket delivery ownership. The measured defect was a completed
result retained by its executor completion after logical close, until socket
polling resumed. The new cancellation owner drops that payload and Reading
state immediately, including racing completion and replacement. It does not
discard row deltas, ready messages or cursors, increase a cap, or enqueue cold
work behind a new semaphore. Other existing request, snapshot and outbound
bounds remain unchanged.

On the earlier matched synthetic queue specimen, seven real-shaped results
retained about 2.1 MB after logical close before the fix; after the fix, no
result payload remained and requested live bytes were about 31 KB. The new
staging probe independently observes seven results fall to zero, Reading turn
false, and 1,977,467 requested bytes fall to 32,564 without polling completion
(28,000 after drain). Its 21.90 ms close observation includes a 20 ms grace;
26.84 ms is process CPU, not a delivery SLO measurement.

The separate 1421-derived principal pair measured sampled Rust live memory at
50 ready users as 336.36 -> 289.47 MiB, and after close as 275.21 -> 272.42 MiB.
The `Shared::resolved` allocation source fell from 34,881,855 to 393,312 bytes.
Role/fingerprint strings remain per resolution. These are allocator samples,
not RSS or cgroup totals; SQLite C allocations, V8 and file charges differ.
Sampled VmHWM was higher after that pair. Large warmed snapshot, parent ACL and
module owners remain. This is neither attribution of the entire OOM to queues
nor a new before/after performance claim for staging 512.

## Qualification on the new staging base

The new runtime binary SHA256 is
`63be2478b21d946ea7918c7265f5ff51c5fba25c14f5bd7a59d3164441f548e6`.
The newly built module host is
`5e5898f3ffd326ed12210a130bf43ef55ba8ac1dd91bf25bc353f9879c7d0c5f`.
The main binary uses the existing `local-load` profile with the synthetic auth
emulator feature. Library/oracle tests use release with LTO disabled, recorded
separately from performance binaries. The 28,672-byte optimized future budget
is unchanged and passes.

All workloads used an actual 6 GiB / 2 CPU aggregate cgroup. Measured runtime
children used 1 CPU / 2 GiB with affinity 7,15, SQLite cache 8192 KiB and host
concurrency 32. Fresh clones came from the original historical synthetic lab.
The short Linux specimen reached 50 users, all 6,600 subscriptions ready and
431,440 rows. Close/drain returned to zero subscriptions; a warmed-server-cache
25-user pass reached 3,300 subscriptions and 226,317 rows and closed to zero.
There were no driver errors or OOM events. Sampled main-runtime VmHWM peaked
at 1,328,939,008 bytes; the runtime-child cgroup peak was 1,237,540,864 bytes and
its whole-case CPU was 18.91 seconds. Those are different memory quantities.
Sampled rolling profiler peaks were 199 snapshot waiters, 200 cold opens,
49 queued offload tasks and two active snapshots.

The corrected harness `15cb6ca120f8f540a9f5ab7ff78046643a031392` and frozen shipping
SDK entry `60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a`
produced 1,056 qualified row comparisons over all 132 windows: cold and cached
SDK/thin opens for two principals, then fresh versus warm SDK/thin after a real
share and revoke. Principal, authority, window, cursor and complete row digest
must match. Grant took 1,936 ms and revoke 1,683 ms; this does not pass a one-second
permission SLO. The frozen client payload map remains
`392387f207e602ecf09bc5c3dff4fce9fd7a385bf62a5239676cdb4bbe71ae33`.

Validation includes default/fixture runtime libraries (411/416 passed), store
44, profiler 11, protocol 23, 24 integration suites / 41 tests, and the actual
native host with all 411 runtime tests and 18 receipt tests. The strict native
200-member legacy/v2 fan-out pair passes with no skipped test (227.77 seconds).
Process crash recovery, authenticated SDK/storage composition, browser rollback
and recovery, account/tenant switches, contract replacement and their mandatory
negative controls pass. Focused SQL probes compare 228 resolutions at each of
12 mutations (2,736), exercise equal-clock races, and match all 2,823 raw row
hashes. No runtime import cycle was added (two existing cyclic edges -> two).
Package types, 1,059 base-client tests, module SDK and CLI contract tests, the
corrected harness's 59 Node / five Python tests, both projects' codegen and
agent API output, and docs metadata/typecheck pass.

## Evidence and retained failures

The complete local report, source/patch-id receipts, exact scripts, source pins,
caps, SQL counts, profiler samples and raw outcomes are under
`/mnt/data/work/gonvex-staging-followups-20261105`.
The sealed earlier before/after evidence remains under
`/mnt/data/work/gonvex-queue-backpressure-20261105`.

Interrupted native diagnostics were retained: the first slow native fan-out
observation was not enough to establish a regression; the completion run above
passed. Setup failures for missing private dependency links, an overlong tsx
IPC path and an absent private browser cache were corrected without changing
production code or assertions. An interrupted partial fixture was preserved;
fresh-case wrappers now refuse an existing output directory. The initial SQL
count run omitted TMPDIR and created temporary synthetic fixtures outside /mnt;
the corrected owned-/mnt run is the reported count proof. Original logs remain.

The original 1,000-user Mac gate is still **FAILED**, with its frozen 532 harness
and unresolved original visibility failures. It was not rerun or reclassified.
No full Whagons/starter acceptance, capacity projection, SLO relaxation or
network subtraction is claimed by this follow-up.
