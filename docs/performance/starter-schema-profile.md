# Fresh starter schema validation: measured execution cost

The metadata-batch prerequisite was not established. **No production query,
check, binding, provider interface or proof changed.** This descendant of accepted
`6c030c1d` retains the original SQLx path and adds explicit test-only attribution.
There is no batching speedup to claim. The original cheap-starter goal is unmet.

## Method and measurements

Release opt3/thin-LTO, the same 9,002 authorized synthetic tasks and one hidden
row, 400-byte extra bodies, exact projection/order/window, ready source index and
C42 SDK as accepted 73/6c. Every operation starts a new Node SDK/Memory storage;
server and OS are warm. Starter repeat 0 builds the immutable proof; repeats 1/2
reuse the proof while retaining fresh SQL membership/authority checks. Other
heavy lanes were active; this is not a capacity or browser-performance result.
Actual aggregate limits were 6 GiB / 200% CPU. Admission checked available memory,
both I/O pressure averages and actual target headroom before every phase.

Primary measurements disable allocation capture and the new schema trace. An
immutable accepted 6c executable is the baseline; the descendant has the same
production behavior. Warm numbers overlap and cold numbers vary; these are
controls for attribution, not evidence of an optimization.

| Case | Rust process CPU ms, cold / warm / warm | Node CPU ms, cold / warm / warm | Wall ms, cold / warm / warm |
| --- | --- | --- | --- |
| Accepted 6c normal | 55.84 / 12.63 / 11.66 | 683.84 / 651.43 / 659.99 | 512.64 / 455.56 / 463.79 |
| Accepted 6c starter | 139.53 / 128.18 / 127.48 | 703.81 / 779.04 / 721.78 | 653.72 / 691.68 / 658.86 |
| Descendant normal, trace off | 56.84 / 12.65 / 13.76 | 693.92 / 667.51 / 685.27 | 536.76 / 471.73 / 491.52 |
| Descendant starter, trace off | 161.06 / 127.82 / 128.13 | 720.79 / 713.50 / 753.96 | 684.14 / 635.86 / 686.29 |

The private, test-only trace attaches to the **actual SQLx SQLite connection**.
It records native worker thread CPU/monotonic spans and SQLite statement counters,
without retaining SQL/row values or replacing the cancellation progress handler.
Separate leaf spans measure process CPU and wall time. Native spans start at
`SQLITE_TRACE_STMT`, so preparation is excluded. Process CPU includes all Rust
threads. The residual includes preparation, decoding, filesystem work, caller
work, scheduling and handoff; it is an **upper bound on IPC**, not pure IPC time.

Per complete starter operation, allocation capture off, all three trace runs:

| Leaf | Calls | Wall ms range | Native worker CPU ms range | Top-level SQL / internal SQL |
| --- | --- | --- | --- | --- |
| Physical local admission | 20 | 0.93–1.40 | 0.257–0.343 | 20 / 20 |
| Actual DDL | 20 | 0.43–0.53 | 0.116–0.147 | 20 / 0 |
| Actual columns | 20 | 0.77–0.94 | 0.446–0.539 | 20 / 20 |
| Full key shape | 20 | 20.83–23.25 | 20.35–22.61 | 20 / 0 |
| Rust DDL/column validation | 40 segments | 0.109–0.174 | — | 0 / 0 |
| Exact source-authority serialization | 20 | 0.461–0.545 | — | 0 / 0 |
| Full identity serialization/hash | 19 cold, 17 warm | 1.20–1.63 | — | 0 / 0 |

The three metadata leaves together take **2.13–2.87 ms wall**, with
0.818–1.029 ms native worker CPU. Their combined non-worker wall residual is
1.32–1.84 ms, including filesystem/path validation and preparation. The process-CPU residual is 1.65–2.09 ms and also includes other Rust threads.
Removing all of that residual would be an unrealistic ceiling on metadata
batching benefit, roughly 1–2% of measured warm starter Rust CPU. It cannot
close the gap.
There are 80 successful source-schema top-level statements per starter, hence
80 SQLx Execute submissions on this path, plus 40 internal PRAGMA statements.
This mapping follows inspected sqlx 0.8.6 worker code; it does not measure kernel
context switches. A batch must also preserve rejection of invalid DDL before
column reads and Rust definition validation before the full key scan. No eager
batch, JSON conversion or alternate binary-binding path was introduced.

The full shape checks execute **1,800,940 VM instructions** and report
**180,040 FULLSCAN_STEPs**, returning 20 scalar results. These are actual native
counters, not sampled/inferred row counts. A separate actual-TX EXPLAIN shows the
9,003 physical-row table is already scanned through its **covering primary-key
index**. No full row payload or second table copy is necessary for this scan.
DDL lookup reports 380 full-scan steps across the small sqlite_master relation;
column reads report 20 sorts. Internal PRAGMA VM counts remain separate from
outer counts to avoid double counting.

The first trace implementation incorrectly assumed nested PRAGMA PROFILE
completion was LIFO and reported 20 accounting errors. Its files remain for
audit and are explicitly superseded. The checked-in fixed active-statement arena
passes the nested-PRAGMA/drop test; every selected v2 report has **zero errors**.
Static bounded contexts survive dropped initiating scopes and are released at
connection close. This instrumentation is not enabled in production.

## Allocation, bytes and unchanged safeguards

Separate allocation-profile cases show warm accepted 6c starter about
130,103–130,124 Rust requests / 27.626 MB versus traced descendant
130,214–130,215 / 27.640–27.642 MB. Instrumentation adds a little work; it does not
reduce allocations. These counters cover Rust process requests, exclude SQLite
C/Node, and are not live heap sizes. Rust resident samples are 95–97 MB, not peak
RSS or evidence of improvement; Node peak RSS samples are in the raw results.

Raw NDJSON remains 4,021,989 bytes, encoded pieces 103,417 bytes, JSON wire
146,045 versus normal 4,032,973, on-disk about 138,190–138,193 bytes (opaque
identity metadata lengths vary). Node makes 9,034 crypto calls, including the
existing stage memo behavior. SDK staging peaks at 56,483,844 estimated charged
bytes and releases to zero. The bounded artifact cache and 64 KiB/stage codec
workspace are unchanged. Network byte savings remain distinct from CPU cost.

All original SQLx statements, results, type/NULL/BLOB decoding, error order,
physical-source checks, schema/watch/readiness validation and transaction/lease
boundaries remain in place. Tests compare actual DDL bytes before/after ALTER,
reject NULL/BLOB/empty/NUL keys, and reject missing/view/collated sources before
any shape scan. The existing forged checksummed wrong-key cache (zero offer
payload), unchanged-user-atoms row-scope revoke before piece bytes, fresh native
principal/source/serial ordered enqueue, and final normal-v2 SQL/XOR anchor tests
are unchanged. Full provider mutation serial remains selective and supplies no
broad shape-validation token; B keeps all full scans.

## Remaining cost and next proposal

For this current fixture and execution plan, the required full shape work alone
uses 20.3–22.6 ms worker CPU. Separate fresh offer stream timing is 46.3–52.2 ms
and final normal-v2 stream timing 22.9–24.5 ms, with 18,004 authorized projected
rows streamed between those two proofs. These distinct sequential spans explain
roughly 90 ms of current work before the other source/native/module checks. They
are measurements of the current algorithm, **not a universal lower bound**.
Metadata IPC is too small to account for the starter/normal gap.

A concrete next experiment would retain **all 20 full scans** and compare the
current `instr(TEXTkey,char(0))` NUL-presence predicate with
`instr(CAST(TEXTkey AS BLOB),X'00')` in an alternating same-TX optimized kernel.
The measured target is the native scalar/VM work, not fewer row visits or fewer
fresh checks. Before retaining any change, compare exact Boolean/error behavior
on NULL/BLOB/integer/real/empty/NUL/Unicode/malformed TEXT and both indexed and
nonindexed keys; preserve the existing type and empty checks, guard order and
all source authority. Whether the byte-search form is equivalent and faster is
**unproven**. This proposal is not implemented and no win is claimed. Even a
shape-loop improvement leaves both mandatory fresh SQL membership proofs, which
need their own execution algorithm gains to reach the cheap-starter goal.

Full runtime checks pass: **437 ON / 399 OFF**, with 12 explicit opt-in tests
ignored in the ON suite; the real loopback Rust + Node SDK test separately passes
old-client, bootstrap/catchup/normal-XOR resume/retention, old-server and
revoke-before-piece modes. Dashboard/template codegen, docs metadata/typecheck,
owned formatting and cycle comparison pass (2 → 2, no additions). No client or
protocol source changed; prior browser qualification is not a browser performance
claim. No full Whagons, shipping overlay, customer data, integration, CI, PR,
activation or 1,000-user qualification occurred.

Exact measurements, plans, pins, receipts and next-proposal obligations:
`/mnt/data/work/gonvex-starter-schema/results.json` and
`starter-schema-profile.evidence.json` alongside this report.
