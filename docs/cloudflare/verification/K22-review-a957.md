# K22 independent review at a957d17b

Reviewed artifact: PR400, a957d17b17b5d851e7ef6dc29108d1aa81f95912, original base c97b35a7.
Formal review: https://github.com/Whagons-International/gonvex/pull/400#pullrequestreview-5874287424
Verdict: changes required; acceptance UNMET. Later unpushed implementation is outside this verdict.

The independent reviewer inventoried all 101 files, matched Git/GitHub lists and pinned snapshots, and
classified the full diff non-UI. Thirty-one conformance units, four deployment-config units and seven
synthetic source-method failure reproductions are source evidence, not deployed SDK/platform proof.

| Finding | Required correction and evidence |
| --- | --- |
| N1, P1: anonymous nested Reducers gain scheduler/outbox | Preserve server-derived anonymity through every nested invocation. Prove absent surfaces/no jobs or outbox and uncaught nested rollback. A caught missing-surface JS TypeError may retain allowed writes; actual dispatched-host denial still latches. |
| N2, P1: nested/scheduled Reducers bypass migration gate | Guard the shared write entry across migration events before invocation/commit/ack; exercise paused migration with Action and scheduler callers. |
| N3, P1: service Actions lose internal access/provenance and pending becomes successful ingress null | Preserve trusted grant access/root id/channel; return real awaited service/provider/share results or failures, never pending-as-completed success. |
| N4, P1: failed Workflow reply RPC becomes unrecoverable | Separate terminal execution from acknowledged reply settlement; retain bounded full-result/correlation authority and redeliver idempotently without rerunning the Action. Test before/after application faults, large results, scope change and hibernate. See 0068 for generation/skew gaps. |
| N5, P1: old live update consumes pending replacement | Bind semantic initial completion to exact operation generation; preserve successful A when B refuses. Actual interleaving/hibernate tests required; see0064. |
| N6, P2: historical physical-connection epochs unbounded | Bounded authority/in-flight lifecycle and honest admission; no blind fence eviction. See0065; stranded OWNER remains a separate gate. |

The targeted-delivery scope defect is confirmed separately under0063. Stranded OWNER, local CPU enforcement,
deployment recovery/checker and full parity/capacity gaps remain open. Initial canonical a957 run timed out
after 900 seconds in the first thin-SDK scenario; the remaining 37 cases were unexecuted and no final runner
JSON existed. Whole-job resource figures are not isolate measurements.

Owner reports of later fixes and passing named subsets remain labeled by their own frozen source/configuration.
They do not close these findings until the actual fixed artifact and its regressions are reviewed. Every new
independent recheck uses a new delegate. No incomplete K22/X01 implementation is accepted by this record.
