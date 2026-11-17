# M05 foundation493 recheck: three P2 defects remain

Date: 2026-11-12. Fresh independent COMMENT review pinned to
`49399492911e2655d7295f7e9668d2fb911d0475`:
[review5880928422](https://github.com/Whagons-International/gonvex/pull/414#pullrequestreview-5880928422).
Finite FOUNDATION verdict: changes requested. Original full M05 acceptance remains separately UNMET.

Report/receipt: `/mnt/data/work/gonvex-m05-recheck-49399492/{report.md,receipt.json}`.
Report SHA256 `6b6d2e3183871f953fe37241940edfd33b39bf87200cab92c1862751a5435f64`;
receipt SHA256 `465cb9af45291cf3fba241bece8bb759b465b71dad316bea358ebf1da46fbe05`.

Prior F1 monotonic deadline and F3 strict job elapsed checks are fixed. The original F2 off-grid example
is fixed, but broader accounting/privacy defects remain:

| P2 | Accepted-input reproduction and required behavior |
|---|---|
| R1: enclosing-record filter loses an eligible operation | Window[0,100), healthy slot0, failed record scheduled/started100/finished105 with clock uncertainty1 and create attemptedAt99. Validation accepts the declared clock tolerance, then filters away the record before its operation: attempted0 and bounds1/1. Select actual attempts independently, retaining legitimate clock uncertainty and conservative UNKNOWN coverage. |
| R2: impossible application success after cutoff | Check0..100/duration100; create attemptedAt95, durable2/applied10/deadline10, asOf100. Current parser accepts success despite application at105. Bind relative operation timing to its observation/cutoff with explicit clock tolerance; contradictions must fail integrity, not manufacture success. |
| R3: failed timing fields bypass privacy validation | Optional durableMs/appliedMs on failed records accept nested provider/private objects through the advertised strict ingress. Validate every present field regardless of status, or safely reject/strip before any retained/exported record. Missing failure evidence may remain absent/UNKNOWN; never serialize private payloads or erase failed attempts into green reporting. |

The same implementation owner is active on a coherent correction. Preserve445/493 evidence and original
tests; a new immutable head requires a NEW independent recheck. These findings block foundation merge;
they are not objections merely to missing future hosted evidence.

Independent62 foundation/29 conformance tests,33 adverse cases, docs/generators and cycles1→1 pass within
their stated domains. All36 files,20 implementation/workflow files, current tool/artifact mappings and
old evidence hashes were checked. Node SQLite commits/reads are real local fixture operations with a
synthetic applied map, not DO/SDK/Whagons/browser or three-region proof. The adverse cases reproduce the
defects; their successful assertions are not product-success tests. All finite jobs settled; no fixes,
installs, account activity, activation or CI polling occurred. Compile/cycle commands used the unchanged
current wrapper; earlier historical coverage limitations remain.

P1–P5 original definitions are unresolved. Serving spans, real application paths, exact assembled dev,
external schedules/account/heartbeat, authorized drills/alerts/status, security/cost/capacity and actual
14-day/30-day gates remain. No M05/K22/X01 or staging-readiness approval follows.

Subsequent author correction: PR414 head `1b3455bb46132fd4b6cd658d38fb1796cd0d01a9`, tested source
`6ac203fbb45f8828dd26ca4cb447ba4252679b83`, reports all three fixes and84 foundation/29 conformance
tests, generators and cycles1→1 passing. The full45-file diff is non-UI. Revision-r2 retains the earlier
445/493 artifacts and original assertions. A NEW independent exact-head recheck is required; this update
does not relabel the493 verdict, approve the correction, or close any full M05 gate.
