# Frozen4731 diagnostic review: changes required

Date: 2026-11-12. Fresh independent read-only review completed. No source fixes or live operations.
Reviewed local Git `4731ac239fefeb862530c03ebc83897d6e0029fb`, three diagnostic-only commits above
`3d92e1339b2d65dbc47ac2f5713f56bd9e57d738`. PR400 remains at3d92; this is not its new CI/review approval.

Report: `/mnt/data/work/gonvex-builder-diagnostic-review-4731ac23/report.md`, SHA256
`88cef66cc7d832ae9c3baf124aa6f3d958ccda8793a9c532dad0fe65ef082ea6`.
Receipt: same directory `receipt.json`, SHA256
`df5f4e1f7c3992a22d3f1606288c3158c471853d3633c4340c65e380eb656568`.

## Two P2 findings

| Finding | Concrete evidence and required fix |
|---|---|
| R1: mandatory tests assume the author's arch3 path | The package test append invokes three `mkdtemp` tests requiring an existing `/mnt/data/work/gonvex-k22` parent; `prepare.mjs` also restricts offline output to that author root. The Ubuntu workflow creates neither. Source inspection plus reviewer-owned actual ENOENT/helper-root witnesses establish the portability defect, not a hosted CI reproduction. Use portable test scratch and safe explicit offline preparation-root injection while preserving LIVE task-path restrictions. |
| R2: captured app ID disappears from cleanup reasoning | `cleanExact` checks a captured ID only when the name-filtered list finds the app. Empty lists, including the early Worker-absent branch, never resolve that ID. A synthetic renamed-but-live captured app yields one forced Worker DELETE and `remaining:[]`; the zero-delete assertion fails1!=0. Independently resolve captured IDs before ANY mutation; reject foreign/unknown/contradictory state and verify those same IDs absent before a zero-residual receipt. No actual account behavior was observed. |

K22 implementation owner receives both fixes. Keep4731/final3 evidence immutable; the next corrected
diagnostic artifact needs a NEW independent recheck, never a re-task of this completed reviewer.

## Verified domains and limits

All15 changed files were reviewed:14 standalone diagnostic files and one additive package test entry.
All2,317 archive blobs, copied inputs, config/manifest/tool mappings match. Independent executable bundle
matches17,230 bytes/4,663 gzip and the author's JS hash; map differs only in sourceRoot. Runtime cycles
remain1→1 with no added cycle; complete diff is non-UI. Serving builder/owner/placement/M05 code is untouched.

Independent32 Node tests,4 native workerd tests, focused TypeScript and9 supplemental adverse tests pass.
The cleanup negative reproduction has one retained expected failure. Native DO/KV/auth/eviction are real;
Container/account APIs remain synthetic. No Container start, actual API schema/delete/billing/stop deadline,
CPU, SDK deployment or hosted CI proof follows. Full serving suites/generators before a future push remain
required; their absence was disclosed and did not prevent this offline review.

Execution deviations remain part of the record. A negative test-name filter matched the parent and ran
three author-root tests outside the review output boundary. Their random full paths were NOT logged;
only prefixes are known. Passing finally blocks awaited removal, but there was no independent absence
audit. No owner-root glob/cleanup was attempted. Subsequent tests used explicit reviewer-owned paths.
The initial extraction cwd failure and canceled fake-clock run are retained; neither is a source finding.
Earlier author pnpm materialization/dependency-link history is retained without claiming unchanged global
store contents. Review checkout is clean at6273; all logged finite test units are settled.

## LIVE hold and next owned artifacts

Source verdict: **changes required**. Startup cause remains **UNKNOWN**. No live lead-release JSON exists.
Even corrected source does not arm the separately required hard-kill/operator-loss cleanup obligation.

Lead owns the external execution-lane artifact: a finite supervisor outside the operator's cgroup/lifetime,
single-use exact source/config/tool/manifest lease, durable creation intent/observed IDs, and a credential-free
release verifier BEFORE invoking Vault. A separately checked cleanup executor must retain the obligation
after operator death, use corrected exact-ID preflights, issue stop before metadata I/O and preserve the
last pre-intervention causal snapshot. Test child kill/reap, kill at creation/ID capture, stuck metadata and
independent cleanup signals without account calls before release review. No executor is created/armed here.

A same-host supervisor cannot prove machine-loss recovery. Identify an independent reachable recovery
executor or keep that condition unresolved; missed deadlines/residuals remain failures, not a ten-minute
guarantee. Do not retry start or extend waits. The proposed8min work/2min cleanup and70s stop are guards,
not achieved platform or billing results. K22/X01/N6/OWNER/full matrix/CPU/files/eight-package/capacity/
security/uptime and actual14-day/30-day gates remain open. No staging/production or live monitor activation.
