# 0060: Assembly comparison and build evidence

Date: 2026-11-12. Status: decided (lead). Applies to K22 verification; no original gate is waived.

## Change-row REAL spelling

0028 §2a already accepted 0026's named `numbersByValue` comparison for JSON change-row images.
The formal `change-capture` scenario may reuse that exact, unit-tested rule for both sides' `old_value`
and `new_value` strings in `comparableLog`. Strings, keys, key order and integer precision stay exact.
Keep every row, revision, provenance, count and digest assertion and retain the raw observations. This
repairs a formal-harness omission of an existing ruling; it introduces no numeric tolerance or runtime change.

## Captured host-error stacks

0015 S0-1 applies to a real failed host call whose `GonvexHostError` is caught into an Action result string
(such as `actions.toolsProbe`'s `result.duplicate`), as well as a field named `error`. The first line remains
exact contract. A narrow actual-host-error first-line predicate may use only the existing JS stack-line
removal rule; do not add duration/UUID/token masking or discard non-stack lines. Tests must preserve
different first lines, non-stack multiline detail and arbitrary application text resembling a stack. Keep
raw frames and all existing assertions. No handler, Rust or application-module change follows from this.

## Real deployment boundary

K22's local real-entry deployment exposed active gateway socket closure during Wrangler reload:
`receipt-upgrade` timed out waiting for generation 2 on its existing raw socket; `session-handshake`
expected an open control socket and `system.reload`. Retain these failed observations. Do not preload
future generations, substitute a test registry activation, skip assertions or label this a full matrix pass.

0016 G0-2 already permits active objects to close sockets with 1006 during a baked Worker deployment,
followed by the unchanged SDK reconnecting and authenticating to the new artifact. 0028 §1 retains baked
releases and defers Facets. This decision does not adopt a new code-loading architecture or infer remote
behavior from local reloads. Same-socket raw continuity and SDK recovery are distinct evidence.

Add an actual SDK deployment/recovery probe with the real generated-entry swap, preserving generation,
schema/defaults, stale-artifact refusal, durable receipts, idempotency, authorization, replica and live-query
convergence assertions. Exercise tenant and control-only clients and contract-change gating. Record
first-attempt errors, disconnects, elapsed recovery and any ambiguous write. Preserve the original raw
scenarios and their results. Exact assembled development-platform rollout proof remains required;
the local mismatch is not silently converted to success. Review the resulting evidence before claiming
the deployment gate satisfied under the already accepted stage-1 transport adaptation.

## Container provenance and build resources

The original completed K20 task is `mcp:launch:f60992434d04607cdd7674f5b5c8143d7ea5afb09759f9def5d85b8c68f0bb2f`.
Retained BuildKit records `q8t2ymsg6zq7d015homxs4903` (17:23:54–17:23:57 Pacific, November 11)
and `9dnuheao0gurl7c6ri6phsea6` (17:25:45–17:25:49) have image-config attachments hashing to
`sha256:c9acf7bb395b58c6bda81684269332e4213239d730b27f3bdf870f96100c4414`.
Their embedded Dockerfile bytes match integration e72d3a43. SLSA attachments record base-image digests,
the build recipe and a local context, with VCS revision eadbc0d8. Their resolved dependencies do **not**
provide immutable per-file hashes for that local context. The original task rebased during an earlier
build and rebuilt afterwards. Neither that history, the image tag/time nor matching current source closes
the remaining original-input provenance gap. Lead raw metadata: `/mnt/data/work/gonvex-cloudflare-lead/k20-provenance/`.

Do not start a new default `/var/lib/docker` build, alter daemon/cgroup policy, prune others' caches or
claim a `/mnt` buildx volume places systemd-driver Docker children in the admitted heavy slice. A scoped
hosted-CI development Container build is an authorized alternative: exact immutable source SHA, complete
input manifest, pinned base images/locks, output registry digest, and bounded build/test evidence. Keep
credentials in approved wrappers/CI secrets; no untrusted PR-controlled credential execution. Deploy only
`gonvex-cf-dev-*`, with immutable digest provenance. No staging or production action follows from this ruling.

0057's failed local `errors.spin` and the actual private-platform CPU reset/caller quarantine proof remain open.

## Interruption fixture must reach its injected fault

PR399's unchanged-runtime CI exposed a migration test assuming its first 1.5-second event reaches a
particular R2 fetch failure. It can legitimately yield first, returning running/error-null. The fixture now
uses one chunk per event and a bounded actual-alarm drive until that injected failure occurs. It requires
the failure count to reach zero, keeps the exact retry-error/gate assertions and all final schema/recovery
checks, and leaves the test timeout and runtime event budget unchanged. This is a test-fixture correction;
the failed original CI receipt stays recorded. It is not a runtime migration change or a red-check waiver.
