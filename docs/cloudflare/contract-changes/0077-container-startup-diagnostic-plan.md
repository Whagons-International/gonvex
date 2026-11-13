# 0077: Prepare one observable builder-only startup diagnostic

Date: 2026-11-12. Status: preparation permitted; live attempt awaits review of its concrete artifact/manifest.
Owner: K22 implementation agent. Lead owns release of the bounded development test and evidence review.

## Completed forensics and retained failure

The fresh forensic task completed at clean integration `7028c122`. Its report/receipt are under
`/mnt/data/work/gonvex-container-startup-forensics-3d92/`. It checked 2,303 source blobs, image/config/context
identities, nine historical receipts, pinned dependency semantics and official references without executing
the image, editing package source or accessing the account. Startup cause remains **UNKNOWN**.

Confirmed diagnostic defect: frozen 3d92 `kernel/assembly/builder.ts:27` discards monitor failures; line 32
discards health-probe errors. This suppresses causal observations, but establishes no exit cause or fix.
Raw `start` is synchronous, its `env` option is correct, and the omitted scheduling policy selects `default`.
Adding an await, renaming the option or switching policy is not an established repair. The retained TCP
Fetcher is only a conditional hypothesis. Image architecture/entrypoint/command/listen-port facts cohere.

The earlier actual deployment/module-setup failure, inactive snapshot, empty tail and native-only health 200
remain separate evidence. All explicit-manifest resources were removed; images/raw artifacts were retained.
CPU enforcement, quarantine and errors.spin were UNEXECUTED. No vendor-fault conclusion follows.

Primary references checked 2026-11-12: [raw Container API](https://developers.cloudflare.com/containers/api/durable-object-container/)
distinguishes start, readiness and monitor outcomes; [scheduling policies](https://developers.cloudflare.com/containers/configuration/scheduling-policy/)
describes the default selected when the policy is omitted. These current docs do not identify the hosted
runtime build. Pinned definitions and historical evidence remain in the forensic receipt.

Evidence SHA256: report `3f9cb171fc2efe2d0fc1026b8eb26dbbb737a764592ca45441f0613f377273c7`;
receipt `9f4d3b1243299fcfdb4ea652ffe9d8313969d7d333ce0f32da9529062c38c083`.

## Reviewable artifact before one live attempt

At a safe source boundary, K22 prepares a pinned diagnostic Worker/config, local regression evidence,
runnable driver and exact owned manifest. Do not force an owner-prototype checkpoint or re-task the
completed researcher. This preparation ruling does not release a live attempt; lead checks the concrete
artifact/lane and may release it within the existing development authorization. No additional user approval
is invented. Preserve ongoing owner-source work and every acceptance hold.

Use exactly one NEW `gonvex-cf-dev-*` builder Worker/SQLite DO and one Container app, `standard-2`,
`max_instances:1`, compatibility date2026-10-23, default policy and the existing verified 3d92 builder image:
`sha256:dcf301c14329e3de0f6e177e01cdd937f47c5c3dbfaf55fa2e187c2d32da892f`.
Keep its image ENTRYPOINT/CMD. No kernel, Actions, sandbox, module, CPU fixture, R2, Queue, Workflow or DNS.
No new image build is needed while image inputs remain unchanged; final serving owner changes still require
a fresh full-COPY-context image campaign.

Keep the startup ENV schema, with inert nonempty token/account placeholders and an unreachable valid kernel
URL. The inspected pre-health path needs no real application secrets. Record that placeholder values do
not prove credential delivery or deployment equivalence. Actual account operations, if released, use only
the approved Vault wrapper. No secret is copied into GitHub or logged.

## One start, observable outcomes, fixed bounds

Before starting, verify owned Worker/Container observability and expose a bounded durable diagnostic record
of at most 8 KiB. Persist single-attempt intent before `start`; duplicate requests cannot restart it. Capture
start try/catch once and immediately attach monitor settlement. Record normal exit, nonzero exit/numeric
exitCode or a fixed startup/runtime-error enum, elapsed times and required ENV-name presence booleans.
Never persist arbitrary error messages/stacks, values, headers, tokens or signed URLs.

Keep the baseline TCP Fetcher for the primary health path. Bound each GET to 1 s, at most 60 baseline attempts
and the whole observation to 65 s. Record first/last failure, count, status/fixed transport class, running
boolean and terminal monitor outcome; stop observation on readiness or terminal outcome. If still running
but all baseline probes fail, permit ONE fresh `getTcpPort` health GET within the same 65 s budget. A fresh
success narrows a transport-capability discrepancy; it does not prove an image exit cause. No automatic
restart, longer wait, policy/size/ENV/entrypoint toggle or destructive fixture retry.

Force-stop only the owned Container by 70 s; distinguish stop-induced monitor outcomes from pre-stop causal
results. An outside driver must enforce stop/cleanup even after request cancellation. Creation through
full removal is bounded to 10 min; an exceeded provisioning deadline is a retained failure, not permission
to extend the run. Use an exact resource/reference manifest and settled tails before deletion; preserve
image/raw evidence and report residuals. These allocation/time bounds are safeguards, not measured billing.

Required local tests cover synchronous start failure, monitor rejection/numeric exit, normal early exit,
health timeout/cancellation, no repeated start, bounded retained records and no secret values. The artifact
must show stop and cleanup paths, not just the successful health branch.

Record exact source/config/image hashes, Worker version/deployment, app/DO identity, policy/resources/logging
projections, safe timeline and cleanup receipts. A generic exit 2 alone is not an ENV-guard diagnosis. An
opaque failure stays UNKNOWN and needs a separately reasoned next test. Eventual causal source fixes need
proper ownership, immutable artifact, NEW independent verification and original acceptance evidence.

No startup-only success accepts K22/X01, proves module/CPU behavior, replaces the full local/dev matrix,
or waives enabled-file/eight-package, autoscale 26-tenant/1,000-user, capacity/security/uptime or actual-duration
gates. No staging/production deployment, load, monitor activation, purchase or public publication is allowed.

## Durable prepared-record handoff clarification (2026-11-12)

`recordSha256` identifies the exact UTF8 bytes durably published as private `record.json`, not a different
raw HTTP representation or a hash echo. The helper serializes once, fsyncs/renames/dirsyncs those bytes,
then offers their hash. Raw-observed body/hash, if retained, is separate bounded evidence.

Before arming or the third ACK, the guardian independently reads the canonical private bounded file from
its anchored artifact capability, compares the actual byte hash and validates the FULL frozen prepared
record, objectId and current namespace/app binding. Missing/altered/malformed/wrong-hash/partial records
remain UNKNOWN with no ACK/start. Recheck the publication after awaited arming/journal IO before ACK.
Valid JSON whitespace, key order and Unicode escapes need not equal a reserialization. The hash and
objectId are integrity/correlation data, never peer/account authority. No new channel or schema is added.

Outside checkpoint `eaaadb59` omitted this independent read; its55 passing local tests do not prove it.
The lead's bounded source correction retains that artifact and its failures, adds real-filesystem adverse
tests and fixture-data corrections, and requires a new joint pinned artifact/fresh review with K22's helper.
No live release, actual wrapper/account cleanup, deadline or host-loss proof follows from this correction.
