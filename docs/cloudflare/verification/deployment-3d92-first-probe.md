# First frozen3d92 composition deployment: setup failed before CPU test

Status: actual development composition deployed; application module setup blocked. No CPU or SDK pass.
Source3d92, runtime18afb, clean tracked archive and the exact published images in
`container-candidate-3d92.json`. Dirty owner-prototype work is preserved separately.

Owned development name: `gonvex-cf-dev-k22-3d92-platform`. Kernel version
`7fa97c23-6c03-4714-8385-5319c2d561c6`, deployment`b57c6faa-f4d3-4147-a3da-20de1a955bfc`,
API configured`cpu_ms:5000`. Configuration is not proof of CPU enforcement. The adjacent JSON records all
four Workers’ current configuration hashes, deployment/version IDs and raw evidence hashes.

The finite180-second campaign failed during backend setup, before unchanged `errors.spin` executed.
Actual `POST /dev/sync` returned HTTP500. The private root tail records:

```text
release builder /inspect failed (503): release builder did not start
```

The serving builder receiver waits up to60 seconds for Container TCP8790 `/healthz`. Its configured port,
Docker EXPOSE/ENV and server listen default agree; these facts do not identify the startup failure's cause.
Actual builder/Container startup/exit evidence is required. No destructive fixture retry, synthetic timeout,
source/image identity change or successful CPU interpretation follows from this failure.

Raw evidence: `/mnt/data/work/gonvex-k22/platform-3d92/{deployment/deployment.json,cpu-proof/report.json,
cpu-tail-private.jsonl,cpu.log}`. The report has stage`backend-start` and zero samples. Root-tail parsing
found the builder exception; request headers and credential data are not published. K22 owns diagnosis and
the next bounded real probe; the lead did read-only evidence/source inspection, no duplicate account work.

CPU reset, durable causal marker/quarantine mapping, rollback/receipt correctness, unaffected clients,
SDK/deploy continuity, enabled files/eight-package domains, lifecycle and full capacity/security/soak gates
remain open. Original local34/4 failures are unchanged. No staging or production action occurred.

## Subsequent finite diagnosis and cleanup

The exact published builder image's Nodev22.18.0 binary, extracted read-only without Docker, starts the
archived server natively and returns health200 with `target:local`. This tests native entrypoint startup,
not Container startup, actual Wrangler-target deployment or CPU enforcement. Downloaded deployed JS/SQL/
Wasm parts of all four Workers match the clean dry-run bytes. Source maps differ only in `sources`,
`sourceRoot` and `file`; `sourcesContent` and mappings match. The adjacent receipt retains all hashes.

The actual builder app stayed inactive. One separate bounded `/healthz` diagnostic returned503 after
60,613ms, with no module/CPU invocation. Only the owned builder's Container logging was temporarily enabled,
then restored to disabled. The companion tail was empty and inspect API exposed no exit reason. The
Container startup cause remains UNKNOWN. No source/deploy/spin rerun followed these observations.

At2026-11-12T16:55:09Z cleanup finished PASS with zero residuals **within the explicit owned manifest**:
four Workers, two Container apps, the Workflow, Queue consumer/queue, two R2 buckets and synthetic state.
Exact incoming/outgoing/DO references and zero tail consumers were checked before force deletion. Registry
images and raw evidence remain retained. This deployment is historical evidence, not an active test target.
Retained image/evidence storage and billing are not measured by the cleanup result.

Two failures remain visible: the old helper hit Cloudflare10064 because its Queue consumer had to be
removed before the Worker; a strict follow-up refused the consumer API's `script` versus expected
`script_name` shape and deleted nothing. Using the actual exact owned script field and dependency order
then succeeded. These incidents require helper regression coverage, not a broad ownership fallback.

Raw receipts: `cleanup-final.json`, `cleanup-confirmed-owned-references.json`, `remote-content.json`,
`source-map-comparison.json`, `builder-diagnostic.json` and `image-startup/` under the same private evidence
directory. K22 resumes independent0074 source work. No lifecycle, CPU, application or staging-readiness
gate is accepted from diagnosis or cleanup.
