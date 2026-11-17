# Disabled operational preparation

State: implemented definitions/local-tested helpers; **not drilled, activated or continuously observed**.
M05 original P1–P5 definitions/thresholds/windows still require the exact original design body. Resolve
against the audit; preserve import commit and source links. Added symptoms are separate requirements.

Before any write run: verify exact synthetic tenant/identity/permissions, real app source/config/deploy,
origin and auth allowlists, suppressed customer notifications/webhooks/integrations, separate warm/cold
fixtures, serialized durable cleanup ledger, retained fixture count/expiry and remaining cost/write budget.
On cleanup/budget failure pause **new writes**, retain outstanding fixtures/history, classify coverage UNKNOWN.
Never use an operator credential, retry an ambiguous mutator automatically, or clear receipts to get green.

Prepared added condition handling (owner, ack deadline and escalation order remain UNCONFIGURED):

| Symptom | Investigation and authorized recovery |
| --- | --- |
| HTTP/DNS/TLS/bootstrap | Compare external regional runs, DNS/TLS transport and actual browser readiness; separate provider loss from app refusal. |
| Auth refusal | Distinguish expiring synthetic credentials/rotation from app auth outage. Preserve actual application visibility and least privilege. |
| Durable-write/receipt ambiguity | Inspect real transaction/receipt and second-client state. Reconcile under existing idempotency contract; never invent non-invocation or replay Actions. |
| HTTP 200 / stalled sync | Keep writer confirmation separate; inspect ordered gateway/tenant queues, scope/epoch/drain/reconnect and actual second-client applied revision/digest. |
| Job late/dead letter | Use job's documented deadline, scheduler tolerance and terminal evidence; inspect lease/outbox/Workflow state without double execution. |
| File owner/bytes | Verify <=1KiB checksum and unauthorized denial, then object-store dependency. Keep R2 bytes separate from tenant SQLite. Stop writes if cleanup fails. |
| Resource exhaustion | Compare real SQL rows, CPU, isolate memory, queue bytes and quotas; no guessed account limit/capacity claim. |
| Monitor heartbeat/sample/export absent | UNKNOWN. Independent external witness and operator fallback must work without the same provider/Cloudflare failure domain. |

One failing first attempt records a regional symptom; one bounded retry may confirm. Two successive healthy
regional samples propose recovery; unknown breaks the streak. No cross-region quorum hides a single region.
Dedupe environment/component/region/failure class/incident ID while preserving every attempt. Recovery payload
shares dedupe identity. `alertPayload` produces **local payload only**; queued API or local sink success is not
actual delivery. Real test destination, recipient authorization, acknowledgement deadline, owner/escalation
order and actual receipt timestamps must be configured and drilled separately; send nothing from this task.

Status components Web/API/Sync/Files/Background are disabled definitions. `status.whagons.com` inherits the
existing Cloudflare zone DNS dependency. Hosting/CDN/account/login, provider-owned fallback hostname,
DNS TTL/delegation/parent-zone failure and independent operator network are unverified. A different hostname
or hosting brand does not prove independence. Before publication prove operator can log in/update during
Cloudflare/app outage from a separate failure domain, with synthetic-only incident copy. No DNS/login/
publication/account creation is performed by this runbook.

Lead's next finite proofs: exact assembled local + development artifact through real SDK/browser handlers;
external separately scheduled 3 regions (2 Americas) with actual provider IDs and timestamps; scoped V05
route/auth/write/sync/job/file/resource/monitor failures; real authorized test-alert receipt/recovery and
retained first failures; coordinated serving hooks and trace continuity through autoscale/drain/reconnect/
deploy; V04 overhead/retention/billing; canonical P1–P5 original drills; fresh independent artifact review.
Keep K22 runtime18afb/PR4003d92 and X01 PR3976b2 acceptance UNMET despite prior green CI. Full local38
34/4 failures and CPU UNEXECUTED remain separate, unreplaced evidence. No K22 mutable checkout was read.

Activation remains separate authorized release after `providers/operations.mjs` manifest dependencies.
Rollback pauses synthetic writes, preserves raw/incident/ledger history, disables only newly introduced
exports/tracing/schedules, restores previous config, cleans only owned fixtures after actual settlement,
and verifies status/operator fallback/residuals. Never erase incidents or mark suspended coverage green.
