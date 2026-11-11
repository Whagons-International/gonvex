# Gate G0 report

Date: 2026-11-11. Author: the Cloudflare lead. Branch: `cloudflare/integration`.

**G0 passes if** each spike passes or has a written workaround, the contract is frozen, and the harness
passes Rust against Rust (`work-packages.json`).

## Verdict: G0 passes, with three workarounds and one item open

| G0 condition | State | Evidence |
|---|---|---|
| S1 schema | **Workaround** (0014, package A01) | `docs/cloudflare/spikes/S1-schema.md`, PR #329 |
| S2 execution | **Workaround** (A01, plus the mandatory deferred-FK check in 0016 G0-5) | `S2-execution.md`, PR #330 |
| S3 sockets | **Workaround** (gateway placement 0016 G0-1, liveness 0016 G0-6) | `S3-sockets.md`, PR #332 |
| S4 long Actions | **Pass** | `S4-long-actions.md`, PR #331 |
| Contract frozen | **Yes** | `CONTRACT.md` (PR #320) plus decisions 0001-0018. Two verifier passes plus a completeness re-check |
| Harness passes Rust against Rust | **Yes**, as the package reports it | P02 (PR #327): 11/11 scenarios on 3 consecutive runs, 4/4 mutations caught, 177 digests verified per run. Not yet re-run by an independent verifier. |

Open item: **0012 R25 / 0016 G0-11**, what a runaway Reducer's CPU overrun does to the other sockets on its
object. It does not block G0. K04 must measure it before G1.

## Spike results

### S1: real tenant schema in a Durable Object (workaround)

| Measure | Result |
|---|---|
| Full migration set applies | Yes, with rewrites: 26,532 statements plus 1,161 runtime objects, 0 failures, **135 s** in chunks of 200 statements. Migration 0003 as one transaction used 45 s CPU and reset the object. |
| Largest staging tenant imported | 225,854 rows, 522 tables, 125.6 MB source, `databaseSize` 100.0 MB. 0 count mismatches, 0 FK violations, `quick_check` ok |
| Cold wake | Platform **median 997 ms**, max 1,851 ms (8 restarts); warm 93 ms; local 221–340 ms |
| Memory | +69 MB RSS on wake locally (56.2 MB native schema heap). On the platform the loaded object has the same JS headroom as an empty one |
| Statements > 100 KB | 4 (by effective length) |
| Tables > 100 columns / rows > 2 MB | 0 / 0 (widest table 43 columns, widest row 205 KB) |
| Queries > 100 bound parameters | Only in runtime code (live-query `in`/`oneOf`, `existsDataRows`) |
| `regexp()` | 0 in the schema, 1 use in Whagons (`purchasing/common.ts:164`); not available in an object |
| FTS5 trigram | Works |
| Refused constructs | Compound SELECT > 5 terms (22 views), > 100 KB (4 views), expression depth > 100 (3 views and 1 unique index), `CREATE TEMP TABLE` (27), custom functions and hooks (1,140 triggers) |
| **Host-dependent blocker** | On strict hosts, any statement that reads ≥ 10 branches of a reference union fails ("nested too deep"), so 301 write statements on 136 tables fail to compile, including every `tasks` write. |

**Workaround:** a deterministic kernel-side transform of the app's unchanged migrations (0014, T1–T7). S1
verified every rewrite on real data, and T6 (per-branch reference statements) removes the strict-host
failure. This is a **G1 blocker**: no task write commits in an object until A01 lands.

P03 (tenant transfer, merged) confirms the same objects. All 15 staging tenants round-trip with every table
matching by checksum, in both directions, locally and on the platform: 613,764 rows, 706 MB.

### S2: Reducers in a Durable Object (workaround)

| Measure | Result |
|---|---|
| Transaction model | One `ctx.storage.transaction(async)` per Reducer is atomic around awaited host calls. Rollback works on throw, on host-call failure and on deferred FK. Savepoints work as nested storage transactions. |
| Runtime-contract vectors | **34/34** in workerd (31/34 without an Ed25519 length shim) |
| Reducers end to end | 10 of 25 distinct Whagons Reducers. The other 15 are blocked only by refused schema objects (A01). All 25 pass on the full-schema reference with the same host code. |
| Bundle | Query/Reducer split 4.16 MB raw / 876 KB gzip; Worker upload **1,019 KiB gzip**. Complete bundle 1,395 KiB gzip |
| Startup | workerd to first response 91 / **261** / 364 ms (no bundle / split / complete) |
| Heap | 13.5 MB idle with the split bundle, 25.2 MB after Reducers (limit 128 MB) |
| Reducer latency, warm, local | 11–23 ms committed, 1–3 ms failed or read-only |
| Rows per Reducer | 400–3,450 read (95–99 % host catalog lookups, to be cached), 3–13 written |
| **Hazard** | A deferred-FK violation surfaces at the platform's implicit commit as HTTP 500 and an **object reset**. A targeted pre-commit check is mandatory (0016 G0-5). |

### S3: 1,000 WebSockets on one Durable Object (workaround)

| Measure | Result |
|---|---|
| Memory, 1,000 sockets | ≈ 8.5 MB awake (≈ 6.7 KB per socket), 0 while hibernated |
| CPU per fan-out (2 KB, 8 classes) | ≈ 3.8 ms CPU with a per-class index (≈ 4 µs per send); deflate 9.6 ms |
| One object, 30 writes/s to 1,000 sockets | **No.** The ceiling is about 10 fan-outs/s, limited by outbound frame throughput. At 60/s the platform closed every socket with 1013. |
| 8 gateways × 125 sockets, 30 writes/s × 60 s | **Yes**: 1.8M of 1.8M frames, p50 89 ms, p99 342 ms |
| permessage-deflate | Negotiated; 4.4× fewer bytes; keep on |
| 32 MiB | Inbound ≤ 32 MiB − 128 B ok; a band below 32 MiB is lost (1006); > 32 MiB gives 1009. Outbound ≥ 32 MiB is **silently dropped** (0018). |
| Liveness | Auto-response ping works while hibernated at no cost, but needs a client ping. The edge never pings and never closes idle sockets (20 min test). |
| Deploy | Hibernated objects: 0 of 1,000 dropped. Active objects: all dropped (1006) and back in 4–6 s. |
| Hibernation | After 5–10 s idle; wake +25–50 ms at 1,000 sockets; attachments up to 16,384 B |
| Cost | ≈ $0.26/month for the socket layer per 1,000-user tenant; gateways add ≈ $1.25/month at 1 commit/s |

**Workaround:** gateway placement by default (0004 → 0016 G0-1), X01 triggered by frames per second, and
liveness with no SDK change: a periodic `replica.watermark` lets the edge detect dead peers, and K14 measures
the result (0016 G0-6).

### S4: 30-minute agent Action (pass)

| Measure | Result |
|---|---|
| 30-minute run in a Workflow step | 1,801 s, 1 attempt; lease renewed 60 times at 29.987–30.021 s intervals |
| SSE 5 minutes / 64 MB download | Complete and in order / SHA-256 match; 1 GB streamed through a 128 MB isolate |
| Platform limits | None hit on the Workflow path. DO-alarm await and Queue consumers are killed at about 900 s. |
| Exactly-once | Held in all 74 runs: OOM, lease expiry with a zombie, lost and duplicate hand-offs, redeploy mid-run |
| Cost | ≈ $0.0001 per 30-minute Action |
| Design findings | Persist the lease (about 24 rows per running hour); no Queues for Actions; raise `cpu_ms` for streaming (40–100 ms CPU per MB); uploads over 100 MB need presigned upload; deploy skew of 10–40 s (0016 G0-8) |

## Other stage-0 packages

| Package | State |
|---|---|
| P00 contract | Merged (#320, decisions #328 and this PR) |
| P01 scaffold | Merged (#319). CI preview deploys still need the repository secrets that only Gabriel can add (0003). |
| P02 harness | Merged (#327) |
| P03 tenant transfer | Merged (#324) |
| P04 Reducer log feed | Merged (#322). The real staging-day replay moves to M02, because it needs a staging deploy (0015 S0-5). |
| P05 baseline metrics | **Not started.** It changes `packages/client` and needs two weeks on staging, which conflicts with keeping this work independent of `staging` until cutover. Gabriel decides whether to run it as a staging-side change outside this branch. |

## Recommendation for stage 1

**Proceed, with A01 at the front of wave 1.** Spikes found nothing that rules out the design. Every blocker
has a measured workaround. The largest risk is the strict-host schema behavior, so the transform comes first.

Start now (every dependency is merged and every gating decision is made):

1. **A01 schema transform**. It is on the critical path to G1.
2. **K01 storage layer**: transaction and savepoint wrapper (G0-3), the deferred-FK pre-commit check
   (G0-5), `$N` rewrite, read-only Query enforcement and parameter chunking (0011).
3. **K04 module host**: S2's host design, catalog cache, AsyncLocalStorage invocation state, and the R25
   CPU-overrun measurement.
4. **K05 session layer** on gateway objects (G0-1). Gabriel can still overrule the gateway choice; K05's first
   week is placement-neutral (handshake, auth, capabilities, close codes).
5. **K06 control object**: PBKDF2 at 210k rounds outside WebCrypto (0007), tenant routing, membership
   projection.
6. **S5 Worker Loader spike** (new). It decides how `/dev/sync` swaps a generation without dropping sockets,
   and it must report before K19.

7. **R01 outside-in probe** (stage 1b, 0019). It needs nothing from stage 1 and sets the G3b baseline. It needs
   a designated test tenant on staging and production.

Start once A01's transform interface is merged: **K02 migrations and provisioning** (G0-10), then **K03
change capture** (plain-SQL capture comes from A01's T5).

Wave 2 waits on wave 1 as `work-packages.json` says. Stage 1b (R02–R06) follows the kernel packages listed in 0019, and G3b now gates G4. K12 starts on S4's Workflow design. K14 must measure
dead-peer detection under G0-6.

## Decisions Gabriel should confirm at sign-off

1. Gateway objects as the default socket placement (0016 G0-1).
2. For stage 1, `/dev/sync` deploys a Worker version, so active sockets reconnect instead of receiving
   `system.reload`, unless S5 shows the Worker Loader works (G0-2).
3. No SDK ping in stage 1: dead-peer detection for idle sockets depends on the edge's TCP timeout until a
   stage-3 SDK change (G0-6).
4. Platform-forced deviations: the 32 MiB socket cap (0012 R21, 0018) and the 32 MiB `ctx.storage.store`
   cap (R26).
5. Repository secrets for P01's CI preview deploys (0003).
6. P05 (see above).
