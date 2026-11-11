# 0015: Rulings from stage-0 package reviews

Date: 2026-11-11. Status: decided (lead).

| # | Package | Item | Ruling |
|---|---|---|---|
| S0-1 | P02 | Is the `error` text of a failed call contract, or only `class`/`retryable`? | **The first line of `error` is contract** and is compared exactly, including Rust's host wording (`CONTRACT.md` B.2a, B.5, A.5). JS stack-frame lines after it (`\n    at …`) are **not contract**; the harness strips them. |
| S0-2 | P02 | Digest values normalized to `<digest>` | Comparing across backends is not required, but each backend's digests must be **correct**. The harness checks this with a `digest-drift` mutation and, where needed, by recomputing per backend. |
| S0-3 | P02 | `error.*` telemetry frames ignored in frame sequences | Accepted for sequence comparison (they are timer-driven). K15 adds a dedicated scenario for them. |
| S0-4 | P02 | Rust reference behaviors: a duplicate key with different args returns the stored result; `clientContract: 0` is accepted; tenant ids are unique per runtime | **Keep** all three. |
| S0-5 | P04 | `done_when` says "one day of staging calls"; P04 replayed a scripted 1,500-call day on two fresh Rust instances | **Accepted for stage 0.** Recording real staging calls needs a staging deploy with `GONVEX_REDUCER_FEED=1`, which needs Gabriel's approval. M02 runs the real-day replay before G4, and it must first check staging schemas for wall-clock defaults (P04's risk list). |
| S0-6 | P04 | `_gonvex_reducer_feed` is never pruned (about 1 KB per call) | Not a stage-0 issue (off by default). Before the feed is turned on in staging, P04 or M02 adds pruning once the shadow has acknowledged a row. |
| S0-7 | P03 | The transfer tool does not keep the epoch by default and does not store `database_url` | Accepted for P03. The duties move to M03 (0008 amendment). |
| S0-8 | P03 | Separate pnpm workspace pinned to wrangler 4.152.0 (P01 pins 4.155.0) | Accepted for stage 0. It moves under the `cloudflare/` workspace and P01's pin when K-wave tooling needs it. |
| S0-9 | P03 | Read-only `.backup` copies of staging tenant files taken inside the staging app container | Within the brief: it allows copying staging tenant files. Future packages copy files the same way (read-only, server temp deleted) and record it in the PR. |
| S0-10 | P02 | After a re-auth without `subscriptionsPreserved`, Rust reuses delivery-v2 stream tokens, so `expandReplicaBatch` on the client still expands to the dropped subscription id ("phantom" frames the SDK ignores) | **The wire is the contract.** The kernel sends the same `replica.stream` bindings and tokens Rust sends. No client change. The harness keeps comparing expanded traces as it does. |
| S0-11 | S3 | S3 numbered its notes 0015 and 0016, colliding with this note | The lead renumbered them to 0017 (liveness) and 0018 (message caps) before merging S3. |
