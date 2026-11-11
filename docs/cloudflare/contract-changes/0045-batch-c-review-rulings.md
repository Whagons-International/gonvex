# 0045: Rulings from the batch-C review (K12, K13, K15, K16, K17, K18, K21)

Date: 2026-11-11. Status: decided (lead).

## Note numbering (0024 §5)

Four PRs proposed 0040. Assigned by PR order:

| Number | Note |
|---|---|
| 0040 | K21 (#366) |
| 0041 | K15 (#369) |
| 0042 | K16 (#367) |
| 0043 | K18 (#368) |
| 0044 | K17 (#370) |
| 0045 | this note |

The next free number is 0046.

## Merged

- **K12 (#362):** one claimer per table (K11 owns `_gonvex_action_outbox`); direct agent runs are in
  `_gonvex_st_action_runs`. The 30-minute agent Action completes through K11's scheduler.
- **K13 (#354):** its first two CI runs stuck in GitHub's queue (2.5 h, inconsistent state), and an empty commit
  retriggered them. No code change.
- **K17 (#370), approved:**
  - Edits outside its paths are accepted, including the A01 `capture.ts` expression-depth fix. That fix repairs
    the `cloudflare-reducer` conformance fixture, which could not provision (depth about 111).
  - `kernel/principals/harness/` is test-only. The K19 composer promotes `harness/control.ts` rather than writing
    a second adapter.
  - "Connects unchanged" rests on proxy evidence: the consumer contract passes on both runtimes, and the
    scenario replays the gateway's service flow. **V02 runs the real gateway binary**, member sockets included.
  - `GONVEX_SERVICE_PRINCIPALS` becomes a Secrets binding.

## Changes requested

**K21 (#366)** merges as part 1, and the package stays open until assembly. Final `done_when` runs in V02 through
the real bridge.
- `depends_on` gains K10, K15 and K17.
- Rule 5 wording: the app patch lands only at cutover.
- §3: K13 signed URLs use the router's public host. Accepted.
- §2 (patch vs fallback), §4 (cutover routing) and §5 (dropping the Convex-SDK path) go to Gabriel at cutover. The
  lead's recommendation: the patch, plus a router rule for mobile-alias projects.

**K16 (#367)**
- `done_when` is amended: transport limits plus one passing anonymous call per kind here. The seven named calls
  become a V01 item.
- One deadline rule: K12's runtime applies the 60 s cap, and K16 passes `actionTimeoutMs` into it.
- The limiter refuses upgrades when unreachable (`admit`), while call charging (`allow`) stays fail-open.
- Slot maps are persisted, so an evicted limiter cannot reset the caps.
- The per-address limiter object is accepted (0012 R11). So are the edits at K05's hook points.

**K18 (#368)**
- Security fixes are required: an exact-case tenant cache key, and rate limiting before tenant lookup with
  bounded misses.
- §3: channel `system` is set by K17's tenant side from the principal's configuration, never taken from the request.
- §4: K17's tenant service path is the only service-call implementation.
- §6: verification inside the Action is accepted. **0001 IB4 is amended:** "One Ingress Worker for every webhook,
  forwarding the raw body; signature verification happens in the invoked Action or the Ingress Worker, never in an
  external adapter."
- Rust-referenced webhook scenarios are deferred to V01.

**K15 (#369)**
- Fix the live-session leak when presence v2 is off, and the dropped `__proto__` keys.
- §1: Analytics Engine as the sink of record, with a bounded SQLite window. Accepted.
- §2: a 30-day support-session prune and a 1,000-key cap on impact maps. Accepted as **deviations**.
- Rate limits are per gateway (0012 R11).
- Golden replay is accepted for `done_when`. A live Rust-vs-kernel `error-telemetry` run is required once
  `Ports.telemetry` is wired (assembly).

## Assembly (new, needs an owner)

Several merged packages are wired only in tests, and the main Worker still uses K05's stubs:
- K07's `ReducerTenantObject` as the tenant class (0037 K19 follow-up);
- K06 behind K05;
- K08, K12, K13, K15 and K17 ports into the session and tenant object.

The lead recommends a dedicated **assembly package (K22)**, owned by one agent, before V01. Until it lands, every
Rust-vs-kernel scenario that needs more than one package runs through per-package test harnesses only.
