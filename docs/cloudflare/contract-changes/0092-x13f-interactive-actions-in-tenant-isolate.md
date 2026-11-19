# 0092: run a socket's interactive agent Action in the tenant object's isolate (proposal, needs lead decision)

Status: **proposed** by X13f (2026-11-19). It changes 0016 G0-8 ("Actions run in the Action Worker") for one
path only. Nothing is implemented. This note asks for a decision.

## Evidence (dev Workers Logs, the verifier's rounds in #505, 2026-11-19 00:11–00:30Z, actions version b1eb4daa)

- **The Action isolate is almost never reused.**
  - All 8 interactive `ActionWorker` rewrites used 238–297 ms CPU, the cold-isolate cost.
  - About 55 `ActionWorkflow` runs (scheduled/outbox Actions, one every 2–5 s during the session) used 100–300 ms each. Only 2 used about 2 ms.
  - Every outcome was `ok` and the script version was constant, so this is not redeploys or crashes.
- **Our in-isolate cache works when an isolate is reused.** x13r3 warm rewrites used 7–14 ms CPU (the per-isolate `ModuleRegistry` and the lazy exports, #495).
- **Per-isolate memory is modest.** Heap after loading the Whagons bundle and manifest is about 35 MB (node), plus the 26.5 MB script. This looks like platform placement or eviction, not memory-limit kills. That is inferred, not proven.
- **What Rust does.** The module host is one resident V8 isolate per project, evaluated at activation and reused for every call (`module-runtime-v8`). No Workers API keeps a stateless or Workflow isolate resident, and synthetic keep-warm pings are excluded unless they can be shown to match Rust.
- **The tenant object's isolate does stay warm.** Its RPCs in the same rounds used 0–5 ms CPU outside commits, and subscriber keep-alive (BUG-2/9) holds it while sockets are subscribed. That is the closest Workers analogue to Rust's resident host.

## Proposal

Run a **socket's direct agent Action** (`action.call` from a session, X13's inline path) inside the tenant object's isolate.
- The tenant already holds the app's Query/Reducer bundle. It would lazily load the Action bundle (#502) once per isolate lifetime.
- Durable start and completion records stay as in X13/X13b.
- Outbox and scheduled Actions keep the Workflow path (durability, long runs).
- Opt-in per Action, or limited to `profile: "agent"` calls with no `sandbox` and no `storage` capability.

## Costs and risks for the lead to weigh

- **The Action's CPU runs on the tenant's event loop.**
  - I/O waits (provider calls) do not block it, but CPU does: about 7–14 ms per warm rewrite.
  - A CPU-heavy Action would delay that tenant's Reducers. Mitigation: the opt-in list and a per-call CPU budget.
- **The runtime Worker's CPU limit (X10: 5 s per event) applies** instead of the Actions Worker's 300 s. Hence the agent-only, no-sandbox scope.
- **Capability separation.** The tenant gets `fetch` to the Action's declared origins, plus project secrets for that call. The Action host code (`actions/src`) is reused unchanged.
- **Expected effect on create (a):** about −0.25 s on every rewrite whose Action isolate would have been cold. Most "warm" creates in #505 were like that, so the measured warm (a) gap of +360 ms would shrink to roughly provider variance.

## Alternatives already in flight

- **#502, opt-in Action-only bundle:** cold Action CPU about −60% (import 320–346 → 120–147 ms locally), so a cold rewrite costs about 0.1 s instead of about 0.27 s.
- **Pruning the 109 stale dev conformance projects (approved):** about −0.2 s compile per cold isolate on dev only.
- These two shrink the per-call penalty but do not make reuse happen.

## Also measured: (b) pre-commit, warm 348 ms vs Rust 163 ms

The 00:29:05 / 00:29:28 / 00:29:50 warm creates, from Workers Logs and #500's slow-call log:

| Segment | CF | Rust |
|---|---|---|
| Frontend sends the rewrite after submit | ~100 ms | same |
| Rewrite answer → the create's tenant dispatch | 6–26 ms (the depth-guard alarm fired within ~1 ms, so it costs ~0) | — |
| Create tenant event | **167–176 ms wall, 43–53 ms CPU** | — |
| Network | ~55 ms RTT (verifier's allowance) | none (local) |

Inside the create's tenant event, the slow-call log shows queue wait plus async run under 100 ms (no create crossed the threshold). The rest, about 120 ms with no CPU, is the platform holding the reply until the commit is replicated and durable. Rust's reference waits for a local Postgres WAL fsync only.

So **(b)'s CF-specific excess is about 120 ms of durable-write latency, a platform property, plus about 30 ms of extra CPU.** Neither the gateway nor the Action path contributes.

**Cuttable on our side:**
- the create's CPU, by profiling capture and visibility work on a 1,997-function app;
- the number of storage writes in the create's commit, if the gate scales with them (unmeasured).

**Not cuttable:** the platform's replicated durability.
