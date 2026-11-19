# 0094: run a socket's interactive agent Action in the tenant object's isolate (proposal, needs lead decision)

Status: **approved by coordinator 2026-11-19; Alternative B** (per-tenant ActionHost Durable Object, below).
Proposed by X13f (2026-11-19). Proposal A (in-tenant execution) is not adopted. Implementation: X13g.

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

## Already live and not enough

The lead's correction: #502 (the Action-only bundle) and the dev catalog prune are already in 34562a4a, the release the verifier measured. The cold-per-call cost above (238–297 ms CPU per rewrite) is therefore measured *with* both in place. They shrank the penalty but did not make reuse happen.

## Alternative B (lead): a per-tenant ActionHost Durable Object, kept alive only while the tenant has live subscribers

**Shape.**
- An `ActionHost` DO class in the Actions Worker script, one per tenant: `getByName(tenantId)`.
- It runs a socket's interactive agent Actions with X13's inline runner. The durable start and completion records stay in the tenant.
- It is held resident by the same no-expiry rule as the tenant (BUG-9, `SubscriberKeepAlive`): one pending timer, re-armed every 60 s, only while the tenant has a live subscription.
  - The tenant tells the host when its subscription state changes (`hold` / `release`, with no reply needed).
  - It is session-tied, not a synthetic ping, and is released when the last subscriber goes.

**Feasibility, measured locally (miniflare/workerd, 2026-11-19).** A DO in a Worker that lazily evaluates the real 678bdc80 bundles on first use, with the module cached at module level in the isolate (`/home/gabriel/work/gonvex-x13f/run.mjs`):

| | Action-only bundle (3.0 MB, #502) | Complete bundle (6.3 MB) |
|---|---|---|
| Cold: first call RTT (isolate start incl. ~11 MB script load + evaluation) | 715 ms | 729 ms |
| Cold: evaluation inside the DO | **88 ms** | 246 ms |
| Warm: next calls on the same object, RTT | **3–5 ms** | 2.5–3.6 ms |
| A second object in the same isolate | 1 ms in DO (module cache shared) | 1 ms |

- **The evaluated module stays warm across calls for as long as the object (and its isolate) lives.** That is the property the stateless Actions Worker lacks on the platform.
- **Holding works on dev.** Durable Objects are kept from hibernating by a pending timer, which BUG-9 proved on dev: 30-minute sessions with no tenant resets. So an ActionHost would keep its isolate for the session.
- **Not proven on the platform:** whether the platform still evicts a held DO's isolate under machine pressure. The tenant's own CPU in the same rounds (0–5 ms outside commits) suggests it does not.

**Expected costs on dev:**
- **Cold:** paid once per held lifetime, at the first Action of a session. That is about 0.1 s CPU with the Action-only bundle, plus isolate start.
  - It can be paid before the first rewrite, by warming when the tenant gains its first subscriber. That is still session-tied, an evaluation at session start, which is what Rust's module host does at activation.
- **Warm:** about the tenant→ActionHost RPC hop (~15–30 ms on dev, comparable to today's tenant→Actions Worker hop) plus the Action's own CPU (7–14 ms per rewrite). That removes the ~0.25 s per-call cold penalty the verifier saw.

**Compared with proposal A (in-tenant execution):**

| | A: in the tenant isolate | B: per-tenant ActionHost DO |
|---|---|---|
| Determinism boundary | Actions share the Reducers' event loop | Separate object; Reducers untouched |
| CPU limit | the runtime Worker's 5 s per event (X10) | the Actions Worker's 300 s (`cpu_ms` 300000) |
| Capabilities | the tenant gets fetch and secrets for the call | unchanged: Actions Worker bindings (FILES, SANDBOX, network) |
| Warm cost per rewrite | Action CPU only | Action CPU + one DO RPC hop (~15–30 ms) |
| Cold cost | once per tenant isolate | once per session (or prewarmed at first subscriber) |
| Extra billing | none | DO duration while subscribers exist (same rule as the tenant's, already accepted) |
| Contract change | 0016 G0-8 for one path | 0016 G0-8 kept in spirit: Actions still run in the Actions Worker script, in a DO class |

**Recommendation: B.** It keeps Actions out of the deterministic tenant and under the Actions Worker's limits and bindings. It costs one warm RPC hop that A avoids, and it reuses an already-accepted keep-alive rule. Scope it to socket-initiated `profile: "agent"` calls (X13's inline path). Outbox and scheduled Actions stay on Workflows.

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
