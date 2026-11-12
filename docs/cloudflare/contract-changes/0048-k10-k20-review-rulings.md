# 0048: Rulings from the K10 and K20 reviews

Date: 2026-11-12. Status: decided (lead), except §2.2, which goes to Gabriel.

Numbering: K10 keeps **0046**, K20 renumbers to **0047**, and this note is 0048. The next free number is 0049.

## 1. K10, replica delivery (#374): changes requested

The delivery evidence meets the bar locally:
- Rust vs kernel: thin-sdk-parity, reducer-second-client (G1), replica-resume, visibility-masks, replica-windows,
  replica-reset and replica-reconnect-wave;
- G1 call → delta p50 74 ms.

Required: the missing `connectionReset` on K07's tenant object, a rebase onto K17, and no SQL per subscription
per commit in `feed.ts` `affectedRows` (design rule).

| # | Ruling |
|---|---|
| a | **One commit channel.** `ReplicaDelivery` is the single tenant→gateway commit channel. K08's live-query frames ride in its per-socket list ahead of replica frames, and K05's `DeliveryBatch` is retired for commits. Two unordered RPC channels could reorder live-query and replica frames. |
| b | 0046 §2.1, §2.4, §2.7 and §2.8 accepted (including the edited K14 test). |
| c | Live-query resubscribe on tenant restart, the `operatorDataChanged` binding and the K08 wiring go to the **assembly package (K22)**, due before V01, unless K10 does them in its fix. |
| d | Closing with **1011** when a socket's collections span two epochs is accepted. |
| e | The watermark self-check covers subscribed sockets only, because Rust itself sends non-monotonic watermarks to a socket with no subscriptions. Accepted. |

## 2. K20, sandbox (#372): changes requested

1. **The stage-1 contract is the Container path** (CONTRACT B.2a): `rust/crates/sandbox-worker` unchanged as the
   container image, with DuckDB and Rust's limits. Imports go through K13's `ActionFiles.readFile` and its owner
   checks (IB1). The Whagons assistant's SQL is DuckDB SQL, and changing the dialect is module-visible.
2. **For Gabriel:** K20 built the stage-3 L09 design (Dynamic Workers plus DO SQLite) instead. Its measurements:
   first run 638 ms; a 200k-row CSV imports in 1.9 s and groups in 530 ms; 128 MB heap; deadline overshoot to
   about 5 s for spins and 12 s under a host-call flood. The work moves to `cloudflare/spikes/l09-sandbox/` as
   the L09 candidate. Before anyone pulls L09 forward, measure how often real model-written SQL uses
   DuckDB-only syntax.
3. Accepted for either path:
   - concurrency limited per tenant, not globally;
   - the sandbox in its own script with its own `cpu_ms`, while the tenant script stays at 5,000 ms;
   - no conformance scenario yet (V01 item, K13 precedent).
4. K19's release builder needs a Container regardless (0028 §3.1), so K20's Container work also serves K19.

## 3. Package added

**K22, assembly.** It composes the merged packages into the deployed Worker, replacing K05's stubs:
- K07's `ReducerTenantObject` as the tenant class;
- K06 behind K05;
- K08, K10, K12, K13, K15, K16 and K17 ports into the session and tenant object;
- live-query resubscribe;
- `operatorDataChanged`;
- K15's `Ports.telemetry`;
- promotion of K17's `harness/control.ts`.

`done_when`: every Rust-vs-kernel scenario that passed per package passes against the single deployed kernel
Worker (local workerd, then a `gonvex-cf-dev-*` deploy). It depends on K10, K15, K16, K18, K20 and K21, and V01
depends on it.
