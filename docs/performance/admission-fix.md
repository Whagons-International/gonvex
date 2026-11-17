# Snapshot admission: claim on poll (perf/admission-fix)

Branch `perf/admission-fix` from `perf/integrate-5k` 96c408c0. Problem (step log, round 15):
at 1k production the two snapshot slots were busy only 16–30 % of the time while permit wait p90
was 0.8–4 s. In g2-1k-prod-fp, 69.5 builds/s × 6.8 ms mean `permit_hold` = 24 % of 2 slots,
`snapshot.permit_waiting` peaked at only 12, and permit wait p90 was 3.9 s. A queue of 12 with
~7 ms service should wait ~40 ms, so slots sat taken but unused.

## Mechanism (corrected)

`SnapshotAdmission::release` handed a freed slot to the head waiter's oneshot and counted it as
used at once; the slot became a build only when that waiter's task ran again. The waiter is *not*
polled by the connection's `select!` loop: every replica preparation (including `admit`) runs
in a task spawned on the offload runtime (`replica_open_queue::start` → `offload().spawn`).
But on 1 vCPU that runtime is one thread at nice 10. It shares that thread with feed applications
(up to 64-event batches since burst control round 2), retire drops and snapshot encoding, and
competes with the nice-0 network thread for CPU (on the lab, 19–31 % of periods throttled). A
granted waiter therefore waits for that thread. A waiter whose task was aborted (a closed window,
a dropped socket) also kept the slot until the runtime got to dropping it. Either way the slot
counted as busy while nothing ran, and `permit_hold` (which starts only after the waiter resumes)
did not show it.

## Change

`offload.rs` `SnapshotAdmission`: **slots are no longer handed to a waiter; an offered waiter
claims one when its task runs.**
- `release` (and a waiter leaving) increments `free` and calls `offer`. `offer` walks the lanes
  in the existing dispatch order (FIFO; with the opt-in lanes: short first, long ≤ `count − 1`,
  aging) and marks the next `free + 2` waiters as offered, waking them.
- An offered waiter's future (`SlotWait::poll`) takes a slot only if one is still free (and, for
  a long build, the long cap or its age allows). The first offered waiter that actually runs gets
  it. A waiter that lost the race re-registers and is woken again by the next offer.
- A waiter dropped unclaimed (deadline, cancellation, socket gone) removes itself and, if it had
  been offered, offers on. No slot can be held by a waiter that does not run. No permit can
  leak: a slot exists only as a `Slot` value owned by a running build.
- Unchanged: the bounded queue (`GONVEX_SNAPSHOT_MAX_WAITING`, deadline
  `GONVEX_SNAPSHOT_ADMISSION_TIMEOUT_MS` → retryable `replica.error`), the byte budget, the
  fast path for arrivals into an empty lane, cancellation by the open queue (`remove`, `clear`,
  socket drop abort the preparation task, whose drop leaves the lane), and burst control's
  feed-vs-install ordering (`admission_wait` still marks the preparation not reading for the
  whole `admit`, and reading again only after it returns with a permit).

New stages, so the lab can see where a slot's time goes:
- `snapshot.grant_lag`: latest offer → claim (the slot stays free meanwhile, so this is now
  latency only, not idle capacity);
- `snapshot.budget_wait`: slot held while waiting for the build's estimated bytes;
- `snapshot.slot_hold`: slot taken → released (budget wait + permit hold). Slot utilization =
  Σ `slot_hold` / (count × window); compare with Σ `permit_hold`.

Tried and dropped (`admission-fix-results/af-heavy.jsonl`): a separate snapshot runtime (`gonvex-snapshot` thread) so granted builds do
not queue behind feed jobs. On the driver it was clearly worse (220 + 150 users: save p95 0.8 s
→ 6.0–6.6 s, 23 % fewer saves). A second nice-10 thread splits the leftover CPU with the feed
thread, and deliveries (which saves wait for) slow down.

## Measurements (burst driver, `benches/burst_control.rs`, heavy point 220 + 150 users, think 0.8 s, 40 s)

Old handover vs claim-on-poll, interleaved, 2 reps each (`admission-fix-results/af-heavy2.jsonl`).
Slot and permit utilization are Σ hold / (2 × window); values slightly above 100 % come from
builds spanning the window edges.

| variant | saves done | save p50 / p95 / p99 (ms) | open p50 / p95 / p99 (ms) | reconnect p50 / p99 | permit wait p50 / p90 / p99 | slot use | permit use | builds | CPU ms per save |
|---|---:|---|---|---|---|---:|---:|---:|---:|
| handover (before) | 2,470 / 2,928 | 50 / 1,017 / 1,599; 44 / 576 / 1,164 | 3,833 / 7,007 / 8,053; 3,131 / 5,718 / 6,409 | 9.0 / 17.2 s; 8.1 / 15.5 s | 3.7 / 6.3 / 7.4 s; 3.0 / 5.2 / 6.3 s | 102 %, 105 % | 98 %, 102 % | 11,063 / 12,820 | 15.9 / 13.6 |
| **claim on poll** | **2,784 / 3,106** | 45 / 506 / 962; 36 / 713 / 1,155 | 3,569 / 6,103 / 8,779; 2,694 / 6,371 / 6,761 | 9.2 / 14.1 s; 9.6 / 28.8 s | 3.5 / 5.9 / 6.4 s; 2.6 / 5.3 / 7.0 s | 104 %, 102 % | 104 %, 102 % | **12,204 / 13,439** | **14.5 / 12.7** |

On the driver the old handover already kept the slots ~100 % used (grant lag p99 1.3–1.4 ms):
the driver does not reproduce the lab's idle slots, so it can only show that claim-on-poll costs
nothing. It completes 6–13 % more saves and builds per run with the same CPU, and the latency
tails are within run-to-run noise. Byte-budget waits are ~0 on the driver (Σ 0.03–0.15 s). The
effect on the lab gap must be measured in the lab: with the new stages, `slot_hold` vs
`permit_hold` vs `budget_wait` vs `grant_lag` tells which cause (late waiters, budget waits)
held the slots there.

## Not measured

- The 1k production lab run (the lab was held by integration's runs throughout; handed to
  integration). Look at slot utilization (Σ `slot_hold`) vs permit wait, and at `budget_wait`.
  If `budget_wait` is large, the next step is to stop holding a count slot while waiting for
  bytes.
- Open/save tails at 1k with this change.
