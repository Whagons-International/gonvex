# 0001: Intended behavior the kernel implements instead of copying Rust

Date: 2026-11-10. Status: decided (brief, recorded by the lead). Affects: K07, K11, K12, K13, K18.

The brief names four places where the kernel does not copy the Rust runtime. `CONTRACT.md` marks each
affected row **replace** and points here. Conformance scenarios for these rows assert the new behavior, not
Rust's, and each scenario names this note.

| # | Area | Rust at `0bf3ad13` | Kernel | Package |
|---|---|---|---|---|
| IB1 | Private files | The only owner check is sandbox `importFile` (`rust/crates/runtime/src/storage.rs:341`). Minting a download URL, the signed download itself and `delete` do not check the owner. Uploads accept any caller-chosen `ownerId`. | Check the owner on every download-URL mint, every signed download, every metadata read and every delete. `ownerId` on upload defaults to, and must equal, the calling account unless the caller is a system principal. | K13 |
| IB2 | Actions | Outbox Actions (`actions.enqueue`) retry forever with backoff from 1 s capped at 15 min (`rust/crates/store/src/lib.rs:1343-1383`). Scheduled jobs never retry: `finish` marks a failed run `completed` (`rust/crates/runtime/src/scheduler.rs:627-669`). | Outbox Actions: a maximum attempt count (default 8) and a terminal `dead` state. Scheduled jobs: still one attempt, but a failed run ends `dead`, not `completed`. Dead rows are visible to operators and never run again unless an operator requeues them. | K11, K12 |
| IB3 | Reducer receipts | `_gonvex_reducer_idempotency` is never pruned (`rust/crates/store/src/lib.rs:1140-1168`); nothing reads its `created_at` index. | Receipt results expire after the client's offline window; a small tombstone outlives them. Exact rule in 0009. | K07 |
| IB4 | Webhooks | No runtime route; external adapters invoke Actions (see 0002). | One Ingress Worker for every webhook, verifying signatures on the raw body and invoking the Action through a service principal. | K18 |

The default attempt cap in IB2 and the retention numbers in 0009 are the lead's choice for stage 1 and may be tuned
by a later note; the existence of a cap and of expiry is not negotiable.
