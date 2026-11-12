# 0072: Invitation receipt collision is a reference quirk

Date: 2026-11-12. Status: observed in both backends; preserve outside-gate behavior for K22.
Any hardening is a separate decision, not part of the R1 migration-admission fix.

The original Rust `store::claim_reducer` (`rust/crates/store/src/lib.rs:1600`) inserts a receipt and returns
whether the insert changed a row. Its `(subject,idempotency_key)` conflict does not validate stored path.
Invitation acceptance passes `$system.invitation.accept` to it (`runtime/src/control.rs:1701`), but the
unclaimed branch rolls back the tenant transaction and then finishes the Control invitation. It does not
call generic `replay_reducer_result`, whose stored-path validation remains unchanged.

The concrete unchanged-source, actual authorized-principal probe ran against both Rust and assembled
runtime18afb. Bob first invokes `board.add` using key `k22-invitation-collision-0001`; its receipt path is
`board.add`. Alice then invites Bob with role `member`. Bob accepts through the real ControlOnly path using
the same key. Both backends report invitation accepted, preserve the earlier receipt, and fresh Bob
authentication plus declared `principals.whoami` reports `admin` both before and after. A first probe failed
to read a protected members table with404 on both; it is retained and supplies no parity conclusion.

Raw scenario, runner, report, log and result:
`/mnt/data/work/gonvex-k22/invitation-collision-2/`. No operator access, module or Rust code was broadened.
This proves this collision's reference behavior, not a general safety property of invitation receipts.

Preserve outside-gate behavior in K22. R1's exact-path **read-only** already-committed receipt exception
while the migration gate is closed is distinct; other invitation writes remain blocked/transient and keep
their Control claim for the same-key retry. Do not claim a unilateral strict-path change fixes a parity bug.

Potential hardening would require both implementations to validate the stored receipt path for this
special call, with an explicit decision on Control claim, error, retry and partial-completion semantics.
It is deferred here. Generic Reducer receipt/path assertions and every original K22 gate stay mandatory.
