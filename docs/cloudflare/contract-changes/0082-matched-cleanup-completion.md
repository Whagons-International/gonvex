# 0082: Distinguish matched cleanup authorization from its final sync acknowledgment

Lead clarification of0076/0081,2026-11-12. Narrow phase distinction permitted; implementation proof remains.
No lifecycle acceptance or new authority/retirement result.

Witness retention is mandatory through receipt uncertainty BEFORE matched cleanup authorization: lost
drain reply, invalid/disposal-failed receipt, mismatched current local intent/binding, missing authoritative
terminal witness or non-durable future-confirmation denial cannot authorize witness/credit GC.

AFTER a real valid native closed receipt is consumed/disposed, exact current cleanup authorization is
established, current-book/cleanup transition and denial of future old confirmations are durably published,
the bound witness+credit GC transaction may commit.0076's until-matched-closed retention duty then ends.
A later final storage.sync reply ambiguity does not retroactively invalidate that authorization or demand
indefinite readdressable terminal proof. Do not resurrect old recovery generations/witnesses.

Return UNKNOWN CLEANUP COMPLETION for the ambiguous caller result; never fabricate a successful cleanup
ACK, fresh terminal proof, HMAC retirement, revision/application progress or proof from witness absence.
Keep the distinction from an unknown DRAIN outcome. A positive later terminal callback still needs the
real retained exact witness; absence after authorized GC is not positive proof.

An in-memory conservative credit hold is safe during uncertainty, but is not a completed bounded-churn
solution. Reconcile only the exact authorized cleanup using existing bounded durable cleanup context and
actual atomic record/credit state; absence alone, TTL, a new issuer or foreign receipt is insufficient.
If state cannot distinguish authorized committed GC from pending/rollback/unknown, retain uncertainty
and refuse unsafe admission. No unbounded tombstone/history map or per-frame write is authorized.

Actual wake must preserve future old-confirmation denial in BOTH GC-commit and rollback outcomes, with no
newer owner deletion or stale generation restoration. Test loss before and after actual atomic GC, held/
throwing final sync, wake/repeated cleanup and exact count/byte credit behavior. Partial/non-atomic GC
cannot claim the after-commit condition. Keep failed evidence and original N6/OWNER/platform/fullmatrix/
capacity/security/duration gates open. This is a phase distinction, not permission to infer quiescence.
