# Outbox delivery and recovery contract

A committed queue record represents an accepted operation retained for
reconciliation. Sending it again is not delivery progress. Failed and rejected
records require explicit user retry before sending again. Positive server
acceptance settles any surviving record as committed. Once committed, rejection,
retry and failure callbacks must preserve that state and its metadata. Delivery
admission and inflight recovery preserve parked records.

## Production transition

`packages/client/src/outbox-policy.ts` defines the actual pure transition used
by admission, recovery and settlement callbacks for IndexedDB, generic injected
stores and memory fallback. The complete state table on existing records is:

| Input | Start | Recover | Commit | Reject | Retry | Transient failure | Exhausted failure |
| --- | --- | --- | --- | --- | --- | --- | --- |
| pending | inflight | pending | committed | rejected | pending | pending | failed |
| inflight | inflight | pending | committed | rejected | inflight | pending | failed |
| committed | committed | committed | committed | committed | committed | committed | committed |
| failed | failed | failed | committed | rejected | pending | failed | failed |
| rejected | rejected | rejected | committed | rejected | pending | rejected | rejected |

The commit action requires positive acceptance evidence from the caller. This
policy does not authenticate a response or establish its replica watermark.
Single and batch commit apply the same transition. A delayed positive result
can settle an intent that an earlier attempt parked. Repeated rejection does
not replace a settled record's metadata. Explicit retry preserves the original
ID, key and arguments while resetting its attempt budget and error fields.

A repeated single-entry `markInflight` may report an already inflight record as
inflight. Batch admission has a stronger contract: return only records changed
from pending by that admission, once per ID. Atomic store callbacks decide on
the stored state, not a prior selection snapshot. Stores without atomic update
support require serialized ownership; this work does not provide cross-process
atomicity for an arbitrary injected store.

## Proof and trust boundary

`verification/typescript/verify-outbox.mjs` parses the real production file with
the pinned TypeScript API and translates its closed string domains, strict
equality and conditional expression to SMT algebraic datatypes. Unknown syntax,
changed domains, extra statements, calls, defaults and type assertions fail.
Z3 must return UNSAT for every counterexample query. Eleven well-typed broken
implementations must reach the solver and return a counterexample.

The checks establish all 35 state/action outcomes and 16 additional laws,
including committed-state preservation across all seven actions, conclusive
positive acceptance, state-level idempotence and parked-state preservation by
delivery/recovery/failure bookkeeping. Committed-state preservation supplies the
induction step for any finite sequence of these actions. A committed initial
state is the base case. This is an invariant of the closed transition system,
not a bounded sample of action sequences.

The translator, type parser, solver, stated specifications and ordinary compiler
and JavaScript semantics remain trusted. Valid state/action unions and primitive
strings are preconditions. Persisted input validation is not proved. The proof
does not include metadata, async scheduling, storage transactions, batch result
uniqueness or the rest of the delivery protocol; public adapter regressions check
selected integration boundaries. Attempt-budget arithmetic, callback identity,
deletion, admission coalescing and storage lifetime require separate contracts.

## Reproduced defects and integration evidence

On staging c9eed7ce, all 14 new public admission/recovery cases fail across
IndexedDB, memory, KV, atomic stores and batch stores:

- `markInflight` can change committed records back to inflight.
- `markPending` can change committed records back to pending.
- Fallback batch admission returns already inflight/committed records; duplicate
  ID input can also produce duplicate batch output on the optimized path.

The fix uses the policy in the common transition callbacks, distinguishes a new
transition from an already inflight record, and deduplicates admission IDs.
The tests preserve idempotent single-entry admission and protect record identity.
An additional real SQLite restart regression exercises the Expo adapter through
the shared SDK. This is a queue API defect reproduction, not evidence of an
observed deployed duplicate mutation or native-phone execution.

At staging `734c7899`, the next settlement regressions find two gaps. Rejection
can replace a committed record, after which explicit retry makes it sendable.
Optimized batch commit also ignores pending, failed and rejected records while
single commit accepts them. Five adapter profiles reproduce the first defect;
the optimized batch profile reproduces the second. A separate real SQLite
adapter test checks the same settlement state across reopen. Real Chromium
checks that an accepted IndexedDB queue entry survives browser crash and cannot
be re-armed by late rejection, explicit retry or failure bookkeeping. These are controlled
queue API schedules, not observed network incidents or duplicated business
effects. The server's receipt deduplication remains a separate protection.

The settlement fix routes all seven state decisions through the source-linked
policy, preserves metadata for ignored callbacks and makes batch commit match
single commit. Tests also confirm late callbacks cannot recreate an acknowledged
record. Missing-record behavior and metadata are executable checks, outside the
five-state proof. A generic store without atomic updates still requires serialized
ownership; the proof does not add transactions to such a store.

Run the fail-closed translator tests and proof with the pinned CI Z3:

```sh
node --test verification/typescript/verify-outbox.test.mjs
node verification/typescript/verify-outbox.mjs --z3 /absolute/z3 --output /absolute/proof
pnpm --filter @gonvex/client exec vitest run src/outbox-admission.test.ts
pnpm --filter @gonvex/client exec vitest run src/outbox-settlement.test.ts
pnpm --filter @gonvex/expo-sqlite exec vitest run src/outbox.test.ts
```

CI retains source, solver/verifier hashes, SMT queries and broken variants. A
missing/unknown solver result or timeout is a failure. See `guarantees.md` for
unproved composition boundaries and the next durability experiments.
