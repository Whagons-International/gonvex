# Outbox delivery and recovery contract

A committed queue record represents an accepted operation retained for
reconciliation. Sending it again is not delivery progress. Failed and rejected
records require explicit user retry. Delivery admission and inflight recovery
must preserve those three states and their record metadata.

## Production transition

`packages/client/src/outbox-policy.ts` defines the actual pure transition used
by `markInflightChange` and `markPendingChange` for IndexedDB, generic injected
stores and memory fallback. Only these transitions are allowed:

| Input | Start | Recover |
| --- | --- | --- |
| pending | inflight | pending |
| inflight | inflight | pending |
| committed | committed | committed |
| failed | failed | failed |
| rejected | rejected | rejected |

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
Z3 must return UNSAT for every counterexample query. Six well-typed broken
implementations must reach the solver and return a counterexample.

The checks establish the start/recover table, unchanged committed/parked states,
state-level idempotence and closure of active states. The unchanged-state
property supplies the induction step for any finite sequence of start/recover
actions. A protected initial state is the base case. This is an invariant of
this closed transition system, not a bounded sample of action sequences.

The translator, type parser, solver, stated specifications and ordinary compiler
and JavaScript semantics remain trusted. Valid state/action unions and primitive
strings are preconditions. Persisted input validation is not proved. The proof
does not include metadata, async scheduling, storage transactions, batch result
uniqueness or the rest of the delivery protocol; public adapter regressions check
those integration boundaries. Explicit commit, reject, retry and deletion are
outside this policy and require their own contracts.

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

Run the fail-closed translator tests and proof with the pinned CI Z3:

```sh
node --test verification/typescript/verify-outbox.test.mjs
node verification/typescript/verify-outbox.mjs --z3 /absolute/z3 --output /absolute/proof
pnpm --filter @gonvex/client exec vitest run src/outbox-admission.test.ts
```

CI retains source, solver/verifier hashes, SMT queries and broken variants. A
missing/unknown solver result or timeout is a failure. See `guarantees.md` for
unproved composition boundaries and the next durability experiments.
