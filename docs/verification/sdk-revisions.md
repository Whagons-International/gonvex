# SDK revision policies

The SDK keeps TypeScript, IndexedDB and its current public API. This first step
separates five small revision decisions into `packages/client/src/revision-policy.ts`.
Storage, row mutation, network routing and asynchronous scheduling stay with their
existing owners.

## What the checks establish

The verifier reads the actual TypeScript functions with the pinned TypeScript
6.0.3 compiler API already used by the CLI. It translates a restricted AST into
SMT-LIB and asks the Z3 solver included in the pinned Verus release for a
counterexample to each law. The numerical model uses IEEE-754 binary64, including
NaN, infinities and signed zero. It does not replace JavaScript numbers with
unbounded integers.

- Snapshot revisions are exactly safe integers greater than or equal to zero.
- Transaction revisions are exactly safe integers greater than zero.
- Invalid numerical revisions are rejected, including fractions and unsafe integers.
- Equal revisions may update a projected field or delete a row.
- With finite deletion authority, a row can return only at a strictly newer revision.
- An older revision cannot replace a newer field or delete a newer row.
- Field replacement ordering is transitive for finite revisions.
- Valid incoming revisions preserve recovery from legacy NaN authority.

These are source-linked SMT proofs under the translator and builtin model below.
They are not Verus proofs or a verified TypeScript compiler. The whole SDK and
the full row-merge implementation are not formally verified.

## Trusted boundary

The small AST translator, its specifications, TypeScript parser, Z3, Node and
JavaScript engine remain trusted. `Number.isSafeInteger` is modeled as a finite,
integral binary64 value whose absolute value is at most 9007199254740991. The
translation of comparisons uses SMT floating-point comparisons, preserving NaN
behavior. Boolean expressions have no side effects, so Boolean SMT conjunction
and disjunction preserve the result of JavaScript short-circuit evaluation.
Callers supply primitive numbers and standard builtins are unmodified. Ordering
callers validate incoming revisions before comparing them. The comparisons
preserve the old negated-rejection form, including its unordered-NaN behavior,
so legacy NaN authority does not freeze a row after an SDK upgrade. Ordering
and tie laws are qualified to finite revisions where necessary. This does not
repair every historical malformed cursor or authority value.

The accepted source subset is deliberately narrow. It permits only the five
named, exported functions with fixed numeric parameters, a Boolean return type
and exactly one return expression. It accepts numeric comparisons, Boolean
operators, Boolean negation, zero and `Number.isSafeInteger`. Unknown syntax, imports, mutation,
defaults, additional statements and unmodeled calls fail verification rather
than being omitted. Mutation controls must parse and translate successfully,
then produce a satisfiable counterexample. Unsupported syntax has separate
fail-closed tests.

Native regressions exercise the real transaction queue and row merger. They
check that rejection precedes memory/storage changes and that the next valid
transaction can still run. Storage atomicity, crash recovery, browser eviction,
network ordering, epoch routing, prior persisted authority and async races are
outside the theorem. Adapter contract tests remain useful for those boundaries.

## Reproduce

After the normal frozen workspace installation, use the Z3 executable from the
same checksum-pinned Verus archive as `.github/workflows/rust-policy-verification.yml`:

```sh
node --test verification/typescript/verify-revisions.test.mjs
node verification/typescript/verify-revisions.mjs --z3 /absolute/path/to/z3 --output /absolute/path/to/proof
pnpm --filter @gonvex/client exec vitest run src/revision-boundary.test.ts src/revision-upgrade.test.ts src/replica-record.test.ts
```

CI requires AST/SMT checks through the existing policy verification job and
native SDK regressions through the Runtime tests job.
The proof output retains the exact source, SMT problems, broken variants and a
receipt containing source, verifier and solver hashes. An unknown solver answer,
timeout, parse failure or missing law is a failure.

## Bug found while defining the domain

Staging `79668483b46fd8901aa0beacaf9f6e837d2f92e6` rejected transaction revisions
only when `revision <= 0`. That test admits NaN, positive infinity, fractions and
integers outside JavaScript's safe range. The projected-row merger had no revision
domain check. Such inputs can overwrite row authority or advance a transaction
cursor outside the order assumed by later sync decisions. JSON cannot transport
NaN or infinity directly, but fractions and unsafe numeric revisions are JSON
values, and the SDK's local methods also accept JavaScript numbers directly.
No deployed occurrence is claimed. Reproduction and fixed-run evidence must be
recorded with the PR before treating the correction as validated.

## Following modules

Continue with epoch/cursor admission, snapshot page sequencing and duplicate
rejection, then persistence receipts and optimistic command settlement. Extract
small pure decisions first and keep storage interfaces independent. Loops,
dictionaries and asynchronous protocols need additional modeling before this
translator can prove them. Randomized state-machine tests can find bugs there,
but must not be presented as formal proofs.

References: [ECMAScript Number.isSafeInteger](https://tc39.es/ecma262/multipage/numbers-and-dates.html#sec-number.issafeinteger),
[SMT floating-point theory](https://smt-lib.org/theories-FloatingPoint.shtml).
