# Verifying Gonvex Rust with Verus

Verus is a practical way to keep Rust and add machine-checked specifications to
small policy modules. The first pilot verifies the production generation
allocation body. No Verus library, macro or proof code enters the normal runtime
build. This is a starting point for modular verification, not a proof of Gonvex.

## What the pilot proves

`rust/crates/server/src/generation.rs` supplies the function called by
`ModuleRegistry::reserve_generation`. For every pair of `u64` issued/active
high-water marks and every optional requested generation, the proof checks:

- A manual request succeeds exactly when it exceeds both high-water marks, and
  returns exactly the requested value.
- Automatic allocation succeeds exactly when their maximum is below `u64::MAX`,
  and returns that maximum plus one.
- Exhaustion returns `None`; successful allocation cannot overflow or reuse a
  number at or below either mark.

`verification/verus/verify_generation.py` copies the executable function body
byte for byte into a Verus harness and inserts its return binder and postcondition.
The body is not a separately maintained mathematical implementation. The harness
has no extra input preconditions and no admitted facts or external body.
Three deliberate mutations must fail verification: accepting equality, reusing
an existing number and removing the exhaustion guard. The receipt records source
and function hashes. The adapter fails if its expected signature changes.

The proof excludes the caller's locks, ledger lifetime, activation and unload
races. Rust tests exercise those behaviors. The adapter and its specification
remain review obligations. Verus's translation, solver and Rust/compiler stack
are trusted; this is not a proof of the compiler or machine code. The normal
runtime uses Rust 1.94.0, while the pinned verifier uses Rust 1.99.1. The small
body uses ordinary integer, comparison, match and Option operations.

## Comparison with Bend laws

| Question | Bend experiment | Rust with Verus |
| --- | --- | --- |
| Can we state universal laws? | Yes, laws about the supported Bend definitions. | Yes, specifications, quantified statements, preconditions, postconditions and invariants about supported Rust code. |
| How are proofs checked? | The experiment used Bend's proof support and Lean kernel checking for its translated pure definitions. | Verus produces verification conditions for an SMT solver, normally Z3. It does not produce a Lean kernel proof in this workflow. |
| Must we rewrite the server? | A Bend runtime requires a substantial rewrite and foreign wrappers. | We can extract one Rust policy at a time and retain the current V8, SQLx, Tokio and TypeScript code. |
| Does it add runtime checks? | Laws alone do not establish performance of the native implementation. | Verification is static. This pilot leaves plain Rust arithmetic in the runtime, with no verifier runtime dependency. |
| Are external effects proved? | The experiment's database and V8 wrappers remain outside its pure proofs. | Existing Tokio, SQLx, PostgreSQL and V8 operations are outside this pilot. Broader proofs need explicit contracts and verified adapters. |
| Is all existing code accepted? | The experiment still lacks most server integration. | Verus supports a Rust subset. Arbitrary async code and dependencies are not automatically verified. |

The two approaches are comparable in the useful sense: a false implementation
can fail a universal law even when sampled tests pass. They differ in language,
proof machinery, automation and trusted components. Neither guarantees that we
wrote the right specification, covered every invariant, or correctly modeled an
external service. A count of proved laws is not a correctness percentage.

## Reproduce

Use the official Verus release `0.2026.10.18.7d82994`, commit
`7d829946231d6413a398f6e4160cdfe951261dad`, with Rust toolchain
`1.99.1-x86_64-unknown-linux-gnu`. The Linux release SHA-256 is
`e9af03732d4bdd32b93a8fcf471706739c24c1473fa5086618d96c47237845dd`.
The CI workflow pins the release and verifies that digest.

```sh
python3 verification/verus/verify_generation.py \
  --verus /absolute/path/to/verus \
  --output /absolute/path/to/build/generation-proof
```

The production harness must report verification success. Each mutation must
report a verification failure, not just a syntax or installation error.
`receipt.json` records all four outcomes. The committed example is
[verification/verus/results/generation.json](../../verification/verus/results/generation.json). This check runs separately from normal
Rust compilation so the verifier's toolchain does not upgrade the runtime.

On Gabriel's workstation, the pinned verifier is under `/mnt/data/tools/verus/`
and its separate Rust toolchain is under `/mnt/data/tools/verus-rustup/`.
The local `verus` wrapper selects those paths.

## Recommended next steps

Keep Rust as the implementation while evaluating verification module by module.
Next candidates are deadline budget selection, frame length validation, typed
statement-key identity and transaction completion decisions. Give each module a
small executable policy, explicit laws and deliberately broken controls.
Preserve integration tests for the database and V8 effects behind those policies.

Start by proving safety and functional properties. Eventual cleanup, fairness,
deadline scheduling and distributed liveness need more than a pure function
contract. Storage adapters should specify logical rows, ordering, transaction
atomicity and error behavior independently of IndexedDB or any future backend.
Verus checks Rust, not the TypeScript SDK; shared wire fixtures and conformance
tests can keep the SDK aligned, but they are not a formal proof of TypeScript.

Proofs add verification work to CI, so measure that separately from Rust compile
time. They need not add runtime overhead. Any executable refactor, cache change
or database round trip still needs benchmarks; no end-to-end speed claim follows
from this arithmetic proof.

## Primary references

- [Verus overview](https://verus-lang.github.io/verus/guide/): static verification,
  SMT solving, supported scope and compiler/verifier trust boundaries.
- [Specification, proof and executable modes](https://verus-lang.github.io/verus/guide/modes.html).
- [Official installation instructions](https://github.com/verus-lang/verus/blob/main/INSTALL.md).
- [Pinned Verus release](https://github.com/verus-lang/verus/releases/tag/release/0.2026.10.18.7d82994).
- [Bend source and language documentation](https://github.com/bendlang/bend).
