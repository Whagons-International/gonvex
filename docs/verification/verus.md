# Verifying Gonvex Rust with Verus

Verus is a practical way to keep Rust and add machine-checked specifications to
small policy modules. The generation and capability checks verify production
function bodies. No Verus library, macro or proof code enters the normal runtime
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

## Capability policy

`rust/crates/module-runtime/src/capabilities.rs` owns the Query/Reducer/Action
capability policy. V8 calls this shared module API. The policy has no allocations,
I/O or runtime verifier dependency. The host grant remains a separate input.

`verification/verus/verify_capabilities.py` copies the actual datatype
declarations and both executable function bodies into the verifier. It inserts
return binders, postconditions and a law harness. It proves the exact structural
ceiling for each kind and, for every Boolean host grant, the exact intersection,
absence of escalation beyond either input, and idempotence.

The check deliberately removes each of the ten host-grant gates and each of
the ten kind gates. All twenty variants must fail verification. A syntax error
does not count as detecting a broken law. New kinds or capability fields require
reviewing the adapter and specification. Receipts identify production sources,
the adapter, runner and every generated harness by SHA-256. Incomplete runs and
changed inputs cannot produce a successful receipt.

The proof excludes Serde derives, serialization, V8 dispatch, host operation
enforcement, and the compiler/ABI. Normal Rust and V8 tests still exercise the
adapter that calls the verified policy. The specification itself remains a
review obligation, including the intended capabilities of each function kind.

Run `python3 verification/verus/verify_capabilities.py --output /path/to/proof`
with the pinned verifier on PATH, or pass `--verus` explicitly.

The runtime workflow calls the reusable policy workflow in a separate Rust
1.99.1 job. Its `CI` aggregate requires both runtime tests and policy proofs to
succeed. A failed, skipped or cancelled proof job cannot leave that aggregate
green. Production compilation continues to use Rust 1.94.0.

The [proof receipt](../../verification/verus/results/capabilities.json) records
the production check and all twenty failing controls. The
[CPU benchmark receipt](../../verification/results/capabilities-benchmark.json)
compares pinned V8 policy bodies with the actual shared crate API, using the
unchanged ABI types. All 3,072 kind/grant combinations agree before timing.
Local paired medians were 1.84 and 1.78 ns per call. These samples do not measure
V8 dispatch, server throughput or workspace build time. Reproduce with
`python3 verification/benchmarks/capabilities.py --build-dir /path/to/build`.

## Frame length admission

The module-host reader and writer share `framing.rs::frame_length`. Verus
checks the actual function body for every `usize` size and configured limit.
Accepted sizes are nonzero, fit the configured limit and fit the `u32` wire
header. The returned length equals the input, and rejection occurs exactly
when one of those conditions fails. Empty, configured-limit, wire-limit and
length-truncation mutations must each fail verification.

This proof covers the pure decision. Rust tests check that rejected writes
emit no bytes and rejected headers never poll the payload reader. Normal
round-trip tests still exercise framing and JSON. Allocation, Tokio scheduling,
transport correctness and error strings remain outside the formal proof.
The runtime's required proof job runs this check alongside generation and
capabilities. Production Rust gains no Verus dependency.

Run `python3 verification/verus/verify_frame_length.py --output /path/to/proof`.
The [proof receipt](../../verification/verus/results/frame-length.json)
records the production check and all four controls. The
[CPU benchmark receipt](../../verification/results/frame-length-benchmark.json)
compares the old and shared length adapters, using their production bodies and
error fields. Nine alternating paired samples, pinned to one CPU, measured
100 million successful checks each. Local medians were 0.733 and 0.717 ns per
check. This measures length validation only, excluding transport, allocation
and server throughput. Other workstation jobs continued during measurement.
Reproduce with `python3 verification/benchmarks/frame_length.py --build-dir /path/to/build`.

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
Next candidates are deadline budget selection, cursor admission, typed
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
