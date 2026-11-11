# 0002: Webhooks have no route in the Rust runtime

Date: 2026-11-10. Status: decided (lead). Affects: K18, intended behavior IB4.

## Finding

The Rust runtime at `0bf3ad13` serves no webhook or HTTP-action route. The router in
`rust/crates/runtime/src/lib.rs:552-583` merges `native_auth`, `operations` and `operator_data` and has no
fallback. `packages/module-sdk/README.md:260-263` states the model: "An infrastructure adapter that accepts
an inbound webhook verifies and normalizes the request, then invokes an Action; webhooks are not a fourth
module function kind." Telnyx and GitHub webhooks are verified outside this repository today.

## Decision

- The module contract does not change: webhooks are not a function kind, and a webhook still ends as an
  Action invocation (through a service principal, see `CONTRACT.md` C.1 rows 61-62 and C.3).
- The Ingress Worker (K18) **replaces the external adapters**, not a Rust route. Its reference is the
  adapters themselves, which K18 must locate and list (Telnyx, GitHub, any others) before it starts, and
  record in a follow-up note here.
- K18's `done_when` stands: Telnyx and GitHub signature checks pass on forwarded raw bodies.
- Conformance has no Rust-against-Rust scenario for webhooks. K18 adds scenarios that compare the Action
  invocation (path, args, principal) the adapter makes today with the one the Ingress Worker makes.
