# 0007: Password hashes use PBKDF2-SHA256 at 210,000 rounds

Date: 2026-11-10. Status: decided (lead), to be verified by K06.

Rust stores `pbkdf2_sha256$<rounds>$<salt>$<hash>` with 210,000 rounds
(`rust/crates/runtime/src/operations.rs:43`, `3538-3566`; verification in `control.rs:4116`). Workers
WebCrypto has refused PBKDF2 above 100,000 iterations. Existing hashes must keep verifying after import; the
kernel may not rehash at lower cost.

Decision: K06 verifies with a non-WebCrypto PBKDF2 (`node:crypto` under `nodejs_compat` if it accepts the
count, otherwise a pure-JS/WASM SHA-256 PBKDF2), and records the CPU per verification in its PR. If CPU per
verification exceeds 500 ms, K06 stops and reports per Rule 7.
