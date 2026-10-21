# Runtime contract v1

`vectors.json` is the public behavioral contract consumed by the Rust V8 tests
and the SDK tests. G3's harness can import the same vectors and `runner.js`.
These are outcomes and known-answer bytes, rather than checks for API presence.

The isolate provides raw and JWK HMAC SHA-1/SHA-256 and AES-GCM keys, RSA PKCS#8,
SPKI and JWK RSASSA-PKCS1-v1_5 SHA-256 keys, and raw, PKCS#8, SPKI and JWK Ed25519
keys. SHA-1/256/384/512 digests run in ring. AES-GCM supports 128/256-bit keys,
96-bit IVs and 128-bit tags. Other IV/tag/key sizes fail explicitly. It preserves
the WebCrypto ciphertext-plus-16-byte-tag representation used by existing
GitHub and M365 sealed secrets. Imported key material stays in native opaque
objects reclaimed by V8's garbage collector. There is no JS key-byte registry.

Queries and Reducers pin Date.now, Date(), and argument-less new Date() to
ctx.now. Explicit Date arguments, UTC, parse and subclasses retain their usual
semantics. Math.random requires a 64-character lowercase hex intentEntropy;
the shared sfc32 implementation supplies a fresh deterministic sequence per
invocation. This is not a cryptographic random source. Secure random bytes and
UUIDs are available to Actions. Module initialization has no invocation clock
or random source. Actions retain real wall time and secure entropy.

Generated local reducer bundles inject lexical Date, Math and crypto bindings.
They do not patch browser or test-process globals. Invocations in a shared
generated bundle run serially so each keeps its own clock across awaits.
This wrapper belongs to generated bundles, not raw SDK reducer definitions.
Handler errors always clear the invocation state. The Rust suite repeats the
same cases in a recycled isolate to catch state leaking between calls.

Run `cargo test --manifest-path rust/Cargo.toml -p gonvex-module-runtime-v8
published_runtime_contract_vectors` and `pnpm --filter @gonvex/module-sdk test`.
Vectors originate in F2/F3, 2026-10-18. The base64 case reproduces the live
testign relay webhook's missing-atob failure from 2026-10-20. RSA fixtures are
disposable test keys and carry no credentials.
