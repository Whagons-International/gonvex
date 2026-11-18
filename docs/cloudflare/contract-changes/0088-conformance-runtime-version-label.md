# 0088: `session.ready.capabilities.runtimeVersion` in conformance comparisons (proposal, awaits lead)

Raised by X03 (criterion 5) on 2026-11-18. **Proposal only.** No harness, normalizer, assertion or deployment
change is made until the lead decides.

## Observation

`CONTRACT.md` §A.1.3 lists `runtimeVersion` as **adapt**: Rust sends `config.runtime_version`, and the kernel
sends "the Worker version id or the Git SHA from a deploy binding" (`kernel/session/capabilities.ts`,
`placement/gateway.ts`: `env.GONVEX_RUNTIME_VERSION || "development"`).

The conformance normalizer (`cloudflare/conformance/src/normalize.mjs` `RULES`) has no rule for it, so
`session.ready` is compared exactly. The harness starts the pinned Rust reference without a version label
(`"development"`). A correctly labelled deployment therefore differs on every socket:

```
in session.ready differs: capabilities.runtimeVersion: "development" != "858f19e0…"
```

Full suite on dev `858f19e0` (all five Workers, clean fixture tenants, run5): thin-sdk-parity,
reducer-second-client, replica-resume and presence have **no other difference**. Digests 50/46, 39/39,
35/35 and the checksums and observations all agree. The earlier V01 passes only happened because that
deployment still carried the default `"development"` label. K22 #450 now keeps the source label on every
publication, which is required for exact-head provenance.

## Options

1. **Recommended:** the normalizer treats `capabilities.runtimeVersion` as an adapt value, either masked as
   `<runtime-version>` or compared only as a present non-empty string. This matches §A.1.3 and keeps every
   other `session.ready` capability exact.
2. Deploy dev with `GONVEX_RUNTIME_VERSION=development` while conformance runs. This loses the source label
   that provenance relies on, and every publication would have to restore it. Not recommended.
3. Start the reference with the candidate's label. This makes the comparison depend on the candidate under
   test. Not recommended.

Option 1 changes a shared harness file. It needs the lead's decision and, by rule 3, must not weaken any
other assertion.
