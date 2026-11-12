# 0051: K20 integration merge after K15 and K16

Date: 2026-11-12. Status: decided (lead); continuation of 0048 and 0050.

PR #372 keeps its approved Container implementation and original done_when: assistant data-analysis scenarios
pass. The lead merges integration `8547c17f60fa8449798f4df4ba48937cf60bc596` into K20 with an ordinary merge,
without rewriting the package history.

- The only conflict is `cloudflare/package.json`. Keep every integration typecheck and test, including
  K15 telemetry and both K16 public-function suites, then append the sandbox typecheck and suite.
- K15/K16's session, Action and control hooks match integration exactly. No sandbox source, Rust source,
  assertion or conformance scenario changes during reconciliation. IB1 and IB6 remain as decided in 0050.
- Regenerate Wrangler types, docs metadata and SQLite schema options from the combined source. They produce
  no additional tracked changes. The frozen-lockfile install succeeds without changing the lockfile.
- Madge 8, excluding TypeScript type imports, reports the same single pre-existing local-runtime cycle
  on integration and the merged source; no new runtime cycle. The complete diff classifies as non-UI.
- Verify the cloudflare typecheck and full workerd suite, conformance unit suite, bundle dry run, and sandbox
  suite in both CI-stub and Rust-with-DuckDB Container modes. Post exact-head results on the PR.

The current integration branch has no effective protection/ruleset and no configured merge queue. Mergify's
queues apply only to main and staging. Use the permitted non-UI squash lane pinned to the updated head after
its covering CI passes; never use a protection bypass. Neither main nor staging is in scope.
