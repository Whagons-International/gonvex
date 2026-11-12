# 0052: K20 landed; reconcile K10 with the combined integration base

Date: 2026-11-12. Status: decided (lead), continuing 0048, 0050 and 0051.

## K20, PR #372

Current-head review approves `72acc144293bbee70477549d3bc15bd65f9727ab`. Squash-merged into integration as
`9a69e2b32d8d0d903e90e7bee89e20e7776f9e5f` at 07:24:43 UTC. The original done_when is satisfied: all seven
assistant data-analysis scenarios pass against the Rust-with-DuckDB Container (24 sandbox tests pass).
The exact source also passes 762 workerd tests, typecheck, generators and 26 conformance unit tests.

The covering Cloudflare CI, Runtime tests, policy laws and native-scope job all passed on this head.
The unchanged Rust optional-feature job was still running at merge. It is a dependency of the Runtime
regression CI aggregate, but neither job is required by integration's effective rules: protection is absent,
rules are empty and both Mergify queues target other bases. This is a check-scope determination, independent
of the job's name. K20's own DuckDB behavior is covered by the passing Container suite; no contract or branch
requirement was waived. No bypass flags were used.

## K10, PR #374

Ordinary merge of integration `9a69e2b3` into the approved K10 head `7aca7efb`, with no history rewrite.
Keep K10's original done_when: replica scenarios pass, including resume, reset and reconnect wave.

Eight files conflict. Resolve them by preserving both packages' behavior:

- Session record, options, constructor and auth retain K10's delivery state/timers/subscription epochs and
  K15 telemetry plus K16 guard/admission state. Both delivery authentication and telemetry-device capture run.
- Gateway and RPC interfaces retain ordered `deliverReplica` and K16's `liveConnections` audit; retain telemetry
  initialization and guard wiring. Keep K10's close-handshake fix and optional connection-reset no-ops.
- Keep every scenario/backend and both capability additions (`epochReset`, `publicCalls`). Keep the original
  comment and `receiptCrash` last. Retain digest and watermark self-checks.
- The shared Reducer harness uses `harnessConfig`; its default derives from `this.harness`, preserving K10's
  directory override, while K16's public backend still overrides the full config path. Selecting a fixed
  Reducer config here would silently run K10 against the wrong harness.
- Keep all documentation rows and all package suites. K17's schema-transform capture implementation, K20's
  sandbox implementation and K15/K16's dedicated package source remain identical to integration. No K10
  delivery implementation or assertion changes are needed.

Regenerate the Wrangler types, docs metadata and SQLite schema options; keep generated output only if it
changes. Compare runtime cycles with the merged integration base. Run the full workerd/typecheck suites,
conformance units, replica Rust-versus-kernel scenarios and public harness regression before pushing.
Publish exact-head results and the final merge outcome on the PR. The same integration non-UI squash lane
applies; there is no integration queue or protected check, and no staging/main merge is authorized.

## Combined-source verification

- Cloudflare typecheck and full workerd suite: pass, 768 tests (CI-stub sandbox mode), with preview-script tests.
- Conformance units: 29 pass. Rust versus `cloudflare-delivery`: 11 scenarios pass, including parity, second-client
  delivery, resume, reset, windows, visibility masks, orphan reveal, tenant restart, reconnect wave, lost-reply
  receipts and owner-switch receipts. IB5 and tenant-restart are kernel-only as before; both run and pass there.
- Rust versus `cloudflare-public`: public-calls and public-limits both pass, confirming the full-path harness override.
- Wrangler/docs/SQLite generators produce no additional tracked drift. Frozen-lockfile installs leave the lock unchanged.
- Madge 8 excluding type imports: one identical pre-existing local-runtime cycle on base and head, no new cycles.
- Complete diff (53 files including this note): non-UI. Worker dry run: 197.73 KiB, gzip 50.12 KiB.
- The shared-read cost test still covers 1, 10 and 100 subscribers. No SQL, wire or admission contract changes
  were introduced during the merge. Existing follow-ups remain with K22 and V01 as assigned in 0048/0050.
