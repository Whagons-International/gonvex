# 0057: local runaway guard failure and truthful platform proof

Date: 2026-11-12. Status: affected local phase blocked; independent work continues. No gate waived.

K22 reports its exact assembled local full-conformance run timed out at the unchanged errors.spin pure
for(;;) fixture. The next board.add also stopped replying. The owned gonvex-k22-conformance-all-local-1 unit
was stopped; no other job was touched. Evidence: /mnt/data/work/gonvex-k22/assembly-all-local-1.log and the
run's kernel/workerd.log. The log shows 15,001 ms waiting for errors.spin and another timeout on board.add.
This is a FAILED/incomplete local scenario, not a transient reply or successful recovery.

K04 README (R25) and probes/r25/FINDINGS.md already distinguish timer/host-call guards from a pure loop.
A same-isolate JavaScript timeout cannot interrupt non-yielding JS. Prior platform experiments observed a
real DO CPU reset, rollback and surviving gateway sockets, and caller-side mapping/quarantine was required
by 0022. That package evidence is not a certificate for K22's actual assembled call path. Root cpu_ms=5000
in a local config does not demonstrate enforcement; the observed pinned local runtime did not enforce it
in this run. Do not assert a universal emulator limitation without auditing its exact version/config/source.

Lead ruling:

1. Keep fixture, assertions, 15-second harness wait and original gates unchanged. Do not skip the scenario,
   fabricate a transient response, pre-quarantine its command or return a timer error while the JS still
   runs and may later commit. Do not label full local conformance passed. Record this capability/coverage gap.
2. Stop the affected hung fixture as done. Continue source fixes and separately selected independent scenarios
   on NEW OWN fixtures, with clear subset labels and no broad all-pass claim. Do not rerun the pure-loop case
   in the same unsupported local configuration and hang another worker. Source/config audit is permitted.
3. Wire the already required real caller overrun mapping and fresh-stub quarantine into assembled gateway/
   tenant RPCs using K04's contract. Quarantine must be causal and command-bound: do not permanently poison
   unrelated valid calls on an ordinary deploy/reset or blanket overload. Existing durable receipts still
   take precedence; a committed keyed operation must replay its receipt, not execute twice or become hidden.
4. Focused deterministic tests of actual platform-error shapes may verify caller wiring, rollback/quarantine
   state and output mapping. They are unit coverage, explicitly NOT a substitute for the end-to-end pure-loop
   scenario or a synthetic local CPU reset.
5. The actual assembled gonvex-cf-dev-* runtime must run the UNCHANGED reducer-errors scenario with real
   limits.cpu_ms=5000 deployment evidence. Capture actual runtime reset/caller response/quarantine, same-tenant
   follow-up service, other-tenant/quiet-neighbor/gateway behavior, transaction rollback/receipt digests and
   bounded same-command retries. Isolate this bounded destructive dev probe from other proof runs. Preserve
   the module's declared error contract and document measured CPU/wall/scope collateral effects.
6. Record local and remote coverage separately. A remote pass proves the platform path only, not that local
   enforcement passed. K22's original per-package parity matrix, G2/V01 full coverage and staging-readiness
   claims cannot silently erase missing rows. Any still-unmet local acceptance requirement remains a named
   blocker; a new per-call execution/isolation architecture needs a source-backed design/ruling rather than
   an improvised test-only shim. No new architecture or gate relaxation is authorized by this note.

This ruling permits the existing development proof/fix scope, not staging/production load or monitor
activation. K22 remains implementation owner; lead owns the acceptance decision and fresh verification.
