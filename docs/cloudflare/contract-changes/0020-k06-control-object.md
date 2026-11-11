# 0020: K06 control object: findings and adaptations

Date: 2026-11-11. Status: **proposed by K06**, for the lead. Affects: K06, K05, K01/K02/K04, 0007.

## 1. `node:crypto` PBKDF2 is capped at 100,000 rounds on the platform (amends 0007)

0007 asked K06 to try `node:crypto` first. Locally (workerd 1.20261023.1) `pbkdf2Sync` at 210,000
rounds works and matches the Rust output. **Deployed, it throws** `NotSupportedError: Pbkdf2 failed:
iteration counts above 100000 are not supported (requested 210000)`; 100,000 works, 100,001 fails
(probe `gonvex-cf-dev-k06-pbkdf2-probe`, deleted). So 0007's fallback applies.

K06 verifies and hashes with a 12.7 KB WebAssembly module built from the reference's own crates
(`pbkdf2` 0.12.2, `sha2` 0.10.9; `cloudflare/control/wasm/pbkdf2`), with a pure-JS fallback.
Platform CPU per 210k verification, from `wrangler tail`: **WASM 123–176 ms (about 125 ms steady)**,
pure JS 253–417 ms. Below the 500 ms stop line of 0007, so no Rule 7 stop.

**Risk for the lead:** Rust verifies inside the single Control writer too, at about 25 ms native. At
about 125 ms, one control object signs in about 8 passwords per second at most, and every other
control call waits behind each one. Proposed follow-up (not in K06): verify in the stateless caller
(the socket owner) between a claim and a completion RPC, keeping the replay-before-password order of
C.3.9.

## 2. Local workerd does not reproduce platform limits

The PBKDF2 cap above only shows on the platform. Packages that touch WebCrypto or `node:crypto`
limits should smoke-test on the dev account, not only in vitest.

## 3. `ControlChanged` is delivered once, not echoed

Rust's `notify_control_changed` sends the event locally and again through its database listener, so a
socket's live Control Queries refresh a second time shortly after each write. That second refresh only
matters when a result changed without any notification in between, for example after a *failed*
`invitations.accept` leaves its claim. The control object notifies each watcher once per successful
write. The conformance scenario reads that list on demand rather than racing the echo. Proposed:
accept as not contract (timing-dependent).

## 4. Process presence moves to the socket owners

`control.support.listTenants.activeSessions`, `support_live_ids` and the support heartbeat short-circuit
read process memory in Rust. The control object keeps an in-memory map that socket owners update
through `connectionState` (nothing ephemeral in SQLite). After a control-object restart the map
is empty until sockets re-authenticate. Proposed: accept; K05 re-reports on wake.

## 5. The emulator decoder is gated by configuration

Rust compiles the unsigned-token decoder only into debug builds with a feature. A Worker has no
debug build: the kernel enables it only when `GONVEX_ENV=test`, `GONVEX_TEST_AUTH_EMULATOR=true` and
a loopback `FIREBASE_AUTH_EMULATOR_HOST` are all set, plus the reference's demo-project checks.
Production configuration never sets them. Proposed: accept, and have K19's deploy refuse those
variables outside `gonvex-cf-dev-*`.

## 6. The tenant port

The control object reaches tenant data only through `TenantPort` (`cloudflare/control/src/tenant-port.ts`):
one RPC per reference tenant transaction, plus the membership push into `projectMemberships`. K01/K02
implement it on the tenant object; K04 adds the invitation-acceptance module Reducer. Proposed: the
lead records it as the interface between K06 and those packages.
