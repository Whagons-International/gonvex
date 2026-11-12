# 0077 execution-lane preparation: tested core, no live adapter

Date: 2026-11-12. Owner: lead. Status: offline core tested; NOT independently reviewed or released.
Source artifact: `/mnt/data/work/gonvex-cloudflare-lead/0077-execution-lane/source-final.tar`, SHA256
`23e4845df9ab68b82131ef2ebaf61a335fca738654c03a57b57f0f7026f3428b`.
Source is 3 regular files (guard, tests, README); exact hashes/commands/resource receipt are in that
directory's `receipt.json`. No repository runtime import or package implementation changed.

The core checks trusted lead release bytes and seven exact file pins without Vault access, consumes a
root-bound one-use durable claim before launch, and permits expired-release recovery only for cleanup.
It retains UNKNOWN on corrupt/ambiguous state, verifies the recorded process identity, issues stop before
new persistence IO and refuses replacement IDs, false cleanup receipts or deletion without a creation
witness. A hash supplied by its caller is only a structural witness, not account/creation authority.

15 tests pass using real private filesystem operations and an actual owned Node child/SIGKILL/reap,
with SYNTHETIC cleanup callbacks. They cover duplicate/concurrent claim, root mismatch, bad/expired
release or pin, corrupt state, late launch, recovery after child death, kill before resource capture,
reentrant cleanup, live process refusal, unknown ownership and unsettled stop. An unsettled stop is not
exit proof; missing witness/identity remains UNKNOWN and grants zero deletion. Temporary fixtures are
empty after the finite test phase; earlier 14-test/source evidence is retained separately.

These tests do NOT prove actual operator/guardian sibling cgroups, descendant quiescence, API deletion,
Container termination, armed watchdog, host-loss recovery, or70s/10min enforcement. The core supplies no
production adapter, service, timer, live lead release or account mutation. Stored deadline fields alone
do not bound stuck IO/reap/cleanup. It is not safe to activate this core as a standalone supervisor.

Lead next owns the concrete production bridge and independent executor: semantic19b preflight BEFORE
Vault; authenticated durable empty-namespace/create-intent and identity handoffs before effects; an armed
pre-start watchdog ACK; actual sibling-unit/descendant kill-reap; strict original-ID/ref/tail cleanup;
pre-intervention causal snapshot and honest missed-bound/residual output. A same-host process cannot
cover machine loss. Identify a reachable independent recovery executor or retain that unresolved hold.
Review the coherent source/config/manifest and NEW independent evidence before arming any attempt.

Current19b review clears R1 and original R2 for the CLI's ordinary plain records. Its P3 unstable-getter
snapshot edge remains recorded, with no account-input route found. Do not claim arbitrary-proxy safety;
the eventual bridge must preserve the plain-data boundary or strengthen it. Startup cause remains UNKNOWN;
no original K22/X01/CPU/SDK/matrix/capacity/security/uptime/duration gate is closed.

Resource evidence clarification: installed `coach-compile` explicitly implements FOUR slots (Nov03
header/range4); the older README's two-slot statement is drift. No helper, cap or other unit was changed.
New phases use the unchanged wrapper inside headroom/coach-resources/kernel-v1. The first19b focused
phase and earlier lead generator phases lacked an explicit wrapper; their compile-slot coverage is
UNVERIFIED, not retroactively certified. Tests/hashes remain scoped results, not a historical two-slot claim.

Subsequent separate local topology fixture passes: an actual guardian unit survived SIGKILL of an exact
owned operator unit and real Node descendant; original process identities disappeared and its cgroup was
removed. The later stop command returned unit-not-loaded, which was not used as settlement proof. Both
finite units settled and fixture temp data was removed. Receipt/source/raw logs are under the lead root's
`0077-execution-lane/topology-test/`; normal resource gates and the unchanged wrapper were used.
This proves ONE local process topology, not the production Vault/Wrangler bridge, Container stop, API
cleanup,70s/10min bounds or host loss. A concrete three-handoff typed proposal now exists at
`0077-execution-lane/bridge-proposal.md`; trusted transport, actual durable API handoffs/watchdog and an
independent recovery executor remain unimplemented/unarmed. Frozen19b/core archive remain unchanged.
