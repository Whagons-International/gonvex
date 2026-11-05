# Historical shipping baseline qualification

This experiment uses runtime integration `2b7e2ff9f6950ea73a4659c284c9204262314574`,
the unchanged shipping SQLite app `37ca8278be4365951816d05c3312abc66df51dd4`,
its matching module `d39570bbfc685c71f377f30033063f3853e239b37278db7d59b7c7372bdebbd3`,
and the 132-intent shipping fixture. It uses ordinary cold new-device loads;
it does not negotiate starter files or qualify the final current-staging gate.

A fresh owned optimized local-load build produced runtime SHA256
`8367959c2ec47c6480b3fa27baa119d47364ac5c37f9045f8e26b07dfc22abf8`
and module-host SHA256
`a6decd429c53d3fa6df5ff6735eee45c550b42063113935eddb39918eaae4fe3`.
The source is an archive of integration, including its matching module SDK
runtime contract. No permission, starter or heap branch was merged into it.
The first archive omitted that contract and failed compilation; the corrected
build and original failure log are preserved.

The approved experimental shipping candidate-2 SDK is selected only for actual
SDK clients. Its entry SHA256 is
`60b2673135e0ffd245b2219fb8a18cd2f6add3ac98ccbfa6af9cb6f5ae20eb0a`.
All 128 composition payload files were checked against their frozen inventory;
the original patched SQLite.1 SDK and its deferred-open API remain intact.
There was no package installation, PG artifact swap or application change.

Preflight used an offline copy of the stopped, explicitly synthetic observer
lab. The deployed manifest matches the shipping manifest except synthetic
project routing. Every declared column for all 132 required collection intents
exists in all 26 tenant databases. All 1000 measured accounts have an active
membership in their intended tenant. Normal password/OOB verification and
lookup confirmed all 1000 identities; no email-policy bypass was used.

A real owned runtime and actual candidate-2 SDK established cold and warm
132/132 collection readiness for an admin and an ordinary member. The checkpoint
thin client matched all projected rows for every collection in both phases.
The warm SDK explicitly opened `__local.userPresence` with its stored cursor;
its rows were included in the full oracle comparison. This proves this
harness's explicit intent and delivery path, without claiming that the earlier
browser warm omission was fixed. Ordinary shipping share/revoke reducers then
added a genuinely hidden task and removed that old row. Both long-lived clients
matched fresh SDK snapshots for all 132 collections after each change. This
is historical shipping permission behavior, not full permission-primitives
acceptance.

All local runtime, auth, generator and sampler processes belong to one actual
6-GiB/2-CPU parent cgroup. The runtime and V8 children have a nested 2-GiB/1-CPU
cgroup and affinity 7,15. The remaining processes use other CPUs. The network
namespace has loopback only. No perf privileges, system tuning, other workers'
caps, shared lab lock or deployment were changed.

Evidence and the final measured result are kept in
`/mnt/data/work/gonvex-harness/historical-baseline-20261105/REPORT.md`.
The final gate still requires the current SQLite physical/module/runtime pairing,
complete permission-provider coverage and an implemented, negotiated starter
client handoff. PG compatibility receipts, including 1051, do not satisfy those
requirements and were not installed here.
