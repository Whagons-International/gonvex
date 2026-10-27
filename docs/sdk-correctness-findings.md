# SDK correctness findings

Keep findings and their failing reproductions even after a fix merges. Proofs of
small policies do not establish correctness of the surrounding SDK.

## SDK-REVISION-DOMAIN

Base: staging `79668483b46fd8901aa0beacaf9f6e837d2f92e6`.

`LocalReplica.applyTransaction` checked only `revision <= 0`. It could accept NaN,
positive infinity, fractions and integers outside the safe integer range. The
projected-row merger did not check the revision domain at all. These values can
enter memory, per-field authority and persisted transaction cursors even though
sync ordering assumes safe integer revisions. Infinity can prevent later finite
transactions from applying; NaN defeats ordinary less-than comparisons. Fractions
and unsafe integers can also arrive as JSON numbers. This is a reproduced input
validation bug; no deployed occurrence is established.

`packages/client/src/revision-boundary.test.ts` produced nine intended failures
and two passing controls before production changes. The four invalid positive/NaN
transaction cases resolved rather than rejecting; all five invalid row-merge
cases failed to throw. The same regression checks memory and storage preservation
after rejection and verifies the serialized queue can accept the next valid item.

The correction shares a safe-integer domain policy between these paths and
existing snapshot/watermark validation. Snapshot revision zero remains allowed;
transaction revision zero remains rejected. The maximum safe integer remains
accepted. An explicit own-field lookup preserves projected columns such as
`constructor` while extracting numeric ordering into the policy module.

The proof covers five source-linked decision functions under the
documented binary64 model. It excludes full row merging, persisted historical
invalid values, storage atomicity and async scheduling. See
[SDK revision proof boundary](verification/sdk-revisions.md).

An independent pre-merge review caught a proposed extraction that changed
`!(incoming < prior)` into `incoming >= prior`. Those agree for finite numbers
but differ for legacy NaN authority, which IndexedDB can retain. The final policy
preserves the old rejection comparisons. `revision-upgrade.test.ts` exercises
valid update, deletion and resurrection after reopening both IndexedDB adapters.
This avoids introducing a new upgrade regression while fixing incoming admission.
