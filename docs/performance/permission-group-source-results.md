# Local group source and NULL predicate verification

Source checkpoint: `000d7445095d01c1fa4d073bd2c06f8d0b7db8a2`.
This extends the existing reviewed relational-v1 DSL internally, with no SDK,
client, protocol, transport or starter hook changes. V2 remains the default;
primitive serving remains limited to explicitly installed owned local fixtures.

The normalizer now lowers member/namespaced-group grant targets and membership
edges into the existing transactional store indexes. Group identity is an
injective JSON namespace/ID tuple, preserving exact text and preventing namespace
collisions. NULL-only predicates may reference physical timestamp/scalar columns;
projected IDs, atom values and equality operands still require TEXT/BINARY storage.
A SQL NULL test does not coerce JSON text `null` or a nonnull timestamp into NULL.
All referenced NULL columns remain in the signed OLD/NEW dependency certificate
and source write guards. Joins, roles, JSON truthiness and parent policies remain
rejected by this adapter.

A new cross-language fixture is generated from its literal declaration by the
real CLI compiler. The regression installs that signed source program through the
actual private Replica adapter, loads facts, performs indexed SQL projection and
window collection, and compares exact v2 rows/messages after each change:

- Alternate direct grant support and overlapping grants from multiple groups.
- Last membership/grant removal, revoke/reactivate, and namespace isolation.
- A negative-only change adding no positive grant; subtraction remains local to
  its declared finite universe, not a global deny interpretation.
- Row and grant retarget in one transaction; row-only mutations preserve the
  exact canonical principal atom set while invalidating old row authority.
- Rollback and failed non-TEXT normalization, with readiness failing closed
  before any stale payload is returned.

Full runtime/store feature-on tests passed: 407 runtime tests (8 ignored) and
44 store tests. Feature-off passed: 397 runtime tests (8 ignored), 44 store tests.
After freezing the final source checkpoint and the test-only environment-lock
lint annotation, all three cold SQL/v2 oracle tests and clippy passed again.
The JS compiler fixture tests passed (4 tests), and its generator check passed.
Clippy has existing dependency/baseline warnings; there is no warning in the new
source implementation. The new isolated test deliberately owns the same process
environment mutex across awaits as the existing fixtures, documented with a
narrow test-only expectation; no production lock is introduced.

All heavy phases used the lane's asserted MemoryMax=6 GiB/CPUQuota=200%,
MemAvailable >=8 GiB, both IO PSI avg10 metrics below20%, and build-headroom on
actual /mnt outputs including the Rust target. Cargo used the explicit outer
optimized-build/inner coach-compile wrapper chain, conservatively also for debug.
No performance measurement or capacity claim was made during concurrent lanes.

The current normalizer rebuilds each complete logical source. It does not claim
incremental write efficiency or complete candidate-key selection. The app's six
staged task grant sources still have no complete tasks collection binding; the
actual full Whagons rule family, projection/order/budget and production-copy
oracles remain pending. No production route becomes provider-complete merely
because normalized source receipts are complete. The source/candidate/publication
plan and exact unsupported inventory remain the next-work authority.
