# Remaining indexed permission source coverage

The executable boundary is still local, default off and deliberately smaller
than Whagons. The source inventory on the app branch preserves all 342 exact v2
plans, 472 application replica paths and 340 generated local replica paths,
including every set/join/filter and each operator's JSON path. No production
route is ready. This document describes required work; it is not an admission
receipt or an implemented incremental algorithm.

The next source slice uses the already reviewed relational-v1 declarations for
namespaced group-target grants, member/group membership edges, and SQL NULL
predicates on physical scalar columns. Projected IDs/atoms and equality operands
remain strictly TEXT/BINARY. A timestamp used only by `IS NULL`/`IS NOT NULL`
does not need TEXT conversion. JSON text `null` is not SQL NULL. Group IDs use
an injective namespace/ID tuple encoding, never a role/team cohort as permission
identity. Roles, typed JSON truthiness, joins and parent policies are not inferred
from that subset. The compiler must reject them until their own adapters exist.

For each subsequent rule family the implementation order is source normalization,
transaction/readiness maintenance, indexed SQL selection, exact v2 row/window/
message oracle, and only then capability admission. A declaration or successful
codegen does not prove serving. The first two real key-ordered self-owner route
candidates remain `taskResources.myVoiceMemoListens` and
`tasks.myBulkCreateBatches`; source types are known, but converted physical DDL
and per-route query/projection/budget oracles still require qualification.
Omitted v2 ordering has no implicit key sort. Truncated unordered windows cannot
use a different planner prefix: reject until an exact original-v2 fallback before
publication is implemented and tested.

The six staged task grant sources can use direct member targets, namespaced team
targets and active membership edges only where that is exactly equivalent to the
original correlated join. A complete tasks binding still needs workspace AND spot,
strict-true flags, scoped GLOBAL/TEAM/default roles, finite aliased ancestry levels
1 through 4, share/assignment branches, and PERSONAL semantics:
`personal grants OR (ordinary grants EXCEPT personal-workspace rows)`.
Ordinary admin/share/assignment grants stay inside that subtraction. There is no
global deny inferred from this expression. Optional parent predicates replace
parent policy using the child principal. Parent null FK, absent parent onMissing,
and hidden existing parent remain distinct. Via relations are existential over
correlated links. An unbounded recursive ancestor closure is not depth 1–4.

A complete candidate index must retain occurrence provenance, not merely unique
atom edges. For each source occurrence it needs source ID, every participating
alias/table primary key, projected member/group/atom/row IDs, and OLD/NEW images.
Alternate supports and overlapping grants survive deletion of one occurrence.
A join or derived-source change starts indexed reverse joins from the changed
alias keys in both images, preserving each whole correlated tuple. The resulting
old/new output row keys, affected principal/group supports and negative inputs
form candidates. NULL omission and duplicate SQL join rows follow the exact v2
oracle, not a Cartesian product of independent value sets.

The dependency closure must include the following, with indexes and explicit
completeness/version receipts before selective reuse is permitted:

- Principal member/account mappings, status, relevant flags, global roles,
  permissions/default overrides, all member/group edges, grant sources, and
  reverse old/new group reachability. A negative edge or expiry can change rows
  while adding no positive atom; candidate selection must include it.
- Every row-side authorization input, row insert/delete/reparent, finite universe,
  atom write, join/link source, parent existence/visibility and projection input.
  Parent/link reverse indexes must enumerate affected descendants only through
  the declared finite levels, retaining multiparent supports and cycle semantics.
  Child-principal replacement predicates cannot reuse a parent's cached decision.
- Policy/source binding/projection/module/schema versions, physical instance and
  epoch, global/default inputs, and explicit time/expiry authority. A global input
  may correctly require all collection keys or all principals. Report that work
  as full scope; do not call it a selective constant-cost update.

OLD candidates must be established before deletes lose their images. All source,
occurrence, principal-set, row-atom and readiness changes commit atomically with
the writer revision. Failure marks the transaction failed and rolls everything
back; an incomplete/backfilled/outdated index cannot answer. A copied physical
DB needs new independently validated identity/readiness. During the current
complete-source rebuild approach, conservatively invalidate every indexed source
mutation. No candidate completeness or content-only reuse optimization is claimed.

Live delivery remains disabled. A correct future serving barrier needs writer-
ordered admission of the exact canonical set, full signed policy/source/dependency/
projection authority, physical scope/epoch, permission-change serial and index
generation at a single read revision. Manifest acceptance and every authenticated
piece retrieval must validate the same current authority. An encoder cache or raw
content hash is not retrieval authority. A lease/barrier must serialize the final
send admission with revocation/expiry and commit publication; enqueueing bytes
under an earlier check alone does not establish that property. Member-specific
collection arguments and admitted collection instances remain isolated even when
an encoding is shared. Never disclose an old payload and repair it with catchup.
The starter lane owns its bootstrap transport/adapter implementation separately.

Acceptance still requires synthetic property/fuzz cases for overlapping supports,
negative-only flips, multigroup changes, row+grant changes in one TX, rollback,
missing/deleted/cyclic/multiparent/reparented ancestry, null/blank/case/Unicode/JSON
and SQL null distinctions, projection/order/dedup/byte budgets, then all users and
all collections on the independently qualified read-only local v2 copy. Unsupported
collections are failed coverage, never skipped zero-mismatch success. Writes,
disk, memory and query CPU must be measured in a suitable later resource lane;
there is no capacity or performance claim from these correctness checks.
