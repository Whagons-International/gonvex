# Private exact-set starter backend

Status: default-off `starter-files` Cargo feature, private local SQLite indexed
snapshot adapter. No normal open handler, client, SDK, websocket protocol or
application policy port selects it. V2 remains the default. This is backend
implementation evidence, not full Whagons activation or production acceptance.

Base: integration `2b7e2ff9f6950ea73a4659c284c9204262314574`, merged verified
permission primitives `84970d56c8d66e392efd2f727f4a102bf88afd90`. The only merge
conflict was replica module registration; both resume and permission modules
were retained. Other branches/worktrees and integration were not changed.

## Executable path

`Runtime::spawn_local_starter_after_miss` is an explicit private post-normal-load
hook. It immediately returns an optional cancellable task handle. A bounded
queue rejects overload, canonical exact identities singleflight builds, and a
single store build semaphore serializes builders. Work uses the runtime's lower
priority offload pool; an inline offload configuration rejects this experiment.
No call to this hook is inserted into normal opens pending provider/API review.
Aborting the task cancels its offloaded future and releases queued ownership.

The worker reads indexed SQL under one SQLite snapshot, validates the physical
schema/source receipts and active member, and streams the real replica window
scanner's projected rows into bounded JSON-lines pieces. Ordering, projection,
filters, exclusions and row/byte windows use the existing scanner. No new
full-table RAM copy is made. A single row larger than a piece is rejected with
a bounded serializer. A trusted binder must explicitly classify a collection
as `SharedBase`; per-user lists and unclassified providers reject. Parameterized
collections also reject conservatively. The current app providers do not prove
full Whagons coverage; no implicit role expansion or broader set is introduced.

Publication and reads acquire a module activation lease, then the landlord
writer connection and tenant writer connection in that order. Account enabled,
active tenant, current route, active member, complete/current indexed authority,
physical instance, schema, source contract, generation and exact canonical set
are checked while writes are excluded. The synchronous publication/read visitor
finishes before those connections are released. These are read-only uses of
writer transactions and are rolled back. This conservative local barrier holds
writers during disk reads; its cost must be measured before any live activation.
The visitor must not reenter the store or runtime writes, or perform network
I/O while holding this barrier. A later asynchronous transport needs its own
ordered revocation/admission lease, not a copied authority value.

## Identity and format v1

The lookup SHA-256 is an accelerator. Exact canonical set bytes remain equality
authority, including project, independently validated physical database, tenant,
epoch, collection/table, policy, projection and semantics. Canonical effective
atoms preserve global/team/scoped-role/flag/user distinctions from the trusted
binder. Module generation/artifact, permission generation, permission serial and
encoding version are additional identity fields. No role-name or visible-row hash
stands in for effective authority.

Permission serial changes invalidate old payloads even if the member's atom set
is identical. A row can move to another permission scope without changing the
member's atoms; sending its old payload before catchup could disclose a revoked
row. The current normalized adapter conservatively invalidates on every indexed
source mutation. More selective invalidation needs proven source dependencies;
this implementation does not claim reuse across those mutations. Unrelated
changes with unchanged serial can use the stamped cursor and normal catchup.

A manifest has format, full identity, epoch/revision, expiry, row count and ordered
pieces. Each piece has encoded and raw SHA-256, encoded/raw lengths and a
compressed flag. A checksum envelope protects the complete manifest. Pieces are
immutable content-addressed files shared across exact identities only when their
actual projected bytes agree. They use the exact existing websocket level-1,
12-bit, raw RFC7692 no-context-takeover compressor, stripping its sync trailer;
uncompressed pieces are permitted when compression would grow the data. Each
piece is independent. A consumer restores `00 00 ff ff` for inflation and checks
raw size/hash before hydrating rows. This is not a websocket message envelope.
A negotiated window/encoding mismatch must use normal opens, never reinterpret
the encoding. Subscriptions, hashes, window state, ACKs and envelopes remain
per-client and are not stored in shared files.

Temporary files are fsynced, pieces renamed and their directory fsynced, then
the complete manifest is atomically renamed and its directory fsynced. Disk,
piece size/count, manifest count, queued work and TTL are configurable bounds.
The private store root is mode 0700 and exclusively locked. Startup removes
interrupted temporary files. Cleanup derives live piece reachability only from
valid manifests and removes unreachable pieces while holding the same mutex as
publication/reads. Corruption or missing pieces returns a normal-open fallback;
all pieces are checked before the first visitor delivery. No public file URL or
unauthenticated content-address lookup is exposed.

Current-cache resume must bypass starter lookup. Expired or pre-retention
snapshots, future cursors, changed scope/module/policy/projection/generation or
unsupported authority reject. `should_rebuild` accepts measured change byte/row
counts and configurable thresholds. Defaults (10k rows, 8 MiB changes, one-hour
TTL) are provisional bounds, not measured optimal values. Snapshot authorization
expiry may shorten TTL. A future caller schedules rebuilding after normal load,
not synchronously in an open.

## Validation and measurement

Raw receipts/logs and result JSON live under
`/mnt/data/work/gonvex-starter-files`. Each heavy phase verifies actual cgroup
MemoryMax 6 GiB / CPUQuota 200%, MemAvailable >=8 GiB, I/O PSI some avg10 <20%,
and build-headroom on the actual /mnt target. The installed resource controller
uses the existing /home checkout only for startup cwd/receipt bookkeeping;
all new target/cache/generated build output is under /mnt. Explicit
coach-optimized-build -> coach-compile -> cargo preserves the two optimized
build gate and the existing inner compiler-process limits. No other cap changed.

Tests cover exact authority boundaries, duplicate content across distinct sets,
member/account revocation, member-list fallback, cancellation, corruption before
partial delivery, interrupted-build recovery, bounded queue, expiry/retention,
rebuild conditions and independent SQL bootstrap/catchup results. A synthetic
runtime test invokes the actual loaded module's streaming adapter and verifies
projected row IDs, absence of a private column and access-time revocation.
Inherited permission tests exercise overlapping grants and SQL subtraction.

The explicit ignored module benchmark builds a 9,000-task SQLite tenant with
400-byte descriptions. It records a baseline before building any starter, then
seven warm-file-cache starter reads + decompression + ten-row SQL catchups.
It reports ns/op, process CPU, caller-thread allocation count/requested bytes,
whole-process RSS, raw/compressed/disk bytes and build time. It does not measure
the full adapter's authority/source/schema validation cost, client hydration,
network, 78 subscribers, small-tenant concurrency or a cold OS disk cache.
These results cannot establish 1-vCPU capacity or end-to-end cold-open speed.

Full read-only production-derived data consumer remains PENDING until signed
full-policy/source coverage exists. No raw dumps or customer rows were opened;
no real distinct-set counts or file-size claims are inferred from unsupported
providers. The reviewed private manifest interface requires complete opaque
routing/physical identity, checksums, epochs, all schema/module/policy/projection
versions and all grant/member/group/row source provenance. Derived retention
belongs to the producer until all consumers release; originals are Gabriel's
decision.

## Client review still required

Minimal proposed bootstrap contract (not implemented): authenticated offer with
format, opaque manifest ID, epoch/revision, encoding and piece hashes/lengths;
authenticated retrieval under fresh exact authority; validate/decompress/hydrate,
then use normal replica catchup from the stamped cursor. Any authority change,
retention gap, corrupt piece or unsupported collection falls back to a normal
open. Never offer a starter to a client with a current usable cache. No SDK,
app, CLI schema or protocol file was changed for this proposal.

The initial benchmark attempted the tiny 1-KiB-piece fault-test configuration
and correctly hit the 1,024-piece bound before 9,000 rows. Its failed receipt is
preserved. The benchmark uses the actual default 256-KiB piece configuration;
the failure was a fixture configuration error, not a passing performance sample.

The private source/consumer contract additionally pins the permission serial,
ready index generation and dependency-completeness proof. Identity preserves the
exact source authority bytes (bound program, manifest, source schemas/guards),
not just their digest. The local normalized admission checks complete receipts
before marking the authority ready/complete; this does not assert full Whagons
provider completeness. Regression coverage holds member atoms constant, moves a
row out of scope, denies the old manifest and old piece hash before any callback,
and permits a fresh build with that row excluded. Piece lookup is exclusively
through `read_selected_piece` under an authorized current manifest; there is no
hash-only retrieval capability.

The recorded module workload uses highly repetitive `x` descriptions, so its
compression ratio must not be extrapolated to real customer text. Disk bytes
are logical encoded-piece plus manifest payload sizes; filesystem journal,
block allocation and physical write amplification were not instrumented.
Build time includes streaming serialization, hashing, compression and durable
piece/manifest writes. Rebuilding identical content can still write temporary
encoded bytes; dedup reduces retained disk, not all write CPU or I/O.

Recorded debug module sample (seven repetitions, noisy concurrent host): baseline
median 390.81 ms/op; starter read/decompress/10-row catchup median 129.66 ms/op.
Build 677.09 ms; 3,859,893 raw bytes, 68,285 encoded bytes, 72,091 logical retained
piece/manifest bytes. Median caller-thread requests were 29705 allocations for
baseline and 2298 for starter. Whole-process RSS moved from 31277056 to 50057216 bytes across
the complete experiment, not a per-operation retained-memory measurement.

Feature-on runtime suite: 414 passed, 9 ignored; default runtime suite: 399 passed, 8 ignored. The explicit benchmark passes. Generator checks rebuild
this branch's CLI then regenerate dashboard/template bindings, module/agent API
artifacts, and docs metadata. No tracked generated output changes were produced.
TS runtime cycle comparison against the pinned integration base preserves the
same two existing cyclic edges, adding none. New Rust files pass rustfmt.

Permission tests use the existing store-enforced temporary fixture namespace
`/mnt/data/work/gonvex-permission-fixtures`; their temporary SQLite directories
are removed on drop. Retained build/cache/log/benchmark output stays in the
starter lane directory. The worktree registry helper rejected the T3-prepared
path; explicit lane/unit/lock ownership is recorded in `ownership.json`.

Final merged store library suite: 44 passed. The final actual-runtime row-scope
revocation regression passes on the frozen source. The final focused disk tests
(5 passed, benchmark separately passed) include a revoke after durable piece
rename but before manifest publication, followed by restart orphan cleanup.
