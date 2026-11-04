# Fast hashing on the hot paths (perf/fast-hash)

Branch `perf/fast-hash` from `perf/integrate-5k` 8d39925b. Goal: remove std SipHash from
the snapshot-build, visibility, delivery, feed and replica paths. At 1,000 users
(production mix, `ch-1k-prod-b` in `step-log.md`) SipHash was the top self symbol:
`Sip13Rounds::write` 4.1 % + `RandomState::hash_one<&String>` 2.4 % + `hash_one<&str>`
0.4 % ≈ 7 % of all runtime CPU samples.

## Where the SipHash came from

The 1k profile has no user-space call stacks, and DWARF unwinding (`perf record
--call-graph dwarf`) only gives one or two frames for these bench binaries. So the
callers were found by reading the code and by which std maps the hot symbols belong to:

* **`serde_json::Map` (nearly all of it).** `deno_core` turns on serde_json's
  `preserve_order` feature for the whole workspace, so every JSON object is an
  `IndexMap<String, Value>` with the **default std `RandomState` (SipHash-1-3)**. Every
  parsed SQLite row, change image, projection (`project_row`), visibility lookup
  (`Value::get`, `AclRow::from_value`, `literal_constraints_hold`), reducer argument and
  protocol message hashes every key with SipHash. The profile's neighbours are the same
  map: `indexmap::Core<String, Value>::insert_full`, `RawTable<usize>::find::<indexmap
  equivalent<String, Value>>`, `reserve_rehash::<indexmap get_hash<String, Value>>`.
  `CompiledPlan::eval_inherits` → `hash_one<&String>` in a bench sample is a parent-row
  `Value` lookup.
* **Our own std maps** on the same paths (smaller): the per-row duplicate-key set of
  `collect_window` (every snapshot scan row), `concern_tables` (checked per change per
  subscription), the delivery-v4 layout and slow-table sets, the per-connection binding
  maps of `ReplicaBatcher`, the direct-SQLite statement cache (whole SQL text hashed per
  statement) and the capture table-shape map (per captured row).

The 1k profile's other hash symbol, foldhash `hash_one<&(&str, &str)>` 1.15 % +
`HashMap<(&str,&str)>::insert` 0.64 %, is the delivery-v4 `layout` index, already foldhash.
It is computed once per `EventShare`; that it shows up at all suggests shares are being
rebuilt (evicted from `KEEP_EVENTS` under the 24 s delivery backlog) or events with very
many changes. Not investigated here; the index is now pre-sized.

## Changes

1. **Vendored serde_json 1.0.151 with a foldhash object map**
   (`rust/vendor/serde_json`, `[patch.crates-io]` in `rust/Cargo.toml`). The only code
   change is in `src/map.rs`: `type MapImpl<K, V> = IndexMap<K, V,
   foldhash::fast::RandomState>`, plus `MapImpl::default()` / `with_capacity_and_hasher`
   in the three constructors. Same version as the lockfile (1.0.151), so nothing else in
   the dependency graph moves; tests/ and dev-dependencies are dropped from the vendored
   manifest; the workspace `exclude`s `vendor`. Iteration and serialization order stay
   the insertion order (`fast_hash::tests::serde_json_objects_keep_insertion_order`), so
   row texts, canonical hashes and the wire format are byte-identical.
   `Dockerfile.runtime` copies `rust/vendor`. **When upgrading serde_json, re-apply the
   4-line change or drop the patch.**
2. **Runtime/store std maps → foldhash** (table below).
3. **Replica-map costs beyond hashing:** `CachedRow::from_image` no longer deep-clones
   the projection to build the visibility row (it extends the projection in place after
   its canonical text is taken); `project_row` sizes its map for the projected columns
   (no grow/rehash per row); `layout` pre-sizes its index and clones each table name
   once instead of once per change. `collect_window`'s duplicate-key set is foldhash.

### Per site

HashDoS rule: keep SipHash (or a BTreeMap) where keys can be chosen by one tenant's users
**and** the map is shared across tenants or long-lived and unbounded. foldhash
`fast::RandomState` is seeded randomly per process (and per map), so keys cannot be
chosen offline to collide; maps that live for one row, event, open or connection and are
bounded by it can use it.

| Site | Before | After | Keys / lifetime | Why it is safe / why kept |
| --- | --- | --- | --- | --- |
| `serde_json::Map` (every JSON object) | IndexMap + SipHash | IndexMap + foldhash | object keys; one document | One map per object, bounded by the document (request/row size limits). An attacker would need seed-independent collisions in one object's keys; with a random per-process seed that is the same exposure as the other foldhash maps. Order unchanged. |
| `replica::collect_window` `seen` | `HashSet<String>` | `FastSet` | row keys; one scan | Per scan, bounded by its rows. |
| `ReplicaSubscription::concern_tables`, `replica_mask::concern_tables` | `HashSet<String>` | `FastSet` | table names of the module plan; per subscription | Keys come from the deployed plan, not users; tiny. Checked per change per subscription. |
| `replica_fanout::FanoutKeys::slow`, `Layout::tables` | `HashSet<String>` | `FastSet` | table names; per plan / per event | Plan or event's own tables. |
| `replica_fanout::layout` index | `FastMap` default | `FastMap` pre-sized | (table, row id); per event | Already foldhash, now one allocation. |
| `delivery::ReplicaBatcher::{bindings, token_owner}` | `HashMap` | `FastMap` | subscription ids / tokens; per connection | Bounded by that connection's own subscriptions; a client can only slow itself. |
| `presence` patch merge `index` | `HashMap<&str, usize>` | `FastMap` | member ids; one flush | Per call, bounded by the batches. |
| `snapshot_cache::extra_columns` `projected` | `HashSet<&String>` | `FastSet` | column names; one call | Definition columns. |
| store `direct::StatementCache::entries` | `HashMap<Box<str>, _>` | foldhash | SQL text; per connection, LRU-capped at `capacity` | Bounded per connection; hashed once per statement over the whole SQL text. |
| store `capture` `Shapes` | `HashMap<String, Arc<TableShape>>` | foldhash | tenant schema table names; per database | Schema names, looked up per captured row. |
| `presence` hub maps (`scopes`, `connections`, `members`, `support`) | SipHash | **kept** | member/connection ids; process-wide, long-lived | Shared across tenants and long-lived. A few lookups per beat; not in the profile. |
| `delivery::DeliveryHub::tenants`, `snapshot_cache` `slots`, `replica::shared_definition`, `replica_fanout` `ARGS`/`SLOW` registries, `scheduled_lane`, `offload` maps, `action_gate`, `action_calls` | SipHash | **kept** | tenant ids, subscription argument JSON, …; process-wide | Shared across tenants and long-lived (argument JSON is user-chosen). Per open or per event, not per row. |
| `visibility_index::ParentAclIndex` (`BTreeMap<String, …>`) | BTreeMap | **kept** | parent row ids; per tenant, long-lived, unbounded | Row ids can be user-chosen and the index is unbounded and long-lived: BTreeMap cannot be collided. Not among the top symbols. |
| `visibility_class` maps | FxHash | unchanged | small integers / interned ids | Already FxHash. |

BTreeMap → HashMap: no other candidates on these paths were both unordered and hot.
`RowMap`/`RowHashes`, `ReplicaSubscription` maps and the change-log indexes need their
order (digests, windows, cursor order).

## Measurements

Fixture benches (`docs/performance/perf-fixture.md`), bench profile, 1 tokio worker,
arch3 with ~6 cores of other agents' builds running (load average 6–7 on 14 CPUs). Wall
times moved ±20 % between identical runs of the same binary, so the primary metric is
**user-space instructions** (`perf stat -e instructions:u` of the whole bench process,
repeatable to ±0.05 % run to run), with the fixed fixture-setup cost (same binary with
`BENCH_FILTER=zzznone`) subtracted. Before = 8d39925b binaries, after = this branch;
reducers with `GONVEX_SQLITE_SYNCHRONOUS=normal`.

| Bench group (process run) | Before | After | Change |
| --- | ---: | ---: | ---: |
| `baseline` fixture setup only (seed rows through the change feed) | 23.58 G | 21.06 G | **−10.7 %** |
| `baseline` `snapshot_query/` (3 variants × 17 queries of 9,000 rows) | 47.56 G | 44.59 G | **−6.3 %** |
| `baseline` `fanout/task_update/78_members/*` (3 benches × 33 changes) | 1.520 G | 1.381 G | **−9.1 %** |
| `baseline` `visibility/task_update/78_members` (310 × 78) | 0.488 G | 0.413 G | **−15.4 %** |
| `snapshot_cache` `open/` (fresh, cache-miss build, hit, hit + 1k changes; 3 kinds) | 90.16 G | 85.38 G | **−5.3 %** |
| `delivery` `delivery/task_update/*` | 2.544 G | 2.308 G | **−9.3 %** |
| `delivery` `delivery/presence_beats/*` | 2.119 G | 2.057 G | **−3.0 %** |

Snapshot builds stay SQLite-bound (`sqlite3VdbeExec`, SQLite `json_object`/JSON text,
SHA-256 of canonical rows); the hashing share inside them is what moved:

| `snapshot_cache` `open/`, tokio worker self time | Before | After |
| --- | ---: | ---: |
| SipHash (`Sip13Rounds::write` + std `hash_one`) | 3.9 % | 0 |
| foldhash `hash_one<&String>` (same lookups) | 0.1 % | 0.6 % |

Allocations per op (counting allocator, deterministic):

| Bench | Before | After |
| --- | ---: | ---: |
| `snapshot_cache` `open/…/Admin` cache-miss build (10,500 rows) | 773,139 | 748,463 (−3.2 %) |
| `snapshot_cache` `open/…/TeamAllSpots` cache-miss build | 773,097 | 748,413 (−3.2 %) |
| `snapshot_cache` `open/…/Admin` hit + 1k changes | 298,998 | 292,004 (−2.3 %) |
| `fanout/task_update/78_members/apply` | 14,900 | 14,651 (−1.7 %) |
| other baseline benches | — | within ±10 allocations |

Wall-clock medians (median of 3 interleaved runs per binary; noisy, see above):
`fanout/task_update/78_members/apply` 1.24 → 1.14 ms (−8 %),
`visibility/task_update/78_members` 112.5 → 97.6 µs (−13 %); snapshot opens and
reducers moved by more than their run-to-run spread in both directions and are not
claimed. Resident memory: unchanged by design (same entries; a foldhash `RandomState`
is 16 bytes like std's).

Not measured: the 1k-user step itself (only the integration agent runs e2e). Expected
effect at 1k from the profile: ~6–7 % of CPU in SipHash replaced by ~1 % foldhash.

## Tests

* `cargo test --workspace --no-fail-fast` (debug, featureless): 656 passed, 1 failed,
  8 ignored. The failure, `gonvex-store` `connection_budget::
  one_warm_reader_outlives_the_idle_timeout_until_the_warm_window_ends` (14 ≠ 16 open
  connections), fails the same way on the unmodified base 8d39925b (2/2 runs): a 400 ms
  idle timeout races test setup under machine load. Not caused by this branch.
* `cargo test -p gonvex-runtime --features perf-fixture --no-fail-fast`: 405 passed,
  1 failed, 9 ignored; every oracle suite passes (`snapshot_cache_oracle`,
  `delivery_oracle`, `raw_rows_oracle`, `class_sharing_oracle`, `visibility_fanout`,
  `offload_equivalence`, `replica_state`). The failure,
  `call_watermark_latency::a_call_whose_commit_sends_the_caller_no_rows_completes_within_50_ms`
  (65–86 ms in a debug build), is flaky on base as well: 1 of 3 runs failed on 8d39925b,
  1 of 3 on this branch.
* `cargo clippy -p gonvex-runtime -p gonvex-store --tests --benches`: no new warnings
  (`redundant_clone` is already `warn` for the runtime).
* New test: `fast_hash::tests::serde_json_objects_keep_insertion_order`.

## Risks

* The vendored serde_json must track lockfile upgrades (a `cargo update -p serde_json`
  will not apply while the patch pins 1.0.151; Cargo warns that the patch is unused if
  the requirement moves past it). Re-apply the `map.rs` change to the new version.
* Every crate that depends on serde_json (deno_core, sqlx, axum, …) rebuilds once after
  the patch lands.
* HashDoS exposure of JSON object maps moves from SipHash to randomly seeded foldhash
  (see the table). No protocol, client or wire-format change.
