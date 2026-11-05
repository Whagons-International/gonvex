# Raw snapshot rows end to end (perf/snapshot-raw)

Branch `perf/snapshot-raw`, from `perf/integrate-5k` 3db7f121. Module-level work on the
shared perf fixture; no lab run. Bench: `cargo bench -p gonvex-runtime --features
perf-fixture --bench snapshot_raw` (scenarios in the file header). Site counters:
`gonvex_runtime::raw_sites` (perf-fixture only; a no-op otherwise).

## Why

1k production mix (step-log, ov-1k-prod-b): CPU-bound; the offload pool (snapshot builds)
44 % of samples with "building JSON rows" 10.5 %; top self symbols `indexmap` insert /
bucket clone / `drop_glue<Value>`. At 500 (default mix) the top symbol was change
collapsing (`BTreeMap<(&str,&str), CollapsedChange>`). delivery-v4 made snapshot rows raw
text, but several paths still built `serde_json::Value` rows.

## Where `Value` rows were still built (site table, before)

`snapshot_raw` bench, 9,000 tasks, 78 members, one tokio worker pinned to one core, per
operation (median of 5). Allocations are counted on the calling thread inside each site
(`raw_sites`); site times are wall time inside the site (they include preemption by the
SQLite threads on the shared core, so use allocations and the scenario CPU column).

| site (what) | scenario | calls / op | allocations / op before | after |
|---|---|---:|---:|---:|
| `scan_row_parsed`: SQL-scanned row whose text the canonical check rejected → `Value` + `stable_json` | Admin cold open (fresh) | 2,663 of 9,000 rows | 272,417 (102 / row) | 0 (all 9,000 rows raw) |
| `scan_row_raw`: the raw path per row | Admin cold open | 6,337 → 9,000 | 76,962 (12.1 / row) | 65,329 (7.3 / row) |
| `cache_row_parsed` + `log_image_row`: cache fill row parsed, projected, re-serialized | Admin cache miss | 2,663 | 475,595 (179 / row) | 0 |
| `cache_row_raw` | Admin cache miss | 6,337 → 9,000 | 73,889 (11.7 / row) | 74,316 (8.3 / row) |
| `cache_row_parsed` (other 10 login collections: ACL/filter columns outside the projection) | cold login, cache miss | 234 | ≈ 22,000 | 0 |
| `log_row_decode`: change-log row → `LogChange` with `Value` images | Admin hit + 1k changes | 1,000 | 166,322 (166 / row) | 17,980 (18 / row) |
| `log_image_row`: cached row from a log image (`project_row` + `stable_json` + eval `Value`) | Admin hit + 1k changes | 1,000 | 86,753 | 9,992 |
| `cache_view` (incl. the log rows above) | Admin hit + 1k changes | 1 | 94,262 | 16,501 |
| `collapse_changes`: `BTreeMap` per connection per event | feed, 50-row burst, 78 connections | 78 → 1 | 4,446 | 106 |
| `feed_apply_rows`: per subscription (decision clones, touches) | feed, 50-row burst | 235 | 237,128 | 111,342 |
| `resume_image`: aggregate resume, source images projected + hashed | Admin resume after 200 | 400 | 34,572 | 65,426 (now parses the images it needs; log decode −36k) |
| `acl_decode`: cached rows' ACL columns for a class scope (once per entry and scope) | Admin cache miss | 1 | 72,833 | 72,833 (unchanged, see "Left") |

Why rows fell off the raw path: every `tasks` row with a non-null `audienceRules` JSON
column (2,663 of 9,000 in the fixture; `{"mode":…,"teamIds":…,"roles":…}`): the canonical
check gave up on any nested object whose keys are not sorted. Login collections whose
visibility/filter/order columns are not projected took the `Value` path on every cache
fill. Change-log images were always parsed.

## What changed

1. **Nested objects are re-ordered on the raw path** (`canonical_row`). The checker
   records each nested object with unsorted keys (byte range + members sorted by key) and
   the canonical writer emits recorded objects in key order, recursively, with number
   replacements inside them. Escaped or duplicate keys still fall back. Fixture: every row
   of both tenants is now on the raw path (raw_rows_oracle: 2,320 / 2,320 and 503 / 503,
   was 71 %), 0 hash mismatches.
2. **Fewer allocations per raw row.** Rows are admitted straight from the SQLite batch
   (`&str`, no `String` per row), the member list buffer is reused, the key is an
   `Arc<str>` sliced from the text, the hash is hex-formatted on the stack into one
   `Arc<str>`, and `JsonRow::from_str_unchecked` builds the shared text in one allocation
   without the second validation pass (`RawValue::from_string`) — the row checker already
   validated it (debug builds assert it). Scan rows 12.1 → 7.3 allocations.
3. **Cache fills with extra columns stay raw.** The cache read is `json_object(projection…,
   extra…)`; the projection is the leading members, so it is sliced out of the text (`{` +
   prefix + `}`), hashed via the canonical check; the full text is the eval text. Rows whose
   members do not line up with the definition fall back to the old path.
4. **Change-log images stay text** (`raw_log`): `read_changes` keeps each image as the
   trigger's JSON with its top-level member ranges (`RawImage`); cache views read key /
   filter / order columns from the ranges and build cached rows by slicing the projection
   (`CachedRow::from_raw_image`). Images the checker rejects are parsed and use the old
   `Value` functions. The aggregate resume parses only the source-table images it decides
   (visibility there is still the interpreted `Value` check, unchanged).
5. **One change collapse per event** (`CollapseIndex` in `feed_share`): positions into the
   event's change list, sorted by (table, row) and built by the first connection; every
   other connection binary-searches it. Same order and same before/after images as the
   per-connection `BTreeMap`.
6. **Feed decisions hold `Arc<Value>` rows** (`RowDecision`, touches, upserts): a class-shared
   decision is no longer deep-cloned per subscription; the delta's upserts are unwrapped
   (cloned only when shared) when the message is built.

Wire and hash contract: row hashes and digests are byte-identical (oracles below). Row
*text* of cached rows built from SQL rows with extra columns, and of rows replayed from the
log, is now the column order SQLite / the triggers wrote (same members and values) instead
of the canonical (sorted) text; the fresh SQL path always sent SQLite's order, so clients
already receive both. No protocol change.

## Results

arch3 (Ryzen 7 5800X, load average 12–15 from other agents' builds), benches pinned to one
core, one tokio worker. Before = 3c5b939e (= base + counters + bench only), after = head.

`snapshot_raw` (9,000 tasks, 78 members; user CPU of the process, median of 5):

| scenario | user CPU ms | allocations | allocated MB | peak heap MB |
|---|---|---|---|---|
| Admin cold open, fresh SQL (9,000 rows) | 178.7 → 119.4 | 362,554 → 73,824 (−80 %) | 149 → 65 | 18.0 → 18.0 |
| Admin cache miss (build + view) | 219.1 → 139.2 | 639,307 → 155,133 (−76 %) | 191 → 78 | 28.4 → 28.4 |
| Admin cache hit | 11.3 → 3.2 | 6,104 → 6,107 | 3.6 → 3.7 | 1.8 → 1.8 |
| Admin hit + 32 changes | 14.7 → 6.5 | 15,055 → 7,808 | 6.2 → 5.4 | 2.9 → 2.7 |
| Admin hit + 1,000 changes | 87.7 → 37.0 | 284,239 → 57,083 (−80 %) | 54 → 28 | 21.1 → 13.6 |
| Admin resume after 10 / 200 changes | 14.0 → 5.9 / 38.7 → 19.5 | 12,077 → 7,552 / 93,782 → 79,176 | | 3.3 → 2.9 / 9.4 → 6.4 |
| Admin cold login (11 collections), cache miss | 335.8 → 192.0 | 934,231 → 377,110 (−60 %) | 255 → 122 | 62.7 → 62.7 |
| Admin cold login, cache hit | 44.1 → 29.4 | 75,547 → 75,546 | 21.4 → 21.4 | 1.8 |
| Admin detail window (8 collections), miss / hit | 5.6 → 4.1 / 5.6 → 3.7 | 23,435 → 23,297 | | 0.4 |
| TeamSpots cold open, fresh (652 rows) | 38.1 → 23.5 | 30,552 → 9,987 | 11.5 → 5.4 | 1.8 |
| TeamSpots cache miss | 235.8 → 132.1 | 636,355 → 152,186 | 188 → 76 | 26.9 |
| TeamSpots hit + 1,000 changes | 68.6 → 35.3 | 281,388 → 54,053 | 51 → 25 | 19.9 → 17.1 |
| TeamSpots cold login, miss / hit | 249.1 → 191.1 / 30.4 → 28.6 | 924,275 → 367,148 / 65,594 → 65,593 | 249 → 116 | 39.0 |
| feed: 1-row burst to 78 connections | 1.35 → 1.5 | 11,419 → 10,514 | 1.4 → 1.0 | 1.3 → 1.2 |
| feed: 50-row burst to 78 connections | 39.2 → 37.4 | 424,000 → 380,434 | 59 → 45 | 9.3 → 8.7 |

The existing `snapshot_cache` bench (10,500 tasks; process CPU median of 5) agrees:
Admin fresh 204.9 → 188.0 ms / 419,626 → 84,683 allocations; cache miss build 250.5 → 198.3
ms / 740,889 → 179,611; hit + 1k changes 124.4 → 65.5 ms / 283,961 → 56,972, peak 21.2 →
13.7 MB; TeamAllSpots fresh 60.2 → 46.0 ms; resumes 10–35 % less CPU, −60..−80 %
allocations. GrantOnly (45 rows): within noise (hit + 1k changes 27.0 → 38.4 ms and fresh
after 1k 35.4 → 42.9 ms in this run, allocations −25 %).

CPU numbers are from a shared host (load 12–15); allocations are exact and repeatable.

### 1k plateau replay (open_visibility driver)

`BENCH_FILTER=driver BENCH_MEMBERS=1000 DRIVER_SECONDS=60 DRIVER_LOGIN_RATE=4.4
DRIVER_DETAIL_RATE=4.3 DRIVER_WRITE_RATE=10 DRIVER_GRANT_RATE=0.3`, one core, two runs each,
interleaved: user CPU before 21.7 / 18.4 s, after 18.8 / 22.2 s; login p99 518 / 124 ms vs
125 / 345 ms; detail p99 100 / 76 ms vs 75 / 167 ms. **No measurable difference**: the
run-to-run spread is larger than the change, and this replay is almost all cache hits
(2,642 hits; its 2,075 builds are keyed detail entries of a few rows), so the cold-scan,
cache-miss and log-replay savings above barely occur in it. The production profile's share
of `Value` construction comes from those paths (misses after max-age / sparse keys / roll
limits, fresh scans of sparse keys, resumes, feed bursts), which this replay does not
reproduce. Not measured: a lab run (integration thread).

## Tests

* `cargo test -p gonvex-runtime -p gonvex-profiler -p gonvex-protocol -p gonvex-store
  --features gonvex-runtime/perf-fixture --no-fail-fast`: all pass (runtime lib 379 passed /
  7 ignored; `raw_rows_oracle`, `snapshot_cache_oracle`, `principal_cache_oracle`,
  `delivery_oracle`, `class_sharing_oracle`, `replica_state`, `offload_equivalence`,
  `visibility_fanout`, `slow_reader_delivery`, …; store 43 + integration suites).
* `cargo check --workspace` (featureless): passes.
* `cargo clippy -p gonvex-runtime -p gonvex-protocol --features gonvex-runtime/perf-fixture
  --benches --tests`: no warnings in the new files; pre-existing warnings elsewhere remain.
* New unit tests: nested re-ordering cases in `canonical_row` (deep nesting, numbers inside
  re-ordered objects, duplicate / escaped nested keys fall back); `raw_log`:
  filters / key / column / projection of raw images equal the `Value` functions for valid,
  escaped, U+2028, duplicate-key and nested images.

## Risks

* `JsonRow::from_str_unchecked` is `unsafe` (an `Arc<str>` → `Arc<RawValue>` cast, same
  layout as serde_json's own `Box` cast). Every caller passes text the row checker
  accepted (a full JSON grammar check, no whitespace); debug builds assert validity.
* Cache fills with extra columns rely on SQLite writing `json_object` arguments in order
  (checked per row: members must equal the definition's columns; otherwise the old path).
* Cached rows from SQL-with-extras and from the log now carry non-canonical member order
  (hashes unchanged). A client that compared row *text* instead of hashes would see a
  difference; the fresh path already sent this order.
* `CollapseIndex` is shared through the event share: it must be built from the same change
  list (the share matches by `Arc::ptr_eq` on the list); without a share each connection
  builds its own.

## Left

* `acl_decode` (8 allocations per cached row, once per entry and class scope: owned ACL
  strings) and per-row allocations inside the compiled evaluator for member-hit rows
  (≈ 2.3 per row on login cache hits): visibility layer, not row JSON.
* Feed full path: the delta and transaction messages are still `Value` payloads
  (`ServerMessage::ReplicaDelta`), so a sent upsert is cloned once per receiving
  subscription; changing that is a protocol-level change. Most connections take the
  shared fan-out path in production anyway.
* Aggregate resume still parses the source-table images it decides (interpreted
  visibility); a compiled `RowView::Json` check would remove that.
* In the `snapshot_cache` bench the resume scenarios cost about the same as a full SQL
  scan on both binaries (Admin 140–155 ms vs 8 ms for a cache hit). Cause: the per-key
  sparse bypass of perf/cache-heap (see `cache-heap.md`, "Per-member sparse decisions").
  The bench's GrantOnly opens made the tasks key sparse, so every later resume skipped the
  cache. Fixed on perf/resume-fix.
