# V8 module-host memory (workstream 10a: split the bundle)

Date: 2026-11-03. Branch `perf/v8-memory` (base `claude/sqlite-storage-lab` at 72e6d30).
Machine: arch3 (Linux, glibc, 14 CPUs, shared with other agents' builds, so latencies are noisy).
Bundle: the real Whagons module (lab checkout `lab/sqlite-with-masks` 55969f041, 1,982 functions:
992 Reducers, 852 Queries, 138 Actions). Complete bundle 7.9 MB; split Query/Reducer bundle 5.1 MB
(1,506 functions).

## Summary

| option | RSS, pool 1 | RSS, pool 4 | RSS during a generation swap (pool 1) | per-invoke p50 beat / task-update | cold start (load, pool 1) | semantics risk |
|---|---|---|---|---|---|---|
| base (before) | 176 MB | 420 MB | 313 MB, stays 276 MB after | 0.55 / 5.4 ms | 386 ms | — |
| + `malloc_trim` after isolate start/stop and Load | 114 MB | 225 MB | 187 MB, back to 111 MB | 0.54 / 5.1 ms | 464 ms | none |
| + split bundle (Queries/Reducers warm, complete bundle on demand) | 106 MB | 181 MB | 175 MB, back to 108 MB | 0.59 / 5.7 ms | 659 ms | low (see below) |
| + one-byte (Latin-1) bundle source | **101 MB** | **164 MB** | 167 MB, back to 99 MB | 0.51 / 5.1 ms | 673 ms | very low |
| + `GONVEX_V8_FLAGS=--jitless` (opt-in only) | 87 MB | (pool 2: 109 MB) | 155 MB, back to 88 MB | 0.99 / 9.6 ms in the final run, no difference in an earlier sweep | 1,197 ms | none, but no WebAssembly and slower CPU-bound JS |
| first Action on a split engine | +29 MB while warm (102 → 131 MB), released after idle TTL (back to 102 MB) | | | first Action 360 ms (isolate start), then ~1 ms | | none |
| startup snapshot (Node proxy, not implemented) | V8 heap total 42 → 24.5 MB per reducer isolate; +19.5 MB blob per generation | | | unchanged | bundle start ~220 → ~50 ms | medium: bundle must be snapshot-safe |
| code cache (Node proxy, not implemented) | heap unchanged | | | unchanged | compile 150 → 10 ms (reducer), 190 → 21 ms (complete) | none (V8 validates the cache) |

RSS is the module-host process `VmRSS` after load, 20 warm-up and 400 measured calls of each workload,
with `--second-generation` loading generation 2 and activating it. "Per-invoke" is the full socket
round trip of the micro-driver, including fake host calls (2 for `presence.beat`, 27 for
`tasks.updateDescription`). The host is CPU-light: the beat is ~0.5 ms and the task update ~5 ms
end to end, dominated by host-call round trips, so no option changes per-invoke cost measurably except
`--jitless` (inconclusive under machine load).

Before → after for the recommended defaults (trim + split + one-byte source):
**176 → 101 MB at pool 1 (−43%), 420 → 164 MB at pool 4 (−61%), and a deploy peak of 313 → 167 MB that
now returns to 99 MB instead of staying at 276 MB.**

## Where the memory was

V8 heap per isolate right after the bundle evaluates (`GONVEX_V8_HEAP_STATS=1`):

| bundle | heap total | heap used | of which the source string |
|---|---|---|---|
| complete, as shipped | 38.4 MB | 33.5 MB | 15.8 MB |
| complete, one-byte | 29.0 MB | 26.4 MB | 7.9 MB |
| reducer bundle | 24.9 MB | 22.0 MB | 10.2 MB |
| reducer bundle, one-byte | 21.1 MB | 18.8 MB | 5.1 MB |

A heap snapshot (`GONVEX_V8_HEAP_SNAPSHOT_DIR`) of the complete bundle: 15.8 MB source string, 3.1 MB
objects, 2.1 MB shapes, 1.8 MB small strings, 1.5 MB object boilerplate, 1.4 MB closures, 0.9 MB
bytecode. **Half the heap is the bundle source**, kept by V8 for lazy compilation and stored as UTF-16
because the bundle contains 12,256 characters above U+00FF (HTML-entity tables, an em dash in a
comment, combining-mark regex classes). One such character makes the whole 7.9 M-character string
two-byte.

Outside the V8 heap, the dominant cost was glibc: V8 parses and compiles on each isolate's own thread,
and glibc keeps the freed parser/compiler zones in that thread's malloc arena. Each isolate therefore
cost ~100 MB RSS for a 38 MB heap, and a retired generation's isolates gave nothing back (276 MB after a
swap). Rust's `mimalloc` global allocator (used by the runtime) does not cover V8's C++ mallocs.

Process baseline: ~10 MB idle, ~27 MB file-backed (binary text). Each nesting depth has its own
isolate pool (9 pools per bundle), but only depth 0 is warmed; nested calls start depth-n isolates lazily.

## What changed (all in this branch)

1. **Native heap trim** (`module-runtime-v8/src/isolate.rs`, `trim_native_heap`): `malloc_trim(0)`
   after an isolate finishes loading, after its thread exits, and after a `load` request returns.
   On by default on Linux/glibc; `GONVEX_MODULE_MALLOC_TRIM=0` disables. Cost: one trim per isolate
   start/stop, not per call. `gonvex_module_runtime_v8::trim_native_heap` is public so an in-process
   embedding (perf/native-reducers) can call it after evaluating its bundle.
2. **Split bundle** (CLI `packages/gonvex/src/module-artifact.ts`, runtime `modules.rs`, module host
   `artifact.rs`/`session.rs`, engine `module-runtime-v8/src/lib.rs`, `pool.rs`): see the contract below.
   A split engine warms only the Query/Reducer bundle. The complete bundle is evaluated once at load
   (so a bundle that throws still fails the load; `V8Config::validate_on_demand_bundle`) and released;
   Action isolates start on the first Action and are released after `--on-demand-idle-ms`
   (env `GONVEX_MODULE_HOST_ON_DEMAND_IDLE_MS`, default 60 s) by a per-engine reaper thread.
3. **One-byte bundle source** (CLI `latin1JavaScript`): split artifacts write both bundles with every
   character above U+00FF escaped as `\uXXXX`, located with rolldown's `parseAst`; tagged-template
   quasis are left alone because their raw text is observable. The output is re-parsed and every string
   literal and template cooked value is compared with the original; any difference keeps the original
   source. Only `Function.prototype.toString()` text can differ. Build time for Whagons 4 → 8 s.
4. **Diagnostics**: `GONVEX_V8_FLAGS` (applied before V8 initializes), `GONVEX_V8_HEAP_STATS=1`,
   `GONVEX_V8_HEAP_SNAPSHOT_DIR=<dir>`.

Isolate pool size: the runtime already defaults `GONVEX_MODULE_HOST_ISOLATE_POOL_SIZE` to the CPU
count clamped to 1–4, i.e. 1 on a 1-vCPU box, and the module host's own default is 1. Not changed.
Heap ceiling (`--max-heap-mb`, 64 MB) already exists; `--max-semi-space-size=1` and
`--optimize-for-size` showed no RSS change (within ±4 MB, noise) and are not set.

## Contract: how an app declares the split

```jsonc
// gonvex.json
{ "module": { "entrypoint": "gonvex/index.ts", "split": true } }
```

With `module.split`, `gonvex` emits next to the unchanged `module.javascript` (complete bundle):

```jsonc
"reducerBundle": {
  "path": "gonvex/_build/module.reducers.js",
  "hash": "<sha256 of the bundle bytes>",
  "code": "<base64>",
  "functions": ["presence.beat", "tasks.updateDescription", "..."]   // sorted; Queries and Reducers only
}
```

- The bundler generates a virtual entry that imports each Query/Reducer export from its defining file
  and re-exports it **under its exact function path** (`export { beat as "presence.beat" }`), so
  rolldown drops Action-only exports and whatever only they import.
- Hosts resolve `reducerBundle` functions **by path only**. (The complete bundle's resolver tries the
  `export` name on the entry namespace first; in Whagons 41 replica Queries such as `calling.settings`
  collide with an unrelated top-level namespace (`settings`). They are runtime-served replica Queries,
  so nothing breaks today, but path-only resolution removes the ambiguity.)
- Functions not listed run from `module.javascript`: all Actions, the SDK's `__local.*` replica
  Queries, and any function without a static export binding.
- `reducerBundle.{path,hash,functions}` is part of the signed artifact hash (CLI `artifactHash`, Rust
  `canonical_artifact_hash`). Unsplit artifacts keep their existing hash. The runtime rejects a
  `reducerBundle` that lists an unknown function or an Action; the module host verifies its SHA-256.
- Target shape (Gabriel, 2026-11-03): the runtime's in-process isolate loads `reducerBundle`
  (perf/native-reducers) and the module host keeps only the complete bundle for Actions, started on
  demand. `V8ModuleEngine::from_split_artifact(artifact, Some(ReducerBundle { code, functions }), config)`
  serves both roles today.
- Whagons opts in on `perf/bundle-split` (whagons5-client 51501c890, `gonvex.json` only). Older CLIs
  ignore the key.

### Semantics

- Equivalence check on Whagons (`bench/v8-memory/split-equivalence.mjs`): all 1,506 split functions
  resolve to the same definition as in the complete bundle (same kind, identical handler source after
  normalizing bundler `name$N` renames).
- Remaining risk: a Query/Reducer that depends on a top-level side effect of a file it does not import
  (for example a registry filled by an Action-only file) would see different state in the reducer
  bundle. Each isolate already had its own module state, so cross-isolate state was never shared.
- Load still fails on a complete bundle that throws while evaluating.

## Measured potential of the bigger options (plans)

### Remove Action-only libraries from Query/Reducer code paths (app change, not done)

The reducer bundle still contains `ai`, `@ai-sdk/*`, `linkedom`, `parse5`, `@json-render/*` and the
765 KB `assistantAgentCatalog.generated.ts`, because Reducers really call into them (for example
`gonvex/workspaceViews.ts` → `@json-render`, `gonvex/workbenchOperationHelpers.ts` → `parse5`,
`gonvex/agents.ts` Reducers → `shared/agentSkills.ts` → assistant catalog). Marking SDK definition
helpers pure (`treeshake.manualPureFunctions`) changes nothing (5.12 vs 5.13 MB). Stubbing those
libraries in a measurement build gives 3.98 MB of source and a 21–22 MB heap (two-byte), about
4 MB per isolate less, ~9 MB less RSS at pool 2. Plan: move the AI/HTML-dependent work in those
Reducers behind Actions or lazily-imported helpers in the Whagons app, then re-measure with
`build-artifact.mjs --split` + `driver.py`.

### V8 code cache (not implemented)

Node 22 (V8 12.4) proxy on the same bundles (`v8-lab/snap/codecache.js`): compile 150 → 10 ms for the
reducer bundle, 190 → 21 ms for the complete bundle; cache 2.6 / 5.2 MB; heap unchanged. Worth it for
on-demand Action isolates (first Action 360 ms today). Plan: install a deno_core `ModuleLoader` that
serves the bundle with `ModuleSource` code cache, produced by the generation's first isolate and kept
next to the source (one copy per generation). No semantics risk: V8 rejects a cache that does not match
the source and flags.

### Startup snapshot (not implemented)

Node proxy (`node --build-snapshot`): reducer bundle start ~220 → ~50 ms, heap total 42 → 24.5 MB
(compacted, no young generation yet), blob 19.5 MB (complete: 28.4 MB). RSS per isolate is not lower
than a trimmed one-byte isolate, and each generation adds its blob, so it is a cold-start option, not a
memory option. **Blocker found:** the Whagons bundle cannot be snapshotted as is: top-level
`new Intl.Collator(...)` (`naturalCompare`) and module-level `new TextDecoder()` instances hold native
objects V8's serializer rejects. With those made lazy (patched copy) the snapshot builds. Plan:
deno_core `JsRuntimeForSnapshot` with the same `gonvex_host` extension, bootstrap and bundle, a build
step that fails on non-snapshottable top-level state, and snapshot-safe app code (lazy Intl/TextDecoder).
Risk: top-level `Date.now()`/`Math.random()` values are frozen into the snapshot.

### Dropping source strings / sharing between isolates and generations

V8 needs the source for lazy compilation, so it cannot be dropped after compile. deno_core 0.320 only
creates external (off-heap, shareable) strings for `'static` ASCII sources, so sharing one copy across
isolates would need a deno_core change or leaking each generation's source; the one-byte bundle takes
half of that win without either. The Rust side keeps one decoded copy per bundle per generation (7.9 +
5.1 MB) for starting new isolates.

## Reproduce

```sh
pnpm install && pnpm run build:packages
node bench/v8-memory/build-artifact.mjs <whagons-checkout> /tmp/whagons-split.json --split
cargo build --release -p gonvex-module-host     # see BRIEF build rules
python3 bench/v8-memory/driver.py --host target/release/gonvex-module-host \
  --manifest /tmp/whagons-split.json --tenant-db <copy of a lab tenant .db> --calls 400 \
  --second-generation --host-arg=--isolate-pool --host-arg=1 --env GONVEX_V8_HEAP_STATS=1 \
  [--no-split] [--action microsoft365.getMicrosoftConnectionStatus --idle-wait 4 \
   --host-arg=--on-demand-idle-ms --host-arg=2000]
node bench/v8-memory/split-equivalence.mjs /tmp/whagons-split.json
```

The driver starts one module host, loads the artifact over the socket protocol and answers host calls
from a copy of a lab tenant SQLite file (each invoke runs in a rolled-back transaction). It is a
component micro-benchmark, not a user simulator. Raw results: `/home/gabriel/work/gonvex-perf/v8-lab/runs/F-*.json`.

Not measured: the Mac lab container (1 CPU / 2 GB); the in-process runtime embedding; Actions with real
network I/O; the effect on the full runtime's RSS.
