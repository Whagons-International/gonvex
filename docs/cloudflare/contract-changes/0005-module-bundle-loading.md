# 0005: Loading tenant module bundles without `eval`

Date: 2026-11-10. Status: **decided in 0016 G0-2** (bundle in the deployed Worker version for stage 1; S5 tests the Worker Loader). Affects: K04, K19, S2.

## Finding

Rust loads each project's bundle into a V8 isolate at runtime, one generation per `/dev/sync`
(`rust/crates/module-runtime-v8/src/isolate.rs:1102-1123`). Workers forbid `eval` and `new Function`, so
the kernel cannot load a bundle that arrives through `/dev/sync` the same way.

## Options

1. **Worker Loader (dynamic Workers).** The tenant object loads the bundle for its generation into a
   dynamic isolate and calls it with host-call bindings. Keeps `/dev/sync` semantics (no wrangler deploy
   per sync). Cost: an RPC hop per host call unless the loader isolate gets a SQL binding.
2. **Per-generation script upload.** `/dev/sync` uploads the bundle as a Worker version and the tenant object
   binds to it by version. Host calls cross a service binding.
3. **Bundle the app into the kernel at build time.** Fastest at runtime, but `/dev/sync` becomes a deploy;
   breaks `gonvex dev`.

S2 runs reducers through `packages/local-runtime`; its report must state which loading path it used and the
startup time of the full app bundle under that path. K04 may not start until this note is decided.
