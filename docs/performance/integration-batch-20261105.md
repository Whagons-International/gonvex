# Authorized integration batch — failed native historical gate

The integration starts at exact `2b7e2ff9f6950ea73a4659c284c9204262314574`, merges finished `6cbf35541065a9649f3d22800619edfcfb470aa0` ancestry and cherry-picks only `ec00c7fe12f7272cca357e1220bd1730f1d8dd22`. Cold-open `39ce591e` is already incorporated as `eeed71e3`; `1c94c8ac` and existing `bc526fbd` are patch-id identical. Full receipts are retained. No full-provider WIP or heap changes were consumed; shared `perf/integrate-5k` stays unchanged.

The combined timestamp/starter exhaustive match now explicitly declines unqualified timestamp wire encoding/order. Its regression binds fresh authority to an actual changed SQLite schema. Default gate features remain off, and the newer finished SDK hydration core remains in source. The historical gate independently uses the pinned candidate-2 shipping SQLite.1 composition, not this branch's optional SDK output.

Verification passes: runtime407 default/412 fixture/456 starter tests, store/protocol/profiler suites, six integration oracle suites,1111 client tests,58 module-SDK tests,49 selected CLI/artifact tests,19 OPFS checks, admission checks, types/generators/docs metadata and unchanged28672-byte optimized future budget. Full-package runtime cyclic edges2→2, none added. Canonical generation produces no tracked change.

One native Mac application series used the historical local-load profile,1CPU/2GiB runtime at7,15,6GiB/2CPU aggregate, unchanged1000-user schedule and full132-intent model. It failed in the runtime child OOM at approximately702.149s (raw cross-host wall timestamps). Durable stop:698 attempts,676 authenticated,396 initially ready,8/8 fsynced worker partials. Both75-user burst windows started on time;150-user burst/steady/cached refresh were not reached. Runtime cgroup peak2148564992 bytes; aggregate3328163840. Sampled snapshot waiter peak516, cold-open active258, last sampled cumulative refusals1018145. Ten mandatory visibility oracle failures remain. No completed1k, full Whagons/starter acceptance or5k/core projection.

The harness was frozen at532e835a before the separately reviewed15cb6ca1 oracle repair and was not swapped. A prestart database hashing pass warmed filesystem cache, so differences from the original baseline are not an isolated causal performance comparison. Wrong-profile preflight and a recovered density-receipt path mistake are preserved separately. No staging/production/deployment/CI/PR/customer-data work occurred.

Detailed report, source/artifact pins, all phase p95/p99/error/CPU/memory results, limitations, preserved failures and cleanup receipts:
`/mnt/data/work/gonvex-integration-batch-20261105/REPORT.md`.
