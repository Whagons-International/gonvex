# SQLite hydration findings

Baseline: staging `79668483b46fd8901aa0beacaf9f6e837d2f92e6`. Tests use the actual Expo storage implementation and public LocalReplica methods over Node's SQLite database adapter.

## Bounded startup claims complete coverage

Loading a working set stops at its row or byte budget but previously omitted `residentPartialTables`. Hydration could then mark a table complete even though some or all of its rows remained only on disk. A table scan could return too few rows without requesting SQLite fallback.

Row and byte budget regressions reproduce the incomplete result on staging. Three additional cases cover a data-only scope when no row fits. The adapter now compares admitted rows with live row counts from the same read transaction. Omitted tables become partial and scans retry through disk. This adds a grouped count query over the stored scope; it avoids decoding omitted JSON into JavaScript but does add database work proportional to the scope's stored rows.

## New epoch leaves old disk rows

`applyTransaction` cleared epoch-bound state on epoch changes, but `replaceWindow` did not. A new-epoch snapshot could clear memory while leaving other old windows, rows and row authority on disk. A later cold start loaded those obsolete records again.

The public snapshot/cold-hydration regression reproduces old records surviving on disk. Both write paths now share the same epoch cleanup inside their transaction, before reading prior membership or merging fields. A rollback regression verifies that a cursor-write failure restores the old entities, windows, authority and cursor. Sessions, unrelated metadata and other scopes remain covered.

Review also exposed a correction needed for a cursor-bearing window written with a cursorless snapshot. The adapter now checkpoints the window epoch at global revision zero rather than mistaking the collection revision for a proven global feed floor. Consecutive windows survive, and the monotone checkpoint upsert preserves an existing same-epoch global floor. A regression first reproduced repeated resets in the initial proposal, then covered both windows and cold hydration.

## Validation boundary

These tests establish adapter and LocalReplica behavior with Node SQLite. They do not establish native Expo bridge scheduling, concurrent callers on an Expo database connection, phone performance or formal protocol correctness. Memory publication of stale writes rejected by storage, optimistic settlement and outbox ordering are separate audit candidates.

## Bounded cost measurement

A warm, in-memory Node SQLite comparison loaded 64 rows from scopes with 1,000 and 10,000 stored rows. Eight alternating pairs of 30 calls measured median wall time of 0.197 to 0.308 ms and 0.192 to 2.191 ms respectively. The grouped count adds work as stored rows grow. This measures the exact proposed loadWorkingSet implementation, before the independent checkpoint correction, with one CPU and 512 MiB, zero swap. The initial benchmark preparation failed on a missing relative module import; the corrected preparation completed. This is a local microbenchmark, not disk startup latency, an Expo bridge measurement or a performance-equivalence claim.
