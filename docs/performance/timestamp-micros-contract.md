# Explicit SQLite microsecond timestamps

This slice implements the physical contract from the local source-compat
candidate `3438d803a7db8deaf1c7229be5c74544b10c9952`. It does not qualify the
converted dataset or activate a permission provider.

`TIMESTAMP_MICROS` is a new, exact declared kind. Values are signed SQLite i64
microseconds since Unix epoch, or SQL NULL. The source NOT NULL and storage-class
CHECK constraints are independent and remain the converter's responsibility.
The encoder rejects non-INTEGER storage and timestamps outside the original
chrono `DateTime<Utc>` wire range. Native integer input never passes through f64
or milliseconds. SQL projection, SQLx row decoding, direct row encoding and
native captured old/new images use the same canonical RFC3339 AutoSi UTC-Z
representation, preserving microseconds and negative epochs.

`timestamp_micros::encode(i64)` is the pure codec entry point for the exporter.
`parse_rfc3339(&str)` is the explicitly typed parameter conversion. It normalizes
RFC3339 offsets to exact UTC microseconds and rejects naive timestamps,
more-than-six fractional digits and leap-second normalization. Those rejected
domains require independent PostgreSQL normalization/rounding proof. A typed
`Parameter::column(..., JsonKind::TimestampMicros)` accepts only RFC3339 text or
NULL. Existing untyped scalar, INTEGER and millisecond-number behavior is
unchanged; no numeric JSON value is guessed to be a timestamp.

Enable the store Cargo feature `timestamp-micros` for the experimental physical
schema. `permission-primitives` enables it transitively. Default builds do not
install a clock trace, so normal v2 queries acquire no clock-hook overhead.
The factory registers the functions on every new SQLx writer/reader connection;
direct sessions lease those same connections. Standalone native consumers must
call `timestamp_micros::install()` before opening connections.

- `pg_now_micros()` captures native wall-clock microseconds at BEGIN, preserves
  that value across statements/savepoints until COMMIT/ROLLBACK, and gives each
  autocommit statement one stable value including its trigger bodies.
- `pg_clock_timestamp_micros()` reads the native wall clock on every invocation.
- `gonvex_timestamp_micros_rfc3339(value)` is the strict deterministic SQL row
  projection function. It preserves NULL and errors on invalid storage/range.

The connection-open trace delegates the framework's existing statement profiler
and leaves pre-update/commit/rollback/WAL hooks unchanged. Trace data is retained
by SQLite connection client-data ownership independently of replaceable UDFs.
Its destructor does not touch the trace callback, so it cannot remove a later
native observer. Registration is an exclusive connection-open operation;
subsequent native instrumentation must cooperate with this dispatcher. Arbitrary
replacement of its statement trace is not a supported timestamp-clock mode.
Engine direct sessions always run inside a tenant transaction; overlapping live
statements in that transaction share the same start. No broader raw-native
cursor/concurrency contract is claimed.

Synthetic verification includes actual store SQL projection, SQLx/direct/capture
parity, transaction/savepoint/commit/rollback and automatic rollback, failed BEGIN,
failed statements, reset/cancelled cursor, abandoned SQLx transaction/reuse,
triggers/defaults, registration failure/close lifetime and profiler delegation.
An independent embedded PostgreSQL fixture checks 20 exact values, 50 typed
filter cases, six order/LIMIT cases, NULL ordering and transaction clock
properties. This is not an all-application-route or production-data oracle.

The explicit ignored hook observation alternates 12 pairs of 10,000 native
`SELECT 1` executions on one connection using thread CPU time. One debug local
observation measured medians 307.332 ns without the hook and 599.489 ns with it
(+292.157 ns/statement). Other host lanes were uncontrolled. These numbers
measure only hook overhead; they establish no app speed, capacity, cold-open or
full-provider acceptance. Default builds keep the hook off.
