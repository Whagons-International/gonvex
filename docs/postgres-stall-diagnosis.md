# Diagnosing PostgreSQL stalls

The 2026-10-28 staging incident exhausted SQLx pools and then the shared SQL
admission budget. `agents.dispatch`, `deepResearch.tick` and
`journeySync.processDue` failed waiting for connections. Requests resumed
together around 21:04:17 UTC after approximately 30 seconds, then around
21:05:19 UTC after approximately 60 seconds.

Retained Coolify Sentinel samples show spare host CPU and memory during both
pauses. Runtime and PostgreSQL CPU activity fell during the pauses. Neither
container restarted or reported an OOM. The active app manifest and latest
tenant migrations were installed around 18:21 UTC, before the incident.
This rules out a contemporaneous deployment and provides no evidence for CPU
or memory exhaustion. It does not identify the initiating blocker.

SQLx's statement timer starts before `PgConnection::wait_until_ready`, so even
a slow `SELECT 1` can include waiting for the preceding operation's protocol
responses. Statement elapsed time alone cannot establish PostgreSQL execution
time. The deployed legacy database-wait metrics are zero placeholders.
Separate acquisition metrics are proposed in PR #207, currently on hold.

## Capture the blocker independently

Run `scripts/postgres-stall-recorder.py` on the Docker host with the exact
staging PostgreSQL and runtime container names:

```sh
python3 scripts/postgres-stall-recorder.py \
  --postgres-container STAGING_POSTGRES_CONTAINER \
  --runtime-container STAGING_RUNTIME_CONTAINER \
  --samples 600 --output /private/path/stall.jsonl
```

The recorder connects through PostgreSQL's local socket, independently of
Gonvex's admission semaphore and SQLx pools. It uses the container's existing
PostgreSQL user/database configuration. Both probes run concurrently.
Each record includes direct runtime health latency, database probe latency,
session states, PostgreSQL wait events, transaction/query ages, blocking PIDs
and query hashes. Query text, parameters, passwords and connection URLs are
excluded. Files are private and rotate across three files of 10 MiB each,
matching the deployed Docker logging budget. A file can exceed the rotation
size by one record.

The default one-second interval matches the existing SQLx slow-statement
threshold. The two-second probe timeout matches the slow-acquisition warning.
PostgreSQL's one-second statement timeout applies only to the diagnostic
connection. The sample count is mandatory; the recorder ends after that count.
Container inspection follows IP changes for the supplied runtime container
name. A replacement with a different name needs a new recording command.

The initial staging capture recorded 180 samples at one-second intervals from
21:18:18 to 21:21:17 UTC, with no blocked sessions or failed probes. A bounded
24-hour capture then started at 21:22:35 UTC under the transient systemd unit
`gonvex-staging-stall-20261028.service`. It samples every five seconds, providing
six observations within the shorter 30-second stall seen in this incident.
Its private output is
`/var/lib/gonvex-diagnostics/staging-pool-24h-20261028.jsonl` on the staging host.
The unit has a one-day runtime limit and a finite 17,280-sample count. It records
evidence only; it does not restart services or claim to fix the initiating stall.

Interpret a recording together with PostgreSQL's timestamped logs:

- `Lock` waits and `blockers` identify a database lock chain. An idle transaction
  can be the blocker even when it is using no CPU.
- `ClientRead` means PostgreSQL is waiting for its client. Many idle transactions
  with responsive runtime health merit checking application-held transactions,
  nested pool acquisition and singleflight waits.
- `ClientWrite` means PostgreSQL is waiting to send data to its client. Check
  result consumption and the runtime's network/executor progress.
- Slow runtime health with a fast independent database probe points toward the
  runtime or its HTTP path. `/healthz` itself performs no SQL and does not prove
  that Gonvex can acquire a database connection.
- A slow independent database probe requires checking PostgreSQL and the host.
  Its elapsed time also includes Docker execution overhead.

Do not increase connection limits based only on an acquisition timeout. In the
current implementation, requests take global admission before waiting for a
per-database connection; a busy pool can consume permits while its requests
wait. Establish the initiating wait and any nested acquisition before changing
that scheduling policy.

## Staging logging enabled during this investigation

On 2026-10-28, the staging PostgreSQL container
`gonvex-postgres-o3ii81t9yduegfooyn52pg1t` was configured with:

```sql
ALTER SYSTEM SET log_parameter_max_length = '0';
ALTER SYSTEM SET log_parameter_max_length_on_error = '0';
ALTER SYSTEM SET log_lock_waits = 'on';
ALTER SYSTEM SET log_min_duration_statement = '1000';
SELECT pg_reload_conf();
```

The duration threshold matches SQLx's existing one-second statement warning;
lock waits use PostgreSQL's existing one-second `deadlock_timeout`. Bind
parameters are suppressed. SQL statement text can still contain literals, so
treat raw server logs as private. These settings survive PostgreSQL restarts.
All were verified effective without a pending restart. Application queries,
connection limits and timeouts were not changed.

Before this change, both parameter-length settings and both logging settings
used PostgreSQL defaults. Restore that configuration, if needed, with:

```sql
ALTER SYSTEM RESET log_parameter_max_length;
ALTER SYSTEM RESET log_parameter_max_length_on_error;
ALTER SYSTEM RESET log_lock_waits;
ALTER SYSTEM RESET log_min_duration_statement;
SELECT pg_reload_conf();
```

Execute each `ALTER SYSTEM` outside a transaction. Verify `pg_settings` after
reload. Retain error groups and their history; a diagnostic change is not proof
that the original stall has been fixed.

## Verify the recorder locally

With an isolated PostgreSQL Docker container using the default `postgres` user
and database, run:

```sh
GONVEX_STALL_TEST_CONTAINER=LOCAL_TEST_CONTAINER \
  python3 scripts/postgres-stall-recorder.test.py -v
```

The integration test creates a test table, holds an exclusive table lock,
starts a blocked reader, and checks that the observer identifies both the
waiting session and idle transaction holding the lock. It releases its
connections and drops its test table afterward. CI runs this test against its
PostgreSQL service container.
