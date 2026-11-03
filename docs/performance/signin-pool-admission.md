# Sign-in admission and burst connection reuse

The realistic local load test on 2026-11-02 found a control-pool deadlock after
bounded pools shipped in #261. `session_result_from_directory` held the sign-in
transaction while `admit_member` acquired control capacity again to resolve a
tenant route. Two concurrent sign-ins could occupy both control connections and
wait for each other. Tenant admission could also exhaust shared SQL slots while
holding control admission, even with separate physical tenant databases.

Session credentials and their idempotency result now commit together before
membership admission. The stored result contains credentials and the requested
tenant, without an admitted directory snapshot. Every response, including an
idempotent replay, loads the directory, releases that transaction, and validates
current tenant-local membership. This allows retries after a directory failure
without issuing another credential family. A successful credential commit can
precede a failed response, so clients should retry with the same idempotency key.
Native code and refresh-token exchanges also release their issuance transaction
before building the tenant response.

The runtime acquisition audit covered control queries and writes, native auth,
impersonation, invitations, provisioning, scheduler, operations, storage,
telemetry, module execution and PostgreSQL transaction helpers. Two additional
nested paths needed changes:

- Invitation acceptance resolves the inviter session before opening its tenant
  transaction. The existing `FOR SHARE` member check still validates current
  authority and holds the lock through the application hook and commit. Already
  committed invitation handoffs can recover without current inviter admission.
- The support tenant overview releases its directory transaction before route
  resolution and tenant member counts.

SQL work that already owns a transaction must use that transaction or finish it
before calling a helper that acquires pool or SQL admission capacity. A different
physical database does not make nested shared admission safe.

## Idle retention

The default idle timeout is now 60 seconds, configurable through
`GONVEX_DB_IDLE_TIMEOUT_MS`. Both the default maximum and retained idle count
remain two per database. There are no minimum warm connections. The load-test
artifacts under `/mnt/data/work/gonvex-realistic-load-artifacts` reported about
6.6 new PostgreSQL connections per second at one-second expiry, versus
0.01–0.15 at 60 seconds; an idle cap of one caused about 56 per second. These are
measurements of that fixture, not throughput promises.

Retention does not change the physical ceiling: 26 databases with two query
connections and one listener each use at most 78 connections. A server limited
to 100 connections then has 22 slots for control connections if not already
included, other listeners, maintenance and other clients. Every runtime replica
requires its own allocation. `MAX_TOTAL_CONNS` bounds active SQL jobs, not retained
physical connections. Setting an idle cap below the pool maximum can close
connections on each burst even with a long timeout.

## Scheduled work and user requests

Scheduled reducers call `execute_tenant_reducer_with_access`; scheduled actions
call `execute_tenant_action_with_access`, the same execution paths used by user
requests. Both use `PoolRegistry::admit` for per-database and shared SQL slots.
Root calls also share the module host's `HostState.calls` semaphore and isolate
pool. Neither admission system gives user requests priority over scheduled work.
Module execution deadlines include time waiting for host admission.

A background job can therefore delay a user save. A safe change needs coordinated
bounded scheduling at SQL admission and module execution, using trusted invocation
provenance to distinguish scheduled work. Reserve foreground capacity while
bounding background concurrency, preserve nested-call ownership and cancellation,
and provide fairness so recurring work still progresses. A SQL-only priority
queue leaves module-host contention; a second unbudgeted execution lane could
increase overload. This requires separate design and contention tests, including
user save latency, background progress and cancellation. It is not included in
the pool fix.

## Regression checks

`auth_contract::concurrent_signins_complete_with_two_sql_slots` runs 20 concurrent
password sign-ins and idempotent replays with two control connections and two
shared slots. Control and tenant use the same pool to cover the strictest case;
each result must contain its authoritative tenant member within ten seconds.

`connection_budget::default_pools_reuse_both_connections_across_idle_bursts`
checks both PostgreSQL backend IDs over three bursts spaced 1.5 seconds apart.
The original one-second default loses those backend sessions. Existing
`pool_admission` and `connection_budget` assertions remain unchanged.

All database tests use an isolated disposable PostgreSQL on numeric loopback.
No production hosts or load-test containers are involved.
