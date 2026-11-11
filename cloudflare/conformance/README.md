# Conformance harness

The conformance harness shows that a backend behaves like the Rust runtime. It runs the same scenarios against two or more backends. It drives them with the real `@gonvex/client` SDK, the thin protocol client (`bench/whagons/thin-client.mjs`) and raw frames. Then it compares the results, normalized frame by frame, and checksums every table.

It runs Rust against Rust (two processes of the same build), or Rust against the Cloudflare kernel on local workerd (`--backend rust=rust --backend cf=cloudflare`). The kernel serves the session layer (K05) today, so only the `session-*` scenarios pass on it until the Reducer, Query and replica packages land.

```text
cloudflare/conformance/
  src/cli.mjs          entry point: builds the fixture, starts backends, runs, diffs, reports
  src/runner.mjs       runs one scenario on one backend: seeds, gives the scenario its ctx, records the trace
  src/sessions.mjs     SDK / thin / raw clients with frame capture
  src/normalize.mjs    frame normalizer (every rule is listed in RULES)
  src/differ.mjs       compares traces, prints readable diffs
  src/checksum.mjs     per-table checksums
  src/digest-check.mjs per-backend digest self-check (digests are masked in diffs)
  src/capture-golden.mjs records Rust's change log for `change-capture` as the kernel's parity golden (K03)
  src/fixture.mjs      builds the fixture bundle with the repo's own `gonvex codegen`
  src/backend.mjs      backend adapter interface
  src/backends/        rust.mjs (reference), cloudflare.mjs (kernel on local workerd), mutant.mjs (harness self-test), index.mjs (registry)
                       cloudflare-reducer.mjs (K07: the kernel with the Reducer path and the fixture bundles built in)
  scenarios/           one module per scenario, plus index.mjs (registry, run order)
  fixtures/app         test module bundle: board, notes, note_shares, errors, storage, capture (generation 1)
  fixtures/app-v2      overlay for generation 2 (upgrade scenario)
  fixtures/app-v3      overlay for generation 3: generation 1 with a client contract (session-handshake)
  test/                unit tests (no backend) and the mutation self-test (needs the Rust binary)
```

## Running

You need Node 22.18 or later. CI uses Node 24. You also need the built JavaScript packages and a `gonvex-runtime` binary:

```bash
pnpm install --frozen-lockfile
pnpm build:packages                          # protocol, module-sdk, client, cli and the rest
(cd rust && cargo build --locked -p gonvex-runtime)

cd cloudflare/conformance
node src/cli.mjs                             # every scenario, rust-a against rust-b
node src/cli.mjs --scenario presence         # one scenario (repeat the flag for more)
node src/cli.mjs --list                      # scenario names and descriptions
node --test test/*.test.mjs                  # unit tests for the normalizer and differ
node --test test/mutation.check.mjs          # self-test: injected drift must be caught
```

| Flag | Meaning |
| --- | --- |
| `--backend label=kind` | Add a backend. Repeat the flag for each one. `kind` is a registry name (`rust`) or a path to a module. The default is `rust-a=rust rust-b=rust`. The first backend is the reference, and every other backend is diffed against it. |
| `--scenario name` | Run only the named scenario. Repeat the flag for more. |
| `--json file` | Write the full report to a file: every trace, the normalized frames, observations, checksums and diffs. |
| `--work dir` | Use this directory for scratch files: data directories, runtime logs and fixture builds. |
| `--keep` | Keep the scratch directory. It is always kept when a run fails. |
| `--serial` | Run the backends one after another. By default every scenario runs on all backends at once. |
| `--verbose` | Print each step and its duration. |

The runtime binary is found in this order: `GONVEX_CONFORMANCE_RUST_BINARY`, then `$CARGO_TARGET_DIR/debug/gonvex-runtime`, then `rust/target/{debug,release}/gonvex-runtime`. `GONVEX_CONFORMANCE_TMP` sets where scratch directories go. `GONVEX_CONFORMANCE_RUST_LOG` sets `RUST_LOG`; the default is `warn`. `GONVEX_CONFORMANCE_UNTIL_MS` changes the default `ctx.until` limit of 15 s; the mutation self-test lowers it so a broken backend fails fast.

The command exits with 0 only when every scenario passed on every backend and the traces are equal. A full run of 22 scenarios on two debug-build backends takes a few minutes.

The `cloudflare` backend runs `wrangler dev` from `cloudflare/` (install its dependencies first) with the session layer's stub routes enabled; see `src/backends/cloudflare.mjs`.

### CI

The `Conformance unit tests` job in `.github/workflows/cloudflare.yml` runs `node --test test/*.test.mjs` on every PR to `cloudflare/integration`. Those tests need no backend. Rust against Rust (`node src/cli.mjs`) and the mutation self-test (`node --test test/mutation.check.mjs`) need the Rust binary and are run by hand for now. For G0, the verifier runs them.

## What is compared

Each scenario runs on every backend in a fresh project. Generation 1 of the fixture is deployed through the adapter, which uses `POST /dev/sync` on Rust. Then the scenario's tenants and Members are seeded. Each scenario's trace has four parts:

- **Steps.** Each step records the error it threw, if any. A step error or a failed `ctx.expect` fails that backend's run, even when the traces agree.
- **Observations.** These are values the scenario chose to record: Reducer results, error classes, collection rows and presence maps.
- **Frames.** Every frame each session sent and received, normalized and grouped by step. Within a step, frames are compared as a multiset per session. Ordering between sessions, and between frames that race, is not part of the contract.
- **Digests (per backend).** The normalizer masks digests, because they cover values that differ between runs. So each backend's raw frames are checked on their own, before normalization. For every collection subscription, `src/digest-check.mjs` keeps the row hashes the client holds: from snapshots, from deltas, and from the `keys`/`hashes` a client claims when it resumes. It then recomputes every digest in `replica.ready` and `replica.delta` with the SDK's algorithm (`xor1` aggregate or the legacy pair hash, from `packages/client/src/replica-integrity.ts`). A mismatch fails that backend's run. The report prints how many digests were verified on each backend.
- **Table checksums.** For every seeded tenant, the harness reads all rows of `board`, `notes` and `note_shares` through `readTable`. It masks them, sorts them, and hashes them with sha256 over canonical JSON lines. When a checksum differs, the diff shows the differing rows.

### Normalization

The rules live in `RULES` in `src/normalize.mjs`, and each is unit-tested in `test/normalize.test.mjs`. They cover only what the contract leaves free:

| Rule | Why |
| --- | --- |
| Backend ids become aliases: `<project>`, `<tenant:a>`, `<account:alice>`, `<member:alice@a>`, `<access-token>`. | They are assigned by each backend. |
| Client-minted ids (`id`, `idempotencyKey`, `originCommandId`, UUIDs) become `<id#n>` or `<uuid#n>`, numbered by first appearance in the session and keyed by value. | The same id stays linked across frames. |
| Server-minted race tokens (`snapshotId`, `windowRevision`, `stream`) become `<snapshotId>` and similar, unnumbered. | Two Live Query windows changing in one commit may be answered in either order. |
| Opaque scopes and epochs become `<scope#n>` and `<epoch#n>`. | They are hashes or random values. |
| Revisions become ranks `r1, r2, …`. Live Query `sequence` values get their own ranks, `s1, …`. In observations they become `<revision>`. Ranks are built from exactly-compared frames only; a watermark keeps the rank of an equal compared revision, else shows `<barrier>`. | Order is contract. Absolute numbers depend on provisioning. Watermarks are compared only as sent or not, and Rust sends or coalesces quiet-tenant watermarks on a timer, so their values must not renumber the session (0022 §6). |
| Clocks (`*At`, `*Ms`, `since`, `serverTime`) become `<time>`, as numbers and as RFC 3339 strings (Control Plane timestamps such as `expiresAt`). `trace` and `device` are dropped. Durations inside error text (`1999 ms`) become `<n> ms`. | They vary from run to run. |
| JWTs (`eyJ….eyJ….sig`) become `<jwt>`. | They are ID tokens an identity provider (the Firebase emulator) mints per run with clocks inside; a scenario sends them as `exchangeExternalToken` arguments. |
| In error text, JavaScript stack frame lines (`\n    at …`) are stripped. | The message, which is the first line and includes the host wording such as `host call failed:`, is contract and compared exactly. Stack positions are not contract (ruling 0015). |
| Digests and row hashes become `<digest>`. | They cover values that differ between runs. The digest self-check verifies each one on its own backend instead. |
| `session.ready` loses `replicaNativeRows`, `replicaSetDigest` and `replicaFieldPatches`. | Rust advertises them to every socket, but the shipped SDK never uses them and the kernel does not advertise them (contract-changes 0012 R20). |
| `replica.batch`, `replica.readyMany`, `query.batch`, `query.fanout` and `reducer.callMany` are expanded into the frames they stand for. | Batching is a transport choice. |
| Snapshot pages are merged into one `replica.snapshot` with rows sorted by key. ACKs are dropped. | Page size and credits are tuning. |
| The `opens` of a batch are sorted by path and args. | The SDK orders them by random subscription id. |
| `replica.needHashes`, followed by the client's integrity reopen and a reply that moves no row, is dropped. Episodes that move rows are kept. | Rust asks for hashes depending on timing. |
| In an integrity reopen that answers a `replica.needHashes` (same id, same socket), `cursor.revision` becomes `<persisted-watermark>`, before revisions are ranked. Integrity opens a client sends on its own keep their cursor. | The client resumes from the cursor it persisted, which includes the last watermark it applied, and Rust sends quiet-tenant watermarks on a timer. In `thin-sdk-parity` the r6 watermark lands before or after the `needHashes` depending on timing (0022 §6). |
| `replica.watermark`, `control.watermark`, `query.progress` and `replica.syncing` are compared over the whole scenario per session, as sent or not sent, not per step. Watermarks are compared only for sessions that subscribed to something. | These are coalescible barriers. Rust also sends quiet-tenant watermarks on a timer. |
| Telemetry and error-reporting frames (`telemetry.*`, `error.register`, `error.envelope`, `error.heartbeat`, `error.ack`) are ignored. | The SDK sends them on timers, so their sequence is not comparable. `CONTRACT.md` A.2 marks the `error.*` frames **keep**, so they still need coverage: **K15 owns a dedicated scenario for them**, with its own client that sends each frame and asserts its `error.ack`. |

Error **text**: following ruling 0015, the message is compared exactly, host wording included. Only stack frame lines and the masks above are removed.

## Scenarios

| Scenario | Covers | From |
| --- | --- | --- |
| `thin-sdk-parity` | One Member through the SDK and through the thin client, kept equal in rows and in frames sent: creates, edits, share and revoke (row gain and loss through visibility sets), a collection released (`replica.close` after retention), the Members' own Reducers, a network drop and edits during recovery | `bench/whagons/thin-conformance.ts` |
| `reducer-second-client` | A Reducer commit (insert, update, delete) reaching a second client's Replica Collection, through the SDK and the thin client | |
| `replica-resume` | Open and snapshot; resume with cursor, keys and hashes after missed commits; resume with nothing changed; foreign epoch; wrong digest; SDK reconnect with commits during the outage | |
| `reducer-errors` | `rejected` for a throw (the earlier write rolls back), bad args, a bad result, an unknown path and the wrong kind; `transient` for the deadline; `update_required` for a stale artifact; `unauthenticated` for a control-only session | |
| `presence` | Snapshot, active / idle / offline patches, a socket drop, two sockets for one Member, control-only refusal; SDK and thin client | |
| `live-query-window` | Window initial result, a row entering and pushing one out, updates outside and inside the window, a delete inside, moving the window, the limit cap, unsubscribe | |
| `control-auth` | Public settings, login failures, login, `accounts.me`, tenant auth in two tenants, refresh rotation and reuse of a rotated token, logout, invalid and unknown tokens, tenants and projects | |
| `control-sessions` | The `auth` frame's branches (control-only, default tenant, project mismatch, non-member), login replay before the password check, key reuse across paths, refresh reuse after the 5 s grace revoking the family, logout of every session, password change | C.0, C.3.6-C.3.9 |
| `control-tenants` | Live `tenants.mine` refreshed before the Reducer result, `tenants.create` domain rules, replay and conflicts, `getByDomain`, profile and timezone, member login provisioning, tenant-admin and project-admin refusals, seat limits and exceptions, soft delete | C.1 rows 4, 13-20, 58 |
| `control-invitations` | Validation, a live list refreshed from another socket, lookup and the legacy lookup, update, revoke, provider policy, wrong account, acceptance with replay and a stale key, a payload without an acceptance Reducer, member providers, member password reset revoking sessions | C.1 rows 3, 12, 21-26, 59-60 |
| `control-support` | Support sessions and commands, the tenant overview, support impersonation (single use, rotating reconnect credential), developer mode, self-provisioned developer membership, agent claim tokens, assistant and voice settings, project developers, demo accounts | C.1 rows 27-57 |
| `control-external-auth` | Realm configuration and validation, `exchangeExternalToken` with the Firebase Auth Emulator (new account, returning identity, verified-email linking, invite-only signup, `previousRefreshToken`), token and provider failures, a Firebase-only project refusing passwords. Needs `GONVEX_CONFORMANCE_FIREBASE_EMULATOR`; skipped otherwise | C.1 rows 6, 9-11; C.3.4 |
| `control-native-auth` | The native OAuth HTTP routes: `/auth/config`, authorize with PKCE and redirect registration, the callback's state handling, `/auth/token` with both grants, `/auth/logout`, CORS | C.3.5 |
| `receipt-lost-reply` | A keyed call whose reply is lost; replay returns the stored result once; receipt query (found, unknown key, other path, missing args); same key with other args; a replayed rejection | `backend-receipts` lost_reply |
| `receipt-crash` | A client process killed with SIGKILL after sending a keyed call; the backend killed after a commit (receipt, rows and SDK recovery intact) and right after a keyed send (exactly-once replay) | `sdk_process_crash`, `check_restart` |
| `receipt-upgrade` | `/dev/sync` of generation 2 while connected: migration default for old rows, the new Reducer, a call pinned to the old artifact (`update_required`), generation 1 receipts still answering, SDKs on the v1 and v2 definitions | `sdk_module_upgrade`, `check_module_upgrade` |
| `receipt-owner-switch` | One SDK socket switching account A to B to A (owner-scoped notes, receipts per owner, a foreign write refused) and tenant A to B to A (no leak in either direction) | `sdk_owner_switch` |
| `storage-sql` | Module SQL through Reducer results: `$N` placeholders by number, integer and JSON parameters, decoding by declared type (JSON, BOOLEAN, BLOB, expressions, `RETURNING`), refused statements (several statements, ATTACH, PRAGMA, VACUUM, BEGIN), module savepoints and a caught batch failure, a caught SQL error that must not commit, a deferred foreign key failing at commit while the backend keeps serving, `existsDataRows` over 100 bound parameters, the `matches` predicate (REGEXP) | K01 storage layer |
| `session-handshake` | `session.ready` first; every `auth` branch and error code; the replica directive's `scope` and `visibilityScope` recomputed from the result; re-auth in place with and without `subscriptionsPreserved`; a tenant switch; a failed re-auth tearing the session down (the unauthenticated reply of every frame kind); `clientContract` gating at auth and on a deploy while connected (`client.updateRequired`, `system.reload`); the real SDK logging in, authenticating and switching tenant | K05 |
| `session-frames` | The decoder's accept/close boundary (82 vectors: integer widths and lexemes, `null`, duplicates, the sequence form, nesting depth, lone surrogates); binary frames; the public transport (mismatch, non-public tenant, the 64 KiB limit); 64 KiB on unscoped sockets and 16 MiB after auth; the 10 s pending-auth deadline | K05 |
| `change-capture` | 18 Reducer transactions through `capture.apply`, then the tenant's change log read through the adapter (`readChangeLog`, capability `changeLog`): change rows, ordinals, command ids, revisions (relative), provenance and the clock. Covers every column kind and edge values, no-op updates, savepoints, trigger-written rows, a composite `WITHOUT ROWID` key, a derived table, a logical record with an owned table and a record dependency, a rejected Reducer. `node src/capture-golden.mjs` writes Rust's result to `kernel/capture/test/golden/`, which the kernel's capture is replayed against in workerd | K03 |
| `visibility-masks` | Masks v2 through two Replica Collections for three Members (SDK and thin): a principal set over two permission tables (team join, workspace link), a grant set (assignment and revocation), a creator context, and notes that `inherits` their task with `onMissing` for orphans (a task moving out of reach takes its notes; a deleted task leaves its notes to their author). Tables `mask_*` in the fixture | K09 |
| `receipt-expiry` | Intended behavior IB3 (0001, 0009), kernel only (`receiptExpiry`): a keyed call replays inside the offline window + 24 h; after it the result expires and a late duplicate gets `Reducer receipt expired` (rejected), as does the receipt query; 365 days later the tombstone is gone and the key runs again. Rust never prunes receipts, so it skips this scenario | K07 |
| `scheduler` | One Reducer schedules `runAfter(0)` and `runAt(+300 ms)` of `scheduler.tick` and three identical `wakeAt` calls that must coalesce into one run, and a Reducer reschedules itself with `runAfter(0)` until the durable hop limit (32) stops it; after a fixed settle time (`GONVEX_CONFORMANCE_SCHEDULER_SETTLE_MS`, default 5 s) the `board` rows must show two ticks, one wake and 32 hops. The kernel's scheduler rules are unit-tested in `kernel/scheduler/test/` | K11 |

Two receipt checks stay in `verification/backend-receipts/`, because they inject faults inside the Rust process through test-only coordinators that a black-box harness cannot reach: the SDK's own SQLite outbox replay, and the injected replica and receipt write failures.

## Adding a scenario

1. Create `scenarios/<name>.mjs`:

   ```js
   import { watch, call, rowsWhen, project, rawReducer, RAW_CAPABILITIES } from './helpers.mjs';

   export default {
     name: 'my-scenario',                       // unique; used in tenant ids and --scenario
     description: 'One line for --list',
     tenants: { a: ['alice', 'bob'] },          // seeded per backend; Members get a password login
     requires: [],                              // backend capabilities, e.g. ['crash']; missing -> skipped
     tables: ['board', 'notes', 'note_shares'], // optional: tables to checksum (this is the default)
     async run(ctx) {
       let alice, view;
       await ctx.step('alice watches the board', async () => {
         alice = await ctx.sdk('alice', { member: 'alice' });      // real SDK, logged in and authenticated
         view = watch(alice, ctx.api.board.all);
         await rowsWhen(ctx, view, rows => rows.length === 0, 'empty board');
       });
       await ctx.step('bob adds a row', async () => {
         const bob = await ctx.thin('bob', { member: 'bob' });      // thin protocol client
         ctx.observe('add', await call(bob, ctx.api.board.add, { id: 'x', title: 'x', position: 1 }));
         ctx.observe('rows', project(await rowsWhen(ctx, view, r => r.length === 1, 'row arrives'), ['id', 'title']));
       });
     },
   };
   ```

2. Add it to `scenarios/index.mjs`. Scenarios that restart the backend go last.
3. Run it with `node src/cli.mjs --scenario my-scenario --verbose`. Then run the whole suite three times. The scenario must pass Rust against Rust every time.

**The `ctx` API**

| Member | What it does |
| --- | --- |
| `ctx.sdk(tag, { member, tenant, options, authenticate, fresh })` | A `GonvexClient`. A Member's first client logs in with `control.auth.passwordLogin` on its own socket. Later clients reuse that session, unless `fresh` is set. `options` go to the SDK constructor. |
| `ctx.thin(tag, { member, tenant, options, fresh })` | A `ThinClient`. Pass `options: { presenceV2: true }` for presence. |
| `ctx.raw(tag, { public })` and `ctx.rawLogin(socket, member)` | A `RawSocket` with `send(frame)`, `sendRaw(textOrBytes)`, `next(match)`, `auth({ project, tenant, token, capabilities })`, `whenClosed(ms)`, `closeEvent` (`{ code, reason, at }`), `close()` and `drop()`. `public: true` connects with `?public=1`. |
| `ctx.step(name, fn)` | Groups frames and observations. A throw is recorded, and the next step still runs. |
| `ctx.observe(label, value)` | Records a value that every backend must reproduce. |
| `ctx.expect(cond, msg)` | Asserts a fact that must hold on every backend. |
| `ctx.until(check, label, ms)` | Polls until `check` returns true. The default limit is 15 s, or `GONVEX_CONFORMANCE_UNTIL_MS`. |
| `ctx.quiesce()` | Waits until no frame has arrived for 300 ms. |
| `ctx.frames(tag, { dir, type })` | The raw frames of a session. |
| `ctx.deploy(n)` | Deploys generation `n` of the fixture (2: upgrade, 3: client contract). |
| `ctx.restart({ crash })` | Restarts the backend. `crash: true` kills it without shutdown. |
| `ctx.api` and `ctx.fixtures[n].api` | Generated function references. |
| `ctx.alias(value, name)` | Registers a server-minted id (a created tenant, invitation, account, grant) as `<name>`, as seeded ids are, so it compares across backends. |
| `ctx.members.alice.memberIds.a` | The id of Member alice in tenant a. |
| `ctx.tenants.a` | The tenant id. |
| `ctx.project` | The project id. |
| `ctx.control` | The `@gonvex/client` control references. |

**Determinism rules.** Every rule below came from a real flake.

- Choose every id in the scenario: row ids, idempotency keys, tenant names. Never use `Date.now()` or random values in arguments.
- Wait inside a step for everything that step causes. If a step drops a socket or restarts a backend, it must wait for the reconnect (`connectionState().connectionCount`, or a new `auth` frame). Otherwise the reconnect's frames land in whichever step is running.
- Never send two things whose order the backend may choose freely and then compare the result. For example, `presence.set` and `presence.subscribe` from different sockets at the same moment. Sequence them, or observe only the end state.
- Record values with `ctx.observe`, not `console.log`. Use `ctx.expect` for facts that must hold on every backend, and for anything the diff alone would miss when both backends agree on a wrong answer.
- Do not weaken a scenario or an assertion to make a backend pass. If the reference itself is nondeterministic, add a normalization rule in `RULES`, with a unit test and a line in the table above, and say why in the PR.

To extend the fixture, edit `fixtures/app`, and `fixtures/app-v2` for the upgrade. Codegen only accepts static `schema.*` literals inside `reducer(...)` and similar calls, so do not factor schemas or options into constants. Keep clock columns out of the tables, so checksums stay exact.

## Plugging in a backend

1. Subclass `Backend` from `src/backend.mjs` and implement these methods:
   - `start()` and `stop()`. Each start begins with empty state. `start()` must set `this.wsUrl`.
   - `createProject(name)`, which returns `{ projectId }`.
   - `deploy(projectId, manifest)`, which takes the CLI manifest exactly as `/dev/sync` receives it. The adapter sets `manifest.project`.
   - `createTenant(projectId, tenantId, name)`. It is called after the first deploy.
   - `createMember(projectId, tenantId, { email, password, name })`. It creates or reuses an account with a password login and makes it an active Member. It returns `{ accountId, memberId }`.
   - `readTable(projectId, tenantId, table)`, which returns every row as a plain object.
   - Optional: `restart({ crash })`, with `capabilities` returning `{ restart: true, crash: true }`. `wsUrl` must stay the same across a restart, because connected clients reconnect to it.
   - Optional, for the control-plane scenarios: `createAccount(projectId, { email, password, name })` (a password login with no membership), `addProjectMember(projectId, { email, name, role })` (a project developer), `configureGoogleAuth(projectId, { redirectUri })` (native Google OAuth with a registered app redirect).
   - Optional: `readChangeLog(projectId, tenantId)`, with `capabilities.changeLog`, returning the tenant's `_gonvex_sync_clock` row and every `_gonvex_sync_changes` and `_gonvex_sync_transactions` row (`change-capture`). Rust reads the tenant file directly: its `/dev/data` routes refuse `_gonvex_*` tables.
2. Register it in `src/backends/index.mjs`, or pass a module path: `--backend cf=path/to/backend.mjs`, where the default export is the class.
3. Run `node src/cli.mjs --backend rust=rust --backend cf=cloudflare`. Rust is the reference. A diff is printed per step and per session, and the report is written to `--json`.

Seeding goes through the adapter, because the routes that create test projects and password Members are not part of the contract. The session token always comes from `control.auth.passwordLogin` over the socket, because that is contract. `src/backends/mutant.mjs` is a worked example: Rust behind a proxy that rewrites frames, used by `test/mutation.check.mjs` to prove the differ catches drift.

## Control plane (package K06)

The `cloudflare-control` backend runs `cloudflare/control` in local workerd (`wrangler dev`): the
control object, a stub tenant object and a minimal control socket owner. It serves `auth` and
`scope:"control"` frames and the native OAuth routes; tenant-scope frames are refused, so run only the
control scenarios against it. Admin work in the scenarios runs on control-only sockets with
`capabilities.tenantControl=1`, as the dashboard does, so no tenant change feed is involved.

```bash
# Firebase Auth Emulator (Java 11+), for control-external-auth
npx -y firebase-tools@15.35.0 emulators:start --only auth --project demo-gonvex-k06
# Rust with the emulator decoder (docs/firebase-auth-emulator.md)
(cd rust && cargo build --locked -p gonvex-runtime --features test-firebase-auth-emulator)

export GONVEX_CONFORMANCE_FIREBASE_EMULATOR=127.0.0.1:9099
node src/cli.mjs --backend rust=rust --backend cf=cloudflare-control \
  --scenario control-auth --scenario control-sessions --scenario control-tenants \
  --scenario control-invitations --scenario control-support --scenario control-external-auth \
  --scenario control-native-auth
```

Both adapters give their backend `GONVEX_AUTH_URL` (its own base URL) and the fixed provider
secret and Google fallback client of `CONFORMANCE_SECRETS` (`src/backends/rust.mjs`). With
`GONVEX_CONFORMANCE_FIREBASE_EMULATOR` set they also enable the emulator decoder
(`GONVEX_TEST_AUTH_EMULATOR`, `GONVEX_ENV=test`, `FIREBASE_AUTH_EMULATOR_HOST`). The two backends share
one emulator, so `control-external-auth` never changes a Firebase user's verification: each state has
its own user.

## Reference behaviours and known differences

The lead has accepted the following as Rust contract, as observed against Rust at `0bf3ad13`:

1. **Same idempotency key, different arguments.** Rust returns the stored result of the first call without error (`receipt-lost-reply`, step 5).
2. **Stale `clientContract: 0`.** A Reducer call with a lower contract is accepted (`reducer-errors`, step 7). Only a stale `artifactHash` gives `update_required`.
3. **Tenant ids are unique per runtime, not per project.** `gonvex_runtime_tenants.relationship_id` is the primary key, so the harness names tenants `<scenario>-<name>`.
4. **Rust nondeterminism.** Quiet-tenant watermarks, no-op `needHashes` episodes and Live Query arrival order are absorbed by the normalization rules above.
5. **`ControlChanged` echo.** Rust sends every `ControlChanged` locally and again through its database listener, so a live Control Query can refresh a second time shortly after a write. That only shows state that changed without a notification, such as the claim a failed `invitations.accept` leaves. The control object notifies once per write (contract-changes 0025). `control-invitations` reads the list on demand around failed acceptances instead of racing the echo.

Other notes:

- **Presence offline after a socket drop takes about 10 s on Rust.** The `presence` step "the last socket drops" waits for it. The kernel can be faster without failing the scenario.
- **Legacy visibility manifest.** The fixture's `visibility()` plans make Rust log a deprecation warning for v1 masks. Moving to v2 masks is a fixture change if the contract freezes on v2.
- **Node 22 WebSocket.** Node 22.23 fires `error` but no `close` on a refused connection and leaves `readyState` at CONNECTING. Node 26 and browsers follow the spec. Both SDKs reconnect from `close`, so `src/sessions.mjs` completes the spec's failure steps. Without this, any client retrying while a backend restarts stalls for 30 s.
- **Stream bindings outlive a re-auth.** When one socket re-authenticates without `subscriptionsPreserved`, for example on a tenant switch, Rust drops every subscription. But the delivery v2 stream token can be the same in the new auth, and `expandReplicaBatch` (used by the SDK, the thin client and this harness) keeps a socket's bindings across auths. So Rust's next `replica.delta` and `replica.ready` for that stream also expand to the dropped subscription id (`receipt-owner-switch`, step "tenant switch A -> B"). The SDK ignores the unknown id, and the digest self-check skips subscriptions dropped by a re-auth. Those phantom frames are still in the normalized trace, so a kernel that unbinds or renames its streams at auth shows a diff there. It needs a ruling: either the client clears its bindings at `auth.result`, or the server must unbind.
- **The SDK alone reacts to a wrong digest.** It reopens the collection again and again (about 80,000 frames in one scenario) and the run eventually fails on timeouts. The digest self-check fails it at the first mismatch and names it. It also covers the thin and raw clients, which never verify digests.
