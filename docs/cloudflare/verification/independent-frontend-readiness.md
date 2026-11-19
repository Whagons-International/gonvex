# Independent verification: frontend readiness

Date: 2026-11-19 (UTC), run 2026-11-18 23:55 – 2026-11-19 00:40Z.
Claim checked: `docs/cloudflare/frontend-readiness.md` at `1d40b87a`: 6/6 criteria PASS, 80/90 feature cells PASS plus 10 "matches staging".
The verifier did not write any of the work being checked. No backend code was changed. Nothing was written to staging or production. Staging was only read: `docker inspect` and `docker save` of the runtime image, plus `GET /healthz`.

## Verdict: NOT VERIFIED

- **Create latency does not match the Rust reference.** The CF realtime create cells were passed as "matches staging". Against the same unmodified frontend on the Rust runtime that staging runs, CF is about 0.5 s slower end to end on warm creates and about 1.1 s slower on cold creates. The gap is 0.4 s and 1.0 s after allowing for the network. Both are over the +300 ms tolerance. The backend part alone, (b) pre-commit, is 2.1× Rust warm and 3.6× Rust cold.
- **About 55 of the 80 PASS cells have evidence that is missing, stale, or does not support the claim.** At least 18 should go back to FAIL.
- **SpotsMapView cold-load crash: confirmed client bug.** It reproduces on Rust, so it matches staging.
- **1006 close at socket age 32 min: not a backend bug.** The server never sent a close. The drop happened between the browser and the Cloudflare edge, and the client recovered cleanly. It is not a fixed socket-lifetime limit.

Criteria that should go back to FAIL: **3** (feature matrix) and **6** (matrix with evidence for every result). Criteria 1, 2, 4 and 5 were not re-audited in depth. Nothing found here contradicts them, except that criterion 4's evidence is from backend 8a568028/01eaeceb, not the current 34562a4a.

## Task 2: Rust baseline for create latency

### Setup

Rust reference, as close to staging as possible:
- **Image:** the exact image the staging runtime container runs (`runtime-3vb4f3kj…`, `GONVEX_RUNTIME_VERSION=0bf3ad13419d25d9370607e1470ae58da4c43235`). It was copied with a read-only `docker save` from the staging host.
  - Note: the image's OCI label `org.opencontainers.image.revision` says `84bb4faf`, while the env says `0bf3ad13`. The env value is the one the runtime reports.
- **Environment:** run locally on arch3 with staging's non-secret runtime settings: presence v2, module host and sandbox on, 4 Tokio workers, SQLite cache 1 GiB, rerun concurrency 64, `GONVEX_ENVIRONMENT=production`, `GONVEX_REQUIRE_AUTH=true`.
- **Data:** a reflink copy of the read-only staging snapshot `staging-all-20261117` (control.db plus all 15 tenant DBs, captured 2026-11-17 21:27–21:30Z). That is the same source P03 imported into `devcopy-stg1117-NN`. The app module is the staging manifest in that snapshot (1,997 functions, deployed 2026-11-17 21:17Z).
- **Sanitized before start:**
  - Every project env var removed except `OPENROUTER_API_KEY`, which was set from the `openrouter` vault credential through `whagons-dev credentials exec` and never printed. The CF dev `set-env-result.json` does not record which vault entry it used, so credential identity with dev is assumed, not proven.
  - All 215 pending scheduled jobs deleted.
  - Firebase admin and OAuth client secrets nulled. ID-token checks only need Google's public JWKS.
  - No S3, Telegram or Resend credentials.
  - Recreated crons failed on the missing env, as intended. Nothing could email, push or write to R2.
- **Logins:** the same two vault accounts, `staging-smoke-admin` (A) and `staging-smoke-test` (B), mirroring `membership/CHANGES.md` for -11:
  - Added with the runtime's `/dev/internal/e2e/members`.
  - All 3 live teams added through `/dev/data/tables/memberTeams/rows`.
  - A got role `owner` with the -11 owner's exact `members.permissions` (48 keys, the same count CF dev used).
  - B got role `member` with the most common member grant set.
- **Frontend:** the same staging `678bdc80` source tree (reflink copy of the tree used for `dist-dev-custom`), production `pnpm build`. Only `VITE_GONVEX_URL`/`VITE_GONVEX_WS_URL` (`http://localhost:48180`), `VITE_BASE_DOMAIN=localhost` and an empty `VITE_ASSET_ORIGIN` differ from staging. The project ID is staging's own.
- **Harness:** the walkthrough's own controllers and `ev16/x13g.js`, unchanged apart from ports and paths: 150 s idle, then 1 cold + 3 warm creates, two separate browsers, synthetic tasks archived (`remaining: 0` every round).
- **Fresh CF rounds:** to control for OpenRouter drift, two new CF rounds on `devcopy-stg1117-11` were interleaved with the Rust rounds. They used the same harness copy against the existing `dist-dev-custom` preview, under `/home/gabriel/work/gonvex-dev-lane.lock` (00:11:45–00:18:05Z and 00:23:57–00:30:10Z). Backend before and after both rounds: kernel e785f303, k-actions/k-runtime/k-builder 34562a4a, sandbox cc317f86, runtimeIdentity 8918a3da23ef09a2. This is the same deployment as `run-x13r3.out`.

Definitions are the walkthrough's:
- (a) = `assistant.rewriteText` call → result frame on A.
- (b) = submit→`tasks.create` result, minus (a).
- (c.frame) = commit → `replica.batch` arrival on B.
- (c.vis) = frame → row visible in B's grid.
- e2e = submit → row visible on B.

### Results (ms)

| Run | Kind | (a) rewrite | (b) pre-commit | (c) frame | (c) render | e2e submit→B visible |
|---|---|---:|---:|---:|---:|---:|
| CF -11 x13r3 22:27Z | cold | 2610 | 777 | 39 | 319 | 3745 |
| | warm | 1521 / 1420 / 1930 | 377 / 442 / 394 | 32 / 31 / 24 | 332 / 273 / 268 | 2262 / 2166 / 2616 |
| CF -04 x13r3 22:33Z | cold | 2424 | 900 | 22 | 346 | 3692 |
| | warm | 1827 / 1675 / 1752 | 449 / 300 / 352 | 15 / 10 / −40 | 338 / 304 / 353 | 2629 / 2289 / 2417 |
| CF -11 r1 00:16Z (this audit) | cold | 2544 | 584 | −43 | 280 | 3365 |
| | warm | 1465 / 2338 / 2291 | 346 / 320 / 236 | 49 / 47 / 41 | 314 / 286 / 257 | 2174 / 2991 / 2825 |
| CF -11 r2 00:28Z (this audit) | cold | 2695 | 304 | 19 | 443 | 3461 |
| | warm | 1864 / 2081 / 1838 | 350 / 324 / 182 | −12 / 24 / 30 | 311 / 342 / 319 | 2513 / 2771 / 2369 |
| **Rust -11 r1 00:09Z** | cold | 1511 | 159 | 1 | 258 | 1929 |
| | warm | 1604 / 1010 / 1194 | 130 / 109 / 159 | 3 / 0 / 1 | 247 / 241 / 260 | 1984 / 1360 / 1614 |
| **Rust -11 r2 00:22Z** | cold | 1798 | 257 | 0 | 410 | 2465 |
| | warm | 1472 / 1529 / 2400 | 163 / 183 / 201 | 1 / 1 / 2 | 317 / 298 / 334 | 1953 / 2011 / 2937 |
| **Rust -11 r3 00:34Z** | cold | 2230 | 189 | 3 | 311 | 2733 |
| | warm | 1420 / 1676 / 1386 | 190 / 183 / 143 | 1 / −3 / 4 | 301 / 268 / 274 | 1912 / 2124 / 1807 |

Negative frame values come from clock offset between the two browser processes. They are kept as recorded.

Medians (min–max):

| | n | (a) | (b) | (c) frame | (c) render | e2e |
|---|---:|---:|---:|---:|---:|---:|
| CF warm | 12 | 1832 (1420–2338) | 348 (182–449) | 27 | 312 | **2465 (2166–2991)** |
| Rust warm | 9 | 1472 (1010–2400) | 163 (109–201) | 1 | 274 | **1953 (1360–2937)** |
| CF cold | 4 | 2577 (2424–2695) | 680 (304–900) | 20 | 332 | **3576 (3365–3745)** |
| Rust cold | 3 | 1798 (1511–2230) | 189 (159–257) | 1 | 311 | **2465 (1929–2733)** |

### Network allowance

The local Rust runtime has no network hops. Real staging (`gonvex-stg.whagons.com`) and CF dev are both behind Cloudflare. From arch3, TCP connect time was 59 ms to staging and 54 ms to CF dev (5 samples each).

Submit→B visible crosses about 2 RTT: the rewrite round trip, the create request, and delivery to B. So the staging-equivalent Rust figure is about +110 ms: **warm ≈ 2.06 s, cold ≈ 2.58 s**.

### Verdict

**CF does not match Rust within +300 ms.** Warm e2e is +512 ms raw and about +400 ms after the network allowance. Cold is +1.1 s raw and about +1.0 s adjusted. Rust is materially faster.

Where the warm gap is (medians):
- **(b) pre-commit, about +185 ms** (+130 ms after one RTT). This is the backend that was credited as 310–473 ms. Rust does the same work in 109–201 ms. On a cold create the gap is about +490 ms: CF 304–900 ms against Rust 159–257 ms.
- **(a) rewrite, about +360 ms** (+300 ms after one RTT). The rewrite is not pure OpenRouter time: on both backends it is a backend Action. CF Workers Logs for the `-actions` Worker show the split (event timestamps are event end times; evidence `t4/act.py` output below):
  - In **x13r3**, the warm `ActionWorker` invocations used 7–14 ms CPU, with wall time 1417–1910 ms, almost all of (a).
  - In this audit's **two CF rounds**, every warm `ActionWorker` invocation used **238–297 ms CPU** (8/8), with wall time 1244–2099 ms. That is the cold-isolate cost, paid again on "warm" calls. Cold calls used 241–403 ms CPU.
  - So whether the Action isolate is reused varies between runs, and when it is not, each rewrite pays about 0.25 s of CPU that Rust's warm module host does not.
  - The rest of the (a) gap is OpenRouter variance: Rust warm (a) also ranges from 1.0 to 2.4 s.
- **(c) delivery: CF 10–49 ms against Rust about 1 ms.** That is the one-way network hop, so it is network, not backend.
- **Render: CF 312 ms against Rust 274 ms.** This is client-side and within noise.

The "same ordering as Rust" statement in the realtime cell is true. The client awaits the rewrite before it sends `tasks.create` on both backends. But "matches staging" is not: CF adds about 0.4 s warm and about 1 s cold on top of that ordering.

Neither backend meets the matrix's literal requirement, "Second-client update within 1 second", for task create. Rust warm e2e is 1.36–2.94 s. On parity terms, CF is still clearly slower.

## Task 1: matrix audit

This audit read all 90 cells, the walkthrough REPORT, ev3–ev16, `identity/`, `membership/CHANGES.md`, `cells.py` and `staging-proofs.json`.

### Structural findings

- `REPORT.md`, which nearly every cell links, still has the b7 header and row table ("47/90", afecea5e). It has written sections only for b16–b19. **Batches b8–b15 have no written report.**
- Many cell numbers exist only in `cells.py`, the script that generates the cell text, not in any raw result file. Examples: `forms.submitTaskForm 7113`, `2690`, `2700`, `2911`, `tasks.bulkReschedule 2900`, `members.saveProfileAndTeams 7071/7072`, `6991/6992`, presence "11.3 s / 9.3 s", "-12 0.52/0.43 s".
  - The ev11–ev14 folders hold screenshots and `*.log.json` files that only record errors (they are empty).
  - Structured results exist only for b6 (`ev4/run-*`, `rows-*`), b7 (`ev5/run-T12`), b9 (`ev9/run-T12`), b10 (`ev10/run-*`), b12 (`ev12/rows-T12`) and b18/b19 (`ev16/rows-T12-D`, `preview-T12`, `x13-*`).
- **Only one cell set has evidence on the current backend 34562a4a: realtime -11/-04.** Every other cell's evidence predates the Action-path changes 944d8298 (inline Actions), 24089a09 (X13c) and b00013d9/34562a4a (X13d Action-only bundle). That includes all rows that use Actions other than create/rewrite: files (`generateUploadUrl`, `getFileUrls`), API keys, AI and automations.
- The 10 "matches staging (data)" cells are supported by `/home/gabriel/work/gonvex-c3-walkthrough-20261117/staging-proofs.json` (written 2026-11-18 05:29Z). Its counts agree with all 10 claims, but the doc links none of them to it, and the file records no snapshot ID or query. One mismatch: boards -12 says "0 board messages", which is the dev copy's count. Staging shows the boards plugin disabled, consistent with N/A.

Spot-checked directly by the verifier:
- `hasAssigned:false` in `ev4/run-T11.json`, `ev10/run-T11.json` and `ev12/rows-T12.json`.
- tasks-edit second client 6,605 ms in `ev10/run-T11.json`.
- `ev14/form-T12f.png` shows the form at 14/34, not submitted, with a required field outlined in red.
- `ev12/notifications-B12-*.png` shows the grid with no notification.
- `ev16/nav16-T12-0.json` was written 08:26Z (b17), not b18.
- The b11–b14 commit numbers appear only in `cells.py`.

### Cells that should go back to FAIL (missing or unsupported evidence)

| Row | Tenant(s) | Reason |
|---|---|---|
| realtime | -12, -11, -04 | -12 has no measurement on 34562a4a, and b18 -12 second-client times were 7.9–11.2 s (`ev16/rows-T12-D.json`, `preview-T12.json`). -11/-04: e2e is 2.2–3.0 s warm, and CF is about +0.4 s warm and +1.0 s cold over the Rust reference (Task 2). The cell's own b19 note still says "stays FAIL". |
| tasks-status | -12 | `bVisibleMs` is null in `ev9/run-T12` and `ev12/rows-T12`, and the `ev5` run errored. "0.52/0.43 s" appears only in `cells.py`. |
| tasks-assign | all 3 | "Assignee notified": `hasAssigned:false` in every run file. |
| notifications | all 3 | `hasAssigned:false` everywhere. The `ev12` screenshot shows no notification. |
| tasks-bulk | -12, -04 | -12 reschedule `nopick` (`ev5`), run errored (`ev9`). -04 errored in `ev4`/`ev10`. "2900 b13" appears only in `cells.py`, and ev13 has no bulk file. |
| forms-fields | -12, -11 | The screenshots (`ev14/form-T12f.png`, `ev13/form-filled.png`) show the form before Finalize. The commit numbers appear only in `cells.py`. |
| presence | -12, -04 | No presence evidence file for b12. |
| admin-invitations | -04 | The only claim screenshot is for -11 (`ev3/invite-claim-…-11-B11`). |
| admin-integrations | -12 | The b11 evidence was taken with admin grants. The cell says owner grants, but `membership/CHANGES.md` (04:31) says the -12 owner grants lack `config.integrations`. BUG-8 is still Open in `bugs/BUG-8…md`. |

That is **18 cells**. With them back at FAIL, the matrix is at most 62/90 PASS + 10 matches staging.

### Stale: re-run on the current backend before crediting

| Row | Tenant(s) | Latest evidence |
|---|---|---|
| auth-refresh | all | `ev3/auth-refresh-*`, b4 (c43308d9), before the BUG-2/BUG-4 fixes |
| workspace-load | all | `ev3/workspace-load-*`, b3. The row is "versus staging", but staging was never measured. The Rust reference above would make this measurable. |
| navigation | -11, -04 | `ev4/nav-T*-result-*`, b6 (858f19e0) |
| tasks-edit | all | b10/b12. Second client took 6.6 s (-11) and 4.3 s (-12), which contradicts the realtime PASS. |
| files-upload, files-download, files-preview | -11, -04 | b10 (cc317f86) only. These rows use Actions. |
| offline | -11, -04 | b6; b11 screenshot only; `bConvergedMs` null in `ev10/run-T04` |
| presence | -11 | b8 screenshots; the 11.3 s / 9.3 s figures are not in any raw file |
| admin-integrations | -04 | b12 screenshot; `apiKeys.create` is an Action |
| jobs-automations | -04 | b13 (Actions cc317f86) |
| jobs-ai | all | b10/b11, before inline agent Actions (944d8298) |

### Contradictory text or wrong links (PASS is defensible, but the cell must be fixed)

- **tasks-create, all:** the text still says "commits in 5–8 s". The latest -12 create had the second client at 7.9 s and 11.2 s.
- **navigation -12:** the link points to `ev16/nav16-T12-0.json`, which is the b17 run with errors. The b18 run is `ev16/navc-T12-0.json`. That run also has the /activity/map crash, which Task 3 now shows matches staging.
- **tasks-delete, files-download and files-preview on -12:** the link points to the b17 "No templates available" screenshot. The real evidence is `ev16/rows-T12-D.json` and `preview-T12.json` (b18). The cells still contain "held" and "untested" text. The 20:52Z preview had `ok:false`, and only the 21:00Z rerun passed.
- **Weak, but left as PASS:**
  - search-filters -12/-04: `filteredRows:0`.
  - boards -11/-04: screenshots show the card moves; the numbers appear only in `cells.py`.
  - admin-users -12: the list is shown; the edit appears only in `cells.py`.

**Solid on recent evidence:**
- auth-login ×3, auth-logout ×3, comments ×3, admin-roles ×3
- tasks-status -11/-04, tasks-delete -11/-04, admin-users -11/-04, admin-invitations -11/-12
- tasks-bulk -11, files-upload -12, search-filters -11, offline -12, jobs-reports -04

That is **25 cells**. Most of them are still on pre-34562a4a backends.

## Task 3: SpotsMapView cold-load crash

**It reproduces on Rust: client bug, matches staging.**

Method: the same 678bdc80 build against the Rust reference. A fresh browser context was signed in as the test admin (team-scoped, as on dev). The next navigation went straight to `/activity/map` with the client's pushState, then waited 8 s.

| Backend / tenant | Result | Evidence |
|---|---|---|
| Rust -12 copy, cold | `TypeError: Ze is not iterable at Ce (SpotsMapView-CUqDId_2.js:22:3476)`, then `[ERROR][UI] Route render failed`, and an empty main pane | `h/ev16/activity-map-cold-R12.json` |
| Rust -11 copy, cold | no error; the map rendered (0 mapped / 30 missing location) | `h/ev16/activity-map-cold-R11.json`, `.png` |
| CF -12, cold (b18) | same TypeError | `ev16/navc-T12-0.json`, `ev16/activity-map-T12.png` |

Same chunk hash, same symbol, same tenant. This is the client bug BUG-11: `recentWorkspaceTasks` is iterated before its first load. It shows on the large tenant whichever backend serves it. It is not a CF behavior difference.

## Task 4: the 1006 close at socket age 32:01 (B, -11, 2026-11-18 06:04:10.178Z)

**Not a server close, and the gateway Durable Object did not reset.** The drop was on the browser↔Cloudflare-edge leg. The client recovered cleanly. This is platform behavior, acceptable since recovery is clean, and it is not a fixed lifetime limit.

Evidence: Workers Logs (observability API) and DO analytics (GraphQL), read-only with `full-cloudflare-access`. Tools: `t4/q.mjs`, `t4/gql2.mjs`, and the transport tools' `window.mjs`.

1. **Which object held the socket.** B's socket #2 was upgraded at 05:32:09.051Z (`GET /ws`). It was held by `AssemblyGateway` object `2cf20b…` (event 05:32:09.059Z). B's #1 was on `6de76d…` and did not close.
2. **No server-initiated close.** Every server close logs `session: server close` (`cloudflare/kernel/session/session.ts` `close()`, BUG-9). The window 06:04:00–06:04:20Z holds 114 events, under the 200 cap, so the query returned all of them. It contains no such line, and no `webSocketClose`/`webSocketError` invocation on `2cf20b`.
   - The only `2cf20b` events are its routine 60 s alarm pair (06:04:10.093 ok, .108 `canceled`).
   - The same pair recurs every minute: 06:02, 06:03, 06:05:10 ok/canceled. DO analytics count it as `clientDisconnected` for each of those minutes. It is not specific to the close.
3. **No DO reset.** DO periodic analytics for `2cf20b`:
   - 0 `fatalInternalErrors`, `exceededCpu` and `exceededMemory` errors throughout.
   - Alarms keep running.
   - `activeWebsocketConnections` stays at 1 from 05:32 through at least 06:20, and reaches 0 by 06:27.
   - So the gateway never learned the socket had closed: the server side stayed half-open for about 16–23 min. That is consistent with the TCP/WebSocket leg between the browser and the edge being dropped, not the worker.
4. **Not a lifetime limit.** In the -12 session the sockets lived about 57 min with no close (`ev16/wsobs-T12-*.json`). A's -11 sockets also outlived 32 min.
5. **Recovery.** Socket #3 opened at 06:04:11.502 through gateway `dcac1a…` (101, auth answered, no `replica.reset`, no 5xx). The harness made no writes between the window end (06:04:02) and the reconnect, so no data was at risk. Lost or duplicated data was not checked by comparing data after the fact. The evidence is the resumed (non-reset) replica stream.
6. **Minor follow-up, not a blocker.** The server-side half-open socket was held for 16–23 min after the client was gone. Liveness sends to a dead edge leg do not fail. The cost is a resource, and possibly presence, hygiene gap, not a correctness bug.

Cloudflare's edge logs are not accessible to this account, so the exact edge cause (for example an edge restart) cannot be named.

## Evidence

All under `/home/gabriel/work/gonvex-indep-verify-20261118/`:
- Rust reference:
  - `run-rust.sh`: container env, no secrets in the file
  - `sanitize.py`, `grants.mjs`, `teams12.mjs`
  - `build-rust.py`, `build-rust-env.json` (frontend env differences)
  - `dist-rust-local/` (the built frontend)
- Create measurement:
  - `interleave.sh`, `interleave.out` (full run log, versions and lock times)
  - `h/ev16/run-rust-x13-1.out`
  - `h/ev16/x13-R11*-rust0bf3ad13.json`, `h-cf/ev16/x13-C11r*-cf34562a4a.json`
  - `stats.txt` (all rows and medians)
- Action split: `t4/act.py`, applied to the CF JSON files above and to the walkthrough's `ev16/x13-T{11,04}-34562a4a.json`
- SpotsMapView: `h/ev16/activity-map-cold-R1{1,2}.json`, `.png`
- 1006 close: `t4/q.mjs`, `t4/qws.mjs`, `t4/gql2.mjs`

Tenant names, domains and staging tenant IDs stay in `t11-private.json`/`t12-private.json` (mode 600). The copy of the staging data was deleted after the runs.

Side effects on dev: 8 synthetic C3-SYNTH-X13 tasks were created on `devcopy-stg1117-11`, and all were archived (`remaining: 0` both rounds). The lane lock was taken and released twice. `identity.mjs` wrote `identity/indep-*.json` (read-only identity snapshots) into the walkthrough folder. Nothing was deployed.
