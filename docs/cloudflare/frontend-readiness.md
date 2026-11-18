# Current staging frontend readiness

Updated 2026-11-17. This file is the single source of truth for Cloudflare frontend readiness. ROOT owns matrix updates; implementation and verification owners supply evidence.

**Acceptance: 2/6 criteria PASS. Feature cells: 34/90 PASS.** FAIL includes not yet verified, and does not by itself assert a product defect. The three test-tenant columns remain unselected until the complete current inventory and row counts identify the largest tenant and two typical tenants. Expand the import table for every actual staging tenant; do not treat these three test selections as the import denominator.

## Exact target

- Frontend: `Whagons-International/whagons5-client`, branch `staging`, head `678bdc80c2c55fc3a32aca91c6e2504b6a1d3834`. Latest successful staging deployment `xqldpc1wnircj38oil26sgqz` finished 2026-11-17 08:58:32 UTC at `https://staging.whagons.com`; captured at 19:43:46 UTC. [Deployment metadata](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/staging-frontend-deployment.json), [branch head](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/staging-head.txt). Confirm again before testing if staging advances.
- Required dev build: exact unmodified frontend source/lockfile/generated refs from that SHA. Only backend URL/environment may differ. Source patches in the frontend are forbidden; incompatibilities are backend contract bugs. Prepared and login-verified 2026-11-17 (criterion 1). Note: the UI's task "Delete" calls `tasks.archive` (restorable); the tasks-delete row tests that behavior.
- Dev: [assembled Worker](https://gonvex-cf-dev-integration-20261117.whagons-international.workers.dev). Last verified live backend `8646997ffd05875072c88101cdb9b75fd5e7d086`, four Workers; integration `471a01c6ca047c42865e2fd9668a9e3c9d73836e` Runtime migration is being resumed. [Last release](/mnt/data/work/gonvex-cf-dev-integration-20261117/release-439/REPORT.md). Record exact backend versions with each test batch.
- Earlier browser evidence used frontend `3ad4fa47ed7f6c0c7d41d0b3d5a9cfd92c8d01b9`; none is credited as a PASS for this current target.
- Staging, production, main and frontend source/deployment are read-only. Dev signing-key rotation is authorized; recovery/private-signer work is no longer a prerequisite. Capacity telemetry, driver and tuning are secondary and do not gate parity.

## Six acceptance criteria

| ID | Required result | Status | Evidence / remaining work |
|---|---|---|---|
| 1 | Exact current staging frontend, unmodified, against CF dev | PASS | Built 2026-11-17 from staging `678bdc80`: git tree 6510/6510 files identical, frozen lockfile, production `pnpm build`. The only env differences are VITE_GONVEX_URL, VITE_GONVEX_PROJECT_ID, VITE_BASE_DOMAIN=localhost and an empty VITE_ASSET_ORIGIN. Served locally at `devcopy-source-3.localhost:41784`. Real copied-user login reached /welcome with 0 console/page errors and the /ws socket open (7.7 s sign-in to /welcome; control connection took 3.6 s). [Build root](/mnt/data/work/gonvex-current-staging-frontend-20261117/dist-dev-local), [login evidence](/mnt/data/work/gonvex-current-staging-frontend-20261117/login/result.json). Workspace and feature rows are not yet verified. |
| 2 | Every current staging tenant imported with P03 and matching row counts | PASS | 15/15 current tenants, 473,192 source = imported rows, 0 checksum mismatches, 0 schema refusals, on `471a01c6`. See the ledger below. Redo the import if staging adds tenants. |
| 3 | Complete real-browser feature matrix on largest plus two typical tenants, with original copied-user logins | FAIL | No current-build rows verified. [Prior limited browser run](/mnt/data/work/gonvex-v01-864-browser-20261117/REPORT.md) is historical only. |
| 4 | Continuous 30-minute session, 2+ clients, zero transport503/unexpected close/reset | FAIL | Latest limited run had503/1011/1006 and resets; recovered flow does not pass. [Failure log](/mnt/data/work/gonvex-v01-864-browser-20261117/REPORT.md). Workers Logs root cause (20:20 UTC): all three 1011 closes match gateway "Subrequest depth limit exceeded" errors in tenant→gateway `deliverReplicaInitial` context. The 503s are router deadline expiries (slow GatewayController/limiter) whose cause was swallowed. Resets and 1006 closes are unexplained. [Findings](/home/gabriel/work/gonvex-transport-rootcause-20261117/FINDINGS.md). Fix owner: X02 transport delegate. |
| 5 | Full conformance green on exact deployed head | FAIL | Last complete run20PASS/18FAIL/1SKIP on older deployment; full current run pending. [Suite log](/mnt/data/work/gonvex-v01-dev-verification-20261117/REPORT.md) |
| 6 | Current feature-by-tenant matrix with evidence for each result | FAIL | Matrix initialized; tenant selection, complete inventory and current execution evidence are unfinished. |

## Every-tenant P03 import ledger

**15/15 current staging tenants imported, 473,192/473,192 rows, 0 mismatches.**

Inventory: the backend behind `gonvex-stg.whagons.com` is the Coolify runtime container on the staging origin. It mounts `/data/whagons-staging/current`. Its Control database lists project "Whagons Staging" (multiTenant) with 16 tenant rows: 15 active, each with exactly one tenant database file, and one deleted with no file. That makes 15 tenants. The only other project there is the single-database dashboard auth project, which is not a Whagons application tenant.

How the snapshot was taken: a read-only stream of each tenant DB plus WAL, and of `control.db`, captured 2026-11-17 21:27:56–21:30:28 UTC. A file was accepted only when the DB file's size and mtime were the same before and after the read; the WAL read is a valid framed prefix. Nothing on staging was opened by SQLite, locked or written. All 15 local copies pass `quick_check` and share the same 15 applied migrations.

Import path: `gonvex-tenant-stage` → A01 whole-schema image (`a01.1`, 0 mismatches) → the kernel's `/__tenant-transfer` bridge. The bridge writes directly into `AssemblyTenant` objects in the deployed `471a01c6` composition, under fresh `devcopy-stg1117-NN` names. For every tenant:
- All six comparison legs had 0 mismatches: source→staged, staged→object, source→object, object→raw, raw→out and source→out.
- 0 DDL refusals and 0 schema differences.
- Object `quick_check` ok, 0 foreign-key violations, FTS ok.
- `complete.ready=true`.

Row counts cover the P03-copied tables only. Sync history, migration ledger, bootstrap and reference ledger are excluded or rebuilt by design. The transfer settings were removed from both Workers afterwards, and the endpoint returns 404. All 15 copies were then registered in the dev multiTenant app project through `POST /dev/tenants` (15/15 HTTP 201). Tenant names, domains and the mapping to staging tenant IDs are kept only in the [private label map](/home/gabriel/work/gonvex-cf-dev-integration-20261117/p03/labels-private.json). [Snapshot provenance](/home/gabriel/work/gonvex-cf-dev-integration-20261117/staging-all-20261117/provenance.json), [summary](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/summary-private.json).

Browser-matrix selection, by copied rows:
- Largest: `devcopy-stg1117-12`, 115,547 rows.
- Typical A: `devcopy-stg1117-11`, 8,018 rows. This is the median, with 16 members and 76 tasks.
- Typical B: `devcopy-stg1117-04`, 6,182 rows, next to the median, with 62 tasks.

| Tenant / dev copy | Current P03 | Source rows | Imported rows | Row match | Evidence |
|---|---|---:|---:|---|---|
| `devcopy-stg1117-01` | PASS | 26,178 | 26,178 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-01.json) |
| `devcopy-stg1117-02` | PASS | 41,217 | 41,217 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-02.json) |
| `devcopy-stg1117-03` | PASS | 2,335 | 2,335 | PASS (380 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-03.json) |
| `devcopy-stg1117-04` | PASS | 6,182 | 6,182 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-04.json) |
| `devcopy-stg1117-05` | PASS | 4,204 | 4,204 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-05.json) |
| `devcopy-stg1117-06` | PASS | 102,054 | 102,054 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-06.json) |
| `devcopy-stg1117-07` | PASS | 945 | 945 | PASS (380 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-07.json) |
| `devcopy-stg1117-08` | PASS | 89,439 | 89,439 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-08.json) |
| `devcopy-stg1117-09` | PASS | 1,001 | 1,001 | PASS (380 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-09.json) |
| `devcopy-stg1117-10` | PASS | 3,324 | 3,324 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-10.json) |
| `devcopy-stg1117-11` | PASS | 8,018 | 8,018 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-11.json) |
| `devcopy-stg1117-12` | PASS | 115,547 | 115,547 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-12.json) |
| `devcopy-stg1117-13` | PASS | 51,485 | 51,485 | PASS (381 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-13.json) |
| `devcopy-stg1117-14` | PASS | 5,700 | 5,700 | PASS (380 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-14.json) |
| `devcopy-stg1117-15` | PASS | 15,563 | 15,563 | PASS (380 tables, 0 checksum mismatches) | [Report](/home/gabriel/work/gonvex-cf-dev-integration-20261117/import/devcopy-stg1117-15.json) |
| Historical `devcopy-source-2` / `devcopy-source-3` | Superseded | 1001 / 2335 historical | 1001 / 2335 historical | Historical PASS only | [Old import](/mnt/data/work/gonvex-cf-dev-integration-20261117/import-431/report.json) |

## Feature by selected tenant

Every FAIL below currently means not verified against frontend678bdc80 and the exact current dev backend, unless a specific defect/evidence link replaces it. Capture actual UI screenshots and content-free logs for each result; maintain original login/account ownership and copied-user authorization. Identify unsupported staging features from actual staging behavior rather than silently omitting them. No synthetic ready flag, fake receipt or mock can pass a row.

| ID | Feature | Largest `devcopy-stg1117-12` | Typical A `devcopy-stg1117-11` | Typical B `devcopy-stg1117-04` | Current evidence / blocker |
|---|---|---|---|---|---|
| auth-login | Login with an existing copied user | PASS | PASS | PASS | [Rendered-form login, member resolved on all three (b3 22:14, kernel 537df10e). O-1: one retried `auth.error transient` per login. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| auth-logout | Logout | FAIL: BUG-4 | FAIL: not run | FAIL: not run | [Not rerun on these copies (verified on warm-up source-3 only); waits for lane lock. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| auth-refresh | Session refresh | FAIL: BUG-4 | PASS | PASS | [Reload restores session and grid on 858f19e0 (01:12); T12 stuck "Restoring your secure session…" (BUG-4). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| workspace-load | Initial workspace load versus staging | FAIL: BUG-4 | PASS | PASS | [Dev only (staging not measurable without login): Get Started→grid 1.0–1.4 s, first rows 1.6–1.9 s; reload grid 1.3 s. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| navigation | Every main navigation screen; no backend HTTP/console errors | FAIL: BUG-4 | PASS | PASS | [147 static routes, 0 backend 4xx/5xx/console errors on 858f19e0 (01:18–01:31). Disabled powerups redirect to /welcome (31/22). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-create | Create task | FAIL: BUG-4 | PASS | PASS | [tasks.create commits in 3–8 s; second client sees it after the commit. O-2: assistant.rewriteText action.error (dev OPENROUTER secret missing). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-edit | Edit task | FAIL: BUG-4 | PASS | PASS | [tasks.updateDetails/updateDescription committed; second client converged. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-status | Change task status | FAIL: BUG-4 | PASS | PASS | [Grid status change, second client 0.53 s (-11) / 0.73 s (-04). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-assign | Assign task | FAIL: BUG-4 | PASS | PASS | [Assign non-admin member, second client 0.54 s / 0.52 s; notification delivered to assignee. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-delete | Delete task | FAIL: BUG-4 | PASS | PASS | [UI Delete → tasks.archive, archived view, restore → tasks.bulkRestore. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| tasks-bulk | Bulk task actions | FAIL: BUG-4 | FAIL: partial | FAIL: partial | [bulkUpdatePriority, bulkReassign, bulkComplete, bulkArchive OK; reschedule (date picker) not exercised. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| comments | Comments | FAIL: BUG-4 | PASS | PASS | [createNote/updateNote/removeNote on 858f19e0. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| files-upload | File upload | FAIL: BUG-4 | PASS | PASS | [generateUploadUrl → upload → createNote → getFileUrls; stored bytes sha256 match fixture (-11). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| files-download | File download | FAIL: BUG-4 | FAIL: BUG-6 | FAIL: BUG-6 | [Page fetch of signed URL fails (service worker cached opaque entry; dev image proxy 404). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| files-preview | File preview | FAIL: BUG-4 | FAIL: BUG-6 | FAIL: BUG-6 | [Full image renders; thumbnails via image proxy 404 (BUG-6). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| files-private | Private-file owner and foreign-user checks | FAIL: BUG-4 | FAIL: not run | FAIL: not run | [Confidential files not exercised yet. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| forms-fields | Forms and custom fields | FAIL: BUG-4 | FAIL: not run | FAIL: not run | [Not exercised yet. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| boards-dashboards | Boards and dashboards | FAIL: BUG-4 | FAIL: not run | FAIL: not run | [Not exercised yet. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| search-filters | Search and filters | FAIL: BUG-4 | PASS | PASS | [Search finds marker; priority filter applied and cleared. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| notifications | Notifications | FAIL: BUG-4 | PASS | PASS | [Non-admin receives "Assigned to you"; click opens the task; Clear all. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| presence | Presence | FAIL: BUG-4 | FAIL: not verified | FAIL: not verified | [presence.snapshot flows to both clients; no UI indicator of the other member was verifiable. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| admin-users | Settings/admin: users | FAIL: BUG-4 | FAIL: recheck | PASS | [Users list + team edit (members.saveProfileAndTeams); non-admin denied on -04; -11 denial check flaky at 00:46. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| admin-roles | Settings/admin: roles | FAIL: BUG-4 | PASS | PASS | [8 / 6 roles listed; non-admin gets "You don't have access". Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| admin-invitations | Settings/admin: invitations | FAIL: BUG-4 | PASS | PASS | [control.invitations.create + claim by the non-admin test member. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| admin-integrations | Settings/admin: integrations and API keys | FAIL: BUG-4 | FAIL: partial | FAIL: partial | [apiKeys.create OK on 6397e41d/858f19e0 (BUG-1 fixed); revoke not exercised. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| realtime | Second-client update within 1 second | FAIL: BUG-4 | FAIL | FAIL | [Status/assign ≤1 s; create/edit 3–10 s end to end (reducer commit latency; delivery <0.25 s after commit). No change-feed-reset outside publications (#449). BUG-5 half-open socket after a publication. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| offline | Offline edit, reconnect and convergence | FAIL: BUG-4 | PASS | PASS | [Offline status change queued, replayed on reconnect (0.3 s / 4.8 s), second client converged. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| jobs-reports | UI-triggered scheduled reports/jobs | FAIL: BUG-4 | FAIL: disabled | FAIL: not run | [-11: /cleaning and /reports disabled on this copy; -04: /reports enabled, not exercised. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| jobs-automations | UI-triggered automations/actions | FAIL: BUG-4 | FAIL: not run | FAIL: not run | [Not exercised yet. Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |
| jobs-ai | AI/agent actions present on staging | FAIL: BUG-4 | FAIL: O-2 | FAIL: O-2 | [assistant.createThread/submitUserMessage commit; reply never arrives (dev OPENROUTER_API_KEY not configured). Backend 858f19e0 (kernel 738336a3) unless noted.](/home/gabriel/work/gonvex-c3-walkthrough-20261117/REPORT.md) |

Matrix notes (C3 walkthrough, 2026-11-18):
- Tenant logins (DEV copies only, coordinator-authorized): `staging-smoke-admin` was added as admin to -12/-11/-04 via `POST /dev/internal/e2e/members`, then given all teams on -11/-04 through Settings → Users (`members.saveProfileAndTeams`). The vault test account `staging-smoke-test` was invited as a non-admin member with all teams on -11/-04 (`control.invitations.create`, claimed through the invitation link). These are added test members, not original copied users. No Firebase, staging or production account was changed, and no password was set. -12 could not be completed: its admin UI never loads (BUG-4), so no teams or non-admin member could be added there. [Change record](/home/gabriel/work/gonvex-c3-walkthrough-20261117/membership/CHANGES.md)
- Open bugs: BUG-4 -12 `change-feed-lagged` loop (#453 pending); BUG-5 half-open application socket after a publication (auth unanswered, writes parked until reload); BUG-6 storage fetch / dev image proxy 404. BUG-1 (#448) and BUG-2 (#449) are verified fixed on 6397e41d/858f19e0: Actions rows work, and no `change-feed-reset` occurred outside fixture publications. O-2: dev lacks the OPENROUTER project secret. [Repros](/home/gabriel/work/gonvex-c3-walkthrough-20261117/bugs)
- The 30-minute two-client session runs on -11 while -12 is blocked, under `/home/gabriel/work/gonvex-dev-lane.lock`.
- Cutover follow-up (Gabriel, non-blocking): the whagons5-client service worker must not serve an opaque cached response to fetch() (BUG-6 triage, #456).

## Execution and failure handling

1. Same K22 deployment owner rotates the DEV storage key, completes reviewed Runtime migration and reconciles/imports ALL current staging tenants. Preserve old evidence and data; do not claim old signed dev URLs survive rotation. Coordinate a provider-write lane so releases/imports do not disrupt browser or conformance observations.
2. Prepare the exact current frontend outside its source repository with backend-only environment differences, verify source/generated-ref correspondence and actual served versions, and select test tenants from measured row counts. Use existing permitted real logins; missing credentials remain explicit and must not be replaced by fabricated access.
3. Root-cause transport503/unexpected closes/resets first. Carry actual request/holder/epoch/service identity and safe refusal/close provenance; no cause inference from timing or a branch bit. Fix backend defects as ordinary PRs to cloudflare/integration; deploy and rerun affected rows only.
4. Run the listed UI features on three tenants, compare initial workspace timing with read-only staging, measure second-client delivery latency, then conduct the uninterrupted30-minute two-client session. No deployments, fixture resets or operator cleanup inside its measurement interval.
5. Run the full unchanged conformance suite on the recorded deployment, serially with its fixture-publication lane. Preserve failed assertions, original deadlines and truthful unsupported outcomes. Each completed batch updates this matrix and only its affected rows.

Source-reviewed/CI-green backend PRs may merge while these downstream criteria remain open. Planned, implemented, drilled and continuously observed are different states. No capacity/load/soak/FIN/OWNER/monitor/production acceptance is implied here.
