# Current staging frontend readiness

Updated 2026-11-17. This file is the single source of truth for Cloudflare frontend readiness. ROOT owns matrix updates; implementation and verification owners supply evidence.

**Acceptance: 2/6 criteria PASS. Feature cells: 0/90 PASS.** FAIL includes not yet verified, and does not by itself assert a product defect. The three test-tenant columns remain unselected until the complete current inventory and row counts identify the largest tenant and two typical tenants. Expand the import table for every actual staging tenant; do not treat these three test selections as the import denominator.

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

| ID | Feature | Largest (selection pending) | Typical A (pending) | Typical B (pending) | Current evidence / blocker |
|---|---|---|---|---|---|
| auth-login | Login with an existing copied user | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| auth-logout | Logout | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| auth-refresh | Session refresh | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| workspace-load | Initial workspace load versus staging | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| navigation | Every main navigation screen; no backend HTTP/console errors | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-create | Create task | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-edit | Edit task | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-status | Change task status | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-assign | Assign task | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-delete | Delete task | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| tasks-bulk | Bulk task actions | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| comments | Comments | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| files-upload | File upload | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| files-download | File download | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| files-preview | File preview | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| files-private | Private-file owner and foreign-user checks | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| forms-fields | Forms and custom fields | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| boards-dashboards | Boards and dashboards | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| search-filters | Search and filters | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| notifications | Notifications | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| presence | Presence | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| admin-users | Settings/admin: users | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| admin-roles | Settings/admin: roles | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| admin-invitations | Settings/admin: invitations | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| admin-integrations | Settings/admin: integrations and API keys | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| realtime | Second-client update within 1 second | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| offline | Offline edit, reconnect and convergence | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| jobs-reports | UI-triggered scheduled reports/jobs | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| jobs-automations | UI-triggered automations/actions | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |
| jobs-ai | AI/agent actions present on staging | FAIL | FAIL | FAIL | [Not yet run against current target](/mnt/data/work/gonvex-cloudflare-lead/frontend-readiness-20261117/current-target.json) |

## Execution and failure handling

1. Same K22 deployment owner rotates the DEV storage key, completes reviewed Runtime migration and reconciles/imports ALL current staging tenants. Preserve old evidence and data; do not claim old signed dev URLs survive rotation. Coordinate a provider-write lane so releases/imports do not disrupt browser or conformance observations.
2. Prepare the exact current frontend outside its source repository with backend-only environment differences, verify source/generated-ref correspondence and actual served versions, and select test tenants from measured row counts. Use existing permitted real logins; missing credentials remain explicit and must not be replaced by fabricated access.
3. Root-cause transport503/unexpected closes/resets first. Carry actual request/holder/epoch/service identity and safe refusal/close provenance; no cause inference from timing or a branch bit. Fix backend defects as ordinary PRs to cloudflare/integration; deploy and rerun affected rows only.
4. Run the listed UI features on three tenants, compare initial workspace timing with read-only staging, measure second-client delivery latency, then conduct the uninterrupted30-minute two-client session. No deployments, fixture resets or operator cleanup inside its measurement interval.
5. Run the full unchanged conformance suite on the recorded deployment, serially with its fixture-publication lane. Preserve failed assertions, original deadlines and truthful unsupported outcomes. Each completed batch updates this matrix and only its affected rows.

Source-reviewed/CI-green backend PRs may merge while these downstream criteria remain open. Planned, implemented, drilled and continuously observed are different states. No capacity/load/soak/FIN/OWNER/monitor/production acceptance is implied here.
