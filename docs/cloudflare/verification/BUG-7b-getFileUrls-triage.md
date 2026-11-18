# BUG-7b triage: `files.getFileUrls` fails the batch on devcopy-stg1117-12

Walkthrough 2026-11-17 (report `BUG-7-getFileUrls-missing-file-throws.md`). Backend afecea5e, tenant
devcopy-stg1117-12, admin client. Every `files.getFileUrls` answered
`execution_failed … GonvexHostError: host call failed: storage host call failed [host_call_failed]: file not found`.

## Verdict

The kernel matches the Rust runtime. A missing **blob** never fails a call on either runtime. What fails is a
missing **row**: `_gonvex_files` has no row for the id under the caller's tenant. Rust fails the call with the
same text in that case. The data comes from P03: it copies `_gonvex_files` rows verbatim with the source tenant's
id, and the copied tenant's id is its object name. This PR does not change backend behavior. It adds a regression
test that pins Rust's per-item behavior for missing blobs and Rust's failure for an id with no row in this tenant.

## Which host call throws

`files.getFileUrls` is a Whagons Action (`whagons5-client/gonvex/files.ts`). For each id it calls
`ctx.storage.getMetadata(id)`, checks `memberCanReadStorageFile`, then calls `ctx.storage.generateDownloadUrl(id, 10 min)`.
It catches nothing, so one throwing host call fails the whole batch. That is the app's code, and it does the same
on Rust.

| Case | Rust (`rust/crates/runtime/src/storage.rs`) | Kernel (`cloudflare/kernel/files/tenant-files.ts`) |
| --- | --- | --- |
| Row present, `uploaded`, blob missing | `getMetadata` returns the row. `getUrl`/`generateDownloadUrl` mint a URL without touching the object (219-243). The download is a 404 `object not found` (`lib.rs:730`). | Same: `metadata` reads the row, `downloadUrl` signs it, and the /storage proxy answers 404 `object not found`. |
| Row `pending`, blob never arrived | Finalize HEADs, gets 404 and leaves the row pending (660-674). A URL is minted. | Same (`metadata`, finalize branch). |
| No row with this `id` **and** `tenant_id` | `file not found` (649-659, `WHERE id=$1 AND tenant_id=$2`) | `file not found` (`row(fileId, session.tenantId)`) |
| Private file of another owner | Metadata returned. The app's `memberCanReadStorageFile` gives `null`. | `file not found` (IB1, intended behavior). Not this bug: every `_gonvex_files` row in -12 is `tenant`. |

## Why -12 has no rows for its own tenant

Read-only check of the staged P03 file `p03/devcopy-stg1117-12.a01.db` (local, development copy):

- `_gonvex_files`: 1,732 rows, all `visibility='tenant'`, 1,453 `uploaded`, 279 `pending`, none deleted. All of them
  have `tenant_id = 'tenant_a937…'` (the staging tenant) and object keys `01f19dd6…/tenant_a937…/<id>`.
- Every storage id the app references (`members.avatar_url`, `documents.storageId`, every `taskNotes.attachments`
  `storageId`, 1,428 distinct) has a row. No reference dangles.
- The Cloudflare tenant id is the tenant object's name (`ctx.id.name`, `kernel/assembly/tenant.ts`; Actions get
  `env.TENANT.getByName(session.tenantId)`), so the copied tenant is `devcopy-stg1117-12`.

Every copied file therefore misses the `tenant_id` filter and reads `file not found`, on either runtime. The
same holds for every devcopy tenant (-04: `tenant_1f97…`, -11: `tenant_9091…`). Their successful calls in the
walkthrough came after files were uploaded on the copy itself, which carry the object's name as their tenant id.

## Regression test

`cloudflare/kernel/files/test/files.test.ts`, "a batch with a missing blob (BUG-7b)", runs the app's per-id loop
against a real tenant object and local R2:

- One present file, one `uploaded` file whose R2 object was deleted, and one `pending` file that was never uploaded:
  every id gets a signed URL, as in Rust. The present file downloads its bytes. The other two download 404
  `object not found`, and the pending row stays pending.
- A row whose `tenant_id` is the P03 source tenant: `getMetadata` fails with `file not found` and the batch fails,
  as in Rust.

Returning `null` per item for an unknown id would diverge from Rust and the contract (`CONTRACT.md`, `storage` op
`getMetadata`/`getUrl`/`generateDownloadUrl`: error "file not found"). Doing that would need a contract-change note.

## Follow-ups (not done here)

- P03 (`cloudflare/tools/tenant-transfer`): when the target object's tenant id differs from the source, rewrite
  `_gonvex_files.tenant_id` to the target. Copying the blobs into the dev bucket, or rewriting `object_key`, also
  makes them downloadable. Without the blobs the rows resolve, but each download is a per-item 404, as in Rust.
  This changes the transfer's "copied row for row" checksum rule, so it is the lead's call.
- Re-import devcopy-stg1117-12 (and the other devcopy tenants) once P03 rewrites the tenant id, then re-run the
  walkthrough's navigation on -12.
