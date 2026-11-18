# 0091: dev-only `POST /dev/internal/files/remap-tenant` for P03 copies (dev tooling)

Raised by X11b on 2026-11-18, after the BUG-7b triage (`docs/cloudflare/verification/BUG-7b-getFileUrls-triage.md`).
**Dev tooling only.** It has no Rust counterpart and changes no contract surface that clients, the module SDK or
the Whagons modules see. Approved fix direction: remap the copied file rows.

## Why

P03 copies `_gonvex_files` row for row, so a `devcopy-stg1117-NN` tenant keeps the staging tenant id, the
staging object key (`<staging project>/<staging tenant>/<id>`) and `bucket = whagons-staging`. Storage lookups
filter on `id AND tenant_id` (Rust `storage.rs:649-659`, kernel `TenantFiles.row`), so every copied file reads
`file not found`, and `files.getFileUrls` fails its whole batch, as it would on Rust with the same rows.
No existing dev route can repair the rows. `PATCH /dev/data/tables/*` and `/dev/data/references/replace`
refuse `_gonvex_*` tables and tenant-id columns. `/__tenant-transfer` answers 409 once the tenant is registered.

## The operation

`POST /dev/internal/files/remap-tenant`, body `{projectId, tenantId, fromTenantId, dryRun?: boolean}`.

Gate, in order:

1. The runtime admin key, as for every `/dev/internal` route (`/dev/internal/e2e/members`). Otherwise 403
   `runtime admin key is required`.
2. The P03 dev-transfer gate (`devTransferAllowed`, the one `/__tenant-transfer` uses):
   `GONVEX_DEV_TRANSFER_TARGET` starts with `gonvex-cf-dev-`, the tenant name matches `devcopy-*`, and it is
   listed exactly in `GONVEX_DEV_TRANSFER_TENANTS`. Otherwise 404 `file remap is not available on this
   deployment`. Staging and production set neither variable, so the route does not exist there. The control
   object checks the gate, then the tenant object checks it again on its own name.
3. The tenant resolves in `projectId` (else 404 `tenant database was not found`). The tenant object belongs to
   `projectId` (else 409). Storage is configured (else 503).

Effect, in one tenant transaction on the reducer serial queue, for each row with `tenant_id = fromTenantId`:

- `tenant_id` becomes the tenant's own id.
- `object_key` becomes `objectKey(projectId, tenantId, id)`, the key a native upload gets (`safe(project)/safe(tenant)/id`).
- `bucket` becomes this deployment's `GONVEX_STORAGE_BUCKET`.
- The row's `_gonvex_storage_uploads` ticket, if any, follows the key and project.

Nothing else changes: status, sizes, owners, visibility and timestamps stay, and rows under any third tenant
id are left alone. Blobs are not copied. A remapped file resolves, and its download is a per-item 404 until
its object is in the dev bucket at the new key, which matches Rust for a missing object.

Result: `{fromTenantId, tenantId, projectId, bucket, dryRun, matched, remapped, remaining, native, otherTenants, total}`.
`matched` counts rows under `fromTenantId` before the call. `remapped` counts rows this call wrote (0 on a dry
run). `remaining` counts rows still under `fromTenantId`. `native` counts rows under the tenant's id with the
native key and bucket. **Idempotent:** a repeat matches 0 rows and writes nothing.

## Tests

- `cloudflare/kernel/files/test/files.test.ts`, "dev remap of P03-copied file rows (X11b)": dry run, rewrite,
  ticket, a third tenant left alone, idempotent repeat, then the BUG-7b batch resolving per item. Also the
  refusals.
- `cloudflare/kernel/assembly/test/tenant-transfer.spec.ts`: the shared gate, and `AssemblyTenant` admitting a
  listed copy for its own project only and refusing an unlisted name.
- `cloudflare/router/dev/test/data.test.ts`: the admin key, the body checks, and 404 on a deployment without
  the dev-transfer variables.
