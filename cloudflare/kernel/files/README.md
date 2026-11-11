# Kernel files (K13)

File storage for the tenant object, on an R2 binding. The reference is `rust/crates/runtime/src/storage.rs` and the
`/storage` routes in `rust/crates/runtime/src/lib.rs:560-885` at `0bf3ad13`; the contract rows are `CONTRACT.md`
D.1 "Storage and files" and E.d, with IB1 (0001) and 0012 R6, R12, R13 and R26. Findings and proposed rulings are in
`docs/cloudflare/contract-changes/0035-k13-files.md`.

## Where each part runs

| Part | Runs in | File |
|---|---|---|
| `_gonvex_files` rows, upload tickets, URL minting, owner checks, finalize | tenant object | `tenant-files.ts` |
| Reducer `ctx.storage.getMetadata` (K04's `FileMetadataSource`) | tenant object, in the Reducer's transaction | `reducer.ts` |
| Action `ctx.storage.*` and sandbox `importFile` reads (K12, K20) | Action Worker → tenant RPC; `store` bytes written here | `rpc.ts` (`ActionFiles`) |
| `GET/HEAD/POST/PUT/OPTIONS /storage/{*key}` | router Worker | `router/storage/index.ts` |
| Proxy signatures, SigV4 presign, keys, inline allowlist, payload parsing | anywhere | `signing.ts`, `sigv4.ts`, `keys.ts`, `media.ts`, `request.ts` |

Upload and download bytes stream between the router and R2. They never pass through the tenant object, which only
answers one small RPC per request (a ticket claim, or a row lookup).

## Wiring it in (for the packages that own the objects)

```ts
// Tenant object (K07): once per instance, then the FilesTenantRpc methods and the Reducer source.
const files = new TenantFiles({ sql: ctx.storage.sql, transactionSync: (f) => ctx.storage.transactionSync(f),
  bucket: env.FILES, config: filesConfigFromEnv(env) });
files.ensureSchema();
const rpc = filesRpc(files);            // filesCall, filesPrepareStore, filesRecordStored, filesReadTarget,
                                        // filesClaimUpload, filesReleaseUpload, filesDownloadTarget
new TenantModuleHost({ ..., files: reducerFiles({ bucket: env.FILES, configured: files.configured }) });

// Action Worker (K12): one per Action call; `system: true` only for principals with no account (scheduler).
new ActionFiles({ tenant: env.TENANT.getByName(tenantId), bucket: env.FILES, config }).call(session, op, payload);

// Router (M01; src/index.ts until then):
if (url.pathname.startsWith(STORAGE_PREFIX)) return handleStorageRequest(request,
  { bucket: env.FILES, config: filesConfigFromEnv(env), tenant: (id) => env.TENANT.getByName(id) });
```

Configuration: R2 binding `FILES`; vars `GONVEX_PUBLIC_URL`, `GONVEX_STORAGE_BUCKET`; secret `GONVEX_STORAGE_SECRET`
(set it to Rust's S3 secret access key to keep URLs valid across a cutover); optional `GONVEX_STORAGE_PROXY_MAX_BYTES`
(default 100,000,000) and the R2 S3 API credentials `GONVEX_S3_ENDPOINT`, `GONVEX_S3_ACCESS_KEY_ID`,
`GONVEX_S3_SECRET_ACCESS_KEY`, `GONVEX_S3_REGION` (default `auto`) for direct uploads. Without the three required
values every call fails with Rust's `storage is not configured`.

## Behaviour

**Kept from Rust:** the table and its indexes, object keys (`{safe(project)}/{safe(tenant)}/{fileId}`), the metadata
JSON (field order, omitted empty fields, chrono's timestamp format), the payload parser with serde's messages and
`deny_unknown_fields`, the operation texts (`fileId is required`, `upload too large`, `unknown storage operation "x"`,
`file not found`, `file is not uploaded`), download and upload signatures byte for byte, the URL shape, upload TTL
15 min and download TTL 10 min, the single-use ticket semantics (claim before the PUT, `If-None-Match: *` at the store,
release on failure, 409 `{"error":"upload already consumed"}`), the 128 MiB declared and body caps (413), the download
headers (`accept-ranges`, `content-length`, `content-range`, `content-type`, `etag`, `last-modified`,
`cache-control: private, max-age=300`), range requests (206), the inline allowlist with `nosniff` and the CSP, the
`/storage` CORS headers and the OPTIONS preflight, axum's 400 texts for a malformed query, and finalize-on-read
(`getUrl`, `getMetadata` and the Reducer finalize; `generateDownloadUrl` and `delete` do not).

**Intended behaviour (IB1, R12, R13):**
- A private file is readable, linkable and deletable only by its owner: `getUrl`, `generateDownloadUrl`,
  `getMetadata` (Action and Reducer), `delete` and the sandbox read answer `file not found` to anyone else. The check
  runs before finalize, so a non-owner cannot change the row. No session kind is exempt on reads.
- The owner is the caller's account id **or its tenant Member id** (Whagons writes `ownerId: ctx.member.id`).
  `ownerId` on upload or store must name the caller, else `ownerId must be the calling account or its tenant member`.
  A system principal (no account) may name any owner. Anonymous callers store `tenant` files with no owner, as in Rust.
- The proxy looks the row up on every download: an unknown or deleted file is `404 object not found`. A private file's
  URL is signed with its owner bound in (`"gonvex-storage-private:"` domain, message `"{key}\n{exp}\n{ownerId}"`), and
  only owner-checked mints produce it. A plain download signature, which is what any unchecked mint produces, gets
  403 on a private file.

**Platform-forced:**
- `store` decodes at most 32 MiB (R26): `storage content exceeds the 33554432 byte limit`. Its bytes are decoded and
  written in the Action Worker, since 43 MiB of base64 exceeds one RPC argument. Whagons has no `store` call site.
- Declared uploads over `proxyMaxBytes` (100 MB, the Worker request-body limit S4 measured) get a direct presigned R2
  `PUT` with `If-None-Match: *` and the declared `Content-Length` signed in. A second PUT is refused by R2. There is no
  proxy ticket for it. Undeclared uploads always get the proxy URL, so an undeclared upload over 100 MB fails at the
  platform with 413, as S4 found.
- A proxy upload with no `Content-Length` is buffered, capped at 32 MiB (413 above it). Browsers always send a length.
- Proxy URL TTLs are clamped to 1 s – 7 days, as presigned ones are (E.d.3).

## Tests

`pnpm test` runs `vitest run --root kernel/files`: 26 tests in workerd with a SQLite-backed tenant object, the real
router and a local R2 bucket. They cover uploads (single use, release on a failed PUT, every 403/409/413, OPTIONS,
query errors), downloads (headers, inline allowlist, 206 ranges, HEAD, deleted files), the IB1 refusals on every route,
finalize (Action and inside a Reducer through K04's host), `store` and its cap, direct presigned uploads, SigV4 against
the AWS documentation vector, the payload parser, and rows read and written per operation.

## Numbers

Rows on the tenant object (from the test; writes include index entries):

| Operation | Statements | Rows read | Rows written |
|---|---|---|---|
| `generateUploadUrl` (row + ticket) | 2 | 0 | 6 |
| proxy upload: ticket claim | 3 | 4 | 1 |
| `getMetadata` with finalize | 2 | 3 | 1 |
| `getUrl` (uploaded) | 1 | 1 | 0 |
| proxy download: row lookup | 1 | 1 | 0 |
| `store` | 1 | 1 | 4 |
| `delete` | 2 | 2 | 1 |

One RPC to the tenant object per proxy download and per proxy upload (two when a PUT fails). Rust ran the DDL on every
storage call; the kernel runs it once per object instance. Bundle: `kernel/files` plus `router/storage` is 38.3 KB
(10.7 KB gzip) unminified, 22.0 KB (8.0 KB gzip) minified.
