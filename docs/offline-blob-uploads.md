# Design note: durable offline blob uploads

Status: proposal, not implemented.

## Problem

Reducer intents survive offline through the durable outbox, but file uploads
do not. `client.action()` has no queue, and uploads go through an Action
(`ctx.storage.generateUploadUrl`) followed by an HTTP PUT to object storage.
Offline, the Action fails at once, so an attachment created next to an offline
edit is lost, or the app has to build its own queue (Whagons mobile keeps
`offline_upload_queue` in a separate SQLite database and re-drives reducers
itself). The reducer that references the file (for example
`attachments.create({ storageId })`) cannot be queued either, because the
storage id does not exist until the upload finishes.

## Goals

- An attachment chosen offline is durable across restarts, like an intent.
- The reducer that references it is queued now, replays exactly once, and is
  never sent before its blob is stored.
- No new server-side trust: blobs are still admitted by a server-issued,
  identity-scoped upload grant.
- Queue visibility and control reuse the intent API (`listIntents`,
  `retryIntent`, `discardIntent`).

## Proposal

### 1. Client-generated blob ids

The client allocates `blobId = uuid()` when the file is picked and stores the
bytes locally (OPFS/IndexedDB on web, the app's document directory plus a
SQLite row on React Native). The reducer argument references the blob, not a
server storage id: `attachments.create({ file: { $blob: blobId } })`.

### 2. Blob queue next to the outbox

A new `BlobQueueStore` persists one row per blob:
`{ blobId, scope, localUri | bytesKey, contentType, size, sha256, state:
pending | uploading | stored | failed, attempts, lastError, storageId? }`.
It shares scope isolation, `listScopes`/`purgeScope`, and the retry policy
(`maxAttempts`, backoff, park as `failed`) with the reducer outbox. The
Expo SQLite store would add a `_gonvex_blobs` table beside `_gonvex_outbox`.

### 3. Dependencies instead of ordering tricks

An outbox entry gains `blobDependencies: string[]` (derived from `$blob`
markers in its args at enqueue time). `firstReady` treats an entry as not
ready until every dependency is `stored`, exactly like a backing-off entry:
it keeps its place in the causal chain, and a *parked* blob parks the entry
(failed, with the blob's error) instead of blocking the chain forever.
Discarding the intent discards blobs no other intent references.

### 4. Upload protocol

A new reserved Action-free runtime operation, `storage.upload.begin`
(`{ blobId, size, contentType, sha256 }`), returns a short-lived signed PUT
URL bound to the account, tenant, blobId and hash. It is idempotent per
`(account, blobId)`: a retry after a lost response returns the same pending
object key. After the PUT, `storage.upload.commit` verifies size and hash and
records `blobId -> storageId`. Because the grant is keyed by the client
blobId, a crash at any point resumes without orphaning a second object.

### 5. Resolving references at execution

When the reducer runs, the runtime resolves `{ $blob: blobId }` to the
committed `storageId` for the calling account, or rejects with
`class: "transient"` if the commit has not been observed yet (it cannot be
missing for a correct client, but this keeps a racing replay safe). Local
execution resolves the marker to a local preview URI so the optimistic row
can render the attachment immediately.

## Exactly-once and failure handling

- Intent: unchanged, idempotency key per reducer call.
- Blob: idempotent per `(account, blobId)`; re-PUT of the same bytes to the
  same key is harmless; commit is a compare-and-set on the recorded hash.
- Transient upload failures back off and park like reducer intents; a blob
  rejected by policy (size, type, quota) is `rejected` and rejects its
  dependent intent with the same error so the UI shows one failure.
- Orphans: blobs committed but never referenced by a committed reducer are
  garbage-collected server-side after a TTL.

## Open questions

- Byte storage quota on the device and eviction policy for large files.
- Resumable (chunked) uploads for large media on flaky mobile links.
- Whether `$blob` resolution belongs in the generic argument schema or in a
  dedicated `schema.blob()` type the code generator understands.
