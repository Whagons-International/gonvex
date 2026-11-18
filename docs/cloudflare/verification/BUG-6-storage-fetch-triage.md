# BUG-6 triage: page `fetch()` of a signed /storage URL fails on CF dev

Walkthrough 2026-11-17 (report file `BUG-5-storage-browser-fetch-fails.md`, renamed BUG-6 because BUG-5 is now a
different issue). Backend 858f19e0, tenants devcopy-stg1117-11 and -04, unmodified staging frontend 678bdc80.

## Verdict

The backend is not at fault, and its /storage CORS is at parity with Rust and staging. The failure comes from the
frontend's service worker, and the dev environment's image-proxy 404 sets it off. This PR changes no behavior. It adds
a regression test that pins the browser request shapes.

## What the browser sends and gets

`fetch(url)` from the app (`downloadFile`, `ImagePreviewModal`, `StoredFileMedia`) is a CORS-mode **simple GET**:
`Origin` from the app, no custom headers, so the browser **sends no preflight**. Measured 2026-11-18 ~01:40 UTC in
headless Chromium 148 (`--disable-gpu`):

| Context | Request | Result |
| --- | --- | --- |
| Fresh browser context, app origin | `fetch(validSignedUrl)` | 200, 845 bytes, ACAO `*` |
| Fresh context | `fetch(urlWithBadSig)` | 403 readable (ACAO `*`) |
| Logged-in app context | `fetch(validSignedUrl)`, also with `cache: 'no-store'` | `TypeError: Failed to fetch`, CDP `loadingFailed net::ERR_FAILED`, no response or ExtraInfo |
| Logged-in app context | same path, different `sig` | `Failed to fetch` |
| Logged-in app context | different path on the same host | 403, readable |
| Logged-in app context, CDP `Network.setBypassServiceWorker` | `fetch(validSignedUrl)` | **200** |

The app's Cache Storage `whagons-task-files-v1-<tenant>-<member>` held the data entry for that storage path as
`type: "opaque", status: 0`.

## Root cause

`public/firebase-messaging-sw.js` (whagons5-client 678bdc80) has a `fetch` handler. For any URL whose path contains
`/storage/`, it answers from a task-file cache keyed by origin and path only. The query (`exp`/`sig`) is not part of
the key. On a cache miss it calls `fetch(event.request)` and stores the result. `storeTaskFileResponse` accepts
`response.type === 'opaque'`.

1. On dev, the thumbnail `https://imgproxy.whagons.com/unsafe/rs:fit:320:320:0/<b64 dev URL>` returns 404. The staging
   image proxy cannot fetch the dev host. This is frontend environment configuration, not the backend.
2. The UI then loads the direct storage URL in an `<img>`. That is a no-cors request, so the SW stores an
   **opaque** response for the path.
3. Any later CORS `fetch()` of that path, with any signature, gets the opaque response through `respondWith`. A
   CORS request answered with an opaque response is a network error, which shows up as `Failed to fetch`.

The `OPTIONS → 403` in the report was a hand-made request. The browser never sends it for this GET. Rust
`storage_options` (rust/crates/runtime/src/lib.rs) returns 204 only for a valid upload signature (`upload=1`). The
kernel (`cloudflare/router/storage/index.ts`) does the same. Staging answered a read-only probe
`OPTIONS https://gonvex-stg.whagons.com/storage/p/t/x?exp=…&sig=x` with `Origin` and
`Access-Control-Request-Method: GET` with **403**, the same CORS headers (ACAO `*`, methods `GET, POST, PUT, OPTIONS`,
headers `Content-Type`, max-age 600) and the same body. A backend that answered 204 here would differ from the
reference and would fix nothing.

## Regression test

`cloudflare/kernel/files/test/files.test.ts`, "signed storage from a browser origin (BUG-6)":

- An upload preflight with `Origin`, `Access-Control-Request-Method: POST` and `-Headers: content-type` gets 204 with
  the storage CORS headers. The same preflight with a tampered signature gets 403.
- A cross-origin simple GET of a valid download URL gets 200, ACAO `*`, no Allow-Credentials, and the exact bytes.
  Tampered, empty or expired signatures on the actual GET get a readable 403 with ACAO `*`.
- A hand-made GET preflight on a download URL gets 403, as in Rust and staging.

## Follow-ups (outside this repository; not done here)

- Frontend (whagons5-client, at cutover): never store an opaque response in the task-file cache, or never serve one to
  a request whose `mode` is `cors`. Or bypass the cache when `event.request.mode !== 'no-cors'`. This bug also reaches
  staging and production whenever a direct `<img>` load of a storage path comes before a `fetch()` of the same path.
- Dev environment: let the image proxy fetch the dev storage host, or point the dev build at a proxy that can. The
  thumbnail fallback then stops loading storage URLs no-cors.
- Walkthrough: clear site data, or bypass the SW, before re-running files-download, because the opaque entries persist
  in the existing browser contexts.

## BUG-6b: image-proxy parity (deploy-agent change, not applied)

What the proxy is: the 678bdc80 build sets no `VITE_IMGPROXY_URL` (it is absent from the served public env).
`src/utils/imgproxy.ts` therefore uses its default, `https://imgproxy.whagons.com`. That is one shared imgproxy
instance (Whagons infra), and staging and dev builds use the same one. The client wraps any URL on the
`VITE_GONVEX_URL` host or any `/storage/` path as
`/unsafe/rs:fit:W:H:0/<b64 source>.avif`. The imgproxy allow-list (`IMGPROXY_ALLOWED_SOURCES`, mirrored in
`gridImageCache.ts`) admits `whagons.com`, `convex.cloud`, `convex.site`, Google storage and `novastone-ca.com`
hosts. Staging's storage origin `https://gonvex-stg.whagons.com` is allowed, so thumbnails work there. Dev's is
`https://gonvex-cf-dev-integration-20261117.whagons-international.workers.dev`. That host is not allowed, so
imgproxy refuses it with 404, the UI falls back to a direct no-cors `<img>`, and the SW caches the opaque
response. The allow-list comes from the client's mirror and the observed 404. The live imgproxy env was not read.

A kernel route cannot close this gap. imgproxy refuses the source by host, and the frontend may change only its
backend URL. The parity fix is to serve the dev backend on a `whagons.com` host, as staging does:

1. Cloudflare (whagons.com zone; needs lead/Gabriel approval, because the brief fences DNS and zones): attach a
   Worker Custom Domain `gonvex-cf-dev-integration.whagons.com` → the router Worker
   `gonvex-cf-dev-integration-20261117`. Keep `workers_dev` on.
2. `cloudflare/kernel/assembly/deploy.mjs`, `deployAssembly` (line ~94): make the public origin overridable
   instead of always using workers.dev:
   `const url = publicUrl ?? \`https://${name}.${subdomain.subdomain}.workers.dev\`;` with `publicUrl` taken from the
   deploy invocation (for example `GONVEX_CF_DEV_PUBLIC_URL=https://gonvex-cf-dev-integration.whagons.com`). It then
   flows to `GONVEX_PUBLIC_URL` (storage URLs, OAuth callback base) and `GONVEX_KERNEL_URL` through
   `servingConfigs`. Retain `GONVEX_STORAGE_SECRET` (no rotation).
3. Frontend dev build (backend URL only): `VITE_GONVEX_URL=https://gonvex-cf-dev-integration.whagons.com`,
   `VITE_GONVEX_WS_URL=wss://gonvex-cf-dev-integration.whagons.com/ws`. Leave `VITE_IMGPROXY_URL` unset, as on
   staging.
4. Verify: `getFileUrls` returns `https://gonvex-cf-dev-integration.whagons.com/storage/...`. The thumbnail
   `https://imgproxy.whagons.com/unsafe/rs:fit:320:320:0/<b64>.avif` returns 200 image/avif. In a fresh browser
   context (site data cleared), opening the note and then calling `fetch(signedUrl)` returns 200 with the exact
   bytes. The SW task-file cache holds no `opaque` entry for the path.

Risk: imgproxy and its CDN cache the transformed bytes by URL. The signed URL carries `exp`/`sig`, so each
signature makes a distinct cache entry, which matches staging. Private files are already protected by the owner
signature check on the source GET.
