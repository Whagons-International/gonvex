Browser starter qualification fixtures

The optional `@gonvex/client/starter-browser` export provides
`await createBrowserStarterInflater({signal?, maxInputBytes?})`. It returns
`undefined` if the actual native raw-DEFLATE EOF probe fails. Pass the returned
codec as `starterInflater` only with the existing explicit `replicaStarterFiles`
opt-in and durable storage readiness checks. Importing it activates nothing.

Integration requires the owner's SDK workspace-charge patch
42b872cac7103b78840ee6225a93d8a278765a48 (64KiB fixed workspace per pending
stage). The frozen 3a codec branch alone does not qualify SDK aggregate accounting.
The existing stage retains raw*12, encoded*8 and fixed workspace until settlement;
these are conservative JS/native buffer estimates, not an engine RSS guarantee.
The preflight uses three reusable canonical Huffman tables (under 4KiB), counts
output without materializing backreferences, and rejects invalid/oversized input
before constructing a native decoder. All native output plus the returned buffer
is bounded by the preflight's predicted output, at most 256KiB. No concatenation
of output chunks is used. Native internal allocator metadata remains opaque.

`codec-fixture.mjs` runs unchanged inside a browser or Node Web APIs.
`qualify.mjs` generates actual Node level1/window12/SyncFlush pieces, validates
restored+FINAL bytes against the Node oracle, and tests decoder/limit failures.
Set STARTER_BROWSER_ROOT to an owned artifact directory containing a browser ESM
bundle named codec.mjs; run `node qualify.mjs`. No dependencies are installed.
A static localhost page can import codec-fixture.mjs and pass corpus.json to
`qualify`. Retain the exact bundle hash, corpus, native browser version and result.

`live-fixture.mjs` bundles against the candidate client dist. `live-runner.mjs`
implements the original ignored Rust starter_live_sdk test's stdin/stdout contract
in native Chromium and IndexedDB. Set STARTER_BROWSER_ROOT,
STARTER_BROWSER_PLAYWRIGHT (absolute installed module path),
STARTER_SDK_CLIENT_ARTIFACT and STARTER_LIVE_SDK_RUNNER; run the owner's
wrapper-enabled ignored Rust test. The browser launches with --disable-gpu and
closes in finally. All authentication, certificates, source checks, SQL mutations,
row-scope revocation, and XOR resume assertions remain owned by the real Rust
fixture. The runner creates only synthetic localhost pages and a fresh browser
context. It checks no visible or durable starter checkpoint before final ready,
reads the committed final cursor through an independent native IndexedDB storage
instance, verifies exact projected rows, and exercises normal resume and old-peer
fallback. It never manufactures readiness or authorization booleans.

Format references:
- https://www.rfc-editor.org/rfc/rfc7692#section-7.2.2
- https://www.rfc-editor.org/rfc/rfc1951#section-3.2
- https://compression.spec.whatwg.org/#decompression-stream

The final empty stored block is appended only after preflight verifies that the
restored four bytes are exactly the last empty non-final stored block's LEN/NLEN.
A native decompression API alone cannot enforce maxOutputBytes: the Compression
Standard permits decompressing an entire input chunk before enqueueing output.
