# Firebase Auth Emulator in disposable tests

The normal runtime accepts signed RS256 Firebase ID tokens. Release binaries
never include support for unsigned emulator tokens, even when the feature is
selected. This does not change project Account, session or Member admission.

To run an isolated integration test, build the debug runtime with
`cargo build -p gonvex-runtime --features test-firebase-auth-emulator` and set:

- `GONVEX_TEST_AUTH_EMULATOR=true`
- `GONVEX_ENV=test`
- `GONVEX_ADDR=127.0.0.1:8080`
- `FIREBASE_AUTH_EMULATOR_HOST=127.0.0.1:9099`

Configure a Firebase realm with a `demo-*` project, its matching audience and
`https://securetoken.google.com/<demo-project>` issuer. Do not configure Firebase
Admin credentials on that realm. The test decoder requires unsigned `alg:none`
tokens with an empty signature. It validates expiration, issuer, audience, subject,
issued-at, auth-time and tenant claims, then uses normal canonical identity and
Member admission. Wrong environments, non-loopback hosts, real projects and
Admin credentials fail closed.

Production builds use `cargo build --release` and cannot enable this decoder
through environment variables. The feature is also absent from default debug
builds. Runtime regression tests cover default rejection and opt-in claim/guard
validation. The emulator has no production credential and requires no remote
Firebase sign-in or Google JWKS request.
