# 0049: Batch-C re-review (K15, K16, K18, K21)

Date: 2026-11-12. Status: decided (lead).

| PR | Result | Notes |
|---|---|---|
| K18 (#368), note 0043 | merged | The tenant cache is keyed on the exact selector, the selector rate limit comes before lookup, the negative cache is capped, and lookups have a per-project budget. Its edits to K06, K17 and K07 that carry the principal's configured channel are accepted (as in 0044). Webhook Actions answer 503 until K22 composes K12 into the tenant object. |
| K21 (#366), note 0040 | merged as **part 1** | The kernel run is reproducible from committed files. `bridge-upstream` matches Rust through sign-in, tenant auth, keyed reducer replay, refresh, re-auth and ping. `TestBridgeE2E` needs a Firebase-mode project, a seeded Whagons tenant and K10: **V02**. The package stays open. |
| K16 (#367), note 0042 | approved | One deadline rule (K12's). `public-calls` passes Rust vs kernel through a test harness. The limiter fails closed on `admit` (503 `connection limiter is unavailable`) and persists its slots. The idempotency-key claim is removed. |
| K15 (#369), note 0041 | approved | The live-session leak with presence off is fixed (`live` flag), `__proto__` keys are kept, and references are fixed. |

## Rulings

1. **Test harness backends.** `cloudflare-public` (K16) and `cloudflare-principals` (K17) compose merged packages
   for tests only. K22 replaces them with the real assembly and must not add a third composition.
2. **Load-dependent flakes.** K16 saw one `service-principals` failure while the machine was shared by several
   suites. V01 runs the full conformance suite on an otherwise idle runner, and repeats any failing scenario
   3 times before reporting it as a defect.
3. **New `limits` writes.** The public limiter writes 2–3 rows per socket into its own object. Accepted:
   persistent admission state, not ephemeral data (same basis as K05's routing KV entry, 0022 §4.5).
