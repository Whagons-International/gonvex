/**
 * Dev-only HTTP upstream for the `actions` conformance scenario on a deployed backend.
 *
 * A deployed Worker cannot fetch the harness machine's 127.0.0.1:47812 (Cloudflare: `error code: 1003`).
 * This Worker serves the identical deterministic `upstream()` answers. Run the suite with
 * `GONVEX_CONFORMANCE_UPSTREAM_ORIGIN=https://<this worker's host>` so the shared fixture declares and fetches
 * this origin on every backend. It holds no state, secrets or bindings.
 */
import { upstream } from '../scenarios/actions-upstream.mjs';

export default { fetch: (request) => upstream(request) };
