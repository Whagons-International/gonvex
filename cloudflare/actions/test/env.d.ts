// Bindings of test/wrangler.jsonc.
declare namespace Cloudflare {
  interface Env {
    TENANT: DurableObjectNamespace<import("./worker.js").TestTenant>;
    ACTION_WORKFLOW: Workflow<import("../src/calls.js").CallJob | import("../src/handoff.js").HandoffJob>;
    ACTION_STEP_TIMEOUT: string;
    ACTION_STEP_RETRY_DELAY: string;
  }
}
interface Env extends Cloudflare.Env {}
declare module "@conformance-scenarios/actions-upstream.mjs" {
  export const UPSTREAM_ORIGIN: string;
  export function upstream(request: Request): Promise<Response>;
}
