// Bindings of test/wrangler.jsonc.
declare namespace Cloudflare {
  interface Env {
    TENANT: DurableObjectNamespace<import("./worker.js").TestTenant>;
  }
}
interface Env extends Cloudflare.Env {}
