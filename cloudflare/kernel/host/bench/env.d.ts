declare namespace Cloudflare {
  interface Env {
    TENANT: DurableObjectNamespace<import("./worker.js").BenchTenant>;
  }
}
interface Env extends Cloudflare.Env {}
