// Bindings of test/wrangler.jsonc.
declare namespace Cloudflare {
  interface Env {
    SANDBOX_CONTAINER: DurableObjectNamespace<import("../src/container").SandboxContainer>;
    SANDBOX_REGISTRY: DurableObjectNamespace<import("../src/registry").SandboxRegistry>;
    SANDBOX_CONTAINER_URL: string;
    TENANT: DurableObjectNamespace<import("./worker").FilesTenant>;
    FILES: R2Bucket;
  }
}
interface Env extends Cloudflare.Env {}
