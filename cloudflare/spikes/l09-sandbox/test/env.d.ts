// Bindings of test/wrangler.jsonc.
declare namespace Cloudflare {
  interface Env {
    SANDBOX_WORKSPACE: DurableObjectNamespace<import("../src/workspace").SandboxWorkspace>;
    SANDBOX_ANALYSIS: DurableObjectNamespace<import("../src/analysis").SandboxAnalysis>;
    SANDBOX_REGISTRY: DurableObjectNamespace<import("../../../sandbox/src/registry").SandboxRegistry>;
    SANDBOX_SLOTS: DurableObjectNamespace<import("../src/slots").RunSlots>;
    LOADER: WorkerLoader;
    FILES: R2Bucket;
  }
}
interface Env extends Cloudflare.Env {}
