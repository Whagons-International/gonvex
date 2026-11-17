// K19's release builder runs outside the 128 MiB Worker isolate, in a Container.
import { DurableObject } from "cloudflare:workers";
interface Env {
  BUILDER: DurableObjectNamespace<ReleaseBuilderContainer>;
  GONVEX_RELEASE_BUILDER_TOKEN: string;
  GONVEX_KERNEL_URL: string;
  GONVEX_KERNEL_SCRIPT: string;
  CLOUDFLARE_ACCOUNT_ID: string;
  CLOUDFLARE_API_TOKEN: string;
}
export class ReleaseBuilderContainer extends DurableObject<Env> {
  override async fetch(request: Request): Promise<Response> {
    if (!this.env.GONVEX_KERNEL_SCRIPT.startsWith("gonvex-cf-dev-")) return new Response("development target required", { status: 503 });
    const container = this.ctx.container;
    if (!container) return new Response("release builder Container is unavailable", { status: 503 });
    if (!container.running) {
      container.start({ enableInternet: true, env: {
        GONVEX_RELEASE_BUILDER_TOKEN: this.env.GONVEX_RELEASE_BUILDER_TOKEN,
        GONVEX_KERNEL_URL: this.env.GONVEX_KERNEL_URL,
        GONVEX_KERNEL_SCRIPT: this.env.GONVEX_KERNEL_SCRIPT,
        GONVEX_KERNEL_WRANGLER_CONFIG: "/gonvex/cloudflare/wrangler.jsonc",
        GONVEX_KERNEL_GENERATED_DIR: "/gonvex/cloudflare/kernel/assembly/.generated",
        GONVEX_BUILDER_TARGET: "wrangler",
        CLOUDFLARE_ACCOUNT_ID: this.env.CLOUDFLARE_ACCOUNT_ID,
        CLOUDFLARE_API_TOKEN: this.env.CLOUDFLARE_API_TOKEN,
      } });
      this.ctx.waitUntil(container.monitor().catch(() => undefined));
    }
    const port = container.getTcpPort(8790);
    const start = Date.now();
    for (;;) {
      try { if ((await port.fetch("http://builder/healthz")).ok) break; } catch { /* cold Container */ }
      if (Date.now() - start > 60_000) return new Response("release builder did not start", { status: 503 });
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    return port.fetch(request);
  }
}
export default { fetch: (request: Request, env: Env) => env.BUILDER.getByName("builder").fetch(request) };
