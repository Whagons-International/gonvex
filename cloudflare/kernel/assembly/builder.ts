// K19's release builder runs outside the 128 MiB Worker isolate, in a Container.
import { DurableObject } from "cloudflare:workers";
import { resident, residentHandler, stackJurisdiction } from "./residency";
interface Env {
  BUILDER: DurableObjectNamespace<Builder>;
  GONVEX_RELEASE_BUILDER_TOKEN: string;
  GONVEX_KERNEL_URL: string;
  GONVEX_KERNEL_SCRIPT: string;
  /** Release source SHA set by the release that deployed this builder and its image. */
  GONVEX_RUNTIME_VERSION?: string;
  /** deploy.mjs `stackOptions`: what the builder's runtime redeploys must keep (jurisdiction, backups, ...). */
  GONVEX_STACK_OPTIONS?: string;
  /** "1" when the image passes GONVEX_STACK_OPTIONS on (built from source that has it). */
  GONVEX_BUILDER_RESIDENCY?: string;
  GONVEX_JURISDICTION?: string;
  CLOUDFLARE_ACCOUNT_ID: string;
  CLOUDFLARE_API_TOKEN: string;
}
export function containerRequest(request: Request): Request {
  const upstream = new URL(request.url);
  upstream.protocol = "http:";
  return new Request(upstream, request);
}
class Builder extends DurableObject<Env> {
  override async fetch(request: Request): Promise<Response> {
    if (!this.env.GONVEX_KERNEL_SCRIPT.startsWith("gonvex-cf-dev-")) return new Response("development target required", { status: 503 });
    // Data residency: an image that predates GONVEX_STACK_OPTIONS would redeploy the runtime without the jurisdiction.
    if (stackJurisdiction(this.env) && this.env.GONVEX_BUILDER_RESIDENCY !== "1") {
      return new Response("this stack has a jurisdiction and its release builder image cannot keep it; publish with a residency-aware builder image", { status: 503 });
    }
    const container = this.ctx.container;
    if (!container) return new Response("release builder Container is unavailable", { status: 503 });
    if (!container.running) {
      container.start({ enableInternet: true, env: {
        GONVEX_RELEASE_BUILDER_TOKEN: this.env.GONVEX_RELEASE_BUILDER_TOKEN,
        GONVEX_KERNEL_URL: this.env.GONVEX_KERNEL_URL,
        GONVEX_KERNEL_SCRIPT: this.env.GONVEX_KERNEL_SCRIPT,
        GONVEX_KERNEL_GENERATED_DIR: "/gonvex/cloudflare/kernel/assembly/.generated",
        GONVEX_BUILDER_TARGET: "wrangler",
        GONVEX_SOURCE_VERSION: this.env.GONVEX_RUNTIME_VERSION ?? "",
        GONVEX_STACK_OPTIONS: this.env.GONVEX_STACK_OPTIONS ?? "",
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
    // Container TCP fetchers speak HTTP even when the public/service request is HTTPS.
    return port.fetch(containerRequest(request));
  }
}
/** Data residency: restricted to GONVEX_JURISDICTION when the stack has one (residency.ts). */
export const ReleaseBuilderContainer = resident(Builder);
export type ReleaseBuilderContainer = Builder;
export default residentHandler({ fetch: (request: Request, env: Env) => env.BUILDER.getByName("builder").fetch(request) });
