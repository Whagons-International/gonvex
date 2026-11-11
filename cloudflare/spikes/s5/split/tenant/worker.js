// Tenant-object script for the split-deploy probe. /dev/sync would deploy only this script.
import { DurableObject } from "cloudflare:workers";

export class Tenant extends DurableObject {
  constructor(ctx, env) { super(ctx, env); this.inst = crypto.randomUUID().slice(0, 8); }
  run(n) { return { build: this.env.BUILD, inst: this.inst, n }; }
}
export default { fetch() { return new Response("tenant script", { status: 404 }); } };
