// Dev fixture controls only. These drive faults/inspection of the serving object;
// no alternate Query, Reducer, auth or delivery implementation lives here.
import { rpc } from "../session/rpc";
import type { AssemblyTenant } from "./tenant";
import type { BakedRelease } from "./releases";
import type { AssemblyGateway } from "./gateway";
import { CONTROLLER_CELLS } from "../gateway/placement";
import { gatewayShards } from "../session/placement/gateway";
// @ts-expect-error K19 release build alias.
import * as generated from "gonvex:releases";
interface Env {
  TENANT: DurableObjectNamespace<any>;
  GATEWAY: DurableObjectNamespace<any>;
  GATEWAY_CONTROLLER?: DurableObjectNamespace<any>;
  GONVEX_GATEWAY_SHARDS?: string;
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
  WORKER_VERSION?: WorkerVersionMetadata;
}
type FixtureTenant = Pick<AssemblyTenant, "fixtureDeliveryState" | "fixtureFailRecoveryOnce" | "fixtureEpoch" | "fixtureRestart" | "fixtureChangeLog" | "pruneReceipts" | "deliveryStats">;
export async function fixtureControl(request: Request, env: Env): Promise<Response | null> {
  const url = new URL(request.url);
  if (!url.pathname.startsWith("/__assembly/")) return null;
  if (!env.GONVEX_ASSEMBLY_FIXTURE_KEY || request.headers.get("authorization") !== `Bearer ${env.GONVEX_ASSEMBLY_FIXTURE_KEY}`) return new Response(null, { status: 404 });
  if (url.pathname === "/__assembly/version") {
    const releases = (generated as { releases: Record<string, BakedRelease> }).releases;
    return Response.json({ version: (generated as { version?: string }).version ?? null, worker: env.WORKER_VERSION ?? null, releases: Object.fromEntries(Object.entries(releases).map(([id, release]) => [id, release.hash])) });
  }
  if(request.method==="GET"&&url.pathname==="/__assembly/bootstrap-gate"){
    const holder=url.searchParams.get("holder");
    if(!holder||holder.length>128)return new Response("holder required",{status:400});
    return Response.json(await rpc<Pick<AssemblyGateway,"fixtureBootstrapControl">>(env.GATEWAY,holder).fixtureBootstrapControl());
  }
  if (request.method === "GET" && url.pathname === "/__assembly/lifecycle") {
    const holder = url.searchParams.get("holder");
    if (!holder || holder.length > 128) return new Response("holder required", {status:400});
    return Response.json(await rpc<Pick<AssemblyGateway,"fixtureLifecycle">>(env.GATEWAY,holder).fixtureLifecycle());
  }
  if (request.method === "GET" && url.pathname === "/__assembly/holders") {
    const project = url.searchParams.get("project");
    if (!project) return new Response("project required", { status: 400 });
    const names = env.GATEWAY_CONTROLLER
      ? (await Promise.all(Array.from({ length: CONTROLLER_CELLS }, (_, i) => rpc<{ status(): Promise<{ slots: { name: string; mode: string }[] }> }>(env.GATEWAY_CONTROLLER!, `gateway-cell-${i}`).status()))).flatMap((s) => s.slots.filter((slot) => slot.mode !== "retired").map((slot) => slot.name))
      : Array.from({ length: gatewayShards(env) }, (_, i) => `gw-${i}`);
    // At most eight concurrent inspection RPCs. This endpoint does not select
    // placements or offer traffic and exists only on an explicit dev fixture.
    const holders = [];
    for (let i = 0; i < names.length; i += 8) holders.push(...await Promise.all(names.slice(i, i + 8).map((name) => rpc<Pick<AssemblyGateway, "fixtureTelemetry">>(env.GATEWAY, name).fixtureTelemetry(project))));
    return Response.json({ placement: env.GATEWAY_CONTROLLER ? "automatic" : "static", holders: holders.filter((h) => h.sockets.length) });
  }
  if (request.method === "GET" && url.pathname === "/__assembly/changelog") return Response.json(await rpc<FixtureTenant>(env.TENANT, url.searchParams.get("tenant") ?? "").fixtureChangeLog(Math.max(0, Number(url.searchParams.get("offset") ?? 0))));
  if(request.method==='GET'&&url.pathname==='/__assembly/delivery-state')return Response.json(await rpc<FixtureTenant>(env.TENANT,url.searchParams.get('tenant')??'').fixtureDeliveryState());
  if (request.method === "POST") {
    const body = await request.json<{ tenant: string; holder?:string;hold?:boolean;phase?:'bootstrap'|'challenge';epoch?: string; now?: number; offlineMaxAgeMs?: number | null }>();
    if(url.pathname==="/__assembly/bootstrap-gate"){
      if(!body.holder||body.holder.length>128||typeof body.hold!=="boolean")return new Response("holder/hold required",{status:400});
      if(body.phase!==undefined&&body.phase!=='bootstrap'&&body.phase!=='challenge')return new Response('invalid gate phase',{status:400});
      return Response.json(await rpc<Pick<AssemblyGateway,"fixtureBootstrapControl">>(env.GATEWAY,body.holder).fixtureBootstrapControl(body.hold,body.phase));
    }
    const tenant = rpc<FixtureTenant>(env.TENANT, body.tenant);
    if (url.pathname === "/__assembly/recovery-fail-once") await tenant.fixtureFailRecoveryOnce();
    else if (url.pathname === "/__assembly/epoch") await tenant.fixtureEpoch(body.epoch ?? "");
    else if (url.pathname === "/__assembly/tenant-restart") await tenant.fixtureRestart().catch(() => undefined);
    else if (url.pathname === "/__assembly/receipt-prune") return Response.json(await tenant.pruneReceipts(body.now, true, body.offlineMaxAgeMs));
    else return new Response(null, { status: 404 });
    return Response.json({ ok: true });
  }
  return new Response(null, { status: 404 });
}
