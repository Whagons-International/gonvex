// Dev fixture controls only. These drive faults/inspection of the serving object;
// no alternate Query, Reducer, auth or delivery implementation lives here.
import { rpc } from "../session/rpc";
import type { AssemblyTenant } from "./tenant";
import type { BakedRelease } from "./releases";
// @ts-expect-error K19 release build alias.
import * as generated from "gonvex:releases";
interface Env {
  TENANT: DurableObjectNamespace<any>;
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
  WORKER_VERSION?: WorkerVersionMetadata;
}
type FixtureTenant = Pick<AssemblyTenant, "fixtureEpoch" | "fixtureRestart" | "fixtureChangeLog" | "pruneReceipts" | "deliveryStats">;
export async function fixtureControl(request: Request, env: Env): Promise<Response | null> {
  const url = new URL(request.url);
  if (!url.pathname.startsWith("/__assembly/")) return null;
  if (!env.GONVEX_ASSEMBLY_FIXTURE_KEY || request.headers.get("authorization") !== `Bearer ${env.GONVEX_ASSEMBLY_FIXTURE_KEY}`) return new Response(null, { status: 404 });
  if (url.pathname === "/__assembly/version") {
    const releases = (generated as { releases: Record<string, BakedRelease> }).releases;
    return Response.json({ version: (generated as { version?: string }).version ?? null, worker: env.WORKER_VERSION ?? null, releases: Object.fromEntries(Object.entries(releases).map(([id, release]) => [id, release.hash])) });
  }
  if (request.method === "GET" && url.pathname === "/__assembly/changelog") return Response.json(await rpc<FixtureTenant>(env.TENANT, url.searchParams.get("tenant") ?? "").fixtureChangeLog(Math.max(0, Number(url.searchParams.get("offset") ?? 0))));
  if (request.method === "POST") {
    const body = await request.json<{ tenant: string; epoch?: string; now?: number; offlineMaxAgeMs?: number | null }>();
    const tenant = rpc<FixtureTenant>(env.TENANT, body.tenant);
    if (url.pathname === "/__assembly/epoch") await tenant.fixtureEpoch(body.epoch ?? "");
    else if (url.pathname === "/__assembly/tenant-restart") await tenant.fixtureRestart().catch(() => undefined);
    else if (url.pathname === "/__assembly/receipt-prune") return Response.json(await tenant.pruneReceipts(body.now, true, body.offlineMaxAgeMs));
    else return new Response(null, { status: 404 });
    return Response.json({ ok: true });
  }
  return new Response(null, { status: 404 });
}
