/**
 * The K17 harness: K05's socket path (router, gateway objects) in front of
 * K06's control object and K07's tenant object, with service principals
 * wired through (`kernel/principals`). It serves the conformance backend
 * `cloudflare-principals` and the workerd specs in `../test`.
 *
 * Routes: `/ws`; the reference's seeding routes (admin key: `/dev/projects`,
 * `/dev/tenants`, `/dev/internal/e2e/members`; project key or admin key:
 * `/dev/sync`, `/dev/data/tables/{t}/rows`); and `GET /dev/manifest` with
 * the service-principal branch (`kernel/principals/manifest.ts`, the same
 * decision K19's operator route makes) and the project-key branch.
 *
 * Local and test only; never deployed.
 */
import { findPrincipal, parseServicePrincipals as parsePrincipals } from "../../../control/src/auth";
import { GatewayObjectBase, type GatewayBindings, type GatewayEnv } from "../../session/placement/gateway";
import { handleWebSocketUpgrade } from "../../session/router";
import type { ClientFrame, Json } from "../../session/decode";
import type {
  Impersonation, ModuleInfo, Outcome, Ports, RevalidationItem, ServiceAuthOutcome, SessionIdentity, SocketView, TenantRoute, TenantSession,
} from "../../session/ports";
import { type TenantRpc, rpc } from "../../session/rpc";
import type { ConnectionReport } from "../../presence/hub";
import type { ServiceControlOutcome } from "../frames";
import type { ServiceGrant } from "../grant";
import { MANIFEST_DENIED, manifestAccess, serviceManifestBody } from "../manifest";

export const CONTROL_NAME = "control";

export interface PrincipalsEnv extends GatewayEnv {
  GATEWAY: DurableObjectNamespace<any>;
  CONTROL: DurableObjectNamespace<any>;
  TENANT: DurableObjectNamespace<any>;
  GONVEX_ADMIN_KEY?: string;
  GONVEX_SERVICE_PRINCIPALS?: string;
}

/** The RPC surface of `PrincipalsControlObject` (typed through plain shapes, as K05's `rpc.ts` does). */
interface ControlRpc {
  sessionProject(projectId: string): Promise<Outcome<null>>;
  sessionIdentity(token: string, projectId: string | undefined): Promise<Outcome<SessionIdentity>>;
  sessionAuthenticate(token: string, projectId: string | undefined, tenant: string | undefined): Promise<Outcome<TenantSession>>;
  sessionImpersonation(token: string, projectId: string | undefined, tenant: string | undefined, connectionId: string): Promise<Outcome<Impersonation>>;
  sessionPublicTenant(projectId: string, tenant: string): Promise<Outcome<TenantRoute>>;
  sessionRevalidate(items: RevalidationItem[]): Promise<boolean[]>;
  sessionDispatch(socket: SocketView, frame: ClientFrame): Promise<string[]>;
  moduleInfo(projectId: string): Promise<ModuleInfo | null>;
  moduleEpoch(projectId: string, databaseUrl: string): Promise<string>;
  serviceAuthenticate(token: string, projectId: string | undefined, tenant: string | undefined): Promise<ServiceAuthOutcome>;
  serviceControl(grant: ServiceGrant, path: string, args: Json): Promise<ServiceControlOutcome>;
  devCreateProject(input: { name: string; databaseMode?: string }): Promise<{ projectId: string; projectKey: string }>;
  devSaveManifest(projectId: string, manifest: Record<string, unknown>): Promise<{ ok: true; functionCount: number }>;
  devCreateTenant(projectId: string, tenantId: string, name: string): Promise<{ tenantId: string }>;
  devE2eMember(input: Record<string, unknown>): Promise<Record<string, string>>;
  devProjectKey(projectId: string): Promise<string | null>;
  moduleManifest(projectId: string): Promise<Record<string, unknown> | null>;
}

interface HarnessTenantRpc extends Required<TenantRpc> {
  readTable(table: string): Promise<Record<string, unknown>[]>;
}

const control = (env: PrincipalsEnv) => rpc<ControlRpc>(env.CONTROL, CONTROL_NAME);
const tenant = (env: PrincipalsEnv, tenantId: string) => rpc<HarnessTenantRpc>(env.TENANT, tenantId);

/** The session layer's ports over K06 (control) and K07 (tenant), with K17's service calls. */
export function principalsPorts(env: PrincipalsEnv): Ports {
  return {
    control: {
      project: (projectId) => control(env).sessionProject(projectId),
      loadSessionIdentity: (token, projectId) => control(env).sessionIdentity(token, projectId),
      authenticateSession: (token, projectId, tenantId) => control(env).sessionAuthenticate(token, projectId, tenantId),
      authenticateImpersonation: (token, projectId, tenantId, connectionId) => control(env).sessionImpersonation(token, projectId, tenantId, connectionId),
      resolvePublicTenant: (projectId, tenantId) => control(env).sessionPublicTenant(projectId, tenantId),
      revalidate: (items) => control(env).sessionRevalidate(items),
      dispatch: (socket, frame) => control(env).sessionDispatch(socket, frame),
      authenticateService: (token, projectId, tenantId) => control(env).serviceAuthenticate(token, projectId, tenantId),
      serviceCall: (grant, path, args) => control(env).serviceControl(grant, path, args),
    },
    modules: {
      module: (projectId) => control(env).moduleInfo(projectId),
      epoch: (projectId, databaseUrl) => control(env).moduleEpoch(projectId, databaseUrl),
    },
    tenants: {
      dispatch: (socket, frame) => tenant(env, socket.tenant!.route.tenantId).dispatch(socket, frame),
      presenceLeave: (route, connectionId, host, previous?: ConnectionReport | null) => tenant(env, route.tenantId).presenceLeave(connectionId, host, previous ?? null),
      serviceCall: (grant, call) => tenant(env, grant.route.tenantId).serviceCall(grant, call),
    },
  };
}

export const principalsBindings: GatewayBindings = {
  ports: (env) => principalsPorts(env as PrincipalsEnv),
  tenantLink: (env, route) => {
    const t = tenant(env as PrincipalsEnv, route.tenantId);
    return { attachGateway: (name) => t.attachGateway(name), detachGateway: (name) => t.detachGateway(name) };
  },
};

/** The gateway object bound as `GATEWAY`. */
export class PrincipalsGateway extends GatewayObjectBase<PrincipalsEnv> {
  protected bindings() { return principalsBindings; }
}

const json = (body: unknown, status = 200) => Response.json(body, { status });

async function body(request: Request): Promise<Record<string, unknown>> {
  try {
    return (await request.json()) as Record<string, unknown>;
  } catch {
    return {};
  }
}

/** `GET /dev/manifest` (operations.rs `manifest`): the service-principal branch, then the project key. */
async function manifestRoute(request: Request, env: PrincipalsEnv, url: URL, principals: ReturnType<typeof parsePrincipals>): Promise<Response> {
  const project = (url.searchParams.get("project") ?? "").trim() || (request.headers.get("x-gonvex-project-id") ?? "").trim();
  if (!project) return json({ error: "project is required" }, 400);
  const access = manifestAccess((token) => findPrincipal(principals, token), request.headers.get("authorization"), project);
  if (access.kind === "denied") return json({ error: MANIFEST_DENIED }, 403);
  if (access.kind === "none") {
    const key = await control(env).devProjectKey(project);
    const bearer = (request.headers.get("authorization") ?? "").trim().replace(/^Bearer /, "");
    // The harness has no operator accounts: the project key or the admin key, else Rust's 401.
    if (!(key && bearer === key) && !(env.GONVEX_ADMIN_KEY && bearer === env.GONVEX_ADMIN_KEY)) {
      return json({ error: "dashboard sign-in or personal access token is required" }, 401);
    }
  }
  const manifest = await control(env).moduleManifest(project);
  if (!manifest) return json({ error: "project module is not installed" }, 404);
  const module = (manifest.module ?? {}) as { hash?: string; generation?: number };
  const deployed = { hash: String(module.hash ?? ""), generation: Number(module.generation ?? 0) };
  if (access.kind === "allowed") return json(serviceManifestBody(project, manifest.functions ?? {}, deployed));
  return json({ project, functions: manifest.functions ?? {}, schema: manifest.schema ?? null, visibility: manifest.visibility ?? {}, module: deployed });
}

let parsed: { source: string | undefined; principals: ReturnType<typeof parsePrincipals> } | undefined;

/** The harness Worker's fetch. */
export async function principalsFetch(request: Request, env: PrincipalsEnv): Promise<Response> {
  const url = new URL(request.url);
  if (url.pathname === "/" || url.pathname === "/healthz") return json({ ok: true, service: "gonvex-cloudflare-k17-harness" });
  if (url.pathname === "/ws") return handleWebSocketUpgrade(request, env as unknown as Parameters<typeof handleWebSocketUpgrade>[1]);
  if (!url.pathname.startsWith("/dev/")) return json({ error: "not found" }, 404);
  if (request.method === "GET" && url.pathname === "/dev/manifest") {
    const cached = parsed?.source === env.GONVEX_SERVICE_PRINCIPALS ? parsed : { source: env.GONVEX_SERVICE_PRINCIPALS, principals: parsePrincipals(env.GONVEX_SERVICE_PRINCIPALS) };
    parsed = cached;
    return manifestRoute(request, env, url, cached.principals);
  }

  const bearer = (request.headers.get("authorization") ?? "").replace(/^Bearer\s+/i, "");
  const admin = !!env.GONVEX_ADMIN_KEY && bearer === env.GONVEX_ADMIN_KEY;
  const projectKey = async (project: string) => !!project && (await control(env).devProjectKey(project)) === bearer;
  try {
    if (request.method === "POST" && url.pathname === "/dev/sync") {
      const manifest = await body(request);
      const project = String(manifest.project ?? "");
      if (!admin && !(await projectKey(project))) return json({ error: "invalid Gonvex sync key" }, 401);
      const saved = await control(env).devSaveManifest(project, manifest);
      return json({ ok: true, project, functionCount: saved.functionCount, schemaDefinition: manifest.schema ?? null, runtimeReloaded: true });
    }
    const rows = /^\/dev\/data\/tables\/([^/]+)\/rows$/.exec(url.pathname);
    if (request.method === "GET" && rows) {
      const project = url.searchParams.get("project") ?? "";
      if (!admin && !(await projectKey(project))) return json({ error: "project access is required" }, 403);
      const all = await tenant(env, url.searchParams.get("tenant") ?? "").readTable(decodeURIComponent(rows[1]!));
      return json({ rows: all, total: all.length });
    }
    if (!admin) return json({ error: "runtime admin key is required" }, 403);
    if (request.method === "POST" && url.pathname === "/dev/projects") {
      const input = await body(request);
      const created = await control(env).devCreateProject({ name: String(input.name ?? "project"), databaseMode: String(input.databaseMode ?? "multiTenant") });
      return json({ project: { id: created.projectId }, projectKey: created.projectKey });
    }
    if (request.method === "POST" && url.pathname === "/dev/tenants") {
      const input = await body(request);
      const created = await control(env).devCreateTenant(String(input.projectId), String(input.id), String(input.name ?? input.id));
      return json({ id: created.tenantId });
    }
    if (request.method === "POST" && url.pathname === "/dev/internal/e2e/members") {
      return json(await control(env).devE2eMember(await body(request)));
    }
  } catch (error) {
    return json({ error: error instanceof Error ? error.message : String(error) }, 500);
  }
  return json({ error: "not found" }, 404);
}
