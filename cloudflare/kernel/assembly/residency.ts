// Data residency: one stack-level jurisdiction for every Durable Object the stack creates.
//
// `GONVEX_JURISDICTION` (a plain var set by deploy.mjs) names the jurisdiction. When it is set, every Durable Object
// namespace binding a Worker or object of the stack receives is replaced, once, at the entry point, by
// `namespace.jurisdiction(<j>)`. Call sites keep writing `env.TENANT.getByName(id)`: the namespace they see is already
// restricted, so no path can create or reach an object of the stack outside the jurisdiction. The entry points are the
// exported classes and default handlers of the five Workers (src/index.ts and kernel/assembly/{runtime,actions,
// sandbox,builder}.ts); each export goes through `resident` or `residentHandler` below.
//
// Fail closed: an unknown jurisdiction value throws on first use instead of silently placing objects in the default
// location, and a resident object whose own id carries another jurisdiction refuses to construct.
// See docs/cloudflare/data-residency.md.

/** Jurisdictions a whole stack may use: both Durable Objects and R2 support them. */
export const STACK_JURISDICTIONS = ["eu", "fedramp"] as const;
export type StackJurisdiction = typeof STACK_JURISDICTIONS[number];

export interface ResidencyEnv { GONVEX_JURISDICTION?: string }

/** The stack's jurisdiction, or `null` when the stack has none (objects are placed near their first request). */
export function stackJurisdiction(env: unknown): StackJurisdiction | null {
  const raw = (env as ResidencyEnv | null | undefined)?.GONVEX_JURISDICTION;
  if (raw === undefined || raw === null || raw === "") return null;
  if (!(STACK_JURISDICTIONS as readonly string[]).includes(raw)) {
    throw new Error(`GONVEX_JURISDICTION ${JSON.stringify(raw)} is not one of ${STACK_JURISDICTIONS.join(", ")}`);
  }
  return raw as StackJurisdiction;
}

/** Service bindings (RPC ones answer any member with a callable) are never namespaces; see origin.ts `isFetcher`. */
function isFetcher(value: unknown): boolean {
  return typeof value === "object" && value !== null && typeof (value as Fetcher).fetch === "function";
}

function isNamespace(value: unknown): value is DurableObjectNamespace {
  return typeof value === "object" && value !== null && !isFetcher(value)
    && typeof (value as DurableObjectNamespace).idFromName === "function"
    && typeof (value as DurableObjectNamespace).get === "function"
    && typeof (value as DurableObjectNamespace).jurisdiction === "function";
}

const RESIDENT = Symbol.for("gonvex.residency.env");
const cache = new WeakMap<object, object>();

/**
 * `env` with every Durable Object namespace restricted to the stack's jurisdiction. The same `env` object always maps
 * to the same copy. Without a jurisdiction it is `env` itself.
 */
export function residentEnv<E>(env: E): E {
  if (typeof env !== "object" || env === null) return env;
  if ((env as Record<PropertyKey, unknown>)[RESIDENT]) return env;
  const jurisdiction = stackJurisdiction(env);
  if (!jurisdiction) return env;
  const known = cache.get(env as object);
  if (known) return known as E;
  const out: Record<PropertyKey, unknown> = { ...(env as Record<string, unknown>) };
  for (const [key, value] of Object.entries(out)) {
    if (isNamespace(value)) out[key] = value.jurisdiction(jurisdiction);
  }
  Object.defineProperty(out, RESIDENT, { value: jurisdiction, enumerable: false });
  cache.set(env as object, out);
  return out as E;
}

/** The jurisdiction an id carries (`undefined` when the runtime does not expose it). */
export function idJurisdiction(id: DurableObjectId | undefined): string | null | undefined {
  if (!id) return undefined;
  return "jurisdiction" in id ? (id.jurisdiction ?? null) : undefined;
}

/** Throws when an object of a jurisdiction stack was reached through an unrestricted namespace. */
export function assertResidentObject(ctx: unknown, env: unknown): void {
  const jurisdiction = stackJurisdiction(env);
  if (!jurisdiction) return;
  const id = (ctx as { id?: DurableObjectId } | null)?.id;
  const actual = idJurisdiction(id);
  // A WorkerEntrypoint/Workflow has no id; an id the runtime does not label cannot be checked here.
  if (actual === undefined) return;
  if (actual !== jurisdiction) throw new Error(`object ${id!.toString().slice(0, 12)}… is outside the stack jurisdiction ${jurisdiction} (${actual ?? "none"})`);
}

/** What `GET /_gonvex/residency` reports for one object, measured inside it. */
export interface ObjectResidency {
  id: string;
  jurisdiction: string | null;
  stackJurisdiction: string | null;
  /** From https://cloudflare.com/cdn-cgi/trace fetched by the object: where it runs at this moment. */
  colo: string | null;
  loc: string | null;
}

/** Fetched from inside an object (subrequest), so `colo` is the data centre the object runs in. */
export async function measureResidency(ctx: { id?: DurableObjectId }, env: unknown): Promise<ObjectResidency> {
  let colo: string | null = null;
  let loc: string | null = null;
  try {
    const trace = await (await fetch("https://cloudflare.com/cdn-cgi/trace", { signal: AbortSignal.timeout(5_000) })).text();
    colo = /^colo=(.+)$/m.exec(trace)?.[1] ?? null;
    loc = /^loc=(.+)$/m.exec(trace)?.[1] ?? null;
  } catch { /* evidence only */ }
  return { id: ctx.id?.toString() ?? "", jurisdiction: idJurisdiction(ctx.id) ?? null, stackJurisdiction: stackJurisdiction(env), colo, loc };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Constructor = abstract new (...args: any[]) => object;

/**
 * The exported form of a Durable Object, WorkerEntrypoint or WorkflowEntrypoint class: it receives `residentEnv(env)`
 * and refuses an id outside the stack jurisdiction. Every object also answers `gonvexResidency()` (evidence).
 * The class keeps its name; statics are inherited.
 */
export function resident<C extends Constructor>(Base: C): C {
  // @ts-expect-error A mixin over an abstract constructor with any signature; the platform calls it (ctx, env, ...).
  class Resident extends Base {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    constructor(...args: any[]) {
      const [ctx, env, ...rest] = args;
      assertResidentObject(ctx, env);
      super(ctx, residentEnv(env), ...rest);
    }
    async gonvexResidency(): Promise<ObjectResidency> {
      const self = this as unknown as { ctx?: { id?: DurableObjectId }; env?: unknown };
      return measureResidency(self.ctx ?? {}, self.env);
    }
  }
  Object.defineProperty(Resident, "name", { value: Base.name });
  return Resident as unknown as C;
}

/** A default export (`fetch`, `queue`, `scheduled`, ...) whose handlers receive `residentEnv(env)`. */
export function residentHandler<H extends Record<string, unknown>>(handler: H): H {
  const out: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(handler)) {
    out[key] = typeof value === "function"
      ? (event: unknown, env: unknown, ...rest: unknown[]) => (value as (...a: unknown[]) => unknown).call(handler, event, residentEnv(env), ...rest)
      : value;
  }
  return out as H;
}
