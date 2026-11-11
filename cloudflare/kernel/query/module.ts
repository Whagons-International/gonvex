/**
 * What the query engine reads from the active module generation: function
 * definitions and visibility plans, as `modules.rs` builds them from the
 * `/dev/sync` manifest (functions keyed by path; the plan is
 * `dependencies.liveQueryPlan`; visibility keyed by table). K19 owns loading;
 * `moduleFromManifest` is the adapter from the manifest JSON.
 */
import { rustDebug } from "./json";
import { validatePlan, type LiveQueryPlan } from "./plan";
import type { VisibilityPlan } from "./visibility";

export interface QueryFunction {
  readonly kind: string;
  readonly internal: boolean;
  readonly public: boolean;
  /** "" (absent), "oneShot", "live" or "replica". */
  readonly delivery: string;
  readonly liveQueryPlan: LiveQueryPlan | null;
  readonly replica: unknown;
  readonly argsSchema: unknown;
}

export interface QueryModule {
  readonly projectId: string;
  readonly artifactHash: string;
  readonly functions: ReadonlyMap<string, QueryFunction>;
  /** Visibility plans by table. */
  readonly visibility: ReadonlyMap<string, VisibilityPlan>;
}

interface ManifestFunction {
  kind?: string;
  internal?: boolean;
  public?: boolean;
  delivery?: string;
  replica?: unknown;
  args?: unknown;
  dependencies?: { liveQueryPlan?: LiveQueryPlan } | null;
}

export interface Manifest {
  functions?: Record<string, ManifestFunction>;
  visibility?: Record<string, VisibilityPlan>;
  module?: { hash?: string; functions?: Record<string, ManifestFunction>; visibility?: Record<string, VisibilityPlan> };
}

/** The query view of a deploy manifest. Plans are validated as Rust validates them at load (`modules.rs` 889). */
export function moduleFromManifest(projectId: string, manifest: Manifest, artifactHash = manifest.module?.hash ?? ""): QueryModule {
  const functions = new Map<string, QueryFunction>();
  for (const [path, fn] of Object.entries(manifest.module?.functions ?? manifest.functions ?? {})) {
    const plan = fn.dependencies?.liveQueryPlan ?? null;
    if (plan) {
      try { validatePlan(plan); } catch (error) {
        throw new Error(`invalid module artifact: function ${rustDebug(path)}: ${(error as Error).message}`);
      }
    }
    functions.set(path, {
      kind: fn.kind ?? "",
      internal: fn.internal === true,
      public: fn.public === true,
      delivery: fn.delivery ?? "",
      liveQueryPlan: plan,
      replica: fn.replica ?? null,
      argsSchema: fn.args ?? { kind: "object", fields: {} },
    });
  }
  const visibility = new Map(Object.entries(manifest.module?.visibility ?? manifest.visibility ?? {}));
  return { projectId, artifactHash, functions, visibility };
}
