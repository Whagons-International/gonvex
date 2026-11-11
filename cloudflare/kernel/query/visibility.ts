/**
 * What the query engine needs from visibility (K09). This mirrors the Rust
 * functions `live_query.rs` calls, so K09 can implement it directly:
 *
 * | Port method          | Rust (`visibility.rs`)                                             |
 * | -------------------- | ------------------------------------------------------------------ |
 * | `resolve`            | `resolve_with_parents` (version 2) / `resolve_for_query` (legacy)   |
 * | `resolveRelated`     | `resolve_with_parents` (version 2) / `resolve_related_query`       |
 * | `compilePredicate`   | `compile_predicate`                                                |
 * | `changeConcerns`     | optional: `mask_change_relevant`'s visibility half (v2 relevance)  |
 * | `dependencyTables`   | `VisibilityPlan::dependencies` (tables whose writes can change it) |
 *
 * Everything is synchronous: Durable Object SQL is synchronous and the
 * engine runs one plan inside one K01 invocation (one snapshot).
 *
 * Errors: throw an `Error` whose `message` is `VisibilityError`'s Display
 * text (`invalid visibility plan: …`, `active tenant member is required for
 * visibility`); the engine sends it verbatim, as Rust's `#[error(transparent)]`
 * does.
 *
 * K09 implements it over the tenant object's `VisibilityRegistry`
 * (`kernel/visibility/query-port.ts`); `kernelVisibility` below is the one
 * constructor the tenant object uses.
 */
import type { StorageSession } from "../storage";
import { createQueryVisibility, type VisibilityRegistry } from "../visibility";
import type { TenantSession } from "../session/ports";

/** A table's visibility plan as the manifest carries it (`VisibilityPlan`, camelCase). */
export interface VisibilityPlan {
  version?: number | null;
  permissionTables?: string[];
  table: string;
  key: string;
  sets?: Record<string, VisibilitySet>;
  where: VisibilityExpression;
}

export interface VisibilitySet {
  kind?: string;
  table: string;
  alias?: string;
  select: string;
  selectFrom?: string;
  joins?: VisibilityJoin[];
  where?: VisibilityConstraint[];
  from?: string;
  key?: string;
}
export interface VisibilityJoin { table: string; alias?: string; leftAlias?: string; leftColumn: string; rightColumn: string }
export interface VisibilityConstraint { table?: string; column: string; context?: string; value?: unknown; inSet?: string }
export interface VisibilityExpression {
  operator: string;
  column?: string;
  context?: string;
  set?: string;
  value?: unknown;
  children?: VisibilityExpression[];
  table?: string;
  key?: string;
  where?: VisibilityConstraint[];
  predicate?: VisibilityExpression | null;
  via?: { table: string; childColumn: string; parentColumn: string } | null;
  onMissing?: VisibilityExpression | null;
}

/**
 * A member's resolution of one plan at one snapshot (`ResolvedVisibility`).
 * The engine reads only these fields; implementations carry whatever else
 * they need (sets, parents, indexes) on the same object.
 */
export interface ResolvedVisibility {
  /** Verified developer admission without a persisted Member: no live membership check. */
  readonly ephemeralMember: boolean;
  /** `account.id`, `member.id`, `tenant.id`. */
  readonly direct: Readonly<Record<string, string>>;
  /** Identifies equal resolutions: subscriptions with equal plan, args and fingerprints share one execution. */
  readonly fingerprint: string;
}

/** One captured change, as the feed carries it (`change_feed::LogChange`). */
export interface LogChange {
  readonly table: string;
  readonly rowId: string;
  readonly operation: string;
  readonly changedColumns: readonly string[];
  readonly oldValue: Readonly<Record<string, unknown>> | null;
  readonly newValue: Readonly<Record<string, unknown>> | null;
}

export interface VisibilityContext {
  /** The open K01 invocation the query runs in (read-only). */
  readonly db: StorageSession;
  readonly session: TenantSession;
  /** Every visibility plan of the active module, by table. */
  readonly plans: ReadonlyMap<string, VisibilityPlan>;
  /** `_gonvex_sync_clock.revision` read in this snapshot. */
  readonly revision: number;
}

export interface QueryVisibility {
  resolve(context: VisibilityContext, plan: VisibilityPlan): ResolvedVisibility;
  /** A plan reached through `inRelation`, for the same member and snapshot. */
  resolveRelated(context: VisibilityContext, plan: VisibilityPlan, member: ResolvedVisibility): ResolvedVisibility;
  /** SQL that admits a row aliased `rowAlias`; appends bound values to `parameters` and refers to them as `$N` (1-based). */
  compilePredicate(plan: VisibilityPlan, resolved: ResolvedVisibility, rowAlias: string, parameters: unknown[]): string;
  /** Tables whose writes can change what `plan` admits for some member (members, permission tables, set tables, parents). */
  dependencyTables(plan: VisibilityPlan, plans: ReadonlyMap<string, VisibilityPlan>): ReadonlySet<string>;
  /**
   * Optional finer relevance (Rust's v2 `mask_change_relevant`): false only
   * when `change` cannot alter what this member sees through `plan`. Without
   * it the engine reruns on every write to a dependency table, which sends
   * the same frames (an unchanged result sends nothing) at a higher cost.
   */
  changeConcerns?(plan: VisibilityPlan, resolved: ResolvedVisibility, change: LogChange): boolean;
}

/**
 * K09's implementation over the tenant object's registry. Create **one**
 * `VisibilityRegistry` per tenant object and share it with every query, so
 * the parent ACL indexes and the principal cache are shared. (K09 types the
 * port structurally without importing this package, hence the cast.)
 */
export function kernelVisibility(registry: VisibilityRegistry): QueryVisibility {
  return createQueryVisibility(registry) as unknown as QueryVisibility;
}
