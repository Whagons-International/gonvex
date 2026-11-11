// The visibility port of the query engine (K08, `kernel/query/visibility.ts`):
// the functions `live_query.rs` calls, over manifest plans as JSON. Typed
// structurally, so this package does not import K08.

import { readClock, type LogChange } from "./feed.ts";
import { get, scalar, type JsonValue } from "./json.ts";
import { rowMatches } from "./mask.ts";
import { dependencyColumns, inheritedTables, inheritsAny, parsePlan, type VisibilityPlan } from "./plan.ts";
import {
  ancestorDerivedChangeConcerns,
  ancestorRowChangeConcerns,
  derivedChangeConcerns,
  grantChangeConcerns,
  parentAdmits,
  principalChangeConcerns,
  resolveMember,
  resolveRelatedQuery,
  resolveWithParents,
  type ResolutionMetrics,
  type VisibilitySession,
} from "./principal.ts";
import type { CatalogSql } from "./registry.ts";
import type { ResolvedVisibility } from "./resolved.ts";
import { compilePredicate } from "./sql.ts";
import type { VisibilityRegistry } from "./tenant.ts";

/** What K08 passes: its open K01 invocation, the session, the module's plans (manifest JSON) and the snapshot revision. */
export interface QueryVisibilityContext {
  readonly db: CatalogSql;
  readonly session: VisibilitySession;
  readonly plans: ReadonlyMap<string, unknown>;
  readonly revision: number;
}

const parsedPlans = new WeakMap<object, VisibilityPlan>();
const parsedMaps = new WeakMap<object, ReadonlyMap<string, VisibilityPlan>>();

/** A manifest plan, parsed once per object. */
export function planOf(raw: unknown): VisibilityPlan {
  if (typeof raw !== "object" || raw === null) return parsePlan(raw);
  let plan = parsedPlans.get(raw);
  if (!plan) parsedPlans.set(raw, (plan = parsePlan(raw)));
  return plan;
}

/** The module's plans, parsed once per map. */
export function plansOf(raw: ReadonlyMap<string, unknown>): ReadonlyMap<string, VisibilityPlan> {
  let plans = parsedMaps.get(raw);
  if (!plans) parsedMaps.set(raw, (plans = new Map([...raw].map(([table, plan]) => [table, planOf(plan)]))));
  return plans;
}

/** `mask_visibility_change_relevant` (live_query.rs): false only when `change` cannot alter what this member sees through `plan`. */
export function maskChangeRelevant(plan: VisibilityPlan, resolved: ResolvedVisibility, change: LogChange): boolean {
  const oldRow = change.oldValue;
  const newRow = change.newValue;
  if (change.table === plan.table) return [oldRow, newRow].some((row) => row !== null && rowMatches(plan, resolved, row));
  if (principalChangeConcerns(plan, resolved, change.table, change.operation, change.changedColumns, oldRow, newRow)) return true;
  if (grantChangeConcerns(plan, resolved, change.table, oldRow, newRow)) return true;
  if (derivedChangeConcerns(plan, resolved, change.table, change.rowId, oldRow, newRow) || ancestorDerivedChangeConcerns(resolved, change.table, change.rowId, oldRow, newRow)) return true;
  for (const expression of inheritsAny(plan)) {
    const via = expression.via;
    const parent = resolved.inherited.get(expression.table);
    if (!via || !parent || via.table !== change.table) continue;
    for (const link of [oldRow, newRow]) {
      const value = get(link, via.parentColumn);
      if (value === undefined || value === null) continue;
      const row = parent.row(scalar(value));
      if (row !== undefined && parentAdmits(plan, resolved, expression.table, parent, row)) return true;
    }
  }
  if (ancestorRowChangeConcerns(resolved, change.table, oldRow, newRow)) return true;
  const parent = resolved.inherited.get(change.table);
  if (parent) return [oldRow, newRow].some((row) => row !== null && rowMatches(parent.plan, parent.resolved, row));
  return false;
}

/** Tables whose writes can change what `plan` admits for some member: its sets' tables, `members`, and its parents' (recursively). */
export function dependencyTables(plan: VisibilityPlan, plans: ReadonlyMap<string, VisibilityPlan>): Set<string> {
  const tables = new Set<string>();
  const seen = new Set<string>();
  const visit = (current: VisibilityPlan) => {
    if (seen.has(current.table)) return;
    seen.add(current.table);
    for (const table of dependencyColumns(current).tables()) tables.add(table);
    for (const expression of inheritsAny(current)) if (expression.via) tables.add(expression.via.table);
    for (const table of inheritedTables(current)) {
      tables.add(table);
      const parent = plans.get(table);
      if (parent) visit(parent);
    }
  };
  visit(plan);
  return tables;
}

/** K08's `QueryVisibility`, backed by the tenant object's registry. */
export function createQueryVisibility(registry: VisibilityRegistry, metrics?: ResolutionMetrics) {
  const resolve = (context: QueryVisibilityContext, raw: unknown): ResolvedVisibility => {
    const plan = planOf(raw);
    if (plan.version !== 2) return resolveMember(context.db, context.session, plan, context.revision, false, metrics);
    const plans = plansOf(context.plans);
    const acl = registry.acl.ensure(context.db, plans, [plan]);
    const clock = readClock(context.db);
    return resolveWithParents(context.db, context.session, plan, context.revision, plans, acl, metrics, { cache: registry.principals, clock });
  };
  return {
    resolve,
    resolveRelated(context: QueryVisibilityContext, raw: unknown, member: { readonly ephemeralMember: boolean; readonly direct: Readonly<Record<string, string>>; readonly fingerprint: string }): ResolvedVisibility {
      const plan = planOf(raw);
      if (plan.version === 2) return resolve(context, raw);
      return resolveRelatedQuery(plan, member as ResolvedVisibility);
    },
    compilePredicate(raw: unknown, resolved: { readonly direct: Readonly<Record<string, string>> }, rowAlias: string, parameters: unknown[]): string {
      return compilePredicate(planOf(raw), resolved as ResolvedVisibility, rowAlias, parameters as JsonValue[]);
    },
    dependencyTables(raw: unknown, plans: ReadonlyMap<string, unknown>): ReadonlySet<string> {
      return dependencyTables(planOf(raw), plansOf(plans));
    },
    changeConcerns(
      raw: unknown,
      resolved: { readonly direct: Readonly<Record<string, string>> },
      change: { readonly table: string; readonly rowId: string; readonly operation: string; readonly changedColumns: readonly string[]; readonly oldValue: unknown; readonly newValue: unknown },
    ): boolean {
      return maskChangeRelevant(planOf(raw), resolved as ResolvedVisibility, {
        table: change.table,
        rowId: change.rowId,
        operation: change.operation,
        changedColumns: change.changedColumns,
        oldValue: (change.oldValue ?? null) as JsonValue,
        newValue: (change.newValue ?? null) as JsonValue,
      });
    },
  };
}
