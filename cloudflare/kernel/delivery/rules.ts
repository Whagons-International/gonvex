// Change relevance rules shared by the feed and resumes (replica.rs
// `table_change_relevance`, `bounded_sql_visibility`).

import type { VisibilityPlan } from "../visibility/plan.ts";
import { needsMaterializedSets, resolvedDependencyColumns } from "../visibility/principal.ts";
import type { ResolvedVisibility } from "../visibility/resolved.ts";
import { bounded, type ReplicaDefinition } from "./definition.ts";

/** `table_change_relevance`: (the source table changed, a visibility input changed). */
export function tableChangeRelevance(source: string, plan: VisibilityPlan, resolved: ResolvedVisibility, changes: Iterable<{ table: string; operation: string; changedColumns: readonly string[] }>): [boolean, boolean] {
  const dependencies = resolvedDependencyColumns(plan, resolved);
  let sourceChanged = false, dependencyChanged = false;
  for (const change of changes) {
    if (change.table === source) sourceChanged = true;
    if (dependencies.changeAffects(change.table, change.operation, change.changedColumns)) dependencyChanged = true;
  }
  return [sourceChanged, dependencyChanged];
}

/** Version 1 plans whose sets stay in SQL cannot decide bounded windows in memory. */
export function boundedSqlVisibility(definition: ReplicaDefinition, plan: VisibilityPlan, resolved: ResolvedVisibility): boolean {
  return plan.version !== 2 && bounded(definition) && needsMaterializedSets(plan, resolved);
}
