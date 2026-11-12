// One committed transaction applied to one connection's subscriptions
// (replica.rs `apply_feed_event_with`, `legacy_step`, `apply_rows_with`,
// `decide_row_with`, `row_step`, `normalize_entity_changes`; replica_mask.rs
// `mask_step`, `affected_rows`). Pure in-memory work except the targeted
// read of rows a moved token or parent flip may reveal (`affected`), which
// runs synchronously inside the delivery's own read.

import { type EvalMemo, type MemberHandle } from "../visibility/class.ts";
import type { JsonObject, JsonValue } from "../visibility/json.ts";
import { indexColumns, inheritedColumns, inheritsAny, type VisibilityExpression, type VisibilityPlan } from "../visibility/plan.ts";
import {
  activeGrantSets, ancestorDerivedChangeConcerns, ancestorRowChangeConcerns, boundIndexes, changeConcernsMember, derivedChangeConcerns,
  grantChangeConcerns, grantContribution, indexesAhead, parentAdmits, principalChangeConcerns, recomputeDerived,
  type GrantContribution, type VisibilitySql,
} from "../visibility/principal.ts";
import { readyNow } from "../visibility/acl-index.ts";
import { scalar } from "../visibility/json.ts";
import { withParentOverlay, type ParentOverlay, type ResolvedVisibility } from "../visibility/resolved.ts";
import { jsonObjectExpr } from "../query/compile.ts";
import type { JsonKind } from "../storage/values.ts";
import { canonical } from "./canonical.ts";
import { bounded, type ReplicaDefinition } from "./definition.ts";
import { boundedSqlVisibility, tableChangeRelevance } from "./rules.ts";
import { type Change, type Provenance } from "./frames.ts";
import { CollapsedChanges, type FeedTransaction, type LogChange } from "./log.ts";
import type { Subscription } from "./subscription.ts";
import { collectionFiltersAdmit, isObject, projectRow, quote, rowInCollection, rowKey, visible, windowAdmits, withinBoundary } from "./window.ts";

export type Resync = "principal" | "consistency" | "window";

interface ApplyContext {
  /** The principal before this transaction's grants (undefined: unchanged). */
  before?: ResolvedVisibility;
  /** Parent rows as they were before this transaction. */
  overlay?: ParentOverlay;
  /** Source rows whose visibility may have moved without the row changing, as they are at the event. */
  affected: Array<[string, JsonValue]>;
}

type Step = { kind: "skip" } | { kind: "resync"; reason: Resync } | { kind: "apply"; context: ApplyContext };

/** What delivery needs from the tenant for one event: catalog kinds and one synchronous read. */
export interface FeedReads {
  readonly sql: VisibilitySql;
  readonly kinds: (table: string) => ReadonlyMap<string, JsonKind>;
  readonly clock: () => { revision: number; retained: number };
}

/**
 * The reads `affected_rows` makes, once per transaction for every subscription
 * and socket (design rule: no SQL per subscriber). Rust shares the same read
 * through a recent-reads cache keyed by event, definition and values; here one
 * object lives for one transaction. Rows are whole rows (every column of the
 * table), so subscriptions of any collection on the table can share them.
 */
export class TransactionReads {
  private clockValue: { revision: number; retained: number } | undefined;
  private readonly byValues = new Map<string, Map<string, JsonValue>>();
  private readonly later = new Map<string, ReadonlyArray<readonly [string, JsonValue]>>();
  /** Statements made through this object (the PR's numbers). */
  statements = 0;

  constructor(readonly reads: FeedReads) {}

  clock(): { revision: number; retained: number } {
    if (!this.clockValue) {
      this.statements += 1;
      this.clockValue = this.reads.clock();
    }
    return this.clockValue;
  }

  /** Rows of `table` whose `column` is one of `values`, by key, at the current state. */
  rowsWhere(table: string, key: string, column: string, values: readonly string[]): ReadonlyMap<string, JsonValue> {
    const memo = `${table}\u0000${column}\u0000${JSON.stringify(values)}`;
    let rows = this.byValues.get(memo);
    if (!rows) {
      const kinds = this.reads.kinds(table);
      const columns = [...kinds.keys()].sort().map((name) => { quote(name); return [name, kinds.get(name)!] as const; });
      rows = new Map();
      this.statements += 1;
      const result = this.reads.sql.run(
        `SELECT ${jsonObjectExpr("r", columns)} FROM ${quote(table)} AS r WHERE r.${quote(column)} IN (SELECT value FROM json_each($1))`,
        [JSON.stringify(values)],
      );
      for (const raw of result.rows) {
        if (raw[0] === null || raw[0] === undefined) continue;
        const row = JSON.parse(String(raw[0])) as JsonValue;
        rows.set(rowKey(row, key), row);
      }
      this.byValues.set(memo, rows);
    }
    return rows;
  }

  /** `(row id, old value)` of `table`'s changes after `revision` up to the clock, in order. */
  laterChanges(table: string, revision: number): ReadonlyArray<readonly [string, JsonValue]> {
    const memo = `${table}\u0000${revision}`;
    let rows = this.later.get(memo);
    if (!rows) {
      this.statements += 1;
      const result = this.reads.sql.run(
        `SELECT row_id, old_value FROM _gonvex_sync_changes WHERE table_name = $1 AND revision > $2 AND revision <= $3 ORDER BY revision, ordinal`,
        [table, revision, this.clock().revision],
      );
      rows = result.rows.map(([rowId, oldText]) => [String(rowId), oldText === null || oldText === undefined ? null : (JSON.parse(String(oldText)) as JsonValue)] as const);
      this.later.set(memo, rows);
    }
    return rows;
  }
}

// ---- legacy (version 1) ------------------------------------------------------------------

function legacyStep(subscription: Subscription, changes: readonly LogChange[]): Step {
  const { definition, plan, resolved } = subscription;
  const [source, dependency] = tableChangeRelevance(
    definition.table, plan, resolved,
    changes.filter((c) => c.table === definition.table || changeConcernsMember(plan, resolved, c.table, c.oldValue, c.newValue)),
  );
  if (!source && !dependency) return { kind: "skip" };
  if (dependency) return { kind: "resync", reason: "principal" };
  if (boundedSqlVisibility(definition, plan, resolved)) return { kind: "resync", reason: "window" };
  return { kind: "apply", context: { affected: [] } };
}

// ---- version 2 masks (replica_mask.rs) -----------------------------------------------------

function concernTables(plan: VisibilityPlan, resolved: ResolvedVisibility, out: Set<string>): void {
  out.add(plan.table);
  for (const set of plan.sets.values()) {
    out.add(set.table);
    for (const join of set.joins) out.add(join.table);
  }
  for (const table of inheritedColumns(plan).keys()) out.add(table);
  for (const expression of inheritsAny(plan)) {
    out.add(expression.table);
    if (expression.via) out.add(expression.via.table);
  }
  for (const [table, parent] of resolved.inherited) {
    out.add(table);
    concernTables(parent.plan, parent.resolved, out);
  }
}

function subscriptionConcernTables(subscription: Subscription): Set<string> {
  if (!subscription.concernTables) {
    const tables = new Set<string>([subscription.definition.table, "members"]);
    concernTables(subscription.plan, subscription.resolved, tables);
    subscription.concernTables = tables;
  }
  return subscription.concernTables;
}

function subscriptionParentAclColumns(subscription: Subscription): Map<string, Set<string>> {
  if (!subscription.parentAclColumns) {
    const out = new Map<string, Set<string>>();
    for (const [table, parent] of subscription.resolved.inherited) {
      const plans = new Map<string, VisibilityPlan>([[table, parent.plan], [subscription.plan.table, subscription.plan]]);
      out.set(table, new Set(indexColumns(plans, table, parent.plan.key)));
    }
    subscription.parentAclColumns = out;
  }
  return subscription.parentAclColumns;
}

type GrantDelta = { kind: "none" } | { kind: "added"; tokens: Array<[string, string]> } | { kind: "rebuild" };

function grantDelta(plan: VisibilityPlan, resolved: ResolvedVisibility, change: LogChange): GrantDelta {
  const added: Array<[string, string]> = [];
  for (const [name, set] of activeGrantSets(plan, resolved)) {
    if (set.table !== change.table) continue;
    const old: GrantContribution = grantContribution(name, set, resolved, change.oldValue);
    const next: GrantContribution = grantContribution(name, set, resolved, change.newValue);
    if (old.kind === "unknown" || next.kind === "unknown") return { kind: "rebuild" };
    const same = old.kind === next.kind && (old.kind !== "token" || old.token === (next as { token: string }).token);
    if (same) continue;
    if (old.kind === "token") return { kind: "rebuild" };
    if (next.kind === "token") added.push([name, next.token]);
  }
  return added.length ? { kind: "added", tokens: added } : { kind: "none" };
}

function setColumns(expression: VisibilityExpression, set: string, output: Set<string>): void {
  if (expression.operator === "inSet" && expression.set === set) output.add(expression.column);
  for (const child of expression.children) setColumns(child, set, output);
  if (expression.onMissing) setColumns(expression.onMissing, set, output);
}

function setTestedAsAWhole(expression: VisibilityExpression, set: string): boolean {
  return (expression.operator === "nonEmpty" && expression.set === set)
    || expression.children.some((child) => setTestedAsAWhole(child, set))
    || (expression.onMissing !== undefined && setTestedAsAWhole(expression.onMissing, set));
}

/** A copy of a resolution whose sets and parents can be patched without touching the original. */
function cloneResolved(resolved: ResolvedVisibility): ResolvedVisibility {
  return { ...resolved, sets: new Map(resolved.sets), inherited: new Map(resolved.inherited), indexes: new Map(resolved.indexes) };
}

function addToken(sets: ResolvedVisibility, name: string, token: string): void {
  const next = new Map(sets.sets);
  const current = new Set(next.get(name) ?? []);
  current.add(token);
  next.set(name, current);
  sets.sets = next;
}

function maskStep(subscription: Subscription, revision: number, changes: readonly LogChange[], collapsed: CollapsedChanges, reads: TransactionReads): Step {
  const plan = subscription.plan;
  const parents = inheritedColumns(plan);
  const anyParents = inheritsAny(plan).filter((e) => e.via).map((e) => [e.via!, e.table] as const);
  const tables = subscriptionConcernTables(subscription);
  let source = false;
  const parentChanges: LogChange[] = [];
  const granted: Array<[string, string, string]> = [];
  let derivedTouched = false;
  for (const change of changes) {
    if (!tables.has(change.table)) continue;
    const resolved = subscription.resolved;
    if (principalChangeConcerns(plan, resolved, change.table, change.operation, change.changedColumns, change.oldValue, change.newValue)) return { kind: "resync", reason: "principal" };
    if (change.table === subscription.definition.table) source = true;
    if (derivedChangeConcerns(plan, resolved, change.table, change.rowId, change.oldValue, change.newValue)) derivedTouched = true;
    const ancestorGrant = [...resolved.inherited.values()].some((parent) => [...parent.resolved.inherited.values()].some((ancestor) =>
      grantChangeConcerns(ancestor.plan, ancestor.resolved, change.table, change.oldValue, change.newValue)));
    if (ancestorDerivedChangeConcerns(resolved, change.table, change.rowId, change.oldValue, change.newValue)
      || ancestorRowChangeConcerns(resolved, change.table, change.oldValue, change.newValue) || ancestorGrant) {
      return { kind: "resync", reason: "principal" };
    }
    if (anyParents.some(([via, table]) => via.table === change.table || table === change.table)) return { kind: "resync", reason: "principal" };
    if (parents.has(change.table)) parentChanges.push(change);
    const own = grantDelta(plan, resolved, change);
    if (own.kind === "rebuild") return { kind: "resync", reason: "principal" };
    if (own.kind === "added") for (const [name, token] of own.tokens) granted.push([plan.table, name, token]);
    for (const [table, parent] of resolved.inherited) {
      const delta = grantDelta(parent.plan, parent.resolved, change);
      if (delta.kind === "rebuild") return { kind: "resync", reason: "principal" };
      if (delta.kind === "added") for (const [name, token] of delta.tokens) granted.push([table, name, token]);
    }
  }
  if (!source && !parentChanges.length && !granted.length && !derivedTouched) return { kind: "skip" };

  const context: ApplyContext = { affected: [] };
  const affected = new Map<string, Set<string>>();
  const addAffected = (column: string, tokens: Iterable<string>) => {
    let set = affected.get(column);
    if (!set) affected.set(column, (set = new Set()));
    for (const token of tokens) set.add(token);
  };
  if (granted.length || derivedTouched) {
    context.before = subscription.resolved;
    subscription.resolved = cloneResolved(subscription.resolved);
    subscription.mask.clear();
  }
  for (const [scope, name, token] of granted) {
    if (scope === plan.table) {
      addToken(subscription.resolved, name, token);
      if (setTestedAsAWhole(plan.predicate, name)) return { kind: "resync", reason: "principal" };
      const columns = new Set<string>();
      setColumns(plan.predicate, name, columns);
      for (const column of columns) addAffected(column, [token]);
    } else {
      const parent = subscription.resolved.inherited.get(scope);
      if (!parent) continue;
      const parentResolved = cloneResolved(parent.resolved);
      addToken(parentResolved, name, token);
      const derived = recomputeDerived(parent.plan, parentResolved);
      subscription.resolved.inherited.set(scope, Object.assign(Object.create(Object.getPrototypeOf(parent)), parent, { resolved: parentResolved }));
      const columns = new Set<string>();
      setColumns(parent.plan.predicate, name, columns);
      for (const change of derived) setColumns(parent.plan.predicate, change.set, columns);
      if (setTestedAsAWhole(parent.plan.predicate, name) || [...columns].some((column) => column !== parent.plan.key)) return { kind: "resync", reason: "principal" };
      const tokens = [token, ...derived.flatMap((change) => [...change.added, ...change.removed])];
      for (const column of parents.get(scope) ?? []) addAffected(column, tokens);
    }
  }
  if (derivedTouched || granted.length) {
    for (const change of recomputeDerived(plan, subscription.resolved)) {
      if (setTestedAsAWhole(plan.predicate, change.set)) return { kind: "resync", reason: "principal" };
      const columns = new Set<string>();
      setColumns(plan.predicate, change.set, columns);
      for (const column of columns) addAffected(column, [...change.added, ...change.removed]);
    }
  }
  if (parentChanges.length) {
    const overlay: Array<readonly [string, string, JsonValue | undefined]> = [];
    const aclColumnsByTable = subscriptionParentAclColumns(subscription);
    const before = context.before ?? subscription.resolved;
    for (const change of parentChanges) {
      const net = collapsed.get(change.table, change.rowId);
      const image = net ? net.old : change.oldValue;
      if (overlay.some(([table, key]) => table === change.table && key === change.rowId)) continue;
      overlay.push([change.table, change.rowId, image === null ? undefined : image]);
      const wasParent = before.inherited.get(change.table);
      const nowParent = subscription.resolved.inherited.get(change.table);
      if (!wasParent || !nowParent) continue;
      const last = net ? net.new : change.newValue;
      if (!context.before && image !== null && last !== null) {
        const acl = aclColumnsByTable.get(change.table);
        const unchanged = net !== undefined && net.changedColumns.length > 0 && acl !== undefined && net.changedColumns.every((column) => !acl.has(column));
        if (unchanged) continue;
      }
      const was = parentAdmits(plan, before, change.table, wasParent, image);
      const now = parentAdmits(plan, subscription.resolved, change.table, nowParent, last);
      // Rust skips a parent this transaction created ("no children beyond the transaction's own rows"), so an
      // orphan child that becomes visible when its parent appears is never delivered. Intended behavior IB5
      // (0032): a parent that appears or disappears switches its children between `inherits` and `onMissing`,
      // so they are re-decided; the client converges on what a fresh snapshot shows.
      const appeared = image === null || last === null;
      if (was !== now || appeared) for (const column of parents.get(change.table) ?? []) addAffected(column, [change.rowId]);
    }
    context.overlay = overlay;
  }
  if (indexesAhead(subscription.resolved, revision) && (source || affected.size)) return { kind: "resync", reason: "consistency" };
  if (affected.size) {
    const rows = affectedRows(subscription, affected, revision, collapsed, reads);
    if (!rows) return { kind: "resync", reason: "consistency" };
    context.affected = rows;
  }
  return { kind: "apply", context };
}

/** `affected_rows` / `read_affected_rows`: source rows by column values as of `revision`, minus rows this event changed. */
function affectedRows(subscription: Subscription, affected: Map<string, Set<string>>, revision: number, collapsed: CollapsedChanges, reads: TransactionReads): Array<[string, JsonValue]> | undefined {
  const definition = subscription.definition;
  const clock = reads.clock();
  if (clock.revision < revision || clock.retained > revision) return undefined;
  const rows = new Map<string, JsonValue>();
  for (const [column, values] of [...affected].sort((a, b) => (a[0] < b[0] ? -1 : 1))) {
    for (const [key, row] of reads.rowsWhere(definition.table, definition.key, column, [...values].sort())) rows.set(key, row);
  }
  if (clock.revision > revision) {
    // Rows written after the event: their image at the event is the before-image of their first later change.
    const first = new Set<string>();
    for (const [id, old] of reads.laterChanges(definition.table, revision)) {
      if (first.has(id)) continue;
      first.add(id);
      const matches = [...affected].some(([column, values]) => {
        const value = isObject(old) && Object.hasOwn(old, column) ? old[column] : undefined;
        return value !== undefined && value !== null && values.has(scalar(value));
      });
      if (matches) rows.set(id, old);
      else rows.delete(id);
    }
  }
  return [...rows].filter(([id]) => !collapsed.has(definition.table, id)).sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
}

/** Every bound index reflects the event (`indexes_ready`): delivery pushed it before applying. */
function indexesReady(resolved: ResolvedVisibility, revision: number, epoch: string): boolean {
  return boundIndexes(resolved).every((index) => readyNow(index, revision, epoch) === true);
}

// ---- row decisions ----------------------------------------------------------------------

type Decision =
  | { kind: "outside" }
  | { kind: "unchanged"; row: JsonObject }
  | { kind: "upsert"; old?: { row: JsonObject; hash: string; bytes: number }; row: JsonObject; hash: string; bytes: number }
  | { kind: "delete"; old: JsonObject; hash: string; bytes: number }
  | { kind: "reconcile" };

/** `decide_row_with`. `check(row, isOld)` answers visibility from a compiled class, or undefined for the interpreter. */
export function decideRow(
  definition: ReplicaDefinition, args: JsonValue, plan: VisibilityPlan, before: ResolvedVisibility, after: ResolvedVisibility,
  overlay: ParentOverlay | undefined, window: Subscription["window"], old: JsonValue, next: JsonValue,
  check: (row: JsonValue, isOld: boolean) => boolean | undefined = () => undefined,
): Decision {
  const oldVisible = old !== null && collectionFiltersAdmit(old, args, definition)
    && (check(old, true) ?? withParentOverlay(overlay, () => visible(plan, before, old)));
  const newVisible = next !== null && collectionFiltersAdmit(next, args, definition) && (check(next, false) ?? visible(plan, after, next));
  if (!oldVisible && !newVisible) return { kind: "outside" };
  let oldIn = oldVisible, newIn = newVisible;
  if (window.truncated) {
    const admits = (row: JsonValue) => (!definition.orderBy || !window.boundary ? undefined : withinBoundary(definition, window.boundary, row));
    const oldAdmitted = admits(old), newAdmitted = admits(next);
    if (oldAdmitted === undefined || newAdmitted === undefined) return { kind: "reconcile" };
    oldIn &&= oldAdmitted;
    newIn &&= newAdmitted;
    if (oldIn !== newIn) return { kind: "reconcile" };
    if (!oldIn) return { kind: "outside" };
    const boundary = window.boundary!;
    const nextObject = isObject(next) ? next : {};
    const key = Object.hasOwn(nextObject, definition.key) ? nextObject[definition.key] : undefined;
    const order = Object.hasOwn(nextObject, definition.orderBy) ? nextObject[definition.orderBy] : undefined;
    if (key !== undefined && JSON.stringify(key) === JSON.stringify(boundary[1]) && (order === undefined || JSON.stringify(order) !== JSON.stringify(boundary[0]))) {
      return { kind: "reconcile" };
    }
  }
  const project = (row: JsonValue) => {
    const projected = projectRow(row, definition);
    const { hash, bytes } = canonical(projected);
    return { row: projected, hash, bytes };
  };
  const o = oldIn ? project(old) : undefined;
  const n = newIn ? project(next) : undefined;
  if (o && n && o.hash === n.hash) return { kind: "unchanged", row: o.row };
  if (n) return { kind: "upsert", ...(o ? { old: o } : {}), row: n.row, hash: n.hash, bytes: n.bytes };
  if (o) return { kind: "delete", old: o.row, hash: o.hash, bytes: o.bytes };
  return { kind: "outside" };
}

interface RowTouch {
  changed: boolean;
  old: JsonObject | undefined;
  next: JsonObject | undefined;
  changedColumns: readonly string[];
}

interface SubscriptionDelta {
  upserts: Map<string, JsonObject>;
  deleted: Set<string>;
  touches: Array<[string, string, RowTouch]>;
}

/** Per-event memo of row decisions shared by subscriptions of one visibility class (`feed_share`). */
export class EventShare {
  readonly decisions = new Map<string, Decision>();
  constructor(readonly memo: EvalMemo) {}
}

function rowStep(subscription: Subscription, context: ApplyContext, id: string, old: JsonValue, next: JsonValue, changedColumns: readonly string[], delta: SubscriptionDelta, compiled: { handle: MemberHandle; share: EventShare } | undefined, memoize: boolean): boolean {
  const samePrincipal = !context.before && !context.overlay;
  let sharedKey: string | undefined;
  if (compiled && compiled.handle.kind === "compiled" && memoize && samePrincipal && !subscription.window.truncated) {
    const member = compiled.handle.member;
    const hit = (old !== null && member.hitMemo(compiled.share.memo, old)) || (next !== null && member.hitMemo(compiled.share.memo, next));
    if (!hit) sharedKey = `${member.klass.id}\u0000${subscription.path}\u0000${subscription.argsText}\u0000${subscription.definition.table}\u0000${id}`;
  }
  let decision = sharedKey ? compiled!.share.decisions.get(sharedKey) : undefined;
  if (!decision) {
    const check = (row: JsonValue, isOld: boolean): boolean | undefined => {
      if (!compiled || (isOld && !samePrincipal)) return undefined;
      if (compiled.handle.kind !== "compiled") return undefined;
      return memoize ? compiled.handle.member.visibleMemo(compiled.share.memo, row) : compiled.handle.member.visible({ value: row });
    };
    decision = decideRow(subscription.definition, subscription.args, subscription.plan, context.before ?? subscription.resolved, subscription.resolved, context.overlay, subscription.window, old, next, check);
    if (sharedKey) compiled!.share.decisions.set(sharedKey, decision);
  }
  let touch: RowTouch;
  switch (decision.kind) {
    case "outside": return true;
    case "reconcile": return false;
    case "unchanged": touch = { changed: false, old: decision.row, next: decision.row, changedColumns }; break;
    case "upsert": {
      if (decision.old) {
        subscription.digest.remove(decision.old.hash);
        subscription.window.rows = Math.max(0, subscription.window.rows - 1);
        subscription.window.bytes -= decision.old.bytes;
      }
      subscription.digest.add(decision.hash);
      subscription.window.rows += 1;
      subscription.window.bytes += decision.bytes;
      delta.deleted.delete(id);
      delta.upserts.set(id, decision.row);
      touch = { changed: true, old: decision.old?.row, next: decision.row, changedColumns };
      break;
    }
    case "delete":
      subscription.digest.remove(decision.hash);
      subscription.window.rows = Math.max(0, subscription.window.rows - 1);
      subscription.window.bytes -= decision.bytes;
      delta.upserts.delete(id);
      delta.deleted.add(id);
      touch = { changed: true, old: decision.old, next: undefined, changedColumns };
      break;
  }
  delta.touches.push([subscription.definition.table, id, touch]);
  return true;
}

function applyRows(subscription: Subscription, context: ApplyContext, collapsed: CollapsedChanges, share: EventShare | undefined, scope: string): SubscriptionDelta | undefined {
  if (context.before) subscription.mask.clear();
  const handle = share ? subscription.mask.get(subscription.plan, subscription.resolved, scope) : undefined;
  const compiled = share && handle ? { handle, share } : undefined;
  const delta: SubscriptionDelta = { upserts: new Map(), deleted: new Set(), touches: [] };
  for (const row of collapsed.table(subscription.definition.table)) {
    if (!rowStep(subscription, context, row.rowId, row.old, row.new, row.changedColumns, delta, compiled, true)) return undefined;
  }
  for (const [id, row] of context.affected) {
    if (!rowStep(subscription, context, id, row, row, [], delta, compiled, false)) return undefined;
  }
  const d = subscription.definition;
  const overBudget = (d.maxRows > 0 && subscription.window.rows > d.maxRows) || (d.maxBytes > 0 && subscription.window.bytes > d.maxBytes);
  if (bounded(d) && !subscription.window.truncated && overBudget) return undefined;
  return delta;
}

/** What `holds` answers for a collection that did not decide a row in this event. */
function holds(subscription: Subscription, row: JsonValue): boolean | undefined {
  if (row === null) return false;
  if (!rowInCollection(row, subscription.args, subscription.definition, subscription.plan, subscription.resolved)) return false;
  return windowAdmits(subscription.definition, subscription.window, row);
}

interface EntityTouch {
  changed: boolean;
  oldHeld: boolean;
  oldValue: JsonObject | undefined;
  changedColumns: Set<string>;
  decided: Map<string, JsonObject | undefined>;
}

function normalizeEntityChanges(touches: Map<string, EntityTouch>, collapsed: CollapsedChanges, subscriptions: Iterable<Subscription>): Change[] {
  const out: Change[] = [];
  const subs = [...subscriptions].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  const keys = [...touches.keys()].sort((a, b) => {
    const [ta, ia] = a.split("\u0000") as [string, string];
    const [tb, ib] = b.split("\u0000") as [string, string];
    return ta < tb ? -1 : ta > tb ? 1 : ia < ib ? -1 : ia > ib ? 1 : 0;
  });
  for (const key of keys) {
    const touch = touches.get(key)!;
    if (!touch.changed) continue;
    const [table, id] = key.split("\u0000") as [string, string];
    const image = collapsed.get(table, id)?.new;
    const fields: JsonObject = {};
    let any = false;
    let unknown = false;
    for (const subscription of subs) {
      if (subscription.definition.table !== table) continue;
      if (touch.decided.has(subscription.id)) {
        const row = touch.decided.get(subscription.id);
        if (row) { Object.assign(fields, row); any = true; }
        continue;
      }
      if (image === undefined) { unknown = true; continue; }
      const held = holds(subscription, image);
      if (held === true) {
        try { Object.assign(fields, projectRow(image, subscription.definition)); any = true; } catch { /* missing key: skipped, as Rust's `if let Ok` */ }
      } else if (held === undefined) unknown = true;
    }
    const changedColumns = [...touch.changedColumns].sort();
    if (!any) {
      if (unknown) continue;
      out.push({ entity: table, id, operation: "delete", ...(touch.oldValue ? { oldValue: touch.oldValue } : {}), changedColumns });
    } else {
      out.push({ entity: table, id, operation: touch.oldHeld ? "update" : "insert", ...(touch.oldValue ? { oldValue: touch.oldValue } : {}), newValue: fields, changedColumns });
    }
  }
  return out;
}

// ---- the event --------------------------------------------------------------------------

export type FeedFrame =
  | { kind: "transaction"; cursor: { epoch: string; revision: number }; originCommandId?: string; provenance?: Provenance; changes: Change[] }
  | { kind: "delta"; subscription: Subscription; upserts: JsonObject[]; deleted: string[] }
  | { kind: "ready"; subscription: Subscription }
  | { kind: "needHashes"; id: string; path: string };

/**
 * `apply_feed_event_with` for one connection: frames in Rust's order (the
 * transaction first, then a delta and a ready per changed collection, then
 * nothing for unchanged ones; `needHashes` for each collection that must
 * reconcile, which is removed). `subscriptions` is mutated in place.
 */
export function applyTransaction(
  subscriptions: Map<string, Subscription>, event: FeedTransaction, collapsed: CollapsedChanges, reads: TransactionReads, share: EventShare | undefined, scope: string,
): { frames: FeedFrame[]; removed: Subscription[] } {
  const frames: FeedFrame[] = [];
  const touches = new Map<string, EntityTouch>();
  const resynced: Subscription[] = [];
  const ordered = [...subscriptions.values()].sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
  for (const subscription of ordered) {
    if (event.revision <= subscription.cursor.revision) continue;
    let step: Step;
    if (subscription.plan.version === 2) {
      step = !indexesReady(subscription.resolved, event.revision, event.epoch)
        ? { kind: "resync", reason: "consistency" }
        : maskStep(subscription, event.revision, event.changes, collapsed, reads);
    } else {
      step = legacyStep(subscription, event.changes);
    }
    if (step.kind === "skip") {
      subscription.cursor = { epoch: subscription.cursor.epoch, revision: event.revision };
      continue;
    }
    if (step.kind === "resync") {
      frames.push({ kind: "needHashes", id: subscription.id, path: subscription.path });
      resynced.push(subscription);
      continue;
    }
    const delta = applyRows(subscription, step.context, collapsed, share, scope);
    if (!delta) {
      frames.push({ kind: "needHashes", id: subscription.id, path: subscription.path });
      resynced.push(subscription);
      continue;
    }
    subscription.cursor = { epoch: subscription.cursor.epoch, revision: event.revision };
    for (const [table, id, touch] of delta.touches) {
      const key = `${table}\u0000${id}`;
      let entity = touches.get(key);
      if (!entity) touches.set(key, (entity = { changed: false, oldHeld: false, oldValue: undefined, changedColumns: new Set(), decided: new Map() }));
      entity.changed ||= touch.changed;
      if (touch.old) {
        entity.oldHeld = true;
        entity.oldValue = { ...(entity.oldValue ?? {}), ...touch.old };
      }
      for (const column of touch.changedColumns) entity.changedColumns.add(column);
      entity.decided.set(subscription.id, touch.next);
    }
    if (!delta.upserts.size && !delta.deleted.size) continue;
    const ids = [...delta.upserts.keys()].sort();
    frames.push({ kind: "delta", subscription, upserts: ids.map((id) => delta.upserts.get(id)!), deleted: [...delta.deleted].sort() });
    frames.push({ kind: "ready", subscription });
  }
  for (const subscription of resynced) subscriptions.delete(subscription.id);
  const changes = normalizeEntityChanges(touches, collapsed, subscriptions.values());
  if (changes.length) {
    const origin = event.changes.find((c) => c.originCommandId !== "")?.originCommandId;
    const first = event.changes[0];
    frames.unshift({
      kind: "transaction",
      cursor: { epoch: event.epoch, revision: event.revision },
      ...(origin !== undefined ? { originCommandId: origin } : {}),
      ...(first ? { provenance: first.provenance } : {}),
      changes,
    });
  }
  return { frames, removed: resynced };
}
