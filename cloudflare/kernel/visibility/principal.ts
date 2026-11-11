// Principal resolution and change routing (visibility.rs `resolve_member`,
// `resolve_with_parents`, visibility_snapshot.rs `resolve_derived`,
// `recompute_derived`, grant contributions and the `*_change_concerns`
// family).
//
// Resolution is the only part that reads SQL (V3: member-owned permission
// rows, the grant sets' source rows and the derived sets' indexed rows). It
// runs synchronously through K01's kernel SQL (`StorageSession.run`), which
// binds `$N` parameters with Rust's rules. Everything after resolution is
// in memory.

import { sha256Hex } from "../capture/digest.ts";
import type { ParentAclIndex } from "./acl-index.ts";
import { get, isObject, jsonEqual, scalar, serdeJson, sortedText, type JsonValue } from "./json.ts";
import { literalConstraintsHold, matchExpression, rowMatches } from "./mask.ts";
import {
  activeSets,
  dependencyColumnsForSets,
  inheritedTables,
  inheritsAny,
  memberColumnChanged,
  planToJson,
  principalInputs,
  principalTables,
  quote,
  setEntries,
  setOf,
  validatePlan,
  viaIndexName,
  visibilityLiteral,
  VisibilityError,
  type VisibilityDependencies,
  type VisibilityConstraint,
  type VisibilityExpression,
  type VisibilityPlan,
  type VisibilitySet,
} from "./plan.ts";
import { InheritedParent, type DirectContext, type ResolvedVisibility, type TokenSets } from "./resolved.ts";
import { compileLiteralComparison, compileSet } from "./sql.ts";
import type { PrincipalCache, PrincipalClock } from "./principal-cache.ts";

/** Grant-set join tokens live beside ordinary sets under this suffix (the member's team ids for a grant joined to memberTeams). */
export const GRANT_JOIN_SUFFIX = "::join";

/** One kernel SQL statement with `$N` parameters (K01 `StorageSession.run` satisfies it). */
export interface VisibilitySql {
  run(sql: string, parameters?: readonly unknown[]): { readonly columnNames: readonly string[]; readonly rows: readonly (readonly SqlStorageValue[])[]; readonly rowsRead: number; readonly rowsWritten: number };
}

/** What resolution reads of a tenant session (kernel/session/ports `TenantSession`). */
export interface VisibilitySession {
  readonly identity: { readonly account: { readonly id: string } };
  readonly route: { readonly tenantId: string };
  readonly member: { readonly id: string; readonly accountId: string; readonly status: string; readonly role: string; readonly permissions: unknown };
  readonly developerSession: unknown;
  readonly anonymous: boolean;
}

/** Indexes prepared before the snapshot (registry `PreparedParentAcl`). */
export interface ParentAclSource {
  get(table: string, key: string): ParentAclIndex;
}

/** Counts visibility SQL statements (`visibility_metrics`). */
export interface ResolutionMetrics {
  visibilitySqlStatements: number;
}

const MEMBER_SQL = `SELECT id, role, permissions
           FROM members WHERE account_id = $1 AND status = 'active'`;

function textColumn(rows: readonly (readonly SqlStorageValue[])[]): Set<string> {
  const values = new Set<string>();
  for (const row of rows) {
    const value = row[0];
    if (value !== null && value !== undefined) values.add(typeof value === "string" ? value : String(value));
  }
  return values;
}

function sortedSet(values: Iterable<string>): ReadonlySet<string> {
  return new Set(sortedText(values));
}

function decodePermissions(cell: SqlStorageValue | undefined): JsonValue {
  if (cell === null || cell === undefined) throw new Error('error occurred while decoding column "permissions": unexpected null; try decoding as an `Option`');
  if (typeof cell === "number") return cell;
  if (typeof cell !== "string") throw new Error('error occurred while decoding column "permissions": mismatched types');
  try {
    return JSON.parse(cell) as JsonValue;
  } catch (error) {
    throw new Error(`error occurred while decoding column "permissions": ${(error as Error).message}`);
  }
}

/** `visibility_fingerprint`: SHA-256 of the plan, the direct context, role, permissions and sets. */
export function visibilityFingerprint(plan: VisibilityPlan, direct: DirectContext, role: string, permissions: JsonValue, sets: TokenSets): string {
  const setsJson: Record<string, JsonValue> = {};
  for (const [name, tokens] of sets) setsJson[name] = sortedText(tokens);
  return sha256Hex(serdeJson({ plan: planToJson(plan), direct: { ...direct }, role, permissions, sets: setsJson }));
}

/**
 * `resolve_member`: the member row and the active principal and grant sets.
 * `materialize` false is `resolve_for_query` (legacy plans keep set
 * membership in SQL).
 */
export function resolveMember(sql: VisibilitySql, session: VisibilitySession, plan: VisibilityPlan, revision: number, materialize: boolean, metrics?: ResolutionMetrics): ResolvedVisibility {
  validatePlan(plan);
  const accountId = session.identity.account.id;
  const found = sql.run(MEMBER_SQL, [accountId]).rows[0];
  let memberId: string, role: string, permissions: JsonValue, ephemeralMember: boolean;
  if (found) {
    memberId = String(found[0]);
    role = String(found[1]);
    permissions = decodePermissions(found[2]);
    ephemeralMember = false;
  } else if (
    // Admission already verified this self developer grant; use its
    // ephemeral principal without creating a Member.
    session.developerSession !== null &&
    session.developerSession !== undefined &&
    !session.anonymous &&
    session.member.id === `developer:${accountId}` &&
    session.member.accountId === accountId &&
    session.member.status === "active"
  ) {
    memberId = session.member.id;
    role = session.member.role;
    permissions = (session.member.permissions ?? null) as JsonValue;
    ephemeralMember = true;
  } else {
    throw VisibilityError.memberRequired();
  }
  const direct: DirectContext = { "account.id": accountId, "member.id": memberId, "tenant.id": session.route.tenantId };
  const sets = new Map<string, ReadonlySet<string>>();
  const active = materialize || plan.version === 2 ? activeSets(plan, role) : new Set<string>();
  for (const [name, set] of setEntries(plan)) {
    if (!active.has(name) || set.kind === "derived") continue;
    const compiled = compileSet(set, direct);
    sets.set(name, sortedSet(textColumn(sql.run(compiled.sql, compiled.parameters).rows)));
    if (metrics) metrics.visibilitySqlStatements += 1;
    if (plan.version === 2 && set.kind === "grant" && set.joins.length) {
      const joinSet = grantJoinSet(set);
      if (joinSet) {
        const join = compileSet(joinSet, direct);
        sets.set(`${name}${GRANT_JOIN_SUFFIX}`, sortedSet(textColumn(sql.run(join.sql, join.parameters).rows)));
        if (metrics) metrics.visibilitySqlStatements += 1;
      }
    }
  }
  return {
    ephemeralMember,
    revision,
    direct,
    role,
    permissions,
    sets,
    fingerprint: visibilityFingerprint(plan, direct, role, permissions, sets),
    inherited: new Map(),
    indexes: new Map(),
  };
}

/** `resolve_related_query` (legacy only): a related legacy plan shares the member and snapshot. */
export function resolveRelatedQuery(plan: VisibilityPlan, member: ResolvedVisibility): ResolvedVisibility {
  validatePlan(plan);
  if (plan.version === 2) throw VisibilityError.invalid("related mask plans require their own resolved principal and parents");
  const sets = new Map<string, ReadonlySet<string>>();
  return { ...member, sets, inherited: new Map(member.inherited), indexes: new Map(member.indexes), fingerprint: visibilityFingerprint(plan, member.direct, member.role, member.permissions, sets) };
}

/** visibility_snapshot.rs `resolve_derived`: derived sets from the same snapshot as the rows they authorize. Returns statements run. */
export function resolveDerived(sql: VisibilitySql, plan: VisibilityPlan, resolved: ResolvedVisibility): number {
  let statements = 0;
  const sets = new Map(resolved.sets);
  const pending = new Set([...activeSets(plan, resolved.role)].filter((name) => setOf(plan, name).kind === "derived"));
  while (pending.size) {
    const name = [...pending].find((candidate) => !pending.has(setOf(plan, candidate).from));
    if (name === undefined) throw VisibilityError.invalid("cyclic derived visibility sets");
    const set = setOf(plan, name);
    const source = sets.get(set.from);
    if (!source) throw VisibilityError.invalid(`mask principal set ${JSON.stringify(set.from)} was not resolved`);
    if (source.size === 0) {
      sets.set(name, new Set());
      pending.delete(name);
      continue;
    }
    const values: JsonValue[] = [];
    const tokens = (column: string, setName: string) => {
      const found = sets.get(setName);
      if (!found) throw VisibilityError.invalid(`mask principal set ${JSON.stringify(setName)} was not resolved`);
      values.push(sortedText(found));
      return `r.${quote(column)} IN (SELECT value FROM json_each($${values.length}))`;
    };
    const predicates = [tokens(set.key, set.from)];
    for (const constraint of set.constraints) {
      if (constraint.context) {
        const value = resolved.direct[constraint.context];
        if (value === undefined) throw VisibilityError.invalid(`unresolved context ${JSON.stringify(constraint.context)}`);
        values.push(value);
        predicates.push(`r.${quote(constraint.column)} = $${values.length}`);
      } else if (constraint.inSet) {
        predicates.push(tokens(constraint.column, constraint.inSet));
      } else {
        predicates.push(compileLiteralComparison("r", constraint.column, visibilityLiteral(constraint.value), values, false));
      }
    }
    const statement = `SELECT DISTINCT CAST(r.${quote(set.select)} AS TEXT) FROM ${quote(set.table)} AS r WHERE ${predicates.join(" AND ")}`;
    statements += 1;
    sets.set(name, sortedSet(textColumn(sql.run(statement, values).rows)));
    pending.delete(name);
  }
  resolved.sets = sets;
  resolved.fingerprint = visibilityFingerprint(plan, resolved.direct, resolved.role, resolved.permissions, resolved.sets);
  return statements;
}

/** The SQL part of a version 2 resolution: member row, principal and grant sets, then derived sets. */
export function resolveMemberSql(sql: VisibilitySql, session: VisibilitySession, plan: VisibilityPlan, revision: number, metrics?: ResolutionMetrics): ResolvedVisibility {
  const resolved = resolveMember(sql, session, plan, revision, true, metrics);
  const statements = resolveDerived(sql, plan, resolved);
  if (metrics) metrics.visibilitySqlStatements += statements;
  return resolved;
}

export interface SharedResolution {
  readonly cache: PrincipalCache;
  readonly clock: PrincipalClock;
}

/**
 * `resolve_with_parents`: a version 2 plan and, recursively, every plan it
 * inherits from for the same member, with the tenant-shared indexes attached.
 * Legacy plans resolve as `resolve_member` with sets materialized.
 */
export function resolveWithParents(
  sql: VisibilitySql,
  session: VisibilitySession,
  plan: VisibilityPlan,
  revision: number,
  plans: ReadonlyMap<string, VisibilityPlan>,
  acl: ParentAclSource,
  metrics?: ResolutionMetrics,
  shared?: SharedResolution,
): ResolvedVisibility {
  if (plan.version !== 2) return resolveMember(sql, session, plan, revision, true, metrics);
  let resolved: ResolvedVisibility;
  const key = shared?.cache.key(session, plan);
  if (shared && key !== undefined) {
    const hit = shared.cache.lookup(key, plan, sql, shared.clock);
    if (hit) {
      validatePlan(plan);
      resolved = hit(revision);
    } else {
      resolved = resolveMemberSql(sql, session, plan, revision, metrics);
      shared.cache.store(key, plan, sql, shared.clock, resolved);
    }
  } else {
    resolved = resolveMemberSql(sql, session, plan, revision, metrics);
  }
  for (const table of inheritedTables(plan)) {
    const parent = plans.get(table);
    if (!parent) throw VisibilityError.invalid(`unknown inherited parent ${JSON.stringify(table)}`);
    const parentResolved = resolveWithParents(sql, session, parent, revision, plans, acl, metrics, shared);
    resolved.inherited.set(table, new InheritedParent(parent, parentResolved, acl.get(parent.table, parent.key)));
  }
  for (const expression of inheritsAny(plan)) {
    const via = expression.via;
    if (!via) continue;
    for (const column of [via.childColumn, via.parentColumn]) resolved.indexes.set(viaIndexName(via, column), acl.get(via.table, column));
  }
  for (const name of activeSets(plan, resolved.role)) {
    const set = setOf(plan, name);
    if (set.kind === "derived") resolved.indexes.set(name, acl.get(set.table, set.key));
  }
  return resolved;
}

// ---- in-memory maintenance ---------------------------------------------------------

/** Rows of a derived index that pass the set's filters for this member. */
function derivedTokens(set: VisibilitySet, index: ParentAclIndex, source: ReadonlySet<string>, sets: TokenSets, direct: DirectContext): Set<string> {
  const next = new Set<string>();
  for (const token of source) {
    for (const row of index.getAll(token)) {
      const passes = set.constraints.every((constraint: VisibilityConstraint) => {
        const value = get(row, constraint.column);
        if (constraint.context) return value !== undefined && value !== null && direct[constraint.context] === scalar(value);
        if (constraint.inSet) return value !== undefined && value !== null && (sets.get(constraint.inSet)?.has(scalar(value)) ?? false);
        return literalConstraintsHold([constraint], row);
      });
      if (!passes) continue;
      const selected = get(row, set.select);
      if (selected !== undefined && selected !== null) next.add(scalar(selected));
    }
  }
  return next;
}

export interface DerivedChange {
  readonly set: string;
  readonly added: ReadonlySet<string>;
  readonly removed: ReadonlySet<string>;
}

/** `recompute_derived`: every active derived set from its source set and the shared index. */
export function recomputeDerived(plan: VisibilityPlan, resolved: ResolvedVisibility): DerivedChange[] {
  const changes = new Map<string, { added: Set<string>; removed: Set<string> }>();
  const order: string[] = [];
  const active = activeSets(plan, resolved.role);
  // Chains are at most two deep: two passes reach a fixed point.
  for (let pass = 0; pass < 2; pass++) {
    for (const [name, set] of setEntries(plan)) {
      if (set.kind !== "derived" || !active.has(name)) continue;
      const index = resolved.indexes.get(name);
      if (!index) continue;
      const source = resolved.sets.get(set.from) ?? new Set<string>();
      const next = derivedTokens(set, index, source, resolved.sets, resolved.direct);
      let sets = resolved.sets;
      // A resolved empty set differs from a set that was never loaded.
      if (!sets.has(name)) {
        sets = new Map(sets).set(name, new Set());
        resolved.sets = sets;
      }
      const previous = sets.get(name)!;
      const added = [...next].filter((token) => !previous.has(token));
      const removed = [...previous].filter((token) => !next.has(token));
      if (!added.length && !removed.length) continue;
      let change = changes.get(name);
      if (!change) {
        changes.set(name, (change = { added: new Set(), removed: new Set() }));
        order.push(name);
      }
      for (const token of added) change.added.add(token);
      for (const token of removed) change.removed.add(token);
      resolved.sets = new Map(resolved.sets).set(name, sortedSet(next));
    }
  }
  return order.map((name) => {
    const change = changes.get(name)!;
    return { set: name, added: sortedSet(change.added), removed: sortedSet(change.removed) };
  });
}

/** Does a change to `table` touch a row some derived set maps one of its source tokens through? */
export function derivedChangeConcerns(plan: VisibilityPlan, resolved: ResolvedVisibility, table: string, rowId: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean {
  for (const [name, set] of setEntries(plan)) {
    if (set.kind !== "derived" || set.table !== table || !resolved.indexes.has(name)) continue;
    const source = resolved.sets.get(set.from);
    if (!source) continue;
    for (const row of [oldRow, newRow]) {
      const value = get(row, set.key);
      if (value !== undefined && source.has(scalar(value))) return true;
    }
    if (source.has(rowId)) return true;
  }
  return false;
}

/** An inherited principal can depend on derived data at any admitted depth. */
export function ancestorDerivedChangeConcerns(resolved: ResolvedVisibility, table: string, rowId: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean {
  for (const parent of resolved.inherited.values()) {
    if (derivedChangeConcerns(parent.plan, parent.resolved, table, rowId, oldRow, newRow) || ancestorDerivedChangeConcerns(parent.resolved, table, rowId, oldRow, newRow)) return true;
  }
  return false;
}

/** Active grant sets of a version 2 plan, in name order. */
export function activeGrantSets(plan: VisibilityPlan, resolved: ResolvedVisibility): Array<[string, VisibilitySet]> {
  const active = activeSets(plan, resolved.role);
  return setEntries(plan).filter(([name, set]) => set.kind === "grant" && active.has(name));
}

/** Grant rows route recursively through inherited parents. */
export function grantChangeConcerns(plan: VisibilityPlan, resolved: ResolvedVisibility, table: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean {
  for (const [name, set] of activeGrantSets(plan, resolved)) {
    if (set.table !== table) continue;
    for (const row of [oldRow, newRow]) if (grantContribution(name, set, resolved, row).kind !== "none") return true;
  }
  for (const parent of resolved.inherited.values()) if (grantChangeConcerns(parent.plan, parent.resolved, table, oldRow, newRow)) return true;
  return false;
}

/** Does some `inherits`/`inheritsAny` of this plan admit children of `row`, a row of the inherited `table`? */
export function parentAdmits(plan: VisibilityPlan, child: ResolvedVisibility, table: string, parent: InheritedParent, row: JsonValue | undefined): boolean {
  if (row === undefined || row === null) return false;
  const visit = (expression: VisibilityExpression): boolean =>
    ((expression.operator === "inherits" || expression.operator === "inheritsAny") &&
      expression.table === table &&
      literalConstraintsHold(expression.parentWhere, row) &&
      (expression.predicate ? isObject(row) && matchExpression(expression.predicate, child, row) : rowMatches(parent.plan, parent.resolved, row))) ||
    expression.children.some(visit) ||
    (expression.onMissing !== undefined && visit(expression.onMissing));
  return visit(plan.predicate);
}

/** Changes to a more distant ancestor's row or association can reveal or hide existing descendants. */
export function ancestorRowChangeConcerns(resolved: ResolvedVisibility, table: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean {
  for (const parent of resolved.inherited.values()) {
    const state = parent.resolved;
    const ancestor = state.inherited.get(table);
    if (ancestor) {
      const admitted = (row: JsonValue | undefined) => row !== undefined && row !== null && parentAdmits(parent.plan, state, table, ancestor, row);
      if (admitted(oldRow) !== admitted(newRow)) return true;
    }
    for (const expression of inheritsAny(parent.plan)) {
      const via = expression.via;
      const linked = state.inherited.get(expression.table);
      if (!via || !linked || via.table !== table) continue;
      for (const row of [oldRow, newRow]) {
        const value = get(row, via.parentColumn);
        if (value === undefined || value === null) continue;
        const parentRow = linked.row(scalar(value));
        if (parentRow !== undefined && parentAdmits(parent.plan, state, expression.table, linked, parentRow)) return true;
      }
    }
    if (ancestorRowChangeConcerns(state, table, oldRow, newRow)) return true;
  }
  return false;
}

/** After making row decisions: has a bound index already applied ACL changes later than `revision`? */
export function indexesAhead(resolved: ResolvedVisibility, revision: number): boolean {
  for (const index of resolved.indexes.values()) if (index.poisoned || index.modifiedRevision > revision) return true;
  for (const parent of resolved.inherited.values()) {
    if (parent.index.poisoned || parent.index.modifiedRevision > revision || indexesAhead(parent.resolved, revision)) return true;
  }
  return false;
}

/** All indexes bound into `resolved`, recursively. */
export function boundIndexes(resolved: ResolvedVisibility): ParentAclIndex[] {
  const indexes: ParentAclIndex[] = [];
  const pending = [resolved];
  while (pending.length) {
    const next = pending.pop()!;
    indexes.push(...next.indexes.values());
    for (const parent of next.inherited.values()) {
      indexes.push(parent.index);
      pending.push(parent.resolved);
    }
  }
  return indexes;
}

/**
 * For a grant set joined to permission tables: the permission side as its own
 * principal set selecting the first join's key. Undefined when a later join
 * hangs off the grant row itself.
 */
export function grantJoinSet(set: VisibilitySet): VisibilitySet | undefined {
  const base = set.alias || set.table;
  const first = set.joins[0];
  if (!first) return undefined;
  if (first.leftAlias && first.leftAlias !== base) return undefined;
  if (set.joins.slice(1).some((join) => join.leftAlias === base)) return undefined;
  if (set.selectFrom && set.selectFrom !== base) return undefined;
  const firstAlias = first.alias || first.table;
  return {
    kind: "principal",
    from: "",
    key: "",
    table: first.table,
    alias: first.alias,
    select: first.rightColumn,
    selectFrom: "",
    joins: set.joins.slice(1).map((join) => ({ ...join })),
    constraints: set.constraints
      .filter((constraint) => constraint.table !== "" && constraint.table !== base)
      .map((constraint) => ({ ...constraint, inSet: "", table: constraint.table === firstAlias ? "" : constraint.table })),
  };
}

/** How one grant-table row bears on a member's grant set. */
export type GrantContribution = { kind: "token"; token: string } | { kind: "none" } | { kind: "unknown" };

const NONE: GrantContribution = { kind: "none" };
const UNKNOWN: GrantContribution = { kind: "unknown" };

/** Decides in memory whether a grant row contributes to this member's set, from the row, the context and the join tokens alone. */
export function grantContribution(name: string, set: VisibilitySet, resolved: ResolvedVisibility, row: JsonValue | undefined): GrantContribution {
  if (!isObject(row)) return NONE;
  const base = set.alias || set.table;
  if (set.selectFrom && set.selectFrom !== base) return UNKNOWN;
  for (const constraint of set.constraints) {
    if (constraint.table && constraint.table !== base) continue;
    const value = get(row, constraint.column);
    if (value === undefined) return UNKNOWN;
    let holds: boolean;
    if (!constraint.context) {
      const literal = isObject(constraint.value) && Object.hasOwn(constraint.value, "literal") ? constraint.value["literal"] : undefined;
      if (literal === undefined) return UNKNOWN;
      holds = jsonEqual(value, literal);
    } else {
      const expected = resolved.direct[constraint.context];
      if (expected === undefined) return UNKNOWN;
      holds = scalar(value) === expected;
    }
    if (!holds) return NONE;
  }
  const first = set.joins[0];
  if (first) {
    const tokens = resolved.sets.get(`${name}${GRANT_JOIN_SUFFIX}`);
    if (!tokens) return UNKNOWN;
    const value = get(row, first.leftColumn);
    if (value === undefined) return UNKNOWN;
    if (!tokens.has(scalar(value))) return NONE;
  }
  const selected = get(row, set.select);
  if (selected === undefined) return UNKNOWN;
  if (selected === null) return NONE;
  return { kind: "token", token: scalar(selected) };
}

// ---- principal routing -------------------------------------------------------------

/** Role-pruned dependencies of this member's resolution. */
export function resolvedDependencyColumns(plan: VisibilityPlan, resolved: ResolvedVisibility): VisibilityDependencies {
  return dependencyColumnsForSets(plan, activeSets(plan, resolved.role), true);
}

/** Whether row decisions need materialized sets for this member's role. */
export function needsMaterializedSets(plan: VisibilityPlan, resolved: ResolvedVisibility): boolean {
  return activeSets(plan, resolved.role).size > 0;
}

function occurrences(set: VisibilitySet, includeBase: boolean): Array<[string, string]> {
  const base = set.alias || set.table;
  const output: Array<[string, string]> = includeBase ? [[set.table, base]] : [];
  for (const join of set.joins) output.push([join.table, join.alias || join.table]);
  return output;
}

/**
 * Version 2: does this change alter the member's principal (a principal
 * set, a grant set's permission join, or the member row)? Grant source rows
 * are not principal inputs; they are patched in memory. Parents are checked
 * recursively with their own resolution.
 */
export function principalChangeConcerns(
  plan: VisibilityPlan,
  resolved: ResolvedVisibility,
  table: string,
  operation: string,
  changedColumns: readonly string[],
  oldRow: JsonValue | undefined,
  newRow: JsonValue | undefined,
): boolean {
  const rows = [oldRow, newRow].filter((row): row is JsonValue => row !== undefined && row !== null);
  if (table === "members") {
    const member = resolved.direct["member.id"];
    const namesMember = rows.some((row) => {
      const id = get(row, "id");
      return typeof id !== "string" || id === member;
    });
    if (memberColumnChanged(operation, changedColumns) && (namesMember || rows.length === 0)) return true;
  }
  if (principalTables(plan).has(table)) {
    const inputs = principalInputs(plan);
    for (const name of activeSets(plan, resolved.role)) {
      const set = setOf(plan, name);
      // Derived inputs are data rows, routed through their source tokens.
      if (set.kind === "derived") continue;
      const found = occurrences(set, set.kind !== "grant").filter(([source]) => source === table);
      if (!found.length) continue;
      const input = inputs.get(name)!;
      if (!input.dependencies.changeAffects(table, operation, changedColumns)) continue;
      if (rows.length === 0) return true;
      for (const [, alias] of found) {
        // Unanchored permission occurrences stay conservative.
        const anchors = input.anchors.get(alias);
        if (!anchors) return true;
        for (const row of rows) {
          const excludes = [...anchors].some((pair) => {
            const [column, context] = pair.split("\u0000") as [string, string];
            const value = get(row, column);
            const expected = resolved.direct[context];
            return value !== undefined && expected !== undefined && scalar(value) !== expected;
          });
          if (!excludes) return true;
        }
      }
    }
  }
  for (const parent of resolved.inherited.values()) {
    if (principalChangeConcerns(parent.plan, parent.resolved, table, operation, changedColumns, oldRow, newRow)) return true;
  }
  return false;
}

/**
 * Conservative routing for legacy declarations (`change_concerns_member`): a
 * changed occurrence is skipped only when its own context constraint proves
 * both rows belong to another principal.
 */
export function changeConcernsMember(plan: VisibilityPlan, resolved: ResolvedVisibility, table: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean {
  const rows = [oldRow, newRow].filter((row): row is JsonValue => row !== undefined && row !== null);
  if (rows.length === 0) return true;
  if (table === "members") {
    for (const row of rows) {
      const id = get(row, "id");
      if (!(typeof id === "string" && id !== resolved.direct["member.id"])) return true;
    }
  }
  for (const name of activeSets(plan, resolved.role)) {
    const set = setOf(plan, name);
    const base = set.alias || set.table;
    for (const [, alias] of occurrences(set, true).filter(([source]) => source === table)) {
      const anchors = set.constraints.filter((c) => (c.table === alias || (c.table === "" && alias === base)) && c.context !== "");
      if (!anchors.length) return true;
      for (const row of rows) {
        const excludes = anchors.some((anchor) => {
          const value = get(row, anchor.column);
          const expected = resolved.direct[anchor.context];
          return value !== undefined && expected !== undefined && scalar(value) !== expected;
        });
        if (!excludes) return true;
      }
    }
  }
  return false;
}
