// Visibility plans: the manifest shape, its validation, and the analyses that
// depend on the plan alone (visibility.rs: `VisibilityPlan`, `validate`,
// `validate_manifest`, `role_only_value`, `active_sets`, `acl_columns`,
// `index_columns`, `required_indexes`, dependency columns and anchors).
// Pure: no SQL, no I/O.

import { compareText, debugString, isObject, serdeJson, sortedText, type JsonValue } from "./json.ts";

export interface VisibilityJoin {
  table: string;
  alias: string;
  leftAlias: string;
  leftColumn: string;
  rightColumn: string;
}

export interface VisibilityConstraint {
  table: string;
  column: string;
  context: string;
  /** `Option<Value>`: JSON null deserializes to absent, as serde does. */
  value: JsonValue | undefined;
  /** `derived` filters only: the indexed row's column must be one of this member's tokens in that set. */
  inSet: string;
}

export interface VisibilitySet {
  kind: string;
  table: string;
  alias: string;
  select: string;
  selectFrom: string;
  joins: VisibilityJoin[];
  constraints: VisibilityConstraint[];
  /** `derived` only: the set whose tokens are mapped through `table`. */
  from: string;
  /** `derived` only: the indexed table's key column the tokens name. */
  key: string;
}

export interface VisibilityVia {
  table: string;
  childColumn: string;
  parentColumn: string;
}

export interface VisibilityExpression {
  operator: string;
  column: string;
  context: string;
  set: string;
  value: JsonValue | undefined;
  children: VisibilityExpression[];
  onMissing: VisibilityExpression | undefined;
  table: string;
  key: string;
  /** `inherits`/`inheritsAny` only: literal filters on the parent row (`where`). */
  parentWhere: VisibilityConstraint[];
  /** `inherits`/`inheritsAny` only: evaluated on the parent row with the child plan's principal. */
  predicate: VisibilityExpression | undefined;
  /** `inheritsAny` only: the association rows linking a row to its parents. */
  via: VisibilityVia | undefined;
}

export interface VisibilityPlan {
  version: number | undefined;
  permissionTables: string[];
  table: string;
  key: string;
  /** BTreeMap in Rust: iterate with `setEntries` for its order. */
  sets: Map<string, VisibilitySet>;
  predicate: VisibilityExpression;
}

// ---- errors ------------------------------------------------------------------

export type VisibilityErrorKind = "invalid" | "member-required";

/** `VisibilityError`: `Invalid` and `MemberRequired` keep Rust's Display text. Database errors propagate unchanged. */
export class VisibilityError extends Error {
  constructor(
    readonly kind: VisibilityErrorKind,
    readonly detail: string,
  ) {
    super(kind === "invalid" ? `invalid visibility plan: ${detail}` : "active tenant member is required for visibility");
    this.name = "VisibilityError";
  }

  static invalid(detail: string): VisibilityError {
    return new VisibilityError("invalid", detail);
  }

  static memberRequired(): VisibilityError {
    return new VisibilityError("member-required", "");
  }
}

// ---- parsing (serde's defaults) -----------------------------------------------

/** A manifest plan as serde reads it. Throws `VisibilityError` on a shape serde would refuse. */
export function parsePlan(value: unknown): VisibilityPlan {
  const object = record(value, "VisibilityPlan");
  const version = object["version"];
  if (version !== undefined && version !== null && !(typeof version === "number" && Number.isInteger(version) && version >= 0 && version <= 255)) {
    throw parseError(`invalid value for \`version\`: expected u8`);
  }
  const sets = new Map<string, VisibilitySet>();
  const rawSets = object["sets"];
  if (rawSets !== undefined) {
    const map = record(rawSets, "map of sets");
    for (const name of sortedText(Object.keys(map))) sets.set(name, parseSet(map[name]));
  }
  return {
    version: typeof version === "number" ? version : undefined,
    permissionTables: stringArray(object["permissionTables"], "permissionTables"),
    table: requiredString(object, "table"),
    key: requiredString(object, "key"),
    sets,
    predicate: parseExpression(required(object, "where")),
  };
}

export function parseSet(value: unknown): VisibilitySet {
  const object = record(value, "VisibilitySet");
  return {
    kind: optionalString(object, "kind"),
    table: requiredString(object, "table"),
    alias: optionalString(object, "alias"),
    select: requiredString(object, "select"),
    selectFrom: optionalString(object, "selectFrom"),
    joins: list(object["joins"], "joins").map(parseJoin),
    constraints: list(object["where"], "where").map(parseConstraint),
    from: optionalString(object, "from"),
    key: optionalString(object, "key"),
  };
}

function parseJoin(value: unknown): VisibilityJoin {
  const object = record(value, "VisibilityJoin");
  return {
    table: requiredString(object, "table"),
    alias: optionalString(object, "alias"),
    leftAlias: optionalString(object, "leftAlias"),
    leftColumn: requiredString(object, "leftColumn"),
    rightColumn: requiredString(object, "rightColumn"),
  };
}

export function parseConstraint(value: unknown): VisibilityConstraint {
  const object = record(value, "VisibilityConstraint");
  return {
    table: optionalString(object, "table"),
    column: requiredString(object, "column"),
    context: optionalString(object, "context"),
    value: optionValue(object["value"]),
    inSet: optionalString(object, "inSet"),
  };
}

export function parseExpression(value: unknown): VisibilityExpression {
  const object = record(value, "VisibilityExpression");
  const onMissing = object["onMissing"];
  const predicate = object["predicate"];
  const via = object["via"];
  return {
    operator: requiredString(object, "operator"),
    column: optionalString(object, "column"),
    context: optionalString(object, "context"),
    set: optionalString(object, "set"),
    value: optionValue(object["value"]),
    children: list(object["children"], "children").map(parseExpression),
    onMissing: onMissing === undefined || onMissing === null ? undefined : parseExpression(onMissing),
    table: optionalString(object, "table"),
    key: optionalString(object, "key"),
    parentWhere: list(object["where"], "where").map(parseConstraint),
    predicate: predicate === undefined || predicate === null ? undefined : parseExpression(predicate),
    via: via === undefined || via === null ? undefined : parseVia(via),
  };
}

function parseVia(value: unknown): VisibilityVia {
  const object = record(value, "VisibilityVia");
  return {
    table: requiredString(object, "table"),
    childColumn: requiredString(object, "childColumn"),
    parentColumn: requiredString(object, "parentColumn"),
  };
}

function parseError(detail: string): VisibilityError {
  return VisibilityError.invalid(detail);
}

function record(value: unknown, expected: string): Record<string, unknown> {
  if (!isObject(value as JsonValue)) throw parseError(`invalid type: expected struct ${expected}`);
  return value as Record<string, unknown>;
}

function required(object: Record<string, unknown>, key: string): unknown {
  if (!Object.hasOwn(object, key)) throw parseError(`missing field \`${key}\``);
  return object[key];
}

function requiredString(object: Record<string, unknown>, key: string): string {
  const value = required(object, key);
  if (typeof value !== "string") throw parseError(`invalid type for \`${key}\`: expected a string`);
  return value;
}

function optionalString(object: Record<string, unknown>, key: string): string {
  const value = object[key];
  if (value === undefined || !Object.hasOwn(object, key)) return "";
  if (typeof value !== "string") throw parseError(`invalid type for \`${key}\`: expected a string`);
  return value;
}

function list(value: unknown, key: string): unknown[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw parseError(`invalid type for \`${key}\`: expected a sequence`);
  return value;
}

function stringArray(value: unknown, key: string): string[] {
  return list(value, key).map((item) => {
    if (typeof item !== "string") throw parseError(`invalid type in \`${key}\`: expected a string`);
    return item;
  });
}

function optionValue(value: unknown): JsonValue | undefined {
  return value === undefined || value === null ? undefined : (value as JsonValue);
}

/** The plan as manifest JSON (serde's serialization, keys in field order). */
export function planToJson(plan: VisibilityPlan): JsonValue {
  const constraint = (c: VisibilityConstraint): JsonValue => ({
    table: c.table,
    column: c.column,
    context: c.context,
    value: c.value ?? null,
    ...(c.inSet ? { inSet: c.inSet } : {}),
  });
  const expression = (e: VisibilityExpression): JsonValue => ({
    ...(e.onMissing ? { onMissing: expression(e.onMissing) } : {}),
    ...(e.table ? { table: e.table } : {}),
    ...(e.key ? { key: e.key } : {}),
    ...(e.parentWhere.length ? { where: e.parentWhere.map(constraint) } : {}),
    ...(e.predicate ? { predicate: expression(e.predicate) } : {}),
    ...(e.via ? { via: { table: e.via.table, childColumn: e.via.childColumn, parentColumn: e.via.parentColumn } } : {}),
    operator: e.operator,
    column: e.column,
    context: e.context,
    set: e.set,
    value: e.value ?? null,
    children: e.children.map(expression),
  });
  const sets: Record<string, JsonValue> = {};
  for (const [name, set] of setEntries(plan)) sets[name] = setToJson(set, constraint);
  return {
    version: plan.version ?? null,
    permissionTables: [...plan.permissionTables],
    table: plan.table,
    key: plan.key,
    sets,
    where: expression(plan.predicate),
  };
}

function setToJson(set: VisibilitySet, constraint: (c: VisibilityConstraint) => JsonValue): JsonValue {
  return {
    kind: set.kind,
    table: set.table,
    alias: set.alias,
    select: set.select,
    selectFrom: set.selectFrom,
    joins: set.joins.map((j) => ({ table: j.table, alias: j.alias, leftAlias: j.leftAlias, leftColumn: j.leftColumn, rightColumn: j.rightColumn })),
    where: set.constraints.map(constraint),
    ...(set.from ? { from: set.from } : {}),
    ...(set.key ? { key: set.key } : {}),
  };
}

/** Sets in BTreeMap order. */
export function setEntries(plan: VisibilityPlan): [string, VisibilitySet][] {
  return [...plan.sets].sort(([a], [b]) => compareText(a, b));
}

export function setOf(plan: VisibilityPlan, name: string): VisibilitySet {
  const set = plan.sets.get(name);
  if (!set) throw new Error(`visibility set ${debugString(name)} is not declared`);
  return set;
}

// ---- per-plan caches (Rust: `active_sets_cache`, shared by clones) ------------

interface PlanCaches {
  sets: Map<string, ReadonlySet<string>>;
  inherited?: Map<string, Set<string>>;
  inheritsAny?: VisibilityExpression[];
  principalTables?: Set<string>;
  principalInputs?: Map<string, PrincipalInputs>;
  setsSignature?: string;
  fingerprintJson?: string;
}

const caches = new WeakMap<VisibilityPlan, PlanCaches>();

export function planCaches(plan: VisibilityPlan): PlanCaches {
  let found = caches.get(plan);
  if (!found) {
    found = { sets: new Map() };
    caches.set(plan, found);
  }
  return found;
}

/** Drops cached analyses after a test or caller mutates a plan in place. */
export function forgetPlanCaches(plan: VisibilityPlan): void {
  caches.delete(plan);
}

// ---- identifiers ----------------------------------------------------------------

function isIdentifier(value: string): boolean {
  if (value.length === 0 || value.length > 63) return false;
  for (let index = 0; index < value.length; index++) {
    const code = value.charCodeAt(index);
    const alpha = (code >= 65 && code <= 90) || (code >= 97 && code <= 122);
    if (!(alpha || code === 95 || (index > 0 && code >= 48 && code <= 57))) return false;
  }
  return true;
}

export function identifier(value: string): string {
  if (isIdentifier(value)) return value;
  throw VisibilityError.invalid(`invalid SQL identifier ${debugString(value)}`);
}

export function quote(value: string): string {
  return `"${identifier(value)}"`;
}

export function contextKey(value: string): void {
  if (value === "account.id" || value === "member.id" || value === "tenant.id") return;
  throw VisibilityError.invalid(`unsupported context ${debugString(value)}`);
}

/** `visibility_literal`: `{literal: value}` and nothing else. */
export function visibilityLiteral(value: JsonValue | undefined): JsonValue {
  if (!isObject(value)) throw VisibilityError.invalid("visibility literal must be an object");
  const keys = Object.keys(value);
  if (keys.length !== 1 || !Object.hasOwn(value, "literal")) throw VisibilityError.invalid("visibility literal must contain only literal");
  return value["literal"]!;
}

/** `value.get("literal")` of an `Option<Value>`. */
export function literalOf(value: JsonValue | undefined): JsonValue | undefined {
  return isObject(value) && Object.hasOwn(value, "literal") ? value["literal"] : undefined;
}

// ---- validation -------------------------------------------------------------------

export function validatePlan(plan: VisibilityPlan): void {
  identifier(plan.table);
  identifier(plan.key);
  if (plan.version !== undefined && plan.version !== 2) throw VisibilityError.invalid("visibility version must be 2");
  const permissions = new Set(plan.permissionTables);
  for (const table of plan.permissionTables) identifier(table);
  if (permissions.size !== plan.permissionTables.length) throw VisibilityError.invalid("permissionTables contains duplicates");
  for (const [name, set] of setEntries(plan)) {
    if (plan.version === 2) {
      const fix = "use a permission-only principal set, a single-source grant set, or inherits";
      const invalid = (reason: string) => VisibilityError.invalid(`set ${debugString(name)}: ${reason}; ${fix}`);
      if (set.kind !== "principal" && set.kind !== "grant" && set.kind !== "derived") throw invalid("kind must be principal, grant or derived");
      if (set.kind === "derived") {
        identifier(set.key);
        const source = plan.sets.get(set.from);
        if (!source) throw invalid(`derived from unknown set ${debugString(set.from)}`);
        if (source.kind === "derived" && plan.sets.get(source.from)?.kind === "derived") throw invalid("derived chains are limited to two sets");
        if (set.from === name || (source.kind === "derived" && source.from === name)) throw invalid("derived sets must be acyclic");
        if (set.table === plan.table) throw invalid("a derived set maps through another table");
        if (set.joins.length) throw invalid("a derived set has no joins");
        for (const constraint of set.constraints) {
          if (constraint.table) throw invalid("derived filters name indexed-row columns only");
          if (constraint.inSet) {
            const other = plan.sets.get(constraint.inSet);
            if (!(other && (other.kind === "principal" || other.kind === "grant"))) {
              throw invalid(`derived inSet ${debugString(constraint.inSet)} must name a principal or grant set`);
            }
          }
        }
      }
      // A grant set may read its own source table; a principal set over its
      // own source would make every source write a principal change (V2),
      // unless the source is itself a declared permission table (F2).
      const sourceIsPermission = permissions.has(plan.table);
      if (!sourceIsPermission && ((set.kind !== "grant" && set.table === plan.table) || set.joins.some((join) => join.table === plan.table))) {
        throw invalid("reads its own source table");
      }
      if (set.kind === "principal" && !permissions.has(set.table)) throw invalid(`undeclared permission table ${debugString(set.table)}`);
      if (set.kind !== "derived") {
        for (const join of set.joins) if (!permissions.has(join.table)) throw invalid(`joins data table ${debugString(join.table)}`);
      }
      if (set.kind === "grant" && !set.constraints.some((c) => c.context === "member.id" || c.context === "account.id")) {
        throw invalid("grant requires member.id or account.id anchor");
      }
      if (set.constraints.some((c) => c.context === "tenant.id")) throw invalid("tenant.id context is forbidden");
    }
    identifier(name);
    validateSet(set);
  }
  validateExpression(plan.predicate, plan.sets);
}

function validateSet(set: VisibilitySet): void {
  identifier(set.table);
  identifier(set.select);
  let baseAlias = set.table;
  if (set.alias) {
    identifier(set.alias);
    baseAlias = set.alias;
  }
  const aliases = new Set([baseAlias]);
  const physical = new Map<string, number>([[set.table, 1]]);
  for (const join of set.joins) {
    identifier(join.table);
    identifier(join.leftColumn);
    identifier(join.rightColumn);
    let alias = join.table;
    if (join.alias) {
      identifier(join.alias);
      alias = join.alias;
    }
    if (aliases.has(alias)) throw VisibilityError.invalid(`visibility alias ${debugString(alias)} is repeated`);
    aliases.add(alias);
    physical.set(join.table, (physical.get(join.table) ?? 0) + 1);
  }
  for (const [table, count] of physical) {
    if (count > 1 && ((table === set.table && !set.alias) || set.joins.some((join) => join.table === table && !join.alias))) {
      throw VisibilityError.invalid("every occurrence of a repeated visibility table requires an alias");
    }
  }
  for (const constraint of set.constraints) {
    identifier(constraint.column);
    if (constraint.table) {
      identifier(constraint.table);
      if (!aliases.has(constraint.table)) throw VisibilityError.invalid(`unknown constraint alias ${debugString(constraint.table)}`);
    }
    const hasContext = constraint.context !== "";
    const hasValue = constraint.value !== undefined;
    const hasSet = constraint.inSet !== "";
    if ([hasContext, hasValue, hasSet].filter(Boolean).length !== 1) {
      throw VisibilityError.invalid("visibility constraint requires exactly one of context, value or inSet");
    }
    if (hasSet && set.kind !== "derived") throw VisibilityError.invalid("inSet constraints are allowed only on derived sets");
    if (hasContext) contextKey(constraint.context);
    else if (hasValue) visibilityLiteral(constraint.value);
  }
}

function nestedInheritance(expression: VisibilityExpression): boolean {
  return expression.operator === "inherits" || expression.operator === "inheritsAny" || expression.children.some(nestedInheritance);
}

function validateExpression(expression: VisibilityExpression, sets: ReadonlyMap<string, VisibilitySet>): void {
  switch (expression.operator) {
    case "public":
      break;
    case "permission":
    case "role":
      if (typeof expression.value !== "string" || expression.value.trim() === "") {
        throw VisibilityError.invalid(`${expression.operator} requires a value`);
      }
      break;
    case "eq":
      identifier(expression.column);
      visibilityLiteral(expression.value);
      break;
    case "eqContext":
      identifier(expression.column);
      contextKey(expression.context);
      break;
    case "inherits":
    case "inheritsAny":
      identifier(expression.column);
      if (expression.operator === "inheritsAny") {
        const via = expression.via;
        if (!via) throw VisibilityError.invalid("inheritsAny requires via");
        identifier(via.table);
        identifier(via.childColumn);
        identifier(via.parentColumn);
      }
      identifier(expression.table);
      identifier(expression.key);
      if (expression.predicate) {
        if (nestedInheritance(expression.predicate)) throw VisibilityError.invalid("an inherited parent predicate cannot inherit again");
        validateExpression(expression.predicate, sets);
      }
      for (const constraint of expression.parentWhere) {
        identifier(constraint.column);
        if (constraint.context !== "" || constraint.value === undefined) throw VisibilityError.invalid("inherited parent filters are literals only");
        visibilityLiteral(constraint.value);
      }
      break;
    case "inSet":
    case "nonEmpty":
      if (expression.operator === "inSet") identifier(expression.column);
      if (!sets.has(expression.set)) throw VisibilityError.invalid(`unknown visibility set ${debugString(expression.set)}`);
      break;
    case "and":
    case "or":
      if (expression.children.length === 0) throw VisibilityError.invalid(`${expression.operator} requires children`);
      break;
    case "not":
      if (expression.children.length !== 1) throw VisibilityError.invalid("not requires one child");
      break;
    default:
      throw VisibilityError.invalid(`unsupported operator ${debugString(expression.operator)}`);
  }
  for (const child of expression.children) validateExpression(child, sets);
  if (expression.onMissing) {
    if (expression.operator !== "inherits") throw VisibilityError.invalid("onMissing is allowed only on inherits");
    validateExpression(expression.onMissing, sets);
  }
}

/** Cross-plan contracts, checked once when a module is admitted (`validate_manifest`). */
export function validateManifest(plans: ReadonlyMap<string, VisibilityPlan>): void {
  const parents = (expression: VisibilityExpression, output: VisibilityExpression[]) => {
    if (expression.operator === "inherits") output.push(expression);
    for (const child of expression.children) parents(child, output);
    if (expression.onMissing) parents(expression.onMissing, output);
  };
  const visit = (name: string, path: Set<string>, depth: number): void => {
    if (depth > 4) throw VisibilityError.invalid(`inheritance exceeds four edges at ${debugString(name)}`);
    if (path.has(name)) throw VisibilityError.invalid(`inheritance cycle at ${debugString(name)}`);
    path.add(name);
    const plan = plans.get(name)!;
    const references: VisibilityExpression[] = [];
    parents(plan.predicate, references);
    for (const reference of references) {
      const parent = plans.get(reference.table);
      if (!parent) throw VisibilityError.invalid(`plan ${debugString(name)} inherits unknown parent ${debugString(reference.table)}`);
      if (plan.version !== 2 || parent.version !== 2) throw VisibilityError.invalid(`plan ${debugString(name)}: inherits requires version 2 child and parent`);
      if (reference.key !== parent.key) {
        throw VisibilityError.invalid(`plan ${debugString(name)}: inherited key ${debugString(reference.key)} differs from parent key ${debugString(parent.key)}`);
      }
      visit(reference.table, path, depth + 1);
    }
    path.delete(name);
  };
  let declaration: string | undefined;
  for (const name of sortedText(plans.keys())) {
    const plan = plans.get(name)!;
    validatePlan(plan);
    if (plan.version === 2) {
      const tables = JSON.stringify(sortedText(new Set(plan.permissionTables)));
      if (declaration !== undefined && declaration !== tables) {
        throw VisibilityError.invalid(`plan ${debugString(name)}: permissionTables must agree across version 2 plans`);
      }
      declaration = tables;
    }
    visit(name, new Set(), 0);
  }
}

// ---- role pruning and active sets ---------------------------------------------------

/**
 * Only constants and the admitted role: row values, set contents and
 * permissions stay unknown. SQL compilation uses the same pruning for v2
 * masks, so it never asks for a set the resolver deliberately omitted.
 */
export function roleOnlyValue(expression: VisibilityExpression, role: string): boolean | undefined {
  switch (expression.operator) {
    case "public":
      return true;
    case "role":
      return typeof expression.value === "string" ? expression.value === role : undefined;
    case "and":
    case "or": {
      if (expression.children.length === 0) return undefined;
      const isAnd = expression.operator === "and";
      let unknown = false;
      for (const child of expression.children) {
        const value = roleOnlyValue(child, role);
        if (value === undefined) unknown = true;
        else if (value !== isAnd) return value;
      }
      return unknown ? undefined : isAnd;
    }
    case "not": {
      if (expression.children.length !== 1) return undefined;
      const value = roleOnlyValue(expression.children[0]!, role);
      return value === undefined ? undefined : !value;
    }
    default:
      return undefined;
  }
}

/** Sets the predicate references for this role, plus every set a `derived` set maps from, transitively. Sorted. */
export function activeSets(plan: VisibilityPlan, role: string): ReadonlySet<string> {
  const cache = planCaches(plan).sets;
  const found = cache.get(role);
  if (found) return found;
  const active = new Set<string>();
  activeVisibilitySets(plan.predicate, role, active);
  const pending = [...active];
  while (pending.length) {
    const name = pending.pop()!;
    const set = plan.sets.get(name);
    if (set?.kind !== "derived") continue;
    for (const needed of [set.from, ...set.constraints.map((c) => c.inSet)]) {
      if (needed && !active.has(needed)) {
        active.add(needed);
        pending.push(needed);
      }
    }
  }
  const sorted: ReadonlySet<string> = new Set(sortedText(active));
  cache.set(role, sorted);
  return sorted;
}

function activeVisibilitySets(expression: VisibilityExpression, role: string, sets: Set<string>): void {
  if (roleOnlyValue(expression, role) !== undefined) return;
  if (expression.operator === "inSet" || expression.operator === "nonEmpty") sets.add(expression.set);
  for (const child of expression.children) activeVisibilitySets(child, role, sets);
  if (expression.onMissing) activeVisibilitySets(expression.onMissing, role, sets);
  if (expression.predicate) activeVisibilitySets(expression.predicate, role, sets);
}

// ---- columns, parents and indexes ---------------------------------------------------

/** Row columns a plan's predicate reads, including inherited foreign keys and the key. */
export function aclColumns(plan: VisibilityPlan): Set<string> {
  const columns = new Set<string>([plan.key]);
  const visit = (expression: VisibilityExpression) => {
    if ((expression.operator === "eq" || expression.operator === "eqContext" || expression.operator === "inSet" || expression.operator === "inherits") && expression.column) {
      columns.add(expression.column);
    }
    for (const child of expression.children) visit(child);
    if (expression.onMissing) visit(expression.onMissing);
  };
  visit(plan.predicate);
  return columns;
}

/** Index name of an `inheritsAny` association, keyed by one of its columns. */
export function viaIndexName(via: VisibilityVia, column: string): string {
  return `via\u0000${via.table}\u0000${column}`;
}

/** Every `inheritsAny` expression of a plan. */
export function inheritsAny(plan: VisibilityPlan): readonly VisibilityExpression[] {
  const cache = planCaches(plan);
  if (!cache.inheritsAny) {
    const output: VisibilityExpression[] = [];
    const visit = (expression: VisibilityExpression) => {
      if (expression.operator === "inheritsAny") output.push(expression);
      for (const child of expression.children) visit(child);
      if (expression.onMissing) visit(expression.onMissing);
    };
    visit(plan.predicate);
    cache.inheritsAny = output;
  }
  return cache.inheritsAny;
}

/** Tables a version 2 plan inherits from, directly. Sorted. */
export function inheritedTables(plan: VisibilityPlan): string[] {
  const tables = new Set<string>();
  const visit = (expression: VisibilityExpression) => {
    if (expression.operator === "inherits" || expression.operator === "inheritsAny") tables.add(expression.table);
    for (const child of expression.children) visit(child);
    if (expression.onMissing) visit(expression.onMissing);
  };
  visit(plan.predicate);
  return sortedText(tables);
}

/** Child columns that reference each directly inherited parent table (`inherits` only). */
export function inheritedColumns(plan: VisibilityPlan): ReadonlyMap<string, ReadonlySet<string>> {
  const cache = planCaches(plan);
  if (!cache.inherited) {
    const output = new Map<string, Set<string>>();
    const visit = (expression: VisibilityExpression) => {
      if (expression.operator === "inherits") {
        let columns = output.get(expression.table);
        if (!columns) output.set(expression.table, (columns = new Set()));
        columns.add(expression.column);
      }
      for (const child of expression.children) visit(child);
      if (expression.onMissing) visit(expression.onMissing);
    };
    visit(plan.predicate);
    cache.inherited = output;
  }
  return cache.inherited;
}

/** (table, key column) of every index `resolveWithParents` binds for `plan`, sorted. */
export function requiredIndexes(plans: ReadonlyMap<string, VisibilityPlan>, plan: VisibilityPlan): Array<[string, string]> {
  const output = new Map<string, [string, string]>();
  const add = (table: string, key: string) => output.set(`${table}\u0000${key}`, [table, key]);
  const seen = new Set<string>();
  const visit = (plan: VisibilityPlan) => {
    if (plan.version !== 2 || seen.has(plan.table)) return;
    seen.add(plan.table);
    for (const table of inheritedTables(plan)) {
      const parent = plans.get(table);
      if (!parent) throw VisibilityError.invalid(`unknown inherited parent ${debugString(table)}`);
      add(parent.table, parent.key);
      visit(parent);
    }
    for (const expression of inheritsAny(plan)) {
      if (!expression.via) continue;
      add(expression.via.table, expression.via.childColumn);
      add(expression.via.table, expression.via.parentColumn);
    }
    for (const set of plan.sets.values()) if (set.kind === "derived") add(set.table, set.key);
  };
  visit(plan);
  return [...output.values()].sort(([a, x], [b, y]) => compareText(a, b) || compareText(x, y));
}

/** Columns a tenant index of `table` keyed by `key` projects. Sorted. */
export function indexColumns(plans: ReadonlyMap<string, VisibilityPlan>, table: string, key: string): string[] {
  const columns = new Set<string>([key]);
  const own = plans.get(table);
  if (own && own.key === key) for (const column of aclColumns(own)) columns.add(column);
  const read = (expression: VisibilityExpression) => {
    if ((expression.operator === "eq" || expression.operator === "eqContext" || expression.operator === "inSet") && expression.column) columns.add(expression.column);
    for (const nested of expression.children) read(nested);
  };
  const parentFilters = (expression: VisibilityExpression) => {
    if ((expression.operator === "inherits" || expression.operator === "inheritsAny") && expression.table === table) {
      for (const constraint of expression.parentWhere) columns.add(constraint.column);
      if (expression.predicate) read(expression.predicate);
    }
    for (const child of expression.children) parentFilters(child);
    if (expression.onMissing) parentFilters(expression.onMissing);
  };
  for (const plan of plans.values()) {
    if (own && own.key === key) parentFilters(plan.predicate);
    for (const expression of inheritsAny(plan)) {
      const via = expression.via;
      if (!via) continue;
      if (via.table === table && via.childColumn === key) columns.add(via.parentColumn);
      if (via.table === table && via.parentColumn === key) columns.add(via.childColumn);
    }
    for (const set of plan.sets.values()) {
      if (set.kind === "derived" && set.table === table && set.key === key) {
        columns.add(set.select);
        for (const constraint of set.constraints) columns.add(constraint.column);
      }
    }
  }
  return sortedText(columns);
}

/** The column the change feed uses as a table's row id: its plan key, or `_id`. */
export function rowIdColumn(plans: ReadonlyMap<string, VisibilityPlan>, table: string): string {
  return plans.get(table)?.key ?? "_id";
}

// ---- dependencies and principal inputs ----------------------------------------------

/** Tables and columns a set (or a plan's sets) reads. */
export class VisibilityDependencies {
  constructor(readonly columns: ReadonlyMap<string, ReadonlySet<string>>) {}

  tables(): string[] {
    return sortedText(this.columns.keys());
  }

  changeAffects(table: string, operation: string, changedColumns: readonly string[]): boolean {
    const relevant = this.columns.get(table);
    if (!relevant) return false;
    if (operation.toLowerCase() !== "update") return true;
    return changedColumns.some((column) => relevant.has(column));
  }
}

const MEMBER_COLUMNS = ["id", "account_id", "status", "role", "permissions"];

export function dependencyColumnsForSets(plan: VisibilityPlan, active: ReadonlySet<string> | undefined, includeMember: boolean): VisibilityDependencies {
  const columns = new Map<string, Set<string>>();
  const add = (table: string, column: string) => {
    let set = columns.get(table);
    if (!set) columns.set(table, (set = new Set()));
    set.add(column);
  };
  if (includeMember) for (const column of MEMBER_COLUMNS) add("members", column);
  for (const [name, set] of setEntries(plan)) {
    if (active && !active.has(name)) continue;
    const baseAlias = set.alias || set.table;
    const aliases = new Map<string, string>([[baseAlias, set.table]]);
    let previous = baseAlias;
    for (const join of set.joins) {
      const leftAlias = join.leftAlias || previous;
      const leftTable = aliases.get(leftAlias);
      if (leftTable !== undefined) add(leftTable, join.leftColumn);
      add(join.table, join.rightColumn);
      const alias = join.alias || join.table;
      aliases.set(alias, join.table);
      previous = alias;
    }
    const selectTable = aliases.get(set.selectFrom || baseAlias);
    if (selectTable !== undefined) add(selectTable, set.select);
    for (const constraint of set.constraints) {
      const table = constraint.table ? aliases.get(constraint.table) : set.table;
      if (table !== undefined) add(table, constraint.column);
    }
  }
  return new VisibilityDependencies(columns);
}

export function dependencyColumns(plan: VisibilityPlan): VisibilityDependencies {
  return dependencyColumnsForSets(plan, undefined, true);
}

export interface PrincipalInputs {
  dependencies: VisibilityDependencies;
  /** alias → (column, context) pairs that anchor that occurrence on the principal. */
  anchors: Map<string, Set<string>>;
}

/** Encodes an anchor pair for a Set. */
export function anchorPair(column: string, context: string): string {
  return `${column}\u0000${context}`;
}

/**
 * Propagates context equalities through inner equijoins: a `members`
 * occurrence joined on memberTeams.memberId = members.id is anchored even when
 * the context constraint was declared only on memberTeams. Never inferred from
 * an unrelated join column.
 */
export function principalAnchors(set: VisibilitySet): Map<string, Set<string>> {
  const base = set.alias || set.table;
  const anchors = new Map<string, Set<string>>();
  const insert = (alias: string, column: string, context: string): boolean => {
    let found = anchors.get(alias);
    if (!found) anchors.set(alias, (found = new Set()));
    const pair = anchorPair(column, context);
    if (found.has(pair)) return false;
    found.add(pair);
    return true;
  };
  for (const constraint of set.constraints) {
    if (constraint.context) insert(constraint.table || base, constraint.column, constraint.context);
  }
  const edges: Array<[[string, string], [string, string]]> = [];
  let previous = base;
  for (const join of set.joins) {
    const left = join.leftAlias || previous;
    const right = join.alias || join.table;
    edges.push([
      [left, join.leftColumn],
      [right, join.rightColumn],
    ]);
    previous = right;
  }
  for (;;) {
    let changed = false;
    for (const [left, right] of edges) {
      for (const [from, to] of [
        [left, right],
        [right, left],
      ] as const) {
        const contexts = [...(anchors.get(from[0]) ?? [])]
          .map((pair) => pair.split("\u0000") as [string, string])
          .filter(([column]) => column === from[1])
          .map(([, context]) => context);
        for (const context of contexts) changed = insert(to[0], to[1], context) || changed;
      }
    }
    if (!changed) break;
  }
  return anchors;
}

export function principalInputs(plan: VisibilityPlan): ReadonlyMap<string, PrincipalInputs> {
  const cache = planCaches(plan);
  if (!cache.principalInputs) {
    cache.principalInputs = new Map(
      setEntries(plan).map(([name, set]) => [name, { dependencies: dependencyColumnsForSets(plan, new Set([name]), false), anchors: principalAnchors(set) }]),
    );
  }
  return cache.principalInputs;
}

/** Tables any non-derived set reads (base or join), for every role. */
export function principalTables(plan: VisibilityPlan): ReadonlySet<string> {
  const cache = planCaches(plan);
  if (!cache.principalTables) {
    const tables = new Set<string>();
    for (const set of plan.sets.values()) {
      if (set.kind === "derived") continue;
      tables.add(set.table);
      for (const join of set.joins) tables.add(join.table);
    }
    cache.principalTables = tables;
  }
  return cache.principalTables;
}

/**
 * Version 2: a member- and role-independent superset of
 * `principalChangeConcerns` for this plan alone: false means the change
 * concerns no member's principal.
 */
export function principalChangePossible(plan: VisibilityPlan, table: string, operation: string, changedColumns: readonly string[]): boolean {
  if (table === "members" && (operation.toLowerCase() !== "update" || changedColumns.some((column) => MEMBER_COLUMNS.includes(column)))) return true;
  const inputs = principalInputs(plan);
  for (const [name, set] of setEntries(plan)) {
    if (set.kind === "derived") continue;
    const reads = (set.kind !== "grant" && set.table === table) || set.joins.some((join) => join.table === table);
    if (reads && inputs.get(name)!.dependencies.changeAffects(table, operation, changedColumns)) return true;
  }
  return false;
}

export function memberColumnChanged(operation: string, changedColumns: readonly string[]): boolean {
  return operation.toLowerCase() !== "update" || changedColumns.some((column) => MEMBER_COLUMNS.includes(column));
}

/** SHA-256 input of the plan's version and set definitions (`principal_cache::sets_signature`). */
export function setsSignatureText(plan: VisibilityPlan): string {
  const cache = planCaches(plan);
  if (cache.setsSignature === undefined) {
    const json = planToJson(plan) as Record<string, JsonValue>;
    cache.setsSignature = serdeJson([json["version"]!, json["sets"]!]);
  }
  return cache.setsSignature;
}

/** Canonical JSON of the whole plan (fingerprints). */
export function planJsonText(plan: VisibilityPlan): string {
  const cache = planCaches(plan);
  if (cache.fingerprintJson === undefined) cache.fingerprintJson = serdeJson(planToJson(plan));
  return cache.fingerprintJson;
}
