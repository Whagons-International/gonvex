// Compiled visibility programs and shared visibility classes
// (visibility_class.rs). Pure and synchronous: no database handle (V1). The
// interpreter in mask.ts is the reference; this module must agree with it,
// and with the SQL compiler, on every row and principal (corpus tests).
//
// A version 2 plan compiles once into a flat node array over column slots:
//  - class atoms depend on shared tokens only: `role`, `permission`,
//    `nonEmpty` (one bit each) and `inSet` over principal or principal-derived
//    sets (tokens interned to integers, sorted);
//  - member atoms depend on one member: `eqContext`, and `inSet` over grant
//    sets, grant-derived sets and member-owned principal sets;
//  - row atoms: `eq` literals.
// Members whose class atoms resolve identically share one class. A member
// that no member atom hits on a row sees exactly what its class's default
// (member atoms false) sees, so the default is evaluated once per class per
// row and only hit members are evaluated alone.
//
// Lifetimes: Rust frees a class when its last member handle drops. Here a
// handle is released explicitly (`MemberVisibility.release()`, or
// `HandleCache.clear()`); a class leaves its scope when its last member is
// released. A handle that is never released keeps its class registered.

import { sha256Hex } from "../capture/digest.ts";
import type { ParentAclIndex } from "./acl-index.ts";
import { field, get, isObject, jsonEqual, scalar, serdeJson, sortedText, truthy, type JsonValue } from "./json.ts";
import { literalConstraintsHold } from "./mask.ts";
import { literalOf, planJsonText, type VisibilityConstraint, type VisibilityExpression, type VisibilityPlan } from "./plan.ts";
import type { ResolvedVisibility } from "./resolved.ts";

// ---- program --------------------------------------------------------------------

type Node =
  | { readonly t: "const"; readonly value: boolean }
  | { readonly t: "bit"; readonly bit: number }
  | { readonly t: "eq"; readonly column: number; readonly literal: JsonValue }
  | { readonly t: "inClass"; readonly column: number; readonly set: number }
  | { readonly t: "inMember"; readonly column: number; readonly set: number }
  | { readonly t: "context"; readonly column: number; readonly context: number }
  | { readonly t: "inherits"; readonly spec: number }
  | { readonly t: "and" | "or"; readonly start: number; readonly length: number }
  | { readonly t: "not"; readonly child: number };

type BitAtom = { readonly kind: "role" | "permission" | "nonEmpty"; readonly name: string };

type MemberAtom = { readonly kind: "inSet"; readonly column: number; readonly set: number } | { readonly kind: "context"; readonly column: number; readonly context: number };

interface InheritsSpec {
  readonly column: number;
  readonly table: string;
  readonly parentWhere: readonly VisibilityConstraint[];
  /** Child-principal predicate on the parent row, over `overrideColumns`. */
  readonly overrideRoot: number | undefined;
  readonly overrideColumns: readonly string[];
  readonly overrideMemberAtoms: readonly MemberAtom[];
  readonly onMissing: number | undefined;
}

interface Residual {
  readonly shape: readonly number[];
  readonly sets: readonly boolean[];
}

/** Whether a set's tokens are member-specific (grant, grant-derived, or member-owned principal). */
function memberSet(plan: VisibilityPlan, name: string): boolean {
  let current = name;
  for (let step = 0; step < 8; step++) {
    const set = plan.sets.get(current);
    if (!set) return false;
    if (set.kind === "grant") return true;
    if (set.kind === "derived") {
      current = set.from;
      continue;
    }
    // Rows of the set's own table, selected by key and anchored on this
    // member (workspaces the member created): unique per member.
    const base = set.alias || set.table;
    return (
      set.joins.length === 0 &&
      (set.selectFrom === "" || set.selectFrom === base) &&
      (set.select === "_id" || set.select === "id") &&
      set.constraints.some((c) => c.context !== "" && (c.table === "" || c.table === base || c.table === set.table))
    );
  }
  return false;
}

interface Domain {
  columns: string[];
  memberAtoms: MemberAtom[];
  classAtoms: Array<[number, number]> | undefined;
  allowInherits: boolean;
}

function intern(names: string[], name: string): number {
  const index = names.indexOf(name);
  if (index >= 0) return index;
  names.push(name);
  return names.length - 1;
}

class Compiler {
  readonly nodes: Node[] = [];
  readonly kids: number[] = [];
  readonly bits: BitAtom[] = [];
  readonly classSets: string[] = [];
  readonly memberSets: string[] = [];
  readonly contexts: string[] = [];
  readonly inherits: InheritsSpec[] = [];
  unsupported: string | undefined;

  constructor(private readonly plan: VisibilityPlan) {}

  private push(node: Node): number {
    this.nodes.push(node);
    return this.nodes.length - 1;
  }

  private fail(reason: string): number {
    this.unsupported ??= reason;
    return this.push({ t: "const", value: false });
  }

  private bit(atom: BitAtom): number {
    let index = this.bits.findIndex((existing) => existing.kind === atom.kind && existing.name === atom.name);
    if (index < 0) {
      this.bits.push(atom);
      index = this.bits.length - 1;
    }
    return this.push({ t: "bit", bit: index });
  }

  compile(expression: VisibilityExpression, domain: Domain): number {
    switch (expression.operator) {
      case "public":
        return this.push({ t: "const", value: true });
      case "nonEmpty":
        return this.bit({ kind: "nonEmpty", name: expression.set });
      case "permission":
      case "role":
        return typeof expression.value === "string" ? this.bit({ kind: expression.operator, name: expression.value }) : this.push({ t: "const", value: false });
      case "eq": {
        const literal = literalOf(expression.value);
        if (literal === undefined) return this.push({ t: "const", value: false });
        return this.push({ t: "eq", column: intern(domain.columns, expression.column), literal });
      }
      case "eqContext": {
        const column = intern(domain.columns, expression.column);
        const context = intern(this.contexts, expression.context);
        domain.memberAtoms.push({ kind: "context", column, context });
        return this.push({ t: "context", column, context });
      }
      case "inSet": {
        const column = intern(domain.columns, expression.column);
        if (memberSet(this.plan, expression.set)) {
          const set = intern(this.memberSets, expression.set);
          domain.memberAtoms.push({ kind: "inSet", column, set });
          return this.push({ t: "inMember", column, set });
        }
        const set = intern(this.classSets, expression.set);
        if (domain.classAtoms && !domain.classAtoms.some(([c, s]) => c === column && s === set)) domain.classAtoms.push([column, set]);
        return this.push({ t: "inClass", column, set });
      }
      case "inherits": {
        if (!domain.allowInherits) return this.fail("inherits inside a parent predicate");
        const column = intern(domain.columns, expression.column);
        const overrideColumns: string[] = [];
        const overrideMemberAtoms: MemberAtom[] = [];
        const overrideRoot = expression.predicate
          ? this.compile(expression.predicate, { columns: overrideColumns, memberAtoms: overrideMemberAtoms, classAtoms: undefined, allowInherits: false })
          : undefined;
        const onMissing = expression.onMissing ? this.compile(expression.onMissing, domain) : undefined;
        this.inherits.push({ column, table: expression.table, parentWhere: expression.parentWhere, overrideRoot, overrideColumns, overrideMemberAtoms, onMissing });
        return this.push({ t: "inherits", spec: this.inherits.length - 1 });
      }
      case "and":
      case "or": {
        const children = expression.children.map((child) => this.compile(child, domain));
        const start = this.kids.length;
        this.kids.push(...children);
        return this.push({ t: expression.operator, start, length: children.length });
      }
      case "not":
        if (expression.children.length !== 1) return this.fail("not without exactly one child");
        return this.push({ t: "not", child: this.compile(expression.children[0]!, domain) });
      default:
        return this.fail(`operator ${expression.operator}`);
    }
  }
}

let nextClassId = 1;

/** One plan's program, built once per plan and shared by every binding of it. */
export class CompiledPlan {
  readonly table: string;
  /** Content hash of the plan. */
  readonly fingerprint: string;
  readonly columns: readonly string[];
  private readonly nodes: readonly Node[];
  private readonly kids: readonly number[];
  readonly root: number;
  readonly bits: readonly BitAtom[];
  readonly classSets: readonly string[];
  readonly memberSets: readonly string[];
  readonly contexts: readonly string[];
  readonly inherits: readonly InheritsSpec[];
  /** Distinct (column, class set) atoms of the row's own domain. */
  readonly classAtoms: ReadonlyArray<readonly [number, number]>;
  /** Member atoms of the row's own domain (including `onMissing`). */
  readonly memberAtoms: readonly MemberAtom[];
  /** Why this plan falls back to the interpreted evaluator, if it does. */
  readonly unsupported: string | undefined;
  private readonly scopes = new Map<string, ClassScope>();
  private readonly residuals = new Map<string, Residual>();

  constructor(plan: VisibilityPlan) {
    const compiler = new Compiler(plan);
    if (plan.version !== 2) compiler.unsupported = "legacy (unversioned) plan";
    const columns: string[] = [];
    const memberAtoms: MemberAtom[] = [];
    const classAtoms: Array<[number, number]> = [];
    this.root = compiler.compile(plan.predicate, { columns, memberAtoms, classAtoms, allowInherits: true });
    this.table = plan.table;
    this.fingerprint = sha256Hex(planJsonText(plan));
    this.columns = columns;
    this.nodes = compiler.nodes;
    this.kids = compiler.kids;
    this.bits = compiler.bits;
    this.classSets = compiler.classSets;
    this.memberSets = compiler.memberSets;
    this.contexts = compiler.contexts;
    this.inherits = compiler.inherits;
    this.classAtoms = classAtoms;
    this.memberAtoms = memberAtoms;
    this.unsupported = compiler.unsupported;
  }

  /** Whether the plan inherits visibility from parent rows. */
  get hasInherits(): boolean {
    return this.inherits.length > 0;
  }

  /** The class registry of one tenant database. */
  scope(name: string): ClassScope {
    let scope = this.scopes.get(name);
    if (!scope) this.scopes.set(name, (scope = new ClassScope(this)));
    return scope;
  }

  private children(start: number, length: number): readonly number[] {
    return this.kids.slice(start, start + length);
  }

  /** Row-independent value of a node given the class bits, if any. */
  fixed(node: number, bits: readonly boolean[]): boolean | undefined {
    const n = this.nodes[node]!;
    switch (n.t) {
      case "const":
        return n.value;
      case "bit":
        return bits[n.bit]!;
      case "and": {
        let all = true;
        for (const child of this.children(n.start, n.length)) {
          const value = this.fixed(child, bits);
          if (value === false) return false;
          if (value === undefined) all = false;
        }
        return all ? true : undefined;
      }
      case "or": {
        let none = true;
        for (const child of this.children(n.start, n.length)) {
          const value = this.fixed(child, bits);
          if (value === true) return true;
          if (value === undefined) none = false;
        }
        return none ? false : undefined;
      }
      case "not": {
        const value = this.fixed(n.child, bits);
        return value === undefined ? undefined : !value;
      }
      default:
        return undefined;
    }
  }

  /**
   * The residual program for these bits: the class sets an evaluation can
   * still consult, and the shape of fixed subtrees. Other sets never change
   * a decision, so they stay out of the class key.
   */
  residual(bits: readonly boolean[]): Residual {
    const cacheKey = bits.map((bit) => (bit ? "1" : "0")).join("");
    const found = this.residuals.get(cacheKey);
    if (found) return found;
    const sets = this.classSets.map(() => false);
    const shape: number[] = [];
    const mark = (node: number) => {
      const value = this.fixed(node, bits);
      if (value !== undefined) {
        shape.push(node * 4 + 2 + (value ? 1 : 0));
        return;
      }
      shape.push(node * 4);
      const n = this.nodes[node]!;
      if (n.t === "inClass") sets[n.set] = true;
      else if (n.t === "and" || n.t === "or") for (const child of this.children(n.start, n.length)) mark(child);
      else if (n.t === "not") mark(n.child);
      else if (n.t === "inherits") {
        const spec = this.inherits[n.spec]!;
        if (spec.overrideRoot !== undefined) mark(spec.overrideRoot);
        if (spec.onMissing !== undefined) mark(spec.onMissing);
      }
    };
    mark(this.root);
    const residual = { shape, sets };
    this.residuals.set(cacheKey, residual);
    return residual;
  }

  /** Default evaluation with class tokens false (Kleene): False means gated. */
  tri(node: number, bits: readonly boolean[]): "false" | "true" | "unknown" {
    const n = this.nodes[node]!;
    switch (n.t) {
      case "const":
        return n.value ? "true" : "false";
      case "bit":
        return bits[n.bit] ? "true" : "false";
      case "eq":
      case "inherits":
        return "unknown";
      case "inClass":
      case "inMember":
      case "context":
        return "false";
      case "and": {
        let result: "true" | "unknown" = "true";
        for (const child of this.children(n.start, n.length)) {
          const value = this.tri(child, bits);
          if (value === "false") return "false";
          if (value === "unknown") result = "unknown";
        }
        return result;
      }
      case "or": {
        let result: "false" | "unknown" = "false";
        for (const child of this.children(n.start, n.length)) {
          const value = this.tri(child, bits);
          if (value === "true") return "true";
          if (value === "unknown") result = "unknown";
        }
        return result;
      }
      case "not": {
        const value = this.tri(n.child, bits);
        return value === "true" ? "false" : value === "false" ? "true" : "unknown";
      }
    }
  }

  eval(node: number, row: AclRow, klass: VisibilityClass, member: MemberVisibility | undefined): boolean {
    const n = this.nodes[node]!;
    switch (n.t) {
      case "const":
        return n.value;
      case "bit":
        return klass.bits[n.bit]!;
      case "eq":
        return row.equals(n.column, n.literal);
      case "inClass": {
        const token = row.token(n.column);
        return token !== undefined && contains(klass.sets[n.set]!, token);
      }
      case "inMember": {
        if (!member) return false;
        const token = row.token(n.column);
        return token !== undefined && contains(member.memberSets[n.set]!, token);
      }
      case "context": {
        const expected = member?.contexts[n.context];
        return expected !== undefined && row.scalar(n.column) === expected;
      }
      case "inherits":
        return this.evalInherits(n.spec, row, klass, member);
      case "and":
        for (let at = n.start, end = n.start + n.length; at < end; at++) if (!this.eval(this.kids[at]!, row, klass, member)) return false;
        return true;
      case "or":
        for (let at = n.start, end = n.start + n.length; at < end; at++) if (this.eval(this.kids[at]!, row, klass, member)) return true;
        return false;
      case "not":
        return !this.eval(n.child, row, klass, member);
    }
  }

  private evalInherits(specIndex: number, row: AclRow, klass: VisibilityClass, member: MemberVisibility | undefined): boolean {
    const spec = this.inherits[specIndex]!;
    const parent = klass.parents[specIndex];
    if (!parent) return false;
    const key = row.value(spec.column);
    if (key === undefined || key === null) return false;
    const parentRow = parent.index.get(scalar(key));
    if (parentRow === undefined) return spec.onMissing !== undefined && this.eval(spec.onMissing, row, klass, member);
    if (!literalConstraintsHold(spec.parentWhere, parentRow) || !isObject(parentRow)) return false;
    if (spec.overrideRoot !== undefined) {
      const decoded = AclRow.fromValue(parentRow, spec.overrideColumns);
      decoded.resolveTokens(klass.scope);
      return this.eval(spec.overrideRoot, decoded, klass, member);
    }
    if (!parent.klass) return false;
    return parent.klass.visibleValue(parentRow, member?.parents[specIndex]);
  }
}

const compiledPlans = new WeakMap<VisibilityPlan, CompiledPlan>();

/** The compiled program of a plan, built on first use. */
export function compiled(plan: VisibilityPlan): CompiledPlan {
  let found = compiledPlans.get(plan);
  if (!found) compiledPlans.set(plan, (found = new CompiledPlan(plan)));
  return found;
}

// ---- rows -------------------------------------------------------------------------

/** A changed row: a parsed value, or the raw JSON object text the change log holds. */
export type RowView = { readonly value: JsonValue | undefined } | { readonly json: string };

export const rowValue = (value: JsonValue | undefined): RowView => ({ value });
export const rowJson = (json: string): RowView => ({ json });

/** The ACL columns of one row, with interned token ids per column. */
export class AclRow {
  private tokens: Array<number | undefined> | undefined;
  private generation = 0;

  private constructor(
    readonly object: boolean,
    private readonly values: ReadonlyArray<JsonValue | undefined>,
  ) {}

  static absent(): AclRow {
    return new AclRow(false, []);
  }

  static fromValue(value: JsonValue | undefined, columns: readonly string[]): AclRow {
    if (!isObject(value)) return AclRow.absent();
    return new AclRow(
      true,
      columns.map((column) => field(value, column)),
    );
  }

  static fromJson(text: string, columns: readonly string[]): AclRow {
    let value: unknown;
    try {
      value = JSON.parse(text);
    } catch {
      return AclRow.absent();
    }
    return AclRow.fromValue(value as JsonValue, columns);
  }

  /** Whether the row is a JSON object at all (non-objects are never visible). */
  get isRow(): boolean {
    return this.object;
  }

  value(column: number): JsonValue | undefined {
    return this.values[column];
  }

  scalar(column: number): string {
    return scalar(this.values[column]);
  }

  equals(column: number, literal: JsonValue): boolean {
    const value = this.values[column];
    return value !== undefined && jsonEqual(value, literal);
  }

  token(column: number): number | undefined {
    return this.tokens?.[column];
  }

  /** Re-resolves the row's tokens if the scope interned new ones since. */
  refresh(scope: ClassScope): void {
    if (this.stale(scope)) this.resolveTokens(scope);
  }

  stale(scope: ClassScope): boolean {
    return this.tokens === undefined || this.generation !== scope.generation;
  }

  resolveTokens(scope: ClassScope): void {
    this.generation = scope.generation;
    this.tokens = this.values.map((value) => (value === undefined || value === null ? undefined : scope.tokenId(typeof value === "string" ? value : scalar(value))));
  }
}

// ---- tokens ------------------------------------------------------------------------

/** A sorted, deduplicated token set. */
export type TokenSet = Uint32Array;

function contains(set: TokenSet, token: number): boolean {
  const length = set.length;
  if (length <= 8) {
    for (let index = 0; index < length; index++) if (set[index] === token) return true;
    return false;
  }
  let low = 0;
  let high = length - 1;
  while (low <= high) {
    const middle = (low + high) >>> 1;
    const value = set[middle]!;
    if (value === token) return true;
    if (value < token) low = middle + 1;
    else high = middle - 1;
  }
  return false;
}

const EMPTY: TokenSet = new Uint32Array(0);

// ---- classes and members -----------------------------------------------------------

interface ParentClass {
  readonly index: ParentAclIndex;
  /** The parent plan's class for members of this class; undefined when the spec evaluates its own override. */
  readonly klass: VisibilityClass | undefined;
}

/** Members whose class atoms resolve identically. Immutable once built. */
export class VisibilityClass {
  /** Process-local id (memo keys). */
  readonly id = nextClassId++;
  private references = 0;

  constructor(
    /** Content-derived id: equal for every member whose rows decide identically under the compiled rule. 16 hex characters. */
    readonly stableId: string,
    readonly plan: CompiledPlan,
    readonly scope: ClassScope,
    readonly bits: readonly boolean[],
    readonly sets: readonly TokenSet[],
    readonly parents: ReadonlyArray<ParentClass | undefined>,
    /** Whether the class can see a row only through one of its tokens. */
    readonly gated: boolean,
    readonly key: string,
  ) {}

  retain(): void {
    this.references += 1;
  }

  release(): void {
    this.references -= 1;
    if (this.references > 0) return;
    this.scope.dropClass(this);
    for (const parent of this.parents) parent?.klass?.release();
  }

  /** Visibility for a member of this class that no member atom hits. `row` comes from this scope's `decode`. */
  visibleDefault(row: AclRow): boolean {
    return row.object && this.plan.eval(this.plan.root, row, this, undefined);
  }

  visibleValue(row: JsonValue, member: MemberVisibility | undefined): boolean {
    const decoded = AclRow.fromValue(row, this.plan.columns);
    if (!decoded.object) return false;
    decoded.resolveTokens(this.scope);
    return this.plan.eval(this.plan.root, decoded, this, member);
  }
}

/** One member's binding of one plan: its shared class plus member atoms. */
export class MemberVisibility {
  private released = false;

  constructor(
    readonly klass: VisibilityClass,
    readonly memberSets: readonly TokenSet[],
    readonly contexts: ReadonlyArray<string | undefined>,
    readonly parents: ReadonlyArray<MemberVisibility | undefined>,
    /** Whether any member atom exists, here or on an inherited parent. A member without one always sees its class default. */
    readonly hasMemberAtoms: boolean,
  ) {}

  get scope(): ClassScope {
    return this.klass.scope;
  }

  get live(): boolean {
    return !this.released;
  }

  /** Releases this binding (Rust: dropping the handle). Idempotent. */
  release(): void {
    if (this.released) return;
    this.released = true;
    for (const parent of this.parents) parent?.release();
    this.klass.release();
  }

  private decodeMemo(memo: EvalMemo, row: JsonValue | undefined): AclRow {
    let byScope = memo.rows.get(row);
    if (!byScope) memo.rows.set(row, (byScope = new Map()));
    let decoded = byScope.get(this.klass.scope);
    if (!decoded) byScope.set(this.klass.scope, (decoded = AclRow.fromValue(row, this.klass.plan.columns)));
    decoded.refresh(this.klass.scope);
    return decoded;
  }

  private memoDefault(memo: EvalMemo, row: JsonValue | undefined, decoded: AclRow): boolean {
    let byRow = memo.defaults.get(this.klass.id);
    if (!byRow) memo.defaults.set(this.klass.id, (byRow = new Map()));
    const found = byRow.get(row);
    if (found !== undefined) return found;
    memo.stats.classEvaluations += 1;
    const visible = this.klass.visibleDefault(decoded);
    byRow.set(row, visible);
    return visible;
  }

  /** `hit` for a change row that outlives `memo`. */
  hitMemo(memo: EvalMemo, row: JsonValue | undefined): boolean {
    if (!this.hasMemberAtoms) return false;
    return this.hit(this.decodeMemo(memo, row));
  }

  /** Undefined when a member atom hits the row (evaluate this member alone), else the memoized class default. */
  classDefaultMemo(memo: EvalMemo, row: JsonValue | undefined): boolean | undefined {
    const decoded = this.decodeMemo(memo, row);
    if (!decoded.object) return false;
    if (this.hasMemberAtoms && this.hit(decoded)) return undefined;
    return this.memoDefault(memo, row, decoded);
  }

  /**
   * For a changed row of an inherited parent `table` (one plain `inherits`
   * on it, no override): whether the parent admits its children differently
   * before and after, by the parent class default. Undefined when the parent
   * member is hit on either image or the spec is not of that form.
   */
  parentDefaultFlip(memo: EvalMemo, table: string, oldRow: JsonValue | undefined, newRow: JsonValue | undefined): boolean | undefined {
    const specs = this.klass.plan.inherits.map((spec, index) => [spec, index] as const).filter(([spec]) => spec.table === table);
    if (specs.length !== 1) return undefined;
    const [spec, index] = specs[0]!;
    if (spec.overrideRoot !== undefined) return undefined;
    const parent = this.parents[index];
    if (!parent) return undefined;
    const admits = (row: JsonValue | undefined): boolean | undefined => {
      if (!isObject(row) || !literalConstraintsHold(spec.parentWhere, row)) return false;
      return parent.classDefaultMemo(memo, row);
    };
    const before = admits(oldRow);
    if (before === undefined) return undefined;
    const after = admits(newRow);
    if (after === undefined) return undefined;
    return before !== after;
  }

  private atomTrue(atom: MemberAtom, row: AclRow): boolean {
    if (atom.kind === "inSet") {
      const token = row.token(atom.column);
      return token !== undefined && contains(this.memberSets[atom.set]!, token);
    }
    const expected = this.contexts[atom.context];
    return expected !== undefined && row.scalar(atom.column) === expected;
  }

  /** Is some member atom true on this row (directly or on a parent row)? */
  hit(row: AclRow): boolean {
    if (!this.hasMemberAtoms || !row.object) return false;
    const plan = this.klass.plan;
    if (plan.memberAtoms.some((atom) => this.atomTrue(atom, row))) return true;
    for (let index = 0; index < plan.inherits.length; index++) {
      const spec = plan.inherits[index]!;
      const parentMember = this.parents[index];
      if (spec.overrideMemberAtoms.length === 0 && !parentMember?.hasMemberAtoms) continue;
      const parent = this.klass.parents[index];
      if (!parent) continue;
      const key = row.value(spec.column);
      if (key === undefined || key === null) continue;
      const parentRow = parent.index.get(scalar(key));
      if (parentRow === undefined) continue;
      if (spec.overrideMemberAtoms.length) {
        const decoded = AclRow.fromValue(parentRow, spec.overrideColumns);
        decoded.resolveTokens(this.klass.scope);
        if (spec.overrideMemberAtoms.some((atom) => this.atomTrue(atom, decoded))) return true;
      }
      if (parentMember) {
        const decoded = AclRow.fromValue(parentRow, parentMember.klass.plan.columns);
        decoded.resolveTokens(parentMember.klass.scope);
        if (parentMember.hit(decoded)) return true;
      }
    }
    return false;
  }

  /** Full evaluation for this member. `row` comes from `scope.decode`. */
  visibleRow(row: AclRow): boolean {
    if (!row.object) return false;
    if (this.hit(row)) return this.klass.plan.eval(this.klass.plan.root, row, this.klass, this);
    return this.klass.visibleDefault(row);
  }

  /** Decodes and evaluates one row. */
  visible(row: RowView): boolean {
    return this.visibleRow(this.klass.scope.decode(row));
  }

  /** Evaluates a change row that stays alive and unmodified for the whole memo. */
  visibleMemo(memo: EvalMemo, row: JsonValue | undefined): boolean {
    memo.stats.compiledChecks += 1;
    const decoded = this.decodeMemo(memo, row);
    if (!decoded.object) return false;
    if (this.hit(decoded)) {
      memo.stats.memberEvaluations += 1;
      return this.klass.plan.eval(this.klass.plan.root, decoded, this.klass, this);
    }
    return this.memoDefault(memo, row, decoded);
  }
}

export interface EvalStats {
  /** Row checks answered by a compiled program. */
  compiledChecks: number;
  /** Class default evaluations (one per class per row). */
  classEvaluations: number;
  /** Members evaluated individually because a member atom hit the row. */
  memberEvaluations: number;
  /** Row checks answered by the interpreted fallback evaluator. */
  fallbackChecks: number;
}

/** Per-feed-event memo of decoded change rows and class defaults, keyed by row identity. */
export class EvalMemo {
  readonly rows = new Map<JsonValue | undefined, Map<ClassScope, AclRow>>();
  readonly defaults = new Map<number, Map<JsonValue | undefined, boolean>>();
  readonly stats: EvalStats = { compiledChecks: 0, classEvaluations: 0, memberEvaluations: 0, fallbackChecks: 0 };
}

/** A member's compiled visibility, or an explicit fallback to `rowMatches`. */
export type MemberHandle = { readonly kind: "compiled"; readonly member: MemberVisibility } | { readonly kind: "fallback" };

const FALLBACK: MemberHandle = { kind: "fallback" };

/** Binds one member's principal for `plan` in a tenant database (`scope`). Release the handle when done. */
export function memberVisibility(plan: VisibilityPlan, resolved: ResolvedVisibility, scope: string): MemberHandle {
  const program = compiled(plan);
  if (program.unsupported !== undefined) return FALLBACK;
  const member = bind(program, program.scope(scope), scope, resolved, 0);
  return member ? { kind: "compiled", member } : FALLBACK;
}

function bind(plan: CompiledPlan, scope: ClassScope, scopeName: string, resolved: ResolvedVisibility, depth: number): MemberVisibility | undefined {
  if (depth > 8) return undefined;
  // Parents first: a child class is keyed by its parents' classes.
  const parentClasses: Array<ParentClass | undefined> = [];
  const parentMembers: Array<MemberVisibility | undefined> = [];
  const abandon = () => {
    for (const member of parentMembers) member?.release();
    return undefined;
  };
  for (const spec of plan.inherits) {
    const parent = resolved.inherited.get(spec.table);
    if (!parent) {
      parentClasses.push(undefined);
      parentMembers.push(undefined);
      continue;
    }
    if (spec.overrideRoot !== undefined) {
      parentClasses.push({ index: parent.index, klass: undefined });
      parentMembers.push(undefined);
      continue;
    }
    const parentPlan = compiled(parent.plan);
    if (parentPlan.unsupported !== undefined) return abandon();
    const member = bind(parentPlan, parentPlan.scope(scopeName), scopeName, parent.resolved, depth + 1);
    if (!member) return abandon();
    parentClasses.push({ index: parent.index, klass: member.klass });
    parentMembers.push(member);
  }

  const bits = plan.bits.map((atom) => {
    if (atom.kind === "role") return resolved.role === atom.name;
    if (atom.kind === "permission") return truthy(get(resolved.permissions, atom.name));
    return (resolved.sets.get(atom.name)?.size ?? 0) > 0;
  });
  const residual = plan.residual(bits);
  const tokens = (names: readonly string[]): TokenSet[] => {
    const built: TokenSet[] = [];
    for (const name of names) {
      const ids = [...(resolved.sets.get(name) ?? [])].map((token) => scope.intern(token));
      const sorted = Uint32Array.from(new Set(ids)).sort();
      // Identical sets (one team→workspace set under several names) share one array.
      const same = built.find((existing) => existing.length === sorted.length && existing.every((value, index) => value === sorted[index]));
      built.push(same ?? sorted);
    }
    return built;
  };
  const sets = tokens(plan.classSets).map((set, index) => (residual.sets[index] ? set : EMPTY));
  const memberSets = tokens(plan.memberSets);
  const key = [
    residual.shape.join(","),
    sets.map((set) => set.join(",")).join(";"),
    parentClasses.map((parent) => (parent ? `${parent.index.id}:${parent.klass?.id ?? 0}` : "-")).join(","),
  ].join("|");
  // Stable id: plan content, residual shape, the consulted class sets' token text, the parents' stable ids.
  const stableInput = serdeJson([
    plan.fingerprint,
    [...residual.shape],
    plan.classSets.map((name, index) => (residual.sets[index] ? sortedText(resolved.sets.get(name) ?? []) : null)),
    parentClasses.map((parent) => parent?.klass?.stableId ?? null),
  ]);
  const klass = scope.classFor(key, bits, sets, parentClasses, () => sha256Hex(stableInput).slice(0, 16));
  const contexts = plan.contexts.map((context) => resolved.direct[context]);
  const hasMemberAtoms =
    plan.memberAtoms.length > 0 || plan.inherits.some((spec) => spec.overrideMemberAtoms.length > 0) || parentMembers.some((parent) => parent?.hasMemberAtoms === true);
  return new MemberVisibility(klass, memberSets, contexts, parentMembers, hasMemberAtoms);
}

// ---- scope: interner, class registry, candidate index ---------------------------------

/** Classes and members that may see one changed row. */
export interface Candidates {
  /** Evaluate `visibleDefault` once per class; it applies to every member of the class not listed in `members`. */
  readonly classes: VisibilityClass[];
  /** Registered members hit by a member atom: evaluate individually. */
  readonly members: MemberVisibility[];
}

export interface ScopeStats {
  classes: number;
  gatedClasses: number;
  tokens: number;
  indexEntries: number;
}

/** Classes of one plan in one tenant database. */
export class ClassScope {
  private readonly ids = new Map<string, number>();
  generation = 0;
  private readonly classes = new Map<string, VisibilityClass>();
  private readonly byToken = new Map<string, VisibilityClass[]>();
  private ungated: VisibilityClass[] = [];
  private readonly membersByToken = new Map<string, MemberVisibility[]>();
  private readonly membersByContext = new Map<string, MemberVisibility[]>();
  private membersAlways: MemberVisibility[] = [];

  constructor(readonly plan: CompiledPlan) {}

  tokenId(token: string): number | undefined {
    return this.ids.get(token);
  }

  intern(token: string): number {
    let id = this.ids.get(token);
    if (id === undefined) {
      id = this.ids.size;
      this.ids.set(token, id);
      this.generation += 1;
    }
    return id;
  }

  /** Decodes a row's ACL columns and resolves its tokens. */
  decode(row: RowView): AclRow {
    const decoded = "json" in row ? AclRow.fromJson(row.json, this.plan.columns) : AclRow.fromValue(row.value, this.plan.columns);
    decoded.resolveTokens(this);
    return decoded;
  }

  stats(): ScopeStats {
    let entries = this.ungated.length;
    for (const list of this.byToken.values()) entries += list.length;
    return {
      classes: this.classes.size,
      gatedClasses: [...this.classes.values()].filter((klass) => klass.gated).length,
      tokens: this.ids.size,
      indexEntries: entries,
    };
  }

  classFor(key: string, bits: readonly boolean[], sets: readonly TokenSet[], parents: ReadonlyArray<ParentClass | undefined>, stableId: () => string): VisibilityClass {
    const existing = this.classes.get(key);
    if (existing) {
      existing.retain();
      return existing;
    }
    const plan = this.plan;
    const gated = plan.tri(plan.root, bits) === "false";
    const klass = new VisibilityClass(stableId(), plan, this, bits, sets, parents, gated, key);
    klass.retain();
    for (const parent of parents) parent?.klass?.retain();
    this.classes.set(key, klass);
    if (gated) {
      const done = new Set<number>();
      for (const [, set] of plan.classAtoms) {
        if (done.has(set)) continue;
        done.add(set);
        for (const token of klass.sets[set]!) push(this.byToken, `${set}:${token}`, klass);
      }
    } else this.ungated.push(klass);
    return klass;
  }

  dropClass(klass: VisibilityClass): void {
    if (this.classes.get(klass.key) === klass) this.classes.delete(klass.key);
    if (!klass.gated) {
      this.ungated = this.ungated.filter((entry) => entry !== klass);
      return;
    }
    for (const [, set] of this.plan.classAtoms) {
      for (const token of klass.sets[set]!) {
        const key = `${set}:${token}`;
        const entries = this.byToken.get(key)?.filter((entry) => entry !== klass);
        if (entries?.length) this.byToken.set(key, entries);
        else this.byToken.delete(key);
      }
    }
  }

  /** Indexes a member's member atoms for `candidates`. A released member is skipped and swept lazily. */
  register(member: MemberVisibility): void {
    const plan = this.plan;
    if (plan.inherits.length && member.hasMemberAtoms) this.membersAlways.push(member);
    const setsDone = new Set<number>();
    const contextsDone = new Set<number>();
    for (const atom of plan.memberAtoms) {
      if (atom.kind === "inSet") {
        if (setsDone.has(atom.set)) continue;
        setsDone.add(atom.set);
        for (const token of member.memberSets[atom.set]!) push(this.membersByToken, `${atom.set}:${token}`, member);
      } else {
        if (contextsDone.has(atom.context)) continue;
        contextsDone.add(atom.context);
        const value = member.contexts[atom.context];
        if (value !== undefined) push(this.membersByContext, value, member);
      }
    }
  }

  /** Drops index entries of released members. */
  sweep(): void {
    for (const map of [this.membersByToken, this.membersByContext]) {
      for (const [key, members] of map) {
        const live = members.filter((member) => member.live);
        if (live.length) map.set(key, live);
        else map.delete(key);
      }
    }
    this.membersAlways = this.membersAlways.filter((member) => member.live);
  }

  /**
   * Every class whose default may admit `oldRow` or `newRow`, and every
   * registered member a member atom hits on either. A member in neither sees
   * neither row. Rows come from this scope's `decode`.
   */
  candidates(oldRow: AclRow | undefined, newRow: AclRow | undefined): Candidates {
    const classes = new Set<VisibilityClass>(this.ungated);
    const members = new Set<MemberVisibility>();
    const addMembers = (list: readonly MemberVisibility[] | undefined) => {
      for (const member of list ?? []) if (member.live) members.add(member);
    };
    for (const row of [oldRow, newRow]) {
      if (!row?.object) continue;
      for (const [column, set] of this.plan.classAtoms) {
        const token = row.token(column);
        if (token === undefined) continue;
        for (const klass of this.byToken.get(`${set}:${token}`) ?? []) classes.add(klass);
      }
      for (const atom of this.plan.memberAtoms) {
        if (atom.kind === "inSet") {
          const token = row.token(atom.column);
          if (token !== undefined) addMembers(this.membersByToken.get(`${atom.set}:${token}`));
        } else addMembers(this.membersByContext.get(row.scalar(atom.column)));
      }
    }
    addMembers(this.membersAlways);
    return { classes: [...classes], members: [...members] };
  }
}

function push<K, V>(map: Map<K, V[]>, key: K, value: V): void {
  const list = map.get(key);
  if (list) list.push(value);
  else map.set(key, [value]);
}

/** A subscription's cached member binding. Clear it whenever its resolution (or a parent's) changes. */
export class HandleCache {
  private handle: MemberHandle | undefined;

  peek(): MemberHandle | undefined {
    return this.handle;
  }

  get(plan: VisibilityPlan, resolved: ResolvedVisibility, scope: string): MemberHandle {
    this.handle ??= memberVisibility(plan, resolved, scope);
    return this.handle;
  }

  /** Releases the binding. */
  clear(): void {
    if (this.handle?.kind === "compiled") this.handle.member.release();
    this.handle = undefined;
  }
}

