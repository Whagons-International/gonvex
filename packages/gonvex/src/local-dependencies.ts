// Static data dependencies of Reducers that the client executes locally.
//
// The generated client runs every locally executing Reducer body on the
// device before the server replays it. The portable local runtime can read
// only tables that have a generated `__local.<table>` Replica Collection. A
// read of any other table stops the local run with IncompleteReplicaError; the
// client then queues the intent with no local result, so the change waits for
// the server and never shows while offline.
//
// This analysis finds, for each such Reducer, every table its body can touch,
// directly or through any project function it calls, using the TypeScript
// type checker:
//
// - Module SDK data helpers (`selectRows`, `selectFirst`, `getRow`,
//   `selectDataBatch`, `existsDataRows`, `summarizeRows`, `insertDataRows`,
//   `updateDataRows`, `deleteDataRows`, `updateDataWhere`, `deleteDataWhere`,
//   `applyDataWrites`) and the database methods (`select`, `insert`, `update`,
//   `delete`, `deleteMany`, `query`) on any value typed as a Gonvex database.
// - The table expression is resolved from literals, literal types, constants,
//   object and array literals, spreads, `for...of` and array callbacks. When
//   it comes from a parameter, the parameter becomes a sink of that function
//   and every call site resolves its own argument, so generic helpers that
//   receive a table name are followed per call site.
// - A table expression that cannot be resolved is reported, never dropped.
//
// Precision: the analysis over-approximates. Every branch counts, whether or
// not it runs for a given argument, and a helper that can touch several tables
// contributes all of them. It does not follow functions reached only through
// dynamic dispatch (a function stored in a map and called by key, a class
// method called through an interface); a table taken from a parameter of
// such a function is reported as unresolved rather than guessed.
//
// Local semantics (pinned by @gonvex/local-runtime tests): every structured
// read needs coverage; `update`, `delete`, `deleteMany` and the data-write
// helpers read the row first; `insert` reads only when the row carries an
// explicit primary key (an intent-generated key is new by construction).
// Raw SQL (`db.query`) always fails locally.
//
// The TypeScript compiler is loaded lazily, only for projects that have
// locally executing Reducers.
import { existsSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join, relative, resolve, sep } from "node:path";
import type * as TS from "typescript";

export type LocalTableAccess = {
  table: string;
  operation: string;
  /** False only for inserts with an intent-generated key: they never read locally. */
  reads: boolean;
  /** Project-relative `file:line`. */
  site: string;
};

export type UnresolvedLocalAccess = { operation: string; site: string; expression: string };

export type LocalReducerAnalysis = {
  path: string;
  accesses: LocalTableAccess[];
  unresolved: UnresolvedLocalAccess[];
  rawQueries: string[];
};

export type LocalReducerSource = {
  /** Public function path. */
  path: string;
  /** Project-relative module file. */
  file: string;
  /** Exported binding that holds the Reducer definition. */
  exportName: string;
};

export type LocalDependencyViolation = {
  reducer: string;
  table: string;
  operation: string;
  site: string;
  reason: string;
};

type FunctionUnit = TS.FunctionLikeDeclaration;
type Unit = FunctionUnit | TS.Expression;

type ParamSink = { index: number; path: string[]; operation: string; reads: boolean; site: string };
/** A table taken from a parameter of an enclosing function (a factory's closure). */
type ClosureSink = ParamSink & { owner: FunctionUnit };

type Summary = {
  accesses: LocalTableAccess[];
  params: ParamSink[];
  closureParams: ClosureSink[];
  unresolved: UnresolvedLocalAccess[];
  rawQueries: string[];
  references: Set<Unit>;
  /** Project functions this unit calls directly. */
  calls: Set<Unit>;
};

type ParamRef = { index: number; path: string[]; owner?: FunctionUnit };
type Evaluation = { tables: Set<string>; params: ParamRef[]; unresolved: boolean };

const readHelpers: Record<string, { argument: number; path: string[]; operation: string }> = {
  selectRows: { argument: 1, path: ["table"], operation: "select" },
  selectFirst: { argument: 1, path: ["table"], operation: "select" },
  summarizeRows: { argument: 1, path: ["table"], operation: "summarize" },
  getRow: { argument: 1, path: [], operation: "select" },
  selectDataBatch: { argument: 1, path: ["[]", "table"], operation: "select" },
  existsDataRows: { argument: 1, path: ["[]", "table"], operation: "exists" },
  updateDataRows: { argument: 1, path: [], operation: "update" },
  deleteDataRows: { argument: 1, path: [], operation: "delete" },
  updateDataWhere: { argument: 1, path: ["table"], operation: "update" },
  deleteDataWhere: { argument: 1, path: ["table"], operation: "delete" },
};

const databaseMethods = new Set(["select", "insert", "update", "delete", "deleteMany", "query"]);
const nonExecutedDefinitions = new Set(["action", "query", "internalQuery", "liveQuery", "replicaCollection", "visibility", "cron", "tenantCron"]);
const arrayCallbacks = new Set(["map", "flatMap", "forEach", "filter", "some", "every", "find", "findIndex", "reduce"]);

// The compiler API is loaded on demand; every helper below runs after it is set.
let ts: typeof TS;

const empty = (): Evaluation => ({ tables: new Set(), params: [], unresolved: false });
const unresolvedEvaluation = (): Evaluation => ({ tables: new Set(), params: [], unresolved: true });

function merge(...values: Evaluation[]): Evaluation {
  const result = empty();
  for (const value of values) {
    value.tables.forEach((table) => result.tables.add(table));
    result.params.push(...value.params);
    result.unresolved ||= value.unresolved;
  }
  return result;
}

function isFunctionUnit(node: TS.Node): node is FunctionUnit {
  return ts.isFunctionDeclaration(node) || ts.isFunctionExpression(node) || ts.isArrowFunction(node)
    || ts.isMethodDeclaration(node) || ts.isGetAccessorDeclaration(node) || ts.isSetAccessorDeclaration(node)
    || ts.isConstructorDeclaration(node);
}

function skipOuter(expression: TS.Expression): TS.Expression {
  let current = expression;
  while (ts.isParenthesizedExpression(current) || ts.isAsExpression(current) || ts.isSatisfiesExpression(current)
    || ts.isNonNullExpression(current) || ts.isTypeAssertionExpression(current)) current = current.expression;
  return current;
}

function propertyName(name: TS.PropertyName | TS.BindingName | undefined): string | undefined {
  if (!name) return undefined;
  if (ts.isIdentifier(name) || ts.isStringLiteralLike(name) || ts.isNumericLiteral(name)) return name.text;
  return undefined;
}

function slash(path: string): string {
  return path.split(sep).join("/");
}

class LocalReducerDependencyAnalyzer {
  private readonly checker: TS.TypeChecker;
  private readonly summaries = new Map<Unit, Summary>();
  private readonly inProgress = new Set<Unit>();

  constructor(private readonly program: TS.Program, private readonly root: string, private readonly sdkRoots: readonly string[]) {
    this.checker = program.getTypeChecker();
  }

  private isSdkFile(fileName: string): boolean {
    const file = slash(fileName);
    return file.includes("/@gonvex/module-sdk/") || this.sdkRoots.some((root) => file.startsWith(root));
  }

  private isProjectFile(file: TS.SourceFile): boolean {
    if (file.isDeclarationFile || this.isSdkFile(file.fileName)) return false;
    const path = slash(relative(this.root, file.fileName));
    return !path.startsWith("..") && !path.includes("node_modules/") && !path.includes("/_generated/");
  }

  private site(node: TS.Node): string {
    const file = node.getSourceFile();
    const { line } = file.getLineAndCharacterOfPosition(node.getStart(file));
    return `${slash(relative(this.root, file.fileName))}:${line + 1}`;
  }

  private symbol(node: TS.Node): TS.Symbol | undefined {
    let symbol = this.checker.getSymbolAtLocation(node);
    if (symbol && symbol.flags & ts.SymbolFlags.Alias) symbol = this.checker.getAliasedSymbol(symbol);
    return symbol;
  }

  private sdkHelper(callee: TS.Expression): string | undefined {
    const target = ts.isPropertyAccessExpression(callee) ? callee.name : callee;
    const symbol = this.symbol(target);
    const declaration = symbol?.declarations?.[0];
    if (!symbol || !declaration) return undefined;
    if (!this.isSdkFile(declaration.getSourceFile().fileName) || !ts.isFunctionDeclaration(declaration)) return undefined;
    return symbol.getName();
  }

  private isDatabase(expression: TS.Expression): boolean {
    const type = this.checker.getNonNullableType(this.checker.getTypeAtLocation(expression));
    const has = (name: string) => !!this.checker.getPropertyOfType(type, name);
    if (has("query") && (has("insert") || has("select"))) return true;
    // An untyped context (`ctx: any`) still reaches the database as `ctx.db`.
    const untyped = (type.flags & (ts.TypeFlags.Any | ts.TypeFlags.Unknown)) !== 0;
    const target = skipOuter(expression);
    return untyped && ((ts.isPropertyAccessExpression(target) && target.name.text === "db") || (ts.isIdentifier(target) && target.text === "db"));
  }

  /** Literal string values of the value at `path` inside the expression's type. */
  private literalTables(expression: TS.Expression, valuePath: string[]): Set<string> | undefined {
    let types: TS.Type[] = [this.checker.getTypeAtLocation(expression)];
    for (const step of valuePath) {
      const next: TS.Type[] = [];
      for (const type of types) {
        for (const member of type.isUnion() ? type.types : [type]) {
          if (step === "[]") {
            const element = this.checker.isTupleType(member)
              ? this.checker.getTypeArguments(member as TS.TypeReference)
              : [this.checker.getIndexTypeOfType(member, ts.IndexKind.Number)];
            for (const value of element) if (value) next.push(value);
          } else if (step === "{}") {
            for (const property of this.checker.getPropertiesOfType(member)) next.push(this.checker.getTypeOfSymbol(property));
            const index = this.checker.getIndexTypeOfType(member, ts.IndexKind.String);
            if (index) next.push(index);
          } else if (/^\d+$/.test(step) && !this.checker.isTupleType(member)) {
            const index = this.checker.getIndexTypeOfType(member, ts.IndexKind.Number);
            if (!index) return undefined;
            next.push(index);
          } else {
            const property = this.checker.getPropertyOfType(member, step);
            if (!property) return undefined;
            next.push(this.checker.getTypeOfSymbol(property));
          }
        }
      }
      if (!next.length) return undefined;
      types = next;
    }
    const values = new Set<string>();
    for (const type of types) {
      for (const member of type.isUnion() ? type.types : [type]) {
        if (member.flags & (ts.TypeFlags.Undefined | ts.TypeFlags.Null)) continue;
        if (!member.isStringLiteral()) return undefined;
        values.add(member.value);
      }
    }
    return values.size ? values : undefined;
  }

  private unitOfParameter(parameter: TS.ParameterDeclaration): FunctionUnit | undefined {
    return isFunctionUnit(parameter.parent) ? parameter.parent : undefined;
  }

  /** The array a callback parameter iterates, for `items.map((item) => ...)`. */
  private callbackSource(fn: FunctionUnit, index: number): TS.Expression | undefined {
    const call = fn.parent;
    if (!ts.isCallExpression(call) || call.arguments[0] !== fn || !ts.isPropertyAccessExpression(call.expression)) return undefined;
    const method = call.expression.name.text;
    if (!arrayCallbacks.has(method)) return undefined;
    if (index !== (method === "reduce" ? 1 : 0)) return undefined;
    return call.expression.expression;
  }

  private evaluateParameter(parameter: TS.ParameterDeclaration, valuePath: string[], unit: Unit, depth: number): Evaluation {
    const fn = this.unitOfParameter(parameter);
    if (!fn) return unresolvedEvaluation();
    const index = fn.parameters.indexOf(parameter);
    if (fn === unit) {
      const own: Evaluation = { tables: new Set(), params: [{ index, path: valuePath }], unresolved: false };
      // A default value is used when a call site omits the argument.
      return parameter.initializer ? merge(own, this.evaluate(parameter.initializer, valuePath, unit, depth + 1)) : own;
    }
    const source = this.callbackSource(fn, index);
    if (source) return this.evaluate(source, ["[]", ...valuePath], unit, depth + 1);
    // A factory parameter captured by an inner function: its call sites decide.
    for (let current: TS.Node | undefined = unit.parent; current; current = current.parent) {
      if (current === fn) return { tables: new Set(), params: [{ index, path: valuePath, owner: fn }], unresolved: false };
    }
    return unresolvedEvaluation();
  }

  private evaluateDeclaration(declaration: TS.Declaration, valuePath: string[], unit: Unit, depth: number): Evaluation {
    if (ts.isParameter(declaration)) return this.evaluateParameter(declaration, valuePath, unit, depth);
    if (ts.isBindingElement(declaration)) {
      // Walk out of the destructuring pattern, prefixing the property names.
      const prefix: string[] = [];
      let current: TS.Node = declaration;
      while (ts.isBindingElement(current)) {
        const pattern: TS.BindingPattern = current.parent;
        if (ts.isArrayBindingPattern(pattern)) prefix.unshift(String(pattern.elements.indexOf(current)));
        else {
          const name = propertyName(current.propertyName ?? current.name);
          if (!name) return unresolvedEvaluation();
          prefix.unshift(name);
        }
        current = pattern.parent;
      }
      if (ts.isParameter(current)) return this.evaluateParameter(current, [...prefix, ...valuePath], unit, depth);
      if (ts.isVariableDeclaration(current)) return this.evaluateVariable(current, [...prefix, ...valuePath], unit, depth);
      return unresolvedEvaluation();
    }
    if (ts.isVariableDeclaration(declaration)) return this.evaluateVariable(declaration, valuePath, unit, depth);
    if (ts.isPropertyAssignment(declaration)) return this.evaluate(declaration.initializer, valuePath, unit, depth + 1);
    if (ts.isShorthandPropertyAssignment(declaration)) {
      const value = this.checker.getShorthandAssignmentValueSymbol(declaration);
      const target = value?.valueDeclaration;
      return target ? this.evaluateDeclaration(target, valuePath, unit, depth + 1) : unresolvedEvaluation();
    }
    if (ts.isEnumMember(declaration) && declaration.initializer) return this.evaluate(declaration.initializer, valuePath, unit, depth + 1);
    return unresolvedEvaluation();
  }

  private evaluateVariable(declaration: TS.VariableDeclaration, valuePath: string[], unit: Unit, depth: number): Evaluation {
    const list = declaration.parent;
    const loop = list?.parent;
    if (loop && ts.isForOfStatement(loop) && loop.initializer === list) return this.evaluate(loop.expression, ["[]", ...valuePath], unit, depth + 1);
    if (!declaration.initializer) return unresolvedEvaluation();
    return this.evaluate(declaration.initializer, valuePath, unit, depth + 1);
  }

  /** Tables the value at `valuePath` inside `expression` can name. */
  evaluate(input: TS.Expression, valuePath: string[], unit: Unit, depth = 0): Evaluation {
    if (depth > 24) return unresolvedEvaluation();
    const expression = skipOuter(input);
    const typed = this.literalTables(expression, valuePath);
    if (typed) return { tables: typed, params: [], unresolved: false };
    if (!valuePath.length && ts.isStringLiteralLike(expression)) return { tables: new Set([expression.text]), params: [], unresolved: false };
    if (ts.isConditionalExpression(expression)) {
      return merge(this.evaluate(expression.whenTrue, valuePath, unit, depth + 1), this.evaluate(expression.whenFalse, valuePath, unit, depth + 1));
    }
    if (ts.isBinaryExpression(expression) && [ts.SyntaxKind.QuestionQuestionToken, ts.SyntaxKind.BarBarToken, ts.SyntaxKind.AmpersandAmpersandToken].includes(expression.operatorToken.kind)) {
      return merge(this.evaluate(expression.left, valuePath, unit, depth + 1), this.evaluate(expression.right, valuePath, unit, depth + 1));
    }
    if (ts.isObjectLiteralExpression(expression) && valuePath.length) {
      const [head, ...rest] = valuePath;
      const parts: Evaluation[] = [];
      for (const property of expression.properties) {
        if (ts.isSpreadAssignment(property)) {
          // A spread may or may not carry the property: follow it unless a
          // later explicit property overrides it.
          parts.push(this.evaluateOptional(property.expression, valuePath, unit, depth + 1));
        } else if (head === "{}" || propertyName(property.name) === head) {
          if (head !== "{}") parts.length = 0;
          if (ts.isPropertyAssignment(property)) parts.push(this.evaluate(property.initializer, rest, unit, depth + 1));
          else if (ts.isShorthandPropertyAssignment(property)) {
            const value = this.checker.getShorthandAssignmentValueSymbol(property);
            parts.push(value?.valueDeclaration ? this.evaluateDeclaration(value.valueDeclaration, rest, unit, depth + 1) : unresolvedEvaluation());
          } else if (head !== "{}") parts.push(unresolvedEvaluation());
        }
      }
      return merge(...parts);
    }
    if (ts.isArrayLiteralExpression(expression) && (valuePath[0] === "[]" || /^\d+$/.test(valuePath[0] ?? ""))) {
      const rest = valuePath.slice(1);
      const position = Number(valuePath[0]);
      if (valuePath[0] !== "[]" && !expression.elements.some(ts.isSpreadElement)) {
        const element = expression.elements[position];
        return element ? this.evaluate(element, rest, unit, depth + 1) : empty();
      }
      return merge(...expression.elements.map((element) => ts.isSpreadElement(element)
        ? this.evaluate(element.expression, ["[]", ...rest], unit, depth + 1)
        : this.evaluate(element, rest, unit, depth + 1)));
    }
    if (ts.isIdentifier(expression)) {
      const symbol = this.symbol(expression);
      const declaration = symbol?.valueDeclaration ?? symbol?.declarations?.[0];
      return declaration ? this.evaluateDeclaration(declaration, valuePath, unit, depth) : unresolvedEvaluation();
    }
    if (ts.isPropertyAccessExpression(expression)) {
      const symbol = this.symbol(expression.name);
      const declaration = symbol?.valueDeclaration;
      if (declaration && (ts.isPropertyAssignment(declaration) || ts.isShorthandPropertyAssignment(declaration)) && this.isProjectFile(declaration.getSourceFile())) {
        const direct = this.evaluateDeclaration(declaration, valuePath, unit, depth + 1);
        if (!direct.unresolved) return direct;
      }
      return this.evaluate(expression.expression, [expression.name.text, ...valuePath], unit, depth + 1);
    }
    if (ts.isElementAccessExpression(expression)) {
      const argument = skipOuter(expression.argumentExpression);
      if (ts.isStringLiteralLike(argument) || ts.isNumericLiteral(argument)) return this.evaluate(expression.expression, [argument.text, ...valuePath], unit, depth + 1);
      const receiver = this.checker.getTypeAtLocation(expression.expression);
      const arrayLike = this.checker.isArrayLikeType(receiver);
      return this.evaluate(expression.expression, [arrayLike ? "[]" : "{}", ...valuePath], unit, depth + 1);
    }
    if (ts.isCallExpression(expression) && ts.isIdentifier(expression.expression) && expression.expression.text === "String" && !valuePath.length && expression.arguments[0]) {
      return this.evaluate(expression.arguments[0], valuePath, unit, depth + 1);
    }
    if (ts.isCallExpression(expression) && ts.isPropertyAccessExpression(expression.expression)
      && ts.isIdentifier(expression.expression.expression) && expression.expression.expression.text === "Object" && expression.arguments[0]) {
      // Object.values(x) / Object.entries(x): the table sits in a property value.
      const method = expression.expression.name.text;
      const source = expression.arguments[0];
      if (method === "values" && valuePath[0] === "[]") return this.evaluate(source, ["{}", ...valuePath.slice(1)], unit, depth + 1);
      if (method === "entries" && valuePath[0] === "[]" && valuePath[1] === "1") return this.evaluate(source, ["{}", ...valuePath.slice(2)], unit, depth + 1);
    }
    if (ts.isCallExpression(expression) && ts.isPropertyAccessExpression(expression.expression)) {
      // A string method that keeps a name: table.replaceAll('"', '""'), table.trim().
      const method = expression.expression.name.text;
      if (!valuePath.length && ["replaceAll", "replace", "trim", "toString", "valueOf"].includes(method)) {
        return this.evaluate(expression.expression.expression, valuePath, unit, depth + 1);
      }
      // Array plumbing keeps the element shape: rows.filter(...), [...].concat(...).
      if (valuePath[0] === "[]" && ["filter", "slice", "concat", "flat", "reverse", "sort", "toSorted", "toReversed"].includes(method)) {
        return merge(this.evaluate(expression.expression.expression, valuePath, unit, depth + 1),
          ...(method === "concat" ? expression.arguments.map((argument) => this.evaluateOptional(argument, valuePath, unit, depth + 1)) : []));
      }
      if (valuePath[0] === "[]" && (method === "map" || method === "flatMap")) {
        const callback = expression.arguments[0];
        if (callback && (ts.isArrowFunction(callback) || ts.isFunctionExpression(callback))) {
          return this.evaluateReturns(callback, method === "flatMap" ? valuePath : valuePath.slice(1), unit, depth + 1);
        }
      }
    }
    if (ts.isCallExpression(expression)) {
      // A project helper that builds the read: follow its return expressions.
      for (const target of this.callTargets(expression)) {
        if (this.inProgress.has(target)) continue;
        this.inProgress.add(target);
        try {
          const returned = this.evaluateReturns(target, valuePath, target, depth + 1);
          // The helper's parameters are this call's arguments.
          return merge({ tables: returned.tables, params: [], unresolved: returned.unresolved },
            ...returned.params.map((param) => {
              const argument = expression.arguments[param.index];
              return argument ? this.evaluate(argument, param.path, unit, depth + 1) : empty();
            }));
        } finally {
          this.inProgress.delete(target);
        }
      }
    }
    return unresolvedEvaluation();
  }

  /** Like evaluate, but a value without the property contributes nothing. */
  private evaluateOptional(expression: TS.Expression, valuePath: string[], unit: Unit, depth: number): Evaluation {
    const type = this.checker.getTypeAtLocation(skipOuter(expression));
    if (valuePath.length && valuePath[0] !== "[]" && !this.checker.getPropertyOfType(type, valuePath[0]!)
      && !this.checker.getIndexInfosOfType(type).length) return empty();
    return this.evaluate(expression, valuePath, unit, depth);
  }

  private evaluateReturns(fn: FunctionUnit, valuePath: string[], unit: Unit, depth: number): Evaluation {
    if (!fn.body) return unresolvedEvaluation();
    if (!ts.isBlock(fn.body)) return this.evaluate(fn.body, valuePath, unit, depth);
    const parts: Evaluation[] = [];
    const visit = (node: TS.Node) => {
      if (isFunctionUnit(node)) return;
      if (ts.isReturnStatement(node) && node.expression) parts.push(this.evaluate(node.expression, valuePath, unit, depth));
      ts.forEachChild(node, visit);
    };
    ts.forEachChild(fn.body, visit);
    return parts.length ? merge(...parts) : unresolvedEvaluation();
  }

  /** Project functions a call can invoke. */
  private callTargets(call: TS.CallExpression): FunctionUnit[] {
    const callee = skipOuter(call.expression);
    const target = ts.isPropertyAccessExpression(callee) ? callee.name : callee;
    return this.unitsOfSymbol(this.symbol(target)).filter(isFunctionUnit);
  }

  private unitsOfSymbol(symbol: TS.Symbol | undefined): Unit[] {
    const units: Unit[] = [];
    for (const declaration of symbol?.declarations ?? []) {
      if (!this.isProjectFile(declaration.getSourceFile())) continue;
      const unit = this.unitOfDeclaration(declaration);
      if (unit) units.push(unit);
    }
    return units;
  }

  private unitOfDeclaration(declaration: TS.Declaration): Unit | undefined {
    if (isFunctionUnit(declaration)) return declaration.body ? declaration : undefined;
    if ((ts.isVariableDeclaration(declaration) || ts.isPropertyAssignment(declaration) || ts.isPropertyDeclaration(declaration)) && declaration.initializer) {
      const initializer = skipOuter(declaration.initializer);
      if (ts.isVariableDeclaration(declaration) && ts.isCallExpression(initializer) && ts.isIdentifier(initializer.expression)) {
        // Queries, Actions and visibility plans never run inside a Reducer body.
        const definition = this.symbol(initializer.expression);
        const file = definition?.declarations?.[0]?.getSourceFile().fileName ?? "";
        if (definition && nonExecutedDefinitions.has(definition.getName()) && this.isSdkFile(file)) return undefined;
      }
      if (isFunctionUnit(initializer)) return initializer;
      // A local value inside a function is already part of that function's
      // unit; only module-level values are units of their own.
      for (let current: TS.Node | undefined = declaration.parent; current; current = current.parent) {
        if (isFunctionUnit(current) || ts.isClassStaticBlockDeclaration(current)) return undefined;
      }
      return initializer;
    }
    if (ts.isShorthandPropertyAssignment(declaration)) {
      const value = this.checker.getShorthandAssignmentValueSymbol(declaration)?.valueDeclaration;
      return value ? this.unitOfDeclaration(value) : undefined;
    }
    return undefined;
  }

  private addAccess(summary: Summary, evaluation: Evaluation, operation: string, reads: boolean, node: TS.Node, expression: TS.Node): void {
    const site = this.site(node);
    for (const table of evaluation.tables) summary.accesses.push({ table, operation, reads, site });
    for (const { owner, ...param } of evaluation.params) {
      if (owner) summary.closureParams.push({ ...param, owner, operation, reads, site });
      else summary.params.push({ ...param, operation, reads, site });
    }
    if (evaluation.unresolved) summary.unresolved.push({ operation, site, expression: expression.getText().replace(/\s+/g, " ").slice(0, 160) });
  }

  /** Whether an inserted row carries an explicit primary key, which makes the local insert read. */
  private insertCarriesKey(row: TS.Expression | undefined, elementPath: string[] = []): boolean {
    if (!row) return true;
    const expression = skipOuter(row);
    if (ts.isArrayLiteralExpression(expression) && elementPath[0] === "[]") {
      return expression.elements.some((element) => this.insertCarriesKey(ts.isSpreadElement(element) ? element.expression : element, ts.isSpreadElement(element) ? elementPath : elementPath.slice(1)));
    }
    if (ts.isObjectLiteralExpression(expression) && !elementPath.length) {
      for (const property of expression.properties) {
        if (ts.isSpreadAssignment(property)) {
          if (this.insertCarriesKey(property.expression)) return true;
        } else if (propertyName(property.name) === "_id") {
          const value = ts.isPropertyAssignment(property) ? skipOuter(property.initializer) : undefined;
          // `_id: undefined` lets the SDK generate the key.
          if (!(value && ts.isIdentifier(value) && value.text === "undefined")) return true;
        }
      }
      return false;
    }
    let type = this.checker.getTypeAtLocation(expression);
    for (const step of elementPath) {
      const element = step === "[]" ? this.checker.getIndexTypeOfType(type, ts.IndexKind.Number) : undefined;
      if (!element) return true;
      type = element;
    }
    for (const member of type.isUnion() ? type.types : [type]) {
      const property = this.checker.getPropertyOfType(member, "_id");
      if (property) {
        const valueType = this.checker.getTypeOfSymbol(property);
        if (!(valueType.flags & ts.TypeFlags.Undefined)) return true;
        continue;
      }
      if (this.checker.getIndexInfosOfType(member).length) return true;
    }
    return false;
  }

  private inspectCall(call: TS.CallExpression, unit: Unit, summary: Summary): boolean {
    const callee = skipOuter(call.expression);
    const helper = this.sdkHelper(callee);
    const spec = helper ? readHelpers[helper] : undefined;
    if (spec) {
      const argument = call.arguments[spec.argument];
      if (argument) this.addAccess(summary, this.evaluate(argument, spec.path, unit), spec.operation, true, call, argument);
      return true;
    }
    if (helper === "insertDataRows") {
      const table = call.arguments[1];
      if (table) this.addAccess(summary, this.evaluate(table, [], unit), "insert", this.insertCarriesKey(call.arguments[2], ["[]"]), call, table);
      return true;
    }
    if (helper === "applyDataWrites") {
      const operations = call.arguments[1];
      if (!operations) return true;
      const list = skipOuter(operations);
      if (!ts.isArrayLiteralExpression(list)) {
        this.addAccess(summary, this.evaluate(operations, ["[]", "table"], unit), "write", true, call, operations);
        return true;
      }
      for (const element of list.elements) {
        if (ts.isSpreadElement(element)) {
          this.addAccess(summary, this.evaluate(element.expression, ["[]", "table"], unit), "write", true, call, element);
          continue;
        }
        const operation = skipOuter(element);
        const kind = ts.isObjectLiteralExpression(operation) ? this.literalTables(operation, ["kind"]) : undefined;
        const isInsert = kind?.size === 1 && kind.has("insert");
        const rows = ts.isObjectLiteralExpression(operation)
          ? operation.properties.find((property): property is TS.PropertyAssignment => ts.isPropertyAssignment(property) && propertyName(property.name) === "rows")
          : undefined;
        const reads = !isInsert || this.insertCarriesKey(rows?.initializer, ["[]"]);
        this.addAccess(summary, this.evaluate(element, ["table"], unit), isInsert ? "insert" : "write", reads, call, element);
      }
      return true;
    }
    if (helper) return true;
    if (ts.isPropertyAccessExpression(callee) && databaseMethods.has(callee.name.text) && this.isDatabase(callee.expression)) {
      const method = callee.name.text;
      if (method === "query") {
        summary.rawQueries.push(this.site(call));
        return true;
      }
      if (method === "select") {
        const read = call.arguments[0];
        if (read) this.addAccess(summary, this.evaluate(read, ["table"], unit), "select", true, call, read);
        return true;
      }
      const table = call.arguments[0];
      if (!table) return true;
      const reads = method === "insert" ? this.insertCarriesKey(call.arguments[1]) : true;
      this.addAccess(summary, this.evaluate(table, [], unit), method, reads, call, table);
      return true;
    }
    return false;
  }

  private summary(unit: Unit): Summary {
    const cached = this.summaries.get(unit);
    if (cached) return cached;
    const summary: Summary = { accesses: [], params: [], closureParams: [], unresolved: [], rawQueries: [], references: new Set(), calls: new Set() };
    this.summaries.set(unit, summary);
    this.inProgress.add(unit);
    const body = isFunctionUnit(unit) ? unit.body : unit;
    const visit = (node: TS.Node): void => {
      if (ts.isTypeNode(node) || ts.isInterfaceDeclaration(node) || ts.isTypeAliasDeclaration(node) || ts.isImportDeclaration(node)) return;
      if (node !== unit && isFunctionUnit(node)) {
        const parent = node.parent;
        const named = ts.isFunctionDeclaration(node) || ts.isMethodDeclaration(node)
          || ((ts.isVariableDeclaration(parent) || ts.isPropertyAssignment(parent) || ts.isPropertyDeclaration(parent)) && parent.initializer === node);
        if (named) {
          // A named inner function is its own unit, reached through its
          // references. Tables it takes from this unit's parameters are this
          // unit's parameter sinks.
          summary.references.add(node);
          const inner = this.inProgress.has(node) ? this.summaries.get(node) : this.summary(node);
          for (const { owner, ...sink } of inner?.closureParams ?? []) {
            if (owner === unit) summary.params.push(sink);
            else summary.closureParams.push({ ...sink, owner });
          }
          return;
        }
      }
      if (ts.isCallExpression(node) && !this.inspectCall(node, unit, summary)) {
        for (const target of this.callTargets(node)) {
          summary.calls.add(target);
          const callee = this.inProgress.has(target) ? this.summaries.get(target) : this.summary(target);
          for (const param of callee?.params ?? []) {
            const argument = node.arguments[param.index];
            if (!argument) continue;
            this.addAccess(summary, this.evaluate(argument, param.path, unit), param.operation, param.reads, node, argument);
          }
        }
      }
      if (ts.isIdentifier(node) && !this.isDeclarationName(node)) {
        for (const target of this.unitsOfSymbol(this.symbol(node))) {
          if (target === unit) continue;
          summary.references.add(target);
          // A function passed as a value has callers the analysis cannot see.
          if (isFunctionUnit(target) && !this.isCallee(node)) {
            const passed = this.inProgress.has(target) ? this.summaries.get(target) : this.summary(target);
            for (const param of passed?.params ?? []) {
              summary.unresolved.push({ operation: param.operation, site: this.site(node), expression: `${node.getText()} passed as a value (table from parameter ${param.index})` });
            }
          }
        }
      }
      ts.forEachChild(node, visit);
    };
    if (body) visit(body);
    this.inProgress.delete(unit);
    return summary;
  }

  private isDeclarationName(node: TS.Identifier): boolean {
    const parent = node.parent;
    return (ts.isVariableDeclaration(parent) || ts.isFunctionDeclaration(parent) || ts.isParameter(parent)
      || ts.isBindingElement(parent) || ts.isPropertyAssignment(parent) || ts.isMethodDeclaration(parent)
      || ts.isPropertyDeclaration(parent) || ts.isClassDeclaration(parent) || ts.isFunctionExpression(parent))
      && (parent as { name?: TS.Node }).name === node;
  }

  private isCallee(node: TS.Node): boolean {
    let current: TS.Node = node;
    while (ts.isPropertyAccessExpression(current.parent) && current.parent.name === current) current = current.parent;
    return ts.isCallExpression(current.parent) && current.parent.expression === current;
  }

  /** Every table access reachable from one exported Reducer definition. */
  reducer(entry: LocalReducerSource): LocalReducerAnalysis {
    const file = this.program.getSourceFile(resolve(this.root, entry.file));
    if (!file) throw new Error(`Reducer source ${entry.file} is not part of the analyzed program`);
    const moduleSymbol = this.checker.getSymbolAtLocation(file);
    const exported = moduleSymbol && this.checker.getExportsOfModule(moduleSymbol).find((symbol) => symbol.getName() === entry.exportName);
    const resolved = exported && exported.flags & ts.SymbolFlags.Alias ? this.checker.getAliasedSymbol(exported) : exported;
    const declaration = resolved?.valueDeclaration;
    let root: Unit | undefined;
    if (declaration && ts.isVariableDeclaration(declaration) && declaration.initializer) root = skipOuter(declaration.initializer);
    else if (declaration && isFunctionUnit(declaration) && declaration.body) root = declaration;
    if (!root) throw new Error(`Reducer ${entry.path} is not an exported definition in ${entry.file}`);
    const result: LocalReducerAnalysis = { path: entry.path, accesses: [], unresolved: [], rawQueries: [] };
    const seen = new Set<Unit>();
    const called = new Set<Unit>();
    const queue: Unit[] = [root];
    while (queue.length) {
      const unit = queue.pop()!;
      if (seen.has(unit)) continue;
      seen.add(unit);
      const summary = this.summary(unit);
      result.accesses.push(...summary.accesses);
      result.unresolved.push(...summary.unresolved);
      result.rawQueries.push(...summary.rawQueries);
      summary.calls.forEach((target) => called.add(target));
      summary.references.forEach((next) => queue.push(next));
    }
    // A function that takes a table from a parameter is resolved at its call
    // sites. One that nothing reachable calls directly (the Reducer handler,
    // whose arguments come from the client, or a function reached through
    // dynamic dispatch) has callers the analysis cannot see.
    for (const unit of seen) {
      if (!isFunctionUnit(unit) || called.has(unit)) continue;
      for (const param of this.summary(unit).params) {
        result.unresolved.push({ operation: param.operation, site: param.site, expression: `table from parameter ${param.index} of a function with no static call site` });
      }
    }
    return result;
  }
}

/**
 * Whether an access reads the table locally. The analysis checks inserts for
 * an explicit `_id`; a table keyed by another column is treated as read by any
 * insert, since the row may carry that key.
 */
export function readsLocally(access: LocalTableAccess, tableKeys: ReadonlyMap<string, string>): boolean {
  return access.reads || (access.operation === "insert" && (tableKeys.get(access.table) ?? "_id") !== "_id");
}

/** Tables a Reducer needs in the Local Replica to run locally, sorted. */
export function localDependencyTables(analysis: LocalReducerAnalysis, tableKeys: ReadonlyMap<string, string>): string[] {
  return [...new Set(analysis.accesses.filter((access) => readsLocally(access, tableKeys)).map((access) => access.table))].sort();
}

/** Accesses a local run cannot complete. `tableKeys` maps every local-schema table to its key. */
export function localDependencyViolations(
  analyses: readonly LocalReducerAnalysis[],
  localTables: ReadonlySet<string>,
  tableKeys: ReadonlyMap<string, string>,
): LocalDependencyViolation[] {
  const violations: LocalDependencyViolation[] = [];
  for (const reducer of analyses) {
    const seen = new Set<string>();
    const add = (key: string, violation: Omit<LocalDependencyViolation, "reducer">) => {
      if (seen.has(key)) return;
      seen.add(key);
      violations.push({ reducer: reducer.path, ...violation });
    };
    for (const access of reducer.accesses) {
      if (!tableKeys.has(access.table)) {
        add(`unknown\0${access.table}\0${access.site}`, { table: access.table, operation: access.operation, site: access.site, reason: "not a table in the tenant schema" });
      } else if (readsLocally(access, tableKeys) && !localTables.has(access.table)) {
        add(`${access.table}\0${access.site}`, { table: access.table, operation: access.operation, site: access.site, reason: "no generated local collection" });
      }
    }
    for (const unresolved of reducer.unresolved) {
      add(`unresolved\0${unresolved.site}\0${unresolved.expression}`, { table: "?", operation: unresolved.operation, site: unresolved.site, reason: `table cannot be determined statically: ${unresolved.expression}` });
    }
    for (const site of reducer.rawQueries) {
      add(`query\0${site}`, { table: "*", operation: "query", site, reason: "raw SQL cannot run locally" });
    }
  }
  return violations;
}

/** A readable report grouped by table, for CLI errors and warnings. */
export function formatLocalDependencyViolations(violations: readonly LocalDependencyViolation[]): string {
  const byTable = new Map<string, LocalDependencyViolation[]>();
  for (const violation of violations) {
    const key = `${violation.table} (${violation.reason})`;
    byTable.set(key, [...(byTable.get(key) ?? []), violation]);
  }
  const lines = [
    `${violations.length} data access(es) in locally executing Reducers cannot run on the device.`,
    "A Reducer the client runs locally touches data with no generated __local collection. The local run then stops",
    "and the intent is queued with no local result: the change waits for the server and is invisible offline.",
    "Give the table a visibility plan and a Replica Collection, keep the access out of the Reducer body, or make",
    "the Reducer online-only (offline: { mode: \"onlineOnly\", reason }) so it is not executed locally.",
    "",
  ];
  for (const [table, entries] of [...byTable].sort(([left], [right]) => left.localeCompare(right))) {
    const reducers = [...new Set(entries.map((entry) => entry.reducer))];
    lines.push(`${table}: ${reducers.length} Reducer(s)`);
    for (const site of [...new Set(entries.map((entry) => `${entry.site} ${entry.operation}`))].sort()) lines.push(`  at ${site}`);
    lines.push(`  e.g. ${reducers.slice(0, 8).join(", ")}${reducers.length > 8 ? ", ..." : ""}`);
  }
  return lines.join("\n");
}

/** Compiler options for the backend: its own tsconfig when present, otherwise bundler defaults. */
function compilerOptions(root: string, backendDir: string, declared?: string): TS.CompilerOptions {
  const defaults: TS.CompilerOptions = {
    target: ts.ScriptTarget.ES2022,
    module: ts.ModuleKind.ESNext,
    moduleResolution: ts.ModuleResolutionKind.Bundler,
    strict: true,
    skipLibCheck: true,
    allowJs: true,
    allowImportingTsExtensions: true,
    jsx: ts.JsxEmit.Preserve,
  };
  if (declared && !existsSync(resolve(root, declared))) throw new Error(`gonvex.json module.tsconfig ${JSON.stringify(declared)} does not exist`);
  const candidates = declared ? [resolve(root, declared)] : [join(backendDir, "tsconfig.json"), join(root, "tsconfig.json")];
  for (const candidate of candidates) {
    if (!existsSync(candidate)) continue;
    const config = ts.readConfigFile(candidate, ts.sys.readFile);
    if (config.error) {
      if (declared) throw new Error(`gonvex.json module.tsconfig: ${ts.flattenDiagnosticMessageText(config.error.messageText, "\n")}`);
      continue;
    }
    const parsed = ts.parseJsonConfigFileContent(config.config, ts.sys, dirname(candidate));
    // A solution-style tsconfig (`files: []` plus references) has no options.
    if (!declared && !parsed.options.moduleResolution && !parsed.options.paths && !parsed.options.baseUrl) continue;
    return { ...defaults, ...parsed.options, noEmit: true, composite: false, incremental: false, declaration: false };
  }
  return { ...defaults, noEmit: true };
}

/** Directories that hold @gonvex/module-sdk as seen by the project, symlinks resolved. */
function sdkRoots(backendDir: string, options: TS.CompilerOptions): string[] {
  const roots = new Set<string>();
  const resolution = ts.resolveModuleName("@gonvex/module-sdk", join(backendDir, "index.ts"), options, ts.sys);
  let directory = resolution.resolvedModule ? dirname(resolution.resolvedModule.resolvedFileName) : undefined;
  while (directory && directory !== dirname(directory)) {
    const manifest = join(directory, "package.json");
    if (existsSync(manifest)) {
      try {
        if (JSON.parse(readFileSync(manifest, "utf8")).name === "@gonvex/module-sdk") {
          roots.add(`${slash(directory)}/`);
          roots.add(`${slash(ts.sys.realpath?.(directory) ?? directory)}/`);
          break;
        }
      } catch { /* Keep walking. */ }
    }
    directory = dirname(directory);
  }
  return [...roots];
}

/**
 * Analyze the table dependencies of locally executing Reducers. Loads the
 * TypeScript compiler bundled with the CLI on first use.
 */
export async function analyzeLocalReducerDependencies(input: {
  root: string;
  backendDir: string;
  files: readonly string[];
  reducers: readonly LocalReducerSource[];
  /** Project-relative tsconfig whose compiler options (paths, resolution) apply to the backend. */
  tsconfig?: string;
}): Promise<LocalReducerAnalysis[]> {
  if (!input.reducers.length) return [];
  ts ??= createRequire(import.meta.url)("typescript") as typeof TS;
  const options = compilerOptions(input.root, input.backendDir, input.tsconfig);
  const program = ts.createProgram({ rootNames: [...input.files], options });
  const analyzer = new LocalReducerDependencyAnalyzer(program, input.root, sdkRoots(input.backendDir, options));
  return [...input.reducers]
    .sort((left, right) => left.path.localeCompare(right.path))
    .map((reducer) => analyzer.reducer(reducer));
}
