/** Literal IR validation and dependency extraction. No application callbacks or
 * SQL strings. Runtime normalization/serving capabilities are checked separately. */
import type { JsonValue } from "./manifest-types.js";
export type PermissionDependency = {
  source: string; table: string; key: string; alias: string; columns: string[];
  images: "oldAndNew";
};
export type PermissionPrimitiveManifest = {
  version: 1;
  declaration: JsonValue;
  dependencies: Record<string, PermissionDependency[]>;
};
type RecordValue = Record<string, unknown>;
const identifier = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
function record(value: unknown, keys?: readonly string[]): RecordValue {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("permissionPrimitives: expected literal record");
  const result = value as RecordValue;
  if (![Object.prototype, null].includes(Object.getPrototypeOf(result))) throw new Error("permissionPrimitives: plain literal record required");
  if (keys && Object.keys(result).some((key) => !keys.includes(key))) throw new Error("permissionPrimitives: unknown field");
  return result;
}
function string(value: unknown): string {
  if (typeof value !== "string" || !value.length) throw new Error("permissionPrimitives: expected nonempty string");
  return value;
}
function name(value: unknown): string {
  const result = string(value);
  if (!identifier.test(result)) throw new Error(`permissionPrimitives: invalid identifier ${JSON.stringify(result)}`);
  return result;
}
function array(value: unknown): unknown[] {
  if (!Array.isArray(value)) throw new Error("permissionPrimitives: expected literal array");
  return value;
}

export function compilePermissionPrimitives(value: unknown): PermissionPrimitiveManifest {
  const declaration = record(value, ["version", "atoms", "providers", "collections"]);
  if (declaration.version !== 1) throw new Error("permissionPrimitives: unsupported version");
  const atoms = record(declaration.atoms);
  for (const [id, value] of Object.entries(atoms)) {
    string(id);
    const atom = record(value, ["namespace", "resource", "principal"]);
    string(atom.namespace); string(atom.resource);
    if (atom.principal !== undefined) string(atom.principal);
  }
  const providers = record(declaration.providers);
  const dependencies: Record<string, PermissionDependency[]> = Object.create(null);
  const rowSlots = new Map<string, Set<string>>();
  const rowScopes = new Map<string, Set<string>>();
  for (const [providerId, value] of Object.entries(providers)) {
    string(providerId);
    const provider = record(value, ["adapter", "version", "sources"]);
    if (provider.adapter !== "relational-v1") throw new Error("permissionPrimitives: unsupported provider adapter");
    string(provider.version);
    const deps: PermissionDependency[] = [];
    const sources = new Set<string>();
    rowSlots.set(providerId, new Set()); rowScopes.set(providerId, new Set());
    for (const value of array(provider.sources)) {
      const source = record(value, ["id", "from", "joins", "where", "kind", "target", "atoms", "group", "member", "table", "key", "universe", "role"]);
      const id = string(source.id);
      if (sources.has(id)) throw new Error("permissionPrimitives: duplicate source id");
      sources.add(id);
      const aliases = new Map<string, PermissionDependency>();
      const table = (value: unknown): void => {
        const input = record(value, ["table", "key", "as"]);
        const alias = name(input.as), key = name(input.key);
        if (aliases.has(alias)) throw new Error("permissionPrimitives: duplicate join alias");
        const dependency: PermissionDependency = { source: id, table: name(input.table), key, alias, columns: [key], images: "oldAndNew" };
        aliases.set(alias, dependency); deps.push(dependency);
      };
      const field = (value: unknown): void => {
        const input = record(value, ["alias", "column"]);
        const dependency = aliases.get(name(input.alias));
        if (!dependency) throw new Error("permissionPrimitives: undeclared/future alias");
        const column = name(input.column);
        if (!dependency.columns.includes(column)) dependency.columns.push(column);
      };
      const text = (value: unknown): void => {
        const input = record(value);
        if (Object.keys(input).length === 1 && typeof input.literal === "string") return;
        record(input, ["field", "type", "onNull"]);
        if (input.type !== "text" || !["omitFact", "reject"].includes(String(input.onNull))) throw new Error("permissionPrimitives: unsupported scalar/null adapter");
        field(input.field);
      };
      const atom = (value: unknown): void => {
        const input = record(value, ["namespace", "resource", "principal"]);
        string(input.namespace); text(input.resource);
        if (input.principal !== undefined) text(input.principal);
      };
      const subject = (value: unknown): void => {
        const input = record(value, ["kind", "id", "namespace"]);
        if (input.kind !== "member" && input.kind !== "group") throw new Error("permissionPrimitives: subject must explicitly be member/group");
        if (input.kind === "group") string(input.namespace);
        else if (input.namespace !== undefined) throw new Error("permissionPrimitives: member cannot have a group namespace");
        text(input.id);
      };
      const role = (value: unknown): void => {
        const input = record(value, ["id", "scope"]); text(input.id);
        const scope = record(input.scope, ["kind", "namespace", "id", "table", "key"]);
        if (scope.kind === "group") { record(scope, ["kind", "namespace", "id"]); string(scope.namespace); text(scope.id); }
        else if (scope.kind === "row") { record(scope, ["kind", "table", "key"]); name(scope.table); text(scope.key); }
        else if (scope.kind === "tenant") record(scope, ["kind"]);
        else throw new Error("permissionPrimitives: unsupported role scope");
      };
      const sourceKeys: Record<string, string[]> = {
        grant: ["target", "atoms"], membership: ["group", "member"], row: ["table", "key", "universe", "atoms"],
        roleDefinition: ["role", "atoms"], roleAssignment: ["role", "target"],
      };
      const own = sourceKeys[String(source.kind)];
      if (!own) throw new Error("permissionPrimitives: unsupported source kind");
      record(source, ["id", "kind", "from", "joins", "where", ...own]);
      table(source.from);
      for (const value of array(source.joins)) {
        const join = record(value, ["from", "on"]);
        const before = new Set(aliases.keys());
        table(join.from);
        const joined = name(record(join.from).as);
        const tuple = array(join.on);
        if (!tuple.length) throw new Error("permissionPrimitives: empty join tuple");
        for (const value of tuple) {
          const pair = record(value, ["left", "right"]);
          text(pair.left); text(pair.right);
          const left = record(record(pair.left).field), right = record(record(pair.right).field);
          if (!before.has(name(left.alias)) || name(right.alias) !== joined) throw new Error("permissionPrimitives: joins require old-alias → new-alias correlated fields");
        }
      }
      for (const value of array(source.where)) {
        const filter = record(value, ["op", "field", "left", "right"]);
        if (filter.op === "isNull" || filter.op === "isNotNull") { record(filter, ["op", "field"]); field(filter.field); }
        else if (filter.op === "textEq") { record(filter, ["op", "left", "right"]); text(filter.left); text(filter.right); }
        else throw new Error("permissionPrimitives: unsupported filter adapter");
      }
      switch (source.kind) {
        case "grant": subject(source.target); array(source.atoms).forEach(atom); break;
        case "membership": {
          const group = record(source.group, ["namespace", "id"]);
          string(group.namespace); text(group.id); subject(source.member); break;
        }
        case "row": {
          const table = name(source.table), universe = string(source.universe); text(source.key);
          rowScopes.get(providerId)!.add(JSON.stringify([table, universe]));
          for (const value of array(source.atoms)) {
            const item = record(value, ["slot", "atom"]);
            rowSlots.get(providerId)!.add(JSON.stringify([table, universe, string(item.slot)])); atom(item.atom);
          }
          break;
        }
        case "roleDefinition": role(source.role); array(source.atoms).forEach(atom); break;
        case "roleAssignment": role(source.role); subject(source.target); break;
        default: throw new Error("permissionPrimitives: unsupported source kind");
      }
    }
    for (const dependency of deps) dependency.columns.sort();
    dependencies[providerId] = deps;
  }
  const collections = record(declaration.collections);
  for (const [path, value] of Object.entries(collections)) {
    string(path);
    const collection = record(value, ["provider", "table", "key", "universe", "policy"]);
    const provider = string(collection.provider), table = name(collection.table), universe = string(collection.universe); name(collection.key);
    if (!Object.hasOwn(providers, provider) || !rowScopes.get(provider)!.has(JSON.stringify([table, universe]))) throw new Error("permissionPrimitives: collection requires a bound finite row universe");
    const expression = (value: unknown, depth: number): void => {
      if (depth > 64) throw new Error("permissionPrimitives: expression depth exceeds 64");
      const input = record(value, ["op", "slot", "atom", "children", "universe", "allow", "remove"]);
      const keys: Record<string, string[]> = { deny: [], has: ["atom"], slot: ["slot"], all: ["children"], any: ["children"], except: ["universe", "allow", "remove"] };
      const own = keys[String(input.op)];
      if (!own) throw new Error("permissionPrimitives: unsupported policy operator");
      record(input, ["op", ...own]);
      switch (input.op) {
        case "deny": break;
        case "has": if (!Object.hasOwn(atoms, string(input.atom))) throw new Error("permissionPrimitives: undeclared required atom"); break;
        case "slot": if (!rowSlots.get(provider)!.has(JSON.stringify([table, universe, string(input.slot)]))) throw new Error("permissionPrimitives: slot has no typed source binding"); break;
        case "all": case "any": {
          const children = array(input.children);
          if (input.op === "all" && !children.length) throw new Error("permissionPrimitives: empty all rejected");
          children.forEach((child) => expression(child, depth + 1)); break;
        }
        case "except":
          if (input.universe !== universe) throw new Error("permissionPrimitives: subtraction universe mismatch");
          expression(input.allow, depth + 1); expression(input.remove, depth + 1); break;
        default: throw new Error("permissionPrimitives: unsupported policy operator");
      }
    };
    expression(collection.policy, 0);
  }
  return { version: 1, declaration: value as JsonValue, dependencies };
}
