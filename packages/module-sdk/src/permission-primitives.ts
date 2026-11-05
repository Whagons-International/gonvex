/** Experimental source-bound permission IR. Declaration alone never activates
 * serving. V2 remains default; backend/provider capability checks fail closed.
 * No SQL, callbacks, client-supplied scope or opaque application masks. */
export type PermissionField = {
  readonly field: { readonly alias: string; readonly column: string };
  /** Initial adapter accepts TEXT/BINARY only; no implicit scalar coercion. */
  readonly type: "text";
  /** NULL either omits this fact or rejects normalization, explicitly. */
  readonly onNull: "omitFact" | "reject";
};
export type PermissionText = PermissionField | { readonly literal: string };
export type PermissionTable = { readonly table: string; readonly key: string; readonly as: string };
export type PermissionFilter =
  | { readonly op: "isNull" | "isNotNull"; readonly field: { readonly alias: string; readonly column: string } }
  | { readonly op: "textEq"; readonly left: PermissionText; readonly right: PermissionText };
export type PermissionJoin = {
  readonly from: PermissionTable;
  /** One correlated tuple; never project independent token sets. Inner join
   * NULL keys do not match. All keys must be TEXT with binary collation. */
  readonly on: readonly { readonly left: PermissionField; readonly right: PermissionField }[];
};
export type PermissionAtomProjection = {
  readonly namespace: string;
  readonly resource: PermissionText;
  readonly principal?: PermissionText;
};
export type PermissionSubjectProjection =
  | { readonly kind: "member"; readonly id: PermissionText }
  | { readonly kind: "group"; readonly namespace: string; readonly id: PermissionText };
export type PermissionRoleScope =
  | { readonly kind: "tenant" }
  | { readonly kind: "group"; readonly namespace: string; readonly id: PermissionText }
  | { readonly kind: "row"; readonly table: string; readonly key: PermissionText };
export type PermissionSourceBinding = {
  /** Versioned logical source. Support occurrence also includes every base/join
   * alias key and emitted fact index; OLD and NEW tuples are dependencies. */
  readonly id: string;
  readonly from: PermissionTable;
  readonly joins: readonly PermissionJoin[];
  readonly where: readonly PermissionFilter[];
} & (
  | { readonly kind: "grant"; readonly target: PermissionSubjectProjection; readonly atoms: readonly PermissionAtomProjection[] }
  | { readonly kind: "membership"; readonly group: { readonly namespace: string; readonly id: PermissionText }; readonly member: PermissionSubjectProjection }
  | { readonly kind: "row"; readonly table: string; readonly key: PermissionText; readonly universe: string;
      readonly atoms: readonly { readonly slot: string; readonly atom: PermissionAtomProjection }[] }
  | { readonly kind: "roleDefinition"; readonly role: { readonly id: PermissionText; readonly scope: PermissionRoleScope }; readonly atoms: readonly PermissionAtomProjection[] }
  | { readonly kind: "roleAssignment"; readonly role: { readonly id: PermissionText; readonly scope: PermissionRoleScope }; readonly target: PermissionSubjectProjection }
);
export type PermissionExpression =
  | { readonly op: "deny" }
  | { readonly op: "slot"; readonly slot: string }
  /** Refers to a declared atom, never an arbitrary caller atom. */
  | { readonly op: "has"; readonly atom: string }
  | { readonly op: "all" | "any"; readonly children: readonly PermissionExpression[] }
  | { readonly op: "except"; readonly universe: string; readonly allow: PermissionExpression; readonly remove: PermissionExpression };
export type PermissionPrimitiveDeclaration = {
  readonly version: 1;
  readonly atoms: Readonly<Record<string, { readonly namespace: string; readonly resource: string; readonly principal?: string }>>;
  readonly providers: Readonly<Record<string, {
    readonly adapter: "relational-v1";
    readonly version: string;
    readonly sources: readonly PermissionSourceBinding[];
  }>>;
  readonly collections: Readonly<Record<string, {
    readonly provider: string; readonly table: string; readonly key: string;
    readonly universe: string; readonly policy: PermissionExpression;
  }>>;
};

// The artifact compiler performs complete semantic/reference/dependency checks.
// This helper enforces immutable literal JSON at module execution as well; no
// function/provider callback can survive as an untracked permission input.
const freezeLiteral = (value: unknown, seen: Set<object>): unknown => {
  if (value === null || typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value !== "object" || seen.has(value)) throw new Error("permissionPrimitives requires finite literal JSON");
  const prototype = Object.getPrototypeOf(value);
  if (!Array.isArray(value) && prototype !== Object.prototype && prototype !== null) throw new Error("permissionPrimitives requires plain literal records");
  seen.add(value);
  let result: unknown;
  if (Array.isArray(value)) {
    const array: unknown[] = [];
    for (let i = 0; i < value.length; i += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, String(i));
      if (!descriptor || !("value" in descriptor)) throw new Error("permissionPrimitives forbids sparse arrays/accessors");
      array.push(freezeLiteral(descriptor.value, seen));
    }
    result = Object.freeze(array);
  } else {
    const object: Record<string, unknown> = Object.create(null);
    for (const key of Object.keys(value).sort()) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key)!;
      if (!("value" in descriptor)) throw new Error("permissionPrimitives forbids accessors");
      object[key] = freezeLiteral(descriptor.value, seen);
    }
    result = Object.freeze(object);
  }
  seen.delete(value);
  return result;
};
/** Literal source→fact declaration. This is NOT a provider executor or a
 * capability token. Unsupported adapters reject when bound by the runtime. */
/* @__NO_SIDE_EFFECTS__ */
export function permissionPrimitives(options: PermissionPrimitiveDeclaration): PermissionPrimitiveDeclaration {
  if (options.version !== 1) throw new Error("unsupported permissionPrimitives version");
  return freezeLiteral(options, new Set()) as PermissionPrimitiveDeclaration;
}
