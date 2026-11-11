// A member's resolved principal for one plan (visibility.rs
// `ResolvedVisibility`, `InheritedParent`, the parent overlay). Everything an
// in-memory check needs and nothing that can reach the database (V1).

import type { ParentAclIndex } from "./acl-index.ts";
import type { JsonValue } from "./json.ts";
import type { VisibilityPlan } from "./plan.ts";

/** Context values: `member.id`, `account.id`, `tenant.id`. */
export type DirectContext = Readonly<Record<string, string>>;

/** Token sets by name; each set holds non-null tokens only. Treat as immutable: replace a set (or the map) to change it. */
export type TokenSets = ReadonlyMap<string, ReadonlySet<string>>;

export interface ResolvedVisibility {
  /** Set only by verified developer admission when no active Member exists. */
  ephemeralMember: boolean;
  revision: number;
  direct: DirectContext;
  role: string;
  permissions: JsonValue;
  sets: TokenSets;
  fingerprint: string;
  /** Version 2 `inherits` parents, by parent table. */
  inherited: Map<string, InheritedParent>;
  /** Version 2 `derived` sets and `inheritsAny` associations: the tenant-shared index each one reads. */
  indexes: Map<string, ParentAclIndex>;
}

/** Everything an in-memory `inherits` check needs. Holds no database handle. */
export class InheritedParent {
  constructor(
    readonly plan: VisibilityPlan,
    readonly resolved: ResolvedVisibility,
    readonly index: ParentAclIndex,
  ) {}

  /** The parent's projected ACL row, or undefined when the parent does not exist. */
  row(key: string): JsonValue | undefined {
    const overlaid = overlaidParentRow(this.plan.table, key);
    if (overlaid !== NOT_OVERLAID) return overlaid;
    return this.index.get(key);
  }
}

/** Parent rows before a transaction: (parent table, key, row or undefined when absent). */
export type ParentOverlay = ReadonlyArray<readonly [string, string, JsonValue | undefined]>;

const NOT_OVERLAID = Symbol("not overlaid");
let overlay: ParentOverlay | undefined;

function overlaidParentRow(table: string, key: string): JsonValue | undefined | typeof NOT_OVERLAID {
  if (!overlay) return NOT_OVERLAID;
  for (const [parent, parentKey, row] of overlay) if (parent === table && parentKey === key) return row;
  return NOT_OVERLAID;
}

/**
 * Runs the synchronous `evaluate` with `rows` overriding parent index rows,
 * so the shared (already advanced) index answers with the rows as they were
 * before the transaction being applied. `evaluate` must not await.
 */
export function withParentOverlay<R>(rows: ParentOverlay | undefined, evaluate: () => R): R {
  if (!rows) return evaluate();
  const previous = overlay;
  overlay = rows;
  try {
    return evaluate();
  } finally {
    overlay = previous;
  }
}

export function emptyResolution(direct: DirectContext, role: string, permissions: JsonValue, revision = 0): ResolvedVisibility {
  return { ephemeralMember: false, revision, direct, role, permissions, sets: new Map(), fingerprint: "", inherited: new Map(), indexes: new Map() };
}
