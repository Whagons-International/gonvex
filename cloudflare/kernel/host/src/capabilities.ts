// Capability sets per function kind (CONTRACT.md B.1; MR/capabilities.rs,
// MR/lib.rs HostCall::capability / check_capability).
import type { FunctionKind } from "./globals.js";

export interface Capabilities {
  dbRead: boolean;
  dbWrite: boolean;
  actionOutbox: boolean;
  actionTools: boolean;
  scheduler: boolean;
  network: boolean;
  storage: boolean;
  sandbox: boolean;
  secrets: boolean;
  functions: boolean;
}

export const noCapabilities: Readonly<Capabilities> = Object.freeze({
  dbRead: false, dbWrite: false, actionOutbox: false, actionTools: false, scheduler: false,
  network: false, storage: false, sandbox: false, secrets: false, functions: false,
});

/** The maximum capability set of a kind, before host grants apply. */
export function structuralCapabilities(kind: FunctionKind): Capabilities {
  switch (kind) {
    case "query": return { ...noCapabilities, dbRead: true };
    case "reducer": return { ...noCapabilities, dbRead: true, dbWrite: true, actionOutbox: true, scheduler: true, storage: true };
    case "action": return { ...noCapabilities, actionTools: true, scheduler: true, network: true, storage: true, sandbox: true, secrets: true, functions: true };
  }
}

/** Effective = structural(kind) AND host grant. */
export function effectiveCapabilities(kind: FunctionKind, granted: Partial<Capabilities>): Capabilities {
  const structural = structuralCapabilities(kind);
  const out = { ...noCapabilities };
  for (const name of Object.keys(out) as (keyof Capabilities)[]) out[name] = structural[name] && granted[name] === true;
  return out;
}

/** Host grants the kernel computes for a Query or Reducer in the tenant object (RT/execution.rs:585-588, :1530-1533). */
export function databaseGrant(kind: "query" | "reducer", options: { anonymous?: boolean; public?: boolean; internal?: boolean; interactive?: boolean } = {}): Partial<Capabilities> {
  if (kind === "query") return { dbRead: true };
  const anonymous = options.anonymous === true;
  return {
    dbRead: true,
    dbWrite: true,
    actionOutbox: !anonymous,
    scheduler: !anonymous,
    storage: options.public === true || options.internal === true || options.interactive !== true,
  };
}

/** Host-call op kinds (the JSON `kind` tag used on every layer, B.2a). */
export type HostCallKind =
  | "dbQuery" | "dbInsert" | "dbUpdate" | "dbDelete" | "dbDeleteMany" | "permissionsHas"
  | "actionEnqueue" | "toolInvoke" | "functionInvoke" | "scheduleAfter" | "scheduleAt"
  | "fetch" | "fetchRead" | "fetchCancel" | "storage" | "sandbox";

/** The capability (by its Rust name) a host call needs. */
export const capabilityOf: Record<HostCallKind, string> = {
  dbQuery: "db_read", permissionsHas: "db_read",
  dbInsert: "db_write", dbUpdate: "db_write", dbDelete: "db_write", dbDeleteMany: "db_write",
  actionEnqueue: "action_outbox",
  toolInvoke: "action_tools",
  functionInvoke: "functions",
  scheduleAfter: "scheduler", scheduleAt: "scheduler",
  fetch: "network", fetchRead: "network", fetchCancel: "network",
  storage: "storage",
  sandbox: "sandbox",
};

const flagOf: Record<string, keyof Capabilities> = {
  db_read: "dbRead", db_write: "dbWrite", action_outbox: "actionOutbox", action_tools: "actionTools",
  scheduler: "scheduler", network: "network", storage: "storage", sandbox: "sandbox", functions: "functions",
};

/** `check_capability`: there is no `secrets` arm, so any other name is denied. */
export function allows(capabilities: Capabilities, capability: string): boolean {
  const flag = flagOf[capability];
  return flag !== undefined && capabilities[flag];
}
