// The tenant session an Action runs as (Rust `TenantSession`, the shape K06's
// control object returns from `tenantSessionForAccount`). It crosses RPC, so
// it is plain data.
import type { Identity } from "../../kernel/host/src/index.js";

export interface ActionSession {
  /** Server-owned route for a direct Workflow call's eventual reply; never part of module identity. */
  reply?: { host: string; connectionId: string; epoch: number; id: string; path: string; traceJson?: string };
  projectId: string;
  tenantId: string;
  account: { id: string; email?: string; name?: string; avatarUrl?: string };
  member: { id: string; accountId?: string; status?: string; role?: string; displayName?: string; permissions?: unknown };
  delegation?: unknown;
  developerSession?: Record<string, unknown> | null;
}

/** `TenantSession::is_anonymous`: an anonymous (public transport) session has no account. */
export const isAnonymous = (session: ActionSession): boolean => session.account.id === "";

export const SYSTEM_ACCOUNT_ID = "_gonvex_system";

/** `system_tenant_session`: crons, sweeps and outbox rows queued by system actors. */
export function systemSession(projectId: string, tenantId: string): ActionSession {
  return {
    projectId,
    tenantId,
    account: { id: SYSTEM_ACCOUNT_ID, email: "", name: "Gonvex", avatarUrl: "" },
    member: { id: SYSTEM_ACCOUNT_ID, accountId: SYSTEM_ACCOUNT_ID, status: "system", role: "system", displayName: "Gonvex", permissions: {} },
    delegation: null,
    developerSession: null,
  };
}

const nonempty = (value: string | undefined): string | null => {
  const trimmed = (value ?? "").trim();
  return trimmed ? trimmed : null;
};

/** The module-facing identity (`invocation()` in RT/execution.rs): no account or member when anonymous. */
export function identityOf(session: ActionSession): Identity {
  const anonymous = isAnonymous(session);
  return {
    account: anonymous ? null : {
      id: session.account.id,
      email: nonempty(session.account.email),
      name: nonempty(session.account.name),
      avatarUrl: nonempty(session.account.avatarUrl),
    },
    member: anonymous ? null : {
      id: session.member.id,
      accountId: session.member.accountId ?? session.account.id,
      status: nonempty(session.member.status),
      role: nonempty(session.member.role),
      displayName: nonempty(session.member.displayName),
      permissions: session.member.permissions ?? {},
    },
    tenant: { id: session.tenantId, projectId: session.projectId, name: null },
    developerSession: session.developerSession ?? null,
  };
}
