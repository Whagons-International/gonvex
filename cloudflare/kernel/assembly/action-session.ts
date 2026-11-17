import type { TenantSession } from "../session/ports";
import type { ActionSession } from "../../actions/src/session";
export function actionSession(session: TenantSession): ActionSession {
  const a = session.identity.account, m = session.member;
  return {
    projectId: session.route.projectId, tenantId: session.route.tenantId,
    account: { id: a.id, email: a.email, name: a.name, avatarUrl: a.avatarUrl },
    member: { id: m.id, accountId: m.accountId, status: m.status, role: m.role, displayName: m.displayName, permissions: m.permissions },
    delegation: session.delegation, developerSession: session.developerSession as Record<string, unknown> | null,
  };
}
