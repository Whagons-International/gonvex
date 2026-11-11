/**
 * Control Plane errors with the reference's exact text and classification.
 *
 * Text: `ControlError` / `DatabaseError` / `ExternalAuthError` Display in
 * rust/crates/runtime/src/control.rs:101-135, store/src/lib.rs:60-107 and
 * external_auth.rs:53-74. Class: error_class.rs:55-151. Auth codes:
 * lib.rs `database_auth_error_code`.
 */

export type ErrorClass = "rejected" | "transient" | "update_required" | "unauthenticated";
export type AuthErrorCode = "credential_rejected" | "member_removed" | "transient";

/** Rust `{:?}` of a `&str`: quoted, with `str::escape_debug` escapes. */
export function debugStr(value: string): string {
  let out = '"';
  for (const char of value) {
    const code = char.codePointAt(0)!;
    if (char === '"') out += '\\"';
    else if (char === "\\") out += "\\\\";
    else if (char === "\n") out += "\\n";
    else if (char === "\r") out += "\\r";
    else if (char === "\t") out += "\\t";
    else if (code === 0) out += "\\0";
    else if (code < 0x20 || code === 0x7f || (code >= 0x80 && code < 0xa0)) out += `\\u{${code.toString(16)}}`;
    else out += char;
  }
  return out + '"';
}

export class ControlError extends Error {
  constructor(
    message: string,
    readonly errorClass: ErrorClass = "rejected",
    readonly authCode: AuthErrorCode = "transient",
    readonly kind: string = "control",
  ) {
    super(message);
  }
}

/** `ControlError::InvalidArguments`. */
export const invalid = (message: string) => new ControlError(`invalid arguments: ${message}`, "rejected", "transient", "invalid");
export const unknownFunction = (path: string) => new ControlError(`unknown Control Plane function ${debugStr(path)}`);
export const wrongKind = (path: string, expected: "Query" | "Reducer" | "Action") =>
  new ControlError(`Control Plane function ${debugStr(path)} is not a ${expected}`);
export const authenticationRequired = () => new ControlError("authentication is required", "unauthenticated", "transient", "authenticationRequired");
export const tenantAdminRequired = () => new ControlError("tenant administrator access is required", "rejected", "transient", "tenantAdminRequired");
export const developerRequired = () => new ControlError("developer access is required");
export const projectAdminRequired = () => new ControlError("project administrator access is required");
export const administrationDuringImpersonation = () => new ControlError("project administration is unavailable during impersonation");
export const notImplemented = (path: string) => new ControlError(`Control Plane operation is not yet implemented in Rust: ${path}`);
export const databaseInvariant = (operation: string, detail: string) =>
  new ControlError(`Control Plane database invariant failed during ${operation}: ${detail}`);

/** `gonvex_store::DatabaseError`, wrapped transparently by `ControlError::Database`. */
export const db = {
  missingProject: () => new ControlError("project id is required", "unauthenticated", "transient", "MissingProject"),
  missingTenant: () => new ControlError("tenant id is required", "unauthenticated", "transient", "MissingTenant"),
  missingAccount: () => new ControlError("account id is required", "unauthenticated", "transient", "MissingAccount"),
  projectNotFound: (project: string) => new ControlError(`project ${debugStr(project)} is not active`, "transient", "transient", "ProjectNotFound"),
  tenantNotFound: (project: string, tenant: string) =>
    new ControlError(`tenant ${debugStr(tenant)} does not belong to project ${debugStr(project)}`, "transient", "transient", "TenantNotFound"),
  tenantDatabaseMissing: (tenant: string) =>
    new ControlError(`tenant ${debugStr(tenant)} has no configured physical database`, "transient", "transient", "TenantDatabaseMissing"),
  memberNotFound: (account: string) =>
    new ControlError(`active tenant member for account ${debugStr(account)} not found`, "rejected", "member_removed", "MemberNotFound"),
  invalidSession: () => new ControlError("invalid or expired app session", "unauthenticated", "credential_rejected", "InvalidSession"),
  sessionProjectMismatch: () =>
    new ControlError("app session was issued for a different project", "unauthenticated", "credential_rejected", "SessionProjectMismatch"),
  noTenantMembership: () => new ControlError("the account has no active tenant membership", "rejected", "member_removed", "NoTenantMembership"),
  /** A tenant object did not answer (`DatabaseError::Io` in spirit: transient). */
  unavailable: (detail: string) => new ControlError(`database file error: ${detail}`, "transient", "transient", "Io"),
};

/** `ExternalAuthError` (external_auth.rs:53-74). Wrapped transparently; class rejected. */
export const external = {
  invalidToken: () => new ControlError("external identity token is invalid", "rejected", "transient", "ExternalAuth"),
  wrongIssuer: () => new ControlError("external identity token issuer is invalid", "rejected", "transient", "ExternalAuth"),
  wrongAudience: () => new ControlError("external identity token audience is invalid", "rejected", "transient", "ExternalAuth"),
  expired: () => new ControlError("external identity token is expired", "rejected", "transient", "ExternalAuth"),
  missingSubject: () => new ControlError("external identity token subject is missing", "rejected", "transient", "ExternalAuth"),
  firebaseTenantMismatch: () => new ControlError("Firebase tenant does not match this auth realm", "rejected", "transient", "ExternalAuth"),
  unsafeJwks: () => new ControlError("external identity JWKS endpoint is unsafe", "rejected", "transient", "ExternalAuth"),
  keysUnavailable: () => new ControlError("external identity keys are unavailable", "rejected", "transient", "ExternalAuth"),
  firebaseAccountRevoked: () =>
    new ControlError("Firebase account is disabled or its sessions were revoked", "rejected", "transient", "ExternalAuth"),
  firebaseAccountUnavailable: () => new ControlError("Firebase account status is unavailable", "rejected", "transient", "ExternalAuth"),
};

export const isKind = (error: unknown, kind: string): boolean => error instanceof ControlError && error.kind === kind;

/** Normalizes anything thrown into a ControlError (unexpected errors are transient). */
export function asControlError(error: unknown): ControlError {
  if (error instanceof ControlError) return error;
  const message = error instanceof Error ? error.message : String(error);
  return new ControlError(message, "transient", "transient", "internal");
}
