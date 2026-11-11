/**
 * The Control Plane function registry and argument rules
 * (control.rs:4323-4610; contract C.0, C.1).
 */
import { ControlError, debugStr, invalid, unknownFunction, wrongKind } from "./errors";
import type { ControlKind } from "./types";

export type Authorization = "public" | "account" | "tenantAdmin" | "developer" | "projectAdmin";

export interface Definition {
  kind: ControlKind;
  authorization: Authorization;
  live: boolean;
}

const d = (kind: ControlKind, authorization: Authorization, live = false): Definition => ({ kind, authorization, live });

const TABLE: Record<string, Definition> = {
  "control.accounts.me": d("query", "account"),
  "control.accounts.updatePassword": d("reducer", "account"),
  "control.accounts.resetMemberPassword": d("reducer", "tenantAdmin"),
  "control.accounts.provisionMemberLogin": d("reducer", "tenantAdmin"),
  "control.auth.passwordLogin": d("action", "public"),
  "control.auth.exchangeExternalToken": d("action", "public"),
  "control.auth.refreshSession": d("action", "public"),
  "control.auth.logout": d("reducer", "account"),
  "control.auth.publicSettings": d("query", "public"),
  "control.auth.realms.list": d("query", "projectAdmin", true),
  "control.auth.realms.configure": d("reducer", "projectAdmin"),
  "control.auth.memberProviders": d("query", "tenantAdmin"),
  "control.tenants.mine": d("query", "account", true),
  "control.tenants.create": d("reducer", "account"),
  "control.tenants.getByDomain": d("query", "public"),
  "control.tenants.updateProfile": d("reducer", "tenantAdmin"),
  "control.tenants.updateTimezone": d("reducer", "tenantAdmin"),
  "control.tenants.delete": d("reducer", "tenantAdmin"),
  "control.tenants.setException": d("reducer", "projectAdmin"),
  "control.tenants.setSeatLimit": d("reducer", "projectAdmin"),
  "control.invitations.lookup": d("query", "public"),
  "control.invitations.list": d("query", "tenantAdmin", true),
  "control.invitations.create": d("reducer", "tenantAdmin"),
  "control.invitations.update": d("reducer", "tenantAdmin"),
  "control.invitations.revoke": d("reducer", "tenantAdmin"),
  "control.invitations.accept": d("reducer", "account"),
  "control.agentAuth.issue": d("reducer", "projectAdmin"),
  "control.agentAuth.revoke": d("reducer", "projectAdmin"),
  "control.agentAuth.claim": d("reducer", "account"),
  "control.project.developers.list": d("query", "projectAdmin", true),
  "control.project.developers.invite": d("reducer", "projectAdmin"),
  "control.project.developers.remove": d("reducer", "projectAdmin"),
  "control.developer.status": d("query", "account", true),
  "control.developer.provisionSelf": d("reducer", "developer"),
  "control.developer.removeSelf": d("reducer", "developer"),
  "control.developer.enter": d("reducer", "developer"),
  "control.developer.exit": d("reducer", "account"),
  "control.assistant.getDefaults": d("query", "projectAdmin", true),
  "control.assistant.setDefaults": d("reducer", "projectAdmin"),
  "control.voice.setRateCard": d("reducer", "projectAdmin"),
  "control.voice.setTenantEntitlement": d("reducer", "projectAdmin"),
  "control.voice.setUserOverride": d("reducer", "projectAdmin"),
  "control.voice.getConfiguration": d("query", "projectAdmin", true),
  "control.support.listTenants": d("query", "developer", true),
  "control.support.listSessions": d("query", "projectAdmin", true),
  "control.support.listErrors": d("query", "projectAdmin", true),
  "control.support.getSession": d("query", "projectAdmin"),
  "control.support.getError": d("query", "projectAdmin"),
  "control.support.getTenant": d("query", "projectAdmin"),
  "control.support.pruneSessions": d("reducer", "projectAdmin"),
  "control.support.sendCommand": d("reducer", "projectAdmin"),
  "control.support.createImpersonation": d("reducer", "projectAdmin"),
  "control.support.heartbeat": d("reducer", "account"),
  "control.support.ackCommand": d("reducer", "account"),
  "control.demos.create": d("reducer", "projectAdmin"),
  "control.demos.resetPassword": d("reducer", "projectAdmin"),
  "control.demos.delete": d("reducer", "projectAdmin"),
  "users.myTenants": d("query", "account", true),
  "tenants.getInvitationByToken": d("query", "public"),
  "tenants.acceptInvitation": d("reducer", "account"),
};

export const DEFINITION_NAMES = Object.freeze(Object.keys(TABLE));

export const definition = (path: string): Definition | undefined => (Object.hasOwn(TABLE, path) ? TABLE[path] : undefined);

export function requireDefinition(path: string, kind: ControlKind): Definition {
  const found = definition(path);
  if (!found) throw unknownFunction(path);
  if (found.kind !== kind) throw wrongKind(path, kind === "query" ? "Query" : kind === "reducer" ? "Reducer" : "Action");
  return found;
}

export const isLiveControlQuery = (path: string): boolean => {
  const found = definition(path);
  return !!found && found.kind === "query" && found.live;
};

export type Args = Record<string, unknown>;

const isObject = (value: unknown): value is Args => !!value && typeof value === "object" && !Array.isArray(value);

/** `exact_object`: unknown keys are refused, with the first one named in key order. */
export function exactObject(args: unknown, allowed: readonly string[]): Args {
  if (!isObject(args)) throw invalid("expected an object");
  // The reference's serde_json has `preserve_order`: the first unknown key in wire order.
  const unknown = Object.keys(args).find((key) => !allowed.includes(key));
  if (unknown !== undefined) throw invalid(`unknown field ${debugStr(unknown)}`);
  return args;
}

/** `empty_args`. */
export function emptyArgs(args: unknown): void {
  if (!isObject(args)) throw invalid("expected an object");
  const first = Object.keys(args)[0];
  if (first !== undefined) throw invalid(`unknown field ${debugStr(first)}`);
}

/** `required_string`: trimmed, non-empty. */
export function requiredString(object: Args, key: string): string {
  const value = object[key];
  const trimmed = typeof value === "string" ? value.trim() : "";
  if (!trimmed) throw invalid(`${key} is required`);
  return trimmed;
}

/** `optional_string`: trimmed when a string. */
export function optionalString(object: Args, key: string): string | undefined {
  const value = object[key];
  return typeof value === "string" ? value.trim() : undefined;
}

/** `normalize_email`. */
export function normalizeEmail(value: string): string {
  const email = value.trim().toLowerCase();
  if (new TextEncoder().encode(email).length > 320 || !email.includes("@") || email.startsWith("@") || email.endsWith("@")) {
    throw invalid("email is invalid");
  }
  return email;
}

export function membershipRole(value: string): string {
  if (!["owner", "admin", "member", "viewer"].includes(value)) throw invalid("role is invalid");
  return value;
}

/** `unique_string_array`: sorted, de-duplicated, trimmed. */
export function uniqueStringArray(value: unknown, name: string): string[] {
  if (!Array.isArray(value)) throw invalid(`${name} must be an array`);
  if (value.length > 500) throw invalid(`${name} cannot contain more than 500 values`);
  const seen = new Set<string>();
  for (const item of value) {
    const trimmed = typeof item === "string" ? item.trim() : "";
    if (!trimmed) throw invalid(`${name} must contain strings`);
    seen.add(trimmed);
  }
  // BTreeSet<String> order: byte order.
  return [...seen].sort(byteCompare);
}

export function byteCompare(a: string, b: string): number {
  return Buffer.compare(Buffer.from(a), Buffer.from(b));
}

export function invitationProviders(value: unknown): string[] {
  const providers = uniqueStringArray(value, "allowedAuthProviders");
  for (const provider of providers) {
    if (!["password", "google", "microsoft", "apple", "firebase", "external-oidc"].includes(provider)) {
      throw invalid(`authentication provider ${debugStr(provider)} is unsupported`);
    }
  }
  return providers;
}

export function requireAuthMode(mode: string): void {
  if (!["gonvex-native", "firebase", "external-oidc", "hybrid"].includes(mode)) {
    throw invalid(`unsupported project auth mode ${debugStr(mode)}`);
  }
}

export { ControlError };
