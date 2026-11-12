/**
 * `/dev/manifest` for service principals: `manifest_access`
 * (service_principal.rs:200-232) and the reduced body
 * (operations.rs:1473-1546).
 *
 * A recognized `gvx_svc_` credential is authorized only by its own
 * configuration: `manifest: true` and the project in its `projects`. It never
 * falls back to project-key or operator authorization. An unrecognized
 * credential is not a service principal, and the route's other checks apply
 * unchanged.
 */
/** What the decision reads of a configured principal (K06 `ServicePrincipalConfig`). */
export interface ManifestPrincipal {
  id: string;
  projects: string[];
  manifest: boolean;
}

/**
 * `find_principal`: the configured principal of a presented credential, or
 * null. K06's `findPrincipal` (control/src/auth.ts) trims, requires the
 * `gvx_svc_` prefix and compares every digest in constant time.
 */
export type FindPrincipal = (token: string) => ManifestPrincipal | null;

export type ManifestAccess =
  | { kind: "none" }
  | { kind: "allowed"; principal: string }
  | { kind: "denied"; principal: string };

/** The 403 text a service credential without manifest access to the project gets. */
export const MANIFEST_DENIED = "service principal is not allowed to read this project's manifest";

/** `operations::bearer`: the `Authorization` value without a `Bearer ` prefix, trimmed. */
export function bearer(authorization: string | null): string {
  const value = (authorization ?? "").trim();
  return value.startsWith("Bearer ") ? value.slice("Bearer ".length) : value;
}

/** `manifest_access(principals, bearer(headers), project)`. */
export function manifestAccess(find: FindPrincipal, authorization: string | null, project: string): ManifestAccess {
  const principal = find(bearer(authorization));
  if (!principal) return { kind: "none" };
  return principal.manifest && principal.projects.includes(project)
    ? { kind: "allowed", principal: principal.id }
    : { kind: "denied", principal: principal.id };
}

/** The body a service principal gets: the function catalog and the module, without `schema` and `visibility`. */
export function serviceManifestBody(project: string, functions: unknown, module: { hash: string; generation: number }): Record<string, unknown> {
  return { project, functions, module: { hash: module.hash, generation: module.generation } };
}
