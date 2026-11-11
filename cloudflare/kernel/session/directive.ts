/**
 * The replica directive in `auth.result.result.replica` (A.1.4,
 * `modules.rs:507-567`). `scope` and `visibilityScope` are derived here from
 * the session; `epoch` comes from the module generation and the tenant's
 * database identity (0008), so the session layer receives it ready-made.
 *
 * serde_json with `preserve_order` writes these objects in insertion order with
 * the same escaping as `JSON.stringify` for strings, booleans and integers.
 * A float inside `permissions` would differ (serde writes `1.0`); member
 * permissions are booleans, strings and lists.
 */
import type { Json } from "./decode";

export interface ReplicaDirective {
  protocolVersion: 1;
  scope: string;
  visibilityScope: string;
  epoch: string;
}

const encoder = new TextEncoder();

export async function sha256Hex(text: string): Promise<string> {
  const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(text)));
  let out = "";
  for (const byte of digest) out += byte.toString(16).padStart(2, "0");
  return out;
}

export async function replicaDirective(input: {
  projectId: string;
  tenantId: string;
  accountId: string;
  permissions: Json;
  epoch: string;
}): Promise<ReplicaDirective> {
  const permissionsHash = await sha256Hex(JSON.stringify(input.permissions));
  const accountId = input.accountId.trim() === "" ? "anonymous" : input.accountId;
  const [scope, visibilityScope] = await Promise.all([
    sha256Hex(JSON.stringify({
      protocolVersion: 1, projectId: input.projectId, tenantId: input.tenantId, accountId, permissionsHash, epoch: input.epoch,
    })),
    sha256Hex(JSON.stringify({
      protocolVersion: 1, kind: "replica-visibility", projectId: input.projectId, tenantId: input.tenantId, accountId, permissionsHash,
    })),
  ]);
  return { protocolVersion: 1, scope, visibilityScope, epoch: input.epoch };
}

/**
 * The epoch the Rust runtime would compute (`modules.rs:516-526`): a hash of the
 * generation's function manifest and schema with the tenant's `database_url`.
 * Ports call this with the generation's own values (K19 provides them).
 */
export function replicaEpoch(input: { projectId: string; databaseUrl: string; functions: Json; schema: Json; moduleHash: string }): Promise<string> {
  return sha256Hex(JSON.stringify({
    protocolVersion: 1, project: input.projectId, database: input.databaseUrl,
    functions: input.functions, schema: input.schema, moduleHash: input.moduleHash,
  }));
}
