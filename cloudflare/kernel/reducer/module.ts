/// <reference path="../host/src/shims.d.ts" />
// The active module of a project as the Reducer path reads it
// (rust/crates/runtime/src/modules.rs `ProjectModule`, `FunctionDefinition`),
// built from the CLI manifest `/dev/sync` receives. In stage 1 the bundle is
// part of the Worker version (0016 G0-2): the manifest carries the function
// table, the artifact hash and the client contract; the code comes from the
// bundle the Worker was built with, looked up by artifact hash.

import type { FunctionContract, ModuleArtifact, ModuleExports } from "../host/src/index.js";
import { grantSourceOf, type PermissionGrantSource } from "./permissions.ts";

export interface FunctionDefinition {
  readonly path: string;
  readonly kind: string;
  readonly internal: boolean;
  readonly public: boolean;
  /** SDK default: non-internal Queries and Reducers are interactive. */
  readonly interactive: boolean;
  /** `oneShot`, `live`, `replica` or empty. */
  readonly delivery: string;
  readonly replica: boolean;
  readonly argsSchema: unknown;
  readonly resultSchema: unknown;
  /** Bundle export name (`export`, else `handler`). */
  readonly exportName: string | undefined;
  readonly raw: Readonly<Record<string, unknown>>;
}

export interface ProjectModule {
  readonly projectId: string;
  readonly artifactHash: string;
  readonly generation: number;
  /** `client-contract.json` version, or null. */
  readonly clientContract: number | null;
  /** `client-contract.json` `offlineMaxAgeMs`: a positive integer, or null (unlimited, or no contract). */
  readonly offlineMaxAgeMs: number | null;
  readonly functions: ReadonlyMap<string, FunctionDefinition>;
  readonly permissionGrants: PermissionGrantSource | undefined;
  readonly schema: unknown;
}

export class ManifestError extends Error {}

function base64Json(text: unknown): unknown {
  if (typeof text !== "string") throw new ManifestError("invalid client contract encoding");
  const bytes = Uint8Array.from(atob(text), (c) => c.charCodeAt(0));
  return JSON.parse(new TextDecoder().decode(bytes)) as unknown;
}

/** `client_contract_from_module` (modules.rs:1232-1262), plus the offline window it validates. */
export function clientContractOf(module: Record<string, unknown>): { version: number | null; offlineMaxAgeMs: number | null } {
  const files = module.files as Record<string, unknown> | undefined;
  const encoded = files?.["client-contract.json"];
  if (encoded === undefined) return { version: null, offlineMaxAgeMs: null };
  const contract = base64Json(encoded) as Record<string, unknown>;
  const version = contract.version;
  if (typeof version !== "number" || !Number.isSafeInteger(version) || version <= 0) throw new ManifestError("invalid client contract version");
  const window = contract.offlineMaxAgeMs;
  if (window !== null && (typeof window !== "number" || !Number.isSafeInteger(window) || window <= 0)) throw new ManifestError("invalid offline window");
  return { version, offlineMaxAgeMs: window };
}

/** A project module from the CLI manifest (`manifest.functions`, `manifest.module`). */
export function projectModuleFromManifest(projectId: string, manifest: Record<string, unknown>): ProjectModule {
  const module = (manifest.module ?? {}) as Record<string, unknown>;
  const table = (manifest.functions ?? module.functions ?? {}) as Record<string, Record<string, unknown>>;
  const functions = new Map<string, FunctionDefinition>();
  for (const [path, raw] of Object.entries(table)) {
    const internal = raw.internal === true;
    const kind = String(raw.kind ?? "");
    functions.set(path, {
      path, kind, internal,
      public: raw.public === true,
      interactive: typeof raw.interactive === "boolean" ? raw.interactive : !internal,
      delivery: typeof raw.delivery === "string" ? raw.delivery : "",
      replica: raw.replica !== undefined && raw.replica !== null,
      argsSchema: raw.args,
      resultSchema: raw.result,
      exportName: typeof raw.export === "string" ? raw.export : typeof raw.handler === "string" ? raw.handler : undefined,
      raw,
    });
  }
  const contract = clientContractOf(module);
  return {
    projectId,
    artifactHash: String(module.hash ?? ""),
    generation: typeof module.generation === "number" ? module.generation : 0,
    clientContract: contract.version,
    offlineMaxAgeMs: contract.offlineMaxAgeMs,
    functions,
    permissionGrants: grantSourceOf(module.permissionGrants),
    schema: manifest.schema,
  };
}

/** The K04 artifact of a module generation: Query and Reducer contracts over the bundle's exports. */
export function moduleArtifact(module: ProjectModule, generation: number, exports: ModuleExports): ModuleArtifact {
  const functions: FunctionContract[] = [];
  for (const definition of module.functions.values()) {
    if (definition.kind !== "query" && definition.kind !== "reducer" && definition.kind !== "action") continue;
    if (definition.argsSchema === undefined || definition.resultSchema === undefined) continue;
    functions.push({
      path: definition.path,
      kind: definition.kind,
      internal: definition.internal,
      ...(definition.exportName ? { export: definition.exportName } : {}),
      argsSchema: definition.argsSchema,
      resultSchema: definition.resultSchema,
      metadata: { public: definition.public, interactive: definition.interactive, delivery: definition.delivery },
    });
  }
  return {
    moduleId: module.projectId,
    generation,
    artifactHash: module.artifactHash,
    clientContract: module.clientContract ?? undefined,
    functions,
    exports,
    schema: module.schema,
  };
}

/** `ProjectModule::accepts_client_artifact`. */
export function acceptsClientArtifact(module: ProjectModule, expected: string, contract: bigint | undefined): boolean {
  return expected === module.artifactHash || (module.clientContract !== null && contract !== undefined && contract === BigInt(module.clientContract));
}
