// The P02 conformance fixture app (cloudflare/conformance/fixtures/app) as
// a /dev/sync manifest, built from its source definitions, plus the K07 test
// module. The bundles are part of this test Worker (0016 G0-2), keyed by the
// artifact hash the manifest names.
//
// The fixture modules are resolved through vitest aliases and get the kernel
// prelude (vitest.config.ts); tsc does not type-check them (they need the
// built SDK), hence the expect-error lines.

// @ts-expect-error vitest alias
import * as board from "@conformance-fixture/board.ts";
// @ts-expect-error vitest alias
import * as notes from "@conformance-fixture/notes.ts";
// @ts-expect-error vitest alias
import * as errorsModule from "@conformance-fixture/errors.ts";
// @ts-expect-error vitest alias
import * as host from "@conformance-fixture/host.ts";
// @ts-expect-error vitest alias
import * as storage from "@conformance-fixture/storage.ts";
// @ts-expect-error vitest alias
import * as boardV2 from "@conformance-fixture-v2/board.ts";
// @ts-expect-error JavaScript test module (module code, no types)
import * as k07 from "./rowids.module.js";
// @ts-expect-error vite raw import
import fixtureSql from "../../../conformance/fixtures/app/migrations/0001_fixture.sql?raw";
// @ts-expect-error vite raw import
import labelSql from "../../../conformance/fixtures/app-v2/migrations/0002_board_label.sql?raw";

export const FIXTURE_V1 = "k07-fixture-v1";
export const FIXTURE_V2 = "k07-fixture-v2";

/** Extra tables the K07 test module writes (a permission grant projection). */
const K07_SQL = `-- gonvex:scope tenant
CREATE TABLE IF NOT EXISTS k07_grants (_id TEXT PRIMARY KEY NOT NULL, memberId TEXT NOT NULL, permission TEXT NOT NULL, scope TEXT, deletedAt INTEGER);
`;

type Namespace = Record<string, unknown>;
interface Definition { kind?: unknown; internal?: unknown; options?: Record<string, unknown> }

/** `{ "<file>.<export>": manifest entry }` for every function export, as codegen writes it. */
function functionsOf(modules: Record<string, Namespace>): { functions: Record<string, unknown>; exports: Record<string, unknown> } {
  const functions: Record<string, unknown> = {};
  const exports: Record<string, unknown> = {};
  for (const [file, namespace] of Object.entries(modules)) {
    for (const [name, value] of Object.entries(namespace)) {
      const definition = value as Definition;
      if (!definition || typeof definition !== "object" || !["query", "reducer", "action"].includes(String(definition.kind))) continue;
      const options = definition.options ?? {};
      const path = `${file}.${name}`;
      const json = (v: unknown) => JSON.parse(JSON.stringify(v ?? { kind: "any" })) as unknown;
      functions[path] = {
        kind: definition.kind, handler: name, file: `gonvex/${file}.ts`, args: json(options.args), result: json(options.result),
        ...(definition.internal ? { internal: true } : {}), ...(options.public ? { public: true } : {}),
        ...(options.delivery ? { delivery: options.delivery } : {}), ...(options.replica ? { replica: options.replica } : {}),
        interactive: options.interactive !== false && !definition.internal,
      };
      exports[path] = value;
    }
  }
  return { functions, exports };
}

const b64 = (text: string) => btoa(String.fromCharCode(...new TextEncoder().encode(text)));

const v1 = functionsOf({ board, notes, errors: errorsModule, host, storage, k07 });
const v2 = functionsOf({ board: { ...board, ...boardV2 }, notes, errors: errorsModule, host, storage, k07 });

export interface ManifestOptions {
  /** `client-contract.json` (`{ version, offlineMaxAgeMs }`). */
  readonly clientContract?: { version: number; offlineMaxAgeMs: number | null };
  /** `module.permissionGrants`. */
  readonly permissionGrants?: Record<string, unknown>;
}

/** The fixture as `/dev/sync` receives it: generation 1, or 2 (the upgrade overlay). */
export function fixtureManifest(generation: 1 | 2 = 1, options: ManifestOptions = {}): Record<string, unknown> {
  const files: Record<string, string> = { "migrations/0001_fixture.sql": b64(fixtureSql), "migrations/0001_k07.sql": b64(K07_SQL) };
  if (generation === 2) files["migrations/0002_board_label.sql"] = b64(labelSql);
  if (options.clientContract) files["client-contract.json"] = b64(JSON.stringify(options.clientContract));
  const hash = (generation === 1 ? FIXTURE_V1 : FIXTURE_V2) + (options.clientContract ? `-c${options.clientContract.version}-${options.clientContract.offlineMaxAgeMs}` : "") + (options.permissionGrants ? "-grants" : "");
  return {
    functions: (generation === 1 ? v1 : v2).functions,
    schema: { tables: {}, controlPlaneTables: {}, tenantTables: {} },
    module: { language: "typescript", generation, hash, files, ...(options.permissionGrants ? { permissionGrants: options.permissionGrants } : {}) },
  };
}

/** The bundle of an artifact hash in this Worker version. */
export function bundleFor(hash: string): Record<string, unknown> | undefined {
  if (hash.startsWith(FIXTURE_V2)) return v2.exports;
  if (hash.startsWith(FIXTURE_V1)) return v1.exports;
  return undefined;
}
