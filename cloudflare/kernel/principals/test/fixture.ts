// The conformance fixture's board and principals modules plus the K17 test
// module, as a /dev/sync manifest, and their bundle (part of this test Worker,
// 0016 G0-2). Fixture modules come through vitest aliases with the kernel
// prelude (vitest.config.mjs); tsc does not type-check them.

// @ts-expect-error vitest alias
import * as board from "@conformance-fixture/board.ts";
// @ts-expect-error vitest alias
import * as principals from "@conformance-fixture/principals.ts";
// @ts-expect-error JavaScript test module (module code, no types)
import * as k17 from "./principals.module.js";
// @ts-expect-error vite raw import
import fixtureSql from "../../../conformance/fixtures/app/migrations/0001_fixture.sql?raw";

export const FIXTURE = "k17-fixture-v1";

type Namespace = Record<string, unknown>;
interface Definition { kind?: unknown; internal?: unknown; options?: Record<string, unknown> }

function functionsOf(modules: Record<string, Namespace>) {
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
const built = functionsOf({ board, principals, k17 });

export function fixtureManifest(): Record<string, unknown> {
  return {
    functions: built.functions,
    schema: { tables: {}, controlPlaneTables: {}, tenantTables: {} },
    module: { language: "typescript", generation: 1, hash: FIXTURE, files: { "migrations/0001_fixture.sql": b64(fixtureSql) } },
  };
}

export const bundleFor = (hash: string): Record<string, unknown> | undefined => (hash === FIXTURE ? built.exports : undefined);
