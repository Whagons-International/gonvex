// K07's test fixture (the P02 conformance app as a /dev/sync manifest) plus
// the app's `actions` module, so the K16 harness has a public Query
// (`host.querySurface`, `actions.boardCount`) and a public Action
// (`actions.publicEcho`). The modules get the kernel prelude through the
// vitest config, as K07's do.

// @ts-expect-error vitest alias
import * as actions from "@conformance-fixture/actions.ts";
import { bundleFor as k07Bundle, fixtureManifest as k07Manifest, FIXTURE_V1 } from "../../../reducer/test/fixture.ts";

export const K16_FIXTURE = "k16-fixture-v1";

interface Definition { kind?: unknown; internal?: unknown; options?: Record<string, unknown> }

/** The `actions` module's manifest entries, as codegen writes them (K07's `functionsOf`). */
function actionFunctions(): { functions: Record<string, unknown>; exports: Record<string, unknown> } {
  const functions: Record<string, unknown> = {};
  const exports: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(actions as Record<string, unknown>)) {
    const definition = value as Definition;
    if (!definition || typeof definition !== "object" || !["query", "reducer", "action"].includes(String(definition.kind))) continue;
    const options = definition.options ?? {};
    const json = (v: unknown) => JSON.parse(JSON.stringify(v ?? { kind: "any" })) as unknown;
    functions[`actions.${name}`] = {
      kind: definition.kind, handler: name, file: "gonvex/actions.ts", args: json(options.args), result: json(options.result),
      ...(definition.internal ? { internal: true } : {}), ...(options.public ? { public: true } : {}),
      ...(options.liveQueryPlan ? { liveQueryPlan: json(options.liveQueryPlan) } : {}),
      interactive: options.interactive !== false && !definition.internal,
    };
    exports[`actions.${name}`] = value;
  }
  return { functions, exports };
}

const extra = actionFunctions();

export function fixtureManifest(): Record<string, unknown> {
  const base = k07Manifest(1);
  const module = base.module as Record<string, unknown>;
  return { ...base, functions: { ...(base.functions as Record<string, unknown>), ...extra.functions }, module: { ...module, hash: K16_FIXTURE } };
}

export function bundleFor(hash: string): Record<string, unknown> | undefined {
  if (hash !== K16_FIXTURE) return k07Bundle(hash);
  return { ...k07Bundle(FIXTURE_V1), ...extra.exports };
}
