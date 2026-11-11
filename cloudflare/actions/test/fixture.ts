// Module artifacts for the K12 tests.
//
// - The conformance fixture (`cloudflare/conformance/fixtures/app`): its
//   `actions.*` and `board.*` functions, with the contracts gonvex codegen
//   wrote for them (golden/fixture-functions.json), turned into K04
//   `FunctionContract`s the way the Worker build does.
// - Test Actions for the S4 scenarios (fixtures/long.module.js).
// Resolved by the vitest alias (and dev/wrangler.jsonc); tsc does not type-check the fixture app (it needs the built SDK).
// @ts-expect-error alias
import * as actions from "@conformance-fixture/actions.ts";
// @ts-expect-error alias
import * as board from "@conformance-fixture/board.ts";
import type { FunctionContract, ModuleArtifact } from "../../kernel/host/src/index.js";
import manifest from "./golden/fixture-functions.json";
import * as long from "./fixtures/long.module.js";

export const PROJECT = "proj-k12";
export const ARTIFACT = "fixture-artifact-1";

const METADATA = ["public", "actionProfile", "offline", "optimistic", "interactive", "classification", "description", "agent", "actionCapabilities", "delivery", "replica"] as const;

type ManifestFunction = { kind: "query" | "reducer" | "action"; handler: string; args: unknown; result: unknown; internal?: boolean } & Record<string, unknown>;

/** Manifest entry -> FunctionContract (the metadata RT/modules.rs keeps, plus delivery). */
export function contractOf(path: string, entry: ManifestFunction): FunctionContract {
  const metadata: Record<string, unknown> = {};
  for (const name of METADATA) if (entry[name] !== undefined) metadata[name] = entry[name];
  return { path, kind: entry.kind, internal: entry.internal === true, export: entry.handler, argsSchema: entry.args, resultSchema: entry.result, metadata };
}

const any = { kind: "any" };
const agent = { actionProfile: "agent" };
const UP = "http://127.0.0.1:47812";

/** The S4 test Actions. */
const longFunctions: FunctionContract[] = [
  { path: "long.agent", kind: "action", export: "long_agent", argsSchema: any, resultSchema: any, metadata: { ...agent, actionCapabilities: { networkOrigins: [UP] } } },
  { path: "long.wait", kind: "action", export: "long_wait", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard" } },
  { path: "long.agentWait", kind: "action", export: "long_wait", argsSchema: any, resultSchema: any, metadata: { ...agent, interactive: true } },
  { path: "long.fail", kind: "action", export: "long_fail", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard" } },
  { path: "long.secret", kind: "action", export: "long_secret", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", actionCapabilities: { secrets: ["K12_SECRET"] } } },
  { path: "long.slowFetch", kind: "action", export: "long_slowFetch", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", actionCapabilities: { networkOrigins: [UP] } } },
  { path: "long.agentSlowFetch", kind: "action", export: "long_slowFetch", argsSchema: any, resultSchema: any, metadata: { ...agent, actionCapabilities: { networkOrigins: [UP] } } },
  { path: "long.publicWait", kind: "action", export: "long_publicWait", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", public: true } },
  { path: "long.concurrent", kind: "action", export: "long_concurrent", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", actionCapabilities: { networkOrigins: [UP] } } },
  { path: "long.ports", kind: "action", export: "long_ports", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", actionCapabilities: { storage: true, scheduler: true } } },
  { path: "long.sandbox", kind: "action", export: "long_sandbox", argsSchema: any, resultSchema: any, metadata: { ...agent, actionCapabilities: { sandbox: {} } } },
  { path: "long.standardFunctions", kind: "action", export: "long_wait", argsSchema: any, resultSchema: any, metadata: { actionProfile: "standard", actionCapabilities: { functions: true } } },
  { path: "long.slowRequest", kind: "action", export: "long_slowRequest", argsSchema: any, resultSchema: any, metadata: { ...agent, actionCapabilities: { networkOrigins: [UP] } } },
  { path: "long.effect", kind: "action", export: "long_effect", argsSchema: any, resultSchema: any, metadata: { ...agent } },
  { path: "queue.enqueue", kind: "reducer", export: "queue_enqueue", argsSchema: any, resultSchema: any, metadata: { interactive: false } },
];

export function fixtureArtifact(generation = 1, artifactHash = ARTIFACT): ModuleArtifact {
  const functions = Object.entries(manifest.functions as Record<string, ManifestFunction>).map(([path, entry]) => contractOf(path, entry));
  return {
    moduleId: PROJECT,
    generation,
    artifactHash,
    clientContract: 1,
    functions: [...functions, ...longFunctions],
    exports: { ...actions, ...board, ...long } as Record<string, unknown>,
  };
}

/** The fixture's `board` table (fixtures/app/migrations), as the tenant has it. */
export const BOARD_SQL = "CREATE TABLE IF NOT EXISTS board (id TEXT PRIMARY KEY NOT NULL, title TEXT NOT NULL, position INTEGER NOT NULL DEFAULT 0, bumps INTEGER NOT NULL DEFAULT 0)";
