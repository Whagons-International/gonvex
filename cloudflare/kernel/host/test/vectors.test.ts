// The published runtime-contract vectors (packages/module-sdk/runtime-contract),
// run through the K04 engine in workerd exactly as the Rust suite runs them
// (V8/runtime_contract_tests.rs): every case is a function of the runner
// bundle, all repeated twice in the same isolate so no state may leak between
// invocations.
import { describe, expect, it } from "vitest";
import * as runner from "../../../../packages/module-sdk/runtime-contract/runner.js";
import vectors from "../../../../packages/module-sdk/runtime-contract/vectors.json";
import { HostFailure, ModuleEngine, ModuleError, type FunctionContract, type ModuleHost } from "../src/index.js";

interface Vector {
  kind: "query" | "reducer" | "action";
  id: string;
  now?: number;
  intentEntropy?: string;
  argsSchema?: unknown;
  input?: unknown;
  capabilities?: Record<string, unknown>;
  expected?: unknown;
  expectedError?: string;
  expectActionGlobals?: boolean;
}

/** serde_json Value equality ignores key order. */
const sorted = (value: unknown): unknown => Array.isArray(value) ? value.map(sorted)
  : value !== null && typeof value === "object" ? Object.fromEntries(Object.keys(value).sort().map((key) => [key, sorted((value as Record<string, unknown>)[key])]))
  : value;

const cases = (vectors as { cases: Vector[] }).cases;

const contracts: FunctionContract[] = cases.map((vector) => ({
  path: vector.id,
  kind: vector.kind,
  export: "contract",
  argsSchema: vector.argsSchema ?? { kind: "any" },
  resultSchema: { kind: "any" },
  metadata: vector.capabilities ? { actionCapabilities: vector.capabilities } : {},
}));

/** Rust's NoExternalServices: every host call fails. */
const noExternalServices: ModuleHost = {
  call() { throw new HostFailure("contract case reached an external service"); },
};

describe("runtime-contract vectors", () => {
  it("has the 34 published cases", () => {
    expect(cases.length).toBe(34);
  });

  it("passes every case twice in one isolate", async () => {
    const engine = new ModuleEngine(contracts, runner as Record<string, unknown>);
    const failures: string[] = [];
    let passed = 0;
    for (let round = 0; round < 2; round++) {
      for (const vector of cases) {
        const start = Date.now();
        let value: unknown;
        let error: string | undefined;
        try {
          const result = await engine.invoke(noExternalServices, {
            function: vector.id,
            kind: vector.kind,
            args: vector.input ?? vector,
            context: {
              nowUnixMs: vector.now ?? 1792324800123,
              intentEntropy: vector.intentEntropy,
              identity: {},
              invocation: {},
              capabilities: { network: true },
            },
          });
          value = JSON.parse(result.value);
        } catch (cause) {
          if (!(cause instanceof ModuleError)) throw cause;
          error = cause.message;
        }
        const end = Date.now();
        let ok: boolean;
        if (vector.expectedError !== undefined) ok = error?.includes(vector.expectedError) ?? false;
        else if (error !== undefined) ok = false;
        else if (vector.expectActionGlobals) {
          const v = value as { now: number; constructed: number; random: number; bytes: number; uuid: boolean };
          ok = v.now >= start && v.now <= end && v.constructed >= start && v.constructed <= end && v.random >= 0 && v.random < 1 && v.bytes === 32 && v.uuid === true;
        } else ok = JSON.stringify(sorted(value)) === JSON.stringify(sorted(vector.expected));
        if (ok) passed++;
        else failures.push(`round ${round} ${vector.id}: ${error ?? JSON.stringify(value)}`);
      }
    }
    expect(failures).toEqual([]);
    expect(passed).toBe(68);
  });
});
