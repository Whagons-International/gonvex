// Kernel against Rust, before the harness can drive the kernel's `action.call`
// (K05/K07 wire it): the conformance scenario `actions`
// (cloudflare/conformance/scenarios/actions.mjs) step by step, with the same
// fixture Actions and the same upstream, run through the K12 runtime against
// a real tenant object. The expected values are what the Rust runtime at
// 0bf3ad13 answered (golden/actions-rust.json, recorded by
// scripts/record-golden.mjs from 3 Rust-against-Rust runs).
//
// The harness's normalization applies: the project id is `<project>`, the
// artifact hash `<artifact>`, and JavaScript stack lines are not contract
// (0015 S0-1).
//
// Three answers wait on other packages and are asserted as they are today:
// a `query` tool and `ctx.functions` on a structured plan (`boardRows`) or a
// replica collection (`board.all`) need the plan executors (K08, K10). Rust's
// value for each is in the golden file and in PENDING below.
import { env } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import golden from "./golden/actions-rust.json";
import { ActionRuntime } from "../src/runtime.js";
import type { TenantActionPort } from "../src/ports.js";
import { ARTIFACT, PROJECT } from "./fixture.js";
import { FakeScheduler, member, registry, testControl, testFetcher } from "./worker.js";

type Observation = Record<string, unknown>;
const rust = golden.steps as Record<string, Observation>;

function normalize(value: unknown): unknown {
  const text = JSON.stringify(value)
    .replaceAll(PROJECT, "<project>")
    .replaceAll(ARTIFACT, "<artifact>")
    // JavaScript stack frames after the message (0015 S0-1).
    .replace(/(\\n\s*at [^"\\]*(?:\\.[^"\\]*)*?)+(?=")/g, "");
  return JSON.parse(text);
}

/** Fields whose kernel answer waits on the plan executors (K08/K10). */
const PENDING: Record<string, { step: string; label: string; field: string; kernel: RegExp }> = {
  toolsBefore: { step: "tools", label: "tools", field: "before", kernel: /module export for actions\.boardRows is not a callable query handler/ },
  toolsAfter: { step: "tools", label: "tools", field: "after", kernel: /module export for actions\.boardRows is not a callable query handler/ },
  functionsReplica: { step: "functions.invoke", label: "functions", field: "replica", kernel: /module export for board\.all is not a callable query handler/ },
};

describe("conformance scenario `actions` on the kernel", () => {
  it("answers every step as the Rust runtime does", async () => {
    const tenantId = `parity-${crypto.randomUUID()}`;
    const tenant = env.TENANT.getByName(tenantId);
    await tenant.configure(tenantId);
    const runtime = new ActionRuntime({
      registry: registry(),
      tenant: tenant as unknown as TenantActionPort,
      control: testControl,
      scheduler: new FakeScheduler(),
      fetcher: testFetcher,
    });
    const session = member(tenantId);
    const call = async (path: string, args: unknown) => {
      const report = await runtime.execute({ session, path, args });
      return normalize(report.ok ? { result: JSON.parse(report.value) } : { error: report.error });
    };

    const kernel: Record<string, Observation> = {};
    const step = async (name: string, observations: Array<[string, string, unknown]>) => {
      kernel[name] = {};
      for (const [label, path, args] of observations) kernel[name]![label] = await call(path, args);
    };
    await step("an Action with no capabilities", [["surface", "actions.surface", {}]]);
    await step("fetch within the declared origin", [["fetch", "actions.fetchProbe", {}]]);
    await step("an event stream read as it arrives", [["stream", "actions.streamProbe", {}]]);
    await step("an undeclared origin is a sticky denial", [["undeclared", "actions.fetchUndeclared", {}], ["relative", "actions.fetchRelative", {}]]);
    await step("an unconfigured secret", [["secret", "actions.secretMissing", {}]]);
    await step("tools", [["tools", "actions.toolsProbe", { id: "tool-row" }]]);
    await step("functions.invoke", [["functions", "actions.functionsProbe", { id: "function-row" }]]);
    await step("scheduler", [["scheduler", "actions.schedulerProbe", {}]]);
    await step("storage", [["storage", "actions.storageProbe", {}]]);
    await step("errors", [
      ["throws", "actions.fails", {}], ["unknown", "actions.nope", {}], ["wrongKind", "actions.boardCount", {}],
      ["badArgs", "actions.publicEcho", { text: 1 }], ["internal", "actions.internalAdd", { id: "x" }],
    ]);

    // The pending fields: the kernel's answer today, then Rust's value in its place.
    for (const pending of Object.values(PENDING)) {
      const result = (kernel[pending.step]![pending.label] as { result: Record<string, unknown> }).result;
      expect(JSON.stringify(result[pending.field])).toMatch(pending.kernel);
      result[pending.field] = ((rust[pending.step]![pending.label] as { result: Record<string, unknown> }).result)[pending.field];
    }
    for (const name of Object.keys(rust)) expect.soft(kernel[name], name).toEqual(rust[name]);
    expect(Object.keys(kernel).sort()).toEqual(Object.keys(rust).sort());

    // The tools and functions wrote through the tenant's own Reducers.
    expect(await tenant.board()).toEqual([
      { id: "function-row", title: "from functions" },
      { id: "tool-row", title: "from a tool" },
      { id: "tool-row-hidden", title: "internal" },
    ]);
  });
});
