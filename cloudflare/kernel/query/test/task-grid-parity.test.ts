// K08 done_when: Live Query frames equal the Rust runtime's on the task grid
// scenario (cloudflare/conformance/scenarios/live-query-task-grid.mjs).
//
// golden/rust-task-grid.json is Rust's own run of that scenario
// (conformance/src/live-query-golden.mjs): the change log of every write and
// the query frames Bob and Carol sent and received, per step. This test
// replays it in workerd: each write's change rows are applied to the tables
// and fed to the engine as that revision's feed event, and each client frame
// is dispatched at the revision Rust answered it at. Both sides are then
// normalized by the harness's own normalizer and compared step by step, and
// every `windowRevision` (SHA-256 of the result bytes) must equal Rust's,
// which holds only if the result JSON is byte-identical.
import { describe, expect, it } from "vitest";
// @ts-ignore: plain ESM from the conformance harness (no declarations)
import { frameText, normalizeSession } from "../../../conformance/src/normalize.mjs";
import golden from "./golden/rust-task-grid.json";
import type { SocketView } from "../../session/ports";
import type { ClientFrame } from "../../session/decode";
import type { FeedStats, LogChange } from "../index";
import { sha256Hex } from "../../capture/digest";
import { applyChange, engine, module, provision, session, setRevision, withObject } from "./helpers";

interface Recorded { step: number; dir: "in" | "out"; frame: Record<string, unknown> }
interface Action { step: number; write?: string; revision?: number | null; send?: string; frame?: Record<string, unknown>; revisions?: (number | null)[]; resumeOf?: string }

const g = golden as unknown as {
  migration: string; keys: Record<string, string>; functions: Record<string, unknown>; visibility: Record<string, unknown>;
  members: Array<Record<string, string>>; epoch: string; baseRevision: number; unattributed: number[];
  revisions: Array<{ revision: number; changes: LogChange[] }>; steps: string[]; actions: Action[]; frames: Record<string, Recorded[]>;
  rawIds: Record<string, string>;
};

function byStep(records: Recorded[]): Map<number, string[]> {
  const normalized = normalizeSession(records.map((r) => ({ ...r, socket: 1 }))) as Array<{ step: number; dir: string; frame: unknown }>;
  const out = new Map<number, string[]>();
  for (const entry of normalized) {
    const list = out.get(entry.step) ?? [];
    list.push(`${entry.dir} ${frameText(entry.frame)}`);
    out.set(entry.step, list);
  }
  for (const list of out.values()) list.sort();
  return out;
}

describe("task grid parity with the Rust runtime", () => {
  it("has a golden recorded from Rust with every write attributed", () => {
    expect(g.revisions.length).toBeGreaterThan(40);
    expect(g.unattributed).toEqual([]);
    expect(Object.keys(g.functions)).toContain("tasks.grid");
  });

  it("sends the same query frames as Rust in every step", async () => {
    const result = await withObject(async (state) => {
      provision(state, { epoch: g.epoch, revision: g.baseRevision, members: g.members, migration: g.migration });
      const { engine: queries } = engine(state, module({ functions: g.functions as never, visibility: g.visibility as never }));
      const accounts = Object.fromEntries(g.members.map((m) => [m.id, m.account_id]));
      const views: Record<string, SocketView> = {};
      for (const name of ["bob", "carol"]) {
        const id = `<member:${name}@a>`;
        views[name] = { connectionId: name, tenant: session({ id, accountId: accounts[id]! }) } as unknown as SocketView;
      }
      const revisions = new Map(g.revisions.map((r) => [r.revision, r]));
      const records: Record<string, Recorded[]> = { bob: [], carol: [] };
      const latest = new Map<string, string>();
      const stats: FeedStats = { reruns: 0, executions: 0, skipped: 0, statements: 0, rowsRead: 0 };
      const hashes: Record<string, string[]> = { bob: [], carol: [] };
      const receive = (name: string, step: number, text: string) => {
        const frame = JSON.parse(text) as Record<string, unknown>;
        if (frame.type === "query.result" && typeof frame.windowRevision === "string") {
          // The result's bytes as Rust would hash them: the same text with the real ids.
          let bytes = text.slice(text.indexOf('"result":') + 9, text.lastIndexOf(',"subscriptionRevision":'));
          for (const [alias, raw] of Object.entries(g.rawIds)) bytes = bytes.split(alias).join(raw);
          hashes[name]!.push(`${frame.id}:${sha256Hex(bytes)}`);
        }
        if (frame.type === "query.result" && typeof frame.windowRevision === "string") latest.set(`${name}:${frame.id}`, frame.windowRevision);
        records[name]!.push({ step, dir: "in", frame });
      };
      let applied = g.baseRevision;
      const applyThrough = async (target: number, step: number) => {
        while (applied < target) {
          applied += 1;
          const entry = revisions.get(applied);
          for (const change of entry?.changes ?? []) applyChange(state.storage.sql, g.keys, change as never);
          setRevision(state, applied);
          const frames = await queries.live.applyFeed({ kind: "transaction", revision: applied, epoch: g.epoch, changes: entry?.changes ?? [] }, stats);
          for (const [name, list] of frames) for (const text of list) receive(name, step, text);
        }
      };
      for (const action of g.actions) {
        if (action.write) { await applyThrough(action.revision!, action.step); continue; }
        const target = Math.max(...(action.revisions ?? []).filter((r): r is number => r !== null), applied);
        await applyThrough(target, action.step);
        const name = action.send!;
        const frame = { ...action.frame! };
        // A client resumes with the windowRevision this backend sent it.
        if (action.resumeOf) frame.windowRevision = latest.get(`${name}:${action.resumeOf}`);
        records[name]!.push({ step: action.step, dir: "out", frame: action.frame! });
        // K05 splits query.subscribeMany into single subscribes (kernel/session/session.ts).
        if (frame.type === "query.subscribeMany") {
          for (const sub of (frame.subscribes as Array<Record<string, unknown>>)) {
            const one = await queries.dispatch(views[name]!, { type: "query.subscribe", ...sub } as unknown as ClientFrame);
            for (const text of one ?? []) receive(name, action.step, text);
          }
          continue;
        }
        for (const text of await queries.dispatch(views[name]!, frame as unknown as ClientFrame) ?? []) receive(name, action.step, text);
      }
      return { records, stats, hashes };
    });
    for (const name of ["bob", "carol"]) {
      const rust = byStep(g.frames[name]!);
      const kernel = byStep(result.records[name]!);
      for (const step of new Set([...rust.keys(), ...kernel.keys()])) {
        expect(kernel.get(step) ?? [], `${name}, step "${g.steps[step]}"`).toEqual(rust.get(step) ?? []);
      }
      // Byte-identical results: every result's SHA-256 equals Rust's, in order.
      const rustHashes = g.frames[name]!.filter((r) => r.dir === "in" && typeof r.frame.windowRevision === "string").map((r) => `${r.frame.id}:${r.frame.windowRevision}`);
      expect(result.hashes[name]!.sort(), `${name} windowRevision`).toEqual(rustHashes.sort());
    }
    // Cost of the replay (K09's visibility, legacy masks): four statements per execution
    // (clock, member, window, exact count) and about 117 rows read each (measured: 178
    // executions for 194 reruns, 712 statements, 20,865 rows read).
    expect(result.stats.statements).toBeLessThanOrEqual(result.stats.executions * 4);
    expect(result.stats.rowsRead / result.stats.executions).toBeLessThan(125);
  });
});
