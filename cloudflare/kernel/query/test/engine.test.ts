import { describe, expect, it } from "vitest";
import { sha256Hex } from "../../capture/digest";
import type { SocketView } from "../../session/ports";
import type { ClientFrame } from "../../session/decode";
import { TenantStorage } from "../../storage";
import { VisibilityRegistry } from "../../visibility";
import { fitParameters, JsonObject, QueryEngine, kernelVisibility, snapshotRunner, parseJson, ryu, rustDebug, stringify, type FeedStats, type LiveQueryPlan } from "../index";
import { serdeNumber } from "../json";
import { engine, module, provision, session, setRevision, validateArgs, withObject } from "./helpers";

const alice = { id: "m-alice", accountId: "a-alice" };
const bob = { id: "m-bob", accountId: "a-bob" };
const MEMBERS = [alice, bob].map((m) => ({ id: m.id, account_id: m.accountId, status: "active", role: "member", permissions: "{}" }));

const PAGE: LiveQueryPlan = {
  table: "board", key: "id", columns: ["id", "title", "position", "score", "meta", "done"],
  sort: { columnArgument: "sortColumn", directionArgument: "sortDirection", defaultColumn: "position", defaultDirection: "asc", allowedColumns: ["position", "title"] },
  window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 3, maxLimit: 50 },
};
const MANIFEST = {
  functions: {
    "board.page": { kind: "query", delivery: "live", dependencies: { liveQueryPlan: PAGE } },
    "board.secret": { kind: "query", delivery: "live", internal: true, dependencies: { liveQueryPlan: PAGE } },
    "board.unmasked": { kind: "query", delivery: "live", dependencies: { liveQueryPlan: { ...PAGE, table: "other" } } },
    "board.byIds": { kind: "query", delivery: "oneShot", args: { kind: "object", fields: { ids: { kind: "array", items: { kind: "string" } } } }, dependencies: { liveQueryPlan: { ...PAGE, where: { operator: "in", column: "id", value: { argument: "ids" } }, window: { offsetArgument: "offset", limitArgument: "limit", defaultLimit: 500, maxLimit: 500 } } } },
    "board.collection": { kind: "query", replica: { table: "board" } },
    "board.handler": { kind: "query", public: true },
    "board.add": { kind: "reducer" },
  },
  visibility: {
    board: { table: "board", key: "id", sets: {}, where: { operator: "or", children: [{ operator: "eqContext", column: "owner", context: "member.id" }, { operator: "eq", column: "shared", value: { literal: 1 } }] } },
  },
};
const SCHEMA = `CREATE TABLE board (id TEXT PRIMARY KEY, title TEXT, position INTEGER, score REAL, meta JSON, done BOOLEAN, owner TEXT, shared INTEGER);
CREATE TABLE other (id TEXT PRIMARY KEY);`;

const view = (name: string, member: { id: string; accountId: string } | null, anonymous = false): SocketView =>
  ({ connectionId: name, tenant: member ? { ...session(member), anonymous } : null }) as unknown as SocketView;
const frame = (value: Record<string, unknown>) => value as unknown as ClientFrame;

function insert(state: DurableObjectState, revision: number, row: Record<string, unknown>) {
  const columns = Object.keys(row);
  state.storage.sql.exec(`INSERT OR REPLACE INTO board (${columns.join(", ")}) VALUES (${columns.map(() => "?").join(", ")})`, ...columns.map((c) => row[c] as SqlStorageValue));
  setRevision(state, revision);
  return { kind: "transaction" as const, revision, epoch: "epoch-1", changes: [{ table: "board", rowId: String(row.id), operation: "insert", changedColumns: [], oldValue: null, newValue: row }] };
}

describe("serde_json bytes", () => {
  it("formats floats as ryu does and keeps integers", () => {
    const cases: Array<[number, string]> = [
      [0.1, "0.1"], [5, "5.0"], [-0, "-0.0"], [1e300, "1e300"], [1.5e-7, "1.5e-7"], [1e-5, "0.00001"], [1e-6, "1e-6"],
      [1e15, "1000000000000000.0"], [1e16, "1e16"], [123456789.125, "123456789.125"], [1700000000101.5, "1700000000101.5"],
      [1.2345678901234568e17, "1.2345678901234568e17"], [-3.125, "-3.125"], [Number.MAX_VALUE, "1.7976931348623157e308"], [5e-324, "5e-324"],
    ];
    for (const [value, text] of cases) expect(ryu(value), String(value)).toBe(text);
    expect(serdeNumber("12.0")).toBe("12.0");
    expect(serdeNumber("0.10000000000000001")).toBe("0.1");
    expect(serdeNumber("18446744073709551615")).toBe("18446744073709551615");
    expect(serdeNumber("18446744073709551616")).toBe("1.8446744073709552e19");
    expect(serdeNumber("-9223372036854775808")).toBe("-9223372036854775808");
    expect(serdeNumber("-0")).toBe("-0.0");
  });

  it("keeps object order (integer-like keys included), escapes as serde does", () => {
    const tree = parseJson('{"b":1,"2":[true,null,"x\\u0041\\n\\u001f\\u007f/"],"a":{"z":1.0e+2}}');
    expect(stringify(tree)).toBe('{"b":1,"2":[true,null,"xA\\n\\u001f\u007f/"],"a":{"z":100.0}}');
    expect((tree as JsonObject).entries.map(([k]) => k)).toEqual(["b", "2", "a"]);
    expect(rustDebug('a "b"\\\n')).toBe('"a \\"b\\"\\\\\\n"');
  });
});

describe("100 bound parameters", () => {
  it("packs long IN lists into json_each and keeps the rows", async () => {
    await withObject((state) => {
      const sql = state.storage.sql;
      sql.exec("CREATE TABLE t (id TEXT PRIMARY KEY, n INTEGER, x REAL, s TEXT)");
      for (let i = 0; i < 300; i++) sql.exec("INSERT INTO t VALUES (?, ?, ?, ?)", `id-${i}`, i, i / 2, i % 2 ? "1" : "one");
      const ids = Array.from({ length: 250 }, (_, i) => `id-${i * 2}`);
      const statement = `SELECT id FROM t WHERE id IN (${ids.map((_, i) => `$${i + 1}`).join(", ")}) AND n >= $${ids.length + 1} ORDER BY n`;
      const fitted = fitParameters(statement, [...ids, 10]);
      expect(fitted.packedLists).toBe(1);
      expect(fitted.parameters.length).toBe(2);
      const storage = new TenantStorage(state.storage);
      return storage.invoke("query", async (db) => {
        const rows = db.run(fitted.statement, fitted.parameters).rows.map((r) => r[0]);
        expect(rows).toEqual(ids.filter((id) => Number(id.slice(3)) >= 10));
        // Storage classes survive: an integer list compares as integers, a text "1" stays text.
        const mixed = fitParameters(`SELECT count(*) FROM t WHERE n IN (${Array.from({ length: 101 }, (_, i) => `$${i + 1}`).join(", ")})`, Array.from({ length: 101 }, (_, i) => i));
        expect(db.run(mixed.statement, mixed.parameters).rows[0]![0]).toBe(101);
        const text = fitParameters(`SELECT count(*) FROM t WHERE s IN (${Array.from({ length: 101 }, (_, i) => `$${i + 1}`).join(", ")})`, ["1", ...Array.from({ length: 100 }, () => true)]);
        expect(db.run(text.statement, text.parameters).rows[0]![0]).toBe(150);
        // Not a list: every parameter read from one JSON array.
        const scattered = fitParameters(`SELECT count(*) FROM t WHERE ${Array.from({ length: 120 }, (_, i) => `n = $${i + 1}`).join(" OR ")}`, Array.from({ length: 120 }, (_, i) => i * 2));
        expect(scattered.packedAll).toBe(true);
        expect(db.run(scattered.statement, scattered.parameters).rows[0]![0]).toBe(120);
      });
    });
  });
});

describe("live queries", () => {
  it("answers, reruns on relevant commits only, resumes and unsubscribes with Rust's frames", async () => {
    await withObject(async (state) => {
      provision(state, { revision: 3, members: MEMBERS, migration: SCHEMA });
      state.storage.sql.exec("INSERT INTO board VALUES ('b1', 'one', 10, 2.0, '{\"k\":[1,2.50]}', 1, 'm-alice', 0), ('b2', 'two', 20, 0.1, NULL, 0, 'm-bob', 1), ('b3', 'three', 30, NULL, 'not json', NULL, 'm-bob', 0)");
      const { engine: queries } = engine(state, module(MANIFEST));
      const [initial] = (await queries.dispatch(view("s1", alice), frame({ type: "query.subscribe", id: "q1", path: "board.page", args: { offset: 0, limit: 5 } })))!;
      const result = '[{"id":"b1","title":"one","position":10,"score":2.0,"meta":{"k":[1,2.5]},"done":true},{"id":"b2","title":"two","position":20,"score":0.1,"meta":null,"done":false}]';
      expect(initial).toBe(`{"type":"query.result","id":"q1","path":"board.page","reason":"initial","result":${result},"subscriptionRevision":{"epoch":"epoch-1","sequence":3},"windowRevision":"${sha256Hex(result)}"}`);
      // Resume with the same window: progress only.
      const [resumed] = (await queries.dispatch(view("s1", alice), frame({ type: "query.subscribe", id: "q2", path: "board.page", args: { offset: 0, limit: 5 }, windowRevision: sha256Hex(result) })))!;
      expect(resumed).toBe('{"type":"query.progress","id":"q2","path":"board.page","throughRevision":{"epoch":"epoch-1","sequence":3}}');
      // A write in another table is not relevant: no frame, the revision is certified.
      setRevision(state, 4);
      const stats: FeedStats = { reruns: 0, executions: 0, skipped: 0, statements: 0, rowsRead: 0 };
      expect((await queries.live.applyFeed({ kind: "transaction", revision: 4, epoch: "epoch-1", changes: [{ table: "other", rowId: "x", operation: "insert", changedColumns: [], oldValue: null, newValue: { id: "x" } }] }, stats)).size).toBe(0);
      expect(stats).toMatchObject({ reruns: 0, skipped: 2 });
      expect(queries.live.computedRevisions("s1")).toEqual([4, 4]);
      // A row bob owns and alice cannot see: rerun, unchanged, no frame.
      expect((await queries.live.applyFeed(insert(state, 5, { id: "b0", title: "hidden", position: 1, owner: "m-bob", shared: 0 }), stats)).size).toBe(0);
      // A shared row enters both windows, in subscription-id order, one execution.
      const frames = await queries.live.applyFeed(insert(state, 6, { id: "b4", title: "four", position: 5, owner: "m-bob", shared: 1 }), stats);
      expect(frames.get("s1")!.map((f) => JSON.parse(f).id)).toEqual(["q1", "q2"]);
      expect(JSON.parse(frames.get("s1")![0]!)).toMatchObject({ reason: "change", subscriptionRevision: { sequence: 6 } });
      expect(frames.get("s1")![0]!.slice(frames.get("s1")![0]!.indexOf('"path"'))).toBe(frames.get("s1")![1]!.slice(frames.get("s1")![1]!.indexOf('"path"')));
      expect(stats.executions).toBe(2); // b0 rerun, b4 rerun: one shared execution each
      // Unsubscribe: no more frames for q2.
      expect(await queries.dispatch(view("s1", alice), frame({ type: "query.unsubscribe", id: "q2" }))).toEqual([]);
      const after = await queries.live.applyFeed(insert(state, 7, { id: "b5", title: "five", position: 2, owner: "m-alice", shared: 0 }));
      expect(after.get("s1")!.map((f) => JSON.parse(f).id)).toEqual(["q1"]);
      // A feed reset reruns everything; an epoch change sends `reconnect`.
      state.storage.sql.exec("UPDATE _gonvex_sync_clock SET epoch = 'epoch-2'");
      const reset = await queries.live.applyFeed({ kind: "reset" });
      expect(JSON.parse(reset.get("s1")![0]!)).toMatchObject({ reason: "reconnect", subscriptionRevision: { epoch: "epoch-2", sequence: 7 } });
      queries.live.clear("s1");
      expect(queries.live.size).toBe(0);
    });
  });

  it("refuses with Rust's texts and keeps a subscription a failed re-subscribe names", async () => {
    await withObject(async (state) => {
      provision(state, { revision: 1, members: MEMBERS, migration: SCHEMA });
      const { engine: queries } = engine(state, module(MANIFEST));
      const sub = async (socket: SocketView, path: string, args: unknown = {}) => JSON.parse((await queries.dispatch(socket, frame({ type: "query.subscribe", id: "e", path, args })))![0]!);
      expect(await sub(view("x", null), "board.page")).toEqual({ type: "query.error", id: "e", path: "board.page", error: "authenticate with an active tenant before subscribing to a Live Query" });
      expect((await sub(view("x", alice, true), "board.page")).error).toBe("invalid structured query: authenticate with an active tenant before subscribing to a Live Query");
      expect((await sub(view("x", alice), "board.secret")).error).toBe('invalid structured query: internal query "board.secret" cannot be subscribed by a client');
      expect((await sub(view("x", alice), "board.byIds")).error).toBe('invalid structured query: live query "board.byIds" is not registered');
      expect((await sub(view("x", alice), "board.unmasked")).error).toBe('visibility plan required for table "other"');
      expect((await sub(view("x", alice), "board.page", [1])).error).toBe("structured query arguments must be an object");
      expect((await sub(view("y", { id: "m-ghost", accountId: "a-ghost" }), "board.page")).error).toBe("active tenant member is required for visibility");
      expect((await sub(view("x", alice), "board.page", { offset: 0 })).type).toBe("query.result");
      expect((await sub(view("x", alice), "board.page", [])).type).toBe("query.error");
      expect(queries.live.subscriptionIds("x")).toEqual(["e"]);
      const unloaded = new QueryEngine({ visibility: kernelVisibility(new VisibilityRegistry()), module: () => null, run: snapshotRunner(new TenantStorage(state.storage)), validateArgs });
      expect(JSON.parse((await unloaded.dispatch(view("z", alice), frame({ type: "query.subscribe", id: "e", path: "board.page", args: {} })))![0]!).error).toBe("invalid structured query: project module is not loaded");
      expect(JSON.parse((await unloaded.dispatch(view("z", alice), frame({ type: "query.call", id: "e", path: "board.byIds", args: {} })))![0]!).error).toBe('project "<project>" has no active TypeScript module');
    });
  });

  it("answers query.call through Rust's dispatch chain", async () => {
    await withObject(async (state) => {
      provision(state, { revision: 2, members: MEMBERS, migration: SCHEMA });
      state.storage.sql.exec("INSERT INTO board VALUES ('b1', 'one', 10, 2.5, NULL, 1, 'm-alice', 0), ('b2', 'two', 20, NULL, NULL, 0, 'm-bob', 0)");
      const { engine: queries } = engine(state, module(MANIFEST));
      const call = async (socket: SocketView, path: string, args: unknown = {}) => (await queries.dispatch(socket, frame({ type: "query.call", id: "c", path, args })))![0]!;
      const ids = [...Array.from({ length: 140 }, (_, i) => `nope-${i}`), "b1", "b2"];
      expect(await call(view("x", alice), "board.byIds", { ids })).toBe('{"type":"query.result","id":"c","path":"board.byIds","reason":"initial","result":[{"id":"b1","title":"one","position":10,"score":2.5,"meta":null,"done":true}]}');
      const error = async (socket: SocketView, path: string, args: unknown = {}) => JSON.parse(await call(socket, path, args)).error;
      expect(await error(view("x", null), "board.byIds")).toBe("authenticate with an active tenant before calling a Query");
      expect(await error(view("x", alice), "board.byIds", { ids: 3 })).toBe('function "board.byIds" arguments do not match its schema: $.ids: expected array, received number');
      expect(await error(view("x", alice), "board.page")).toBe('module database capability failed: query "board.page" is delivered as live and cannot be called as a one-shot Query');
      expect(await error(view("x", alice), "board.add")).toBe('function "board.add" is a reducer, not a query');
      expect(await error(view("x", alice), "board.secret")).toBe('internal function "board.secret" cannot be called by a client');
      expect(await error(view("x", alice), "board.none")).toBe('function "board.none" is not registered');
      expect(await error(view("x", alice, true), "board.byIds", { ids: [] })).toBe("module database capability failed: authenticate with an active tenant before calling a non-public query");
      expect(await error(view("x", alice), "board.collection")).toMatch(/^module database capability failed: replica collection query/);
    });
  });
});
