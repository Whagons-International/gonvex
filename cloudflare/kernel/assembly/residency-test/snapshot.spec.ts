import { env } from "cloudflare:workers";
import { describe, expect, it } from "vitest";
import { decodeValue, encodeValue, PART_BYTES, type RestoreStep, type SnapshotManifest } from "../snapshot";
import type { Probe, ProbeEnv } from "./worker";

const e = env as unknown as ProbeEnv;
const probe = (name: string) => e.PLAIN.getByName(name) as unknown as Probe;

const SCHEMA = [
  "CREATE TABLE people (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL, photo BLOB, big INTEGER, score REAL, note TEXT)",
  "CREATE TABLE tags (person INTEGER NOT NULL REFERENCES people(id), tag TEXT NOT NULL, PRIMARY KEY (person, tag)) WITHOUT ROWID",
  "CREATE INDEX people_name ON people(name)",
  "CREATE VIEW named AS SELECT id, name FROM people WHERE name <> ''",
  "CREATE TABLE docs (id INTEGER PRIMARY KEY, body TEXT)",
  "CREATE VIRTUAL TABLE docs_fts USING fts5(body, content='docs', content_rowid='id')",
  "CREATE TABLE audit (id INTEGER PRIMARY KEY, what TEXT)",
  "CREATE TRIGGER people_audit AFTER INSERT ON people BEGIN INSERT INTO audit(what) VALUES ('person ' || NEW.name); END",
  "CREATE TRIGGER docs_ai AFTER INSERT ON docs BEGIN INSERT INTO docs_fts(rowid, body) VALUES (NEW.id, NEW.body); END",
];

async function seed(p: Probe, people = 50) {
  await p.script(SCHEMA);
  const statements: string[] = [];
  for (let i = 1; i <= people; i++) {
    statements.push(`INSERT INTO people(name, photo, big, score, note) VALUES ('p${i}', X'00ff${i.toString(16).padStart(2, "0")}', ${9007199254740993n + BigInt(i)}, ${i / 7}, ${i % 3 ? `'n${i}'` : "NULL"})`);
    statements.push(`INSERT INTO tags VALUES (${i}, 'a'), (${i}, 'b')`);
    statements.push(`INSERT INTO docs(body) VALUES ('document number ${i} about ${i % 2 ? "bridges" : "tunnels"}')`);
  }
  statements.push("DELETE FROM tags WHERE person = 3", "DELETE FROM people WHERE id = 3"); // a gap: AUTOINCREMENT must not reuse it after a restore
  await p.script(statements);
  await p.kvPut("gonvex/reducer/module", { projectId: "p1", manifest: { $weird: 1, nested: [1, "two", null] }, source: "abc" });
  await p.kvPut("bytes", new Uint8Array([0, 1, 254, 255]));
  await p.kvPut("map", new Map<unknown, unknown>([["a", 1], [2, new Set(["x"])]]));
  await p.kvPut("big", 12345678901234567890n);
  await p.kvPut("when", new Date(1_700_000_000_000));
  await p.kvPut("gateway:g-1", 1); // stack-local: skipped by a restore
  await p.kvPut("control/watchers/p1", [{ binding: "GATEWAY", name: "deadbeef" }]); // stack-local
}

async function restoreAll(p: Probe, prefix: string): Promise<{ steps: number; last: RestoreStep }> {
  let last: RestoreStep = { phase: "schema" };
  let steps = 0;
  while (steps++ < 200) { last = await p.restore(prefix); if (last.phase === "done" || last.phase === "failed") break; }
  return { steps, last };
}

describe("object snapshots", () => {
  it("round-trips tables, sequences, FTS, triggers and KV into a fresh object, verified by checksums", async () => {
    const source = probe("source-1");
    await seed(source);
    const manifest = await source.snapshot("t/one") as SnapshotManifest;
    expect(manifest.format).toBe("gonvex-object-snapshot");
    expect(manifest.object.name).toBe("source-1");
    expect(manifest.tables.people).toMatchObject({ rows: 49 });
    expect(manifest.tables.tags).toMatchObject({ rows: 98 });
    expect(manifest.tables.audit).toMatchObject({ rows: 50 });
    expect(Object.keys(manifest.tables)).not.toContain("docs_fts_data"); // FTS shadow tables are rebuilt, not copied
    expect(manifest.kv.entries).toBe(7);
    const stored = await e.BACKUPS.get("t/one/part-000001.json.gz");
    expect(stored).not.toBeNull();

    const target = probe("target-1");
    const { last } = await restoreAll(target, "t/one");
    expect(last.phase, last.error).toBe("done");
    expect(last.report!.mismatches).toEqual([]);
    expect(last.report!.rows).toBe(manifest.totals.rows);
    expect(last.report!.quickCheck).toBe("ok");
    expect(last.report!.fts).toEqual({ docs_fts: "ok" });
    expect(last.report!.kvSkipped.sort()).toEqual(["control/watchers/p1", "gateway:g-1"]);
    await target.finish();

    expect(await target.exec("SELECT name, hex(photo) AS photo, CAST(big AS TEXT) AS big, note FROM people WHERE id = 5")).toEqual([{ name: "p5", photo: "00FF05", big: "9007199254740998", note: "n5" }]);
    expect(await target.exec("SELECT count(*) AS n FROM named")).toEqual([{ n: 49 }]);
    expect(await target.exec("SELECT count(*) AS n FROM docs_fts WHERE docs_fts MATCH 'bridges'")).toEqual([{ n: 25 }]);
    // AUTOINCREMENT continues after the highest id ever used, and triggers fire again after the copy.
    await target.script(["INSERT INTO people(name) VALUES ('new')"]);
    expect(await target.exec("SELECT id FROM people WHERE name = 'new'")).toEqual([{ id: 51 }]);
    expect(await target.exec("SELECT what FROM audit ORDER BY id DESC LIMIT 1")).toEqual([{ what: "person new" }]);
    expect(await target.kvGet("gonvex/reducer/module")).toEqual({ projectId: "p1", manifest: { $weird: 1, nested: [1, "two", null] }, source: "abc" });
    expect(await target.kvGet("bytes")).toEqual(new Uint8Array([0, 1, 254, 255]));
    expect(await target.kvGet("map")).toEqual(new Map<unknown, unknown>([["a", 1], [2, new Set(["x"])]]));
    expect(await target.kvGet("big")).toBe(12345678901234567890n);
    expect(await target.kvGet("when")).toEqual(new Date(1_700_000_000_000));
    expect(await target.kvKeys()).not.toContain("gateway:g-1");
    expect(await target.kvKeys()).not.toContain("gonvex/snapshot-restore");
  });

  it("splits large objects into parts and restores them one part per step", async () => {
    const source = probe("source-big");
    await source.script(["CREATE TABLE blobs (id INTEGER PRIMARY KEY, data TEXT)"]);
    const chunk = "x".repeat(200_000);
    for (let i = 0; i < 60; i++) await source.exec("INSERT INTO blobs(data) VALUES (?)", `${chunk}${i}`);
    const manifest = await source.snapshot("t/big") as SnapshotManifest;
    expect(manifest.parts.length).toBeGreaterThanOrEqual(Math.floor((60 * 200_000) / PART_BYTES));
    const { last, steps } = await restoreAll(probe("target-big"), "t/big");
    expect(last.phase, last.error).toBe("done");
    expect(steps).toBeGreaterThan(manifest.parts.length);
  });

  it("refuses to restore into an object that holds data, and to overwrite a snapshot", async () => {
    const source = probe("source-2");
    await seed(source, 3);
    await source.snapshot("t/two");
    expect(await source.trySnapshot("t/two")).toMatch(/already exists/);
    const busy = probe("busy");
    await busy.script(["CREATE TABLE t (x)", "INSERT INTO t VALUES (1)"]);
    const step = await busy.restore("t/two");
    expect(step).toMatchObject({ phase: "failed", error: expect.stringMatching(/holds data/) });
    expect(await busy.exec("SELECT x FROM t")).toEqual([{ x: 1 }]);
    expect(await probe("empty").restore("t/none")).toMatchObject({ phase: "failed", error: expect.stringMatching(/no complete snapshot/) });
  });

  it("a drill restores, verifies and leaves nothing behind", async () => {
    const source = probe("source-drill");
    await seed(source, 4);
    await source.snapshot("t/drill");
    const scratch = probe("restore-drill-1");
    const { last } = await restoreAll(scratch, "t/drill");
    expect(last.phase, last.error).toBe("done");
    expect(await scratch.discard()).toEqual({ empty: true, kv: 0 });
    expect(await scratch.exec("SELECT count(*) AS n FROM sqlite_schema WHERE name NOT LIKE '\\_cf\\_%' ESCAPE '\\'")).toEqual([{ n: 0 }]);
  });

  it("detects a tampered part and never completes", async () => {
    const source = probe("source-3");
    await seed(source, 5);
    const manifest = await source.snapshot("t/three") as SnapshotManifest;
    const part = manifest.parts[0]!;
    const original = new Uint8Array(await (await e.BACKUPS.get(part.key))!.arrayBuffer());
    const tampered = original.slice(); const at = tampered.length - 9; tampered[at] = tampered[at]! ^ 0xff;
    await e.BACKUPS.put(part.key, tampered);
    const { last } = await restoreAll(probe("target-3"), "t/three");
    expect(last).toMatchObject({ phase: "failed", error: expect.stringMatching(/SHA-256/) });
    // Repaired, the same target restores from scratch (a failed restore may begin again).
    await e.BACKUPS.put(part.key, original);
    const again = await restoreAll(probe("target-3"), "t/three");
    expect(again.last.phase, again.last.error).toBe("done");
  });

  it("is consistent: a write sent during the snapshot lands after it", async () => {
    const source = probe("source-4");
    await seed(source, 20);
    const snapshot = source.snapshot("t/four");
    const write = source.script(["INSERT INTO tags VALUES (1, 'late')"]);
    const [manifest] = await Promise.all([snapshot, write]) as [SnapshotManifest, unknown];
    expect(manifest.tables.tags!.rows).toBe(38);
    expect(await source.exec("SELECT count(*) AS n FROM tags")).toEqual([{ n: 39 }]);
  });
});

describe("snapshot values", () => {
  it("encodes structured-clone values exactly and refuses anything else", () => {
    const values: unknown[] = [null, true, "s", 1.5, -0, NaN, Infinity, 10n ** 30n, undefined, [1, [2]], { a: { $b: 1 } }, new Uint8Array([1]), new Date(5), new Map([[1, 2]]), new Set([1])];
    for (const value of values) expect(decodeValue(JSON.parse(JSON.stringify(encodeValue(value))))).toEqual(value);
    expect(Object.is(decodeValue(JSON.parse(JSON.stringify(encodeValue(-0)))), -0)).toBe(true);
    class Custom {}
    expect(() => encodeValue({ x: new Custom() })).toThrow(/Custom values are not supported/);
    expect(() => encodeValue(new Float64Array(1))).toThrow(/not supported/);
  });
});
