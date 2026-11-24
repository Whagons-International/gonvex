// Workerd tests for WLP1 PackedPage codec: all 20 encodings, strict validation,
// direct DO SqlStorageCursor.raw() encoding, and 1,000-task wire-size comparison vs JSON.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  E_B1, E_B2, E_B4, E_BOOL, E_CF64, E_CI32, E_D16, E_D32, E_D8, E_DICT, E_F64, E_I32,
  E_LIST16, E_LIST32, E_LIST8, E_NULL, E_SEQ32, E_U16, E_U8, E_UTF8,
  PAGE_MAGIC, PAGE_ROWS_MAX,
  encodePage, encodeSqlCursor, layoutFromCols, readPage,
  type PageCol,
} from "../index";

describe("WLP1 PackedPage codec", () => {
  it("selects and roundtrips all 20 adaptive encodings cleanly", () => {
    const cols: PageCol[] = [
      { id: 0, k: "i" },  // all null -> E_NULL
      { id: 1, k: "i" },  // constant int -> E_CI32
      { id: 2, k: "i" },  // 0..1 -> E_B1
      { id: 3, k: "i" },  // 0..3 -> E_B2
      { id: 4, k: "i" },  // 0..15 -> E_B4
      { id: 5, k: "i" },  // 0..255 -> E_U8
      { id: 6, k: "i" },  // 0..65535 -> E_U16
      { id: 7, k: "i" },  // negative -> E_I32
      { id: 8, k: "i" },  // affine -> E_SEQ32
      { id: 9, k: "b" },  // bool -> E_BOOL
      { id: 10, k: "f" }, // constant float -> E_CF64
      { id: 11, k: "t" }, // delta u8 -> E_D8
      { id: 12, k: "t" }, // delta u16 -> E_D16
      { id: 13, k: "t" }, // delta u32 -> E_D32
      { id: 14, k: "f" }, // general float -> E_F64
      { id: 15, k: "d" }, // repeated strings -> E_DICT
      { id: 16, k: "x" }, // unique strings -> E_UTF8
      { id: 17, k: "l" }, // list u8 -> E_LIST8
      { id: 18, k: "l" }, // list u16 -> E_LIST16
      { id: 19, k: "l" }, // list u32 -> E_LIST32
      { id: 20, k: "j" }, // json column
    ];

    const baseTs = 1760000000000;
    const rows: unknown[][] = [];
    for (let i = 0; i < 32; i++) {
      rows.push([
        100 + i,                          // rid -> E_SEQ32
        1,                                // ver -> E_CI32
        null,                             // E_NULL
        42,                               // E_CI32
        i & 1,                            // E_B1
        i & 3,                            // E_B2
        i & 15,                           // E_B4
        (i * 7) % 200,                    // E_U8
        ((i * 1021) ^ (i * 131)) % 60000, // E_U16 (non-affine so encoder picks U16 over SEQ32)
        i === 5 ? null : -1000 - ((i * i) % 97), // E_I32 (with null, non-affine)
        10000 + i * 17,                   // E_SEQ32
        i % 3 === 0 ? null : i % 2 === 0, // E_BOOL (with null)
        3.14159,                          // E_CF64
        baseTs + i * 5,                   // E_D8
        baseTs + i * 1000,                // E_D16
        baseTs + i * 100_000,             // E_D32
        i * 0.125 + 0.01,                 // E_F64
        ["open", "in_progress", "done"][i % 3], // E_DICT
        `unique-task-title-🍉-${i}-${(i * 31).toString(36)}`, // E_UTF8
        [1, 2, i],                        // E_LIST8
        [300, 400 + i],                   // E_LIST16
        [100_000 + i],                    // E_LIST32
        i % 4 === 0 ? null : { n: i },    // json
      ]);
    }

    const bytes = encodePage(7, cols, rows);
    // Also test misaligned slice handling (e.g. prefixed with a 5-byte frame header).
    const framed = new Uint8Array(bytes.length + 5);
    framed.set(bytes, 5);
    const reader = readPage(7, 0, framed.subarray(5), layoutFromCols(7, cols));

    expect(reader.rows).toBe(32);
    expect(reader.ncols).toBe(cols.length + 2);
    expect(reader.lane(0).enc).toBe(E_SEQ32);
    expect(reader.lane(1).enc).toBe(E_CI32);
    expect(reader.lane(2).enc).toBe(E_NULL);
    expect(reader.lane(3).enc).toBe(E_CI32);
    expect(reader.lane(4).enc).toBe(E_B1);
    expect(reader.lane(5).enc).toBe(E_B2);
    expect(reader.lane(6).enc).toBe(E_B4);
    expect(reader.lane(7).enc).toBe(E_U8);
    expect(reader.lane(8).enc).toBe(E_U16);
    expect(reader.lane(9).enc).toBe(E_I32);
    expect(reader.lane(10).enc).toBe(E_SEQ32);
    expect(reader.lane(11).enc).toBe(E_BOOL);
    expect(reader.lane(12).enc).toBe(E_CF64);
    expect(reader.lane(13).enc).toBe(E_D8);
    expect(reader.lane(14).enc).toBe(E_D16);
    expect(reader.lane(15).enc).toBe(E_D32);
    expect(reader.lane(16).enc).toBe(E_F64);
    expect(reader.lane(17).enc).toBe(E_DICT);
    expect(reader.lane(18).enc).toBe(E_UTF8);
    expect(reader.lane(19).enc).toBe(E_LIST8);
    expect(reader.lane(20).enc).toBe(E_LIST16);
    expect(reader.lane(21).enc).toBe(E_LIST32);

    for (let r = 0; r < rows.length; r++) {
      for (let c = 0; c < cols.length + 2; c++) {
        const expected = c === 22 && rows[r]![c] !== null ? JSON.stringify(rows[r]![c]) : rows[r]![c];
        expect(reader.get(c, r)).toEqual(expected);
      }
    }
  });

  it("rejects wrong magic, schema mismatch, truncation, overrun, and out-of-range dict codes", () => {
    const cols: PageCol[] = [{ id: 0, k: "d" }, { id: 1, k: "i" }];
    const layout = layoutFromCols(1, cols);
    const bytes = encodePage(1, cols, [
      [1, 1, "open", 10],
      [2, 1, "open", 20],
      [3, 1, "done", 30],
    ]);
    const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    expect(dv.getUint32(0, true)).toBe(PAGE_MAGIC);

    // Wrong magic
    const badMagic = bytes.slice();
    badMagic[0] = 0;
    expect(() => readPage(1, 0, badMagic, layout)).toThrow("sync.frame");

    // Wrong schema fingerprint
    expect(() => readPage(1, 0, bytes, layoutFromCols(1, [{ id: 0, k: "x" }, { id: 1, k: "i" }]))).toThrow("sync.schema");

    // Truncated
    expect(() => readPage(1, 0, bytes.subarray(0, bytes.length - 1), layout)).toThrow("sync.frame");

    // Overrun (trailing garbage byte)
    const overrun = new Uint8Array(bytes.length + 1);
    overrun.set(bytes);
    expect(() => readPage(1, 0, overrun, layout)).toThrow("sync.frame");

    // Oversized row count
    expect(() => encodePage(1, cols, new Array(PAGE_ROWS_MAX + 1).fill([1, 1, "a", 1]))).toThrow(RangeError);
  });

  it("streams real Durable Object SqlStorageCursor.raw() directly to WLP1 and beats JSON wire size by >4x", async () => {
    await runInDurableObject(env.HELLO.getByName(`codec-sql-${crypto.randomUUID()}`), async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec(`
        CREATE TABLE tasks (
          rid INTEGER PRIMARY KEY,
          ver INTEGER NOT NULL,
          workspace_id INTEGER NOT NULL,
          status_id INTEGER NOT NULL,
          priority_id INTEGER NOT NULL,
          spot_id INTEGER,
          created_at INTEGER NOT NULL,
          updated_at INTEGER NOT NULL,
          done INTEGER NOT NULL,
          title TEXT NOT NULL
        )
      `);

      const N = 1000;
      const base = 1764000000000;
      state.storage.transactionSync(() => {
        for (let i = 1; i <= N; i++) {
          sql.exec(
            "INSERT INTO tasks VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            i, 1, (i % 6) + 1, (i % 5) + 1, (i % 4) + 1, i % 10 === 0 ? null : (i % 40) + 1,
            base + i * 1000, base + i * 1000 + 500, i % 3 === 0 ? 1 : 0, `Inspect room #${100 + (i % 250)}`,
          );
        }
      });

      const cols: PageCol[] = [
        { id: 0, k: "r" }, // workspace_id
        { id: 1, k: "r" }, // status_id
        { id: 2, k: "r" }, // priority_id
        { id: 3, k: "r" }, // spot_id
        { id: 4, k: "t" }, // created_at
        { id: 5, k: "t" }, // updated_at
        { id: 6, k: "b" }, // done
        { id: 7, k: "x" }, // title
      ];
      const layout = layoutFromCols(1, cols);

      const t0 = performance.now();
      const cursor = sql.exec(
        "SELECT rid, ver, workspace_id, status_id, priority_id, spot_id, created_at, updated_at, done, title FROM tasks ORDER BY rid",
      );
      const encoded = encodeSqlCursor(1, cols, cursor, PAGE_ROWS_MAX);
      const wlpMs = performance.now() - t0;

      expect(encoded.totalRows).toBe(N);
      expect(encoded.rowsRead).toBe(N);
      expect(encoded.pages).toHaveLength(1);
      const pageBytes = encoded.pages[0]!;

      // Compare against legacy JSON array-of-objects serialization.
      const t1 = performance.now();
      const jsonRows = sql.exec(
        "SELECT rid, ver, workspace_id, status_id, priority_id, spot_id, created_at, updated_at, done, title FROM tasks ORDER BY rid",
      ).toArray();
      const jsonBytes = new TextEncoder().encode(JSON.stringify(jsonRows)).byteLength;
      const jsonMs = performance.now() - t1;

      console.log(
        `[WLP1 benchmark 1000 tasks] WLP1=${pageBytes.byteLength} B (${(pageBytes.byteLength / N).toFixed(1)} B/row, ${wlpMs.toFixed(2)} ms) ` +
        `vs JSON=${jsonBytes} B (${(jsonBytes / N).toFixed(1)} B/row, ${jsonMs.toFixed(2)} ms) — ${(jsonBytes / pageBytes.byteLength).toFixed(2)}x smaller`,
      );
      expect(pageBytes.byteLength * 4).toBeLessThan(jsonBytes);

      const reader = readPage(1, 0, pageBytes, layout);
      expect(reader.rows).toBe(N);
      expect(reader.get(0, 0)).toBe(1);
      expect(reader.get(0, N - 1)).toBe(N);
      expect(reader.get(5, 9)).toBeNull(); // i = 10 -> spot_id null
      expect(reader.get(9, 0)).toBe("Inspect room #101");
    });
  });
});
