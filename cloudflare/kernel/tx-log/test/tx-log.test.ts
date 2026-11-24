// Real workerd DO SQLite tests for triggerless Tx + hoisted O(tables) WLP1 fan-out.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import { layoutFromCols, readPage, type PageCol } from "../../codec/index";
import {
  TX_OP_DELETE,
  TX_OP_UPSERT,
  buildBatchFanout,
  ensureTxLogSchema,
  pruneTxLog,
  readChangesSince,
  readTxSeq,
  runTx,
  type FanoutMember,
  type FanoutTableSpec,
} from "../index";

const TASK_COLS: PageCol[] = [
  { id: 0, k: "r" }, // workspace_id
  { id: 1, k: "i" }, // status_id
  { id: 2, k: "x" }, // title
];

describe("triggerless Tx change log and hoisted O(tables) WLP1 fan-out", () => {
  it("records mutations without SQLite triggers, deduplicates per-commit, and prunes cleanly", async () => {
    await runInDurableObject(env.HELLO.getByName(`tx-log-${crypto.randomUUID()}`), async (_inst, state) => {
      ensureTxLogSchema(state.storage.sql);
      state.storage.sql.exec(`
        CREATE TABLE tasks (
          rid INTEGER PRIMARY KEY,
          ver INTEGER NOT NULL,
          workspace_id INTEGER NOT NULL,
          status_id INTEGER NOT NULL,
          title TEXT NOT NULL
        )
      `);

      // Verify zero triggers were installed.
      const triggerCount = state.storage.sql
        .exec<{ n: number }>("SELECT count(*) AS n FROM sqlite_master WHERE type = 'trigger'")
        .one().n;
      expect(triggerCount).toBe(0);

      // No-op transaction does not advance seq.
      const noop = runTx(state.storage, () => "empty");
      expect(noop.seq).toBe(0);
      expect(noop.changes).toHaveLength(0);

      // Batch insert 25 rows (>20 changes -> exercises multi-chunk <=100-bind flush) and overwrite rid 1 twice.
      const commit1 = runTx(state.storage, (tx) => {
        for (let i = 1; i <= 25; i++) {
          tx.exec("INSERT INTO tasks VALUES (?, ?, ?, ?, ?)", i, 1, (i % 3) + 1, 1, `Task ${i}`);
          tx.recordUpsert(1, i, (i % 3) + 1);
        }
        // Update rid 1 within same tx -> deduplicated into one change record.
        tx.exec("UPDATE tasks SET ver = 2, title = 'Task 1 updated' WHERE rid = 1");
        tx.recordUpsert(1, 1, 2);
        return 25;
      });
      expect(commit1.seq).toBe(1);
      expect(commit1.changes).toHaveLength(25);
      // 25 task inserts + 1 task update + 1 seq update + 25 change-log rows * 2 (WITHOUT ROWID table + tbl_seq index) = 77.
      expect(commit1.rowsWritten).toBe(77);
      expect(readTxSeq(state.storage.sql)).toBe(1);

      const commit2 = runTx(state.storage, (tx) => {
        tx.exec("DELETE FROM tasks WHERE rid = 2");
        tx.recordDelete(1, 2, 0);
      });
      expect(commit2.seq).toBe(2);

      const logRows = readChangesSince(state.storage.sql, 0, 2);
      expect(logRows).toHaveLength(26);
      expect(logRows[0]).toMatchObject({ seq: 1, tbl: 1, rid: 1, op: TX_OP_UPSERT, scopeHint: 2 });
      expect(logRows[25]).toMatchObject({ seq: 2, tbl: 1, rid: 2, op: TX_OP_DELETE });

      expect(pruneTxLog(state.storage.sql, 1)).toBe(25);
      expect(readChangesSince(state.storage.sql, 0, 2)).toHaveLength(1);
    });
  });

  it("keeps fan-out SQL statements and rowsRead completely flat from 1 member to 50 members and shares WLP1 bytes per class", async () => {
    await runInDurableObject(env.HELLO.getByName(`tx-fanout-${crypto.randomUUID()}`), async (_inst, state) => {
      ensureTxLogSchema(state.storage.sql);
      state.storage.sql.exec(`
        CREATE TABLE tasks (
          rid INTEGER PRIMARY KEY,
          ver INTEGER NOT NULL,
          workspace_id INTEGER NOT NULL,
          status_id INTEGER NOT NULL,
          title TEXT NOT NULL
        )
      `);

      const commit = runTx(state.storage, (tx) => {
        for (let i = 1; i <= 50; i++) {
          const ws = (i % 2) + 1; // workspace 1 or 2
          tx.exec("INSERT INTO tasks VALUES (?, ?, ?, ?, ?)", i, 1, ws, 1, `Room ${i}`);
          tx.recordUpsert(1, i, ws);
        }
        tx.recordDelete(1, 999, 1);
      });

      const specs = new Map<number, FanoutTableSpec>([
        [
          1,
          {
            tbl: 1,
            tableName: "tasks",
            selectColumnsSql: "rid, ver, workspace_id, status_id, title",
            cols: TASK_COLS,
            // classKey bitmask: bit 1 = sees workspace 1, bit 2 = sees workspace 2
            visibleToClass: (classKey, row) => (classKey & (1 << Number(row[2]))) !== 0,
          },
        ],
      ]);

      // Fan-out to 1 member (class 1 = workspace 1 only)
      const oneMember: FanoutMember[] = [{ memberId: "m0", classKey: 1 << 1 }];
      const res1 = buildBatchFanout(state.storage.sql, specs, commit.changes, oneMember);

      // Fan-out to 50 members split across 3 visibility classes (ws1 only, ws2 only, both ws1+ws2)
      const fiftyMembers: FanoutMember[] = [];
      const classMasks = [1 << 1, 1 << 2, (1 << 1) | (1 << 2)];
      for (let i = 0; i < 50; i++) {
        fiftyMembers.push({ memberId: `m${i}`, classKey: classMasks[i % 3]! });
      }
      const res50 = buildBatchFanout(state.storage.sql, specs, commit.changes, fiftyMembers);

      // O(tables) SQL invariant: identical SQL statements (1) and rowsRead (100: 50 IN-list + 50 PK lookups) for 1 vs 50 members!
      expect(res1.stats.sqlStatements).toBe(1);
      expect(res50.stats.sqlStatements).toBe(1);
      expect(res1.stats.rowsRead).toBe(100);
      expect(res50.stats.rowsRead).toBe(res1.stats.rowsRead);
      // Encoded once per visibility class (3 classes), NOT 50 times!
      expect(res50.stats.classesEncoded).toBe(3);
      expect(res50.stats.pagesEncoded).toBe(3);

      // Members in the same visibility class share the exact same Uint8Array instance (zero copy).
      expect(res50.byMember.get("m0")![0]!.upsertPages[0]).toBe(res50.byMember.get("m3")![0]!.upsertPages[0]);

      // Decode WLP1 page for workspace-1-only class and verify exact row filtering.
      const layout = layoutFromCols(1, TASK_COLS);
      const ws1Page = readPage(1, 0, res50.byMember.get("m0")![0]!.upsertPages[0]!, layout);
      expect(ws1Page.rows).toBe(25);
      for (let r = 0; r < ws1Page.rows; r++) expect(ws1Page.get(2, r)).toBe(1);

      const bothPage = readPage(1, 0, res50.byMember.get("m2")![0]!.upsertPages[0]!, layout);
      expect(bothPage.rows).toBe(50);
      expect(Array.from(res50.byMember.get("m0")![0]!.deletedRids)).toEqual([999]);
    });
  });
});
