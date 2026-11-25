// Workerd tests for TxLogController dual-write -> parity -> triggerless cutover & rollback.
import { env, runInDurableObject } from "cloudflare:test";
import { describe, expect, it } from "vitest";
import {
  TxLogController,
  listLegacySyncTriggers,
  runTx,
} from "../index";

describe("TxLogController dual-write -> triggerless cutover", () => {
  it("transitions triggers -> dual -> parity -> triggerless (dropping legacy triggers) -> rollback", async () => {
    await runInDurableObject(env.HELLO.getByName(`tx-bridge-${crypto.randomUUID()}`), async (_inst, state) => {
      const sql = state.storage.sql;
      sql.exec("CREATE TABLE tasks (rid INTEGER PRIMARY KEY, ver INTEGER NOT NULL, title TEXT NOT NULL)");
      sql.exec("CREATE TABLE _gonvex_sync_changes (revision INTEGER NOT NULL, table_name TEXT NOT NULL, row_id TEXT NOT NULL)");
      sql.exec(`
        CREATE TRIGGER _gonvex_sync_tasks_ins AFTER INSERT ON tasks BEGIN
          INSERT INTO _gonvex_sync_changes VALUES (1, 'tasks', CAST(NEW.rid AS TEXT));
        END
      `);
      sql.exec(`
        CREATE TRIGGER _gonvex_sync_tasks_upd AFTER UPDATE ON tasks BEGIN
          INSERT INTO _gonvex_sync_changes VALUES (2, 'tasks', CAST(NEW.rid AS TEXT));
        END
      `);

      const ctrl = new TxLogController(sql, state.storage.kv);
      expect(ctrl.getMode()).toBe("triggers");
      expect(listLegacySyncTriggers(sql)).toEqual(["_gonvex_sync_tasks_ins", "_gonvex_sync_tasks_upd"]);

      // Enter dual mode
      expect(ctrl.enableDualMode()).toBe("dual");
      expect(ctrl.getMode()).toBe("dual");

      // Write 3 rows: triggers record 3 legacy rows AND runTx records 3 compact tx-log rows.
      runTx(state.storage, (tx) => {
        for (let i = 1; i <= 3; i++) {
          tx.exec("INSERT INTO tasks VALUES (?, 1, ?)", i, `Task ${i}`);
          tx.recordUpsert(1, i, 1);
        }
      });

      const parity = ctrl.verifyParity(0, 0);
      expect(parity).toEqual({ ok: true, txLogCount: 3, syncLogCount: 3, txSeq: 1 });

      // Cutover drops every legacy `_gonvex_sync_*` trigger cleanly.
      const cut = ctrl.cutoverToTriggerless();
      expect(cut).toEqual({ mode: "triggerless", triggersDropped: 2 });
      expect(ctrl.getMode()).toBe("triggerless");
      expect(listLegacySyncTriggers(sql)).toEqual([]);

      // Subsequent writes produce 0 legacy sync rows and only compact tx-log entries.
      runTx(state.storage, (tx) => {
        tx.exec("INSERT INTO tasks VALUES (4, 1, 'After cutover')");
        tx.recordUpsert(1, 4, 1);
      });
      const legacyAfter = sql.exec<{ n: number }>("SELECT count(*) AS n FROM _gonvex_sync_changes").one().n;
      expect(legacyAfter).toBe(3);

      // Rollback flag flips cleanly back to triggers.
      expect(ctrl.rollbackToTriggers()).toBe("triggers");
      expect(ctrl.getMode()).toBe("triggers");
    });
  });
});
