// K03 done_when: for the same writes, the kernel's change rows and revisions
// equal the Rust runtime's.
//
// golden/rust-change-capture.json is the Rust runtime's output for the
// conformance scenario `change-capture` (cloudflare/conformance), recorded by
// `node src/capture-golden.mjs`. This test builds the same tenant schema the
// kernel way (infrastructure, the fixture migration through A01, the capture
// install), replays the same Reducer transactions through `invokeCaptured`
// with the provenance Rust recorded, and compares.
//
// The transactions run on K01's real storage layer (`TenantStorage`,
// `StorageSession`: admission, binding, savepoints, the aborted rule and the
// deferred-FK check), in the mixed write tracking K07 uses (0028 §2d, §2e),
// not on the stand-in K03 was written against before K01 merged.

import { describe, expect, it } from "vitest";
import { SchemaTransform } from "../../schema-transform/index.ts";
import { CaptureState, ChangeWake, directSql, installChangeFeed, invokeCaptured, planChangeFeedInstall, type CommitNotice, type Provenance } from "../index.ts";
import golden from "./golden/rust-change-capture.json";
import { TenantStorage, type StorageSession } from "../../storage/index";
import type { WriteOperation } from "../index.ts";
import { rows, withObject } from "./helpers.ts";
import { numbersByValue } from "./numbers.ts";

interface Step {
  sql: string;
  params?: unknown[];
  catch?: boolean;
}
interface Transaction {
  id: string;
  steps: Step[];
  fail?: boolean;
}

type Row = Record<string, SqlStorageValue>;

/** conformance/scenarios/change-capture.mjs `comparableLog`, over the kernel's rows. */
function comparableLog(log: { clock: Row; changes: Row[]; transactions: Row[] }, base: number) {
  const n = (v: SqlStorageValue | undefined) => Number(v);
  const changes = log.changes.filter((c) => c.revision !== null && n(c.revision) > base);
  const order = new Map([...changes].sort((a, b) => n(a.event_id) - n(b.event_id)).map((c, i) => [n(c.event_id), i + 1]));
  return {
    pending: log.changes.filter((c) => c.revision === null).length,
    changes: changes
      .sort((a, b) => n(a.revision) - n(b.revision) || n(a.ordinal) - n(b.ordinal))
      .map((c) => ({
        rev: n(c.revision) - base, sameTransaction: c.transaction_id === c.revision, ordinal: n(c.ordinal), event: order.get(n(c.event_id)),
        command: c.command_id, table: c.table_name, row: c.row_id, op: c.operation,
        old: c.old_value, new: c.new_value, changed: c.changed_columns,
        createdAtIsInteger: Number.isSafeInteger(c.created_at),
      })),
    transactions: log.transactions
      .filter((t) => n(t.revision) > base)
      .map((t) => ({
        rev: n(t.revision) - base, sameTransaction: t.transaction_id === t.revision, root: t.root_command_id, origin: t.origin_command_id,
        rootChannel: t.root_invocation_channel, channel: t.invocation_channel, actorAccount: t.actor_account_id, actorMember: t.actor_member_id,
        onBehalfOf: t.on_behalf_of_member_id, agentExecution: t.agent_execution_id,
      })),
    clock: { revisionsAfterBase: n(log.clock.revision) - base, retainedIsBase: n(log.clock.retained_revision) <= base },
  };
}

/** The provenance Rust recorded for a command (the invocation layer builds it; capture stores it). */
function provenanceFor(id: string): Provenance {
  const t = golden.log.transactions.find((x) => x.origin === id);
  if (!t) return { commandId: id, channel: "ui", actorAccountId: "<account:alice>", actorMemberId: "<member:alice@a>" };
  return {
    commandId: t.origin, rootCommandId: t.root, channel: t.channel, rootChannel: t.rootChannel,
    actorAccountId: t.actorAccount, actorMemberId: t.actorMember, onBehalfOfMemberId: t.onBehalfOf, agentExecutionId: t.agentExecution,
  };
}

/** Change rows whose REAL text differs between Rust and the kernel in this fixture (k03-03-reals and integral REALs). */
const REAL_TEXT_ROWS = 16;

/** A SQLite message as the kernel sees it, without the platform's suffixes. */
const core = (message: string) => message.replace(/: SQLITE_[A-Z_]+( \(extended: [A-Z_]+\))?$/, "").replace(/ at offset \d+$/, "");

/** The tenant schema the kernel way: infrastructure, the fixture migration (A01), the capture install. */
function provision(state: DurableObjectState, capture: boolean): { units: number; statements: number } {
  const sql = directSql(state.storage.sql);
  return state.storage.transactionSync(() => {
    const infrastructure = planChangeFeedInstall(sql)[0]!;
    for (const s of infrastructure.statements) sql.run(s);
    sql.run("INSERT INTO _gonvex_sync_bootstrap(name,digest) VALUES(?1,?2)", ["infrastructure", infrastructure.digest]);
    for (const s of new SchemaTransform().migration("0001_fixture.sql", golden.migration).statements) sql.run(s);
    if (!capture) return { units: 0, statements: 0 };
    const installed = installChangeFeed(sql);
    return { units: installed.units.length, statements: installed.statements };
  });
}

/** What one replayed transaction's K01 session saw: its row counters and the rows capture reported. */
interface SessionView {
  stats: { statements: number; rowsRead: number; rowsWritten: number };
  noted: Array<{ table: string; operation: WriteOperation; rowids: number[] }>;
}

interface Replayed {
  replies: Record<string, unknown>;
  notices: Array<CommitNotice | undefined>;
  sessions: SessionView[];
  captureRows: number[];
}

/** Captured entity tables with rowids: capture reports their rows, so K01 keeps them row-level (mixed mode). */
function rowReported(state: DurableObjectState): (table: string) => boolean {
  const tables = new Set(rows<{ name: string; ddl: string }>(state.storage.sql,
    `SELECT t.name AS name, t.sql AS ddl FROM sqlite_schema t WHERE t.type = 'table' AND EXISTS (SELECT 1 FROM sqlite_schema g
       WHERE g.type = 'trigger' AND g.tbl_name = t.name AND g.name = '_gonvex_sync_' || t.name || '_insert' AND g.sql LIKE '%AFTER INSERT%')`)
    .filter((t) => !/\bWITHOUT\s+ROWID\b/i.test(t.ddl)).map((t) => t.name.toLowerCase()));
  return (table) => tables.has(table);
}

async function replay(state: DurableObjectState, capture: boolean): Promise<Replayed> {
  const storage = new TenantStorage(state.storage, { writeTracking: "mixed", rowReported: rowReported(state) });
  const context = { state: new CaptureState(), wake: new ChangeWake(), reportRows: true };
  const out: Replayed = { replies: {}, notices: [], sessions: [], captureRows: [] };
  for (const t of golden.transactions as Transaction[]) {
    let session!: SessionView;
    const body = async (s: StorageSession) => {
      const noted: SessionView["noted"] = [];
      const note = s.noteRows.bind(s);
      s.noteRows = (table, operation, rowids) => { noted.push({ table, operation, rowids: [...rowids] }); note(table, operation, rowids); };
      session = { stats: s.stats, noted };
      const errors: Array<string | null> = [];
      for (const step of t.steps) {
        try {
          await s.query(step.sql, step.params ?? []);
          errors.push(null);
        } catch (error) {
          if (!step.catch) throw error;
          errors.push(core(String((error as Error).message)));
        }
      }
      if (t.fail) throw new Error("capture fixture: rolled back on purpose");
      return { errors };
    };
    if (capture) {
      const r = await invokeCaptured(storage, "reducer", provenanceFor(t.id), body, context);
      out.replies[t.id] = r.ok ? { committed: r.notice ? "revision" : "none", result: r.value } : { error: core(String((r.error as Error).message)) };
      out.notices.push(r.notice);
      out.captureRows.push(r.capture.rowsWritten);
    } else {
      const r = await storage.invoke("reducer", body);
      out.replies[t.id] = r.ok ? { result: r.value } : { error: core(String((r.error as Error).message)) };
    }
    out.sessions.push(session);
  }
  return out;
}

const readLog = (sql: SqlStorage) => ({
  clock: rows(sql, "SELECT epoch, revision, retained_revision FROM _gonvex_sync_clock")[0]!,
  changes: rows(sql, "SELECT * FROM _gonvex_sync_changes ORDER BY event_id"),
  transactions: rows(sql, "SELECT * FROM _gonvex_sync_transactions ORDER BY revision"),
});

describe("change rows equal the Rust runtime's for the same writes", () => {
  it("installs the same capture scripts as Rust (bootstrap receipts byte-equal)", async () => {
    await withObject((state) => {
      provision(state, true);
      const receipts = rows<{ name: string; digest: string }>(state.storage.sql, "SELECT name, digest FROM _gonvex_sync_bootstrap ORDER BY name");
      // Rust's file also holds receipts for its runtime tables (members, ...), which the kernel's fixture schema lacks.
      const rust = new Map(golden.bootstrap.map((r) => [r.name, r.digest]));
      for (const r of receipts) if (r.name !== "table:greetings") expect(r.digest, r.name).toBe(rust.get(r.name)); // greetings: the P01 HelloObject's own table
      expect(receipts.length).toBeGreaterThanOrEqual(13);
      // Same trigger names on the same tables as Rust; a warm plan installs nothing.
      const triggers = rows<{ table: string; name: string }>(state.storage.sql, "SELECT tbl_name AS \"table\", name FROM sqlite_schema WHERE type='trigger' AND tbl_name LIKE 'capture%' ORDER BY tbl_name, name");
      expect(triggers).toEqual(golden.triggers.map(({ table, name }) => ({ table, name })));
      expect(planChangeFeedInstall(directSql(state.storage.sql))).toEqual([]);
      expect(rows(state.storage.sql, "SELECT count(*) AS n FROM sqlite_schema WHERE type='trigger' AND sql LIKE '%gonvex_capture%' OR sql LIKE '%gonvex_record_value%'")[0]!.n).toBe(0);
    });
  });

  it("replays every transaction with the same outcome, change rows, revisions and provenance", async () => {
    await withObject(async (state) => {
      provision(state, true);
      const replayed = await replay(state, true);
      for (const t of golden.transactions as Transaction[]) {
        const rust = (golden.replies as Record<string, { committed?: string; result?: { errors: Array<string | null> }; error?: string }>)[t.id]!;
        const kernel = replayed.replies[t.id] as { committed?: string; result?: { errors: Array<string | null> }; error?: string };
        if (rust.error !== undefined) {
          // Rust wraps the database error in the module host's text (K04); the SQLite message is the same.
          expect(kernel.error, t.id).toBeDefined();
          expect(rust.error, t.id).toContain(kernel.error!);
          continue;
        }
        expect(kernel.committed, t.id).toBe(rust.committed);
        expect(kernel.result!.errors.length, t.id).toBe(rust.result!.errors.length);
        rust.result!.errors.forEach((e, i) => (e === null ? expect(kernel.result!.errors[i], t.id).toBeNull() : expect(e, t.id).toContain(kernel.result!.errors[i]!)));
      }
      const log = comparableLog(readLog(state.storage.sql), 0);
      expect(log.transactions).toEqual(golden.log.transactions);
      expect(log.clock).toEqual(golden.log.clock);
      expect(log.pending).toBe(0);
      expect(log.changes.length).toBe(golden.log.changes.length);
      // Byte-equal except REAL number text (note 0026): `numbersByValue` rewrites only JSON number tokens, on both sides.
      const byValue = <T extends { old: unknown; new: unknown }>(c: T) => ({ ...c, old: numbersByValue(c.old as string | null), new: numbersByValue(c.new as string | null) });
      let exact = 0;
      for (let i = 0; i < log.changes.length; i += 1) {
        const kernel = log.changes[i]!, rust = golden.log.changes[i]!;
        if (JSON.stringify(kernel) === JSON.stringify(rust)) exact += 1;
        expect(byValue(kernel), `change ${i + 1}`).toEqual(byValue(rust));
      }
      console.log(`K03 parity: ${log.changes.length} change rows equal to Rust's, ${exact} byte for byte, ${log.changes.length - exact} with REAL number text differing (note 0026)`);
      expect(log.changes.length - exact).toBe(REAL_TEXT_ROWS);
    });
  });

  it("reports rows and wakes delivery only for committed revisions; measures rows written per capture", async () => {
    const withCapture = await withObject(async (state) => {
      provision(state, true);
      return replay(state, true);
    });
    const without = await withObject(async (state) => {
      provision(state, false);
      return replay(state, false);
    });
    const ids = (golden.transactions as Transaction[]).map((t) => t.id);
    const table = ids.map((id, i) => {
      const changes = golden.log.changes.filter((c) => c.command === id).length;
      return {
        id,
        changes,
        rowsWrittenWithout: without.sessions[i]!.stats.rowsWritten,
        rowsWrittenWith: withCapture.sessions[i]!.stats.rowsWritten,
        finalizeRowsWritten: withCapture.captureRows[i]!,
        revision: withCapture.notices[i]?.revision ?? null,
      };
    });
    console.log("K03 rows written per transaction (with capture vs without; finalize included in 'with')");
    console.log(table.map((r) => `${r.id.padEnd(22)} changes ${String(r.changes).padStart(3)}  rowsWritten without ${String(r.rowsWrittenWithout).padStart(4)}  with ${String(r.rowsWrittenWith).padStart(4)}  finalize ${String(r.finalizeRowsWritten).padStart(4)}  revision ${r.revision ?? "-"}`).join("\n"));
    for (const [i, row] of table.entries()) {
      if (row.revision !== null) expect(row.rowsWrittenWith, row.id).toBeGreaterThan(row.rowsWrittenWithout);
      const notice = withCapture.notices[i];
      if (notice) expect(notice.changes, row.id).toBe(row.changes);
    }
    // Row reports for K01's row-level FK check: captured entity tables with a rowid.
    const inserted = withCapture.sessions[0]!.noted;
    expect(inserted).toEqual([
      { table: "capture_totals", operation: "insert", rowids: [1] },
      { table: "capture_values", operation: "insert", rowids: [1] },
    ]);
    const deleted = withCapture.sessions[ids.indexOf("k03-11-delete")]!.noted;
    expect(deleted).toEqual([
      { table: "capture_totals", operation: "update", rowids: [1] },
      { table: "capture_values", operation: "delete", rowids: [] },
    ]);
  });
});
