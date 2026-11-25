// Per-tenant dual-write -> triggerless cutover controller (Phase 2d).
// Safely transitions a tenant DO from legacy SQLite `_gonvex_sync_*` JSON triggers to
// triggerless `Tx` change capture (`_gonvex_tx_changes`), with parity verification and rollback.

import { ensureTxLogSchema, readTxSeq } from "./schema";

export type TxLogMode = "triggers" | "dual" | "triggerless";

export const TX_LOG_MODE_KEY = "dod/tx-log-mode";

export interface TxLogKv {
  get<T>(key: string): T | undefined;
  put(key: string, value: unknown): void;
}

export interface ParityReport {
  readonly ok: boolean;
  readonly txLogCount: number;
  readonly syncLogCount: number;
  readonly txSeq: number;
}

/** Lists all legacy `_gonvex_sync_*` SQLite triggers currently installed on the tenant DB. */
export function listLegacySyncTriggers(sql: SqlStorage): string[] {
  const cur = sql.exec(
    "SELECT name FROM sqlite_master WHERE type = 'trigger' AND name LIKE '\\_gonvex\\_sync\\_%' ESCAPE '\\' ORDER BY name",
  );
  const out: string[] = [];
  const iter = cur.raw<[string]>();
  for (;;) {
    const next = iter.next();
    if (next.done) break;
    out.push(next.value[0]);
  }
  return out;
}

/** Drops all legacy `_gonvex_sync_*` triggers cleanly in one batched SQL statement. */
export function dropLegacySyncTriggers(sql: SqlStorage): number {
  const triggers = listLegacySyncTriggers(sql);
  if (triggers.length === 0) return 0;
  let ddl = "";
  for (let i = 0; i < triggers.length; i++) {
    const escaped = triggers[i]!.replaceAll('"', '""');
    ddl += `DROP TRIGGER IF EXISTS "${escaped}";\n`;
  }
  sql.exec(ddl);
  return triggers.length;
}

/** Per-tenant controller managing `triggers -> dual -> triggerless` progression. */
export class TxLogController {
  constructor(
    private readonly sql: SqlStorage,
    private readonly kv: TxLogKv,
  ) {}

  getMode(): TxLogMode {
    const stored = this.kv.get<string>(TX_LOG_MODE_KEY);
    if (stored === "dual" || stored === "triggerless") return stored;
    return "triggers";
  }

  /** Step 1: Install `_gonvex_tx_changes` alongside existing triggers and enter `"dual"` mode. */
  enableDualMode(): TxLogMode {
    ensureTxLogSchema(this.sql);
    this.kv.put(TX_LOG_MODE_KEY, "dual");
    return "dual";
  }

  /**
   * Step 2: Check parity between triggerless `_gonvex_tx_changes` (since `sinceTxSeq`) and legacy
   * `_gonvex_sync_changes` (since `sinceSyncRevision`) before cutting over.
   */
  verifyParity(sinceTxSeq = 0, sinceSyncRevision = 0): ParityReport {
    ensureTxLogSchema(this.sql);
    const txRow = this.sql
      .exec("SELECT count(*) FROM _gonvex_tx_changes WHERE seq > ?", sinceTxSeq)
      .raw<[number]>()
      .next().value;
    const txLogCount = Number(txRow?.[0] ?? 0);

    const hasLegacy = this.sql
      .exec("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = '_gonvex_sync_changes'")
      .raw<[number]>()
      .next();
    let syncLogCount = 0;
    if (!hasLegacy.done) {
      const syncRow = this.sql
        .exec("SELECT count(*) FROM _gonvex_sync_changes WHERE revision > ?", sinceSyncRevision)
        .raw<[number]>()
        .next().value;
      syncLogCount = Number(syncRow?.[0] ?? 0);
    }

    return {
      ok: txLogCount === syncLogCount,
      txLogCount,
      syncLogCount,
      txSeq: readTxSeq(this.sql),
    };
  }

  /** Step 3: Drop all legacy `_gonvex_sync_*` triggers and switch the tenant to `"triggerless"` mode. */
  cutoverToTriggerless(): { readonly mode: "triggerless"; readonly triggersDropped: number } {
    ensureTxLogSchema(this.sql);
    const triggersDropped = dropLegacySyncTriggers(this.sql);
    this.kv.put(TX_LOG_MODE_KEY, "triggerless");
    return { mode: "triggerless", triggersDropped };
  }

  /** Instant rollback flag back to `"triggers"` (caller re-runs `installChangeFeed` to recreate triggers). */
  rollbackToTriggers(): TxLogMode {
    this.kv.put(TX_LOG_MODE_KEY, "triggers");
    return "triggers";
  }
}
