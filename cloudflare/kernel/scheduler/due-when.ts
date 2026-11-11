// A cron's `dueWhen` pre-check (RT/scheduler.rs due_check, due_value_is_truthy;
// CONTRACT E.b.2 "dueWhen"): one admitted read statement on a read-only
// invocation that never commits, `$1` bound to the current time (Rust binds
// `Utc::now()`, not the run's due time), and the run happens only when the
// first column of the first row is truthy. Errors fail open (the caller runs
// the cron).
import type { TenantStorage } from "../storage/index.ts";
import type { DueWhenEvaluator } from "./ports.ts";

/**
 * Truthiness by storage class: NULL, 0, 0.0, '' and '0' are false, a BLOB is
 * true. Rust reads the type name sqlx reports, which follows a column's
 * declared type when the first column is a plain table column (note 0034 §5).
 */
export function dueValueIsTruthy(value: SqlStorageValue | undefined): boolean {
  if (value === null || value === undefined) return false;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") return value !== "" && value !== "0";
  return true;
}

/** `dueWhen` on K01: a Query invocation (read-only, rolled back) with `$1` bound as INTEGER. */
export function storageDueWhen(storage: TenantStorage): DueWhenEvaluator {
  return {
    async evaluate(statement, nowMs) {
      const result = await storage.invoke("query", async (db) => db.run(statement.trim(), [Math.trunc(nowMs)]));
      if (!result.ok) throw result.error;
      const first = result.value.rows[0];
      return first !== undefined && dueValueIsTruthy(first[0]);
    },
  };
}
