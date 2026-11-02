import type { ReducerOutboxEntry } from "./outbox.js";

/** Durable intent history needs explicit recovery rather than memory fallback. */
export class OutboxRecoveryError extends Error {
  readonly code = "outbox_recovery_required";
  constructor(message: string, options?: ErrorOptions) {
    super(message, options);
    this.name = "OutboxRecoveryError";
  }
}

/** IndexedDB can wrap an upgrade failure in its own error/cause chain. */
export function requiresOutboxRecovery(error: unknown): boolean {
  const pending: unknown[] = [error];
  const seen = new Set<object>();
  while (pending.length) {
    const value = pending.pop();
    if (!value || typeof value !== "object" || seen.has(value)) continue;
    seen.add(value);
    const record = value as { name?: string; cause?: unknown; inner?: unknown };
    if (record.name === "OutboxRecoveryError") return true;
    pending.push(record.cause, record.inner);
  }
  return false;
}

/** Validate persisted identity and delivery state before admitting any replay. */
export function validateOutboxEntry(value: unknown, key?: string): asserts value is ReducerOutboxEntry {
  const fail = () => { throw new OutboxRecoveryError("Invalid durable outbox intent; journal recovery is required"); };
  if (!value || typeof value !== "object" || Array.isArray(value)) return fail();
  const entry = value as Record<string, unknown>;
  const text = (field: string) => typeof entry[field] === "string" && entry[field].length > 0;
  const finite = (field: string) => typeof entry[field] === "number" && Number.isFinite(entry[field]);
  const natural = (field: string) => typeof entry[field] === "number" && Number.isSafeInteger(entry[field]) && entry[field] >= 0;
  if (!natural("id") || entry.id === 0 || (key !== undefined && key !== String(entry.id))
    || !text("scope") || !text("path") || !text("idempotencyKey")
    || !Object.prototype.hasOwnProperty.call(entry, "args") || entry.args === undefined
    || !finite("createdAt") || !natural("attempts") || !finite("nextAttemptAt")
    || typeof entry.state !== "string" || !["pending", "inflight", "committed", "failed", "rejected"].includes(entry.state)) return fail();
  if (entry.entityKeys !== undefined && (!Array.isArray(entry.entityKeys) || entry.entityKeys.some(key => typeof key !== "string"))) return fail();
  if (entry.patches !== undefined && !Array.isArray(entry.patches)) return fail();
  if (entry.awaitingReplica !== undefined && (entry.awaitingReplica !== true || entry.state !== "committed")) return fail();
}
