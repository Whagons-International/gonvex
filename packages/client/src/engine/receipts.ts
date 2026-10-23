import { Dexie, type Table } from "dexie";
import type { OutboxStore, ReducerOutboxEntry } from "../outbox.js";

export function engineEntryOutcome(entry: ReducerOutboxEntry) {
  return Object.hasOwn(entry, "engineResult") ? { result: entry.engineResult } : entry.engineOutcome;
}

type Receipt = { key: string; path: string; args: string; result: unknown; retainedAt: number; commandAt: number };

/** Receipts are recovery metadata. The established IDB outbox remains canonical. */
export class EngineReceipts {
  private readonly journal: Dexie;
  private readonly database: Dexie & { receipts: Table<Receipt, string>; metadata: Table<{ key: string; retryFloor: number }, string> };
  constructor(name: string, private readonly limit = 1024, private readonly maxAgeMs = 30 * 24 * 60 * 60 * 1000) {
    this.journal = new Dexie(name, { cache: "disabled" });
    this.database = new Dexie(`${name}-engine-receipts`, { cache: "disabled" }) as typeof this.database;
    this.database.version(1).stores({ receipts: "&key" });
    this.database.version(2).stores({ receipts: "&key,retainedAt", metadata: "&key" }).upgrade(async transaction => {
      const now = Date.now(), receipts = transaction.table<Receipt, string>("receipts");
      const excess = await receipts.count() - this.limit;
      if (excess > 0) {
        await receipts.toCollection().limit(excess).delete();
        await transaction.table("metadata").put({ key: "retryFloor", retryFloor: now });
      }
      await receipts.toCollection().modify({ retainedAt: now, commandAt: now });
    });
  }
  async openJournal() { await this.journal.open(); }
  private key(scope: string, command: string) { return JSON.stringify([scope, command]); }
  async get(scope: string, command: string) {
    const receipt = await this.database.receipts.get(this.key(scope, command));
    return receipt && receipt.retainedAt >= Date.now() - this.maxAgeMs ? receipt : undefined;
  }
  async assertCanAdmit(commandAt: number) {
    const floor = await this.database.metadata.get("retryFloor");
    if (!Number.isSafeInteger(commandAt) || commandAt < Date.now() - this.maxAgeMs || commandAt <= (floor?.retryFloor ?? -Infinity)) {
      throw new Error("Engine command recovery receipt expired; check its server outcome before issuing a new command");
    }
  }
  private pending: Array<{ receipt: Receipt; resolve(): void; reject(error: unknown): void }> = [];
  private scheduled = false;
  private nextExpiryCheckAt = 0;
  private async flush() {
    const pending = this.pending.splice(0); this.scheduled = false;
    let checkedExpiry = false;
    try {
      await this.database.transaction("rw", [this.database.receipts, this.database.metadata], async () => {
        await this.database.receipts.bulkPut(pending.map(item => item.receipt));
        const now = Date.now(), removed: Receipt[] = [];
        const count = await this.database.receipts.count();
        if (now >= this.nextExpiryCheckAt) {
          checkedExpiry = true;
          removed.push(...await this.database.receipts.where("retainedAt").below(now - this.maxAgeMs).toArray());
          await this.database.receipts.bulkDelete(removed.map(receipt => receipt.key));
        }
        const excess = count - removed.length - this.limit;
        if (excess > 0) {
          // Free a small group while retaining the hard cap. A singleton
          // server acknowledgement should not force the same prune every tick.
          const size = Math.max(excess, Math.max(1, Math.floor(this.limit / 16)));
          const excessRows = await this.database.receipts.orderBy("retainedAt").limit(size).toArray();
          removed.push(...excessRows);
          await this.database.receipts.bulkDelete(excessRows.map(receipt => receipt.key));
        }
        if (removed.length) {
          const prior = await this.database.metadata.get("retryFloor");
          await this.database.metadata.put({ key: "retryFloor", retryFloor: Math.max(prior?.retryFloor ?? 0, ...removed.map(receipt => receipt.commandAt)) });
        }
      });
      if (checkedExpiry) this.nextExpiryCheckAt = Date.now() + Math.min(this.maxAgeMs, 24 * 60 * 60 * 1000);
      for (const item of pending) item.resolve();
    } catch (error) { for (const item of pending) item.reject(error); }
  }
  async put(scope: string, command: string, path: string, args: unknown, result: unknown, commandAt = Date.now()) {
    await new Promise<void>((resolve, reject) => {
      this.pending.push({ receipt: { key: this.key(scope, command), path, args: JSON.stringify(args), result, retainedAt: Date.now(), commandAt }, resolve, reject });
      if (!this.scheduled) { this.scheduled = true; queueMicrotask(() => { void this.flush(); }); }
    });
  }
  cachedEntry?: (id: number) => ReducerOutboxEntry | undefined;
  readonly admissions = new Map<string, { result: unknown; commandNow?: number }>();
  wrap(store: OutboxStore): OutboxStore {
    const admitted = <T extends Omit<ReducerOutboxEntry, "id">>(entry: T): T => {
      const outcome = this.admissions.get(entry.idempotencyKey);
      if (!outcome) return entry;
      const { engineOutcome: _legacy, ...value } = entry;
      return { ...value, engineResult: structuredClone(outcome.result), ...(outcome.commandNow === undefined ? {} : { engineCommandNow: outcome.commandNow }) } as T;
    };
    const remember = async (ids: readonly number[]) => {
      const selected = new Set(ids);
      // Read only recovery metadata here. Deletion still goes through the
      // contract-fenced canonical store after the receipt transaction commits.
      const cached = ids.map(id => this.cachedEntry?.(id)).map(entry =>
        entry?.localExecution && engineEntryOutcome(entry) ? entry : undefined);
      const missing = ids.filter((_id, index) => !cached[index]);
      const loaded: Array<ReducerOutboxEntry | undefined> = !missing.length ? [] : store.getMany ? await store.getMany(missing)
        : this.journal.isOpen() ? await this.journal.table("entries").bulkGet(missing) : await store.load();
      const missingIds = new Set(missing);
      const entries = [...cached, ...loaded.filter(entry => entry && missingIds.has(entry.id))];
      await Promise.all(entries.map(entry => {
        if (!entry || !selected.has(entry.id)) return;
        // Local results are durable at admission. A remote entry's queued
        // placeholder is provisional until the server commits it. Successful
        // queued replies are persisted by the host, not by error cleanup.
        if (!entry.localExecution && entry.state !== "committed") return;
        const outcome = engineEntryOutcome(entry);
        if (outcome) return this.put(entry.scope, entry.idempotencyKey, entry.path, entry.args, outcome.result,
          entry.engineCommandNow ?? entry.engineOutcome?.commandNow ?? entry.localExecution?.now ?? entry.createdAt);
      }));
    };
    return { ...store,
      ...(store.append ? { append: async (entry: Omit<ReducerOutboxEntry, "id">) => store.append!(admitted(entry)) } : {}),
      put: entry => store.put(admitted(entry)),
      ...(store.update ? { update: (id: number, change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => store.update!(id, entry => admitted(change(entry))) } : {}),
      ...(store.updateMany ? { updateMany: (ids: number[], change: (entry: ReducerOutboxEntry) => ReducerOutboxEntry) => store.updateMany!(ids, entry => admitted(change(entry))) } : {}),
      ...(store.replacePending ? { replacePending: async (id: number, draft: Omit<ReducerOutboxEntry, "id">) => {
        await remember([id]); return store.replacePending!(id, admitted(draft));
      } } : {}),
      // Persist the receipt first. A crash can leave both records, never neither.
      delete: async id => { await remember([id]); await store.delete(id); },
      deleteMany: async ids => { await remember(ids); if (store.deleteMany) await store.deleteMany(ids); else for (const id of ids) await store.delete(id); },
    };
  }
  async findReceipt(scope: string, command: string, path: string, args: unknown) {
    const receipt = await this.get(scope, command);
    if (receipt && (receipt.path !== path || receipt.args !== JSON.stringify(args))) throw new Error("Engine command ID reused with different arguments");
    return receipt;
  }
  close() { this.cachedEntry = undefined; this.database.close(); this.journal.close(); }
}
