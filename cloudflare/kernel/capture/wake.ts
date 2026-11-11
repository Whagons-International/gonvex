// The in-object wake-up after commit (docs/sqlite-storage.md "Change feed"
// step 3; CONTRACT E.a.2 c5: publish after the transaction returns, the
// output gate covers durability). It replaces Rust's in-process broadcast of
// `{epoch, revision, tables}` to `ChangeFeedHub`.
//
// Delivery (K10) subscribes. Like Rust's feed task, a woken listener reads
// the committed revisions after its own cursor from `_gonvex_sync_changes`,
// so commits made while it is busy coalesce into one wake carrying the newest
// revision. The listener interface is the same whether delivery runs in the
// tenant object or behind a gateway (design rule: two placements), and nothing
// here touches SQLite.

import type { CommitNotice } from "./finalize.ts";

/** One or more commits since the previous wake. */
export interface ChangeWakeup {
  readonly epoch: string;
  /** Highest committed revision. */
  readonly revision: number;
  /** Lowest revision in this wake. Delivery still reads from its own cursor. */
  readonly firstRevision: number;
  /** Union of the commits' tables, in BINARY order. */
  readonly tables: readonly string[];
  /** Change rows across the commits. */
  readonly changes: number;
  /** Commits coalesced into this wake. */
  readonly commits: number;
}

export type ChangeListener = (wake: ChangeWakeup) => void | Promise<void>;

export class ChangeWake {
  private readonly listeners = new Set<ChangeListener>();
  private pending: ChangeWakeup | undefined;
  private scheduled = false;
  private lastRevision = 0;

  constructor(private readonly onError: (error: unknown) => void = (error) => console.error("change wake listener failed", error)) {}

  /** Subscribes; returns the unsubscribe function. */
  subscribe(listener: ChangeListener): () => void {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  }

  /** The highest revision published by this object since it woke (0 before the first commit). */
  get revision(): number {
    return this.lastRevision;
  }

  /**
   * Call after the storage transaction that produced `notice` committed.
   * Listeners run in a later microtask; commits published before then
   * coalesce. A new epoch replaces what is pending.
   */
  publish(notice: CommitNotice): void {
    this.lastRevision = Math.max(this.lastRevision, notice.revision);
    const p = this.pending;
    this.pending =
      p && p.epoch === notice.epoch
        ? {
            epoch: p.epoch,
            revision: Math.max(p.revision, notice.revision),
            firstRevision: Math.min(p.firstRevision, notice.revision),
            tables: [...new Set([...p.tables, ...notice.tables])].sort((a, b) => (a < b ? -1 : a > b ? 1 : 0)),
            changes: p.changes + notice.changes,
            commits: p.commits + 1,
          }
        : { epoch: notice.epoch, revision: notice.revision, firstRevision: notice.revision, tables: [...notice.tables], changes: notice.changes, commits: 1 };
    if (this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => this.flush());
  }

  private flush(): void {
    this.scheduled = false;
    const wake = this.pending;
    this.pending = undefined;
    if (!wake) return;
    for (const listener of this.listeners) {
      try {
        const result = listener(wake);
        if (result && typeof (result as Promise<void>).catch === "function") (result as Promise<void>).catch(this.onError);
      } catch (error) {
        this.onError(error);
      }
    }
  }
}
