// 0058: bounded rows for holder recovery metadata, outside WebSocket attachments.
// This is a recovery record, never an invocation/replay queue. Only semantic
// request/correlation mutations dirty a generation; feed resume hashes stay in memory.
import { sha256Hex } from "../capture/digest";
import type { SessionRecord } from "./session";

const PREFIX = "session-recovery/";
const MANIFESTS = "session-recovery-manifest/";
const OWNERS = "session-recovery-owner/";
const ORPHANS = "session-recovery-orphan/";
const CHARS = 16_384; // <=64KiB UTF-8, including worst-case non-ASCII text.
const GROUPS = ["liveQueries", "pendingLiveQueries", "controlQueries", "replicas", "pendingActions"] as const;
type Group = typeof GROUPS[number];
export interface RecoveryReference { v: 2; socketOwner: string; generation: string; scope: string; chunks: number; hash: string }
interface Manifest extends RecoveryReference { owner: string; createdAt: number; orphanedAt?: number }
interface Owner { v: 2; connectionId: string; current?: string; pending?: string }
const ORPHAN_GRACE_MS = 60_000;
interface Chunk { text: string; end: boolean }
export type AttachmentRecord = Omit<SessionRecord, "connection" | "tenant" | "service"> & Partial<Pick<SessionRecord, "connection" | "tenant" | "service">> & { privateIdentity?: 1 };

export function recoveryScope(record: SessionRecord): string {
  return sha256Hex(JSON.stringify([record.connectionId, record.subscriptionEpoch ?? 0, record.connection.projectId,
    record.connection.authToken, record.tenant?.route, record.tenant?.identity.account.id, record.tenant?.member, record.service ?? null]));
}
const base = (generation: string) => `${PREFIX}${generation}/`;
const key = (generation: string, i: number) => `${base(generation)}c/${String(i).padStart(12, "0")}`;
const manifestKey = (generation: string) => `${MANIFESTS}${generation}`;
const ownerKey = (socketOwner: string) => `${OWNERS}${socketOwner}`;

export function withoutRecovery(record: SessionRecord): SessionRecord {
  const { liveQueries: _live, pendingLiveQueries: _pending, controlQueries: _control, replicas: _replicas, pendingActions: _actions, ...attachment } = record;
  return attachment;
}

/** 0059: legal large identities use the same private verified generation. */
export function attachmentRecord(record: SessionRecord): AttachmentRecord {
  const small = withoutRecovery(record);
  if (new TextEncoder().encode(JSON.stringify(small)).length <= 8192) return small;
  const { connection: _connection, tenant: _tenant, service: _service, ...state } = small;
  return { ...state, privateIdentity: 1 };
}

export class HolderRecoveryStore {
  constructor(private readonly storage: Pick<DurableObjectStorage, "kv" | "transactionSync">) {}

  write(record: SessionRecord, previous?: RecoveryReference, socketOwner = previous?.socketOwner ?? crypto.randomUUID()): RecoveryReference {
    const generation = crypto.randomUUID(), scope = recoveryScope(record);
    let chunks = 0, hash = "";
    // A failed partial write cannot publish a manifest or a usable attachment reference.
    this.storage.transactionSync(() => {
      const owner = this.storage.kv.get<Owner>(ownerKey(socketOwner));
      if (owner && (owner.connectionId !== record.connectionId || !previous || (owner.current !== previous.generation && owner.pending !== previous.generation))) throw new Error("holder recovery publication ownership changed");
      const append = (text: string) => {
        for (let offset = 0; offset < text.length; offset += CHARS) {
          const chunk = { text: text.slice(offset, offset + CHARS), end: offset + CHARS >= text.length };
          this.storage.kv.put(key(generation, chunks++), chunk);
          hash = sha256Hex(hash + JSON.stringify(chunk));
        }
      };
      append(JSON.stringify(["identity", "", { connection: record.connection, tenant: record.tenant, service: record.service ?? null }]));
      for (const group of GROUPS) for (const [id, value] of Object.entries(record[group] ?? {})) {
        let semantic = value;
        if (group === "pendingLiveQueries") {
          semantic = (value as Array<Record<string, unknown>>).map(({ windowRevision: _hash, ...request }) => request);
        } else if (group === "liveQueries") {
          const { windowRevision: _hash, ...request } = value as Record<string, unknown>;
          semantic = request;
        } else if (group === "controlQueries") {
          semantic = { request: (value as { request: unknown }).request, last: "" };
        }
        append(JSON.stringify([group, id, semantic]));
      }
      this.storage.kv.put(manifestKey(generation), { v: 2, socketOwner, owner: record.connectionId, generation, scope, chunks, hash, createdAt: Date.now() } satisfies Manifest);
      // Protect both the old published generation and this uncertain transition
      // BEFORE a socket attachment can reference the new one.
      this.storage.kv.put(ownerKey(socketOwner), { v: 2, connectionId: record.connectionId, ...(previous ? { current: previous.generation } : {}), pending: generation } satisfies Owner);
    });
    return { v: 2, socketOwner, generation, scope, chunks, hash };
  }

  commit(ref: RecoveryReference): void {
    const manifest = this.storage.kv.get<Manifest>(manifestKey(ref.generation));
    const owner = this.storage.kv.get<Owner>(ownerKey(ref.socketOwner));
    if (!manifest || !owner || owner.connectionId !== manifest.owner || (owner.current !== ref.generation && owner.pending !== ref.generation)) throw new Error("holder recovery publication ownership changed");
    if (owner.current === ref.generation && owner.pending === undefined) return;
    this.storage.transactionSync(() => {
      this.storage.kv.put(ownerKey(ref.socketOwner), { v: 2, connectionId: owner.connectionId, current: ref.generation } satisfies Owner);
      for (const generation of [owner.current, owner.pending]) if (generation && generation !== ref.generation) {
        const old = this.storage.kv.get<Manifest>(manifestKey(generation));
        if (old) this.storage.kv.put(`${ORPHANS}${generation}`, { ...old, orphanedAt: Date.now() });
      }
    });
    for (const generation of [owner.current, owner.pending]) if (generation && generation !== ref.generation) {
      const old = this.storage.kv.get<Manifest>(manifestKey(generation));
      if (old) { try { this.discard(old); } catch { /* The unreferenced generation is reclaimed by orphan sweep. */ } }
    }
  }

  restore(record: AttachmentRecord, ref: RecoveryReference): SessionRecord {
    const manifest = this.storage.kv.get<Manifest>(manifestKey(ref.generation));
    const owner = this.storage.kv.get<Owner>(ownerKey(ref.socketOwner));
    if (ref.v !== 2 || !manifest || manifest.v !== 2 || manifest.owner !== record.connectionId
      || manifest.socketOwner !== ref.socketOwner || !owner || owner.connectionId !== record.connectionId || (owner.current !== ref.generation && owner.pending !== ref.generation)
      || manifest.scope !== ref.scope || manifest.chunks !== ref.chunks || manifest.hash !== ref.hash) throw new Error("holder recovery metadata scope/manifest mismatch");
    const maps: Record<Group, Record<string, unknown>> = { liveQueries: Object.create(null), pendingLiveQueries: Object.create(null), controlQueries: Object.create(null), replicas: Object.create(null), pendingActions: Object.create(null) };
    let text = "", hash = "";
    let identity: Pick<SessionRecord, "connection" | "tenant" | "service"> | undefined;
    for (let i = 0; i < ref.chunks; i++) {
      const chunk = this.storage.kv.get<Chunk>(key(ref.generation, i));
      if (!chunk || typeof chunk.text !== "string" || typeof chunk.end !== "boolean" || chunk.text.length > CHARS) throw new Error("holder recovery metadata chunk missing/corrupt");
      hash = sha256Hex(hash + JSON.stringify(chunk));
      text += chunk.text;
      if (chunk.end) {
        const entry: unknown = JSON.parse(text);
        if (!Array.isArray(entry) || entry.length !== 3 || typeof entry[1] !== "string") throw new Error("holder recovery metadata entry corrupt");
        if (entry[0] === "identity" && entry[1] === "" && !identity) identity = entry[2];
        else if (GROUPS.includes(entry[0])) maps[entry[0] as Group][entry[1]] = entry[2];
        else throw new Error("holder recovery metadata entry corrupt");
        text = "";
      }
    }
    if (text || hash !== ref.hash) throw new Error("holder recovery metadata digest mismatch");
    if (!identity?.connection || !("tenant" in identity)) throw new Error("holder recovery identity missing/corrupt");
    const restored = { ...record, ...(record.privateIdentity === 1 ? identity : {}), ...maps } as SessionRecord;
    if (recoveryScope(restored) !== ref.scope) throw new Error("holder recovery metadata scope/manifest mismatch");
    // Return only after the entire generation, identity and scope validated.
    delete (restored as AttachmentRecord).privateIdentity;
    this.commit(ref); // Resolve a crash on either side of attachment publication.
    return restored;
  }

  discard(ref: RecoveryReference): void {
    const owner = this.storage.kv.get<Owner>(ownerKey(ref.socketOwner));
    if (owner?.current === ref.generation || owner?.pending === ref.generation) return;
    this.storage.transactionSync(() => this.deleteGeneration(ref));
  }

  private deleteGeneration(ref: RecoveryReference): void {
    for (const [name] of this.storage.kv.list({ prefix: base(ref.generation) })) this.storage.kv.delete(name);
    this.storage.kv.delete(manifestKey(ref.generation));
    this.storage.kv.delete(`${ORPHANS}${ref.generation}`);
  }

  /** The actual close/error callback owns this physical socket token. */
  abandon(socketOwner: string): void {
    const owner = this.storage.kv.get<Owner>(ownerKey(socketOwner));
    if (!owner) return;
    this.storage.transactionSync(() => {
      this.storage.kv.delete(ownerKey(socketOwner));
      for (const generation of [owner.current, owner.pending]) if (generation) {
        const ref = this.storage.kv.get<Manifest>(manifestKey(generation));
        if (ref) this.deleteGeneration(ref);
      }
    });
  }

  /** Reclaim only indexed generations whose publication ownership was removed.
   * Absence from getWebSockets and elapsed time never authorize owner deletion.
   * A socket whose terminal callback never arrives remains a lifecycle gap. */
  sweep(active: ReadonlySet<string>, now = Date.now()): number | null {
    const orphaned: string[] = [];
    let next: number | null = null;
    // Publication indexes only generations whose owner reference was removed.
    // Never scan every active socket/manifest on an alarm.
    const page = [...this.storage.kv.list<Manifest>({ prefix: ORPHANS, limit: 128 })];
    for (const [name, value] of page) {
      const owner = this.storage.kv.get<Owner>(ownerKey(value.socketOwner));
      if (active.has(value.generation) || owner?.current === value.generation || owner?.pending === value.generation) {
        if (value.orphanedAt !== undefined) { const { orphanedAt: _orphaned, ...live } = value; this.storage.kv.put(name, live); }
        continue;
      }
      const first = value.orphanedAt ?? now;
      if (first + ORPHAN_GRACE_MS <= now) orphaned.push(value.generation);
      else {
        if (value.orphanedAt === undefined) this.storage.kv.put(name, { ...value, orphanedAt: first });
        next = Math.min(next ?? Infinity, first + ORPHAN_GRACE_MS);
      }
    }
    for (const generation of orphaned) {
      const ref = this.storage.kv.get<Manifest>(manifestKey(generation));
      if (ref) this.discard(ref);
      else this.storage.kv.delete(`${ORPHANS}${generation}`);
    }
    if (page.length === 128) next = Math.min(next ?? Infinity, now + ORPHAN_GRACE_MS);
    return next;
  }
}
