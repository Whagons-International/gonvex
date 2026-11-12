// 0058: bounded rows for holder recovery metadata, outside WebSocket attachments.
// This is a recovery record, never an invocation/replay queue. Only semantic
// request/correlation mutations dirty a generation; feed resume hashes stay in memory.
import { sha256Hex } from "../capture/digest";
import type { SessionRecord } from "./session";

const PREFIX = "session-recovery/";
const MANIFESTS = "session-recovery-manifest/";
const CHARS = 16_384; // <=64KiB UTF-8, including worst-case non-ASCII text.
const GROUPS = ["liveQueries", "pendingLiveQueries", "controlQueries", "replicas", "pendingActions"] as const;
type Group = typeof GROUPS[number];
export interface RecoveryReference { v: 1; generation: string; scope: string; chunks: number; hash: string }
interface Manifest extends RecoveryReference { owner: string; createdAt: number }
interface Chunk { text: string; end: boolean }
export type AttachmentRecord = Omit<SessionRecord, "connection" | "tenant" | "service"> & Partial<Pick<SessionRecord, "connection" | "tenant" | "service">> & { privateIdentity?: 1 };

export function recoveryScope(record: SessionRecord): string {
  return sha256Hex(JSON.stringify([record.connectionId, record.subscriptionEpoch ?? 0, record.connection.projectId,
    record.connection.authToken, record.tenant?.route, record.tenant?.identity.account.id, record.tenant?.member, record.service ?? null]));
}
const base = (generation: string) => `${PREFIX}${generation}/`;
const key = (generation: string, i: number) => `${base(generation)}c/${String(i).padStart(12, "0")}`;
const manifestKey = (generation: string) => `${MANIFESTS}${generation}`;

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

  write(record: SessionRecord): RecoveryReference {
    const generation = crypto.randomUUID(), scope = recoveryScope(record);
    let chunks = 0, hash = "";
    // A failed partial write cannot publish a manifest or a usable attachment reference.
    this.storage.transactionSync(() => {
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
        if (group === "liveQueries" || group === "pendingLiveQueries") {
          const { windowRevision: _hash, ...request } = value as Record<string, unknown>;
          semantic = request;
        } else if (group === "controlQueries") {
          semantic = { request: (value as { request: unknown }).request, last: "" };
        }
        append(JSON.stringify([group, id, semantic]));
      }
      this.storage.kv.put(manifestKey(generation), { v: 1, owner: record.connectionId, generation, scope, chunks, hash, createdAt: Date.now() } satisfies Manifest);
    });
    return { v: 1, generation, scope, chunks, hash };
  }

  restore(record: AttachmentRecord, ref: RecoveryReference): SessionRecord {
    const manifest = this.storage.kv.get<Manifest>(manifestKey(ref.generation));
    if (ref.v !== 1 || !manifest || manifest.v !== 1 || manifest.owner !== record.connectionId
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
    return restored;
  }

  discard(ref: RecoveryReference): void {
    this.storage.transactionSync(() => {
      for (const [name] of this.storage.kv.list({ prefix: base(ref.generation) })) this.storage.kv.delete(name);
      this.storage.kv.delete(manifestKey(ref.generation));
    });
  }

  /** On wake: abandoned prepublish generations and closed owners have no attachment. */
  sweep(active: ReadonlySet<string>): void {
    const orphaned: string[] = [];
    for (const [_name, value] of this.storage.kv.list<Manifest>({ prefix: MANIFESTS })) {
      if (active.has(value.generation)) continue;
      orphaned.push(value.generation);
    }
    for (const generation of orphaned) this.discard({ v: 1, generation, chunks: 0, scope: "", hash: "" });
  }
}
