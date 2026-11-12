// Byte values in a Durable Object's key-value storage, in chunks. The KV
// table (`_cf_KV`) is out of reach of SQL, so module SQL in the analysis
// object can never read or change workspace files or execution records.

/** Well under the per-value limit of SQLite-backed objects. */
export const CHUNK_BYTES = 512 * 1024;
/** `storage.put(entries)` / `delete(keys)` take at most 128 keys per call. */
const BATCH = 128;

const chunkKey = (prefix: string, index: number) => `${prefix}#${String(index).padStart(6, "0")}`;

export async function putBlob(storage: DurableObjectStorage, prefix: string, bytes: Uint8Array): Promise<number> {
  const chunks = Math.ceil(bytes.byteLength / CHUNK_BYTES);
  for (let start = 0; start < chunks; start += BATCH) {
    const entries: Record<string, Uint8Array> = {};
    for (let index = start; index < Math.min(chunks, start + BATCH); index++) {
      entries[chunkKey(prefix, index)] = bytes.slice(index * CHUNK_BYTES, (index + 1) * CHUNK_BYTES);
    }
    await storage.put(entries);
  }
  return chunks;
}

export async function getBlob(storage: DurableObjectStorage, prefix: string, chunks: number, size: number): Promise<Uint8Array> {
  const out = new Uint8Array(size);
  let offset = 0;
  for (let start = 0; start < chunks; start += BATCH) {
    const keys = Array.from({ length: Math.min(BATCH, chunks - start) }, (_, i) => chunkKey(prefix, start + i));
    const values = await storage.get<Uint8Array | ArrayBuffer>(keys);
    for (const key of keys) {
      const value = values.get(key);
      if (value === undefined) throw new Error(`sandbox storage is missing ${key}`);
      const bytes = value instanceof Uint8Array ? value : new Uint8Array(value);
      out.set(bytes, offset);
      offset += bytes.byteLength;
    }
  }
  if (offset !== size) throw new Error("sandbox storage returned a truncated value");
  return out;
}

export async function deleteBlob(storage: DurableObjectStorage, prefix: string, chunks: number): Promise<void> {
  for (let start = 0; start < chunks; start += BATCH) {
    await storage.delete(Array.from({ length: Math.min(BATCH, chunks - start) }, (_, i) => chunkKey(prefix, start + i)));
  }
}
