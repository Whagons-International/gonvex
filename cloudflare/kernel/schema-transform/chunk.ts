// T7: one migration is applied as several storage transactions.
//
// S1: migration 0003 as one transaction used 45 s CPU and the platform reset
// the object; transactions of at most 200 statements stayed under 2.4 s each
// (135 s for the full set). DDL cost grows with statement bytes, so a chunk is
// also capped by bytes. Atomicity across chunks is K02's restore point (0014
// T7); chunking itself never reorders statements.

import { utf8Bytes } from "./lexer.ts";

export interface ChunkOptions {
  maxStatements: number;
  maxBytes: number;
}

export const CHUNK_DEFAULTS: Readonly<ChunkOptions> = Object.freeze({ maxStatements: 200, maxBytes: 1_000_000 });

/**
 * Splits statements into chunks. `groups` lists [first, last] index ranges
 * that should share a chunk (a TEMP guard table converted by T4 from its
 * CREATE to its DROP); a range larger than one chunk starts its own chunks.
 */
export function chunkStatements(statements: readonly string[], options: Partial<ChunkOptions> = {}, groups: ReadonlyArray<readonly [number, number]> = []): string[][] {
  const { maxStatements, maxBytes } = { ...CHUNK_DEFAULTS, ...options };
  if (maxStatements < 1 || maxBytes < 1) throw new Error("chunk limits must be positive");
  const groupEnd = new Map<number, number>();
  for (const [first, last] of groups) groupEnd.set(first, Math.max(last, groupEnd.get(first) ?? last));
  const bytes = statements.map(utf8Bytes);
  const chunks: string[][] = [];
  let current: string[] = [];
  let currentBytes = 0;
  const flush = () => {
    if (current.length) chunks.push(current);
    current = [];
    currentBytes = 0;
  };
  for (let i = 0; i < statements.length; i += 1) {
    const end = groupEnd.get(i);
    if (end !== undefined && current.length) {
      // Keep the whole group together when it fits in one chunk.
      let groupBytes = 0;
      for (let k = i; k <= end; k += 1) groupBytes += bytes[k]!;
      const fitsAlone = end - i + 1 <= maxStatements && groupBytes <= maxBytes;
      const fitsHere = current.length + (end - i + 1) <= maxStatements && currentBytes + groupBytes <= maxBytes;
      if (fitsAlone && !fitsHere) flush();
    }
    if (current.length && (current.length >= maxStatements || currentBytes + bytes[i]! > maxBytes)) flush();
    current.push(statements[i]!);
    currentBytes += bytes[i]!;
  }
  flush();
  return chunks;
}
