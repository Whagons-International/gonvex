/**
 * Per-table checksums, computed by the harness from rows an adapter returns,
 * so every backend is checksummed by the same code.
 *
 * Rows are masked with the run's aliases (member ids and the like become
 * `<member:alice>`), canonicalized (sorted keys), sorted by key and hashed:
 * sha256 over one canonical JSON row per line.
 */
import { createHash } from 'node:crypto';
import { canonical } from './normalize.mjs';

/** @returns {{ count: number, sha256: string, rows: object[] }} */
export function checksumRows(rows, { key = 'id', mask = (v) => v } = {}) {
  const masked = rows.map(r => canonical(mask(r)));
  const keyOf = (r) => String(r[key] ?? JSON.stringify(r));
  masked.sort((a, b) => (keyOf(a) < keyOf(b) ? -1 : keyOf(a) > keyOf(b) ? 1 : 0));
  const hash = createHash('sha256');
  for (const r of masked) hash.update(JSON.stringify(r) + '\n');
  return { count: masked.length, sha256: hash.digest('hex'), rows: masked.map(r => ({ __key: keyOf(r), ...r })) };
}
