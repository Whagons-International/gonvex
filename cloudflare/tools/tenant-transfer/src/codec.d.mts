export interface TypedRowPlan {
  name: string;
  copyColumns: string[];
  key: string[];
}
export function readTypedRows(
  storage: { exec(sql: string, ...bindings: SqlStorageValue[]): { raw(): Iterator<unknown[]> } },
  plan: TypedRowPlan,
  options?: { after?: unknown[] | null; limit?: number | null },
): Generator<unknown[], void, unknown>;
export interface HashLike { update(data: Uint8Array): unknown; digest(encoding: 'hex'): string; save?(): unknown }
export class RowHasher {
  constructor(createHash: ((algorithm: 'sha256') => HashLike) | typeof import('node:crypto').createHash);
  rows: number;
  addTyped(pairs: unknown[]): void;
  digest(): { rows: number; sha256: string };
  save(): { rows: number; hash: unknown };
  static resume(state: { rows: number; hash: unknown }, load: (hash: unknown) => HashLike): RowHasher;
}
