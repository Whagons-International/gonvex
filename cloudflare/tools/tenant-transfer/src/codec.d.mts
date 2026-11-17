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
export class RowHasher {
  constructor(createHash: typeof import('node:crypto').createHash);
  addTyped(pairs: unknown[]): void;
  digest(): { rows: number; sha256: string };
}
