export const REQUEST_BUDGET_MS: number;
export class StaleCursorError extends Error {}
type WireRow = unknown[];
type Checksum = { rows: number; sha256: string };
export class TenantTransfer {
  constructor(ctx: DurableObjectState, options?: { budgetMs?: number });
  fetch(request: Request): Promise<Response>;
  // The operations behind `fetch`, also called directly by in-object snapshots (kernel/assembly/snapshot.ts).
  op_status(): { databaseSize: number; tables: number; objects: number };
  op_schema(body: { statements: Array<{ name: string; sql: string }> }): Promise<{ applied: number; failed: Array<{ name: string; bytes: number; error: string }>; consumed: number }>;
  op_insert(body: { parts: Array<{ table: string; columns: string[]; rows: WireRow[] }> }): { rows: number; statements: number; rowsWritten: number };
  op_sequence(body: { sequences: Record<string, unknown> }): { sequences: number };
  op_catalog(): { schema: Array<{ type: string; name: string; tblName: string; sql: string | null; kind: string }>; plans: Array<{ name: string; copyColumns: string[] }>; sequences: Record<string, unknown> };
  op_read(body: { tables: Array<{ table: string; after?: unknown }>; maxBytes?: number }): { pages: Array<{ table: string; columns: string[]; rows: WireRow[]; next: unknown }> };
  op_checksum(body: { tables?: string[] | null; cursor?: unknown }): Promise<{ checksums: Record<string, Checksum>; next?: unknown }>;
  op_check(body: { fts?: string[]; cursor?: unknown }): Promise<{ quickCheck: string; foreignKeyViolations: number; fts: Record<string, string>; next?: unknown }>;
  op_pragma(body: { sql: string }): { value: unknown[] };
}
