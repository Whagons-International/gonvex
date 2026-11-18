export function readPlans(sql: { exec(sql: string, ...bindings: SqlStorageValue[]): { toArray(): unknown[] } }): Array<{ name: string; withoutRowid: boolean; key: string[]; copyColumns: string[] }>;
