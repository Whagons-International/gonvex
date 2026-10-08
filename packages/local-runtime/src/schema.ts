export type LocalColumn = { type: string; nullable: boolean; default?: string; references?: { table: string; column: string } };
export type LocalTableSchema = { key: string; columns: Record<string, LocalColumn> };
export type LocalSchema = Record<string, LocalTableSchema>;
