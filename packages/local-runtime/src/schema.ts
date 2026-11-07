export{sqliteSchemaBootstrap}from'./sqlite-schema-options.js';
export type LocalColumn = { type: string; nullable: boolean; default?: string };
export type LocalForeignKey = { columns: readonly string[]; targetTable: string; targetColumns: readonly string[]; onDelete: string; onUpdate: string };
export type LocalTableSchema = { key: string; columns: Record<string, LocalColumn>; foreignKeys?: readonly LocalForeignKey[] };
export type LocalSchema = Record<string, LocalTableSchema>;
