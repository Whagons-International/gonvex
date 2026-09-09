import type {
  JsonObject,
  JsonValue,
  ReducerContext,
  ReducerDefinition,
} from "@gonvex/module-sdk";
import type { LocalSchema } from "./schema.js";
import { createPortableReducer } from "./portable-client.js";
export type { LocalSchema, LocalTableSchema, LocalColumn } from "./schema.js";
export { MissingReducerDataError as IncompleteReplicaError } from "./portable.js";
/** Schema emitted by code generation, never inferred from possibly empty rows. */
export type LocalColumnType =
  | "text"
  | "boolean"
  | "bigint"
  | "integer"
  | "double precision"
  | "jsonb"
  | "uuid";
export type LocalTables = Readonly<
  Record<string, Readonly<Record<string, LocalColumnType>>>
>;
export type LocalSnapshot = {
  scope: string;
  tables: Record<
    string,
    { complete: boolean; rows: JsonObject[]; columns?: readonly string[] }
  >;
};
export type LocalExecution = {
  scope: string;
  commandId: string;
  intentEntropy?: string;
  now: number;
  artifactHash: string;
  identity: Pick<ReducerContext, "auth" | "tenant" | "member">;
};
export type LocalPatch =
  | {
      entity: string;
      rowId: string;
      op: "patch" | "insert";
      fields: JsonObject;
    }
  | { entity: string; rowId: string; op: "delete" };
export type DeferredWork =
  | { id: string; kind: "action"; path: string; args: JsonValue }
  | { id: string; kind: "schedule"; path: string; args: JsonValue; at: number };
export type LocalTransactionResult = {
  result: JsonValue;
  patches: LocalPatch[];
  deferred: DeferredWork[];
  readTables: string[];
};
/** Persist these envelopes in the SDK outbox, never application state. */
export type LocalIntent = {
  path: string;
  args: JsonValue;
  execution: LocalExecution;
};
export type LocalReplay = {
  transactions: { commandId: string; transaction: LocalTransactionResult }[];
  rejected: { commandId: string; error: Error }[];
};

export type LocalRuntimeOptions = {
  schema?: LocalSchema;
  tables?: LocalTables;
  reducers: Readonly<Record<string, ReducerDefinition<any, any>>>;
  artifactHash: string;
};

/** Executes the shared TypeScript body against the normalized Local Replica. */
export class LocalReducerRuntime {
  private readonly executor;
  constructor(options: LocalRuntimeOptions) {
    const schema =
      options.schema ??
      Object.fromEntries(
        Object.entries(options.tables ?? {}).map(([table, columns]) => [
          table,
          {
            key: "_id",
            columns: Object.fromEntries(
              Object.entries(columns).map(([name, type]) => [
                name,
                { type, nullable: name !== "_id" },
              ]),
            ),
          },
        ]),
      );
    this.executor = createPortableReducer({ ...options, schema });
  }
  async initializeReady(): Promise<void> {
    await this.executor.ready;
  }
  execute(
    path: string,
    args: JsonValue,
    snapshot: LocalSnapshot,
    execution: LocalExecution,
  ): Promise<LocalTransactionResult> {
    return this.executor.execute(path, args, snapshot, execution);
  }
  replay(
    snapshot: LocalSnapshot,
    intents: readonly LocalIntent[],
  ): Promise<LocalReplay> {
    return this.executor.replay(snapshot, intents);
  }
  async close(): Promise<void> {
    this.executor.close();
  }
}
