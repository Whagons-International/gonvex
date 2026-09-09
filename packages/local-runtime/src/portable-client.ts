import type { LocalExecutor } from "./worker-client.js";
import {
  PortableReducerRuntime,
  type PortableReducerOptions,
} from "./portable.js";
import { memoryReadView, overlayReadView } from "./read-view.js";
import type { LocalSnapshot, LocalReplay } from "./index.js";

/** Native JS and browsers use the same executor; no SQL engine or WebView. */
export function createPortableReducer(
  options: PortableReducerOptions,
): LocalExecutor {
  const runtime = new PortableReducerRuntime(options);
  let closed = false;
  const execute: PortableReducerRuntime["execute"] = (...args) => {
    if (closed)
      return Promise.reject(new Error("Local reducer executor is closed"));
    return runtime.execute(...args);
  };
  const coverage = (snapshot: LocalSnapshot) =>
    Object.fromEntries(
      Object.entries(snapshot.tables).map(([table, data]) => [
        table,
        {
          key: options.schema[table]?.key ?? "_id",
          complete: data.complete,
          columns: data.columns,
        },
      ]),
    );
  const view = (snapshot: LocalSnapshot) =>
    memoryReadView(
      new Map(
        Object.entries(snapshot.tables).map(([table, data]) => [
          table,
          new Map(
            data.rows.map((row) => [
              String(row[options.schema[table]?.key ?? "_id"]),
              row,
            ]),
          ),
        ]),
      ),
      coverage(snapshot),
    );
  return {
    ready: Promise.resolve(),
    prepare: path => closed ? Promise.reject(new Error("Local reducer executor is closed")) : runtime.prepare(path),
    executeRead: execute,
    execute: (path, args, snapshot, execution) =>
      execute(path, args, view(snapshot), execution),
    replay: async (snapshot, intents) => {
      let source = view(snapshot);
      const transactions: LocalReplay["transactions"] = [];
      const rejected: LocalReplay["rejected"] = [];
      const ids = new Set<string>();
      for (const intent of intents) {
        if (intent.execution.scope !== snapshot.scope)
          throw new Error("Local reducer scope mismatch");
        if (intent.execution.artifactHash !== options.artifactHash)
          throw new Error("Local reducer artifact mismatch");
        if (ids.has(intent.execution.commandId))
          throw new Error("Duplicate local command id");
        ids.add(intent.execution.commandId);
      }
      for (const intent of intents) {
        try {
          const transaction = await execute(
            intent.path,
            intent.args,
            source,
            intent.execution,
          );
          transactions.push({
            commandId: intent.execution.commandId,
            transaction,
          });
          source = overlayReadView(
            source,
            coverage(snapshot),
            transaction.patches,
          );
        } catch (error) {
          if (error instanceof Error && error.name === "IncompleteReplicaError")
            throw error;
          rejected.push({
            commandId: intent.execution.commandId,
            error: error instanceof Error ? error : new Error(String(error)),
          });
        }
      }
      return { transactions, rejected };
    },
    close: () => {
      closed = true;
    },
  };
}
