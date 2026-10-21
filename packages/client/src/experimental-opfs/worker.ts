import {
  OPFSReplicaEngine,
  type IncomingRow,
  type ReplicaSchema,
  type RowChange,
  type MetadataChange,
} from "./replica.js";
import { opfsFile, type SyncHandle } from "./file.js";
import { assert, encode } from "./codec.js";
import type { PagePlan, PageBudget } from "./query.js";

let engine: OPFSReplicaEngine | undefined;
let snapshot: ReturnType<OPFSReplicaEngine["snapshot"]> | undefined;
let directory: FileSystemDirectoryHandle;
let name = "";
async function file(filename: string) {
  const handle = await directory.getFileHandle(filename, { create: true });
  const sync = await (
    handle as unknown as { createSyncAccessHandle(): Promise<SyncHandle> }
  ).createSyncAccessHandle();
  return opfsFile(sync);
}
const scope = globalThis as unknown as {
  postMessage(value: unknown): void;
  onmessage: ((event: MessageEvent) => void) | null;
};
let lane = Promise.resolve();
scope.onmessage = ({ data }) => {
  lane = lane.then(async () => {
    const id = data?.id;
    try {
      assert(
        Number.isSafeInteger(id) &&
          typeof data.op === "string" &&
          encode(JSON.stringify(data.args)).length <= 262144,
        "Invalid worker request",
      );
      const args = data.args;
      let result: unknown;
      if (data.op === "open") {
        assert(
          !engine &&
            typeof args.scope === "string" &&
            args.scope.length > 0 &&
            args.scope.length <= 512,
          "Invalid worker scope",
        );
        directory = await navigator.storage.getDirectory();
        const digest = new Uint8Array(
          await crypto.subtle.digest("SHA-256", encode(args.scope)),
        );
        name =
          "gonvex-opfs-experiment-" +
          Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join(
            "",
          );
        const databaseFile = await file(name + ".db");
        try {
          engine = new OPFSReplicaEngine(
            databaseFile,
            args.scope,
            args.schema as ReplicaSchema,
          );
        } catch (error) {
          databaseFile.close();
          throw error;
        }
        result = { sequence: engine.database.sequence };
      } else {
        assert(engine, "Replica not open");
        switch (data.op) {
          case "entity":
            result = engine.entity(args.table, args.id);
            break;
          case "metadata":
            result = engine.metadata(args.key);
            break;
          case "page":
            result = engine.page(
              args.plan as PagePlan,
              args.budget as PageBudget,
            );
            break;
          case "apply":
            result = engine.apply(
              args.changes as RowChange[],
              args.metadata as MetadataChange[],
            );
            break;
          case "acceptIntent":
            result = engine.acceptIntent(
              args.id,
              args.payload,
              args.changes as RowChange[],
            );
            break;
          case "beginSnapshot":
            assert(!snapshot, "Snapshot already active");
            snapshot = engine.snapshot(
              await file(name + ".scratch"),
              args.sortBytes,
            );
            break;
          case "appendSnapshot":
            assert(
              snapshot && Array.isArray(args.rows) && args.rows.length <= 512,
              "Invalid snapshot batch",
            );
            for (const row of args.rows as IncomingRow[]) snapshot.add(row);
            break;
          case "commitSnapshot":
            assert(snapshot, "No snapshot");
            {
              const active = snapshot;
              snapshot = undefined;
              result = active.commit();
            }
            break;
          case "abortSnapshot":
            snapshot?.abort();
            snapshot = undefined;
            break;
          case "verify":
            result = {
              entries: engine.database.verify(),
              sequence: engine.database.sequence,
              bytes: engine.database.bytes,
            };
            break;
          case "close":
            snapshot?.abort();
            snapshot = undefined;
            engine.close();
            engine = undefined;
            break;
          default:
            throw new Error("Unknown replica operation");
        }
      }
      scope.postMessage({ id, result });
    } catch (error) {
      scope.postMessage({
        id,
        error: error instanceof Error ? error.message : String(error),
      });
    }
  });
};
