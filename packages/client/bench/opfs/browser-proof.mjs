import { openOPFSReplicaWorker } from "./build/index.js";
import { schema, records } from "./fixture.mjs";

/** Abrupt owner termination, not graceful close, using the public SDK API. */
export async function proveWorkerRecovery() {
  const scope = "worker-crash-proof-" + crypto.randomUUID();
  const workers = [];
  let client;
  const open = async () => {
    for (let attempt = 0; attempt < 5; attempt++) {
      const worker = new Worker("./build/worker.js", { type: "module" });
      workers.push(worker);
      try {
        return await openOPFSReplicaWorker({
          scope,
          schema,
          worker,
          deadlineMs: 5000,
        });
      } catch (error) {
        worker.terminate();
        if (
          !/lock|access handle|modification/i.test(String(error)) ||
          attempt === 4
        )
          throw error;
        await new Promise((resolve) => setTimeout(resolve, 50));
      }
    }
  };
  try {
    client = await open();
    await client.apply([
      {
        table: "tasks",
        id: "old",
        row: { _id: "old", name: "committed old row" },
      },
    ]);
    const before = await client.verify();
    let concurrentRefused = false;
    try {
      const second = await openOPFSReplicaWorker({
        scope,
        schema,
        deadlineMs: 5000,
      });
      await second.close();
    } catch {
      concurrentRefused = true;
    }
    if (!concurrentRefused) throw Error("Concurrent file owner was admitted");
    await client.beginSnapshot();
    await client.appendSnapshot([...records(100)]);
    workers.at(-1).terminate();
    client = undefined;
    client = await open();
    const old = await client.entity("tasks", "old"),
      partial = await client.entity("tasks", "0"),
      after = await client.verify();
    if (
      old?.name !== "committed old row" ||
      partial !== undefined ||
      before.sequence !== after.sequence
    )
      throw Error("Uncommitted snapshot escaped after owner crash");
    const changes = [
      {
        table: "tasks",
        id: "intent-row",
        row: { _id: "intent-row", name: "durable intent" },
      },
    ];
    const receipt = await client.acceptIntent(
      "immutable-1",
      { path: "tasks.create", id: "intent-row" },
      changes,
    );
    workers.at(-1).terminate();
    client = undefined;
    client = await open();
    const retry = await client.acceptIntent(
      "immutable-1",
      { id: "intent-row", path: "tasks.create" },
      changes,
    );
    if (
      !retry.duplicate ||
      retry.sequence !== receipt.sequence ||
      (await client.entity("tasks", "intent-row"))?.name !== "durable intent"
    )
      throw Error("Acknowledged intent did not survive owner crash");
    return {
      concurrentRefused,
      uncommittedSnapshotHidden: true,
      acknowledgedIntentRecovered: true,
      idempotentRetry: true,
      before,
      after,
      receipt,
      retry,
    };
  } finally {
    await client?.close();
    for (const worker of workers) worker.terminate();
    const directory = await navigator.storage.getDirectory(),
      digest = new Uint8Array(
        await crypto.subtle.digest("SHA-256", new TextEncoder().encode(scope)),
      );
    const name =
      "gonvex-opfs-experiment-" +
      Array.from(digest, (byte) => byte.toString(16).padStart(2, "0")).join("");
    for (const suffix of [".db", ".scratch"])
      try {
        await directory.removeEntry(name + suffix);
      } catch {}
  }
}
