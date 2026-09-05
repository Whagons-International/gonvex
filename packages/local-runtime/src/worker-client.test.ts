import { describe, expect, it } from "vitest";
import { createLocalReducerWorker } from "./worker-client.js";

class WorkerFixture extends EventTarget {
  messages: unknown[] = [];
  postMessage(message: unknown) { this.messages.push(message); }
  terminate() {}
  ready() { this.dispatchEvent(new MessageEvent("message", { data: { id: 0 } })); }
}

describe("worker transport failures", () => {
  it("rejects pending and future calls after an undecodable response", async () => {
    const worker = new WorkerFixture();
    const client = createLocalReducerWorker(worker as unknown as Worker);
    worker.ready();
    const first = client.replay({ scope: "scope", tables: {} }, []);
    await Promise.resolve();
    expect(worker.messages).toHaveLength(1);
    worker.dispatchEvent(new Event("messageerror"));
    await expect(first).rejects.toThrow("could not be decoded");
    await expect(client.replay({ scope: "scope", tables: {} }, [])).rejects.toThrow("could not be decoded");
    expect(worker.messages).toHaveLength(1);
    client.close();
  });

  it("settles readiness when closed before initialization", async () => {
    const client = createLocalReducerWorker(new WorkerFixture() as unknown as Worker);
    client.close();
    await expect(client.ready).rejects.toThrow("closed");
    await expect(client.replay({ scope: "scope", tables: {} }, [])).rejects.toThrow("closed");
  });
});

it("isolates request IDs and closure across separate tab workers", async () => {
  const firstWorker = new WorkerFixture();
  const secondWorker = new WorkerFixture();
  const first = createLocalReducerWorker(firstWorker as unknown as Worker);
  const second = createLocalReducerWorker(secondWorker as unknown as Worker);
  firstWorker.ready();
  secondWorker.ready();
  const firstCall = first.replay({ scope: "tenant-a", tables: {} }, []);
  const secondCall = second.replay({ scope: "tenant-b", tables: {} }, []);
  await Promise.resolve();
  expect(firstWorker.messages).toMatchObject([{ id: 1 }]);
  expect(secondWorker.messages).toMatchObject([{ id: 1 }]);
  first.close();
  await expect(firstCall).rejects.toThrow("closed");
  secondWorker.dispatchEvent(new MessageEvent("message", { data: { id: 1, result: { transactions: [], rejected: [] } } }));
  await expect(secondCall).resolves.toEqual({ transactions: [], rejected: [] });
  second.close();
});
