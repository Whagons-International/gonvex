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
