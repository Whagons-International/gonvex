import { describe, expect, it } from "vitest";
import { ModuleCache } from "../session";
import type { Ports } from "../ports";

function ports(hash: { current: string | null }, calls: string[], fail = { next: false }) {
  return {
    modules: {
      module: async () => (hash.current === null ? null : { artifactHash: hash.current, clientContract: null }),
      epoch: async (projectId: string, databaseUrl: string) => {
        calls.push(`${projectId}|${databaseUrl}|${hash.current}`);
        if (fail.next) { fail.next = false; throw new Error("control unavailable"); }
        return `epoch:${databaseUrl}:${hash.current}`;
      },
    },
  } as unknown as Ports;
}

describe("X17 replica epoch cache (one Control read per project, database and module)", () => {
  it("reads once per (project, database, module hash)", async () => {
    const hash = { current: "h1" as string | null }, calls: string[] = [];
    const cache = new ModuleCache(ports(hash, calls));
    expect(await cache.epoch("p", "db1")).toBe("epoch:db1:h1");
    expect(await cache.epoch("p", "db1")).toBe("epoch:db1:h1");
    expect(await cache.epoch("p", "db2")).toBe("epoch:db2:h1");
    expect(calls).toEqual(["p|db1|h1", "p|db2|h1"]);
  });

  it("reads again for a new module hash and after a reload", async () => {
    const hash = { current: "h1" as string | null }, calls: string[] = [];
    const cache = new ModuleCache(ports(hash, calls));
    await cache.epoch("p", "db");
    hash.current = "h2";
    cache.set("p", { artifactHash: "h2", clientContract: null }); // the auth's module refresh
    expect(await cache.epoch("p", "db")).toBe("epoch:db:h2");
    cache.invalidate("p"); // a deploy (moduleReloaded)
    expect(await cache.epoch("p", "db")).toBe("epoch:db:h2");
    expect(calls).toEqual(["p|db|h1", "p|db|h2", "p|db|h2"]);
  });

  it("keeps neither a failed read nor a module without a hash", async () => {
    const hash = { current: "h1" as string | null }, calls: string[] = [], fail = { next: true };
    const cache = new ModuleCache(ports(hash, calls, fail));
    await expect(cache.epoch("p", "db")).rejects.toThrow("control unavailable");
    expect(await cache.epoch("p", "db")).toBe("epoch:db:h1");
    const none = { current: null as string | null }, noneCalls: string[] = [];
    const bare = new ModuleCache(ports(none, noneCalls));
    await bare.epoch("q", "db"); await bare.epoch("q", "db");
    expect(noneCalls).toHaveLength(2);
    expect(calls).toHaveLength(2);
  });
});
