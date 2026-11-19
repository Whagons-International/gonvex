// X17d (#528 review 4, 5): the snapshot cache's byte accounting and eviction across owners.
import { describe, expect, it } from "vitest";
import { ROW_OVERHEAD_BYTES, SnapshotCache, retainedRowBytes, type CachedSnapshot } from "../snapshot-cache.ts";

const entry = (rows: number, text = "x".repeat(100)): CachedSnapshot => {
  const list = Array.from({ length: rows }, (_, i) => ({ key: `k${i}`, text, hash: "h".repeat(64), value: {}, bytes: text.length }));
  return { epoch: "e", revision: 1, fingerprint: "f", schema: 0, rows: list, window: { rows, bytes: 0, truncated: false } as never, bytes: retainedRowBytes(list) };
};

describe("X17d snapshot cache accounting", () => {
  it("counts UTF-16 text, key and hash plus per-row overhead", () => {
    expect(retainedRowBytes([{ key: "ab", text: "héllo", hash: "0".repeat(64) }])).toBe(2 * (5 + 2 + 64) + ROW_OVERHEAD_BYTES);
  });

  it("evicts least recently used entries across owners and frees them", () => {
    const one = entry(100), budget = one.bytes * 4 + 10;
    const cache = new SnapshotCache(budget);
    const a = {}, b = {};
    cache.set(a, "x", one);
    cache.set(b, "y", entry(100));
    cache.set(a, "z", entry(100));
    expect(cache.bytes).toBe(3 * one.bytes);
    cache.get(a, "x"); // x is now more recent than y
    cache.set(b, "w", entry(100));
    cache.set(b, "v", entry(100)); // over budget: the least recently used (y, owned by b) leaves
    expect(cache.get(b, "y")).toBeUndefined();
    expect(cache.get(a, "x")).toBeDefined();
    expect(cache.bytes).toBeLessThanOrEqual(budget);
  });

  it("does not store an entry over a quarter of the budget", () => {
    const cache = new SnapshotCache(1000);
    cache.set({}, "big", entry(10));
    expect(cache.bytes).toBe(0);
  });
});
