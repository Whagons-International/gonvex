// X16: the router draws connection ids from the cell of its last admitted upgrade (warm placement).
import { describe, expect, it } from "vitest";
import { controllerCell, CONTROLLER_CELLS } from "../../gateway/placement";
import { STICKY_MS, STICKY_PER_SECOND, WarmCell } from "../router";

describe("X16 WarmCell", () => {
  it("is uniformly random until an upgrade is admitted", () => {
    const warm = new WarmCell();
    const cells = new Set(Array.from({ length: 400 }, () => controllerCell(warm.connectionId())));
    expect(cells.size).toBeGreaterThan(CONTROLLER_CELLS / 2);
    expect(warm.remembered).toBeUndefined();
  });

  it("draws ids in the remembered cell, distinct UUIDs, up to the per-second rate", () => {
    let now = 1_000_000;
    const warm = new WarmCell(() => now);
    warm.admitted(7);
    const ids = Array.from({ length: STICKY_PER_SECOND }, () => warm.connectionId());
    expect(ids.every((id) => controllerCell(id) === 7)).toBe(true);
    expect(new Set(ids).size).toBe(ids.length);
    expect(ids.every((id) => /^[0-9a-f-]{36}$/.test(id))).toBe(true);
    // Over the rate in this second: spread as before.
    const over = new Set(Array.from({ length: 200 }, () => controllerCell(warm.connectionId())));
    expect(over.size).toBeGreaterThan(8);
    // The next second sticks again.
    now += 1_000;
    expect(controllerCell(warm.connectionId())).toBe(7);
  });

  it("forgets the cell after STICKY_MS and on a refusal", () => {
    let now = 5_000_000;
    const warm = new WarmCell(() => now);
    warm.admitted(3);
    expect(warm.remembered).toBe(3);
    now += STICKY_MS;
    expect(warm.remembered).toBeUndefined();
    warm.admitted(4);
    expect(controllerCell(warm.connectionId())).toBe(4);
    warm.refused();
    expect(warm.remembered).toBeUndefined();
  });
});
