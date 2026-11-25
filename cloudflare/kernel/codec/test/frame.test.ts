// Workerd tests for WLP1 binary frame envelope codec & streaming splitter.
import { describe, expect, it } from "vitest";
import {
  F_COMMIT, F_DELETE, F_END, F_MEMBERS, F_PAGE, F_PING, F_RESET,
  bit, buildMembersFrame, concatFrames, createFrameSplitter, decodeFrame,
  encodeClassDelivery, encodeFrame, encodePage, layoutFromCols, splitFrames,
  type PageCol,
} from "../index";

const COLS: PageCol[] = [{ id: 0, k: "i" }, { id: 1, k: "x" }];
const LAYOUT = layoutFromCols(3, COLS);

describe("WLP1 binary frame envelope and streaming splitter", () => {
  it("roundtrips PAGE, DELETE, COMMIT, MEMBERS, END, RESET, PING across arbitrary byte chunk boundaries", () => {
    const pageBytes = encodePage(3, COLS, [
      [10, 1, 100, "alpha"],
      [11, 2, 200, "beta"],
    ]);
    const body = concatFrames([
      encodeFrame({ k: F_RESET }),
      encodeFrame({ k: F_PAGE, t: 3, flags: 0, bytes: pageBytes }),
      encodeFrame({ k: F_DELETE, t: 3, rids: new Int32Array([5, 6, 99]) }),
      encodeFrame(buildMembersFrame(3, [10, 11, 15])),
      encodeFrame({ k: F_COMMIT, seq: 42, flags: 0, metaBytes: new Uint8Array([91, 93]) }),
      encodeFrame({ k: F_PING }),
      encodeFrame({ k: F_END, seq: 42 }),
    ]);

    // Split in one pass
    const whole = splitFrames(body);
    expect(whole).toHaveLength(7);

    // Stream in 7-byte misaligned chunks to stress splitter reassembly & 8-byte alignment
    const stream = createFrameSplitter();
    const streamed: Uint8Array[] = [];
    for (let off = 0; off < body.length; off += 7) {
      const done = stream.push(body.subarray(off, Math.min(off + 7, body.length)));
      for (let i = 0; i < done.length; i++) streamed.push(done[i]!);
    }
    expect(stream.bufferedBytes).toBe(0);
    expect(streamed).toHaveLength(7);

    const decoded = streamed.map((f) => decodeFrame(f, () => LAYOUT));
    expect(decoded[0]).toEqual({ k: F_RESET });

    const pageFrame = decoded[1]!;
    expect(pageFrame.k).toBe(F_PAGE);
    if (pageFrame.k === F_PAGE) {
      expect(pageFrame.t).toBe(3);
      expect(pageFrame.page.rows).toBe(2);
      expect(pageFrame.page.get(0, 0)).toBe(10);
      expect(pageFrame.page.get(3, 1)).toBe("beta");
    }

    const delFrame = decoded[2]!;
    expect(delFrame.k).toBe(F_DELETE);
    if (delFrame.k === F_DELETE) expect(Array.from(delFrame.rids)).toEqual([5, 6, 99]);

    const memFrame = decoded[3]!;
    expect(memFrame.k).toBe(F_MEMBERS);
    if (memFrame.k === F_MEMBERS) {
      expect(memFrame.base).toBe(10);
      expect(memFrame.n).toBe(6);
      expect(bit(memFrame.bits, 0)).toBe(1); // 10
      expect(bit(memFrame.bits, 1)).toBe(1); // 11
      expect(bit(memFrame.bits, 2)).toBe(0); // 12
      expect(bit(memFrame.bits, 5)).toBe(1); // 15
    }

    expect(decoded[4]).toMatchObject({ k: F_COMMIT, seq: 42, flags: 0 });
    expect(decoded[5]).toEqual({ k: F_PING });
    expect(decoded[6]).toEqual({ k: F_END, seq: 42 });
  });

  it("packages hoisted class deliveries into one binary WS message and rejects corrupt/truncated frames", () => {
    const pageBytes = encodePage(3, COLS, [[1, 1, 7, "hello"]]);
    const msg = encodeClassDelivery(19, [{ tbl: 3, upsertPages: [pageBytes], deletedRids: new Int32Array([2]) }]);
    const frames = splitFrames(msg).map((f) => decodeFrame(f, () => LAYOUT));
    expect(frames.map((f) => f.k)).toEqual([F_PAGE, F_DELETE, F_COMMIT]);

    // Truncated body throws sync.frame
    expect(() => splitFrames(msg.subarray(0, msg.length - 1))).toThrow("sync.frame");
    // Corrupt frame kind throws sync.frame
    const bad = encodeFrame({ k: F_PING });
    bad[4] = 99;
    expect(() => splitFrames(bad)).toThrow("sync.frame");
  });
});
