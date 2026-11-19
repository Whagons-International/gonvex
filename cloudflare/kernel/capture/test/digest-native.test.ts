import { describe, expect, it } from "vitest";
import { nativeSha256, sha256Hex, sha256HexJs } from "../digest";

describe("X16 sha256Hex", () => {
  it("native and JavaScript digests agree (ASCII, Unicode, empty, block edges, large)", () => {
    const cases = ["", "abc", "🍉漢字", "a".repeat(55), "a".repeat(56), "a".repeat(64), "x".repeat(300_000), JSON.stringify({ k: "v\u0000 " })];
    for (const text of cases) expect(sha256Hex(text)).toBe(sha256HexJs(text));
    expect(sha256Hex("abc")).toBe("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  });
  it("uses the native hash in workerd (nodejs_compat)", () => {
    expect(nativeSha256).toBe(true);
  });
});
