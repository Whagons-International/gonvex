import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

describe("client package artifact", () => {
  it("does not ship removed cache or sync modules", () => {
    execFileSync("pnpm", ["run", "build"], { cwd: packageRoot, stdio: "ignore" });
    const packed = execFileSync("npm", ["pack", "--dry-run", "--json"], {
      cwd: packageRoot,
      encoding: "utf8",
      stdio: ["ignore", "pipe", "inherit"],
    });
    const packResult = JSON.parse(packed) as
      | Array<{ files?: Array<{ path: string }> }>
      | Record<string, { files?: Array<{ path: string }> }>;
    const packageResult = Array.isArray(packResult)
      ? packResult[0]
      : Object.values(packResult)[0];
    const files = packageResult?.files?.map((file) => file.path) ?? [];
    const removed = /(?:browser-cache|cache(?:-coordinator)?|persistent-cache|query-cache|sync-store)/;

    expect(files).toContain("dist/index.js");
    expect(files.filter((file) => removed.test(file))).toEqual([]);
    expect(readFileSync(resolve(packageRoot, "dist/index.js"), "utf8")).not.toContain("query-cache.js");
  });
});
