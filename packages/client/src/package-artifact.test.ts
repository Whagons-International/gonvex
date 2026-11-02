import { execFileSync } from "node:child_process";
import { copyFileSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

describe("client package artifact", () => {
  it("does not ship removed cache or sync modules", () => {
    // Other adapter tests import this package's dist while files run in
    // parallel. Compile a private artifact instead of deleting shared dist.
    const artifactRoot = mkdtempSync(resolve(packageRoot, ".artifact-test-"));
    try {
      execFileSync(resolve(packageRoot, "node_modules/.bin/tsc"), ["-p", "tsconfig.json", "--outDir", resolve(artifactRoot, "dist")], {
        cwd: packageRoot, stdio: "ignore", timeout: 60000,
      });
      copyFileSync(resolve(packageRoot, "package.json"), resolve(artifactRoot, "package.json"));
      const packed = execFileSync("npm", ["pack", "--dry-run", "--json"], {
        cwd: artifactRoot,
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
      expect(files).toContain("dist/control.js");
      expect(files).toContain("dist/control.d.ts");
      expect(files.filter((file) => removed.test(file))).toEqual([]);
      expect(readFileSync(resolve(artifactRoot, "dist/index.js"), "utf8")).not.toContain("query-cache.js");
      expect(readFileSync(resolve(artifactRoot, "dist/index.d.ts"), "utf8")).toContain("control");
      expect(readFileSync(resolve(artifactRoot, "dist/local-replica.d.ts"), "utf8")).toContain("collectionState");
    } finally { rmSync(artifactRoot, { recursive: true, force: true }); }
  }, 60000);
});
