import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { writeFileSync } from "node:fs";
const commit = "24cdc5e22bc6fabda3e1557358f3e37a927e8640";
const hashes = {
  "sqlite3.mjs":
    "d3e5161c7880d91594528a95a5e95d5d07c8e595a74c240336c295b306c26891",
  "sqlite3.wasm":
    "ac1926cbebc762e448858d57b79a506ef6c66fcc201bd796d90a7a599fb93a49",
};
for (const [name, expected] of Object.entries(hashes)) {
  let bytes;
  try {
    bytes = execFileSync(
      "git",
      ["show", `${commit}:packages/client/sqlite-native/${name}`],
      { maxBuffer: 8 * 1024 * 1024 },
    );
  } catch {
    throw Error(
      `Fetch the pinned SQLite source first: git fetch origin ${commit}`,
    );
  }
  if (createHash("sha256").update(bytes).digest("hex") !== expected)
    throw Error(`Pinned SQLite asset checksum differs: ${name}`);
  writeFileSync(new URL("./build/" + name, import.meta.url), bytes);
}
process.stdout.write(
  "Pinned SQLite assets verified. Serve the repository on localhost and open packages/client/bench/opfs/browser.html.\n",
);
