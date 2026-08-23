import assert from "node:assert/strict";
import test from "node:test";

import { createModule } from "../dist/index.js";

test("tenant modules cannot declare host-owned Control Plane paths", () => {
  const module = createModule({ name: "tenant-app", version: "1" });
  assert.throws(
    () => module.query("control.accounts.me", { run: async () => null }),
    /host-reserved Control Plane namespace/,
  );
});
