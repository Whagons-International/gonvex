// The Worker version's releases in tests: one baked project.
import * as app from "./fixture-app.js";
export const releases = {
  "fixture-project": {
    project: "fixture-project",
    hash: "fixture-hash",
    generation: 1,
    clientContract: null,
    functions: {
      "notes.ping": { kind: "query", internal: false, args: { kind: "object", fields: {} }, result: { kind: "any" } },
      "notes.stamp": { kind: "reducer", internal: false, args: { kind: "object", fields: {} }, result: { kind: "any" } },
      "notes.send": { kind: "action", internal: false, args: { kind: "object", fields: {} }, result: { kind: "any" } },
    },
    exports: app,
  },
};
