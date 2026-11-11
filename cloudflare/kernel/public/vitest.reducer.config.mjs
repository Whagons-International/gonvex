import { fileURLToPath } from "node:url";
import base from "../reducer/vitest.config.mjs";

// K16 on the real Reducer path: K07's test Worker (K05 sockets, the K07 tenant
// object, the conformance fixture bundles), with K16's anonymous-session specs.
// `vitest run --config kernel/public/vitest.reducer.config.mjs` from cloudflare/.
const reducerRoot = fileURLToPath(new URL("../reducer/", import.meta.url));
export default {
  ...base,
  root: reducerRoot,
  test: { include: [fileURLToPath(new URL("./test/reducer/*.spec.ts", import.meta.url))] },
};
