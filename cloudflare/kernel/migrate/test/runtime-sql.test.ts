// runtime-sql.ts and runtime-prelude.sql are generated from the Rust reference; they
// must stay byte-identical to it (regenerate with verify/generate-runtime-sql.mjs).
import { describe, expect, it } from "vitest";
// @ts-expect-error Vite raw imports
import provision from "../../../../rust/crates/store/src/provision.rs?raw";
// @ts-expect-error Vite raw imports
import feed from "../../../../rust/crates/store/src/feed.rs?raw";
// @ts-expect-error Vite raw imports
import reference from "../../../../rust/crates/store/src/reference_integrity/schema.sql?raw";
// @ts-expect-error Vite raw imports
import options from "../../../../rust/crates/store/src/schema_options.sql?raw";
// @ts-expect-error Vite raw imports
import prelude from "../runtime-prelude.sql?raw";
// @ts-expect-error Vite raw imports
import generated from "../runtime-sql.ts?raw";
// @ts-expect-error plain JS module
import { extract, preludeSql, render } from "../verify/generate-runtime-sql.mjs";

describe("runtime SQL drift", () => {
  it("runtime-sql.ts and runtime-prelude.sql equal what the Rust sources generate", () => {
    const values = extract({ provision, feed, reference, options });
    expect(generated).toBe(render(values));
    expect(prelude).toBe(preludeSql(values));
  });
});
