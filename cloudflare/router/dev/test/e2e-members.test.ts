// X12b: the DEV-only e2e member seed's role/permissions overrides and its read (contract change 0090), through
// the actual operator route table. Both methods sit behind the runtime admin key alone, exactly as the seed did.
// The write and read themselves run against a real AssemblyTenant in kernel/assembly/test/composition.spec.ts.
import { beforeAll, describe, expect, it } from "vitest";
import { call, createProject } from "./helpers.ts";

const PATH = "/dev/internal/e2e/members";
let project: { id: string; key: string };
const seed = (body: Record<string, unknown>, token?: string | null) =>
  call("POST", PATH, { body: { projectId: project.id, tenantId: "t", email: "walkthrough@example.test", password: "x", ...body }, ...(token === undefined ? {} : { token }) });

beforeAll(async () => {
  project = await createProject("e2e-members-project");
});

describe("/dev/internal/e2e/members (DEV only)", () => {
  it("both methods require the runtime admin key: no token and the project key are refused", async () => {
    for (const token of [null, "wrong-key", project.key]) {
      expect((await seed({ permissions: { "config.integrations": true } }, token)).body).toEqual({ error: "runtime admin key is required" });
      const read = await call("GET", `${PATH}?project=${project.id}&tenant=t&accountId=a`, { token });
      expect([read.status, read.body]).toEqual([403, { error: "runtime admin key is required" }]);
    }
  });

  it("validates the optional overrides before touching any tenant", async () => {
    const refused = async (body: Record<string, unknown>) => {
      const reply = await seed(body);
      expect(reply.status).toBe(400);
      return reply.body.error as string;
    };
    expect(await refused({ role: "" })).toBe("role must be a non-empty string of at most 64 bytes");
    expect(await refused({ role: "   " })).toBe("role must be a non-empty string of at most 64 bytes");
    expect(await refused({ role: 7 })).toBe("role must be a non-empty string of at most 64 bytes");
    expect(await refused({ role: "r".repeat(65) })).toBe("role must be a non-empty string of at most 64 bytes");
    for (const permissions of [null, [], ["config.integrations"], "config.integrations", true, 1]) {
      expect(await refused({ permissions })).toBe("permissions must be a JSON object");
    }
    expect(await refused({ permissions: { big: "x".repeat(64 * 1024) } })).toBe("permissions must be at most 65536 bytes of JSON");
    expect(await refused({ accountOnly: true, tenantId: undefined, role: "admin" })).toBe("role and permissions apply to a tenant Member; omit accountOnly");
    expect(await refused({ accountOnly: true, tenantId: undefined, permissions: {} })).toBe("role and permissions apply to a tenant Member; omit accountOnly");
  });

  it("the read needs project, tenant and exactly one selector", async () => {
    for (const query of ["", `project=${project.id}`, `project=${project.id}&tenant=t`, `project=${project.id}&tenant=t&accountId=a&email=e@x.test`, `project=${project.id}&tenant=t&accountId=a&memberId=m`, `tenant=t&memberId=m`]) {
      const reply = await call("GET", `${PATH}?${query}`);
      expect([reply.status, reply.body]).toEqual([400, { error: "project and tenant are required, with exactly one of accountId, email or memberId" }]);
    }
    const missing = await call("GET", `${PATH}?project=${project.id}&tenant=no-such-tenant&accountId=a`);
    expect(missing.status).toBe(422);
  });

  it("no other method is routed", async () => {
    for (const method of ["PUT", "PATCH", "DELETE"]) {
      const reply = await call(method, PATH);
      expect([reply.status, reply.headers.get("allow")]).toEqual([405, "GET,POST"]);
    }
  });
});
