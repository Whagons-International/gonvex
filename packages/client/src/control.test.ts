import { describe, expect, it } from "vitest";
import { control, controlFunctionSchemas } from "./control";

describe("Control Plane references", () => {
  it("ships an explicit schema and authorization boundary for every generated reference", () => {
    const references: Array<Record<string, unknown>> = [];
    const visit = (value: unknown) => {
      if (!value || typeof value !== "object") return;
      const record = value as Record<string, unknown>;
      if (typeof record.path === "string") {
        references.push(record);
        return;
      }
      for (const child of Object.values(record)) visit(child);
    };
    visit(control);

    expect(references.length).toBeGreaterThan(30);
    expect(new Set(references.map((reference) => reference.path))).toHaveProperty("size", references.length);
    for (const reference of references) {
      expect(["query", "reducer", "action"]).toContain(reference.kind);
      expect(reference.scope).toBe("control");
      expect(reference.delivery).toBe(reference.kind === "query" && [
        "control.auth.realms.list","control.tenants.mine","control.invitations.list","control.project.developers.list","control.developer.status",
        "control.assistant.getDefaults","control.voice.getConfiguration","control.support.listTenants",
        "control.support.listSessions","control.support.listErrors","users.myTenants",
      ].includes(reference.path as string) ? "live" : "oneShot");
      expect(["public", "account", "tenantAdmin", "developer", "projectAdmin"]).toContain(reference.authorization);
      expect(reference.argsSchema).toEqual(controlFunctionSchemas[reference.path as string]?.args);
      expect(reference.resultSchema).toEqual(controlFunctionSchemas[reference.path as string]?.result);
    }
    expect(Object.keys(controlFunctionSchemas).sort()).toEqual(
      references.map((reference) => reference.path as string).sort(),
    );
    expect(control.legacy.users.myTenants).toMatchObject({ path: "users.myTenants", scope: "control", delivery: "live" });
    expect(control.legacy.tenants.getInvitationByToken).toMatchObject({ path: "tenants.getInvitationByToken", scope: "control" });
    expect(control.legacy.tenants.acceptInvitation).toMatchObject({ path: "tenants.acceptInvitation", scope: "control" });
  });
});
