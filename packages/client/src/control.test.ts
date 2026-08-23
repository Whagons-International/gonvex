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
      expect(reference.delivery).toBe("oneShot");
      expect(["public", "account", "tenantAdmin", "projectAdmin"]).toContain(reference.authorization);
      expect(reference.argsSchema).toEqual(controlFunctionSchemas[reference.path as string]?.args);
      expect(reference.resultSchema).toEqual(controlFunctionSchemas[reference.path as string]?.result);
    }
    expect(Object.keys(controlFunctionSchemas).sort()).toEqual(
      references.map((reference) => reference.path as string).sort(),
    );
  });
});
