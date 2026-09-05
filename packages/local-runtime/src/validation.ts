import type { PortableSchema } from "@gonvex/module-sdk";

/** Mirrors the Rust portable schema validator, including Unicode lengths. */
export function validateValue(schema: PortableSchema | undefined, value: unknown, path = "$", optional = false): void {
  if (!schema) return;
  const fail = (message: string): never => { throw new Error(`${path}: ${message}`); };
  const object = () => { if (value === null || typeof value !== "object" || Array.isArray(value)) fail("expected object"); return value as Record<string, unknown>; };
  switch (schema.kind) {
    case "any": return;
    case "string": {
      if (typeof value !== "string") fail("expected string");
      const length = [...value as string].length;
      if (schema.minLength !== undefined && length < schema.minLength) fail(`string length must be at least ${schema.minLength}`);
      if (schema.maxLength !== undefined && length > schema.maxLength) fail(`string length must be at most ${schema.maxLength}`);
      return;
    }
    case "id": if (typeof value !== "string") fail("expected entity id string"); return;
    case "number": {
      if (typeof value !== "number" || !Number.isFinite(value)) fail("expected number");
      if (schema.integer && !Number.isInteger(value)) fail("expected integer");
      if (schema.minimum !== undefined && (value as number) < schema.minimum) fail(`number must be at least ${schema.minimum}`);
      if (schema.maximum !== undefined && (value as number) > schema.maximum) fail(`number must be at most ${schema.maximum}`);
      return;
    }
    case "boolean": if (typeof value !== "boolean") fail("expected boolean"); return;
    case "null": if (value !== null) fail("expected null"); return;
    case "literal": if (!equal(schema.value, value)) fail("expected literal"); return;
    case "array":
      if (!Array.isArray(value)) fail("expected array");
      (value as unknown[]).forEach((item, index) => validateValue(schema.items, item, `${path}[${index}]`)); return;
    case "record":
      Object.entries(object()).forEach(([key, item]) => validateValue(schema.values, item, `${path}.${key}`)); return;
    case "object": {
      const input = object();
      for (const [key, field] of Object.entries(schema.fields)) {
        if (!Object.hasOwn(input, key)) { if (field.kind !== "optional") fail(`${key}: required field is missing`); continue; }
        validateValue(field, input[key], `${path}.${key}`, true);
      }
      if (!schema.allowUnknown) for (const key of Object.keys(input)) if (!Object.hasOwn(schema.fields, key)) fail(`${key}: unknown field`);
      return;
    }
    case "optional": if (!optional) fail("optional is only valid on object fields"); validateValue(schema.value, value, path); return;
  }
}
function equal(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (!left || !right || typeof left !== "object" || typeof right !== "object" || Array.isArray(left) !== Array.isArray(right)) return false;
  const a = Object.keys(left), b = Object.keys(right);
  return a.length === b.length && a.every(key => Object.hasOwn(right, key) && equal((left as any)[key], (right as any)[key]));
}
