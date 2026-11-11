// Portable schema validation, ported from rust/crates/module-runtime/src/lib.rs
// (validate_portable_schema / validate_portable_schema_definition). Messages
// are Rust's exactly; `ModuleError::InvalidArguments(message)` wraps them.
// JSON object order is insertion order on both sides (Rust builds serde_json
// with `preserve_order`), so the first reported field matches.

export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
type SchemaObject = Record<string, unknown>;

const isObject = (value: unknown): value is SchemaObject => value !== null && typeof value === "object" && !Array.isArray(value);

function jsonType(value: unknown): string {
  if (value === null || value === undefined) return "null";
  if (Array.isArray(value)) return "array";
  if (typeof value === "boolean") return "boolean";
  if (typeof value === "number") return "number";
  if (typeof value === "string") return "string";
  return "object";
}
const expected = (path: string, what: string, value: unknown) => `${path}: expected ${what}, received ${jsonType(value)}`;

/** serde_json's `Display` of a JSON value (compact). */
const display = (value: unknown) => JSON.stringify(value);

/** Rust formats f64 with `{}`: integral values print without ".0". */
const number = (value: number) => (Number.isInteger(value) ? String(value) : String(value));

export class SchemaError extends Error {}

/** Throws a `SchemaError` with Rust's message when the definition is invalid. */
export function validateSchemaDefinition(schema: unknown, path = "$", optionalAllowed = false): void {
  if (!isObject(schema)) throw new SchemaError(`${path}: schema must be an object`);
  const kind = schema["kind"];
  if (typeof kind !== "string") throw new SchemaError(`${path}.kind: required string`);
  const nonNegativeInteger = (value: unknown, at: string) => {
    if (value !== undefined && !(typeof value === "number" && Number.isInteger(value) && value >= 0)) throw new SchemaError(`${at}: must be a non-negative integer`);
  };
  const optionalNumber = (value: unknown, at: string) => {
    if (value !== undefined && typeof value !== "number") throw new SchemaError(`${at}: must be a number`);
  };
  switch (kind) {
    case "string": {
      const format = schema["format"];
      if (format !== undefined && !["email", "uri", "uuid", "datetime"].includes(format as string)) throw new SchemaError(`${path}.format: unsupported string format`);
      nonNegativeInteger(schema["minLength"], `${path}.minLength`);
      nonNegativeInteger(schema["maxLength"], `${path}.maxLength`);
      return;
    }
    case "number":
      if (schema["integer"] !== undefined && typeof schema["integer"] !== "boolean") throw new SchemaError(`${path}.integer: must be a boolean`);
      optionalNumber(schema["minimum"], `${path}.minimum`);
      optionalNumber(schema["maximum"], `${path}.maximum`);
      return;
    case "boolean": case "null": case "any": return;
    case "id": {
      const entity = schema["entity"];
      if (typeof entity !== "string" || entity.trim() === "") throw new SchemaError(`${path}.entity: required non-empty string`);
      return;
    }
    case "literal":
      if (!Object.hasOwn(schema, "value")) throw new SchemaError(`${path}.value: required`);
      return;
    case "array":
      if (!Object.hasOwn(schema, "items")) throw new SchemaError(`${path}.items: required`);
      validateSchemaDefinition(schema["items"], `${path}.items`, false);
      return;
    case "object": {
      const fields = schema["fields"];
      if (!isObject(fields)) throw new SchemaError(`${path}.fields: required object`);
      if (schema["allowUnknown"] !== undefined && typeof schema["allowUnknown"] !== "boolean") throw new SchemaError(`${path}.allowUnknown: must be a boolean`);
      for (const [name, field] of Object.entries(fields)) validateSchemaDefinition(field, `${path}.fields.${name}`, true);
      return;
    }
    case "record":
      if (!Object.hasOwn(schema, "values")) throw new SchemaError(`${path}.values: required`);
      validateSchemaDefinition(schema["values"], `${path}.values`, false);
      return;
    case "optional":
      if (!optionalAllowed) throw new SchemaError(`${path}: optional schemas are only valid as object fields`);
      if (!Object.hasOwn(schema, "value")) throw new SchemaError(`${path}.value: required`);
      validateSchemaDefinition(schema["value"], `${path}.value`, false);
      return;
    default:
      throw new SchemaError(`${path}.kind: unsupported schema kind ${JSON.stringify(kind)}`);
  }
}

function deepEqual(left: unknown, right: unknown): boolean {
  if (left === right) return true;
  if (typeof left !== typeof right || left === null || right === null || typeof left !== "object") return false;
  if (Array.isArray(left) !== Array.isArray(right)) return false;
  if (Array.isArray(left)) {
    const r = right as unknown[];
    return left.length === r.length && left.every((item, index) => deepEqual(item, r[index]));
  }
  const a = Object.keys(left as object), b = Object.keys(right as object);
  return a.length === b.length && a.every((key) => Object.hasOwn(right as object, key) && deepEqual((left as SchemaObject)[key], (right as SchemaObject)[key]));
}

/** Throws a `SchemaError` with Rust's message when `value` does not match. */
export function validateValue(schema: unknown, value: unknown, path = "$", optionalAllowed = false): void {
  validateSchemaDefinition(schema, path, optionalAllowed);
  const object = schema as SchemaObject;
  const kind = object["kind"] as string;
  switch (kind) {
    case "any": return;
    case "string": {
      if (typeof value !== "string") throw new SchemaError(expected(path, "string", value));
      const length = [...value].length;
      const min = object["minLength"], max = object["maxLength"];
      if (typeof min === "number" && length < min) throw new SchemaError(`${path}: string length must be at least ${min}`);
      if (typeof max === "number" && length > max) throw new SchemaError(`${path}: string length must be at most ${max}`);
      return;
    }
    case "number": {
      if (typeof value !== "number") throw new SchemaError(expected(path, "number", value));
      if (object["integer"] === true && !Number.isInteger(value)) throw new SchemaError(`${path}: expected integer`);
      const min = object["minimum"], max = object["maximum"];
      if (typeof min === "number" && value < min) throw new SchemaError(`${path}: number must be at least ${number(min)}`);
      if (typeof max === "number" && value > max) throw new SchemaError(`${path}: number must be at most ${number(max)}`);
      return;
    }
    case "boolean": if (typeof value !== "boolean") throw new SchemaError(expected(path, "boolean", value)); return;
    case "null": if (value !== null) throw new SchemaError(expected(path, "null", value)); return;
    case "id": if (typeof value !== "string") throw new SchemaError(expected(path, "entity id string", value)); return;
    case "literal":
      if (!deepEqual(object["value"], value)) throw new SchemaError(`${path}: expected literal ${display(object["value"])}`);
      return;
    case "array": {
      if (!Array.isArray(value)) throw new SchemaError(expected(path, "array", value));
      value.forEach((item, index) => validateValue(object["items"], item, `${path}[${index}]`, false));
      return;
    }
    case "object": {
      if (!isObject(value)) throw new SchemaError(expected(path, "object", value));
      const fields = object["fields"] as SchemaObject;
      for (const [name, field] of Object.entries(fields)) {
        const fieldKind = (field as SchemaObject)["kind"];
        if (Object.hasOwn(value, name)) {
          const effective = fieldKind === "optional" ? (field as SchemaObject)["value"] : field;
          validateValue(effective, value[name], `${path}.${name}`, false);
        } else if (fieldKind !== "optional") {
          throw new SchemaError(`${path}.${name}: required field is missing`);
        }
      }
      if (object["allowUnknown"] !== true) for (const name of Object.keys(value)) if (!Object.hasOwn(fields, name)) throw new SchemaError(`${path}.${name}: unknown field`);
      return;
    }
    case "record": {
      if (!isObject(value)) throw new SchemaError(expected(path, "object", value));
      for (const [name, item] of Object.entries(value)) validateValue(object["values"], item, `${path}.${name}`, false);
      return;
    }
    case "optional": validateValue(object["value"], value, path, false); return;
    default: throw new SchemaError(expected(path, kind, value));
  }
}
