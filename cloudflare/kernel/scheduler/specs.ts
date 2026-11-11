// Cron declarations from the module manifest (`module.crons`), validated with
// the Rust runtime's rules and texts (RT/modules.rs CronSpec and its load
// checks; CONTRACT E.b.2 "Spec"). K19 calls `parseCronSpecs` when it loads a
// generation; the scheduler reads the active generation's specs.
import { admitStatement, AdmissionError, requireReadHead } from "../storage/index.ts";

export interface CronSpec {
  readonly name: string;
  readonly function: string;
  /** JSON args (`#[serde(default)]`: null when absent). */
  readonly args: unknown;
  readonly scope: "project" | "tenant";
  readonly intervalMs?: number | null | undefined;
  readonly expression?: string | null | undefined;
  /** One admitted read statement; `$1` is bound to the current time in Unix ms. */
  readonly dueWhen?: string | null | undefined;
}

const FIELDS = new Set(["name", "function", "args", "scope", "intervalMs", "expression", "dueWhen"]);

/** A manifest the runtime refuses (RT/modules.rs `invalid`). */
export class InvalidCronsError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "InvalidCronsError";
  }
}

const debug = (text: string) => JSON.stringify(text);

/**
 * `module.crons` as RT/modules.rs reads it: `deny_unknown_fields`, required
 * name/function/scope, exactly one positive `intervalMs` or non-empty
 * `expression`, unique trimmed names, `dueWhen` admitted as one read
 * statement (blank means none). Expression syntax is checked at sync, as in
 * Rust (`syncCrons`).
 */
export function parseCronSpecs(value: unknown): CronSpec[] {
  if (value === undefined || value === null) return [];
  if (!Array.isArray(value)) throw new InvalidCronsError("invalid crons: invalid type: expected a sequence");
  const crons = value.map((entry, index) => {
    if (typeof entry !== "object" || entry === null || Array.isArray(entry)) {
      throw new InvalidCronsError(`invalid crons: invalid type at index ${index}: expected struct CronSpec`);
    }
    const record = entry as Record<string, unknown>;
    for (const key of Object.keys(record)) {
      if (!FIELDS.has(key)) throw new InvalidCronsError(`invalid crons: unknown field \`${key}\``);
    }
    for (const key of ["name", "function", "scope"] as const) {
      if (!(key in record)) throw new InvalidCronsError(`invalid crons: missing field \`${key}\``);
      if (typeof record[key] !== "string") throw new InvalidCronsError(`invalid crons: invalid type for \`${key}\`: expected a string`);
    }
    const interval = record["intervalMs"];
    if (interval !== undefined && interval !== null && (typeof interval !== "number" || !Number.isInteger(interval) || interval < 0)) {
      throw new InvalidCronsError("invalid crons: invalid value for `intervalMs`: expected u64");
    }
    for (const key of ["expression", "dueWhen"] as const) {
      const text = record[key];
      if (text !== undefined && text !== null && typeof text !== "string") {
        throw new InvalidCronsError(`invalid crons: invalid type for \`${key}\`: expected a string`);
      }
    }
    return {
      name: record["name"] as string,
      function: record["function"] as string,
      args: record["args"] ?? null,
      scope: record["scope"] as CronSpec["scope"],
      intervalMs: (interval ?? undefined) as number | undefined,
      expression: (record["expression"] ?? undefined) as string | undefined,
      dueWhen: (record["dueWhen"] ?? undefined) as string | undefined,
    } satisfies CronSpec;
  });
  const names = new Set<string>();
  for (const cron of crons) {
    if (!cron.name.trim() || !cron.function.trim()) throw new InvalidCronsError("cron name and function are required");
    if (names.has(cron.name.trim())) throw new InvalidCronsError(`duplicate cron name ${debug(cron.name)}`);
    names.add(cron.name.trim());
    if ((cron.scope as string) !== "project" && (cron.scope as string) !== "tenant") {
      throw new InvalidCronsError(`cron ${debug(cron.name)} has invalid scope`);
    }
    const hasInterval = cron.intervalMs !== undefined;
    const hasExpression = cron.expression !== undefined;
    if (hasInterval === hasExpression || cron.intervalMs === 0 || (hasExpression && !cron.expression!.trim())) {
      throw new InvalidCronsError(`cron ${debug(cron.name)} requires exactly one positive intervalMs or expression`);
    }
  }
  return crons.map((cron) => {
    if (cron.dueWhen === undefined) return cron;
    if (!cron.dueWhen.trim()) return { ...cron, dueWhen: undefined };
    try {
      requireReadHead(admitStatement(cron.dueWhen).firstWord);
    } catch (error) {
      if (error instanceof AdmissionError) throw new InvalidCronsError(`cron ${debug(cron.name)} dueWhen: ${error.message}`);
      throw error;
    }
    return cron;
  });
}
