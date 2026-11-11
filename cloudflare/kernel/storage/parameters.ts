// Parameter binding with the Rust runtime's rules on Durable Object SQLite.
//
// Rust binds module parameters by number (store/src/direct.rs `Stmt::bind`):
// `$N` and `?N` take the Nth value, a bare `?` the next one, `$name` and
// `:name` are errors, and once a parameter has no value every later one
// stays NULL. Durable Object SQL binds positionally, by SQLite's parameter
// index: `$name` parameters are numbered by first appearance, so `SELECT $2,
// $1` would swap values (contract-changes 0011 §1). It also binds every JS
// number as REAL, so an integer would reach a TEXT column as "3.0" and
// `json_object` as 3.0.
//
// So the statement is rewritten: every parameter becomes `?k`, numbered by
// first appearance of the slot it occupies, with the value Rust would bind
// there. An integer value becomes `+CAST(?k AS INTEGER)`: the CAST makes the
// value an INTEGER and the unary plus drops the CAST's affinity, so it
// compares, stores and indexes exactly like an integer bound with
// sqlite3_bind_int64. Result columns named after a rewritten parameter get
// their original names back.

import { databaseMessage, protocolMessage } from "./errors";
import type { Token } from "./lexer";
import { NULL_PARAMETER, type Parameter } from "./values";

/** Durable Object SQLite's SQLITE_MAX_VARIABLE_NUMBER. */
export const MAX_BOUND_PARAMETERS = 100;

export type SqlBind = null | number | string;

export class BindError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "BindError";
  }
}

function leadingNumber(text: string): number | undefined {
  const digits = /^[0-9]*/.exec(text)![0];
  return digits ? Number(digits) : undefined;
}

export interface SlotAssignment {
  /** SQLite parameter index of every `param` token, by token index. */
  readonly slotOfToken: ReadonlyMap<number, number>;
  /** The value Rust binds at each SQLite index (1-based; index 0 unused). */
  readonly values: readonly Parameter[];
}

/**
 * Emulates SQLite's parameter numbering (sqlite3ExprAssignVarNumber) and
 * then Rust's `Stmt::bind` over those indexes.
 */
export function assignSlots(tokens: readonly Token[], parameters: readonly Parameter[]): SlotAssignment {
  const slotOfToken = new Map<number, number>();
  const nameOfSlot = new Map<number, string>();
  const slotOfName = new Map<string, number>();
  let count = 0;
  tokens.forEach((found, index) => {
    if (found.type !== "param") return;
    const name = found.text;
    let slot: number;
    if (name === "?") slot = ++count;
    else if (name[0] === "?") {
      slot = Number(name.slice(1));
      // The Rust runtime's SQLite keeps the default limit of 32766.
      if (!Number.isSafeInteger(slot) || slot < 1 || slot > 32766) {
        throw new BindError(databaseMessage(1, "variable number must be between ?1 and ?32766"));
      }
      if (slot > count) count = slot;
      if (!nameOfSlot.has(slot)) nameOfSlot.set(slot, name);
    } else {
      const known = slotOfName.get(name);
      if (known !== undefined) slot = known;
      else {
        slot = ++count;
        slotOfName.set(name, slot);
        nameOfSlot.set(slot, name);
      }
    }
    slotOfToken.set(index, slot);
  });
  const values: Parameter[] = [NULL_PARAMETER];
  let sequential = 0;
  let exhausted = false;
  for (let slot = 1; slot <= count; slot++) {
    if (exhausted) { values.push(NULL_PARAMETER); continue; }
    const name = nameOfSlot.get(slot);
    let index: number;
    if (name === undefined) index = ++sequential;
    else {
      let number: number | undefined;
      if (name.startsWith("?")) number = leadingNumber(name.slice(1));
      else if (name.startsWith("$")) {
        number = leadingNumber(name.slice(1));
        if (number === undefined) throw new BindError(protocolMessage(`parameters with non-integer names are not currently supported: ${name.slice(1)}`));
      } else throw new BindError(protocolMessage(`unsupported SQL parameter format: ${name}`));
      if (number === undefined || number <= 0) throw new BindError(protocolMessage(`invalid SQL parameter: ${name}`));
      index = number;
    }
    const value = parameters[index - 1];
    if (value === undefined) { exhausted = true; values.push(NULL_PARAMETER); continue; }
    values.push(value);
  }
  return { slotOfToken, values };
}

export interface Rewrite {
  /** First token index replaced (inclusive). */
  readonly from: number;
  /** Last token index replaced (inclusive). */
  readonly to: number;
  /** The replacement, rendered by the writer that emits it (parameter numbers are per writer). */
  render(writer: StatementWriter): string;
}

export interface BoundStatement {
  readonly sql: string;
  readonly binds: SqlBind[];
  /** Rewritten parameter text -> original token text, for result column names. */
  readonly renames: ReadonlyMap<string, string>;
}

/**
 * Emits `source` with every parameter token rewritten and the given token
 * ranges replaced. `emit` is also handed to rewrites that need the rewritten
 * text of a token range (a REGEXP operand that holds parameters).
 */
export class StatementWriter {
  private readonly compact = new Map<number, number>();
  readonly binds: SqlBind[] = [];
  readonly renames = new Map<string, string>();

  constructor(
    private readonly source: string,
    private readonly tokens: readonly Token[],
    private readonly slots: SlotAssignment,
  ) {}

  /** The value Rust binds for the parameter token at `index`. */
  valueAt(index: number): Parameter {
    const slot = this.slots.slotOfToken.get(index);
    return slot === undefined ? NULL_PARAMETER : this.slots.values[slot] ?? NULL_PARAMETER;
  }

  private parameterText(index: number): string {
    const slot = this.slots.slotOfToken.get(index)!;
    let compact = this.compact.get(slot);
    const value = this.slots.values[slot] ?? NULL_PARAMETER;
    if (compact === undefined) {
      compact = this.binds.length + 1;
      this.compact.set(slot, compact);
      this.binds.push(value.kind === "null" ? null : value.value);
    }
    const text = value.kind === "integer" ? `+CAST(?${compact} AS INTEGER)` : `?${compact}`;
    if (!this.renames.has(text)) this.renames.set(text, this.tokens[index]!.text);
    return text;
  }

  /** Text of tokens `from..to` (inclusive) with parameters rewritten. */
  emit(from: number, to: number, rewrites: readonly Rewrite[] = []): string {
    let out = "";
    let index = from;
    let rewrite = 0;
    while (index <= to) {
      const replacing = rewrites[rewrite];
      if (replacing && replacing.from === index) {
        out += replacing.render(this);
        index = replacing.to + 1;
        rewrite++;
      } else {
        const found = this.tokens[index]!;
        out += found.type === "param" ? this.parameterText(index) : found.text;
        index++;
      }
      // Keep the original spacing and comments between tokens.
      if (index <= to) out += this.source.slice(this.tokens[index - 1]!.end, this.tokens[index]!.start);
    }
    return out;
  }
}

/** Rewrites a whole statement. `rewrites` are sorted, non-overlapping token ranges. */
export function bindStatement(source: string, tokens: readonly Token[], slots: SlotAssignment, rewrites: readonly Rewrite[] = []): BoundStatement {
  const writer = new StatementWriter(source, tokens, slots);
  const sql = tokens.length ? writer.emit(0, tokens.length - 1, rewrites) : "";
  return { sql, binds: writer.binds, renames: writer.renames };
}

const REWRITTEN_PARAMETER = /\+CAST\(\?\d+ AS INTEGER\)|\?\d+/g;

/**
 * SQLite names an unaliased result column after its source text. Restores
 * the module's own text for a column whose name holds a rewritten parameter,
 * when that original text occurs in the module's statement.
 */
export function restoreColumnName(name: string, renames: ReadonlyMap<string, string>, original: string): string {
  if (!renames.size || !name.includes("?")) return name;
  const restored = name.replace(REWRITTEN_PARAMETER, (text) => renames.get(text) ?? text);
  return restored !== name && original.includes(restored) ? restored : name;
}
