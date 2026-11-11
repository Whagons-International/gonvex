// Cron expressions with the semantics of the `cron` 0.15 crate that the Rust
// runtime uses (RT/scheduler.rs parse_schedule, CONTRACT E.b.2 "Expression"):
//
// - Six fields `sec min hour day-of-month month day-of-week`, an optional
//   seventh `year` (1970–2100), or a shorthand (@yearly @monthly @weekly
//   @daily @hourly). A five-field expression is prefixed with "0 " by the
//   caller (`normalizeExpression`), exactly as Rust does.
// - Day of week is 1–7 with **Sunday = 1** (SUN–SAT); 0 is invalid. A
//   five-field `0 9 * * 1` therefore runs on Sundays, as it does in Rust.
// - Day of month and day of week must **both** match (AND, not Vixie OR).
// - `*`, `?` (day fields only), points, ranges, named ranges, lists and
//   `start/step`; a point start runs to the field maximum.
// - All times are UTC and whole seconds.
//
// The grammar mirrors the crate's winnow parser, so an invalid expression
// fails with the same text, caret position included
// (`cron "<name>" has invalid expression: <input>\n<caret>\n<message>`).
// test/cron.test.ts checks parsing, iteration and errors against the crate's
// own output (verify/rust-vectors).

export class CronSyntaxError extends Error {
  constructor(readonly input: string, readonly offset: number, readonly detail: string) {
    super(`${input}\n${" ".repeat(offset)}^\n${detail}`);
    this.name = "CronSyntaxError";
  }
}

interface Unit {
  readonly name: string;
  readonly min: number;
  readonly max: number;
  readonly names?: (name: string) => number | undefined;
  readonly unitLabel: string;
}

const DAYS: Record<string, number> = {
  sun: 1, sunday: 1, mon: 2, monday: 2, tue: 3, tues: 3, tuesday: 3, wed: 4, wednesday: 4,
  thu: 5, thurs: 5, thursday: 5, fri: 6, friday: 6, sat: 7, saturday: 7,
};
const MONTHS: Record<string, number> = {
  jan: 1, january: 1, feb: 2, february: 2, mar: 3, march: 3, apr: 4, april: 4, may: 5, jun: 6, june: 6,
  jul: 7, july: 7, aug: 8, august: 8, sep: 9, september: 9, oct: 10, october: 10, nov: 11, november: 11,
  dec: 12, december: 12,
};

const SECONDS: Unit = { name: "Seconds", min: 0, max: 59, unitLabel: "Seconds" };
const MINUTES: Unit = { name: "Minutes", min: 0, max: 59, unitLabel: "Minutes" };
const HOURS: Unit = { name: "Hours", min: 0, max: 23, unitLabel: "Hours" };
const DAYS_OF_MONTH: Unit = { name: "Days of Month", min: 1, max: 31, unitLabel: "Days of Month" };
const MONTHS_UNIT: Unit = {
  name: "Months", min: 1, max: 12, unitLabel: "Months",
  names: (name) => MONTHS[name.toLowerCase()],
};
const DAYS_OF_WEEK: Unit = {
  name: "Days of Week", min: 1, max: 7, unitLabel: "Days of Week",
  names: (name) => DAYS[name.toLowerCase()],
};
const YEARS: Unit = { name: "Years", min: 1970, max: 2100, unitLabel: "Years" };

class ExternalError extends Error {}

type Specifier =
  | { kind: "all" }
  | { kind: "point"; value: number }
  | { kind: "range"; start: number; end: number }
  | { kind: "namedRange"; start: string; end: string };

type RootSpecifier =
  | { kind: "specifier"; specifier: Specifier }
  | { kind: "period"; start: Specifier; step: number }
  | { kind: "namedPoint"; name: string };

/** One parsed field: the set of allowed values (indexed by value). */
export type FieldSet = readonly boolean[];

export interface CronFields {
  readonly seconds: FieldSet;
  readonly minutes: FieldSet;
  readonly hours: FieldSet;
  readonly daysOfMonth: FieldSet;
  readonly months: FieldSet;
  readonly daysOfWeek: FieldSet;
  readonly years: FieldSet;
}

const isSpace = (c: string | undefined) => c === " " || c === "\t" || c === "\n" || c === "\r";
const isDigit = (c: string | undefined) => c !== undefined && c >= "0" && c <= "9";
const isAlpha = (c: string | undefined) => c !== undefined && ((c >= "a" && c <= "z") || (c >= "A" && c <= "Z"));

/** A backtracking parse failure at `offset` (winnow's ErrMode::Backtrack). */
class Backtrack {
  constructor(readonly offset: number, readonly message = "") {}
}

class Parser {
  pos = 0;
  constructor(readonly input: string) {}

  private spaces(): void {
    while (isSpace(this.input[this.pos])) this.pos++;
  }

  private ordinal(): number {
    const start = this.pos;
    this.spaces();
    const digits = this.pos;
    while (isDigit(this.input[this.pos])) this.pos++;
    if (this.pos === digits) {
      this.pos = start;
      throw new Backtrack(digits);
    }
    const text = this.input.slice(digits, this.pos);
    this.spaces();
    const value = Number(text);
    // u32::from_str: digits beyond u32 fail the parse.
    if (!Number.isSafeInteger(value) || value > 0xffff_ffff) {
      this.pos = start;
      throw new Backtrack(digits);
    }
    return value;
  }

  private name(): string {
    const start = this.pos;
    this.spaces();
    const letters = this.pos;
    while (isAlpha(this.input[this.pos])) this.pos++;
    if (this.pos === letters) {
      this.pos = start;
      throw new Backtrack(letters);
    }
    const text = this.input.slice(letters, this.pos);
    this.spaces();
    return text;
  }

  private literal(text: string): void {
    if (!this.input.startsWith(text, this.pos)) throw new Backtrack(this.pos);
    this.pos += text.length;
  }

  private attempt<T>(...alternatives: (() => T)[]): T {
    const start = this.pos;
    let last: Backtrack | undefined;
    for (const alternative of alternatives) {
      try {
        return alternative();
      } catch (error) {
        if (!(error instanceof Backtrack)) throw error;
        last = error;
        this.pos = start;
      }
    }
    throw last ?? new Backtrack(start);
  }

  private specifier(withAny: boolean): Specifier {
    const base: (() => Specifier)[] = [
      () => { this.literal("*"); return { kind: "all" }; },
      () => {
        const start = this.ordinal();
        this.literal("-");
        const end = this.ordinal();
        return { kind: "range", start, end };
      },
      () => ({ kind: "point", value: this.ordinal() }),
      () => {
        const start = this.name();
        this.literal("-");
        const end = this.name();
        return { kind: "namedRange", start, end };
      },
    ];
    return this.attempt(...(withAny ? [() => { this.literal("?"); return { kind: "all" } as Specifier; }, ...base] : base));
  }

  private rootSpecifier(withAny: boolean): RootSpecifier {
    return this.attempt<RootSpecifier>(
      () => {
        const start = this.specifier(withAny);
        this.literal("/");
        const step = this.ordinal();
        return { kind: "period", start, step };
      },
      () => ({ kind: "specifier", specifier: this.specifier(withAny) }),
      () => ({ kind: "namedPoint", name: this.name() }),
    );
  }

  /** `delimited(multispace0, separated(1.., root_specifier, ","), multispace0)`. */
  private field(withAny: boolean): RootSpecifier[] {
    const start = this.pos;
    this.spaces();
    let specifiers: RootSpecifier[];
    try {
      specifiers = [this.rootSpecifier(withAny)];
    } catch (error) {
      this.pos = start;
      throw error;
    }
    for (;;) {
      const before = this.pos;
      if (this.input[this.pos] !== ",") break;
      this.pos++;
      try {
        specifiers.push(this.rootSpecifier(withAny));
      } catch (error) {
        if (!(error instanceof Backtrack)) throw error;
        this.pos = before;
        break;
      }
    }
    this.spaces();
    return specifiers;
  }

  /** `field.try_map(Unit::from_field)`: a failed conversion backtracks to the field start with its message. */
  private unitField(unit: Unit, withAny: boolean): FieldSet {
    const start = this.pos;
    const specifiers = this.field(withAny);
    try {
      return fromField(unit, specifiers);
    } catch (error) {
      if (!(error instanceof ExternalError)) throw error;
      this.pos = start;
      throw new Backtrack(start, error.message);
    }
  }

  private shorthand(): CronFields {
    this.spaces();
    const keywords: [string, () => CronFields][] = [
      ["@yearly", () => fields([0], [0], [0], [1], [1], undefined, undefined)],
      ["@monthly", () => fields([0], [0], [0], [1], undefined, undefined, undefined)],
      ["@weekly", () => fields([0], [0], [0], undefined, undefined, [1], undefined)],
      ["@daily", () => fields([0], [0], [0], undefined, undefined, undefined, undefined)],
      ["@hourly", () => fields([0], [0], undefined, undefined, undefined, undefined, undefined)],
    ];
    for (const [keyword, build] of keywords) {
      if (this.input.startsWith(keyword, this.pos)) {
        this.pos += keyword.length;
        this.spaces();
        return build();
      }
    }
    throw new Backtrack(this.pos);
  }

  private longhand(): CronFields {
    const seconds = this.unitField(SECONDS, false);
    const minutes = this.unitField(MINUTES, false);
    const hours = this.unitField(HOURS, false);
    const daysOfMonth = this.unitField(DAYS_OF_MONTH, true);
    const months = this.unitField(MONTHS_UNIT, false);
    const daysOfWeek = this.unitField(DAYS_OF_WEEK, true);
    let years: FieldSet;
    const beforeYears = this.pos;
    try {
      years = this.unitField(YEARS, false);
    } catch (error) {
      if (!(error instanceof Backtrack)) throw error;
      this.pos = beforeYears;
      years = allOf(YEARS);
    }
    if (this.pos !== this.input.length) throw new Backtrack(this.pos);
    return { seconds, minutes, hours, daysOfMonth, months, daysOfWeek, years };
  }

  schedule(): CronFields {
    const result = this.attempt(() => this.shorthand(), () => this.longhand());
    // Parser::parse requires the whole input.
    if (this.pos !== this.input.length) throw new Backtrack(this.pos);
    return result;
  }
}

function allOf(unit: Unit): boolean[] {
  const set: boolean[] = [];
  for (let value = unit.min; value <= unit.max; value++) set[value] = true;
  return set;
}

function only(values: readonly number[]): boolean[] {
  const set: boolean[] = [];
  for (const value of values) set[value] = true;
  return set;
}

function fields(
  seconds: number[] | undefined, minutes: number[] | undefined, hours: number[] | undefined, daysOfMonth: number[] | undefined,
  months: number[] | undefined, daysOfWeek: number[] | undefined, years: number[] | undefined,
): CronFields {
  return {
    seconds: seconds ? only(seconds) : allOf(SECONDS),
    minutes: minutes ? only(minutes) : allOf(MINUTES),
    hours: hours ? only(hours) : allOf(HOURS),
    daysOfMonth: daysOfMonth ? only(daysOfMonth) : allOf(DAYS_OF_MONTH),
    months: months ? only(months) : allOf(MONTHS_UNIT),
    daysOfWeek: daysOfWeek ? only(daysOfWeek) : allOf(DAYS_OF_WEEK),
    years: years ? only(years) : allOf(YEARS),
  };
}

function validate(unit: Unit, value: number): number {
  if (value < unit.min) throw new ExternalError(`${unit.name} must be greater than or equal to ${unit.min}. ('${value}' specified.)`);
  if (value > unit.max) throw new ExternalError(`${unit.name} must be less than ${unit.max}. ('${value}' specified.)`);
  return value;
}

function ordinalFromName(unit: Unit, name: string): number {
  if (!unit.names) throw new ExternalError(`The '${unit.unitLabel}' field does not support using names. '${name}' specified.`);
  const value = unit.names(name);
  if (value === undefined) {
    throw new ExternalError(unit === DAYS_OF_WEEK ? `'${name}' is not a valid day of the week.` : `'${name}' is not a valid month name.`);
  }
  return value;
}

function inRange(unit: Unit, value: number): boolean {
  return value >= unit.min && value <= unit.max;
}

function fromSpecifier(unit: Unit, specifier: Specifier): number[] {
  switch (specifier.kind) {
    case "all": {
      const values: number[] = [];
      for (let value = unit.min; value <= unit.max; value++) values.push(value);
      return values;
    }
    case "point":
      return [specifier.value];
    case "range": {
      if (!inRange(unit, specifier.start) || !inRange(unit, specifier.end) || specifier.start > specifier.end) {
        throw new ExternalError(`Invalid range for ${unit.name}: ${specifier.start}-${specifier.end}`);
      }
      const values: number[] = [];
      for (let value = specifier.start; value <= specifier.end; value++) values.push(value);
      return values;
    }
    case "namedRange": {
      const start = ordinalFromName(unit, specifier.start);
      const end = ordinalFromName(unit, specifier.end);
      if (!inRange(unit, start) || !inRange(unit, end) || start > end) {
        throw new ExternalError(`Invalid named range for ${unit.name}: ${specifier.start}-${specifier.end}`);
      }
      const values: number[] = [];
      for (let value = start; value <= end; value++) values.push(value);
      return values;
    }
  }
}

function fromRootSpecifier(unit: Unit, root: RootSpecifier): number[] {
  switch (root.kind) {
    case "specifier":
      return fromSpecifier(unit, root.specifier);
    case "namedPoint":
      return [ordinalFromName(unit, root.name)];
    case "period": {
      if (root.step === 0) throw new ExternalError("range step cannot be zero");
      if (root.step < 1 || root.step > unit.max) {
        throw new ExternalError(`${unit.name} must be between 1 and ${unit.max}. ('${root.step}' specified.)`);
      }
      let base: number[];
      if (root.start.kind === "point") {
        const start = validate(unit, root.start.value);
        base = [];
        for (let value = start; value <= unit.max; value++) base.push(value);
      } else {
        // BTreeSet order, then step_by.
        base = [...new Set(fromSpecifier(unit, root.start))].sort((a, b) => a - b);
      }
      return base.filter((_, index) => index % root.step === 0);
    }
  }
}

function fromField(unit: Unit, specifiers: readonly RootSpecifier[]): FieldSet {
  const first = specifiers[0];
  if (specifiers.length === 1 && first?.kind === "specifier" && first.specifier.kind === "all") return allOf(unit);
  const set: boolean[] = [];
  for (const specifier of specifiers) {
    for (const value of fromRootSpecifier(unit, specifier)) set[validate(unit, value)] = true;
  }
  return set;
}

/** RT/scheduler.rs parse_schedule: trim, and prefix "0 " to a five-field expression. */
export function normalizeExpression(expression: string): string {
  const trimmed = expression.trim();
  const fieldCount = trimmed.split(/\s+/).filter((part) => part.length > 0).length;
  return fieldCount === 5 ? `0 ${trimmed}` : trimmed;
}

/** Parses an already normalized expression, or throws CronSyntaxError with the crate's text. */
export function parseCronFields(input: string): CronFields {
  const parser = new Parser(input);
  try {
    return parser.schedule();
  } catch (error) {
    if (error instanceof Backtrack) throw new CronSyntaxError(input, error.offset, error.message);
    throw error;
  }
}

const SECOND = 1_000;
const MIN_TIME = Date.UTC(1970, 0, 1);

/** A parsed schedule: occurrences in UTC whole seconds (Unix ms). */
export class CronSchedule {
  constructor(readonly source: string, readonly fields: CronFields) {}

  static parse(input: string): CronSchedule {
    return new CronSchedule(input, parseCronFields(input));
  }

  /** `schedule.after(t).next()`: the first occurrence strictly after `afterMs` (cron resolution is one second). */
  nextAfter(afterMs: number): number | null {
    return this.firstAtOrAfter(Math.floor(afterMs / SECOND) * SECOND + SECOND);
  }

  /** RT/scheduler.rs latest_occurrence_at_or_before: the latest occurrence `<= atMs`. */
  latestAtOrBefore(atMs: number): number | null {
    return this.lastAtOrBefore(Math.floor(atMs / SECOND) * SECOND);
  }

  includes(atMs: number): boolean {
    const d = new Date(atMs);
    const f = this.fields;
    return atMs % SECOND === 0
      && f.years[d.getUTCFullYear()] === true && f.months[d.getUTCMonth() + 1] === true
      && f.daysOfMonth[d.getUTCDate()] === true && f.daysOfWeek[d.getUTCDay() + 1] === true
      && f.hours[d.getUTCHours()] === true && f.minutes[d.getUTCMinutes()] === true && f.seconds[d.getUTCSeconds()] === true;
  }

  private firstAtOrAfter(startMs: number): number | null {
    const f = this.fields;
    let t = startMs;
    for (;;) {
      const d = new Date(t);
      const year = d.getUTCFullYear();
      if (year > YEARS.max) return null;
      if (f.years[year] !== true) {
        t = Date.UTC(year + 1, 0, 1);
        continue;
      }
      const month = d.getUTCMonth();
      if (f.months[month + 1] !== true) {
        t = Date.UTC(year, month + 1, 1);
        continue;
      }
      const day = d.getUTCDate();
      if (f.daysOfMonth[day] !== true || f.daysOfWeek[d.getUTCDay() + 1] !== true) {
        t = Date.UTC(year, month, day + 1);
        continue;
      }
      const hour = d.getUTCHours();
      if (f.hours[hour] !== true) {
        t = Date.UTC(year, month, day, hour + 1);
        continue;
      }
      const minute = d.getUTCMinutes();
      if (f.minutes[minute] !== true) {
        t = Date.UTC(year, month, day, hour, minute + 1);
        continue;
      }
      if (f.seconds[d.getUTCSeconds()] !== true) {
        t += SECOND;
        continue;
      }
      return t;
    }
  }

  private lastAtOrBefore(endMs: number): number | null {
    const f = this.fields;
    let t = endMs;
    for (;;) {
      if (t < MIN_TIME) return null;
      const d = new Date(t);
      const year = d.getUTCFullYear();
      if (f.years[year] !== true) {
        t = Date.UTC(year, 0, 1) - SECOND;
        continue;
      }
      const month = d.getUTCMonth();
      if (f.months[month + 1] !== true) {
        t = Date.UTC(year, month, 1) - SECOND;
        continue;
      }
      const day = d.getUTCDate();
      if (f.daysOfMonth[day] !== true || f.daysOfWeek[d.getUTCDay() + 1] !== true) {
        t = Date.UTC(year, month, day) - SECOND;
        continue;
      }
      const hour = d.getUTCHours();
      if (f.hours[hour] !== true) {
        t = Date.UTC(year, month, day, hour) - SECOND;
        continue;
      }
      const minute = d.getUTCMinutes();
      if (f.minutes[minute] !== true) {
        t = Date.UTC(year, month, day, hour, minute) - SECOND;
        continue;
      }
      if (f.seconds[d.getUTCSeconds()] !== true) {
        t -= SECOND;
        continue;
      }
      return t;
    }
  }
}
