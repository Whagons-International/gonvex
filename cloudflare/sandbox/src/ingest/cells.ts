// Cell text as Rust's `csv_cell` writes it (RT/data_ingest.rs:139-151) from
// calamine's `Data` (0.36): floats with Rust's `Display`, booleans as
// true/false, errors by their Debug name, date cells as their serial number.

/** Rust's `f64` Display: the shortest round-trip digits, never in exponent form. */
export function rustFloat(value: number): string {
  if (Number.isNaN(value)) return "NaN";
  if (!Number.isFinite(value)) return value > 0 ? "inf" : "-inf";
  if (Object.is(value, -0)) return "-0";
  const text = String(value);
  const match = /^(-?)(\d)(?:\.(\d+))?e([+-]\d+)$/.exec(text);
  if (!match) return text;
  const [, sign, lead, rest = "", exponentText] = match;
  const digits = lead! + rest;
  const exponent = Number(exponentText);
  if (exponent >= 0) {
    const whole = digits.length - 1 <= exponent ? digits + "0".repeat(exponent - (digits.length - 1)) : `${digits.slice(0, exponent + 1)}.${digits.slice(exponent + 1)}`;
    return sign + whole;
  }
  return `${sign}0.${"0".repeat(-exponent - 1)}${digits}`;
}

/** calamine `CellErrorType` Debug names, by Excel's text and by BIFF code. */
export const ERROR_NAMES: Record<string, string> = {
  "#DIV/0!": "Div0",
  "#N/A": "NA",
  "#NAME?": "Name",
  "#NULL!": "Null",
  "#NUM!": "Num",
  "#REF!": "Ref",
  "#VALUE!": "Value",
  "#GETTING_DATA": "GettingData",
};

export const BIFF_ERRORS: Record<number, string> = {
  0x00: "Null",
  0x07: "Div0",
  0x0f: "Value",
  0x17: "Ref",
  0x1d: "Name",
  0x24: "Num",
  0x2a: "NA",
  0x2b: "GettingData",
};

/** `unique_table_name` (RT/data_ingest.rs:153-171). */
export function sheetTableName(sheet: string, index: number): string {
  let name = [...sheet]
    .map((character) => (/^[A-Za-z0-9_]$/.test(character) ? character.toLowerCase() : "_"))
    .join("")
    .replace(/^_+|_+$/g, "");
  if (name === "" || /^[0-9]/.test(name)) name = `sheet_${index + 1}`;
  return `${name}_${index + 1}`;
}

/** Rust's `{:?}` for a sheet name in an error. */
export const debugString = (value: string): string => JSON.stringify(value);

/** A sheet as calamine's `Range`: cells keyed by (row, column), bounded by the used cells. */
export interface SheetGrid {
  minRow: number;
  maxRow: number;
  minCol: number;
  maxCol: number;
}
