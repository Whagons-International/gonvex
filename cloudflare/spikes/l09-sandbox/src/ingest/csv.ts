// Delimited text, streamed: DuckDB's `read_csv_auto` dialect detection
// (delimiter among `,` `|` `;` tab; `"` quotes with `""` escapes; UTF-8 BOM
// dropped) and its strict row width.

const CANDIDATES = [",", "|", ";", "\t"] as const;
/** Characters looked at to choose the delimiter. */
const SNIFF_CHARS = 64 * 1024;

/** Field counts of the first lines under one delimiter, honouring quotes. */
function fieldCounts(text: string, delimiter: string, lines: number): number[] {
  const counts: number[] = [];
  let fields = 1;
  let quoted = false;
  let empty = true;
  for (let i = 0; i < text.length && counts.length < lines; i++) {
    const c = text[i]!;
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') i++;
        else quoted = false;
      }
      continue;
    }
    if (c === '"') { quoted = true; empty = false; }
    else if (c === delimiter) { fields++; empty = false; }
    else if (c === "\n" || c === "\r") {
      if (c === "\r" && text[i + 1] === "\n") i++;
      if (!empty) counts.push(fields);
      fields = 1;
      empty = true;
    } else empty = false;
  }
  return counts;
}

export function sniffDelimiter(head: string, complete: boolean): string {
  let best: string = ",";
  let bestScore = 0;
  for (const delimiter of CANDIDATES) {
    const counts = fieldCounts(head, delimiter, 25);
    // The last line of a truncated head may be cut short.
    const usable = complete ? counts : counts.slice(0, Math.max(1, counts.length - 1));
    if (usable.length === 0) continue;
    const width = usable[0]!;
    if (width < 2 || !usable.every((count) => count === width)) continue;
    const score = width * usable.length;
    if (score > bestScore) { best = delimiter; bestScore = score; }
  }
  return best;
}

/** Streams records out of decoded text chunks. */
export class CsvReader {
  private field = "";
  private record: string[] = [];
  private quoted = false;
  private quoteWasClosed = false;
  private hadQuote = false;
  private pendingCR = false;
  private line = 1;

  constructor(private readonly delimiter: string, private readonly onRecord: (record: (string | null)[], line: number) => void) {}

  push(text: string): void {
    const d = this.delimiter;
    for (let i = 0; i < text.length; i++) {
      const c = text[i]!;
      if (this.pendingCR) {
        this.pendingCR = false;
        if (c === "\n") continue;
      }
      if (this.quoted) {
        if (c === '"') { this.quoted = false; this.quoteWasClosed = true; }
        else { if (c === "\n") this.line++; this.field += c; }
        continue;
      }
      if (c === '"') {
        if (this.quoteWasClosed) this.field += '"'; // "" inside a quoted field
        this.quoted = true;
        this.hadQuote = true;
        this.quoteWasClosed = false;
        continue;
      }
      this.quoteWasClosed = false;
      if (c === d) {
        this.endField();
      } else if (c === "\n" || c === "\r") {
        if (c === "\r") this.pendingCR = true;
        this.endRecord();
        this.line++;
      } else {
        this.field += c;
      }
    }
  }

  private endField(): void {
    // DuckDB reads an empty field as NULL, quoted or not (allow_quoted_nulls).
    this.record.push(this.field);
    this.field = "";
    this.hadQuote = false;
  }

  private endRecord(): void {
    if (this.record.length === 0 && this.field === "" && !this.hadQuote) return; // blank line
    this.endField();
    const record = this.record.map((value) => (value === "" ? null : value));
    this.record = [];
    this.onRecord(record, this.line);
  }

  finish(): void {
    if (this.quoted) throw new Error("unterminated quoted field");
    this.endRecord();
  }
}
