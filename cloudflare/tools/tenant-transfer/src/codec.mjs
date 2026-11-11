// Value codec and content checksum shared by the Node CLI and the Durable Object.
//
// SQLite has five storage classes. JavaScript cannot carry all of them
// exactly (integers above 2^53, REAL values that happen to be integral, invalid
// UTF-8 text), so every read goes through `typedColumn()`, which asks SQLite for
// the storage class and an exact form of the value:
//
//   integer -> decimal TEXT        real -> the double itself
//   text    -> bytes (CAST AS BLOB) blob -> bytes
//   null    -> NULL
//
// Wire form (JSON, compact for the common cases):
//   null                    NULL
//   number (integral, safe)  INTEGER
//   number (fractional)      REAL
//   string                   TEXT (valid UTF-8 only; otherwise {"t": base64})
//   {"i": "<decimal>"}       INTEGER outside +/-2^53
//   {"r": <number>|"Infinity"|"-Infinity"}  REAL that is integral or infinite
//   {"b": "<base64>"}        BLOB
//   {"t": "<base64>"}        TEXT whose bytes are not valid UTF-8
//
// The checksum hashes (storage class, exact bytes) per value, rows in key
// order, so it is identical for the same content whichever SQLite produced it.

const utf8 = new TextDecoder('utf-8', { fatal: true });
const utf8Encoder = new TextEncoder();

/** Quote an SQL identifier. */
export function quoteIdent(name) {
  return `"${String(name).replaceAll('"', '""')}"`;
}

/** Two result columns per source column: storage class, exact value. */
export function typedColumn(expr, i = 0) {
  return `typeof(${expr}) AS t${i}, CASE typeof(${expr}) WHEN 'integer' THEN CAST(${expr} AS TEXT) WHEN 'text' THEN CAST(${expr} AS BLOB) ELSE ${expr} END AS v${i}`;
}

function toBytes(value) {
  if (value instanceof Uint8Array) return value;
  if (value instanceof ArrayBuffer) return new Uint8Array(value);
  if (ArrayBuffer.isView(value)) return new Uint8Array(value.buffer, value.byteOffset, value.byteLength);
  if (typeof value === 'string') return utf8Encoder.encode(value);
  throw new Error(`expected bytes, got ${typeof value}`);
}

function base64(bytes) {
  let s = '';
  for (let i = 0; i < bytes.length; i += 0x8000) s += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(s);
}

function unbase64(text) {
  const s = atob(text);
  const out = new Uint8Array(s.length);
  for (let i = 0; i < s.length; i++) out[i] = s.charCodeAt(i);
  return out;
}

/** (storage class, exact value) from a typedColumn() pair -> wire value. */
export function toWire(type, value) {
  switch (type) {
    case 'null':
      return null;
    case 'integer': {
      const n = Number(value);
      return Number.isSafeInteger(n) && String(n) === String(value) ? n : { i: String(value) };
    }
    case 'real':
      if (!Number.isFinite(value)) return { r: String(value) };
      return Number.isInteger(value) ? { r: value } : value;
    case 'text': {
      const bytes = toBytes(value);
      try {
        return utf8.decode(bytes);
      } catch {
        return { t: base64(bytes) };
      }
    }
    case 'blob':
      return { b: base64(toBytes(value)) };
    default:
      throw new Error(`unknown storage class ${type}`);
  }
}

/**
 * Wire value -> {sql, param}: a placeholder expression and its bound value,
 * chosen so SQLite stores the original storage class. `param` is undefined when
 * the expression is a literal (NULL, infinities).
 */
export function fromWire(value) {
  if (value === null) return { sql: 'NULL' };
  if (typeof value === 'string') return { sql: '?', param: value };
  if (typeof value === 'number') {
    // Integral numbers are INTEGER; fractional ones REAL. Bind the integer as
    // decimal text cast back, so no binding layer can turn it into a double.
    return Number.isInteger(value) ? { sql: 'CAST(? AS INTEGER)', param: String(value) } : { sql: '?', param: value };
  }
  if ('i' in value) return { sql: 'CAST(? AS INTEGER)', param: value.i };
  if ('r' in value) {
    if (value.r === 'Infinity') return { sql: '9e999' };
    if (value.r === '-Infinity') return { sql: '-9e999' };
    return { sql: 'CAST(? AS REAL)', param: String(value.r) };
  }
  if ('b' in value) return { sql: '?', param: unbase64(value.b) };
  if ('t' in value) return { sql: 'CAST(? AS TEXT)', param: unbase64(value.t) };
  throw new Error(`bad wire value ${JSON.stringify(value)}`);
}

/** Approximate encoded size of a wire value, for chunking. */
export function wireSize(value) {
  if (value === null) return 4;
  if (typeof value === 'number') return 24;
  if (typeof value === 'string') return value.length * 3 + 2;
  if ('b' in value) return value.b.length;
  if ('t' in value) return value.t.length;
  return 32;
}

/**
 * Incremental content hash. `createHash` is node:crypto's (also provided by
 * workerd under nodejs_compat). Each value contributes a tag byte, a length
 * and its exact bytes; each row ends with a separator.
 */
export class RowHasher {
  constructor(createHash) {
    this.hash = createHash('sha256');
    this.rows = 0;
    this.scratch = new DataView(new ArrayBuffer(8));
  }

  /** `pairs` is the flat [type0, value0, type1, value1, ...] of typedColumn(). */
  addTyped(pairs) {
    for (let i = 0; i < pairs.length; i += 2) this.addValue(pairs[i], pairs[i + 1]);
    this.hash.update(new Uint8Array([0x1e]));
    this.rows++;
  }

  addValue(type, value) {
    const h = this.hash;
    switch (type) {
      case 'null':
        h.update(new Uint8Array([0x00]));
        return;
      case 'integer':
        this.#bytes(0x01, utf8Encoder.encode(String(value)));
        return;
      case 'real':
        this.scratch.setFloat64(0, value);
        h.update(new Uint8Array([0x02]));
        h.update(new Uint8Array(this.scratch.buffer.slice(0)));
        return;
      case 'text':
        this.#bytes(0x03, toBytes(value));
        return;
      case 'blob':
        this.#bytes(0x04, toBytes(value));
        return;
      default:
        throw new Error(`unknown storage class ${type}`);
    }
  }

  #bytes(tag, bytes) {
    this.scratch.setUint32(0, bytes.length);
    this.hash.update(new Uint8Array([tag]));
    this.hash.update(new Uint8Array(this.scratch.buffer.slice(0, 4)));
    this.hash.update(bytes);
  }

  digest() {
    return { rows: this.rows, sha256: this.hash.digest('hex') };
  }
}

/**
 * Table plan from sqlite_schema + pragma_table_xinfo, identical on every side:
 * which columns to copy, the key to page by, whether to carry the rowid.
 */
export function tablePlan({ name, sql, columns, primaryKey }) {
  const withoutRowid = /\bWITHOUT\s+ROWID\b/i.test(sql ?? '');
  const copyable = columns.filter((c) => c.hidden === 0).map((c) => c.name);
  // A rowid table copies its rowid explicitly unless a column aliases it.
  const rowidAlias =
    !withoutRowid &&
    primaryKey.length === 1 &&
    /^INTEGER$/i.test(columns.find((c) => c.name === primaryKey[0])?.type ?? '');
  const carryRowid = !withoutRowid && !rowidAlias;
  const key = withoutRowid ? primaryKey : [rowidAlias ? primaryKey[0] : 'rowid'];
  const copyColumns = carryRowid ? ['rowid', ...copyable] : copyable;
  return { name, withoutRowid, key, copyColumns };
}

/**
 * SELECT of a plan's copy columns in key order. `after` is a keyset cursor:
 * the wire values of the key columns of the last row already read.
 */
export function selectSql(plan, { after = null, limit = null } = {}) {
  const cols = plan.copyColumns.map((c, i) => typedColumn(c === 'rowid' ? 'rowid' : quoteIdent(c), i)).join(', ');
  const keyExprs = plan.key.map((k) => (k === 'rowid' ? 'rowid' : quoteIdent(k)));
  const params = [];
  let where = '';
  if (after) {
    // Row-value comparison keeps keyset paging exact for composite keys.
    const bound = after.map(fromWire);
    where = ` WHERE (${keyExprs.join(', ')}) > (${bound.map((b) => b.sql).join(', ')})`;
    for (const b of bound) if (b.param !== undefined) params.push(b.param);
  }
  return {
    sql: `SELECT ${cols} FROM ${quoteIdent(plan.name)}${where} ORDER BY ${keyExprs.join(', ')}${limit ? ` LIMIT ${Number(limit)}` : ''}`,
    params,
  };
}

/** Indexes of the key columns inside plan.copyColumns. */
export function keyIndexes(plan) {
  return plan.key.map((k) => {
    const i = plan.copyColumns.indexOf(k);
    if (i < 0) throw new Error(`${plan.name}: key column ${k} is not copied`);
    return i;
  });
}
