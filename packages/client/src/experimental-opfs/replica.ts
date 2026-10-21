import {
  assert,
  decode,
  encode,
  hex,
  unhex,
  prefixEnd,
  VALUE_BYTES,
  KEY_BYTES,
} from "./codec.js";
import { ReplicaDatabase, type KeyValue } from "./database.js";
import type { ReplicaFile } from "./file.js";
import { ExternalSort } from "./sort.js";
import { runPage, type PagePlan, type PageBudget } from "./query.js";
import {
  canonical,
  rowPrefix,
  rowKey,
  dirtyPrefix,
  gramPrefix,
  trigrams,
  indexPrefix,
  scalarKey,
  pointerBytes,
  readPointer,
} from "./keys.js";
import { postingGroups } from "./postings.js";
import { validJSON, type JsonValue } from "./codec.js";

export type Json = JsonValue;
export type Row = Record<string, Json>;
export type FieldKind = "text" | "number" | "boolean" | "json";
export type ReplicaSchema = Record<
  string,
  {
    key: string;
    fields: Record<string, FieldKind>;
    search?: readonly string[];
    indexes?: readonly { columns: readonly string[] }[];
  }
>;
export type IncomingRow = { table: string; id: string; row: Row };
export type RowChange = Omit<IncomingRow, "row"> & { row: Row | null };
export type MetadataChange = { key: string; value: Json | undefined };
const own = (object: object, key: string) => Object.hasOwn(object, key);

/** Database/query prototype, not an independent source of authorization. The
 * generated SDK must admit schema, collection membership, scope and coverage
 * before invoking it. There is no HTTP, socket, UI state or second row cache. */
export class OPFSReplicaEngine {
  readonly database: ReplicaDatabase;
  readonly schema: ReplicaSchema;
  private activeSnapshot = false;
  constructor(
    file: ReplicaFile,
    readonly scope: string,
    schema: ReplicaSchema,
  ) {
    assert(
      Object.keys(schema).length > 0 &&
        encode(canonical(schema)).length <= VALUE_BYTES,
      "Schema size",
    );
    for (const [name, table] of Object.entries(schema)) {
      assert(
        name.length > 0 &&
          !name.includes("\0") &&
          decode(encode(name)) === name &&
          encode(name).length <= 128 &&
          Object.keys(table.fields).length <= 256 &&
          own(table.fields, table.key) &&
          table.fields[table.key] === "text",
        "Invalid table schema",
      );
      for (const [field, kind] of Object.entries(table.fields))
        assert(
          field.length > 0 &&
            field.length <= 256 &&
            ["text", "number", "boolean", "json"].includes(kind),
          "Invalid field schema",
        );
      for (const field of table.search ?? [])
        assert(
          own(table.fields, field) && table.fields[field] === "text",
          "Search requires a text column",
        );
      assert((table.indexes?.length ?? 0) <= 64, "Index definition budget");
      for (const index of table.indexes ?? [])
        assert(
          index.columns.length > 0 &&
            index.columns.length <= 8 &&
            index.columns.every(
              (field) =>
                own(table.fields, field) && table.fields[field] !== "json",
            ),
          "Invalid index columns",
        );
    }
    this.schema = JSON.parse(canonical(schema));
    for (const table of Object.values(this.schema)) {
      Object.freeze(table.fields);
      if (table.search) Object.freeze(table.search);
      if (table.indexes) {
        for (const index of table.indexes) {
          Object.freeze(index.columns);
          Object.freeze(index);
        }
        Object.freeze(table.indexes);
      }
      Object.freeze(table);
    }
    Object.freeze(this.schema);
    this.database = new ReplicaDatabase(file, scope);
    const expected = encode(canonical(this.schema)),
      previous = this.database.get("m/schema");
    if (previous)
      assert(
        decode(previous) === decode(expected),
        "Schema changed; explicit rebuild required",
      );
    else {
      const tx = this.database.begin();
      try {
        tx.put("m/schema", expected);
        tx.commit();
      } catch (error) {
        tx.abort();
        throw error;
      }
    }
  }
  private identity(table: string, id: string) {
    assert(
      own(this.schema, table) &&
        id.length > 0 &&
        encode(id).length <= 256 &&
        decode(encode(id)) === id,
      "Unknown table or invalid row identity",
    );
  }
  validate(record: IncomingRow): Uint8Array {
    this.identity(record.table, record.id);
    validJSON(record.row);
    assert(
      record.row &&
        !Array.isArray(record.row) &&
        typeof record.row === "object",
      "Row must be an object",
    );
    const table = this.schema[record.table]!;
    assert(
      own(record.row, table.key) && record.row[table.key] === record.id,
      "Row identity mismatch",
    );
    for (const [field, value] of Object.entries(record.row)) {
      assert(own(table.fields, field), "Undeclared row field");
      const kind = table.fields[field]!;
      assert(
        value === null ||
          kind === "json" ||
          typeof value === (kind === "text" ? "string" : kind),
        "Row field type mismatch",
      );
    }
    const bytes = encode(JSON.stringify(record.row));
    assert(bytes.length <= VALUE_BYTES, "Row byte budget");
    return bytes;
  }
  entity(table: string, id: string): Row | undefined {
    this.identity(table, id);
    const bytes = this.database.get(rowKey(table, id));
    return bytes ? (JSON.parse(decode(bytes)) as Row) : undefined;
  }
  metadata(key: string): Json | undefined {
    assert(
      key.startsWith("m/") || key.startsWith("o/"),
      "Invalid metadata namespace",
    );
    const bytes = this.database.get(key);
    return bytes ? (JSON.parse(decode(bytes)) as Json) : undefined;
  }
  /** The immutable receipt is also the pending intent record. Retrying after
   * an unknown response returns the original sequence without applying twice. */
  acceptIntent(id: string, payload: Json, changes: readonly RowChange[]) {
    assert(id.length > 0 && encode(id).length <= 128, "Intent identity");
    validJSON(payload);
    const identity = canonical(payload),
      key = "o/intent/" + hex(id);
    assert(encode(identity).length <= 65536, "Intent byte budget");
    const previous = this.metadata(key) as
      | { identity: string; sequence: number }
      | undefined;
    if (previous) {
      assert(
        previous.identity === identity,
        "Intent identity reused with different payload",
      );
      return { sequence: previous.sequence, duplicate: true };
    }
    const sequence = this.database.sequence + 1;
    this.apply(changes, [
      { key, value: { identity, sequence, state: "pending" } },
    ]);
    return { sequence, duplicate: false };
  }
  /** One durable root for business rows, source cursor/coverage and queued
   * intent. The SDK retains per-field authority/optimistic replay semantics. */
  apply(
    changes: readonly RowChange[],
    metadata: readonly MetadataChange[] = [],
  ): number {
    assert(
      !this.activeSnapshot && changes.length + metadata.length <= 512,
      "Write transaction entry budget",
    );
    let bytes = 0;
    const admitted = changes.map((change) => {
      this.identity(change.table, change.id);
      const body =
        change.row === null
          ? undefined
          : this.validate({ ...change, row: change.row });
      bytes += (body?.length ?? 0) + encode(change.id).length + 128;
      assert(bytes <= 1048576, "Write transaction byte budget");
      return { ...change, body };
    });
    const meta = metadata.map((item) => {
      assert(
        (item.key.startsWith("m/") || item.key.startsWith("o/")) &&
          item.key !== "m/schema" &&
          item.key !== "m/indexes",
        "Invalid metadata write",
      );
      let value: Uint8Array | undefined;
      if (item.value !== undefined) {
        validJSON(item.value);
        value = encode(JSON.stringify(item.value));
      }
      bytes += item.key.length * 2 + (value?.length ?? 0) + 96;
      assert(bytes <= 1048576, "Write transaction byte budget");
      return { key: item.key, value };
    });
    const tx = this.database.begin();
    try {
      for (const row of admitted) {
        if (row.body) tx.put(rowKey(row.table, row.id), row.body);
        else tx.delete(rowKey(row.table, row.id));
        tx.put(dirtyPrefix(row.table) + hex(row.id), new Uint8Array());
      }
      for (const item of meta) {
        if (item.value) tx.put(item.key, item.value);
        else tx.delete(item.key);
      }
      return tx.commit();
    } catch (error) {
      tx.abort();
      throw error;
    }
  }
  /** Streaming snapshot staging. Old committed reads stay available; another
   * durable writer is refused until commit/abort. No partial coverage escapes. */
  snapshot(scratch: ReplicaFile, budget?: number) {
    assert(!this.activeSnapshot, "Snapshot already active");
    const sorter = new ExternalSort(scratch, budget);
    this.activeSnapshot = true;
    try {
      for (const item of this.database.range("m/", "p/"))
        if (item.key !== "m/indexes") sorter.push(item.key, item.value);
    } catch (error) {
      this.activeSnapshot = false;
      sorter.close();
      throw error;
    }
    let transaction: ReturnType<ReplicaDatabase["begin"]>;
    try {
      transaction = this.database.begin();
    } catch (error) {
      this.activeSnapshot = false;
      sorter.close();
      throw error;
    }
    let closed = false,
      ordinal = 0,
      postingBytes = 0;
    const indexState = Object.fromEntries(
      Object.entries(this.schema).map(([table, schema]) => [
        table,
        { rows: 0, valid: (schema.indexes ?? []).map(() => true) },
      ]),
    );
    const block = new Map<string, Uint8Array>(),
      textCache = new Map<string, Set<string>>(),
      prefixCache = new Map<string, string>();
    const flush = () => {
      for (const [key, bitmap] of block) sorter.push(key, bitmap);
      block.clear();
      postingBytes = 0;
    };
    const prefix = (table: string, gram: string) => {
      const key = table + "\0" + gram;
      let value = prefixCache.get(key);
      if (!value) {
        value = gramPrefix(table, gram);
        if (prefixCache.size >= 2048)
          prefixCache.delete(prefixCache.keys().next().value!);
        prefixCache.set(key, value);
      }
      return value;
    };
    const close = () => {
      if (!closed) {
        closed = true;
        transaction.abort();
        this.activeSnapshot = false;
        sorter.close();
      }
    };
    return {
      add: (record: IncomingRow) => {
        assert(!closed, "Snapshot closed");
        try {
          const key = rowKey(record.table, record.id),
            pointer = pointerBytes(
              transaction.stageValue(this.validate(record)),
            );
          sorter.push(key, pointer);
          indexState[record.table]!.rows++;
          for (const [number, index] of (
            this.schema[record.table]!.indexes ?? []
          ).entries()) {
            const indexed =
              indexPrefix(record.table, number) +
              index.columns
                .map((field) => scalarKey(record.row[field]))
                .join("") +
              hex(record.id);
            // A long indexed value falls back to exact scans for this index,
            // rather than silently excluding the row from indexed queries.
            if (indexed.length <= KEY_BYTES) sorter.push(indexed, pointer);
            else indexState[record.table]!.valid[number] = false;
          }
          sorter.push("u/" + ordinal.toString(16).padStart(14, "0"), pointer);
          const grams = new Set<string>();
          for (const field of this.schema[record.table]!.search ?? []) {
            const value = record.row[field];
            if (typeof value !== "string") continue;
            let cached = value.length >= 64 ? textCache.get(value) : undefined;
            if (!cached) {
              cached = new Set<string>();
              for (const gram of trigrams(value.toLowerCase())) {
                cached.add(gram);
                assert(cached.size <= 16384, "Per-row trigram budget");
              }
              if (value.length >= 64 && value.length <= 4096) {
                if (textCache.size >= 16)
                  textCache.delete(textCache.keys().next().value!);
                textCache.set(value, cached);
              }
            }
            for (const gram of cached) {
              grams.add(gram);
              assert(grams.size <= 16384, "Per-row trigram budget");
            }
          }
          const bucket = Math.floor(ordinal / 256)
              .toString(16)
              .padStart(14, "0"),
            bit = ordinal % 256;
          for (const gram of grams) {
            const key = prefix(record.table, gram) + bucket;
            let bitmap = block.get(key);
            if (!bitmap) {
              if (postingBytes + key.length * 2 + 160 > 1048576) flush();
              bitmap = new Uint8Array(32);
              block.set(key, bitmap);
              postingBytes += key.length * 2 + 160;
            }
            bitmap[bit >>> 3] = bitmap[bit >>> 3]! | (1 << (bit & 7));
          }
          ordinal++;
          if (ordinal % 256 === 0) flush();
        } catch (error) {
          close();
          throw error;
        }
      },
      commit: () => {
        assert(!closed, "Snapshot closed");
        try {
          flush();
          sorter.push("m/indexes", encode(JSON.stringify(indexState)));
          function* references(rows: Iterable<KeyValue>): Generator<KeyValue> {
            for (const item of rows)
              yield item.key.startsWith("r/")
                ? {
                    key: item.key,
                    value: new Uint8Array(),
                    pointer: readPointer(item.value),
                  }
                : item;
          }
          transaction.bulkReplace(references(postingGroups(sorter.finish())));
          const sequence = transaction.commit();
          close();
          return sequence;
        } catch (error) {
          close();
          throw error;
        }
      },
      abort: close,
    };
  }
  page(plan: PagePlan, budget?: PageBudget) {
    return runPage(this, plan, budget);
  }
  close() {
    assert(!this.activeSnapshot, "Snapshot still open");
    this.database.close();
  }
}
