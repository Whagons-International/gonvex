import {
  compileDataRead,
  orderedDataRead,
  type DataRead,
  type JsonObject,
} from "@gonvex/module-sdk";
import {
  memoryReadView,
  primaryReadKeys,
  readIsCovered,
  executionReadColumns,
  projectDataRows,
  type ReadCoverage,
} from "@gonvex/local-runtime/read-view";
import type { ReducerReadView } from "@gonvex/local-runtime/portable";

export type ReplicaReadPage = { id: string; row: JsonObject }[];
export type ReplicaReadRequest = DataRead;
export type ReplicaReadCoverage = ReadCoverage;
export type ReplicaReducerReadView = ReducerReadView;

/** Storage adapters supply bounded, primary-key ordered pages in one read
 * transaction. Only rows needed by this reducer cross the native bridge. */
export function pagedReplicaReadView(
  coverage: ReadCoverage,
  page: (read: DataRead, after: string | undefined) => Promise<ReplicaReadPage>,
): ReducerReadView {
  return {
    async select(input, excludedRowIds = []) {
      const read = orderedDataRead({
        ...input,
        key: input.key ?? coverage[input.table]?.key,
      });
      compileDataRead(read);
      if (read.limit === 0) return { rows: [], complete: true };
      const known = coverage[read.table];
      let after: string | undefined,
        fieldsComplete = true;
      const foundKeys = new Set<string>();
      let rows: JsonObject[] = [];
      const primaryKeys = primaryReadKeys(read, known?.key ?? '_id');
      for (;;) {
        const entries = await page(read, after);
        if (!entries.length) break;
        const last = entries.at(-1)!.id;
        if (last === after)
          throw new Error("Replica read page did not advance");
        after = last;
        for (const { id } of entries) if (primaryKeys?.includes(id)) foundKeys.add(id);
        const view = memoryReadView(
          new Map([
            [read.table, new Map(entries.map(({ id, row }) => [id, row]))],
          ]),
          {
            ...coverage,
            [read.table]: {
              ...known,
              key: known?.key ?? "_id",
              complete: true,
            },
          },
        );
        // Validate only the fields requested by the reducer, while preserving
        // sort keys until all pages have been combined.
        const selected = await view.select(read, excludedRowIds);
        fieldsComplete &&= selected.complete;
        const unprojected = await view.select({ ...read, columns: executionReadColumns(read, known?.key ?? '_id') }, excludedRowIds);
        rows = projectDataRows([...rows, ...unprojected.rows], {
          ...read,
          columns: undefined,
        });
        if (primaryKeys?.every(id => excludedRowIds.includes(id) || foundKeys.has(id))) break;
      }
      return {
        rows: projectDataRows(rows, read),
        complete: fieldsComplete && (readIsCovered(known, read) || (primaryKeys !== undefined && primaryKeys.every(id => excludedRowIds.includes(id) || foundKeys.has(id)))),
      };
    },
  };
}
