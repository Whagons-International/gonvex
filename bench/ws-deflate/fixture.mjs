export function fixture() {
  const rows = Array.from({ length: 20_000 }, (_, i) => {
    const row = { id: `task-${i}` };
    for (let column = 1; column < 50; column++) {
      const key = `task_column_${column}`;
      row[key] = column % 7 === 0 ? null : column % 7 === 1 ? i + column
        : column % 7 === 2 ? i % 2 === 0 : column % 7 === 3 ? `tenant-status-${i % 8}`
        : column % 7 === 4 ? { label: `Área ${i % 20}`, flags: [true, null, i % 5] }
        : column % 7 === 5 ? 1728000000000 + i : `Task ${i} details ${column}`;
    }
    return row;
  });
  const cursor = { epoch: "whagons-bench-epoch", revision: 123456789 };
  const pages = Array.from({ length: Math.ceil(rows.length / 128) }, (_, sequence) => ({
    type: "replica.snapshotPage", snapshotId: "snapshot-1", id: "tasks", path: "tasks.all", scope: "tenant", visibilityScope: "visible",
    sequence, last: sequence === Math.ceil(rows.length / 128) - 1, key: "id", cursor, result: rows.slice(sequence * 128, (sequence + 1) * 128),
    ...(sequence === Math.ceil(rows.length / 128) - 1 ? { digest: "xor1:benchmark" } : {}),
  }));
  const transaction = { type: "replica.transaction", cursor, originCommandId: "command-1", changes: rows.slice(0, 1000).map(row => ({ entity: "tasks", id: row.id, operation: "update", newValue: row, changedColumns: ["task_column_1"] })) };
  const delta = { type: "replica.delta", id: "tasks", cursor, upserts: [rows[0]], deleted: [], originCommandIds: ["command-1"] };
  return { snapshot: pages, transaction: [transaction], delta: [delta] };
}
