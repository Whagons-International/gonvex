export const schema = {
  tasks: {
    key: "_id",
    fields: {
      _id: "text",
      name: "text",
      workspace: "text",
      due: "number",
      version: "number",
      description: "text",
    },
    search: ["name", "description"],
    indexes: [
      { columns: ["workspace", "due"] },
      { columns: ["due"] },
      { columns: ["name"] },
    ],
  },
};
// Same synthetic row content as sqlite-replica/fixture.mjs on 24cdc5e.
export function row(id) {
  return {
    _id: String(id),
    name: `Task ${String(id).padStart(8, "0")} café 東京`,
    workspace: String(id % 10),
    due: id % 365,
    version: 1,
    description:
      "Synthetic maintenance procedure. ".repeat(15) +
      (id % 997 === 0 ? "rare-needle 🔧" : ""),
  };
}
export function* records(count) {
  for (let id = 0; id < count; id++)
    yield { table: "tasks", id: String(id), row: row(id) };
}
export const plan = {
  table: "tasks",
  where: {
    op: "and",
    children: [
      { field: "workspace", op: "eq", value: "0" },
      { field: "due", op: "gte", value: 30 },
    ],
  },
  order: [
    { field: "due", direction: "desc" },
    { field: "name", direction: "asc" },
  ],
  textOrder: "binary",
  search: "rare-needle",
  limit: 50,
};
