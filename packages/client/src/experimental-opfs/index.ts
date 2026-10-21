/** Experimental database API. The default client/storage exports are unchanged. */
export { OPFSReplicaEngine } from "./replica.js";
export { ReplicaDatabase } from "./database.js";
export { opfsFile } from "./file.js";
export { openOPFSReplicaWorker } from "./worker-client.js";
export type { ReplicaFile, SyncHandle } from "./file.js";
export type {
  ReplicaSchema,
  IncomingRow,
  RowChange,
  MetadataChange,
  Row,
  Json,
} from "./replica.js";
export type {
  PagePlan,
  PageCursor,
  PageBudget,
  Page,
  Predicate,
  Order,
} from "./query.js";
