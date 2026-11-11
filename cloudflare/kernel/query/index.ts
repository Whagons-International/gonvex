// Kernel Queries and Live Queries (package K08). See README.md.
import type { ClientFrame } from "../session/decode";
import type { SocketView } from "../session/ports";
import { callQuery, type CallPorts } from "./call";
import { LiveQueries } from "./live";

export { callQuery, REDUCER_RECEIPT_QUERY, type CallOutcome, type CallPorts } from "./call";
export { compilePlan, jsonObjectExpr, jsonValueExpr, type CompiledPlan } from "./compile";
export { executePlan, readClock, shapeResult, RESULT_LIMIT_BYTES, type ExecutionContext, type ExecutionStats, type Snapshot } from "./execute";
export { readTransaction, type FeedEvent } from "./feed";
export { callResult, liveResult, liveResultTail, progress, queryError, windowRevision } from "./frames";
export { JsonNumber, JsonObject, parseJson, ryu, rustDebug, stringify, type JsonTree } from "./json";
export {
  LiveQueries, snapshotRunner, SUBSCRIBE_MANY_ERROR, SUBSCRIBE_MANY_LIMIT,
  type FeedStats, type LiveQueryOptions, type SnapshotRunner, type SubscribeRequest,
} from "./live";
export { moduleFromManifest, type Manifest, type QueryFunction, type QueryModule } from "./module";
export { fitParameters } from "./parameters";
export { LiveQueryError, validatePlan, type LiveQueryPlan } from "./plan";
export { kernelVisibility, type LogChange, type QueryVisibility, type ResolvedVisibility, type VisibilityContext, type VisibilityPlan } from "./visibility";

/**
 * The tenant port for `query.*` frames with tenant scope (K05 forwards
 * them one by one; it splits `query.subscribeMany` and applies its limit).
 * Commits reach the live queries through `live.applyFeed`.
 */
export class QueryEngine {
  readonly live: LiveQueries;

  constructor(readonly ports: CallPorts) {
    this.live = new LiveQueries(ports);
  }

  /** Frames to send for a `query.*` frame, or undefined for any other frame. */
  async dispatch(socket: SocketView, frame: ClientFrame): Promise<string[] | undefined> {
    switch (frame.type) {
      case "query.call":
        return [await callQuery(this.ports, socket.tenant, frame.id, frame.path, frame.args)];
      case "query.subscribe":
        return [await this.live.subscribe(socket.connectionId, socket.tenant, frame)];
      case "query.subscribeMany":
        return this.live.subscribeMany(socket.connectionId, socket.tenant, frame.subscribes);
      case "query.unsubscribe":
        this.live.unsubscribe(socket.connectionId, frame.id);
        return [];
      default:
        return undefined;
    }
  }
}
