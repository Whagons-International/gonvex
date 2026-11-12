// 0022/0057: gateway caller mapping. Only the actual CPU-reset shape can
// quarantine a causally marked command. Deploy/reset/overload remain distinct.
import type { ClientFrame } from "../session/decode";
import { frames } from "../session/protocol";
import { deadlineOverrunError } from "../host/src/invoke";
export async function tenantDispatchWithDeadline(projectId: string, frame: ClientFrame, dispatch: () => Promise<string[]>,
  recordOverrun: (commandId: string, path: string) => Promise<"quarantined" | "committed" | "unrelated">): Promise<string[]> {
  try { return await dispatch(); }
  catch (error) {
    if (!/exceeded its CPU time limit and was reset/.test(String((error as Error)?.message ?? error)) || (frame.type !== "reducer.call" && frame.type !== "query.call")) throw error;
    if (frame.type === "reducer.call") {
      const outcome = await recordOverrun(frame.id, frame.path);
      // K07 checks the durable key before execution; no unkeyed replay.
      if (outcome === "committed" && frame.idempotencyKey) return dispatch();
    }
    const failure = deadlineOverrunError(projectId, frame.path);
    return [frame.type === "reducer.call" ? frames.reducerError(frame.id, frame.path, failure.message, "transient", frame.trace) : frames.queryError(frame.id, frame.path, failure.message)];
  }
}
