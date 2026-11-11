// Tenant RPCs across a deploy (0016 G0-8; S4 "Deploys"): for 10-40 s after a
// deploy a busy tenant object can still run the previous version. A method it
// does not know answers "RPC receiver does not implement the method", and an
// object restarting for the new version answers that it was reset. Both are
// transient: the caller retries (a Workflow step retry, or the next renewal).

const TRANSIENT = /does not implement the method|RPC receiver|reset because its code was updated|Durable Object reset|is overloaded|Network connection lost|storage operation exceeded timeout|Internal error/i;

export class TransientRpcError extends Error {
  readonly transient = true;
  constructor(readonly method: string, cause: unknown) {
    super(`tenant ${method} failed transiently: ${String((cause as Error)?.message ?? cause)}`);
  }
}

export const isTransientRpcError = (error: unknown): boolean =>
  error instanceof TransientRpcError || TRANSIENT.test(String((error as Error)?.message ?? error)) || (error as { retryable?: unknown } | null)?.retryable === true;

/** Calls one tenant method; a deploy-skew or reset failure becomes a `TransientRpcError`. */
export async function tenantRpc<T>(method: string, call: () => Promise<T>): Promise<T> {
  try {
    return await call();
  } catch (error) {
    if (isTransientRpcError(error)) throw new TransientRpcError(method, error);
    throw error;
  }
}
