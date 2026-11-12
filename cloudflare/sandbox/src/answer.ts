// Durable Object methods answer `{ok, value}` or `{ok: false, error}` instead
// of throwing: an exception thrown across RPC is logged by the platform as an
// uncaught exception of the object, and a sandbox error is an ordinary answer.

export type Answer<T> = { ok: true; value: T } | { ok: false; error: string };

export async function answer<T>(work: () => Promise<T> | T): Promise<Answer<T>> {
  try {
    return { ok: true, value: await work() };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error) };
  }
}

export function unwrap<T>(result: Answer<T>): T {
  if (!result.ok) throw new Error(result.error);
  return result.value;
}
