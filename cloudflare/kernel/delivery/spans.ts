/**
 * X13i: opt-in, time-only spans of one socket call's path (dev, `GONVEX_SPAN_TIMING=1`), to split the
 * create's pre-commit time (b) into: gateway queue turn, CallOrigin deferral, gateway -> tenant RPC,
 * tenant serial wait, reducer run, durable write after the reply (output gate), and the tenant -> gateway
 * delivery. Each line carries durations, a frame type and a function path only: never ids, arguments,
 * contents or identities. Off by default; nothing is awaited for it, so it never changes an ordering.
 *
 * Leaf module (no imports): the gateway, the session, the tenant and the delivery outlet all use it.
 */
let enabled = false;
/** Test seam (fixture objects only): captured lines while a fixture switched spans on. */
let captured: Array<{ name: string; fields: SpanFields }> | undefined;

export function setSpanTiming(on: boolean): void { enabled = on; }
export function spanTimingOn(): boolean { return enabled; }
export function spanTimingFromEnv(env: unknown): boolean {
  return (env as { GONVEX_SPAN_TIMING?: string } | undefined)?.GONVEX_SPAN_TIMING === "1";
}

export type SpanFields = Record<string, string | number | boolean | undefined>;

/** One `span: <name>` line when spans are on. */
export function span(name: string, fields: SpanFields): void {
  if (!enabled) return;
  console.log(`span: ${name}`, fields);
  captured?.push({ name, fields });
}

/** Fixture seam: switch spans in this isolate and return (then clear) the lines captured since the last call. */
export function fixtureSpans(on: boolean): Array<{ name: string; fields: SpanFields }> {
  const out = captured ?? [];
  enabled = on;
  captured = on ? [] : undefined;
  return out;
}
