/**
 * X17o: a Reducer call that arrives on a socket while its Action is in flight is forwarded to the tenant at once
 * (the client's frame is a fresh event, so the gateway's call needs no CallOrigin wait), marked with the Action it
 * must follow. The tenant holds it until that Action's dispatch returns an inline result, then runs it; otherwise it
 * answers `FORWARD_REFUSED` and the gateway dispatches the call the ordinary way, in its socket's order. The marker
 * never reaches a client.
 */
export const FORWARD_REFUSED = '{"type":"__gonvex.forwardRefused"}';

/** Whether a forwarded call's reply is the refusal (the gateway then dispatches the call itself). */
export function forwardRefused(replies: readonly string[]): boolean {
  return replies.length === 1 && replies[0] === FORWARD_REFUSED;
}

