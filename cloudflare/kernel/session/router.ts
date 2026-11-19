import { ASSIGNMENT_HEADER, automaticPlacement, controllerCell, withDeadline, type GatewayAssignment } from "../gateway/placement";
/**
 * `GET /ws` in the router Worker (CONTRACT.md A.1.1). The Worker cannot know
 * the tenant (it arrives in the first `auth` frame, 0004), so it only checks
 * the handshake and hands the upgrade to a gateway (`placement/gateway.ts`).
 *
 * - `?public=1` selects the public transport; any other value does not.
 * - An invalid `Sec-WebSocket-Extensions` field is HTTP 400, as in Rust
 *   (`ws/negotiation.rs`). Valid offers pass through: workerd negotiates
 *   permessage-deflate itself (A.1.6 adapt, S3 §4).
 * - K16: the client address comes from `CF-Connecting-IP`; its pending and
 *   public connection slots are reserved in the address limiter before the
 *   upgrade, and an exhausted budget is HTTP 429 with Rust's text.
 */
import { ADMISSION_REASON_HEADER, gatewayFor, type GatewayEnv, UPGRADE_HEADERS } from "./placement/gateway";
import { clientAddress, FIXTURE_ADDRESS_HEADERS } from "../public/address";
import { admitUpgrade, type PublicGuardEnv, publicGuard } from "../public/guard";

export interface WebSocketRouteEnv extends GatewayEnv, PublicGuardEnv {
  GATEWAY: DurableObjectNamespace;
  /** Dev/test only: enables the fixture client-address override (public/address.ts). */
  GONVEX_ASSEMBLY_FIXTURE_KEY?: string;
}

/** Router stages of an upgrade, for content-free refusal diagnostics (X02). */
type UpgradeStage = "placement" | "admission" | "gateway";

/**
 * A finite, content-free class of a refusal cause: never the message, ids or address.
 * `deadline` is the router's own `withDeadline`; the rest are platform error families.
 */
export function refusalCause(error: unknown): string {
  const message = error instanceof Error ? error.message : String(error);
  if (message === "gateway RPC deadline") return "deadline";
  if (/subrequest depth/i.test(message)) return "depth";
  if (/overloaded/i.test(message)) return "overloaded";
  if (/reset|no longer active|exceeded its (cpu|memory)/i.test(message)) return "reset";
  if (/capacity|exhausted|busy|ceiling/i.test(message)) return "capacity";
  return "error";
}

function refusedUpgrade(stage: UpgradeStage, error: unknown, started: number, body: string): Response {
  const cause = refusalCause(error);
  console.error("router: upgrade refused", { stage, cause, elapsedMs: Date.now() - started });
  return new Response(body, { status: 503, headers: { "retry-after": "1", [ADMISSION_REASON_HEADER]: `router-${stage}-${cause}` } });
}

/**
 * X16: warm placement. The cell comes from a hash of the connection id, so a random id put nearly every sign-in on
 * another of the 32 cells: a cold controller, a cold gateway, and that gateway's first-time registrations (Control
 * `watch`, the tenant's `attachGateway`, its routing identity and placement epoch), each a durable write the
 * reply waits for. This isolate remembers the cell of its last admitted upgrade for `STICKY_MS` and draws the next
 * id from that cell (a uniformly random UUID that hashes into it), so back-to-back sign-ins reach the same warm,
 * registered gateway. The cell's controller still picks the gateway and enforces capacity. At most
 * `STICKY_PER_SECOND` upgrades a second use the remembered cell; the rest spread over all cells as before, and any
 * refusal forgets it.
 */
export const STICKY_MS = 120_000;
export const STICKY_PER_SECOND = 16;
const STICKY_DRAWS = 1_024;
export class WarmCell {
  private cell: number | undefined;
  private until = 0;
  private second = 0;
  private used = 0;
  constructor(private readonly now: () => number = Date.now, private readonly uuid: () => string = () => crypto.randomUUID()) {}
  /** A connection id: in the remembered cell while it is fresh and under the rate, else uniformly random. */
  connectionId(): string {
    const now = this.now();
    if (this.cell === undefined || now >= this.until) return this.uuid();
    const second = Math.floor(now / 1_000);
    if (second !== this.second) { this.second = second; this.used = 0; }
    if (this.used >= STICKY_PER_SECOND) return this.uuid();
    this.used++;
    for (let i = 0; i < STICKY_DRAWS; i++) { const id = this.uuid(); if (controllerCell(id) === this.cell) return id; }
    return this.uuid();
  }
  admitted(cell: number): void { this.cell = cell; this.until = this.now() + STICKY_MS; }
  refused(): void { this.cell = undefined; }
  /** Test seam. */
  get remembered(): number | undefined { return this.now() < this.until ? this.cell : undefined; }
}
const warmCell = new WarmCell();

export async function handleWebSocketUpgrade(request: Request, env: WebSocketRouteEnv): Promise<Response> {
  if (request.method !== "GET") return new Response(null, { status: 405, headers: { allow: "GET" } });
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
  const extensions = request.headers.get("sec-websocket-extensions");
  if (extensions !== null && !validExtensionsField(extensions)) return new Response(null, { status: 400 });
  const url = new URL(request.url);
  const placement = automaticPlacement(env);
  const connectionId = placement ? warmCell.connectionId() : crypto.randomUUID();
  const publicTransport = url.searchParams.get("public") === "1";
  const address = clientAddress(request.headers, env.GONVEX_ASSEMBLY_FIXTURE_KEY);
  let assignment: GatewayAssignment | undefined;
  let slots = "";
  let response: Response | undefined;
  const started = Date.now();
  let stage: UpgradeStage = "placement";
  try {
    if (placement) {
      try { assignment = await withDeadline(placement.acquire(connectionId), 5_000); }
      catch (error) { warmCell.refused(); return refusedUpgrade("placement", error, started, "gateway capacity unavailable"); }
    }
    stage = "admission";
    const gatewayName = assignment?.gateway ?? gatewayFor(env, connectionId);
    slots = env.PUBLIC_LIMITER ? (publicTransport ? "pending,public" : "pending") : "";
    // The deadline also travels to the limiter: Promise.race cannot cancel a slow audit/RPC.
    const admissionMs = 2_000;
    const refused = await withDeadline(admitUpgrade(env, { address, connectionId, gateway: gatewayName, publicTransport,
      expiresAt: Date.now() + admissionMs }), admissionMs);
    if (refused) return refused;
    stage = "gateway";
    const headers = new Headers(request.headers);
    headers.delete(ASSIGNMENT_HEADER);
    headers.delete(FIXTURE_ADDRESS_HEADERS.key);
    headers.delete(FIXTURE_ADDRESS_HEADERS.address);
    if (assignment) headers.set(ASSIGNMENT_HEADER, JSON.stringify(assignment));
    headers.set(UPGRADE_HEADERS.connectionId, connectionId);
    headers.set(UPGRADE_HEADERS.publicTransport, publicTransport ? "1" : "0");
    headers.set(UPGRADE_HEADERS.address, address);
    headers.set(UPGRADE_HEADERS.slots, slots);
    response = await withDeadline(env.GATEWAY.getByName(gatewayName).fetch(new Request(request.url, { method: "GET", headers })));
    if (assignment) { if (response.status === 101) warmCell.admitted(assignment.cell); else warmCell.refused(); }
    return response;
  } catch (error) {
    warmCell.refused();
    return refusedUpgrade(stage, error, started, "gateway admission unavailable");
  } finally {
    if (response?.status !== 101) {
      // Release independently: controller failure must not skip K16 cleanup (or mask the refusal).
      await Promise.allSettled([
        ...(slots ? [withDeadline(publicGuard(env).release(address, connectionId, slots.split(",") as Array<"pending" | "public">))] : []),
        ...(assignment && placement ? [withDeadline(placement.release(assignment))] : []),
      ]);
    }
  }
}

const TOKEN = /^[A-Za-z0-9!#$%&'*+\-.^_`|~]+/;

/**
 * RFC 6455 extension list grammar as `ws/negotiation.rs` parses it: an invalid
 * field rejects the handshake; unknown extensions and bad parameter values do
 * not (they only decline that offer). A field with no extension at all is
 * invalid. `Headers.get` joins repeated fields with ", ", which the list
 * grammar accepts the same way.
 */
export function validExtensionsField(field: string): boolean {
  let rest = field;
  let members = 0;
  const ows = () => { rest = rest.replace(/^[ \t]*/, ""); };
  const take = (c: string) => { if (rest.startsWith(c)) { rest = rest.slice(1); return true; } return false; };
  const token = (): boolean => { const m = TOKEN.exec(rest); if (!m) return false; rest = rest.slice(m[0].length); return true; };
  const value = (): boolean => {
    if (!take('"')) return token();
    for (;;) {
      if (rest.length === 0) return false;
      const c = rest[0]!;
      rest = rest.slice(1);
      if (c === '"') return true;
      if (c === "\\") { if (rest.length === 0) return false; rest = rest.slice(1); }
    }
  };
  for (;;) {
    ows();
    if (take(",")) continue;
    if (rest.length === 0) break;
    if (!token()) return false;
    members++;
    for (;;) {
      ows();
      if (!take(";")) break;
      ows();
      if (!token()) return false;
      ows();
      if (take("=")) { ows(); if (!value()) return false; }
    }
    ows();
    if (rest.length === 0) break;
    if (!take(",")) return false;
  }
  return members > 0;
}
