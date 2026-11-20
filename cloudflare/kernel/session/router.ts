import { ASSIGNMENT_HEADER, CONTROLLER_CELLS, DIRECT_HEADER, automaticPlacement, controllerCell, hashConnection, withDeadline, type DirectAdmission, type GatewayAssignment } from "../gateway/placement";
import { setSpanTiming, span, spanTimingFromEnv } from "../delivery/spans";
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
import { ADMISSION_REASON_HEADER, DIRECT_REFUSAL_PREFIX, gatewayFor, type GatewayEnv, UPGRADE_HEADERS } from "./placement/gateway";
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
  /** X17g: the holder (and its epoch) the remembered cell's last upgrade was admitted to. */
  private holder: { gateway: string; epoch: number } | undefined;
  private until = 0;
  private second = 0;
  private used = 0;
  constructor(private readonly now: () => number = Date.now, private readonly uuid: () => string = () => crypto.randomUUID()) {}
  /** A connection id: in the remembered cell while it is fresh and under the rate, else uniformly random. */
  connectionId(): string {
    return this.drawInCell() ?? this.uuid();
  }
  /**
   * X17g: a direct admission to the remembered holder while it is fresh and under the rate (the same budget as
   * `connectionId`), else undefined (the upgrade goes through the controller).
   */
  direct(): DirectAdmission | undefined {
    if (!this.holder || this.cell === undefined) return undefined;
    const { gateway, epoch } = this.holder, cell = this.cell;
    const connectionId = this.drawInCell();
    return connectionId === undefined || controllerCell(connectionId) !== cell ? undefined : { v: 1, cell, gateway, epoch, connectionId };
  }
  private drawInCell(): string | undefined {
    const now = this.now();
    if (this.cell === undefined || now >= this.until) return undefined;
    const second = Math.floor(now / 1_000);
    if (second !== this.second) { this.second = second; this.used = 0; }
    if (this.used >= STICKY_PER_SECOND) return undefined;
    this.used++;
    for (let i = 0; i < STICKY_DRAWS; i++) { const id = this.uuid(); if (controllerCell(id) === this.cell) return id; }
    return undefined;
  }
  admitted(cell: number, holder?: { gateway: string; epoch: number }): void {
    this.cell = cell;
    this.holder = holder;
    this.until = this.now() + STICKY_MS;
  }
  refused(): void { this.cell = undefined; this.holder = undefined; }
  /** X17i: whether a fresh holder is remembered (else the colo's shared one is read). */
  get hasHolder(): boolean { return this.holder !== undefined && this.cell !== undefined && this.now() < this.until; }
  /** Test seam. */
  get remembered(): number | undefined { return this.now() < this.until ? this.cell : undefined; }
}
let warmCell = new WarmCell();
/** Test seam: forget this isolate's warm cell and holder (the next upgrade goes through the controller). */
export function resetWarmCell(cell: WarmCell = new WarmCell(), options: { keepShared?: boolean } = {}): void {
  warmCell = cell;
  lastShared = "";
  // Each test starts without a shared holder too, unless it simulates another isolate of the same colo.
  if (!options.keepShared) sharedIgnoredBefore = Date.now();
}

/**
 * X17i: the warm holder shared by every router isolate of a colo, through the colo's Cache API (5-7 ms on dev SJC).
 * `WarmCell` is per isolate, and a client's second socket usually reaches another isolate: it went through the
 * controller again (acquire 106-168 ms, b28) and to another holder. An isolate with no warm holder reads this one and
 * goes direct. It is only a hint: the holder and the controller's `admitDirect` check everything (identity, epoch,
 * cell, capacity, draining), and a `direct-*` refusal deletes it. Rust has no router; nothing client-visible changes.
 */
export interface SharedHolder { v: 1; cell: number; gateway: string; epoch: number; at: number }
let sharedIgnoredBefore = 0;
let lastShared = "";
const SHARED_READ_MS = 50;
/**
 * #538 review: one entry per client (the server-derived address, `CF-Connecting-IP`), not one per colo: it covers a
 * client's second socket and keeps #533's spread of different clients over holders (and `acquire`'s balancing).
 */
function sharedKey(url: URL, address: string): Request {
  return new Request(`https://gonvex-warm-holder.invalid/${encodeURIComponent(url.host)}/${hashConnection(address).toString(36)}`);
}
function colocache(): Cache | undefined {
  try { return (globalThis as { caches?: { default?: Cache } }).caches?.default; } catch { return undefined; }
}
async function readShared(url: URL, address: string, waitUntil: (work: Promise<unknown>) => void): Promise<SharedHolder | undefined> {
  const cache = colocache();
  if (!cache) return undefined;
  try {
    // A read past the deadline is not awaited by the upgrade, but it still finishes inside the request (waitUntil).
    const match = cache.match(sharedKey(url, address));
    waitUntil(match.catch(() => undefined));
    const hit = await withDeadline(match, SHARED_READ_MS);
    if (!hit) return undefined;
    const value = await hit.json() as SharedHolder;
    if (value?.v !== 1 || !Number.isInteger(value.cell) || value.cell < 0 || value.cell >= CONTROLLER_CELLS || typeof value.gateway !== "string"
      || value.gateway.length > 100 || !Number.isSafeInteger(value.epoch) || value.epoch < 1 || !Number.isFinite(value.at)) return undefined;
    if (value.at < sharedIgnoredBefore || Date.now() - value.at >= STICKY_MS) return undefined;
    return value;
  } catch { return undefined; }
}
function writeShared(url: URL, address: string, holder: { cell: number; gateway: string; epoch: number }, waitUntil?: (work: Promise<unknown>) => void): void {
  const cache = colocache();
  // Only with the request's waitUntil (the Worker entry always passes it): no write outlives its request.
  if (!cache || !waitUntil) return;
  const id = `${address}\u0000${holder.cell}\u0000${holder.gateway}\u0000${holder.epoch}`;
  const now = Date.now();
  // One write per holder change (and a refresh at half the TTL), not one per upgrade.
  if (lastShared.startsWith(id + "\u0000") && now - Number(lastShared.slice(id.length + 1)) < STICKY_MS / 2) return;
  lastShared = `${id}\u0000${now}`;
  const value: SharedHolder = { v: 1, ...holder, at: now };
  const work = cache.put(sharedKey(url, address), new Response(JSON.stringify(value), { headers: { "content-type": "application/json", "cache-control": `max-age=${Math.floor(STICKY_MS / 1000)}` } })).catch(() => {});
  waitUntil(work);
}
function dropShared(url: URL, address: string, waitUntil?: (work: Promise<unknown>) => void): void {
  const cache = colocache();
  if (!cache || !waitUntil) return;
  lastShared = "";
  waitUntil(cache.delete(sharedKey(url, address)).catch(() => false));
}

export async function handleWebSocketUpgrade(request: Request, env: WebSocketRouteEnv, ctx?: { waitUntil(work: Promise<unknown>): void }): Promise<Response> {
  if (request.method !== "GET") return new Response(null, { status: 405, headers: { allow: "GET" } });
  if (request.headers.get("upgrade")?.toLowerCase() !== "websocket") return new Response("Connection header did not include 'upgrade'", { status: 400 });
  const extensions = request.headers.get("sec-websocket-extensions");
  if (extensions !== null && !validExtensionsField(extensions)) return new Response(null, { status: 400 });
  const url = new URL(request.url);
  if (spanTimingFromEnv(env)) setSpanTiming(true);
  const placement = automaticPlacement(env);
  const publicTransport = url.searchParams.get("public") === "1";
  const address = clientAddress(request.headers, env.GONVEX_ASSEMBLY_FIXTURE_KEY);
  const forwarded = (connectionId: string, slots: string) => {
    const headers = new Headers(request.headers);
    headers.delete(ASSIGNMENT_HEADER);
    headers.delete(DIRECT_HEADER);
    headers.delete(FIXTURE_ADDRESS_HEADERS.key);
    headers.delete(FIXTURE_ADDRESS_HEADERS.address);
    headers.set(UPGRADE_HEADERS.connectionId, connectionId);
    headers.set(UPGRADE_HEADERS.publicTransport, publicTransport ? "1" : "0");
    headers.set(UPGRADE_HEADERS.address, address);
    headers.set(UPGRADE_HEADERS.slots, slots);
    return headers;
  };
  // X17g: connect in one hop. The isolate's last admitted holder takes the upgrade itself (one controller call and the
  // limiter, in parallel); a `direct-*` refusal or a failed hop falls back to the controller path below.
  const waitUntil = ctx ? (work: Promise<unknown>) => ctx.waitUntil(work) : undefined;
  let shared: "hit" | "miss" | "ignored" | undefined;
  if (placement && !warmCell.hasHolder) {
    // X17i: no warm holder in this isolate: this client's, if another isolate of the colo admitted it recently.
    const found = waitUntil ? await readShared(url, address, waitUntil) : undefined;
    shared = !waitUntil ? "ignored" : found ? "hit" : "miss";
    if (found) warmCell.admitted(found.cell, { gateway: found.gateway, epoch: found.epoch });
  }
  const direct = placement ? warmCell.direct() : undefined;
  if (direct) {
    const started = Date.now();
    try {
      const headers = forwarded(direct.connectionId, "");
      headers.set(DIRECT_HEADER, JSON.stringify(direct));
      const response = await withDeadline(env.GATEWAY.getByName(direct.gateway).fetch(new Request(request.url, { method: "GET", headers })));
      const reason = response.headers.get(ADMISSION_REASON_HEADER) ?? "";
      span("router.upgrade", { direct: true, gatewayMs: Date.now() - started, totalMs: Date.now() - started, status: response.status, ...(reason ? { reason } : {}), ...(shared ? { shared } : {}) });
      // Any answer but a 101 forgets the holder (#533 review): a refused address must not keep this isolate's upgrades
      // going direct. Only a `direct-*` refusal falls back; a limiter 429/503 is the client's answer.
      if (response.status === 101) {
        warmCell.admitted(direct.cell, { gateway: direct.gateway, epoch: direct.epoch });
        writeShared(url, address, direct, waitUntil);
        return response;
      }
      warmCell.refused();
      // This isolate also stops using the holder another isolate shared before now (#533: a refused address must not
      // keep going direct); a newer shared holder, written after a later admission, is used again.
      sharedIgnoredBefore = Date.now();
      if (!(response.status === 503 && reason.startsWith(DIRECT_REFUSAL_PREFIX))) return response;
      dropShared(url, address, waitUntil);
    } catch (error) {
      warmCell.refused();
      sharedIgnoredBefore = Date.now();
      // A deadline: the holder may still admit this attempt, so it is not retried (no second admission, #533 review).
      if (refusalCause(error) === "deadline") return refusedUpgrade("gateway", error, started, "gateway admission unavailable");
      console.error("router: direct upgrade failed; using the controller", { cause: refusalCause(error), elapsedMs: Date.now() - started });
    }
  }
  const connectionId = placement ? warmCell.connectionId() : crypto.randomUUID();
  let assignment: GatewayAssignment | undefined;
  let slots = "";
  let response: Response | undefined;
  const started = Date.now();
  let stage: UpgradeStage = "placement";
  const times: Record<string, number> = {};
  try {
    if (placement) {
      try { assignment = await withDeadline(placement.acquire(connectionId), 5_000); times.acquireMs = Date.now() - started; }
      catch (error) { warmCell.refused(); return refusedUpgrade("placement", error, started, "gateway capacity unavailable"); }
    }
    stage = "admission";
    const gatewayName = assignment?.gateway ?? gatewayFor(env, connectionId);
    slots = env.PUBLIC_LIMITER ? (publicTransport ? "pending,public" : "pending") : "";
    // The deadline also travels to the limiter: Promise.race cannot cancel a slow audit/RPC.
    const admissionMs = 2_000;
    const admitStarted = Date.now();
    const refused = await withDeadline(admitUpgrade(env, { address, connectionId, gateway: gatewayName, publicTransport,
      expiresAt: Date.now() + admissionMs }), admissionMs);
    times.admitMs = Date.now() - admitStarted;
    if (refused) return refused;
    stage = "gateway";
    const headers = forwarded(connectionId, slots);
    if (assignment) headers.set(ASSIGNMENT_HEADER, JSON.stringify(assignment));
    const gatewayStarted = Date.now();
    response = await withDeadline(env.GATEWAY.getByName(gatewayName).fetch(new Request(request.url, { method: "GET", headers })));
    span("router.upgrade", { ...times, gatewayMs: Date.now() - gatewayStarted, totalMs: Date.now() - started, status: response.status, warm: warmCell.remembered === assignment?.cell, ...(shared ? { shared } : {}) });
    if (assignment) {
      if (response.status === 101) {
        warmCell.admitted(assignment.cell, { gateway: assignment.gateway, epoch: assignment.epoch });
        writeShared(url, address, assignment, waitUntil);
      } else warmCell.refused();
    }
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
