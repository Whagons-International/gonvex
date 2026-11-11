/**
 * The webhook providers the Ingress Worker accepts (K18, IB4, 0002).
 *
 * Reference: the external adapter that serves these today,
 * whagons-api-gateway `src/webhooks.rs` at `01e8193` (`/webhooks/telnyx`,
 * `/webhooks/github`). Each route checks that the request is signed and
 * well formed, then hands the unmodified body and the signature headers to an
 * allowlisted Action. The Action verifies the signature with the tenant's own
 * key (Telnyx keys live in project secrets, the GitHub secret is sealed in the
 * tenant database), so the ingress holds no provider key.
 *
 * The Relay route is new: `relay.ingestTelnyxWebhook` had no adapter anywhere.
 */

export type ProviderId = "telnyx" | "telnyx-relay" | "github";

export interface Provider {
  id: ProviderId;
  /** Request path. */
  route: string;
  /** The Action the delivery becomes. */
  function: string;
  /** (request header, Action argument); all are required. */
  headers: readonly (readonly [string, string])[];
  /** Largest accepted body in bytes. */
  maxBodyBytes: number;
  /** Action error texts that mean "the signature does not verify" (answered 401). */
  invalidSignature: readonly string[];
  /** Action error texts that mean "the body is too large" (answered 413). */
  tooLarge: readonly string[];
  /** How a delivery is told apart from a replay of it. */
  replay: "telnyx-timestamp" | "github-delivery";
}

/** whagons-api-gateway `max_body_bytes` (1 MiB). Telnyx events are a few KiB. */
export const DEFAULT_MAX_BODY_BYTES = 1 << 20;
/** `github.ingestWebhook` refuses more than 5 MiB (`MAX_WEBHOOK_BODY`), as the gateway does. */
export const GITHUB_MAX_BODY_BYTES = 5 << 20;
/** The gateway's cap on a header value and on the tenant selector. */
export const MAX_HEADER_BYTES = 512;
export const MAX_TENANT_BYTES = 128;
/**
 * Telnyx signs `${timestamp}|${body}`. Both Whagons Actions refuse a
 * timestamp more than 300 s old; `calling` also refuses one more than 30 s
 * ahead and `relay` one more than 300 s ahead. The edge refuses only what
 * every Action would refuse, so it never answers 401 for a delivery an Action
 * accepts.
 */
export const TELNYX_WINDOW_SECONDS = 300;
/** GitHub signs no timestamp: a completed delivery is remembered this long. */
export const GITHUB_REPLAY_MS = 60 * 60 * 1000;

export const PROVIDERS: readonly Provider[] = [
  {
    id: "telnyx",
    route: "/webhooks/telnyx",
    function: "calling.ingestWebhook",
    headers: [["telnyx-signature-ed25519", "signature"], ["telnyx-timestamp", "timestamp"]],
    maxBodyBytes: DEFAULT_MAX_BODY_BYTES,
    invalidSignature: ["Invalid Telnyx webhook signature"],
    tooLarge: [],
    replay: "telnyx-timestamp",
  },
  {
    id: "telnyx-relay",
    route: "/webhooks/telnyx/relay",
    function: "relay.ingestTelnyxWebhook",
    headers: [["telnyx-signature-ed25519", "signature"], ["telnyx-timestamp", "timestamp"]],
    maxBodyBytes: DEFAULT_MAX_BODY_BYTES,
    invalidSignature: ["Telnyx webhook signature is invalid", "Telnyx webhook timestamp is outside the replay window"],
    tooLarge: [],
    replay: "telnyx-timestamp",
  },
  {
    id: "github",
    route: "/webhooks/github",
    function: "github.ingestWebhook",
    headers: [["x-hub-signature-256", "signature"], ["x-github-event", "event"]],
    maxBodyBytes: GITHUB_MAX_BODY_BYTES,
    invalidSignature: ["GitHub webhook signature is invalid"],
    tooLarge: ["GitHub webhook payload is too large"],
    replay: "github-delivery",
  },
];

export function providerFor(pathname: string): Provider | undefined {
  return PROVIDERS.find((provider) => provider.route === pathname);
}

/** The gateway's `header_text`: trimmed, non-empty, at most 512 bytes. */
export function headerText(headers: Headers, name: string): string | null {
  const raw = headers.get(name);
  if (raw === null) return null;
  const value = raw.trim();
  if (!value || new TextEncoder().encode(value).byteLength > MAX_HEADER_BYTES) return null;
  return value;
}

/** Telnyx events carry `data.id`; a body without one is malformed (gateway `requires_event_id`). */
export function telnyxEventId(rawBody: string): string | null {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawBody);
  } catch {
    return null;
  }
  const data = parsed !== null && typeof parsed === "object" ? (parsed as { data?: unknown }).data : undefined;
  const id = data !== null && typeof data === "object" ? (data as { id?: unknown }).id : undefined;
  if (typeof id !== "string" || !id || new TextEncoder().encode(id).byteLength > MAX_HEADER_BYTES) return null;
  return id;
}

/** Seconds since the epoch from `telnyx-timestamp`, or `null` if it is not a plain integer. */
export function telnyxSeconds(value: string): number | null {
  if (!/^\d{1,15}$/.test(value)) return null;
  const seconds = Number(value);
  return Number.isSafeInteger(seconds) ? seconds : null;
}

/** True when the signed timestamp is inside the window every Telnyx Action accepts. */
export function telnyxFresh(seconds: number, nowMs: number): boolean {
  return Math.abs(nowMs / 1000 - seconds) <= TELNYX_WINDOW_SECONDS;
}

export type ActionFailure = "signature" | "tooLarge" | "unknownTenant" | "other";

/** Maps an Action's error text to the answer the provider sees. */
export function classifyFailure(provider: Provider, error: string): ActionFailure {
  if (provider.invalidSignature.some((text) => error.includes(text))) return "signature";
  if (provider.tooLarge.some((text) => error.includes(text))) return "tooLarge";
  if (error.includes("tenant is not available")) return "unknownTenant";
  return "other";
}
