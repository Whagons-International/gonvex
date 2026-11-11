/**
 * Small HTTP helpers with the reference's response shapes: every error is
 * `{"error": "..."}` with the status the axum handler returns.
 */

export class HttpError extends Error {
  constructor(readonly status: number, message: string, readonly extra: Record<string, unknown> = {}) {
    super(message);
  }
}

export const json = (body: unknown, status = 200, headers: HeadersInit = {}): Response =>
  new Response(JSON.stringify(body), { status, headers: { "content-type": "application/json", ...headers } });

export const error = (status: number, message: string, extra: Record<string, unknown> = {}): Response => json({ error: message, ...extra }, status);

export function errorResponse(cause: unknown): Response {
  if (cause instanceof HttpError) return error(cause.status, cause.message, cause.extra);
  return error(500, cause instanceof Error ? cause.message : String(cause));
}

/**
 * axum's `Json` extractor: a missing or wrong content type is 415, malformed
 * JSON is 400, and a body that parses but does not fit the handler's struct is
 * 422. Those rejections are plain text, not `{error}` (CONTRACT D.2.2 item 1).
 */
export class Body {
  private constructor(readonly value: unknown) {}

  static async read(request: Request): Promise<Body> {
    const type = request.headers.get("content-type") ?? "";
    if (!/^application\/(?:[^;]*\+)?json\s*(?:;|$)/i.test(type.trim())) {
      throw new PlainError(415, "Expected request with `Content-Type: application/json`");
    }
    const text = await request.text();
    try {
      return new Body(JSON.parse(text));
    } catch (cause) {
      throw new PlainError(400, `Failed to parse the request body as JSON: ${(cause as Error).message}`);
    }
  }

  /** The body as an object, or 422 (a serde struct rejects anything else). */
  object(): Record<string, unknown> {
    if (!this.value || typeof this.value !== "object" || Array.isArray(this.value)) {
      throw new PlainError(422, "Failed to deserialize the JSON body into the target type: invalid type: expected a map");
    }
    return this.value as Record<string, unknown>;
  }

  /** `#[serde] field: String` (required). */
  string(name: string): string {
    const value = this.object()[name];
    if (typeof value !== "string") throw new PlainError(422, `Failed to deserialize the JSON body into the target type: missing field \`${name}\``);
    return value;
  }

  /** `#[serde(default)] field: String`. */
  optionalString(name: string): string {
    const value = this.object()[name];
    if (value === undefined || value === null) return "";
    if (typeof value !== "string") throw new PlainError(422, `Failed to deserialize the JSON body into the target type: ${name}: invalid type, expected a string`);
    return value;
  }

  /** `Option<String>`: absent or null is undefined. */
  maybeString(name: string): string | undefined {
    const value = this.object()[name];
    if (value === undefined || value === null) return undefined;
    if (typeof value !== "string") throw new PlainError(422, `Failed to deserialize the JSON body into the target type: ${name}: invalid type, expected a string`);
    return value;
  }

  maybeBool(name: string): boolean | undefined {
    const value = this.object()[name];
    if (value === undefined || value === null) return undefined;
    if (typeof value !== "boolean") throw new PlainError(422, `Failed to deserialize the JSON body into the target type: ${name}: invalid type, expected a boolean`);
    return value;
  }

  bool(name: string): boolean {
    const value = this.maybeBool(name);
    if (value === undefined) throw new PlainError(422, `Failed to deserialize the JSON body into the target type: missing field \`${name}\``);
    return value;
  }

  /** `#[serde(default)] field: Vec<String>`. */
  strings(name: string): string[] {
    const value = this.object()[name];
    if (value === undefined || value === null) return [];
    if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
      throw new PlainError(422, `Failed to deserialize the JSON body into the target type: ${name}: invalid type, expected a sequence of strings`);
    }
    return value as string[];
  }
}

/** A plain-text rejection (axum extractors), not `{error}`. */
export class PlainError extends Error {
  constructor(readonly status: number, message: string) {
    super(message);
  }
}

export function plainResponse(cause: PlainError): Response {
  return new Response(cause.message, { status: cause.status, headers: { "content-type": "text/plain; charset=utf-8" } });
}

/**
 * chrono's serde form of a `DateTime<Utc>` built from Unix milliseconds:
 * RFC 3339 with `Z`, and the fraction only when it is not zero (AutoSi).
 */
export function isoTime(ms: number): string {
  const iso = new Date(ms).toISOString();
  return iso.endsWith(".000Z") ? `${iso.slice(0, -5)}Z` : iso;
}

export const isoTimeOrNull = (ms: unknown): string | null => (ms === null || ms === undefined ? null : isoTime(Number(ms)));

/** `x-gonvex-project-id`, or the `project` query value when it is set (operations.rs `project_header_or_query`). */
export function projectHeaderOrQuery(request: Request, url: URL): string {
  const query = (url.searchParams.get("project") ?? "").trim();
  if (query) return query;
  return (request.headers.get("x-gonvex-project-id") ?? "").trim();
}

/** Rust `str::trim().to_lowercase()`. */
export const normalizeEmail = (value: string): string => value.trim().toLowerCase();

/** operations.rs `display_name`. */
export function displayName(email: string): string {
  return (email.split("@")[0] ?? email).replace(/[._-]/g, " ");
}

/** Rust `str::len()`: bytes, not UTF-16 units. */
export const byteLength = (value: string): number => new TextEncoder().encode(value).length;
