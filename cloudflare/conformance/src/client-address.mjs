/**
 * How a scenario names its client address on the upgrade request (public-limits, public-calls).
 *
 * The default is `CF-Connecting-IP`, which the Rust reference trusts from 127.0.0.1. Cloudflare's edge
 * refuses a client-supplied `CF-Connecting-IP` (HTTP 403 `error code: 1000`), so a deployed assembly
 * registers the dev/test fixture headers instead (kernel/public/address.ts `FIXTURE_ADDRESS_HEADERS`).
 * Keyed by the backend's `wsUrl`, so serial runs of two backends in one process each get their own.
 */
const byUrl = new Map();

/** @param {string} wsUrl @param {(address: string) => Record<string, string>} headers */
export function registerAddressHeaders(wsUrl, headers) { byUrl.set(wsUrl, headers); }

/** @param {string} wsUrl the backend's ws URL (without query) @param {string} address */
export function addressHeaders(wsUrl, address) {
  return (byUrl.get(wsUrl) ?? ((a) => ({ 'cf-connecting-ip': a })))(address);
}
