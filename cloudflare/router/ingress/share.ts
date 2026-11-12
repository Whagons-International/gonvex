/**
 * Public task share pages (K18): `GET /share/task?token=&tenant=` and
 * `GET /share/task-image?token=&tenant=`.
 *
 * History (0043 §7): Convex served both from `convex/http.ts` (a server-rendered
 * page with Open Graph tags and an SVG card). The Go module kept them
 * (`gonvex/task_public_shares.go`). Since the TypeScript v2 migration
 * (whagons5-client `e8168dc04d`) nothing serves them: `/share/task` is only a
 * route of the web app, which sets its meta tags from JavaScript after an
 * Action call, so link-preview crawlers (which run no JavaScript) see none,
 * and `/share/task-image` does not exist. The ingress serves both again,
 * server-rendered, from the same Action the web app calls,
 * `taskPublicShares.getPublicPreviewByToken`, run through the ingress
 * principal in the tenant the link names.
 *
 * Everything the preview contains is escaped. The page sends a strict CSP
 * with a per-response nonce for its one inline script, no referrer (the token
 * is in the URL) and `noindex`.
 */

export const PREVIEW_FUNCTION = "taskPublicShares.getPublicPreviewByToken";
export const MAX_TOKEN_BYTES = 256;

export interface ShareConfig {
  /** Shown in titles and `og:site_name`. */
  siteName: string;
  /** Deep-link scheme of the mobile app (`<scheme>://task-share/<token>`). */
  appScheme: string;
  /** Web app origin for "Open on web"; `{tenant}` is replaced by the link's tenant selector. Unset: no web link. */
  webUrl: string | undefined;
  /** An imgproxy base: crawlers that refuse SVG get a 1200x630 PNG of the card. Unset: the SVG itself. */
  imageProxyUrl: string | undefined;
}

/** The preview fields the pages read (whagons5-client `PublicTaskSharePreview`). */
export interface SharePreview {
  token?: string;
  tenantId?: string;
  taskId?: string;
  title?: string | null;
  description?: string | null;
  workspaceName?: string | null;
  statusName?: string | null;
  priorityName?: string | null;
  spotName?: string | null;
  creatorName?: string | null;
  assigneeCount?: number | null;
  dueDate?: number | null;
  createdAt?: number | null;
}

export function isPreview(value: unknown): value is SharePreview {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

const text = (value: unknown): string => (typeof value === "string" ? value.trim() : "");

export function escapeHtml(value: string): string {
  return value.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}

/** A JSON literal that is safe inside an inline `<script>`. */
const scriptJson = (value: unknown): string =>
  JSON.stringify(value).replace(/</g, "\\u003c").replaceAll("\u2028", "\\u2028").replaceAll("\u2029", "\\u2029");

/** At most `max` characters (code points), with an ellipsis when cut. */
export function truncate(value: unknown, max: number): string {
  const chars = Array.from(text(value));
  if (chars.length <= max) return chars.join("");
  return `${chars.slice(0, max - 1).join("").trimEnd()}…`;
}

function dateLabel(value: unknown): string | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value <= 0) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  return date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

const assignees = (count: unknown): string | null =>
  typeof count === "number" && Number.isFinite(count) && count > 0 ? `${count} assignee${count === 1 ? "" : "s"}` : null;

export function ogTitle(preview: SharePreview): string {
  const title = truncate(preview.title || "Shared task", 90) || "Shared task";
  const context = [preview.workspaceName, preview.statusName].map((part) => truncate(part, 32)).filter(Boolean).join(" • ");
  return context ? `${title} - ${context}` : title;
}

export function metaDescription(preview: SharePreview): string {
  const summary = truncate(preview.description, 160);
  const due = dateLabel(preview.dueDate);
  const details = [
    text(preview.spotName) ? `Spot ${text(preview.spotName)}` : null,
    text(preview.priorityName) ? `Priority ${text(preview.priorityName)}` : null,
    due ? `Due ${due}` : null,
    assignees(preview.assigneeCount),
    text(preview.creatorName) ? `Shared by ${text(preview.creatorName)}` : null,
  ].filter((part): part is string => part !== null);
  if (summary && details.length) return truncate(`${summary} • ${details.join(" • ")}`, 260);
  return truncate(summary || details.join(" • "), 260);
}

/** `<scheme>://task-share/<token>?tenantId=<selector>`, as the web app builds it (TaskSharePage `buildDeepLink`). */
export function appLink(config: ShareConfig, token: string, tenant: string): string {
  const url = new URL(`${config.appScheme}://task-share/${encodeURIComponent(token)}`);
  if (tenant) url.searchParams.set("tenantId", tenant);
  return url.toString();
}

/** The web app's own share route, on the tenant's web origin. */
export function webLink(config: ShareConfig, token: string, tenant: string): string | null {
  if (!config.webUrl) return null;
  try {
    const origin = new URL(config.webUrl.replaceAll("{tenant}", encodeURIComponent(tenant)));
    const url = new URL("/share/task", origin);
    url.searchParams.set("token", token);
    url.searchParams.set("tenant", tenant);
    return url.toString();
  } catch {
    return null;
  }
}

const base64Url = (value: string): string => btoa(String.fromCharCode(...new TextEncoder().encode(value))).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");

/** The `og:image` URL and type: the SVG card, or a PNG of it through imgproxy (convex/http.ts `buildShareOgImageUrl`). */
export function ogImage(config: ShareConfig, imageUrl: string): { url: string; type: string } {
  if (config.imageProxyUrl) {
    try {
      const base = config.imageProxyUrl.replace(/\/+$/, "");
      new URL(base);
      return { url: `${base}/unsafe/rs:fill:1200:630:0/f:png/${base64Url(imageUrl)}`, type: "image/png" };
    } catch {
      // fall through to the SVG
    }
  }
  return { url: imageUrl, type: "image/svg+xml" };
}

export interface SharePage {
  preview: SharePreview;
  config: ShareConfig;
  shareUrl: string;
  imageUrl: string;
  appUrl: string;
  webUrl: string | null;
  nonce: string;
}

const STYLE = `
:root{color-scheme:dark}*{box-sizing:border-box}
body{margin:0;min-height:100vh;font-family:Inter,system-ui,-apple-system,"Segoe UI",sans-serif;background:linear-gradient(180deg,#06111c 0%,#0a1b2b 100%);color:#f8fafc}
main{min-height:100vh;display:grid;place-items:center;padding:24px 16px}
.card{width:min(720px,100%);border-radius:24px;padding:28px;background:rgba(10,27,43,.9);border:1px solid rgba(148,163,184,.2);box-shadow:0 20px 54px rgba(0,0,0,.32)}
.eyebrow{display:inline-block;padding:8px 12px;border-radius:999px;background:rgba(14,165,233,.12);color:#7dd3fc;font-size:12px;letter-spacing:.08em;text-transform:uppercase}
h1{margin:18px 0 12px;font-size:clamp(26px,4vw,40px);line-height:1.1;overflow-wrap:anywhere}
.description{margin:0;font-size:16px;line-height:1.6;color:#dbeafe;overflow-wrap:anywhere}
.facts{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:24px 0}
.fact{padding:14px 16px;border-radius:16px;background:rgba(8,35,60,.68);border:1px solid rgba(125,211,252,.16)}
.fact span{display:block;margin-bottom:6px;font-size:11px;color:#94a3b8;letter-spacing:.08em;text-transform:uppercase}
.fact strong{font-size:15px;color:#f8fafc;overflow-wrap:anywhere}
.actions{display:flex;flex-wrap:wrap;gap:12px;margin-top:24px}
.button{display:inline-flex;align-items:center;justify-content:center;min-height:48px;padding:0 18px;border-radius:14px;text-decoration:none;font-weight:700}
.primary{background:linear-gradient(135deg,#22c55e 0%,#0ea5e9 100%);color:#04111b}
.secondary{border:1px solid rgba(148,163,184,.26);color:#e2e8f0}
.creator{margin:18px 0 0;color:#94a3b8;font-size:14px}`;

export function renderSharePage(page: SharePage): string {
  const { preview, config } = page;
  const site = escapeHtml(config.siteName);
  const title = text(preview.title) || "Shared task";
  const heading = ogTitle(preview);
  const description = metaDescription(preview) || `Open this shared task in ${config.siteName}.`;
  const imageAlt = truncate(`${title} - ${text(preview.workspaceName) || `${config.siteName} task`}`, 120);
  const image = ogImage(config, page.imageUrl);
  const fact = (label: string, value: string | null) => (value ? `<div class="fact"><span>${label}</span><strong>${escapeHtml(value)}</strong></div>` : "");
  const facts = [
    fact("Workspace", text(preview.workspaceName) || null),
    fact("Status", text(preview.statusName) || null),
    fact("Priority", text(preview.priorityName) || null),
    fact("Spot", text(preview.spotName) || null),
    fact("Due", dateLabel(preview.dueDate)),
    fact("Created", dateLabel(preview.createdAt)),
    typeof preview.assigneeCount === "number" && Number.isFinite(preview.assigneeCount) ? fact("Assignees", String(preview.assigneeCount)) : "",
  ].join("");
  const body = text(preview.description) ? `<p class="description">${escapeHtml(text(preview.description))}</p>` : "";
  const creator = text(preview.creatorName) ? `Shared from ${site} by ${escapeHtml(text(preview.creatorName))}` : `Shared from ${site}`;
  const meta = (property: string, content: string) => `<meta property="${property}" content="${escapeHtml(content)}">`;
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(heading)} | ${site}</title>
<meta name="description" content="${escapeHtml(description)}">
<meta name="robots" content="noindex">
${meta("og:type", "website")}
${meta("og:site_name", config.siteName)}
${meta("og:title", heading)}
${meta("og:description", description)}
${meta("og:url", page.shareUrl)}
${meta("og:image", image.url)}
${meta("og:image:alt", imageAlt)}
${meta("og:image:type", image.type)}
${meta("og:image:width", "1200")}
${meta("og:image:height", "630")}
<meta name="twitter:card" content="summary_large_image">
<meta name="twitter:title" content="${escapeHtml(heading)}">
<meta name="twitter:description" content="${escapeHtml(description)}">
<meta name="twitter:image" content="${escapeHtml(image.url)}">
<meta name="twitter:image:alt" content="${escapeHtml(imageAlt)}">
${meta("al:ios:url", page.appUrl)}
${meta("al:android:url", page.appUrl)}
${meta("al:web:url", page.webUrl ?? page.shareUrl)}
<style>${STYLE}</style>
</head>
<body>
<main>
<section class="card">
<div class="eyebrow">${site} task link</div>
<h1>${escapeHtml(title)}</h1>
${body}
<div class="facts">${facts}</div>
<div class="actions">
<a class="button primary" href="${escapeHtml(page.appUrl)}">Open in app</a>
${page.webUrl ? `<a class="button secondary" href="${escapeHtml(page.webUrl)}">Open on web</a>` : ""}
</div>
<p class="creator">${creator}</p>
</section>
</main>
<script nonce="${page.nonce}">
(function () {
  var ua = navigator.userAgent || "";
  if (/bot|crawler|spider|facebookexternalhit|twitterbot|slackbot|discordbot|whatsapp|telegrambot|linkedinbot/i.test(ua)) return;
  if (!/Android|iPhone|iPad|iPod/i.test(ua)) return;
  var key = "gonvex-share-open:" + ${scriptJson(page.shareUrl)};
  try { if (sessionStorage.getItem(key) === "1") return; sessionStorage.setItem(key, "1"); } catch (e) {}
  setTimeout(function () { location.href = ${scriptJson(page.appUrl)}; }, 120);
})();
</script>
</body>
</html>`;
}

function messagePage(config: ShareConfig, heading: string, message: string): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${escapeHtml(heading)} | ${escapeHtml(config.siteName)}</title>
<meta name="robots" content="noindex,nofollow">
<style>${STYLE}</style>
</head>
<body>
<main><section class="card"><h1>${escapeHtml(heading)}</h1><p class="description">${escapeHtml(message)}</p></section></main>
</body>
</html>`;
}

export const renderNotFound = (config: ShareConfig): string =>
  messagePage(config, "Task link unavailable", "This task link is invalid, revoked, or no longer available.");

export const renderUnavailable = (config: ShareConfig): string =>
  messagePage(config, "Task link temporarily unavailable", "This task link cannot be opened right now. Try again in a minute.");

/** The 1200x630 card (convex/http.ts `renderTaskShareImage`). */
export function renderShareImage(preview: SharePreview, config: ShareConfig): string {
  const title = escapeHtml(truncate(preview.title || "Shared task", 34) || "Shared task");
  const subtitle = escapeHtml(truncate(preview.description, 70));
  const meta = escapeHtml([preview.workspaceName, preview.statusName, preview.priorityName].map((part) => truncate(part, 18)).filter(Boolean).join(" • ") || config.siteName);
  const due = dateLabel(preview.dueDate);
  const detail = escapeHtml([
    text(preview.spotName) ? `Spot ${truncate(preview.spotName, 14)}` : null,
    due ? `Due ${due}` : null,
    assignees(preview.assigneeCount),
  ].filter(Boolean).join(" • "));
  const sharedBy = text(preview.creatorName) ? escapeHtml(`Shared by ${truncate(preview.creatorName, 24)}`) : "";
  const detailY = subtitle ? 520 : 454;
  const sharedByY = subtitle ? 560 : 494;
  const font = `font-family="Inter, Arial, sans-serif"`;
  return `<?xml version="1.0" encoding="UTF-8"?>
<svg width="1200" height="630" viewBox="0 0 1200 630" fill="none" xmlns="http://www.w3.org/2000/svg">
<defs>
<linearGradient id="bg" x1="0" y1="0" x2="1200" y2="630" gradientUnits="userSpaceOnUse"><stop stop-color="#0C1E2F"/><stop offset="1" stop-color="#103850"/></linearGradient>
<linearGradient id="accent" x1="0" y1="0" x2="1200" y2="0" gradientUnits="userSpaceOnUse"><stop stop-color="#22C55E"/><stop offset="1" stop-color="#0EA5E9"/></linearGradient>
</defs>
<rect width="1200" height="630" fill="url(#bg)"/>
<circle cx="1020" cy="96" r="190" fill="#22C55E" fill-opacity="0.10"/>
<circle cx="136" cy="584" r="230" fill="#0EA5E9" fill-opacity="0.12"/>
<rect x="92" y="88" width="340" height="66" rx="33" fill="url(#accent)" fill-opacity="0.24"/>
<text x="130" y="129" fill="#A5F3FC" ${font} font-size="23" font-weight="700" letter-spacing="1.2">${escapeHtml(truncate(config.siteName.toUpperCase(), 14))} TASK</text>
<text x="92" y="266" fill="#F8FAFC" ${font} font-size="94" font-weight="800">${title}</text>
<text x="92" y="354" fill="#CDE6F7" ${font} font-size="42" font-weight="600">${meta}</text>
${subtitle ? `<text x="92" y="452" fill="#E8F4FF" ${font} font-size="48">${subtitle}</text>` : ""}
${detail ? `<text x="92" y="${detailY}" fill="#B6D8EE" ${font} font-size="34" font-weight="600">${detail}</text>` : ""}
${sharedBy ? `<text x="92" y="${sharedByY}" fill="#9FC5DF" ${font} font-size="30">${sharedBy}</text>` : ""}
</svg>`;
}

export const EMPTY_IMAGE = `<?xml version="1.0" encoding="UTF-8"?><svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg"><rect width="1200" height="630" fill="#0F172A"/></svg>`;
