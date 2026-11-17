/** A Worker replacement can cut an HTTP response after staging its release.
 * Keep one request id across retries so the operator resumes its publication,
 * rather than running migrations or advancing the generation a second time. */
export async function postDevSync(url: string, init: RequestInit, fetcher: typeof fetch = fetch): Promise<Response> {
  const headers = new Headers(init.headers);
  if (!headers.has("x-gonvex-sync-request-id")) headers.set("x-gonvex-sync-request-id", crypto.randomUUID());
  const deadline = Date.now() + 180_000;
  let delay = 100;
  for (;;) {
    const response = await fetcher(url, {...init, method:"POST", headers});
    if (response.status !== 503) return response;
    const text = await response.clone().text();
    let retryable = text.startsWith("Your worker restarted mid-request.");
    try { const error = JSON.parse(text); retryable ||= error.retryable === true && typeof error.error === "string" && error.error.startsWith("release "); } catch { /* not an operator JSON response */ }
    if (!retryable || Date.now() >= deadline) return response;
    await response.body?.cancel();
    await new Promise(resolve=>setTimeout(resolve,delay));
    delay = Math.min(delay * 2, 1000);
  }
}
