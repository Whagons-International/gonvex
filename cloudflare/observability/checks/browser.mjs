import { requireCheck } from './contract.mjs';

/** Portable Playwright scenario. Credentials stay in memory; no storageState/global hooks.
 * Selectors and application observation bridges must come from the actual app, approved by V02.
 * No default selectors, auth bypass, invented ready flag, screenshots or trace capture.
 */
export async function browserScenario(c, { browser, application, selectors, credentials, origin }) {
  requireCheck(browser && application && selectors && credentials && c.allowedOrigins?.includes(origin), 'browser-config-missing', 'blocked');
  for (const key of ['username','password','submit','authenticated','create','title','save','task','edit']) requireCheck(typeof selectors[key] === 'string' && selectors[key].length > 0, 'browser-selectors-missing', 'blocked');
  requireCheck(c.domain === 'real-browser', 'browser-domain', 'blocked');
  const contexts = [];
  try {
    for (const role of ['writer','reader']) {
      requireCheck(credentials[role]?.username && credentials[role]?.password, 'browser-identity-missing', 'blocked');
      const context = await browser.newContext(); contexts.push(context);
      const page = await context.newPage();
      // This route fence is a network side-effect allowlist, not an authentication implementation.
      await page.route('**/*', route => c.allowedOrigins.includes(new URL(route.request().url()).origin) ? route.continue() : route.abort());
      await page.goto(origin, { waitUntil:'domcontentloaded', timeout:c.deadlineMs });
      await page.locator(selectors.username).fill(credentials[role].username);
      await page.locator(selectors.password).fill(credentials[role].password);
      await page.locator(selectors.submit).click();
      await page.locator(selectors.authenticated).waitFor({ state:'visible', timeout:c.deadlineMs });
    }
    // Bridge runs the same task assertions, but mutate must use these real UI controls;
    // observer must derive matching fields/revision from actual second page application state.
    const { taskScenario } = await import('./scenarios.mjs');
    const a = await application.bindPages(contexts, selectors);
    requireCheck(a?.mutationBoundary === 'ui-controls' && a?.readBoundary === 'fresh-application-sqlite', 'browser-application-bridge', 'blocked');
    return await taskScenario(c, a);
  } finally { await Promise.all(contexts.map(context => context.close())); }
}
