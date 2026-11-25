// Trusted base code only. Changed paths are data, never executable PR code.
const { classifyChangedFiles } = require('../.github/scripts/ui-diff-classifier.cjs');
// Gonvex is a framework: its SDK packages, runtime and CLI have no screens. The
// shared classifier counts any packages/*/src .tsx file as UI, which gates SDK
// logic on browser evidence it cannot have. Only the dashboard renders UI here.
const RENDERED_UI = /^apps\/dashboard\//;
function classifyGonvexDiff(input) {
  const diff = classifyChangedFiles(input);
  if (!diff.complete) return diff;
  const uiFiles = diff.uiFiles.filter(path => RENDERED_UI.test(path));
  return { ...diff, uiFiles, isUi: uiFiles.length > 0, classification: uiFiles.length ? 'ui' : 'non-ui',
    reason: uiFiles.length ? diff.reason : 'no dashboard UI source changed' };
}
const eligible = pr => pr.state === 'open' && !pr.draft && ['main', 'staging'].includes(pr.base?.ref);
const membership = pulls => JSON.stringify(pulls.map(pr => [pr.number, pr.head.sha, pr.base.sha, pr.body]).sort((a, b) => a[0] - b[0]));
async function existingBlocker({ github, context }, pr) {
  if ((pr.labels || []).some(label => ['hold', 'on-hold', 'do-not-merge', 'blocked', 'wip', 'product-fit-blocked', 'needs-product-fit', 'product-fit', 'mergify/configuration-migration'].includes(label.name))) return 'Explicit hold or product/config flag remains open';
  const comments = await github.paginate(github.rest.issues.listComments, { ...context.repo, issue_number: pr.number, per_page: 100 });
  const flags = new Map();
  for (const comment of [...comments].sort((a, b) => Date.parse(a.updated_at || a.created_at) - Date.parse(b.updated_at || b.created_at))) {
    if (!['desarso', 'github-actions[bot]', 'whagons-ai-review-codex[bot]'].includes(String(comment.user?.login).toLowerCase())) continue;
    for (const flag of (comment.body || '').matchAll(/<!-- whagons-(visual|product-fit)-flag v1 id=([\w-]+) state=(open|resolved) -->/g)) flags.set(`${flag[1]}:${flag[2]}`, flag[3]);
  }
  if ([...flags.values()].includes('open')) return 'Open visual or product-fit flag remains';
  const reviews = await github.paginate(github.rest.pulls.listReviews, { ...context.repo, pull_number: pr.number, per_page: 100 });
  const latest = new Map();
  for (const review of [...reviews].sort((a, b) => Date.parse(a.submitted_at) - Date.parse(b.submitted_at))) if (review.state !== 'COMMENTED') latest.set(review.user?.login, review.state);
  if ([...latest.values()].includes('CHANGES_REQUESTED')) return 'An existing change request remains open';
  const statuses = await github.paginate(github.rest.repos.listCommitStatusesForRef, { ...context.repo, ref: pr.head.sha, per_page: 100 });
  const appStatus = statuses.find(status => status.context === `UI merge policy / ${pr.base.ref}` && /^https:\/\/review\.whagons\.com(?:\/|$)/.test(status.target_url || ''));
  if (appStatus && appStatus.state !== 'success') return appStatus.description || 'Existing review-app blocker remains';
  return null;
}
async function reconcileUiMergeStatus({ github, context }) {
  const sha = context.payload.pull_request.head.sha;
  const members = async () => (await github.paginate(github.rest.pulls.list, { ...context.repo, state: 'open', per_page: 100 })).filter(pr => eligible(pr) && pr.head.sha === sha && pr.base.ref === context.payload.pull_request.base.ref);
  const pulls = await members();
  let ui = false, blocked = null;
  for (const summary of pulls) {
    const pr = (await github.rest.pulls.get({ ...context.repo, pull_number: summary.number })).data;
    if (membership([summary]) !== membership([pr])) throw new Error('PR changed while classifying UI policy.');
    const files = await github.paginate(github.rest.pulls.listFiles, { ...context.repo, pull_number: pr.number, per_page: 100 });
    const diff = classifyGonvexDiff({ files, expectedCount: pr.changed_files });
    if (!diff.complete) throw new Error(diff.reason);
    ui ||= diff.isUi;
    blocked ||= await existingBlocker({ github, context }, pr);
  }
  if (membership(pulls) !== membership(await members())) throw new Error('PR membership changed while classifying UI policy.');
  await github.rest.repos.createCommitStatus({ ...context.repo, sha, context: `UI merge policy / ${context.payload.pull_request.base.ref}`,
    state: ui || blocked ? 'pending' : 'success',
    description: blocked || (ui ? 'UI diff: browser evidence, AI visual review and 24h veto policy pending' : 'Complete diff: no dashboard UI source changed'),
    target_url: `https://github.com/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}` });
}
async function reconcileBrowserLabels({ github, context, validate, version }) {
  const pr = context.payload.pull_request;
  const labels = new Set((pr.labels || []).map(label => label.name));
  if (pr.draft || [1072, 1406].includes(pr.number) || labels.has('hold')) return;
  const files = await github.paginate(github.rest.pulls.listFiles, { ...context.repo, pull_number: pr.number, per_page: 100 });
  const diff = classifyGonvexDiff({ files, expectedCount: pr.changed_files });
  if (!diff.complete) throw new Error(diff.reason);
  const result = validate({ body: pr.body || '', files, expectedCount: pr.changed_files, version, login: pr.user.login, draft: pr.draft });
  const fresh = (await github.rest.pulls.get({ ...context.repo, pull_number: pr.number })).data;
  if (fresh.state !== 'open' || fresh.draft || fresh.head.sha !== pr.head.sha || fresh.base.sha !== pr.base.sha || fresh.body !== pr.body) throw new Error('PR changed; wait for its fresh event.');
  if ((fresh.labels || []).some(label => label.name === 'hold')) return;
  const currentLabels = new Set((fresh.labels || []).map(label => label.name));
  if (!diff.isUi) {
    const removed = [];
    for (const name of ['needs-review', 'needs-browser-test']) {
      if (!currentLabels.has(name)) continue;
      await github.rest.issues.removeLabel({ ...context.repo, issue_number: pr.number, name });
      removed.push(name);
    }
    if (removed.length) await github.rest.issues.createComment({ ...context.repo, issue_number: pr.number,
      body: `[Codex] Diff-only UI policy (${diff.version}) at head ${pr.head.sha}: no rendered UI source changed. Removed ${removed.map(name => '`' + name + '`').join(', ')}. Required CI, explicit holds and product-fit flags still apply.` });
    return diff;
  }
  if (!result.needsBrowser) return diff;
  const name = 'needs-browser-test';
  try { await github.rest.issues.getLabel({ ...context.repo, name }); }
  catch (error) {
    if (error.status !== 404) throw error;
    try { await github.rest.issues.createLabel({ ...context.repo, name, color: 'D4C5F9', description: 'UI diff needs current-head real-browser evidence' }); }
    catch (race) { if (race.status !== 422) throw race; }
  }
  if (!currentLabels.has(name)) await github.rest.issues.addLabels({ ...context.repo, issue_number: pr.number, labels: [name] });
  return diff;
}
async function recheckPrFormat({ github, context, validate, version }) {
  const pr = context.payload.pull_request;
  if (!eligible(pr)) return;
  const files = await github.paginate(github.rest.pulls.listFiles, { ...context.repo, pull_number: pr.number, per_page: 100 });
  const result = validate({ body: pr.body || '', files, expectedCount: pr.changed_files, version, login: pr.user.login, draft: pr.draft });
  const current = (await github.rest.pulls.get({ ...context.repo, pull_number: pr.number })).data;
  if (membership([pr]) !== membership([current])) throw new Error('PR changed while validating its declaration.');
  await github.rest.repos.createCommitStatus({ ...context.repo, sha: pr.head.sha, context: 'PR format',
    state: result.errors.length ? 'failure' : 'success',
    description: result.errors.length ? result.errors[0].slice(0, 140) : 'Trusted base validator: declaration and diff-only UI policy valid',
    target_url: `https://github.com/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}` });
}
module.exports = { classifyGonvexDiff, reconcileBrowserLabels, reconcileUiMergeStatus, recheckPrFormat };
