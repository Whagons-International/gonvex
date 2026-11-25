const test = require('node:test');
const assert = require('node:assert/strict');
const { classifyGonvexDiff, reconcileBrowserLabels, reconcileUiMergeStatus } = require('./pr-ui-policy.cjs');
function fixture(files, changes = {}) {
  const pr = { number: 7, state: 'open', draft: false, base: { ref: 'staging', sha: 'base' }, head: { sha: 'head' }, body: 'Browser-tested: no', user: { login: 'agent' }, changed_files: files.length, labels: ['needs-review', 'needs-browser-test'].map(name => ({name})), ...changes };
  const removed = [], added = [], comments = [], statuses = [];
  const github = { paginate: async endpoint => endpoint === 'files' ? files : endpoint === 'pulls' ? [pr] : [], rest: {
    pulls: { listFiles: 'files', listReviews: 'reviews', list: 'pulls', get: async () => ({data: pr}) },
    repos: { listCommitStatusesForRef: 'statuses', createCommitStatus: async status => statuses.push(status) },
    issues: { listComments: 'comments', removeLabel: async label => removed.push(label.name), addLabels: async labels => added.push(...labels.labels), createComment: async comment => comments.push(comment.body), getLabel: async () => ({}) }
  }};
  return { github, context: { repo: { owner:'x', repo:'r' }, payload: { pull_request: pr }, runId: 1 }, validate: () => ({needsBrowser:true}), pr, removed, added, comments, statuses };
}
test('backend diff removes false UI labels with provenance and passes UI policy', async () => {
  const f = fixture([{filename:'src/backend/api.ts'}]);
  await reconcileBrowserLabels(f); await reconcileUiMergeStatus(f);
  assert.deepEqual(f.removed,['needs-review','needs-browser-test']); assert.equal(f.added.length,0);
  assert.match(f.comments[0], /Diff-only UI policy.*head head/);
  assert.equal(f.statuses[0].state,'success'); assert.equal(f.statuses[0].context,'UI merge policy / staging');
});
test('UI is pending even when review labels are absent and declarations say tested', async () => {
  const f = fixture([{filename:'apps/dashboard/src/pages/Task.tsx'}], {labels:[]});
  f.validate = () => ({needsBrowser:false});
  await reconcileBrowserLabels(f); await reconcileUiMergeStatus(f);
  assert.deepEqual(f.removed,[]); assert.deepEqual(f.added,[]); assert.equal(f.statuses[0].state,'pending');
});
test('incomplete diff and stale events do not remove any labels', async () => {
  const f = fixture([{filename:'backend/api.ts'}], {changed_files:2});
  await assert.rejects(reconcileBrowserLabels(f), /complete changed-file diff unavailable/);
  assert.deepEqual(f.removed,[]);
  const stale = fixture([{filename:'backend/api.ts'}]);
  stale.github.rest.pulls.get = async () => ({data:{...stale.pr,head:{sha:'changed'}}});
  await assert.rejects(reconcileBrowserLabels(stale), /PR changed/); assert.deepEqual(stale.removed,[]);
});
test('holds, drafts and protected PRs keep labels untouched', async () => {
  for (const changes of [{draft:true},{number:1072},{number:1406},{labels:[{name:'hold'},{name:'needs-review'}]}]) {
    const f=fixture([{filename:'backend/api.ts'}],changes); await reconcileBrowserLabels(f); assert.deepEqual(f.removed,[]); assert.deepEqual(f.added,[]);
  }
});

test('existing structured product flags and review-app blockers survive non-UI classification', async () => {
  for (const signal of ['flag', 'review', 'app']) {
    const f = fixture([{filename:'backend/api.ts'}]);
    f.github.paginate = async endpoint => endpoint === 'files' ? [{filename:'backend/api.ts'}] : endpoint === 'pulls' ? [f.pr] : endpoint === 'comments' && signal === 'flag' ? [{user:{login:'whagons-ai-review-codex[bot]'},body:'<!-- whagons-product-fit-flag v1 id=purpose state=open -->'}] : endpoint === 'reviews' && signal === 'review' ? [{user:{login:'desarso'},state:'CHANGES_REQUESTED'}] : endpoint === 'statuses' && signal === 'app' ? [{context:'UI merge policy / staging',state:'pending',target_url:'https://review.whagons.com/?pr=7',description:'Open product-fit flag'}] : [];
    await reconcileUiMergeStatus(f); assert.equal(f.statuses[0].state,'pending',signal);
  }
});

test('only the dashboard is rendered UI; SDK packages, runtime and docs are not', () => {
  for (const filename of ['apps/dashboard/src/App.tsx', 'apps/dashboard/src/styles/main.css']) assert.equal(classifyGonvexDiff([filename]).isUi, true, filename);
  for (const filename of ['packages/react/src/index.tsx', 'packages/client/src/components/Thing.tsx', 'apps/docs/src/app/page.tsx', 'apps/query-cache-test/src/App.tsx', 'rust/crates/runtime/src/control.rs'])
    assert.equal(classifyGonvexDiff([filename]).isUi, false, filename);
  assert.equal(classifyGonvexDiff({ files: ['packages/react/src/index.tsx'], expectedCount: 2 }).classification, 'unknown');
});

test('an SDK-only diff passes the UI merge policy', async () => {
  const f = fixture([{filename:'packages/react/src/index.tsx'}, {filename:'rust/crates/runtime/src/control.rs'}]);
  await reconcileBrowserLabels(f); await reconcileUiMergeStatus(f);
  assert.equal(f.statuses[0].state, 'success');
});
