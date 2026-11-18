import { test } from 'node:test';
import assert from 'node:assert/strict';
import { cleanupProjects } from '../src/cleanup.mjs';

const fake = ({ open = null, failOn = null, openThrows = false } = {}) => {
  const deleted = [];
  return { deleted, name: 'candidate',
    async openDeployment() { if (openThrows) throw new Error('topology 503'); return open; },
    async deleteProject(project) { if (project === failOn) throw new Error('DELETE /dev/projects/p2 -> 422: busy'); deleted.push(project); } };
};

test('deletes every project the run created, in order, through the backend delete path', async () => {
  const b = fake();
  assert.deepEqual(await cleanupProjects(b, ['p1', 'p2'], { keep: false }), [{ project: 'p1', outcome: 'deleted' }, { project: 'p2', outcome: 'deleted' }]);
  assert.deepEqual(b.deleted, ['p1', 'p2']);
});

test('a backend without deleteProject (state thrown away with its process) is left alone', async () => {
  assert.deepEqual(await cleanupProjects({ name: 'reference' }, ['p1'], { keep: false }), []);
  assert.deepEqual(await cleanupProjects(fake(), [], { keep: false }), []);
});

test('an open deployment fence, or an unreadable one, keeps every project rather than orphaning a publication', async () => {
  const open = fake({ open: { id: 'd1', fence: 4 } });
  assert.deepEqual(await cleanupProjects(open, ['p1'], { keep: false }), [{ project: 'p1', outcome: 'kept', reason: 'deployment d1 is still open' }]);
  assert.deepEqual(open.deleted, []);
  const unknown = fake({ openThrows: true });
  const [r] = await cleanupProjects(unknown, ['p1'], { keep: false });
  assert.equal(r.outcome, 'kept'); assert.match(r.reason, /topology 503/); assert.deepEqual(unknown.deleted, []);
});

test('one failed delete is reported and does not stop the others', async () => {
  const b = fake({ failOn: 'p2' });
  const out = await cleanupProjects(b, ['p1', 'p2', 'p3'], { keep: false });
  assert.deepEqual(out.map(r => r.outcome), ['deleted', 'failed', 'deleted']);
  assert.match(out[1].reason, /422/);
  assert.deepEqual(b.deleted, ['p1', 'p3']);
});

test('GONVEX_CONFORMANCE_KEEP_PROJECTS keeps everything', async () => {
  const b = fake();
  assert.deepEqual((await cleanupProjects(b, ['p1'], { keep: true })).map(r => r.outcome), ['kept']);
  assert.deepEqual(b.deleted, []);
});
