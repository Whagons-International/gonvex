// Required-set refusal/recovery through the real thin protocol consumer.
import test from 'node:test';
import assert from 'node:assert/strict';
import { ThinClient, RowPool } from './thin-client.mjs';
const paths = ['presence.list', 'workspaces.list', 'statuses.statuses', 'tags.tags', 'members.listMembers', 'members.listMemberTeams', 'taskResources.taskUsers', 'taskResources.taskTags', 'workspacePreferences.workspacePreferences', 'tasks.recentWorkspaceTasks', 'notifications.listNotifications'];
test('required set retains cached cursors through refusal and only becomes ready after all recover', t => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  t.mock.method(Math, 'random', () => 0);
  const sent = [];
  const c = new ThinClient('ws://unused', { project: 'p', clientContract: { version: 1 }, pool: new RowPool() });
  c.socket = { readyState: 1, send: s => sent.push(JSON.parse(s)), close() {} };
  c.auth = { token: 't', tenant: 'x' }; c.authReady = true;
  c.serverCapabilities = { replicaWatermark: 1, controlWatermark: 1 };
  try {
    const watches = paths.map((path, i) => c.watchReplica({path, replica: {table: `table_${i}`, key: '_id'}}));
    t.mock.timers.tick(1); c.flushOpens();
    const subs = [...c.subscriptions.values()];
    assert.equal(subs.length, paths.length);
    const cursor = {epoch: 'e', revision: 7};
    for (const sub of subs) {
      c.dispatch({type: 'replica.snapshot', id: sub.id, result: [{_id: 'cached'}], cursor, digest: 'd'});
      c.dispatch({type: 'replica.ready', id: sub.id, cursor, digest: 'd', truncated: false});
    }
    assert.ok(watches.every(w => w.status().isUpToDate));
    for (const delay of [200, 400, 800, 1600, 3200, 4000, 4000]) {
      for (const sub of subs) c.dispatch({type: 'replica.error', id: sub.id, error: 'replica snapshot admission overloaded (too many pending replica opens); retry'});
      assert.equal(watches.every(w => w.status().isUpToDate), false);
      for (const sub of subs) { assert.deepEqual(sub.result(), [{_id: 'cached'}]); assert.deepEqual(sub.readyCursor, cursor); }
      const n = sent.length;
      t.mock.timers.tick(delay - 1); c.flushOpens();
      assert.equal(sent.length, n);
      t.mock.timers.tick(1); c.flushOpens();
      const opens = sent.slice(n).flatMap(m => m.opens ?? []);
      assert.equal(opens.length, paths.length);
      // The existing thin driver deliberately cold-opens normal retries; its
      // saved cursor remains available for its full-integrity reconciliation.
      for (const open of opens) { assert.equal(open.cursor, undefined); assert.equal(open.digest, 'xor1:' + '0'.repeat(64)); }
    }
    for (const sub of subs.slice(0,-1)) { c.dispatch({type: 'replica.snapshot', id: sub.id, result: [{_id: 'cached'}], cursor, digest: 'd'}); c.dispatch({type: 'replica.ready', id: sub.id, cursor, digest: 'd', truncated: false}); }
    assert.equal(watches.every(w => w.status().isUpToDate), false);
    c.dispatch({type: 'replica.snapshot', id: subs.at(-1).id, result: [{_id: 'cached'}], cursor, digest: 'd'});
    c.dispatch({type: 'replica.ready', id: subs.at(-1).id, cursor, digest: 'd', truncated: false});
    assert.ok(watches.every(w => w.status().isUpToDate));
  } finally { c.close(); }
});
