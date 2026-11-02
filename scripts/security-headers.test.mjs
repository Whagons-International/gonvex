import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

// nginx discards inherited add_header directives when a child defines any.
test('every nginx response header scope retains frame and MIME protection', () => {
  const config = readFileSync(new URL('../apps/dashboard/nginx.conf', import.meta.url), 'utf8');
  const scopes = [], stack = [];
  for (const line of config.split('\n').map(line => line.replace(/#.*/, '').trim())) {
    if (line.endsWith('{')) { const scope = []; stack.push(scope); scopes.push(scope); }
    if (line.startsWith('add_header ')) stack.at(-1).push(line);
    if (line === '}') stack.pop();
  }
  for (const headers of scopes.filter(scope => scope.length)) {
    assert.ok(headers.includes(`add_header Content-Security-Policy "frame-ancestors 'none'" always;`));
    assert.ok(headers.includes('add_header X-Frame-Options "DENY" always;'));
    assert.ok(headers.includes('add_header X-Content-Type-Options "nosniff" always;'));
  }
  assert.ok(scopes.some(scope => scope.length));
});
