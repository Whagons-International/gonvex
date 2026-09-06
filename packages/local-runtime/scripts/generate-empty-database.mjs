import { PGlite } from '@electric-sql/pglite';
import { createHash } from 'node:crypto';
import { readFile, writeFile } from 'node:fs/promises';
const pkg = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
const db = new PGlite({ initDbStartParams: ['--wal-segsize=1'], postgresqlconf: ['shared_buffers=1MB', 'max_connections=1', 'superuser_reserved_connections=0', 'max_wal_senders=0', 'max_worker_processes=0', 'max_parallel_workers=0'] });
try {
  await db.waitReady;
  const bytes = Buffer.from(await (await db.dumpDataDir('gzip')).arrayBuffer());
  await writeFile(new URL('../assets/empty-database.b64', import.meta.url), bytes.toString('base64').match(/.{1,76}/g).join('\n') + '\n');
  await writeFile(new URL('../assets/empty-database.json', import.meta.url), JSON.stringify({
    pgliteVersion: pkg.dependencies['@electric-sql/pglite'], sha256: createHash('sha256').update(bytes).digest('hex'),
    description: 'Empty PGlite cluster, no application schemas or tenant data.',
  }, null, 2) + '\n');
} finally { await db.close(); }
