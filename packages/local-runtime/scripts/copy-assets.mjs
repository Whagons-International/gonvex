import { createHash } from 'node:crypto';
import { copyFile, readFile } from 'node:fs/promises';
const manifest = JSON.parse(await readFile(new URL('../assets/empty-database.json', import.meta.url), 'utf8'));
const pkg = JSON.parse(await readFile(new URL('../package.json', import.meta.url), 'utf8'));
if (pkg.dependencies['@electric-sql/pglite'] !== manifest.pgliteVersion) throw new Error('Regenerate the empty PGlite cluster when upgrading the engine.');
const data = Buffer.from(await readFile(new URL('../assets/empty-database.b64', import.meta.url), 'utf8'), 'base64');
if (createHash('sha256').update(data).digest('hex') !== manifest.sha256) throw new Error('Empty PGlite cluster checksum mismatch.');
await copyFile(new URL('../assets/empty-database.b64', import.meta.url), new URL('../dist/empty-database.b64', import.meta.url));
