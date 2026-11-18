/**
 * A minimal in-memory S3-compatible object store for the Rust reference (path-style, SigV4 not verified).
 *
 * Rust only enables `ctx.storage` with an S3 endpoint and bucket. A deployed or local assembly has R2, so without
 * this the `actions` storage step compared "storage is not configured" with a configured store. PUT stores the
 * body and content type; GET/HEAD answer the object or 404; DELETE removes it. Nothing leaves the process.
 */
import { createServer } from 'node:http';
import { createHash } from 'node:crypto';

export async function startS3Stub() {
  const objects = new Map();
  const server = createServer((req, res) => {
    const key = decodeURIComponent((req.url ?? '/').split('?')[0]);
    const chunks = [];
    req.on('data', (c) => chunks.push(c));
    req.on('end', () => {
      if (req.method === 'PUT') {
        const body = Buffer.concat(chunks);
        const etag = `"${createHash('md5').update(body).digest('hex')}"`;
        objects.set(key, { body, type: req.headers['content-type'] ?? 'application/octet-stream', etag });
        res.writeHead(200, { etag }); return res.end();
      }
      if (req.method === 'DELETE') { objects.delete(key); res.writeHead(204); return res.end(); }
      const object = objects.get(key);
      if (!object) { res.writeHead(404, { 'content-type': 'application/xml' }); return res.end(req.method === 'HEAD' ? undefined : '<Error><Code>NoSuchKey</Code></Error>'); }
      res.writeHead(200, { 'content-type': object.type, 'content-length': object.body.length, etag: object.etag });
      res.end(req.method === 'HEAD' ? undefined : object.body);
    });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  return {
    endpoint: `http://127.0.0.1:${server.address().port}`,
    objects,
    close: () => new Promise((resolve) => { server.closeAllConnections?.(); server.close(() => resolve()); }),
  };
}
