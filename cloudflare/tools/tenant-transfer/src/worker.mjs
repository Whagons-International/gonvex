// P03 standalone transport host. AssemblyTenant uses the same transport on its own storage.
import { DurableObject } from 'cloudflare:workers';
import { TenantTransfer } from './transport.mjs';
export class TenantStore extends DurableObject {
  transport = new TenantTransfer(this.ctx);
  fetch(request) { return this.transport.fetch(request); }
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const match = url.pathname.match(/^\/t\/([A-Za-z0-9_.-]{1,128})\/([a-z]+)$/);
    if (!match) return new Response('not found', { status: 404 });
    const expected = env.TRANSFER_TOKEN;
    if (!expected || request.headers.get('authorization') !== `Bearer ${expected}`) {
      return new Response('unauthorized', { status: 401 });
    }
    const stub = env.TENANT_STORE.get(env.TENANT_STORE.idFromName(match[1]));
    return stub.fetch(request);
  },
};
