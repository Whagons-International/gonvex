import type { DurableObjectNamespace as NativeNamespace } from '@cloudflare/workers-types';
declare global { interface DurableObjectNamespace extends NativeNamespace {} }
