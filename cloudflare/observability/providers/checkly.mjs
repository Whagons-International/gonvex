import { regionalDefinitions, config } from './config.mjs';
import { requireCheck } from '../checks/contract.mjs';

/** Inject Checkly constructs after an owned pinned install. This file has no deploy/login/network call.
 * API checks are limited to unauthenticated reachability. Semantic checks use a portable runner entrypoint.
 * Runtime/entrypoint/target validation is mandatory even though all returned constructs are disabled.
 */
export function defineCheckly({ ApiCheck, BrowserCheck, Frequency, RetryStrategyBuilder, entrypoints, origin, runtimeId }) {
  requireCheck(/^https:\/\//.test(origin ?? '') && runtimeId && entrypoints && Frequency && RetryStrategyBuilder?.singleRetry, 'checkly-bindings-required', 'blocked');
  const target=new URL(origin);
  requireCheck(target.protocol==='https:' && !target.username && !target.password && !target.search && !target.hash && target.pathname==='/', 'checkly-target-blocked','blocked');
  return regionalDefinitions().map(c => {
    requireCheck(Frequency[`EVERY_${c.intervalSeconds/60}M`] !== undefined,'checkly-frequency-missing','blocked');
    const common = { name:c.key, activated:false, frequency:Frequency[`EVERY_${c.intervalSeconds/60}M`], locations:c.locations, alertChannels:[], muted:true, retryStrategy:RetryStrategyBuilder.singleRetry({baseBackoffSeconds:5,sameRegion:true}), tags:['m05-foundation','synthetic-only'], runtimeId };
    if (c.kind === 'api') return new ApiCheck(c.key, { ...common, request:{ method:'GET',url:origin,followRedirects:false,assertions:[{ source:'STATUS_CODE',comparison:'EQUALS',target:'200' }] } });
    requireCheck(typeof entrypoints[c.id] === 'string' && entrypoints[c.id].length > 0, 'checkly-entrypoint-missing', 'blocked');
    return new BrowserCheck(c.key, { ...common, code:{ entrypoint:entrypoints[c.id] } });
  });
}
export const portability = { checkedOn:'2026-11-12', source:'https://www.checklyhq.com/docs/detect/synthetic-monitoring/browser-checks/playwright-support/',
  supported:['JS/TS','custom fixtures','API testing','subset of global config'],
  unsupported:['projects','globalSetup','globalTeardown','storageState','test retries','parallelism','sharding'],
  remaining:['actual account/plan','runtime/node/browser version','dependency bundling','SDK WebSocket','per-region scheduled observations','export heartbeat'],
  activated:config.activated,
};
