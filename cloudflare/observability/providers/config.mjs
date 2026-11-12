// Proposal only. One Checkly construct per location prevents location rotation hiding gaps.
export const regions = ['us-east-1', 'us-west-1', 'eu-west-1'];
export const checks = [
  ['dns','Web',60,'browser'], ['tls','Web',60,'browser'], ['http','Web',60,'api'],
  ['bootstrap','Web',60,'browser'], ['browser-bootstrap','Web',60,'browser'],
  ['auth','API',60,'browser'], ['api','API',60,'browser'], ['sqlite-read','API',60,'browser'],
  ['realtime','Sync',60,'browser'], ['sdk-task','Sync',300,'browser'], ['browser-task','Web',300,'browser'],
  ['files','Files',900,'browser'],
].map(([id,component,intervalSeconds,kind]) => ({ id,component,intervalSeconds,kind,deadlineMs:intervalSeconds === 60 ? 20000 : 60000, retryMax:1, retryBackoffMs:5000, maxConcurrent:1, warm:true }));
export const config = { schema:'gonvex.monitor-config.v1', activated:false, environment:'dev-synthetic',
  objective:{ monthly:0.999, state:'owner-proposal' }, regions, checks,
  app:{ origin:null, sdkModule:null, functionPaths:null, selectors:null, deployment:null, identities:null },
  heartbeat:{ independentHost:null, intervalSeconds:60, graceSeconds:30 },
  jobs:{ configured:[], state:'blocked-job-specific-deadlines-required' },
  maintenance:[], alertChannels:[], provider:{ preferred:'checkly', account:null, plan:null, runtimeId:null },
};
export function regionalDefinitions() {
  return regions.flatMap(region => checks.map(check => ({ ...check, key:`m05-${check.id}-${region}`, locations:[region], activated:false, alertChannels:[], environment:config.environment })));
}
/** No prices guessed. Quote must have verified plan, dates, currency, rates and backend bound.
 * Counts include a conservative one extra execution per configured retry at every location.
 */
export function estimate(days = 30, quote = null) {
  if (!Number.isInteger(days) || days < 1 || days > 31) throw new Error('bounded month required');
  const rows = regionalDefinitions().map(c => ({ check:c.id, region:c.locations[0], kind:c.kind, scheduled:Math.ceil(days*86400/c.intervalSeconds), maxWithRetries:Math.ceil(days*86400/c.intervalSeconds)*(1+c.retryMax) }));
  const counts = Object.fromEntries(['api','browser'].map(kind => [kind, rows.filter(x=>x.kind===kind).reduce((s,x)=>s+x.maxWithRetries,0)]));
  if (!quote || !quote.verifiedAt || !quote.source || !quote.plan || !quote.currency) return { days,rows,counts,cost:null,state:'blocked-unverified-plan-rates-cloudflare-billing' };
  for (const key of ['base','apiPerRun','browserPerRun','apiIncluded','browserIncluded','cloudflareUpperBound','notificationUpperBound']) if (!Number.isFinite(quote[key]) || quote[key]<0) throw new Error('incomplete quote');
  const cost = quote.base + Math.max(0,counts.api-quote.apiIncluded)*quote.apiPerRun + Math.max(0,counts.browser-quote.browserIncluded)*quote.browserPerRun + quote.cloudflareUpperBound + quote.notificationUpperBound;
  return { days,rows,counts,cost,currency:quote.currency,quoteSource:quote.source,verifiedAt:quote.verifiedAt,state:'estimate-not-billing' };
}
