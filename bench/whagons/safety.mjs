export function loopback(value, protocols = ['http:', 'ws:']) {
  const u = new URL(value);
  if (!/^\w+:\/\/(127\.0\.0\.1|\[::1\])(?=[:/]|$)/.test(value) || !protocols.includes(u.protocol) || !['127.0.0.1', '[::1]'].includes(u.hostname) || u.username || u.password) {
    throw new Error('Load testing requires a literal loopback URL without credentials');
  }
  return u;
}
export function quantiles(values) {
  const xs = [...values].sort((a,b)=>a-b);
  return Object.fromEntries([50,95,99].map(p=>[`p${p}`, xs.length ? xs[Math.ceil(xs.length*p/100)-1] : null]));
}
