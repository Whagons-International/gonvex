import { runCheck, CheckError } from '../checks/contract.mjs';
import { readScenario, realtimeScenario, taskScenario, fileScenario, jobScenario } from '../checks/scenarios.mjs';
import { browserScenario } from '../checks/browser.mjs';
/** Same function for a local runner or Checkly spec. No ambient endpoint or credential discovery.
 * Each entrypoint explicitly binds actual app paths/identities; missing configuration is BLOCKED.
 */
export function dispatch(context, bindings, guard) {
  const { check } = context;
  return runCheck(context, check, async c => {
    if (['dns','tls','http','bootstrap'].includes(check)) {
      if (!bindings?.network?.[check]) throw new CheckError('network-config-missing','blocked');
      return bindings.network[check](c);
    }
    const a=bindings?.application;
    if (!a) throw new CheckError('application-config-missing','blocked');
    switch(check) {
      case 'sqlite-read': case 'api': return readScenario(c,a);
      case 'auth': case 'realtime': return realtimeScenario(c,a);
      case 'sdk-task': return taskScenario(c,a);
      case 'browser-task': return browserScenario(c,bindings.browser);
      case 'browser-bootstrap': {
        if (typeof a.browserBootstrap!=='function') throw new CheckError('browser-bootstrap-missing','blocked');
        const ok=await a.browserBootstrap(c);
        if (ok!==true) throw new CheckError('browser-bootstrap');
        return;
      }
      case 'files': return fileScenario(c,a);
      case 'job': return jobScenario(c,a);
      default: throw new CheckError('unknown-check','blocked');
    }
  },guard);
}
