import Module from 'node:module';

// Playwright 1.61 records Firefox page requests, but worker WebSockets can emit
// Page.webSocketOpened without a corresponding request/response record. Leave
// page instrumentation intact; ignore only these untracked worker events.
export function installFirefoxWorkerWebSocketWorkaround() {
  const original = Module._extensions['.js'];
  let applied = false;
  Module._extensions['.js'] = function(module, filename) {
    if (!filename.endsWith('/playwright-core/lib/coreBundle.js')) return original(module, filename);
    const compile = module._compile;
    module._compile = function(source, file) {
      const before = `const request2 = this._webSocketRequests.get(event.requestId);\n        assert(request2);\n        const response2 = this._webSocketResponses.get(event.requestId);\n        assert(response2);`;
      if (!source.includes(before)) throw new Error('Firefox workaround needs review for this Playwright version');
      const after = `const request2 = this._webSocketRequests.get(event.requestId);\n        const response2 = this._webSocketResponses.get(event.requestId);\n        if (!request2 || !response2) {\n          this._webSocketRequests.delete(event.requestId);\n          this._webSocketResponses.delete(event.requestId);\n          return;\n        }`;
      applied = true;
      return compile.call(this, source.replace(before, after), file);
    };
    try { return original(module, filename); }
    finally { module._compile = compile; Module._extensions['.js'] = original; }
  };
  return () => applied;
}
