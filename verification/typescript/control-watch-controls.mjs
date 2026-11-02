import assert from 'node:assert/strict';

export function controlWatchControls(source, method) {
  const change = (before, after) => {
    assert.equal(method.split(before).length, 2, before);
    return source.replace(method, method.replace(before, after));
  };
  const scopeObserver = method.slice(method.indexOf('      const unsubscribeScope ='), method.indexOf('      stop ='));
  assert(scopeObserver.includes('this.onSessionScopeChange'));
  return [
    { name: 'candidate', pattern: '.', passed: 38, candidate: source },
    { name: 'clear-uses-receipt-activation', pattern: '^scope clear survives.*before-sdk$', failed: 1,
      candidate: change(scopeObserver, scopeObserver.replaceAll('this.sessionScopeGeneration', 'this.receiptReconciliationGeneration')),
      failure: /native close suppressed the new owner's scope-clear notification/ },
    { name: 'clear-uses-transport-owner', pattern: '^scope clear survives.*after-sdk-reconnect$', failed: 1,
      candidate: change('notify(captureSnapshotOwner());', 'notify();'),
      failure: /native close suppressed the new owner's scope-clear notification/ },
    { name: 'skip-scope-observer', pattern: '^Control Query watch snapshot clears immediately', failed: 3,
      candidate: change(scopeObserver, '      const unsubscribeScope = () => undefined;\n'),
      failure: /new owner was not notified that the snapshot cleared/ },
    { name: 'skip-inactive-owner-read', pattern: '^inactive Control Query watch snapshot clears', failed: 3,
      candidate: change('      getSnapshot() {\n        clearStaleSnapshot();', '      getSnapshot() {'),
      failure: /inactive Control Query snapshot crossed its last owner/ },
    { name: 'skip-watch-restart', pattern: '^inactive Control Query watch resumes', failed: 3,
      candidate: change('      onUpdate(listener: WatchUpdateHandler) {\n        start();', '      onUpdate(listener: WatchUpdateHandler) {'),
      failure: /resumed Control Query sent no subscription/ },
    { name: 'notify-before-scope-installed', pattern: '^Control Query watch snapshot clears immediately', failed: 3,
      candidate: change('        queueMicrotask(() => {\n          if (activation === this.sessionScopeGeneration && !this.manuallyClosed) notify(captureSnapshotOwner());\n        });',
        '        if (activation === this.sessionScopeGeneration && !this.manuallyClosed) notify(captureSnapshotOwner());'),
      failure: /new owner was not notified that the snapshot cleared/ },
    { name: 'unfenced-data-notification', pattern: '^queued Control Query watch notifications.*close$', failed: 1,
      candidate: change('          if (!current()) return;\n          if (listeners.has(listener))', '          if (listeners.has(listener))'),
      failure: /closed Control Query watch emitted a queued notification/ },
    { name: 'unfenced-initial-notification', pattern: '^late Control Query watch listener.*close$', failed: 1,
      candidate: change('if (current() && listeners.has(listener)) listener();', 'if (listeners.has(listener)) listener();'),
      failure: /closed Control Query watch emitted an initial notification/ },
    { name: 'check-only-before-watch-fanout', pattern: '^Control Query watch listener fanout.*close$', failed: 1,
      candidate: change('        for (const listener of Array.from(listeners)) {\n          if (!current()) return;',
        '        if (!current()) return;\n        for (const listener of Array.from(listeners)) {'),
      failure: /closed Control Query watch called a later listener/ },
    { name: 'tie-cache-to-connection', pattern: '^Control Query watch retains its valid owner snapshot', failed: 1,
      candidate: source.replace(method, method.replaceAll('captureSnapshotOwner()', 'captureOwner()')),
      failure: /same-owner reconnect discarded an authorized Control Query cache/ },
    { name: 'late-session-scope-invalidation', pattern: '^inactive Control Query cache invalidates before scope observers', failed: 1,
      candidate: (() => {
        const allocation = '    this.sessionScopeGeneration = Symbol();';
        const observers = '    for (const handler of this.sessionScopeHandlers) handler();';
        assert.equal(source.split(allocation).length, 2); assert.equal(source.split(observers).length, 2);
        return source.replace(allocation, '').replace(observers, observers + '\n' + allocation);
      })(),
      failure: /scope observer read an inactive Control Query cache before owner invalidation/ },
    { name: 'reuse-visibility-activation', pattern: '^inactive Control Query watch rejects a previous visibility', failed: 1,
      candidate: (() => {
        const before = '    this.sessionScopeGeneration = Symbol();';
        assert.equal(source.split(before).length, 2);
        return source.replace(before, '    this.sessionScopeGeneration = this.sessionScopeGeneration;');
      })(),
      failure: /inactive Control Query cache survived a different visibility activation/ },
  ];
}
