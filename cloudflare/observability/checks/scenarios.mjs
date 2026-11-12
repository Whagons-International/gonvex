import { CheckError, ids, requireCheck } from './contract.mjs';

function port(a, name) { requireCheck(typeof a?.[name] === 'function', `missing-${name}`, 'blocked'); return a[name].bind(a); }
export async function readScenario(c, a) {
  // Application Query through authenticated SDK/server handler, never cached rows or SQL endpoint.
  const r = await port(a, 'applicationRead')(c.identities.reader, { signal: c.signal, fresh: true });
  requireCheck(r?.authenticated === true && r?.storage === 'tenant-sqlite' && r?.fresh === true && r?.expected === true, 'authoritative-read');
}
export async function realtimeScenario(c, a) {
  const r = await port(a, 'realtimeAuth')(c.identities.reader, { signal: c.signal });
  requireCheck(r?.authenticated === true && r?.protocolReady === true, 'realtime-auth');
}
export async function taskScenario(c, a) {
  for (const name of ['openReader','mutate','freshRead','cleanup']) port(a, name);
  requireCheck(c.guard, 'cleanup-ledger-required', 'blocked');
  const fixtureId = `synthetic-${c.operationId}`;
  await c.guard.begin(Date.now(), fixtureId);
  let cleaned = false, reader;
  const operations = c.operations;
  try {
    // Subscribe BEFORE the writer; a writer echo cannot be the second client's applied result.
    reader = await a.openReader(c.identities.reader, { signal: c.signal, fixtureId });
    requireCheck(reader?.principal === c.identities.reader && typeof reader.waitApplied === 'function', 'independent-reader');
    for (const kind of ['create','update']) {
      requireCheck(!c.signal.aborted && !c.guard.state.paused,'writes-paused','unknown');
      const operation = ids(), begin = performance.now();
      const record = { operationId:operation.operationId, kind, attempted:true, deadlineMs:c.deadlineMs, status:'fail' };
      operations.push(record);
      const receipt = await a.mutate(c.identities.writer, { kind, fixtureId, ...operation, signal: c.signal });
      requireCheck(receipt?.durable === true && receipt?.operationId === operation.operationId && Number.isSafeInteger(receipt.revision) && receipt.revision > 0, 'durable-confirmation');
      const durableMs = performance.now() - begin;
      const applied = await reader.waitApplied({ fixtureId, kind, revision: receipt.revision, signal: c.signal });
      requireCheck(applied?.principal === c.identities.reader && applied?.revision === receipt.revision && applied?.matches === true && applied?.applied === true, 'second-client-applied');
      const appliedMs = performance.now() - begin;
      const fresh = await a.freshRead(c.identities.reader, { fixtureId, kind, signal: c.signal, reload: true });
      requireCheck(fresh?.storage === 'tenant-sqlite' && fresh?.fresh === true && fresh?.matches === true && fresh?.revision >= receipt.revision, 'fresh-persistence');
      requireCheck(!c.signal.aborted && appliedMs <= c.deadlineMs, 'operation-deadline');
      Object.assign(record, { durableMs, appliedMs, status:'pass' });
    }
    return { operations };
  } catch (e) {
    // Preserve attempted operations even if the writer reply is ambiguous or observer times out.
    if (e instanceof CheckError) e.operations = operations;
    throw e;
  } finally {
    // Abort must not be ignored to start a fresh write; cleanup has its own bounded adapter contract.
    try { await reader?.close?.(); cleaned = await a.cleanup(c.identities.writer, { fixtureId, timeoutMs: 5000 }) === true; }
    catch { cleaned = false; }
    await c.guard.finish(fixtureId, cleaned);
    requireCheck(cleaned, 'cleanup-failed', 'unknown');
  }
}
export async function fileScenario(c, a) {
  requireCheck(c.fileBytes instanceof Uint8Array && c.fileBytes.byteLength <= 1024 && c.fileBytes.byteLength > 0, 'file-budget', 'blocked');
  requireCheck(c.fileIntervalMs >= 900000 && c.fileSideEffectsApproved === true && c.guard, 'file-preflight', 'blocked');
  await c.guard.begin(Date.now(), c.operationId);
  let cleaned = false;
  try {
    const f = await port(a, 'privateFileRoundtrip')(c.identities.writer, c.fileBytes, { signal:c.signal, operationId:c.operationId });
    requireCheck(f?.bytes instanceof Uint8Array && Buffer.from(f.bytes).equals(Buffer.from(c.fileBytes)), 'file-bytes');
    const denied = await port(a, 'unauthorizedFileRead')(c.identities.unauthorized, { file: f.privateHandle, signal:c.signal });
    requireCheck(denied?.denied === true && denied?.bytesReturned === 0, 'private-file-owner');
  } finally {
    try { cleaned = await port(a, 'cleanupFile')(c.operationId, { timeoutMs:5000 }) === true; } catch {}
    await c.guard.finish(c.operationId, cleaned);
    requireCheck(cleaned, 'file-cleanup-failed', 'unknown');
  }
}
export async function jobScenario(c, a) {
  requireCheck(Number.isFinite(c.jobDeadlineMs) && c.jobDeadlineMs > 0 && typeof c.jobDeadlineSource === 'string' && c.jobDeadlineSource.length > 0, 'job-deadline-required', 'blocked');
  // Read an already declared job. Creating jobs requires a separate side-effect approval/ledger.
  const r = await port(a, 'jobTerminal')({ signal:c.signal, deadlineMs:c.jobDeadlineMs, deadlineSource:c.jobDeadlineSource });
  requireCheck(r?.durable === true && r?.terminal === 'completed' && r?.elapsedMs <= c.jobDeadlineMs && r?.deadlineSource === c.jobDeadlineSource, 'job-terminal-deadline');
}
