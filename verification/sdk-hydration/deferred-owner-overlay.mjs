// SOURCE ONLY. Apply to the owned shipping JS AFTER transplanting the tested
// core hydration members. This module performs no I/O/build on import.
import { createHash } from 'node:crypto';
const hash = text => createHash('sha256').update(text).digest('hex');
export const changedMembers = [
  'deferredReplicaSubscriptionOwners (new private field)',
  'subscribeReplicaTransport', 'releaseDeferredReplicaOpens',
  'activateReplicaDirective', 'sendReplicaOpen', 'close', 'resetReplicaScopeState',
  'retireDeferredReplicaIntents (new internal helper)',
  'deferredReplicaOwner (new internal helper)',
  'ownsDeferredReplicaOwner (new internal helper)',
  'bindDeferredReplicaIntents (new internal helper)',
  'drainDeferredReplicaIntents (new internal helper)',
];
export function applyDeferredOwnerOverlay(input) {
  let source = input;
  const edits = [];
  const replace = (name, before, after) => {
    if (source.split(before).length !== 2) throw Error(`ambiguous/missing shipping anchor: ${name}`);
    source = source.replace(before, after);
    edits.push({ member: name, beforeSha256: hash(before), afterSha256: hash(after) });
  };
  replace('private owner map', '    replicaReady = Promise.resolve();',
    '    deferredReplicaSubscriptionOwners = new WeakMap();\n    replicaReady = Promise.resolve();');
  replace('subscribeReplicaTransport signature',
    '    subscribeReplicaTransport(ref, args = {}, onMessage, metadataOnly = false) {',
    '    subscribeReplicaTransport(ref, args = {}, onMessage, metadataOnly = false, deferredOwner) {\n        if (deferredOwner && !this.ownsDeferredReplicaOwner(deferredOwner)) return () => {};');
  replace('defer entry owner',
    '            const pending = { ref, args, onMessage, metadataOnly, cancelled: false, release: undefined };',
    `            const owner = this.deferredReplicaOwner();\n            const pending = { ref, args, onMessage, metadataOnly, cancelled: false, release: undefined,\n                phase: owner ? 'admitted' : 'provisional', owner, released: false, task: undefined };`);
  replace('deferred cancellation',
    '                pending.cancelled = true;\n                pending.release?.();',
    '                pending.cancelled = true;\n                this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== pending);\n                pending.release?.();');
  replace('retained subscription owner',
    '            existing.listeners.add(onMessage);\n            // A reset may arrive during the retention grace period with no listeners.',
    '            if (deferredOwner) this.deferredReplicaSubscriptionOwners.set(existing, deferredOwner);\n            else this.deferredReplicaSubscriptionOwners.delete(existing);\n            existing.listeners.add(onMessage);\n            // A reset may arrive during the retention grace period with no listeners.');
  replace('new subscription owner',
    '        this.replicaSubscriptions.set(key, subscription);',
    '        if (deferredOwner) this.deferredReplicaSubscriptionOwners.set(subscription, deferredOwner);\n        this.replicaSubscriptions.set(key, subscription);');
  replace('release deferred requests', `    releaseDeferredReplicaOpens() {
        this.replicaOpenDeferral = undefined;
        const pending = this.deferredReplicaOpens.splice(0);
        for (const open of pending) {
            if (!open.cancelled)
                open.release = this.subscribeReplicaTransport(open.ref, open.args, open.onMessage, open.metadataOnly);
        }
    }`, `    releaseDeferredReplicaOpens() {
        this.replicaOpenDeferral = undefined;
        for (const open of this.deferredReplicaOpens) open.released = true;
        this.drainDeferredReplicaIntents();
    }
    deferredReplicaOwner() {
        return this.hasAuthoritativeReplicaScope && !this.manuallyClosed
            ? { scope: this.replicaScope, activation: this.sessionScopeGeneration }
            : undefined;
    }
    ownsDeferredReplicaOwner(owner) {
        return !this.manuallyClosed && this.hasAuthoritativeReplicaScope
            && owner.scope === this.replicaScope && owner.activation === this.sessionScopeGeneration;
    }
    retireDeferredReplicaIntents() {
        for (const open of this.deferredReplicaOpens) {
            if (open.owner && !this.ownsDeferredReplicaOwner(open.owner)) open.cancelled = true;
        }
        this.deferredReplicaOpens = this.deferredReplicaOpens.filter(open => !open.cancelled);
    }
    bindDeferredReplicaIntents() {
        const owner = this.deferredReplicaOwner();
        if (!owner) return;
        // Undefined means provisional: first admission binds it exactly once.
        // An admitted stale owner is never reinterpreted as provisional.
        for (const open of this.deferredReplicaOpens) {
            if (!open.cancelled && open.phase === 'provisional') {
                open.owner = owner;
                open.phase = 'admitted';
            }
        }
        this.drainDeferredReplicaIntents();
    }
    drainDeferredReplicaIntents() {
        for (const open of [...this.deferredReplicaOpens]) {
            if (open.cancelled || this.manuallyClosed
                || (open.owner && !this.ownsDeferredReplicaOwner(open.owner))) {
                open.cancelled = true;
                this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== open);
                continue;
            }
            if (!open.released || !open.owner || open.task) continue;
            const ready = this.replicaReady;
            const task = { promise: Promise.resolve() };
            open.task = task;
            // Keep the deferred intent (and its original cancellation handle)
            // until current-scope durable hydration is complete.
            task.promise = ready.then(() => {
                if (open.task !== task) return;
                open.task = undefined;
                if (open.cancelled || !this.deferredReplicaOpens.includes(open)) return;
                if (!this.ownsDeferredReplicaOwner(open.owner)) {
                    open.cancelled = true;
                    this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== open);
                    return;
                }
                if (this.replicaReady !== ready) { this.drainDeferredReplicaIntents(); return; }
                this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== open);
                open.release = this.subscribeReplicaTransport(open.ref, open.args,
                    open.onMessage, open.metadataOnly, open.owner);
            }, () => {
                if (open.task !== task) return;
                open.task = undefined;
                open.cancelled = true;
                this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== open);
            }).catch(error => {
                open.cancelled = true;
                this.deferredReplicaOpens = this.deferredReplicaOpens.filter(entry => entry !== open);
                if (open.owner && this.ownsDeferredReplicaOwner(open.owner)) this.rejectReplicaDirective(error);
            });
        }
    }`);
  replace('first admission binding',
    '        this.hasAuthoritativeReplicaScope = true;\n        this.setReplicaReady(this.replica.activateScope(scope));',
    '        this.hasAuthoritativeReplicaScope = true;\n        this.setReplicaReady(this.replica.activateScope(scope));\n        this.bindDeferredReplicaIntents();');
  replace('same scope admission binding',
    '        if (this.hasAuthoritativeReplicaScope && this.replicaScope === scope && this.replicaDefinitionEpoch === directive.epoch) {\n            await this.outboxJournalReady;',
    '        if (this.hasAuthoritativeReplicaScope && this.replicaScope === scope && this.replicaDefinitionEpoch === directive.epoch) {\n            this.bindDeferredReplicaIntents();\n            await this.outboxJournalReady;');
  replace('retire admitted intents on scope reset',
    '        this.sessionScopeGeneration = Symbol();',
    '        this.sessionScopeGeneration = Symbol();\n        this.retireDeferredReplicaIntents();');
  replace('active deferred subscription guard',
    '    sendReplicaOpen(subscription) {',
    '    sendReplicaOpen(subscription) {\n        const deferredOwner = this.deferredReplicaSubscriptionOwners.get(subscription);\n        if (deferredOwner && !this.ownsDeferredReplicaOwner(deferredOwner)) return;');
  // This anchor belongs to the tested core close cleanup only. Do not copy
  // the whole method over shipping auth/visibility behavior.
  replace('close deferred cancellation',
    '        this.replicaOpenHydrations.clear();\n        if (this.querySubscribeFlushTimer)',
    '        this.replicaOpenHydrations.clear();\n        for (const open of this.deferredReplicaOpens) open.cancelled = true;\n        this.deferredReplicaOpens.length = 0;\n        if (this.querySubscribeFlushTimer)');
  return { source, provenance: { inputJsSha256: hash(input), outputJsSha256: hash(source),
    changedMembers, edits, publicApiChanged: false, status: 'requires separately granted artifact execution/tests' } };
}
