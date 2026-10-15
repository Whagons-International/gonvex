import { engineAdmissionLane, type AdmissionLane } from "./cohort.js";
import { GonvexClient, GonvexClientError, type EngineCommand, type FunctionReference, type QueuedReducerOutcome, type CallOptions } from "../index.js";
import { engineEntryOutcome, type EngineReceipts } from "./receipts.js";
import type { JsonValue } from "@gonvex/protocol";
import type { LocalExecution, LocalTransactionResult } from "@gonvex/local-runtime";
import { StoreReducerOutbox, outboxEntryIsLive, type ReducerOutboxEntry, type IntentDependencies, type OutboxErrorClass } from "../outbox.js";
import { optimisticPatchesFromReference, type OptimisticPatch } from "../optimistic.js";

const receiptContexts = new WeakMap<GonvexClient, EngineReceipts>();

// Only engine hosts install durable command identities. Ordinary clients retain
// the established implementation and do not bundle these admission branches.
export function installEngineAdmissions(client: GonvexClient, receipts: EngineReceipts) {
  receiptContexts.set(client, receipts);
  const outbox = client["reducerOutbox"];
  const retiring = new Map<number, ReducerOutboxEntry>();
  receipts.cachedEntry = id => retiring.get(id) ?? (outbox instanceof StoreReducerOutbox ? outbox["entries"].get(id) : undefined);
  const ack = outbox.ack.bind(outbox);
  outbox.ack = async id => {
    // ack removes its memory entry before calling the store. Retain that
    // durable local outcome only until the original deletion settles.
    const entry = receipts.cachedEntry?.(id);
    if (entry) retiring.set(id, entry);
    try { return await ack(id); } finally { retiring.delete(id); }
  };
  const enqueue = outbox.enqueue.bind(outbox);
  outbox.enqueue = async intent => {
    if (intent.engineOutcome && !intent.engineOutcome.fresh) await receipts.assertCanAdmit(intent.engineOutcome.commandNow!);
    const outcome = intent.engineOutcome ? {
      result: structuredClone(intent.engineOutcome.result),
      ...(!intent.localExecution ? { commandNow: intent.engineOutcome.commandNow } : {}),
    } : undefined;
    if (outcome) receipts.admissions.set(intent.idempotencyKey!, outcome);
    try {
      const entry = await enqueue(intent);
      if (intent.engineOutcome) {
        entry.engineResult = structuredClone(outcome!.result);
        if (outcome!.commandNow !== undefined) entry.engineCommandNow = outcome!.commandNow;
        if (outbox instanceof StoreReducerOutbox) {
          const cached = outbox["entries"].get(entry.id);
          if (cached) {
            cached.engineResult = structuredClone(outcome!.result);
            if (outcome!.commandNow !== undefined) cached.engineCommandNow = outcome!.commandNow;
          }
        }
      }
      return entry;
    }
    finally { receipts.admissions.delete(intent.idempotencyKey!); }
  };
  Object.defineProperties(client, { reducer: { value: reducer }, runOptimisticReducer: { value: runOptimisticReducer } });
}
export async function runLocalReducer<T>(this: GonvexClient, ref: FunctionReference, args: JsonValue, command?: EngineCommand, optionsFresh = false, lane?: AdmissionLane): Promise<T | QueuedReducerOutcome> {
  args = structuredClone(args);
  const admittedGeneration = this["outboxScopeGeneration"];
  await this["outboxReady"];
  await this["replicaReady"];
  if (!this["localExecutor"] || !this["localBinding"]) throw new Error("This reducer requires the generated Gonvex client with local execution enabled");
  const reducerId = command?.id ?? randomID();
  const execution: LocalExecution | undefined = this["localIdentity"] ? {
    scope:this["replicaScope"],commandId:reducerId,now:command?.now ?? Date.now(),
    intentEntropy:command?.entropy ?? Array.from(crypto.getRandomValues(new Uint8Array(32)),byte=>byte.toString(16).padStart(2,'0')).join(''),
    artifactHash:this["localBinding"].artifactHash,identity:structuredClone(this["localIdentity"]),
  } : undefined;
  let preparedResult: {transaction:LocalTransactionResult; base:number; pending:string} | undefined;
  let preparedFailure: unknown;
  const prepared = this["preparationLane"].then(async () => {
    if (!execution || this["localBinding"]?.mode !== 'portable' || !this["hasAuthoritativeReplicaScope"] || this["manuallyClosed"] || this["updateRequired"] || admittedGeneration !== this["outboxScopeGeneration"]) return;
    if (this["clientContract"] && this["clientContract"].offlineMaxAgeMs !== null && !this["canSendReducerNow"]() && (Date.now()-this["lastOnlineAtMs"] > this["clientContract"].offlineMaxAgeMs || Date.now() < this["lastOnlineAtMs"])) return;
    const base = this["replica"].executionVersion();
    const pending = this["replica"].pendingReducerPatches();
    try {
      const transaction = await this["executeLocal"](ref.path,args,undefined,execution,pending,true);
      if (base !== this["replica"].executionVersion() || admittedGeneration !== this["outboxScopeGeneration"] || this["manuallyClosed"]) return;
      preparedResult = {transaction,base,pending:JSON.stringify(pending)};
    } catch (error) { preparedFailure = error; /* Recheck coverage under coordinated admission. */ }
  });
  this["preparationLane"] = prepared.catch(() => undefined);
  this["waitingLocalEdits"] += 1;
  let entered = false;
  const admit = async () => {
    entered = true;
    this["waitingLocalEdits"] -= 1;
    await prepared;
    if (admittedGeneration !== this["outboxScopeGeneration"]) throw new GonvexClientError("Session changed before local reducer execution", { code: "superseded" });
    if (!this["localIdentity"] || !this["hasAuthoritativeReplicaScope"]) throw new Error("Restore an authenticated local session before editing offline");
    if (this["manuallyClosed"]) throw new Error("Gonvex client is closed");
    if (this["updateRequired"]) throw new Error("Application update required before editing");
    if (this["clientContract"] && this["clientContract"].offlineMaxAgeMs !== null && !this["canSendReducerNow"]() && (Date.now() - this["lastOnlineAtMs"] > this["clientContract"].offlineMaxAgeMs || Date.now() < this["lastOnlineAtMs"])) throw new Error("Offline editing window expired. Reconnect before making more changes.");
    const scope = this["outboxScope"];
    if (!execution || execution.scope !== this["replicaScope"]) throw new GonvexClientError("Session changed before local reducer execution", {code:'superseded'});
    const entries = await this["reducerOutbox"].list(scope);
    const admitted = command && entries.find(entry => entry.idempotencyKey === command.id);
    const admittedOutcome = admitted && engineEntryOutcome(admitted);
    if (admittedOutcome) {
      if (admitted.path !== ref.path || JSON.stringify(admitted.args) !== JSON.stringify(args)) throw new Error("Engine command ID reused with different arguments");
      return admittedOutcome.result as T;
    }
    if (command && !optionsFresh) {
      const receipt = await receiptContexts.get(this)!.findReceipt(scope, command.id, ref.path, args);
      if (receipt) return receipt.result as T;
    }
    const sharedEntries = entries.filter(outboxEntryIsLive);
    const sharedPatches = sharedEntries?.filter(entry => entry.state !== "committed").flatMap(entry => (entry.patches ?? []).map(patch => patch.op === "delete"
      ? {entity: patch.entity ?? patch.collection!, rowId: patch.rowId, op: "delete" as const}
      : {entity: patch.entity ?? patch.collection!, rowId: patch.rowId, op: patch.op === "upsert" ? "insert" as const : patch.op, fields: patch.fields as import("@gonvex/module-sdk").JsonObject}));
    // A fully resident prediction reads the already published atomic replica.
    // Unrelated snapshots may still be persisting; waiting for those here
    // makes a small edit inherit their disk latency. The shared intent lock
    // and journal still order every edit. Preparation proves resident
    // coverage, even when a published commit invalidates its result while
    // acquiring the lock. Re-execute against the latest atomic resident
    // snapshot below; only known peer changes or committed shared intents
    // require synchronizing storage first.
    const canExecuteResident = preparedResult
      && !this["peerRefreshDirty"] && !this["peerRefreshScheduled"]
      && !sharedEntries.some(entry => entry.state === "committed");
    const uncoveredRead = this["isUncoveredLocalRead"](preparedFailure)
      && !this["peerRefreshDirty"] && !this["peerRefreshScheduled"]
      && !sharedEntries.some(entry => entry.state === "committed");
    if (!canExecuteResident && !uncoveredRead) await this["replica"].synchronizeStorage();
    if (sharedEntries) this["replaceLocalPredictions"](sharedEntries.filter(entry => entry.state !== "committed").map(entry => ({commandId: entry.idempotencyKey, patches: entry.patches ?? []})));
    const executionBaseVersion = this["replica"].executionVersion();
    let transaction: LocalTransactionResult | undefined;
    let fallback: unknown;
    try {
      if (preparedResult && preparedResult.base === executionBaseVersion && preparedResult.pending === JSON.stringify(sharedPatches)) {
        transaction = preparedResult.transaction;
      } else if (this["localBinding"]!.mode === 'portable') {
        // A commit may invalidate preparation while the intent lock is being
        // acquired. Read the newly published atomic replica again before
        // joining disk work that arrived after synchronizeStorage completed.
        try {
          transaction = await this["executeLocal"](ref.path, args, undefined, execution, sharedPatches ?? true, true);
        } catch (error) {
          if (!(error instanceof Error) || error.name !== 'IncompleteReplicaError') throw error;
          transaction = await this["executeLocal"](ref.path, args, undefined, execution, sharedPatches ?? true);
        }
      } else {
        transaction = await this["executeLocal"](ref.path, args, this["localSnapshot"](true), execution, sharedPatches ?? true);
      }
    }
    catch (error) { if (!(error instanceof Error) || error.name !== "IncompleteReplicaError") throw error; fallback = error; }
    if (scope !== this["outboxScope"] || execution.scope !== this["replicaScope"]) throw new GonvexClientError("Session changed during local reducer execution", { code: "superseded" });
    const patches = transaction?.patches ?? [];
    this["directOutboxReducerIds"].add(reducerId);
    try {
      // A visible prediction must already have a durable intent. Otherwise a
      // reload immediately after paint can lose an offline edit. Local
      // execution still avoids a server round trip; disk admission is bounded
      // to this intent, rather than persisting the whole replica.
      const entry = await this["reducerOutbox"].enqueue({
        scope, path: ref.path, args, idempotencyKey: reducerId, state: "pending",
        entityKeys: transaction ? [] : ["__gonvex_local_intents"],
        ...(transaction ? { dependencies: localIntentDependencies(transaction), coalesceKey: transaction.coalesceKey } : {}),
        ...(command ? { engineOutcome: { result: transaction ? transaction.result : { status: "queued", reducerId }, commandNow: command.now, fresh: optionsFresh } } : {}),
        patches, localExecution: execution,
      });
      if (scope !== this["outboxScope"] || execution.scope !== this["replicaScope"]) {
        throw new GonvexClientError("Session changed while persisting local reducer", { code: "superseded" });
      }
      for (const [commandId, id] of this["optimisticOutboxEntryIds"]) if (id === entry.id && commandId !== reducerId) {
        this["optimisticOutboxEntryIds"].delete(commandId);
        this["optimisticReducerIds"].delete(commandId);
        this["localReadVersions"].delete(commandId);
      }
      if (entry.dependencies) this["localReadVersions"].set(reducerId, this["localReadStamp"](entry.dependencies));
      this["optimisticOutboxEntryIds"].set(reducerId, entry.id);
      this["replacingLocal"] = true;
      try {
        if (sharedEntries.some(previous => previous.id === entry.id && previous.idempotencyKey !== reducerId)) {
          this["replaceLocalPredictions"](sharedEntries.filter(previous => previous.state !== "committed").map(previous => previous.id === entry.id
            ? { commandId: reducerId, patches }
            : { commandId: previous.idempotencyKey, patches: previous.patches ?? [] }));
          if (patches.length) this["optimisticReducerIds"].add(reducerId);
        } else this["addOptimisticReducer"](reducerId, patches);
      }
      finally { this["replacingLocal"] = false; }
      if (!transaction) {
        this["reportLocalFallback"](ref.path, reducerId, "call", fallback);
        return { status: "queued" as const, reducerId };
      }
      return transaction.result as T;
    } catch (error) {
      this["optimisticReducerIds"].delete(reducerId);
      this["replica"].rejectCommand(reducerId);
      throw error;
    } finally {
      this["directOutboxReducerIds"].delete(reducerId);
      // This transaction already executed against all earlier predictions.
      // Appending it does not invalidate those predictions. Rebase only if
      // the authoritative execution snapshot changed during execution.
      // An uncovered read has no new data to replay against yet. Its
      // hydration subscription schedules replay when coverage arrives;
      // immediately retrying here only locks edits behind the same disk work.
      if (this["replica"].executionVersion() !== executionBaseVersion) this["scheduleLocalReplay"]();
      void this["drainOutbox"]();
    }
  };
  return (lane ? lane(admit) : this["inLocalLane"](() => this["coordinateOutbox"]("intent", admit))).catch(error => {
    if (!entered) this["waitingLocalEdits"] -= 1;
    this["optimisticReducerIds"].delete(reducerId);
    this["replica"].rejectCommand(reducerId);
    throw error;
  });
}

export function reducer<T extends JsonValue = JsonValue, Args extends JsonValue = JsonValue>(
  this: GonvexClient,
  ref: FunctionReference<Args, T>,
  args: Args = {} as Args,
  options: CallOptions & { engineFresh?: boolean } = {},
): Promise<T | QueuedReducerOutcome> {
  if (this["publicTenant"]) {
    if (options.offline === "queue" || options.optimistic?.length) return Promise.reject(new GonvexClientError("Public reducers are server-only and cannot queue or execute optimistically", { code: "disconnected", path: ref.path, operation: "reducer" }));
    return this["call"]<T>("reducer", ref, args, options.timeoutMs ?? this["timeouts"].reducerTimeoutMs);
  }
  if (ref.localExecution === 1) return runLocalReducer.call(this, ref, args, options.engineCommand, options.engineFresh, engineAdmissionLane(options)) as Promise<T | QueuedReducerOutcome>;
  if (options.offline === "queue" && ref.offline?.mode !== "allowed") {
    return Promise.reject(new GonvexClientError(
      `Reducer ${ref.path} does not allow offline queueing.`,
      { code: "disconnected", path: ref.path, operation: "reducer" },
    ));
  }
  const effectiveOptions: CallOptions = {
    ...options,
    offline: options.offline ?? (ref.offline?.mode === "allowed" ? "queue" : "reject"),
  };
  const reducerId = options.engineCommand?.id ?? randomID();
  const patches = effectiveOptions.optimistic
    ?? optimisticPatchesFromReference(ref.optimistic?.transaction, args);
  if (patches.length === 0 && effectiveOptions.offline !== "queue") {
    return this["call"]<T>(
      "reducer",
      ref,
      args,
      effectiveOptions.timeoutMs ?? this["timeouts"].reducerTimeoutMs,
      reducerId,
      options.engineCommand?.id,
    );
  }
  return this["runOptimisticReducer"]<T>(ref, args, effectiveOptions, reducerId, patches);
}

export async function runOptimisticReducer<T>(
  this: GonvexClient,
  ref: FunctionReference,
  args: JsonValue,
  options: CallOptions & { engineFresh?: boolean },
  reducerId: string,
  patches: OptimisticPatch[],
): Promise<T | QueuedReducerOutcome> {
  // The startup recovery transaction converts abandoned inflight entries to
  // pending. Finish it before inserting a brand-new direct send, otherwise
  // recovery can mistake that live entry for a crashed reducer and race the
  // direct call through the background drain.
  await this["outboxReady"];
  if (this["manuallyClosed"]) {
    throw new GonvexClientError(
      `Gonvex client was closed before reducer ${ref.path} could be sent.`,
      { code: "closed", path: ref.path, operation: "reducer" },
    );
  }
  if (options.engineCommand) {
    const previous = (await this["reducerOutbox"].list(this["outboxScope"])).find(entry => entry.idempotencyKey === reducerId);
    const previousOutcome = previous && engineEntryOutcome(previous);
    if (previousOutcome) {
      if (previous.path !== ref.path || JSON.stringify(previous.args) !== JSON.stringify(args)) throw new Error("Engine command ID reused with different arguments");
      return previousOutcome.result as T;
    }
  }
  this["directOutboxReducerIds"].add(reducerId);
  let entryId: number | undefined;
  try {
    const scope = this["outboxScope"];
    const entry = await this["reducerOutbox"].enqueue({
      scope,
      path: ref.path,
      args,
      idempotencyKey: reducerId,
      ...(options.engineCommand ? { engineOutcome: { result: { status: "queued", reducerId }, commandNow: options.engineCommand.now, fresh: options.engineFresh } } : {}),
      entityKeys: patches.map((patch) => `${patch.entity ?? patch.collection ?? ""}:${patch.rowId}`),
      patches,
      state: "inflight",
    });
    entryId = entry.id;
    if (this["manuallyClosed"]) {
      await this["reducerOutbox"].ack(entry.id);
      throw new GonvexClientError(
        `Gonvex client was closed before reducer ${ref.path} could be sent.`,
        { code: "closed", path: ref.path, operation: "reducer" },
      );
    }
    if (scope !== this["outboxScope"]) {
      await this["reducerOutbox"].ack(entry.id);
      throw new GonvexClientError(
        `Authentication changed before reducer ${ref.path} could be sent.`,
        { code: "disconnected", path: ref.path, operation: "reducer" },
      );
    }
    this["optimisticOutboxEntryIds"].set(reducerId, entry.id);
    this["addOptimisticReducer"](reducerId, patches);
    if (options.offline === "queue" && !this["canSendReducerNow"]()) {
      await this["reducerOutbox"].markPending(entry.id);
      return { status: "queued", reducerId };
    }
    // The direct send is outbox-managed: a crash here replays the entry
    // with the same idempotency key, so the server must dedupe it.
    const result = await this["call"]<T>(
      "reducer",
      ref,
      args,
      options.timeoutMs ?? this["timeouts"].reducerTimeoutMs,
      reducerId,
      reducerId,
    );
    if (options.engineCommand) receiptContexts.get(this)!.admissions.set(reducerId, {
      result: structuredClone(result), commandNow: options.engineCommand.now,
    });
    await this["reducerOutbox"].markCommitted(entry.id);
    if (patches.length > 0) {
      await this["settleOptimisticReducer"](reducerId);
    } else {
      await this["ackOptimisticReducer"](reducerId, entry.id);
    }
    return result;
  } catch (error: unknown) {
    const disposition = deliveryErrorClass(error);
    const queuedEntryId = this["optimisticOutboxEntryIds"].get(reducerId) ?? entryId;
    if (disposition !== "rejected" && options.offline === "queue" && queuedEntryId !== undefined) {
      // Records backoff (or parks the entry) and arms the client's timer,
      // exactly like the background drain path.
      await this["recordDeliveryFailure"]({ id: queuedEntryId }, error, disposition);
      return { status: "queued", reducerId };
    }
    await this["rejectOptimisticReducer"](reducerId, entryId);
    throw error;
  } finally {
    receiptContexts.get(this)?.admissions.delete(reducerId);
    this["directOutboxReducerIds"].delete(reducerId);
    void this["drainOutbox"]();
  }
}


function localIntentDependencies(transaction: LocalTransactionResult): IntentDependencies {
  return {
    reads: transaction.readRows ?? [],
    scans: transaction.scanTables ?? transaction.readTables,
    writes: transaction.patches.map(patch => ({ entity: patch.entity, rowId: patch.rowId })),
  };
}

function randomID() {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (randomUUID) return randomUUID.call(globalThis.crypto);
  return `gonvex_${Date.now().toString(36)}_${Math.random().toString(36).slice(2)}`;
}

function deliveryErrorClass(error: unknown): OutboxErrorClass {
  if (!(error instanceof GonvexClientError)) return "transient";
  switch (error.code) {
    case "server": return error.errorClass ?? "rejected";
    case "timeout": return "transient";
    default: return "network";
  }
}
