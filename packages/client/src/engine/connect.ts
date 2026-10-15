import type { GonvexClient, ReplicaReadyMessage } from "../index.js";
import { GonvexClientError } from "../index.js";
import type { JsonValue, ServerMessage, ReplicaDirective } from "@gonvex/protocol";
import type { LocalExecution } from "@gonvex/local-runtime";

// Worker transport captures the native constructor before denying reducer I/O.
// Default clients retain their original connection implementation.
export function connectEngine(client: GonvexClient, WebSocket: typeof globalThis.WebSocket) {
  if (client["updateRequired"]) return;
  if (client["socket"] && client["socket"].readyState <= WebSocket.OPEN) return;

  client["flushTelemetry"]();
  const isReconnect = client["socket"] !== undefined;
  client["manuallyClosed"] = false;
  const generation = ++client["socketGeneration"];
  let transportUrl = client["url"];
  if (client["publicTenant"]) {
    const publicUrl = new URL(client["url"]);
    publicUrl.searchParams.set("public", "1");
    transportUrl = publicUrl.toString();
  }
  const socket = new WebSocket(transportUrl);
  client["socket"] = socket;
  client["serverCapabilities"] = {};
  socket.addEventListener("open", () => {
    if (client["socket"] !== socket) return;
    if (client["reconnectTimer"]) {
      clearTimeout(client["reconnectTimer"]);
      client["reconnectTimer"] = undefined;
    }
    client["reconnectAttempt"] = 0;
    client["isWebSocketConnected"] = true;
    client["replica"].setFreshness("verifying");
    client["hasEverConnected"] = true;
    client["connectionCount"] += 1;
    client["errorReporter"]?.connectionRestored?.();
    client["sendAuth"](false);
    if (isReconnect) client["resubscribeQueries"](generation);
    void client["drainOutbox"]();
    client["notifyConnectionState"]();
  });
  socket.addEventListener("close", () => {
    if (client["socket"] !== socket || client["manuallyClosed"]) return;
    client["flushTelemetry"]();
    client["isWebSocketConnected"] = false;
    client["replica"].setFreshness("offline");
    if (client["localSessionStateValue"] === "authenticated") client["setLocalSessionState"]("restored");
    client["markReplicaSubscriptionsOutOfDate"]();
    client["authInFlight"] = false;
    client["authResendRequired"] = false;
    client["latestAuthFrameId"] = undefined;
    client["activeAuthFrameId"] = undefined;
    if (client["authWatchdogTimer"]) {
      clearTimeout(client["authWatchdogTimer"]);
      client["authWatchdogTimer"] = undefined;
    }
    // A subscription queued for the old socket is superseded by the complete
    // resubscribe below. Queued reducers/actions are rejected below, so
    // drop them too — flushing them after reconnect would fire writes whose
    // callers already saw a rejection.
    client["pendingMessages"].length = 0;
    client["pendingReplicaFrames"].length = 0;
    // Reducers/actions must fail closed on transport loss: silently
    // replaying a non-idempotent write after reconnect is unsafe, and
    // leaving the promise pending hangs the caller forever.
    client["rejectPendingCalls"]((call) => new GonvexClientError(
      `Connection lost while waiting for ${call.kind} ${call.path}. The operation may or may not have been applied.`,
      { code: "disconnected", path: call.path, operation: call.kind },
    ), (call) => call.scope !== "control");
    client["scheduleReconnect"]();
    client["notifyConnectionState"]();
  });
  socket.addEventListener("message", (event) => {
    if (client["socket"] !== socket) return;
    let message: ServerMessage;
    try {
      message = JSON.parse(String(event.data)) as ServerMessage;
    } catch {
      return;
    }
    if (message.type === "session.ready") {
      client["serverCapabilities"] = message.capabilities ?? {};
      if (client["controlOnly"]) {
        client["flushPendingMessages"]();
        return;
      }
      if (!message.replica) {
        if (client["hasControlPlaneWork"]() || (!!client["auth"].token && !client["auth"].tenant)) {
          client["flushPendingMessages"]();
        } else {
          client["rejectMissingReplicaDirective"]();
        }
        return;
      }
      const ready = client["activateReplicaDirective"](message.replica);
      if (!client["auth"].token && !client["auth"].tenant) {
        void ready
          .then(() => {
            client["resumeQuerySubscriptions"]();
            client["resumeReplicaSubscriptions"]();
          })
          .catch((error) => client["rejectReplicaDirective"](error));
      } else {
        void ready.catch((error) => client["rejectReplicaDirective"](error));
      }
      return;
    }
    if (message.type === "client.updateRequired") {
      client["requireClientUpdate"](message.reason);
      return;
    }
    if (message.type === "system.reload") {
      if (typeof message.artifactHash === "string") {
        client["activeArtifactHashValue"] = message.artifactHash;
      }
      client["resumeQuerySubscriptions"](true);
      client["resumeReplicaSubscriptions"]();
      return;
    }
    if (message.type === "auth.result" || message.type === "auth.error") {
      if (
        message.type === "auth.error"
        && (message.id === "membership-changed" || message.id === "session-expired")
      ) {
        if (["revoked", "member_removed", "account_disabled"].includes(message.code ?? "")) {
          client["authInFlight"] = false;
          client["authFetchGeneration"] += 1;
          client["latestAuthFrameId"] = undefined;
          client["activeAuthFrameId"] = undefined;
          client["authResendRequired"] = false;
          if (client["authWatchdogTimer"]) {
            clearTimeout(client["authWatchdogTimer"]);
            client["authWatchdogTimer"] = undefined;
          }
          client["quarantineReplicaScope"]();
          client["cancelManagedAuthAttempt"](message.error);
          client["notifyAuthError"](message.error, true, message.code);
          return;
        }
        // These are unsolicited connection-scope events, not replies to an
        // auth frame. The runtime has already discarded every tenant
        // subscription. Rotate their routing IDs immediately so delayed
        // Replica errors from that discarded scope cannot reach current
        // React listeners.
        client["pendingReplicaFrames"].length = 0;
        if (message.id === "membership-changed") client["quarantineReplicaScope"]();
        else client["rotateSubscriptionScopes"]();
        client["activeAuthFrameId"] = undefined;

        // If credentials are already being revalidated, that queued auth
        // frame will install the new authoritative scope. Starting another
        // transition here would recreate the out-of-order auth race.
        if (client["latestAuthFrameId"]) return;

        if (client["authWatchdogTimer"]) {
          clearTimeout(client["authWatchdogTimer"]);
          client["authWatchdogTimer"] = undefined;
        }
        client["authInFlight"] = false;
        if (message.id === "membership-changed") {
          client["sendAuth"](true, { useFetcher: false });
          return;
        }

        const fetcher = client["publicTenant"] ? undefined : client["auth"].fetchToken;
        if (fetcher) {
          client["authInFlight"] = true;
          client["authRetriedAfterError"] = true;
          client["armAuthWatchdog"]();
          void client["refreshRejectedAuth"](fetcher, client["auth"].token);
        } else {
          client["notifyAuthError"](message.error, false, message.code);
          client["retryTransientAuth"]();
        }
        return;
      }
      // Auth work may complete out of order inside the runtime. An obsolete
      // response must never settle the current transition, reopen queries,
      // or downgrade an accepted tenant session to an older control-only
      // scope.
      const settlesLatestFrame = message.id === client["latestAuthFrameId"];
      const rejectsActiveFrame = message.type === "auth.error"
        && client["latestAuthFrameId"] === undefined
        && message.id === client["activeAuthFrameId"];
      if (!settlesLatestFrame && !rejectsActiveFrame) return;
      if (settlesLatestFrame) client["latestAuthFrameId"] = undefined;
      if (settlesLatestFrame && client["authResendRequired"]) {
        // The settled frame represented credentials that have already been
        // superseded locally. Do not publish or flush anything from that
        // intermediate server scope; authenticate the newest state now.
        client["authResendRequired"] = false;
        if (client["authWatchdogTimer"]) {
          clearTimeout(client["authWatchdogTimer"]);
          client["authWatchdogTimer"] = undefined;
        }
        client["sendAuth"](true);
        return;
      }
      if (message.type === "auth.result") client["activeAuthFrameId"] = message.id;
      else client["activeAuthFrameId"] = undefined;
      if (message.type === "auth.result") client["flushPendingNativeErrors"]();
      if (client["authWatchdogTimer"]) {
        clearTimeout(client["authWatchdogTimer"]);
        client["authWatchdogTimer"] = undefined;
      }
      if (message.type === "auth.result") {
        const reauthenticatedSameSocket = client["authenticatedSocketGeneration"] === client["socketGeneration"];
        const subscriptionsPreserved = reauthenticatedSameSocket && message.result !== null
          && typeof message.result === "object" && !Array.isArray(message.result)
          && message.result.subscriptionsPreserved === true;
        client["authenticatedSocketGeneration"] = client["socketGeneration"];
        client["lastOnlineAtMs"] = Date.now();
        client["activeArtifactHashValue"] = artifactHashFromAuthResult(message.result) ?? client["activeArtifactHashValue"];
        const developerSessionToken = developerSessionTokenFromAuthResult(message.result);
        if (developerSessionToken) {
          // The activation token is single-use. Use its rotating successor
          // for the next reconnect, and hand it to the provider that owns
          // developer mode so a reload can resume the same grant.
          client["auth"] = { ...client["auth"], token: developerSessionToken, fetchToken: undefined };
          for (const handler of Array.from(client["developerCredentialHandlers"])) handler(developerSessionToken);
        }
        client["authRetriedAfterError"] = false;
        client["authRefreshedToken"] = undefined;
        const directive = client["controlOnly"] ? undefined : replicaDirectiveFromAuthResult(message.result);
        const localIdentity = client["controlOnly"] ? undefined : localIdentityFromAuthResult(message.result);
        if (localIdentity) {
          client["localIdentity"] = localIdentity;
          void client["localExecutor"]?.ready.catch(() => undefined);
        }
        if (!directive) {
          client["authInFlight"] = false;
          if (client["controlOnly"] || !client["auth"].tenant || client["publicTenant"]) {
            if (!subscriptionsPreserved) client["resumeQuerySubscriptions"](reauthenticatedSameSocket);
            client["settleManagedAuthAttempt"](message.id);
          } else {
            client["settleManagedAuthAttempt"](message.id, "Runtime did not provide an authoritative Local Replica visibility scope");
            client["rejectMissingReplicaDirective"]();
          }
          // A reducer admitted while this auth frame was in flight may have
          // been durably queued because the socket was not yet usable. Wake
          // that queue once the successful auth result has made it usable.
          void client["drainOutbox"]();
          client["flushPendingMessages"]();
          return;
        }
        const authScopeGeneration = client["outboxScopeGeneration"];
        const isCurrentAuth = () => client["socket"] === socket && client["activeAuthFrameId"] === message.id
          && client["latestAuthFrameId"] === undefined && authScopeGeneration === client["outboxScopeGeneration"];
        void client["activateReplicaDirective"](directive)
          .then(() => isCurrentAuth() ? client["activateOutboxScope"]() : undefined)
          .then(async () => {
            if (!isCurrentAuth()) return;
            if (localIdentity) await client["localStorage"]?.saveSession?.(client["outboxScope"], {
              directive, identity: localIdentity, artifactHash: client["activeArtifactHashValue"] || client["localBinding"]?.artifactHash || "", lastOnlineAtMs: client["lastOnlineAtMs"],
            });
          })
          .then(() => {
            if (!isCurrentAuth()) return;
            // The accepted server identity is not usable until its durable
            // Replica partition has been activated locally. Only now may
            // tenant calls and subscriptions leave the client.
            client["authInFlight"] = false;
            client["drainPendingReplicaFrames"]();
            client["resumeQuerySubscriptions"](reauthenticatedSameSocket && !subscriptionsPreserved);
            if (!subscriptionsPreserved) client["resumeReplicaSubscriptions"]();
            else for (const subscription of client["replicaSubscriptions"].values()) {
              if (subscription.socketGeneration !== client["socketGeneration"]) client["sendReplicaOpen"](subscription);
            }
            client["setLocalSessionState"]("authenticated");
            client["settleManagedAuthAttempt"](message.id);
            // Same-scope reauthentication does not reload the outbox, so no
            // restore callback will wake a reducer queued during auth.
            void client["drainOutbox"]();
            client["flushPendingMessages"]();
          })
          .catch((error) => {
            if (!isCurrentAuth()) return;
            client["authInFlight"] = false;
            client["pendingReplicaFrames"].length = 0;
            client["settleManagedAuthAttempt"](message.id, error instanceof Error ? error.message : "Runtime returned an invalid Local Replica scope");
            client["rejectReplicaDirective"](error);
            client["flushPendingMessages"]();
          });
        return;
      } else {
        client["authInFlight"] = false;
        client["pendingReplicaFrames"].length = 0;
        const fetcher = client["auth"].fetchToken;
        if (fetcher && !client["authRetriedAfterError"] && !client["authRefreshedToken"] && message.code !== "transient"
          && !["revoked", "member_removed", "account_disabled"].includes(message.code ?? "")) {
          // The installed token was rejected — typically expired while the
          // socket was down. Force-refresh through the fetcher and retry
          // once before treating the rejection as final.
          client["authRetriedAfterError"] = true;
          client["authInFlight"] = true;
          client["armAuthWatchdog"]();
          void client["refreshRejectedAuth"](fetcher, client["auth"].token);
          return;
        }
        const definitive = message.code === "revoked" || message.code === "member_removed"
          || message.code === "account_disabled"
          || (!!client["authRefreshedToken"] && client["authRefreshedToken"] === client["auth"].token
            && message.code === "credential_rejected")
          || (message.code === "credential_rejected" && /^gvx_(dev|imp)_/.test(client["auth"].token ?? ""));
        client["authRetriedAfterError"] = false;
        client["authRefreshedToken"] = undefined;
        client["settleManagedAuthAttempt"](message.id, message.error);
        if (definitive) {
          client["quarantineReplicaScope"]();
          client["notifyAuthError"](message.error, true, message.code);
        } else {
          // Report every rejection; only `definitive` authorizes clearing a session.
          client["notifyAuthError"](message.error, false, message.code);
          // Signed-out calls still reach the server for authorization errors.
          // A restored account must instead keep its reconnect barrier.
          if (client["auth"].token || fetcher || client["localSessionStateValue"] !== "unavailable") client["retryTransientAuth"]();
        }
      }
      client["flushPendingMessages"]();
    }
    if (message.type === "replica.readyMany") {
      for (const ready of message.ready) {
        const readyMessage = { type: "replica.ready", ...ready } as ReplicaReadyMessage;
        client["handlers"].get(ready.id)?.(readyMessage);
      }
      return;
    }
    if (message.type === "replica.transaction") {
      // Replica frames carry no tenant/scope field. During auth renewal we
      // cannot safely attribute a late frame to either side of the switch.
      if (client["authInFlight"]) {
        client["pendingReplicaFrames"].push(message);
        return;
      }
      client["enqueueReplicaTransaction"](message);
      return;
    }
    if (message.type === "replica.watermark") {
      if (client["serverCapabilities"].replicaWatermark === 1) {
        // The runtime emits transactions and watermarks in one ordered
        // stream. Keep both on the same client queue so a watermark cannot
        // advance the cursor past a transaction that was received first but
        // has not finished applying to durable Local Replica storage yet.
        if (client["authInFlight"]) {
          client["pendingReplicaFrames"].push(message);
          return;
        }
        client["enqueueReplicaWatermark"](message.revision);
      }
      return;
    }
    if (message.type === "support.command") {
      const result = message.result && typeof message.result === "object" && !Array.isArray(message.result)
        ? message.result as Record<string, JsonValue>
        : {};
      const command = { id: message.id, kind: String(result.kind ?? ""), payload: result.payload ?? null };
      for (const handler of client["supportCommandHandlers"]) handler(command);
      return;
    }
			if (message.type === "query.fanout") {
				const { ids, queryType, ...shared } = message;
				for (const id of ids) {
					client["handlers"].get(id)?.({ ...shared, type: queryType, id } as ServerMessage);
				}
				return;
			}
			if (message.type === "query.batch") {
				for (const nested of message.messages) {
					if (nested.type === "query.fanout") {
						const { ids, queryType, ...shared } = nested;
						for (const id of ids) {
							client["handlers"].get(id)?.({ ...shared, type: queryType, id } as ServerMessage);
						}
						continue;
					}
					if (nested.type !== "query.batch") {
						const nestedID = "id" in nested ? nested.id : "system";
						client["handlers"].get(nestedID)?.(nested);
					}
				}
				return;
			}
    const id = "id" in message ? message.id : "system";
    client["handlers"].get(id)?.(message);
  });
}


function localIdentityFromAuthResult(result: JsonValue): LocalExecution["identity"] | undefined {
  if (!result || typeof result !== "object" || Array.isArray(result)) return;
  const identity = result.localIdentity as LocalExecution["identity"] | undefined;
  if (!identity?.auth?.account?.id || !identity.tenant?.id || !identity.member?.id) return;
  return structuredClone(identity);
}

function replicaDirectiveFromAuthResult(result: JsonValue): ReplicaDirective | undefined {
  if (!isJsonRecord(result) || !isJsonRecord(result.replica)) return undefined;
  const directive = result.replica;
  if (
    directive.protocolVersion !== 1
    || typeof directive.scope !== "string"
    || typeof directive.visibilityScope !== "string"
    || typeof directive.epoch !== "string"
  ) return undefined;
  return {
    protocolVersion: 1,
    scope: directive.scope,
    visibilityScope: directive.visibilityScope,
    epoch: directive.epoch,
  };
}

function developerSessionTokenFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const token = result.developerSessionToken;
  return typeof token === "string" && token.startsWith("gvx_dev_") ? token : undefined;
}

function artifactHashFromAuthResult(result: JsonValue): string | undefined {
  if (!isJsonRecord(result)) return undefined;
  const hash = result.artifactHash;
  return typeof hash === "string" && hash.length > 0 ? hash : undefined;
}

function isJsonRecord(value: unknown): value is Record<string, JsonValue> {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}
