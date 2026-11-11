/**
 * Query frames in serde's byte order. `query.result` and `query.progress`
 * flatten a `BTreeMap` payload, so after `type` and `id` the keys are
 * alphabetical (`proto:674-685`, CONTRACT A.3 and A.6 item 8).
 *
 * The result text is spliced in once and shared: every subscriber of one
 * execution gets the same bytes after its own `id` (design rule: encode once).
 */
import { sha256Hex } from "../capture/digest";
import { frames } from "../session/protocol";
import { quote } from "./json";

export interface SubscriptionRevision { readonly epoch: string; readonly sequence: number }

/** `result_revision`: SHA-256 (hex) of the serialized result. */
export function windowRevision(resultText: string): string {
  return sha256Hex(resultText);
}

const revisionJson = (revision: SubscriptionRevision) => `{"epoch":${quote(revision.epoch)},"sequence":${revision.sequence}}`;

/**
 * The per-execution part of a live `query.result` (`query_message`):
 * everything after `"id":…,`. Build it once per execution and reason.
 */
export function liveResultTail(path: string, resultText: string, reason: string, revision: SubscriptionRevision, window = windowRevision(resultText)): string {
  return `"path":${quote(path)},"reason":${quote(reason)},"result":${resultText},"subscriptionRevision":${revisionJson(revision)},"windowRevision":${quote(window)}}`;
}

/** A live `query.result` for one subscription id. */
export function liveResult(id: string, tail: string): string {
  return `{"type":"query.result","id":${quote(id)},${tail}`;
}

/** `resume_message`: the client already holds this window, so only the revision advances. */
export function progress(id: string, path: string, revision: SubscriptionRevision): string {
  return `{"type":"query.progress","id":${quote(id)},"path":${quote(path)},"throughRevision":${revisionJson(revision)}}`;
}

/** A one-shot `query.result` (`lib.rs` `query_result`): `{id, path, reason:"initial", result}`. */
export function callResult(id: string, path: string, resultText: string): string {
  return `{"type":"query.result","id":${quote(id)},"path":${quote(path)},"reason":"initial","result":${resultText}}`;
}

export function queryError(id: string, path: string | undefined, error: string): string {
  return frames.queryError(id, path, error);
}
