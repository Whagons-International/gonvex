//! Why Replica Collections open (diagnostics, `/dev/metrics` `replicaOpens`).
//!
//! Staging (2026-11-04 22:12-22:20) reopened ~840 collections a minute with
//! ~10 accounts and no restarts, and nothing on the server said why. These
//! process-wide counters split every open by its request shape and by
//! whether the same socket had already opened that (path, arguments), and
//! count what the server sent that makes a client reopen: `needHashes` by
//! reason and `replica.reset` by reason, plus the client's own closes.
//! Reopens are also counted per path (bounded by the module's paths).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Mutex, OnceLock};

use gonvex_protocol::ReplicaOpenRequest;
use serde_json::{json, Value};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reason {
    /// Cursor + digest, no hashes: a log-only resume.
    Resume,
    /// Cursor with keys/hashes: a reconcile (usually after `needHashes`).
    Reconcile,
    /// No cursor, with a digest (a window the client never stored).
    ColdDigest,
    /// No cursor, no digest.
    Cold,
    /// `fullIntegrity` set.
    FullIntegrity,
    /// The socket had opened this (path, arguments) before.
    ReopenOnSocket,
    /// Client `replica.close`.
    Close,
    /// `needHashes` answering a resume: visibility inputs changed since the
    /// cursor.
    NeedHashesResumeDependency,
    /// ... the client's digest does not match the log (or a truncated
    /// window moved).
    NeedHashesResumeDigest,
    /// ... a whole-set digest mismatch.
    NeedHashesResumeWholeSet,
    /// `needHashes` from feed delivery (`Resync`).
    NeedHashesPrincipal,
    NeedHashesConsistency,
    NeedHashesWindow,
    /// `replica.reset` sent.
    ResetFeedLagged,
    ResetFeedReset,
}

const REASONS: [(Reason, &str); 15] = [
    (Reason::Resume, "resume"),
    (Reason::Reconcile, "reconcile"),
    (Reason::ColdDigest, "coldDigest"),
    (Reason::Cold, "cold"),
    (Reason::FullIntegrity, "fullIntegrity"),
    (Reason::ReopenOnSocket, "reopenOnSocket"),
    (Reason::Close, "clientClose"),
    (
        Reason::NeedHashesResumeDependency,
        "needHashesResumeDependency",
    ),
    (Reason::NeedHashesResumeDigest, "needHashesResumeDigest"),
    (Reason::NeedHashesResumeWholeSet, "needHashesResumeWholeSet"),
    (Reason::NeedHashesPrincipal, "needHashesPrincipal"),
    (Reason::NeedHashesConsistency, "needHashesConsistency"),
    (Reason::NeedHashesWindow, "needHashesWindow"),
    (Reason::ResetFeedLagged, "resetFeedLagged"),
    (Reason::ResetFeedReset, "resetFeedReset"),
];

static COUNTS: [AtomicU64; 15] = [const { AtomicU64::new(0) }; 15];

fn index(reason: Reason) -> usize {
    REASONS
        .iter()
        .position(|(candidate, _)| *candidate == reason)
        .expect("listed")
}

pub(crate) fn add(reason: Reason, count: u64) {
    COUNTS[index(reason)].fetch_add(count, Relaxed);
}

/// The request shape of one open.
pub(crate) fn shape(request: &ReplicaOpenRequest) -> Reason {
    if request.full_integrity {
        Reason::FullIntegrity
    } else if request.cursor.is_some() && (!request.hashes.is_empty() || !request.keys.is_empty()) {
        Reason::Reconcile
    } else if request.cursor.is_some() {
        Reason::Resume
    } else if request
        .digest
        .as_deref()
        .is_some_and(|digest| !digest.is_empty())
    {
        Reason::ColdDigest
    } else {
        Reason::Cold
    }
}

fn by_path() -> &'static Mutex<HashMap<String, [u64; 3]>> {
    static PATHS: OnceLock<Mutex<HashMap<String, [u64; 3]>>> = OnceLock::new();
    PATHS.get_or_init(Default::default)
}

/// Per socket: the (path, arguments) it opened, to count reopens.
#[derive(Default)]
pub(crate) struct SocketOpens {
    seen: std::collections::HashSet<(String, String)>,
}

impl SocketOpens {
    /// Records one open on this socket (counters by shape, reopen and path).
    pub(crate) fn record(&mut self, request: &ReplicaOpenRequest) {
        let shape = shape(request);
        add(shape, 1);
        let reopen = !self.seen.insert((
            request.path.clone(),
            crate::replica::stable_json(&request.args),
        ));
        if reopen {
            add(Reason::ReopenOnSocket, 1);
        }
        let mut paths = by_path().lock().unwrap_or_else(|error| error.into_inner());
        let totals = match paths.get_mut(&request.path) {
            Some(totals) => totals,
            None => paths.entry(request.path.clone()).or_default(),
        };
        totals[0] += 1;
        totals[1] += u64::from(reopen);
        totals[2] += u64::from(shape == Reason::Reconcile);
    }
}

/// `/dev/metrics` `replicaOpens`: totals plus the 40 paths with the most
/// opens (`[opens, reopensOnSocket, reconciles]`).
pub(crate) fn json() -> Value {
    let mut totals = serde_json::Map::new();
    for (position, (_, name)) in REASONS.iter().enumerate() {
        totals.insert((*name).to_owned(), json!(COUNTS[position].load(Relaxed)));
    }
    let mut paths: Vec<(String, [u64; 3])> = by_path()
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .iter()
        .map(|(path, totals)| (path.clone(), *totals))
        .collect();
    paths.sort_by(|a, b| b.1[0].cmp(&a.1[0]).then_with(|| a.0.cmp(&b.0)));
    paths.truncate(40);
    totals.insert(
        "topPaths".to_owned(),
        Value::Object(
            paths
                .into_iter()
                .map(|(path, [opens, reopens, reconciles])| {
                    (path, json!({"opens": opens, "reopensOnSocket": reopens, "reconciles": reconciles}))
                })
                .collect(),
        ),
    );
    Value::Object(totals)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(value: Value) -> ReplicaOpenRequest {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn shapes_and_reopens() {
        let cursor = json!({"epoch": "e", "revision": 3});
        assert_eq!(
            shape(&request(
                json!({"id": "a", "path": "p", "cursor": cursor, "digest": "xor1:0"})
            )),
            Reason::Resume
        );
        assert_eq!(
            shape(&request(
                json!({"id": "a", "path": "p", "cursor": cursor, "keys": ["k"], "hashes": {"k": "h"}})
            )),
            Reason::Reconcile
        );
        assert_eq!(
            shape(&request(
                json!({"id": "a", "path": "p", "digest": "xor1:0"})
            )),
            Reason::ColdDigest
        );
        assert_eq!(
            shape(&request(json!({"id": "a", "path": "p"}))),
            Reason::Cold
        );
        let before = COUNTS[index(Reason::ReopenOnSocket)].load(Relaxed);
        let mut socket = SocketOpens::default();
        socket.record(&request(
            json!({"id": "a", "path": "open-reasons.test", "args": {"x": 1}}),
        ));
        socket.record(&request(
            json!({"id": "b", "path": "open-reasons.test", "args": {"x": 1}}),
        ));
        socket.record(&request(
            json!({"id": "c", "path": "open-reasons.test", "args": {"x": 2}}),
        ));
        assert_eq!(
            COUNTS[index(Reason::ReopenOnSocket)].load(Relaxed) - before,
            1
        );
        assert_eq!(
            json()["topPaths"]["open-reasons.test"]["reopensOnSocket"],
            1
        );
    }
}
