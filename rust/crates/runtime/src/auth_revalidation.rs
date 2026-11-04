//! Batched session revalidation for WebSocket connections (conn-idle).
//!
//! Every authenticated connection used to run `revalidate_connection` on its
//! own 30 s timer: a session lookup, a `last_seen_at` update, a tenant route
//! lookup and a Member read (six SQL statements and a dozen task wakeups per
//! connection per 30 s, ~6 µs/s of CPU per idle connection). Plain session
//! connections now register here instead; one process-wide task checks all of
//! them every [`INTERVAL`] with a few set queries per project and tenant
//! (`ControlPlane::valid_session_hashes`, `ControlPlane::active_member_accounts`)
//! and wakes only the connections whose session stopped being valid.
//!
//! Semantics kept: the same conditions as `revalidate_connection` (session
//! not revoked or expired, account enabled, session project, tenant still
//! resolves, Member still active; `last_seen_at` refreshed at most every
//! 5 minutes), checked at least every 30 s, and a failed check (including a
//! database error) revokes, as before. Impersonation, anonymous public and
//! non-`gvx_session_` connections keep their per-connection timer.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use gonvex_store::ControlPlane;
use tokio::sync::Notify;

use crate::control::ControlConnection;

/// Revalidation period (unchanged from the per-connection timer).
pub(crate) const INTERVAL: Duration = Duration::from_secs(30);
/// Token hashes or account ids per SQL statement.
const CHUNK: usize = 500;

#[derive(Default)]
struct Signal {
    revoked: AtomicBool,
    notify: Notify,
}

impl Signal {
    fn revoke(&self) {
        self.revoked.store(true, Relaxed);
        self.notify.notify_one();
    }
}

struct Entry {
    project: String,
    tenant: Option<String>,
    token_hash: String,
    account_id: String,
    signal: Arc<Signal>,
}

#[derive(Default)]
struct Inner {
    entries: Mutex<HashMap<u64, Entry>>,
    next: AtomicU64,
    rounds: AtomicU64,
    revoked: AtomicU64,
}

#[derive(Clone, Default)]
pub(crate) struct AuthRevalidator {
    inner: Arc<Inner>,
}

/// A connection's registration. Dropping it unregisters.
pub(crate) struct AuthWatch {
    id: u64,
    token: String,
    project: String,
    tenant: Option<String>,
    signal: Arc<Signal>,
    owner: Arc<Inner>,
}

impl Drop for AuthWatch {
    fn drop(&mut self) {
        lock(&self.owner.entries).remove(&self.id);
    }
}

impl AuthWatch {
    /// Completes once a revalidation round found the session invalid.
    pub(crate) async fn revoked(&self) {
        loop {
            let notified = self.signal.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.signal.revoked.load(Relaxed) {
                return;
            }
            notified.await;
        }
    }

    fn matches(&self, connection: &ControlConnection) -> bool {
        self.token == connection.auth_token
            && self.project == connection.project_id
            && self.tenant.as_deref()
                == connection
                    .tenant
                    .as_ref()
                    .map(|session| session.route.tenant_id.as_str())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Connections this module revalidates: plain session tokens, with a tenant
/// session that is not anonymous, or control-only (no tenant).
fn batchable(connection: &ControlConnection) -> Option<&str> {
    let account = &connection.identity.as_ref()?.account.id;
    (connection.auth_token.starts_with("gvx_session_")
        && connection.impersonation_id.is_empty()
        && !account.is_empty()
        && connection
            .tenant
            .as_ref()
            .is_none_or(|session| !session.is_anonymous()))
    .then_some(account.as_str())
}

impl AuthRevalidator {
    /// Keeps `watch` registered for `connection`'s current credentials:
    /// unchanged credentials cost three string compares; changed ones
    /// re-register; a connection that is not batchable gets `None` (and keeps
    /// its per-connection timer).
    pub(crate) fn sync(&self, watch: &mut Option<AuthWatch>, connection: &ControlConnection) {
        if connection.auth_token.is_empty() {
            *watch = None;
            return;
        }
        if watch.as_ref().is_some_and(|watch| watch.matches(connection)) {
            return;
        }
        *watch = batchable(connection).map(|account| self.register(connection, account));
    }

    fn register(&self, connection: &ControlConnection, account: &str) -> AuthWatch {
        let id = self.inner.next.fetch_add(1, Relaxed);
        let signal = Arc::new(Signal::default());
        let tenant = connection
            .tenant
            .as_ref()
            .map(|session| session.route.tenant_id.clone());
        lock(&self.inner.entries).insert(
            id,
            Entry {
                project: connection.project_id.clone(),
                tenant: tenant.clone(),
                token_hash: gonvex_store::token_hash(connection.auth_token.trim()),
                account_id: account.to_owned(),
                signal: signal.clone(),
            },
        );
        AuthWatch {
            id,
            token: connection.auth_token.clone(),
            project: connection.project_id.clone(),
            tenant,
            signal,
            owner: self.inner.clone(),
        }
    }

    pub(crate) fn registered(&self) -> usize {
        lock(&self.inner.entries).len()
    }

    /// (rounds run, connections revoked) since start.
    pub(crate) fn counters(&self) -> (u64, u64) {
        (self.inner.rounds.load(Relaxed), self.inner.revoked.load(Relaxed))
    }

    /// One revalidation round over every registered connection.
    pub(crate) async fn round(&self, control: Option<&ControlPlane>) {
        // (project, token hash, tenant, account, signal) of every entry.
        let entries: Vec<(String, String, Option<String>, String, Arc<Signal>)> =
            lock(&self.inner.entries)
                .values()
                .map(|entry| {
                    (
                        entry.project.clone(),
                        entry.token_hash.clone(),
                        entry.tenant.clone(),
                        entry.account_id.clone(),
                        entry.signal.clone(),
                    )
                })
                .collect();
        self.inner.rounds.fetch_add(1, Relaxed);
        if entries.is_empty() {
            return;
        }
        let revoke = |signal: &Signal| {
            if !signal.revoked.load(Relaxed) {
                self.inner.revoked.fetch_add(1, Relaxed);
                signal.revoke();
            }
        };
        let Some(control) = control else {
            // `revalidate_connection` without a Control Plane: invalid.
            entries.iter().for_each(|entry| revoke(&entry.4));
            return;
        };
        // Sessions, per project.
        let mut by_project = BTreeMap::<&str, Vec<String>>::new();
        for (project, hash, ..) in &entries {
            by_project.entry(project).or_default().push(hash.clone());
        }
        let mut valid_sessions = HashMap::<(&str, String), bool>::new();
        for (project, mut hashes) in by_project {
            hashes.sort_unstable();
            hashes.dedup();
            for chunk in hashes.chunks(CHUNK) {
                let valid = match control.valid_session_hashes(project, chunk).await {
                    Ok(valid) => valid,
                    Err(error) => {
                        tracing::warn!(%error, project, sessions = chunk.len(), "session revalidation failed");
                        Default::default()
                    }
                };
                for hash in chunk {
                    valid_sessions.insert((project, hash.clone()), valid.contains(hash));
                }
            }
        }
        // Members, per (project, tenant), for connections whose session is valid.
        let mut by_tenant = BTreeMap::<(&str, &str), Vec<String>>::new();
        for (project, hash, tenant, account, _) in &entries {
            if let Some(tenant) = tenant {
                if valid_sessions.get(&(project.as_str(), hash.clone())) == Some(&true) {
                    by_tenant
                        .entry((project, tenant))
                        .or_default()
                        .push(account.clone());
                }
            }
        }
        let mut active_members = HashMap::<(&str, &str, String), bool>::new();
        for ((project, tenant), mut accounts) in by_tenant {
            accounts.sort_unstable();
            accounts.dedup();
            for chunk in accounts.chunks(CHUNK) {
                let active = match control.active_member_accounts(project, tenant, chunk).await {
                    Ok(active) => active,
                    Err(error) => {
                        tracing::warn!(%error, project, tenant, members = chunk.len(), "member revalidation failed");
                        Default::default()
                    }
                };
                for account in chunk {
                    active_members.insert((project, tenant, account.clone()), active.contains(account));
                }
            }
        }
        for (project, hash, tenant, account, signal) in &entries {
            let session = valid_sessions.get(&(project.as_str(), hash.clone())) == Some(&true);
            let member = tenant.as_deref().is_none_or(|tenant| {
                active_members.get(&(project.as_str(), tenant, account.clone())) == Some(&true)
            });
            if !(session && member) {
                revoke(signal);
            }
        }
    }
}
