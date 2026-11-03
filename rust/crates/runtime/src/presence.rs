//! Presence v2: in-memory, transition-only tenant presence.
//!
//! The old presence path was an application Reducer per 30-second heartbeat:
//! V8, the tenant's single SQLite writer, a `synchronous=full` commit and a
//! Replica fan-out of the touched row to every tenant member. This module
//! replaces it with process-local state:
//!
//! * A connection reports its own state (`active`/`idle`) only when it
//!   changes (`presence.set`). Liveness is the WebSocket itself: the socket
//!   closing (or failing the server's Ping liveness check) removes the
//!   connection. There is no periodic client heartbeat.
//! * A Member's state is the aggregate of their connections in one
//!   `(project, tenant)` scope: `active` if any connection is active, `idle`
//!   if all are idle, `offline` once the last connection is gone (after a
//!   short grace that absorbs reconnects; an explicit `offline` skips it).
//! * Only aggregate transitions are published. A tick coalesces every change
//!   in a scope into one `presence.patch`, encoded once and shared by every
//!   subscribed connection of that scope. Each connection owns a pending slot
//!   that its socket task drains; a future per-connection frame batcher can
//!   drain the same slot on its own tick.
//! * Audience: subscribed connections of the same `(project, tenant)` only.
//!   Scopes never share state, so presence cannot cross tenants or projects.
//!
//! Support sessions (`control.support.heartbeat`, `error.register`,
//! `error.heartbeat`) keep their durable row, written once per connection;
//! repeated heartbeats are answered from memory and `last_seen_at` is written
//! lazily when the connection closes (batched by `take_last_seen_flush`).
//!
//! Nothing here is durable. After a restart every client reconnects and
//! re-announces its state; the application displays no last-seen time.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use gonvex_protocol::{PresenceMember, PresenceState, ServerMessage};
use tokio::sync::Notify;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PresenceConfig {
    /// `GONVEX_PRESENCE_V2` (default on). Off: no capability is advertised,
    /// `presence.*` frames are rejected and heartbeats keep their old writes.
    pub enabled: bool,
    /// `GONVEX_PRESENCE_TICK_MS` (default 1000): coalescing window.
    pub tick: Duration,
    /// `GONVEX_PRESENCE_OFFLINE_GRACE_MS` (default 10000): how long a Member
    /// whose last connection closed stays published before `offline`.
    pub offline_grace: Duration,
    /// `GONVEX_PRESENCE_PING_SECONDS` (default 20): server WebSocket Ping
    /// interval for presence-v2 connections.
    pub ping_interval: Duration,
    /// `GONVEX_PRESENCE_LIVENESS_SECONDS` (default 60): a presence-v2
    /// connection with no inbound frame (Pong included) for this long is
    /// closed and its presence removed.
    pub liveness_timeout: Duration,
    /// `GONVEX_PRESENCE_LEGACY_BEAT_PATHS`: application keepalive Reducers
    /// that old clients still call. When listed, the runtime answers them in
    /// Rust (no V8, no write) and records the caller as present.
    pub legacy_beat_paths: Vec<String>,
    /// `GONVEX_PRESENCE_LEGACY_TRANSITION_PATHS`: application transition
    /// Reducers (`{state}` argument). They still run normally; a successful
    /// call from a client without presence v2 also updates the in-memory feed.
    pub legacy_transition_paths: Vec<String>,
    /// Pending patches a connection may accumulate before it is resynchronized
    /// with a snapshot instead.
    pub max_pending_batches: usize,
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            tick: Duration::from_millis(1_000),
            offline_grace: Duration::from_secs(10),
            ping_interval: Duration::from_secs(20),
            liveness_timeout: Duration::from_secs(60),
            legacy_beat_paths: Vec::new(),
            legacy_transition_paths: Vec::new(),
            max_pending_batches: 32,
        }
    }
}

/// Parses a client-reported state. `gone` is the old application spelling.
pub fn parse_state(value: &str) -> Option<PresenceState> {
    match value.trim().to_ascii_lowercase().as_str() {
        "active" => Some(PresenceState::Active),
        "idle" => Some(PresenceState::Idle),
        "offline" | "gone" => Some(PresenceState::Offline),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopeKey {
    pub project: Arc<str>,
    pub tenant: Arc<str>,
}

impl ScopeKey {
    pub fn new(project: &str, tenant: &str) -> Self {
        Self {
            project: project.into(),
            tenant: tenant.into(),
        }
    }
}

/// One coalesced tick of changes for one scope, encoded once.
#[derive(Debug)]
pub struct PresenceBatch {
    pub server_time: i64,
    pub changes: Vec<PresenceMember>,
    pub encoded: Arc<str>,
}

#[derive(Default)]
struct SlotState {
    batches: Vec<Arc<PresenceBatch>>,
    resync: bool,
}

/// Per-connection pending presence output. The hub fills it on a tick; the
/// connection drains it (one frame per drain).
#[derive(Default)]
pub struct PresenceSlot {
    state: Mutex<SlotState>,
    notify: Notify,
}

impl PresenceSlot {
    pub async fn notified(&self) {
        self.notify.notified().await;
    }

    fn push(&self, batch: Arc<PresenceBatch>, max: usize) {
        let mut state = lock(&self.state);
        if state.resync {
            return;
        }
        if state.batches.len() >= max {
            state.batches.clear();
            state.resync = true;
        } else {
            state.batches.push(batch);
        }
        drop(state);
        self.notify.notify_one();
    }

    fn take(&self) -> (Vec<Arc<PresenceBatch>>, bool) {
        let mut state = lock(&self.state);
        let resync = std::mem::take(&mut state.resync);
        (std::mem::take(&mut state.batches), resync)
    }

    /// Undelivered batches (for tests and metrics).
    pub fn pending(&self) -> usize {
        lock(&self.state).batches.len()
    }
}

#[derive(Default)]
struct Member {
    /// Connection id → that connection's own reported state (active/idle).
    connections: HashMap<Arc<str>, PresenceState>,
    published: Option<(PresenceState, i64)>,
    grace_until: Option<Instant>,
}

impl Member {
    fn aggregate(&self) -> Option<PresenceState> {
        if self.connections.is_empty() {
            None
        } else if self
            .connections
            .values()
            .any(|state| *state == PresenceState::Active)
        {
            Some(PresenceState::Active)
        } else {
            Some(PresenceState::Idle)
        }
    }
}

#[derive(Default)]
struct Scope {
    members: HashMap<Arc<str>, Member>,
    subscribers: HashMap<Arc<str>, Arc<PresenceSlot>>,
    dirty: HashSet<Arc<str>>,
}

struct Registration {
    scope: ScopeKey,
    member: Option<Arc<str>>,
    subscription_id: Option<String>,
    legacy: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct SupportKey {
    tenant: String,
    account: String,
    release: String,
    environment: String,
}

struct SupportLive {
    project: String,
    connection: Arc<str>,
    key: SupportKey,
}

#[derive(Default)]
struct HubState {
    scopes: HashMap<ScopeKey, Scope>,
    connections: HashMap<Arc<str>, Registration>,
    support: HashMap<String, SupportLive>,
    support_by_connection: HashMap<Arc<str>, Vec<String>>,
    last_seen_flush: Vec<SupportLastSeen>,
}

/// A support session whose connection closed; its durable `last_seen_at`
/// is written by the batched flusher.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SupportLastSeen {
    pub project_id: String,
    pub session_id: String,
    pub last_seen_ms: i64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TickStats {
    pub scopes: usize,
    pub changes: usize,
    pub frames: usize,
}

#[derive(Clone)]
pub struct PresenceHub {
    config: Arc<PresenceConfig>,
    state: Arc<Mutex<HubState>>,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis() as i64)
        .unwrap_or_default()
}

impl Default for PresenceHub {
    fn default() -> Self {
        Self::new(PresenceConfig::default())
    }
}

impl PresenceHub {
    pub fn new(config: PresenceConfig) -> Self {
        Self {
            config: Arc::new(config),
            state: Arc::new(Mutex::new(HubState::default())),
        }
    }

    pub fn config(&self) -> &PresenceConfig {
        &self.config
    }

    pub fn enabled(&self) -> bool {
        self.config.enabled
    }

    pub fn is_legacy_beat(&self, path: &str) -> bool {
        self.config.enabled && self.config.legacy_beat_paths.iter().any(|p| p == path)
    }

    pub fn is_legacy_transition(&self, path: &str) -> bool {
        self.config.enabled
            && self
                .config
                .legacy_transition_paths
                .iter()
                .any(|p| p == path)
    }

    fn lock(&self) -> MutexGuard<'_, HubState> {
        lock(&self.state)
    }

    /// Sets this connection's own state for `member` in `scope`. `Offline`
    /// removes the connection immediately (no grace).
    pub fn set_state(
        &self,
        connection: &str,
        scope: &ScopeKey,
        member: &str,
        state: PresenceState,
        legacy: bool,
        now: Instant,
    ) {
        let mut hub = self.lock();
        let connection: Arc<str> = connection.into();
        let moved = hub
            .connections
            .get(&connection)
            .is_some_and(|registration| {
                registration.scope != *scope
                    || registration
                        .member
                        .as_deref()
                        .is_some_and(|current| current != member)
            });
        if moved {
            detach(&mut hub, &connection, now, &self.config, false, true);
        }
        let member: Arc<str> = {
            let registration = hub
                .connections
                .entry(connection.clone())
                .or_insert_with(|| Registration {
                    scope: scope.clone(),
                    member: None,
                    subscription_id: None,
                    legacy,
                });
            registration.legacy = legacy;
            match &registration.member {
                Some(existing) => existing.clone(),
                None => {
                    let member: Arc<str> = member.into();
                    registration.member = Some(member.clone());
                    member
                }
            }
        };
        let tenant = hub.scopes.entry(scope.clone()).or_default();
        let entry = tenant.members.entry(member.clone()).or_default();
        if state == PresenceState::Offline {
            entry.connections.remove(&connection);
            if entry.connections.is_empty() {
                entry.grace_until = None;
            }
            if let Some(registration) = hub.connections.get_mut(&connection) {
                registration.member = None;
            }
        } else {
            entry.connections.insert(connection, state);
            entry.grace_until = None;
        }
        hub.scopes
            .get_mut(scope)
            .expect("scope inserted above")
            .dirty
            .insert(member);
    }

    /// An old client's keepalive Reducer: registers the connection as active
    /// if it is not yet present; otherwise it changes nothing.
    pub fn legacy_beat(&self, connection: &str, scope: &ScopeKey, member: &str, now: Instant) {
        {
            let hub = self.lock();
            if let Some(registration) = hub.connections.get(connection) {
                if registration.scope == *scope && registration.member.as_deref() == Some(member) {
                    return;
                }
            }
        }
        self.set_state(connection, scope, member, PresenceState::Active, true, now);
    }

    /// Subscribes the connection to its scope's feed. Returns the slot to
    /// drain and the encoded snapshot to send first.
    pub fn subscribe(
        &self,
        connection: &str,
        scope: &ScopeKey,
        id: &str,
        now: Instant,
    ) -> (Arc<PresenceSlot>, String) {
        let mut hub = self.lock();
        let connection: Arc<str> = connection.into();
        if hub
            .connections
            .get(&connection)
            .is_some_and(|registration| registration.scope != *scope)
        {
            detach(&mut hub, &connection, now, &self.config, false, true);
        }
        let registration = hub
            .connections
            .entry(connection.clone())
            .or_insert_with(|| Registration {
                scope: scope.clone(),
                member: None,
                subscription_id: None,
                legacy: false,
            });
        registration.subscription_id = Some(id.to_owned());
        let slot = Arc::new(PresenceSlot::default());
        let tenant = hub.scopes.entry(scope.clone()).or_default();
        tenant.subscribers.insert(connection, slot.clone());
        let snapshot = encode_snapshot(tenant, id);
        (slot, snapshot)
    }

    pub fn unsubscribe(&self, connection: &str) {
        let mut hub = self.lock();
        let Some(registration) = hub.connections.get_mut(connection) else {
            return;
        };
        registration.subscription_id = None;
        let scope = registration.scope.clone();
        let remove = registration.member.is_none();
        if let Some(tenant) = hub.scopes.get_mut(&scope) {
            tenant.subscribers.remove(connection);
        }
        if remove {
            hub.connections.remove(connection);
        }
    }

    /// The connection closed or changed session: remove it with grace.
    pub fn disconnect(&self, connection: &str, now: Instant) {
        let mut hub = self.lock();
        let connection: Arc<str> = connection.into();
        detach(&mut hub, &connection, now, &self.config, true, true);
        if let Some(sessions) = hub.support_by_connection.remove(&connection) {
            let last_seen_ms = now_ms();
            for session_id in sessions {
                if let Some(live) = hub.support.remove(&session_id) {
                    hub.last_seen_flush.push(SupportLastSeen {
                        project_id: live.project,
                        session_id,
                        last_seen_ms,
                    });
                }
            }
        }
    }

    /// Drops the connection's presence and subscription after its session
    /// changed (re-auth, revocation). Grace applies, so a same-tenant
    /// re-auth that immediately re-announces causes no visible transition.
    pub fn reset(&self, connection: &str, now: Instant) {
        let mut hub = self.lock();
        let connection: Arc<str> = connection.into();
        detach(&mut hub, &connection, now, &self.config, true, true);
    }

    /// Drains the connection's slot into at most one frame.
    pub fn drain(&self, connection: &str, slot: &PresenceSlot) -> Option<String> {
        let (batches, resync) = slot.take();
        if resync {
            let hub = self.lock();
            let registration = hub.connections.get(connection)?;
            let id = registration.subscription_id.clone()?;
            let tenant = hub.scopes.get(&registration.scope)?;
            return Some(encode_snapshot(tenant, &id));
        }
        match batches.len() {
            0 => None,
            1 => Some(batches[0].encoded.to_string()),
            _ => {
                let mut merged: Vec<PresenceMember> = Vec::new();
                let mut index: HashMap<&str, usize> = HashMap::new();
                for batch in &batches {
                    for change in &batch.changes {
                        if let Some(position) = index.get(change.member_id.as_str()) {
                            merged[*position] = change.clone();
                        } else {
                            index.insert(change.member_id.as_str(), merged.len());
                            merged.push(change.clone());
                        }
                    }
                }
                let server_time = batches.last().map(|b| b.server_time).unwrap_or_default();
                serde_json::to_string(&ServerMessage::PresencePatch {
                    server_time,
                    changes: merged,
                })
                .ok()
            }
        }
    }

    /// Publishes every pending aggregate transition. Called once per tick.
    pub fn tick(&self, now: Instant, wall_ms: i64) -> TickStats {
        let mut stats = TickStats::default();
        let mut hub = self.lock();
        let max = self.config.max_pending_batches;
        hub.scopes.retain(|_, tenant| {
            if tenant.dirty.is_empty() {
                return !(tenant.members.is_empty() && tenant.subscribers.is_empty());
            }
            stats.scopes += 1;
            let mut changes = Vec::new();
            let dirty: Vec<Arc<str>> = tenant.dirty.iter().cloned().collect();
            for member_id in dirty {
                let Some(member) = tenant.members.get_mut(&member_id) else {
                    tenant.dirty.remove(&member_id);
                    continue;
                };
                let next = match member.aggregate() {
                    Some(state) => state,
                    None if member.grace_until.is_some_and(|until| until > now) => continue,
                    None => PresenceState::Offline,
                };
                tenant.dirty.remove(&member_id);
                let previous = member
                    .published
                    .map(|(state, _)| state)
                    .unwrap_or(PresenceState::Offline);
                if next != previous {
                    changes.push(PresenceMember {
                        member_id: member_id.to_string(),
                        state: next,
                        since: wall_ms,
                    });
                    member.published = Some((next, wall_ms));
                }
                if next == PresenceState::Offline {
                    tenant.members.remove(&member_id);
                }
            }
            if !changes.is_empty() {
                changes.sort_by(|a, b| a.member_id.cmp(&b.member_id));
                stats.changes += changes.len();
                let encoded = serde_json::to_string(&ServerMessage::PresencePatch {
                    server_time: wall_ms,
                    changes: changes.clone(),
                })
                .unwrap_or_default();
                let batch = Arc::new(PresenceBatch {
                    server_time: wall_ms,
                    changes,
                    encoded: encoded.into(),
                });
                for slot in tenant.subscribers.values() {
                    slot.push(batch.clone(), max);
                    stats.frames += 1;
                }
            }
            !(tenant.members.is_empty() && tenant.subscribers.is_empty())
        });
        stats
    }

    /// Online Members of a scope (for tests and diagnostics).
    pub fn published(&self, scope: &ScopeKey) -> Vec<PresenceMember> {
        let hub = self.lock();
        hub.scopes
            .get(scope)
            .map(published_members)
            .unwrap_or_default()
    }

    pub fn tracked_members(&self) -> usize {
        self.lock().scopes.values().map(|s| s.members.len()).sum()
    }

    // ---- Support sessions -------------------------------------------------

    /// True when `session_id` is already registered by this live connection
    /// with the same attribution, so a heartbeat needs no write. `None`
    /// release/environment (error heartbeats) match any registered values.
    #[allow(clippy::too_many_arguments)]
    pub fn support_is_current(
        &self,
        session_id: &str,
        project: &str,
        connection: &str,
        tenant: &str,
        account: &str,
        release: Option<&str>,
        environment: Option<&str>,
    ) -> bool {
        if !self.config.enabled {
            return false;
        }
        let hub = self.lock();
        hub.support.get(session_id).is_some_and(|live| {
            live.project == project
                && &*live.connection == connection
                && live.key.tenant == tenant
                && live.key.account == account
                && release.is_none_or(|release| live.key.release == release)
                && environment.is_none_or(|environment| live.key.environment == environment)
        })
    }

    /// Records a support session after its durable upsert committed.
    #[allow(clippy::too_many_arguments)]
    pub fn support_register(
        &self,
        session_id: &str,
        project: &str,
        connection: &str,
        tenant: &str,
        account: &str,
        release: Option<&str>,
        environment: Option<&str>,
    ) {
        if !self.config.enabled {
            return;
        }
        let mut hub = self.lock();
        let connection: Arc<str> = connection.into();
        let previous = hub.support.get(session_id).map(|live| live.key.clone());
        let key = SupportKey {
            tenant: tenant.to_owned(),
            account: account.to_owned(),
            release: release
                .map(str::to_owned)
                .or_else(|| previous.as_ref().map(|k| k.release.clone()))
                .unwrap_or_default(),
            environment: environment
                .map(str::to_owned)
                .or_else(|| previous.as_ref().map(|k| k.environment.clone()))
                .unwrap_or_default(),
        };
        hub.support.insert(
            session_id.to_owned(),
            SupportLive {
                project: project.to_owned(),
                connection: connection.clone(),
                key,
            },
        );
        let sessions = hub.support_by_connection.entry(connection).or_default();
        if !sessions.iter().any(|id| id == session_id) {
            sessions.push(session_id.to_owned());
        }
    }

    /// Session ids whose connection is open on this runtime: they are seen
    /// "now" even though their durable `last_seen_at` is not refreshed.
    pub fn support_live_ids(&self, project: &str) -> HashSet<String> {
        self.lock()
            .support
            .iter()
            .filter(|(_, live)| live.project == project)
            .map(|(id, _)| id.clone())
            .collect()
    }

    pub fn take_last_seen_flush(&self) -> Vec<SupportLastSeen> {
        std::mem::take(&mut self.lock().last_seen_flush)
    }
}

/// Removes a connection's presence (with grace) and support sessions when
/// its socket task ends, however it ends.
pub struct ConnectionGuard {
    hub: PresenceHub,
    connection: String,
}

impl ConnectionGuard {
    pub fn new(hub: PresenceHub, connection: &str) -> Self {
        Self {
            hub,
            connection: connection.to_owned(),
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.hub.disconnect(&self.connection, Instant::now());
    }
}

fn published_members(tenant: &Scope) -> Vec<PresenceMember> {
    let mut members: Vec<PresenceMember> = tenant
        .members
        .iter()
        .filter_map(|(id, member)| match member.published {
            Some((state, since)) if state != PresenceState::Offline => Some(PresenceMember {
                member_id: id.to_string(),
                state,
                since,
            }),
            _ => None,
        })
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));
    members
}

fn encode_snapshot(tenant: &Scope, id: &str) -> String {
    serde_json::to_string(&ServerMessage::PresenceSnapshot {
        id: id.to_owned(),
        server_time: now_ms(),
        members: published_members(tenant),
    })
    .unwrap_or_default()
}

/// Removes the connection's member state and (optionally) its subscription.
fn detach(
    hub: &mut HubState,
    connection: &Arc<str>,
    now: Instant,
    config: &PresenceConfig,
    grace: bool,
    unsubscribe: bool,
) {
    let Some(registration) = hub.connections.remove(connection) else {
        return;
    };
    let Some(tenant) = hub.scopes.get_mut(&registration.scope) else {
        return;
    };
    if unsubscribe {
        tenant.subscribers.remove(connection);
    }
    if let Some(member_id) = registration.member {
        if let Some(member) = tenant.members.get_mut(&member_id) {
            if member.connections.remove(connection).is_some() && member.connections.is_empty() {
                member.grace_until = grace.then(|| now + config.offline_grace);
            }
            tenant.dirty.insert(member_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hub() -> PresenceHub {
        PresenceHub::new(PresenceConfig {
            offline_grace: Duration::from_secs(10),
            ..PresenceConfig::default()
        })
    }

    fn scope(tenant: &str) -> ScopeKey {
        ScopeKey::new("project", tenant)
    }

    fn frames(hub: &PresenceHub, connection: &str, slot: &PresenceSlot) -> Vec<serde_json::Value> {
        hub.drain(connection, slot)
            .map(|frame| serde_json::from_str(&frame).unwrap())
            .into_iter()
            .collect()
    }

    fn changes(frame: &serde_json::Value) -> Vec<(String, String)> {
        frame["changes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| {
                (
                    c["memberId"].as_str().unwrap().to_owned(),
                    c["state"].as_str().unwrap().to_owned(),
                )
            })
            .collect()
    }

    #[test]
    fn transitions_are_published_once_per_tick_and_only_on_change() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot, snapshot) = hub.subscribe("viewer", &scope("a"), "p1", t0);
        let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(snapshot["type"], "presence.snapshot");
        assert_eq!(snapshot["id"], "p1");
        assert_eq!(snapshot["members"], serde_json::json!([]));

        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.set_state("c2", &scope("a"), "m2", PresenceState::Idle, false, t0);
        let stats = hub.tick(t0, 1_000);
        assert_eq!(
            stats,
            TickStats {
                scopes: 1,
                changes: 2,
                frames: 1
            }
        );
        let got = frames(&hub, "viewer", &slot);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["type"], "presence.patch");
        assert_eq!(
            changes(&got[0]),
            vec![("m1".into(), "active".into()), ("m2".into(), "idle".into())]
        );
        // Re-reporting the same state publishes nothing.
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        assert_eq!(hub.tick(t0, 2_000).changes, 0);
        assert!(frames(&hub, "viewer", &slot).is_empty());
        // A flap inside one tick (active -> idle -> active) is invisible.
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        assert_eq!(hub.tick(t0, 3_000).changes, 0);
    }

    #[test]
    fn presence_never_crosses_tenants_or_projects() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot_a, _) = hub.subscribe("viewer-a", &scope("a"), "p", t0);
        let (slot_b, _) = hub.subscribe("viewer-b", &scope("b"), "p", t0);
        let other_project = ScopeKey::new("other", "a");
        let (slot_p, _) = hub.subscribe("viewer-p", &other_project, "p", t0);
        hub.set_state(
            "c1",
            &scope("a"),
            "same-member-id",
            PresenceState::Active,
            false,
            t0,
        );
        hub.tick(t0, 1);
        assert_eq!(frames(&hub, "viewer-a", &slot_a).len(), 1);
        assert!(frames(&hub, "viewer-b", &slot_b).is_empty());
        assert!(frames(&hub, "viewer-p", &slot_p).is_empty());
        let (_, snapshot_b) = hub.subscribe("viewer-b2", &scope("b"), "p", t0);
        assert!(!snapshot_b.contains("same-member-id"));
        assert!(hub.published(&other_project).is_empty());
        assert_eq!(hub.published(&scope("a")).len(), 1);
    }

    #[test]
    fn multiple_tabs_aggregate_to_the_most_present_state() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot, _) = hub.subscribe("viewer", &scope("a"), "p", t0);
        hub.set_state("tab1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.set_state("tab2", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.tick(t0, 1);
        assert_eq!(
            changes(&frames(&hub, "viewer", &slot)[0]),
            vec![("m1".into(), "active".into())]
        );
        // One tab hidden: still active because the other is.
        hub.set_state("tab1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.tick(t0, 2);
        assert_eq!(
            changes(&frames(&hub, "viewer", &slot)[0]),
            vec![("m1".into(), "idle".into())]
        );
        // Closing one tab keeps the Member online through the other.
        hub.disconnect("tab1", t0);
        hub.tick(t0 + Duration::from_secs(60), 3);
        assert!(frames(&hub, "viewer", &slot).is_empty());
        // An explicit offline from the last tab skips the grace period.
        hub.set_state("tab2", &scope("a"), "m1", PresenceState::Offline, false, t0);
        hub.tick(t0, 4);
        assert_eq!(
            changes(&frames(&hub, "viewer", &slot)[0]),
            vec![("m1".into(), "offline".into())]
        );
        assert_eq!(hub.tracked_members(), 0);
    }

    #[test]
    fn disconnect_goes_offline_only_after_grace_and_reconnect_hides_the_gap() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot, _) = hub.subscribe("viewer", &scope("a"), "p", t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.tick(t0, 1);
        frames(&hub, "viewer", &slot);
        // Socket drops and the client reconnects on a new connection 2 s later.
        hub.disconnect("c1", t0);
        hub.tick(t0 + Duration::from_secs(1), 2);
        hub.set_state(
            "c2",
            &scope("a"),
            "m1",
            PresenceState::Active,
            false,
            t0 + Duration::from_secs(2),
        );
        hub.tick(t0 + Duration::from_secs(3), 3);
        hub.tick(t0 + Duration::from_secs(30), 4);
        assert!(
            frames(&hub, "viewer", &slot).is_empty(),
            "no offline/online flicker"
        );
        // A real disconnect: offline exactly once the grace has elapsed.
        hub.disconnect("c2", t0 + Duration::from_secs(40));
        hub.tick(t0 + Duration::from_secs(49), 5);
        assert!(frames(&hub, "viewer", &slot).is_empty());
        hub.tick(t0 + Duration::from_secs(50), 6);
        let got = frames(&hub, "viewer", &slot);
        assert_eq!(changes(&got[0]), vec![("m1".into(), "offline".into())]);
        assert_eq!(got[0]["changes"][0]["since"], 6);
        assert!(hub.published(&scope("a")).is_empty());
    }

    #[test]
    fn snapshot_lists_online_members_and_patches_follow_it() {
        let hub = hub();
        let t0 = Instant::now();
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.set_state("c2", &scope("a"), "m2", PresenceState::Active, false, t0);
        hub.tick(t0, 7);
        // m3 is dirty but not yet ticked: snapshot shows published state only.
        hub.set_state("c3", &scope("a"), "m3", PresenceState::Active, false, t0);
        let (slot, snapshot) = hub.subscribe("viewer", &scope("a"), "s1", t0);
        let snapshot: serde_json::Value = serde_json::from_str(&snapshot).unwrap();
        assert_eq!(
            snapshot["members"],
            serde_json::json!([
                {"memberId":"m1","state":"idle","since":7},
                {"memberId":"m2","state":"active","since":7}
            ])
        );
        hub.tick(t0, 8);
        assert_eq!(
            changes(&frames(&hub, "viewer", &slot)[0]),
            vec![("m3".into(), "active".into())]
        );
    }

    #[test]
    fn slow_connection_merges_batches_then_resyncs_with_a_snapshot() {
        let hub = PresenceHub::new(PresenceConfig {
            max_pending_batches: 3,
            ..PresenceConfig::default()
        });
        let t0 = Instant::now();
        let (slot, _) = hub.subscribe("viewer", &scope("a"), "s", t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.tick(t0, 1);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.tick(t0, 2);
        assert_eq!(slot.pending(), 2);
        let merged = frames(&hub, "viewer", &slot);
        assert_eq!(merged.len(), 1);
        assert_eq!(changes(&merged[0]), vec![("m1".into(), "idle".into())]);
        for (i, state) in [
            PresenceState::Active,
            PresenceState::Idle,
            PresenceState::Active,
            PresenceState::Idle,
        ]
        .into_iter()
        .enumerate()
        {
            hub.set_state("c1", &scope("a"), "m1", state, false, t0);
            hub.tick(t0, 10 + i as i64);
        }
        let resync = frames(&hub, "viewer", &slot);
        assert_eq!(resync[0]["type"], "presence.snapshot");
        assert_eq!(resync[0]["id"], "s");
        assert_eq!(resync[0]["members"][0]["state"], "idle");
    }

    #[test]
    fn moving_a_connection_to_another_tenant_leaves_the_old_one() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot_a, _) = hub.subscribe("viewer-a", &scope("a"), "p", t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.tick(t0, 1);
        frames(&hub, "viewer-a", &slot_a);
        hub.set_state("c1", &scope("b"), "m9", PresenceState::Active, false, t0);
        hub.tick(t0 + Duration::from_secs(11), 2);
        assert_eq!(
            changes(&frames(&hub, "viewer-a", &slot_a)[0]),
            vec![("m1".into(), "offline".into())]
        );
        assert_eq!(hub.published(&scope("b"))[0].member_id, "m9");
    }

    #[test]
    fn unsubscribed_or_closed_connections_receive_nothing() {
        let hub = hub();
        let t0 = Instant::now();
        let (slot, _) = hub.subscribe("viewer", &scope("a"), "p", t0);
        hub.unsubscribe("viewer");
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Active, false, t0);
        hub.tick(t0, 1);
        assert_eq!(slot.pending(), 0);
        let (slot, _) = hub.subscribe("viewer", &scope("a"), "p", t0);
        hub.disconnect("viewer", t0);
        hub.set_state("c1", &scope("a"), "m1", PresenceState::Idle, false, t0);
        hub.tick(t0, 2);
        assert_eq!(slot.pending(), 0);
    }

    #[test]
    fn legacy_beat_registers_once_and_never_overrides_reported_state() {
        let hub = hub();
        let t0 = Instant::now();
        hub.legacy_beat("old", &scope("a"), "m1", t0);
        hub.tick(t0, 1);
        assert_eq!(hub.published(&scope("a"))[0].state, PresenceState::Active);
        hub.set_state("old", &scope("a"), "m1", PresenceState::Idle, true, t0);
        hub.legacy_beat("old", &scope("a"), "m1", t0);
        hub.tick(t0, 2);
        assert_eq!(hub.published(&scope("a"))[0].state, PresenceState::Idle);
        // Old clients' `gone` transition removes them immediately.
        hub.set_state(
            "old",
            &scope("a"),
            "m1",
            parse_state("gone").unwrap(),
            true,
            t0,
        );
        hub.tick(t0, 3);
        assert!(hub.published(&scope("a")).is_empty());
    }

    #[test]
    fn support_sessions_write_once_and_flush_last_seen_on_close() {
        let hub = hub();
        assert!(!hub.support_is_current(
            "support_c1",
            "p",
            "c1",
            "t",
            "a",
            Some("1.0"),
            Some("prod")
        ));
        hub.support_register("support_c1", "p", "c1", "t", "a", Some("1.0"), Some("prod"));
        assert!(hub.support_is_current(
            "support_c1",
            "p",
            "c1",
            "t",
            "a",
            Some("1.0"),
            Some("prod")
        ));
        assert!(hub.support_is_current("support_c1", "p", "c1", "t", "a", None, None));
        // A different release, tenant or account needs a durable write.
        assert!(!hub.support_is_current(
            "support_c1",
            "p",
            "c1",
            "t",
            "a",
            Some("1.1"),
            Some("prod")
        ));
        assert!(!hub.support_is_current("support_c1", "p", "c1", "t2", "a", None, None));
        assert!(!hub.support_is_current("support_c1", "p", "c1", "t", "b", None, None));
        assert!(!hub.support_is_current("support_c1", "other", "c1", "t", "a", None, None));
        assert_eq!(
            hub.support_live_ids("p"),
            HashSet::from(["support_c1".to_owned()])
        );
        assert!(hub.support_live_ids("other").is_empty());
        hub.disconnect("c1", Instant::now());
        assert!(hub.support_live_ids("p").is_empty());
        let flushed = hub.take_last_seen_flush();
        assert_eq!(flushed.len(), 1);
        assert_eq!(flushed[0].session_id, "support_c1");
        assert!(hub.take_last_seen_flush().is_empty());
    }

    #[test]
    fn disabled_hub_never_short_circuits() {
        let hub = PresenceHub::new(PresenceConfig {
            enabled: false,
            legacy_beat_paths: vec!["presence.beat".into()],
            ..PresenceConfig::default()
        });
        assert!(!hub.is_legacy_beat("presence.beat"));
        hub.support_register("s", "p", "c", "t", "a", None, None);
        assert!(!hub.support_is_current("s", "p", "c", "t", "a", None, None));
    }
}
