//! Process-local observability for the Rust runtime.
//!
//! Durable function/error history lives in the Control Plane. Connection
//! presence is deliberately process-local because a WebSocket belongs to one
//! runtime process and must disappear as soon as that socket closes.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use chrono::{SecondsFormat, Utc};
use gonvex_protocol::BrowserTelemetryInfo;
use serde_json::{json, Value};

use crate::control::ControlConnection;

#[derive(Clone, Default)]
pub struct RuntimeMetrics {
    inner: Arc<Mutex<BTreeMap<String, ConnectionPresence>>>,
}

#[derive(Clone)]
struct ConnectionPresence {
    id: String,
    project: String,
    tenant: String,
    account_id: String,
    account_email: String,
    authenticated: bool,
    connected_at: String,
    last_active_at: String,
    last_activity: String,
    last_path: String,
    browser: String,
    device_type: String,
    platform: String,
    connection_type: String,
    subscriptions: BTreeSet<String>,
}

pub struct ConnectionGuard {
    id: String,
    metrics: RuntimeMetrics,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.metrics.remove(&self.id);
    }
}

impl RuntimeMetrics {
    pub fn register(&self, id: &str) -> ConnectionGuard {
        let now = timestamp();
        self.lock().insert(
            id.to_owned(),
            ConnectionPresence {
                id: id.to_owned(),
                project: String::new(),
                tenant: String::new(),
                account_id: String::new(),
                account_email: String::new(),
                authenticated: false,
                connected_at: now.clone(),
                last_active_at: now,
                last_activity: "connected".to_owned(),
                last_path: String::new(),
                browser: String::new(),
                device_type: String::new(),
                platform: String::new(),
                connection_type: String::new(),
                subscriptions: BTreeSet::new(),
            },
        );
        ConnectionGuard {
            id: id.to_owned(),
            metrics: self.clone(),
        }
    }

    pub fn authenticated(
        &self,
        id: &str,
        connection: &ControlConnection,
        device: Option<&BrowserTelemetryInfo>,
    ) {
        let mut connections = self.lock();
        let Some(presence) = connections.get_mut(id) else {
            return;
        };
        presence.project.clone_from(&connection.project_id);
        presence.tenant = connection
            .tenant
            .as_ref()
            .map(|session| session.route.tenant_id.clone())
            .unwrap_or_default();
        presence.account_id = connection
            .identity
            .as_ref()
            .map(|identity| identity.account.id.clone())
            .unwrap_or_default();
        presence.account_email = connection
            .identity
            .as_ref()
            .map(|identity| identity.account.email.clone())
            .unwrap_or_default();
        presence.authenticated = connection.identity.is_some();
        presence.last_activity = if presence.authenticated {
            "authenticated"
        } else {
            "authentication failed"
        }
        .to_owned();
        presence.last_active_at = timestamp();
        if let Some(device) = device {
            presence.browser = device.browser_name.clone().unwrap_or_default();
            presence.device_type = device.device_type.clone().unwrap_or_default();
            presence.platform = device.platform.clone().unwrap_or_default();
            presence.connection_type = device
                .effective_connection_type
                .clone()
                .or_else(|| device.connection_type.clone())
                .unwrap_or_default();
        }
    }

    pub fn activity(&self, id: &str, activity: &str, path: Option<&str>) {
        let mut connections = self.lock();
        let Some(presence) = connections.get_mut(id) else {
            return;
        };
        presence.last_active_at = timestamp();
        presence.last_activity = activity.to_owned();
        if let Some(path) = path.filter(|path| !path.is_empty()) {
            presence.last_path = path.to_owned();
        }
    }

    pub fn subscriptions<'a>(&self, id: &str, subscriptions: impl IntoIterator<Item = &'a str>) {
        let mut connections = self.lock();
        let Some(presence) = connections.get_mut(id) else {
            return;
        };
        presence.subscriptions = subscriptions.into_iter().map(str::to_owned).collect();
    }

    pub fn snapshot(&self, project: &str) -> Value {
        let connections = self.lock();
        let matching = connections
            .values()
            .filter(|connection| connection.project == project)
            .collect::<Vec<_>>();
        let accounts = matching
            .iter()
            .filter_map(|connection| {
                (!connection.account_id.is_empty()).then_some(connection.account_id.as_str())
            })
            .collect::<BTreeSet<_>>()
            .len();
        let subscriptions = matching
            .iter()
            .map(|connection| connection.subscriptions.len())
            .sum::<usize>();
        let details = matching
            .iter()
            .map(|connection| {
                json!({
                    "id":connection.id,
                    "project":connection.project,
                    "tenant":connection.tenant,
                    "accountId":optional(&connection.account_id),
                    "accountEmail":optional(&connection.account_email),
                    "authenticated":connection.authenticated,
                    "connectedAt":connection.connected_at,
                    "lastActiveAt":connection.last_active_at,
                    "lastActivity":connection.last_activity,
                    "lastPath":optional(&connection.last_path),
                    "browser":optional(&connection.browser),
                    "deviceType":optional(&connection.device_type),
                    "platform":optional(&connection.platform),
                    "connectionType":optional(&connection.connection_type),
                    "subscriptions":connection.subscriptions,
                })
            })
            .collect::<Vec<_>>();
        json!({
            "connections":matching.len(),
            "subscriptions":subscriptions,
            "accounts":accounts,
            "details":details,
        })
    }

    fn remove(&self, id: &str) {
        self.lock().remove(id);
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, ConnectionPresence>> {
        self.inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn timestamp() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true)
}

fn optional(value: &str) -> Option<&str> {
    (!value.is_empty()).then_some(value)
}

#[cfg(test)]
mod tests {
    use gonvex_postgres::{Account, SessionIdentity};

    use super::*;

    #[test]
    fn connection_presence_is_project_scoped_and_removed_with_the_socket() {
        let metrics = RuntimeMetrics::default();
        let guard = metrics.register("connection");
        metrics.authenticated(
            "connection",
            &ControlConnection {
                connection_id: "connection".to_owned(),
                project_id: "project".to_owned(),
                identity: Some(SessionIdentity {
                    project_id: "project".to_owned(),
                    account: Account {
                        id: "account".to_owned(),
                        email: "user@example.test".to_owned(),
                        email_verified: true,
                        name: "User".to_owned(),
                        avatar_url: String::new(),
                        provider: "firebase".to_owned(),
                    },
                }),
                ..ControlConnection::default()
            },
            None,
        );
        metrics.subscriptions("connection", ["tasks.grid", "tasks.recent"]);
        assert_eq!(metrics.snapshot("project")["connections"], 1);
        assert_eq!(metrics.snapshot("project")["subscriptions"], 2);
        assert_eq!(metrics.snapshot("other")["connections"], 0);
        drop(guard);
        assert_eq!(metrics.snapshot("project")["connections"], 0);
    }
}
