//! Read-path-only module fixture over an owned copy of the synthetic lab DBs.
use super::*;

pub struct OfflineFixture {
    pub runtime: Runtime,
    _host: FakeModuleHost,
}

impl OfflineFixture {
    /// The caller copies the lab baseline and rewrites its stored routing
    /// URLs first. The marker prevents accidentally opening the shared source.
    pub async fn start(directory: &Path) -> FixtureResult<Self> {
        if !directory.join(".open-cost-owned").is_file() {
            return Err("offline fixture requires an owned synthetic copy".into());
        }
        let host =
            FakeModuleHost::start(&directory.join("host.sock"), fake_host::whagons_functions())?;
        let mut overrides: BTreeMap<String, String> = std::env::vars()
            .filter(|(name, _)| name.starts_with("GONVEX_"))
            .collect();
        overrides.insert("GONVEX_DATA_DIR".into(), directory.display().to_string());
        overrides.insert("GONVEX_MODULE_HOST_ENABLED".into(), "true".into());
        overrides.insert("GONVEX_MODULE_HOST_ENDPOINT".into(), host.endpoint());
        overrides.insert("GONVEX_MODULE_HOST_IN_PROCESS".into(), "false".into());
        overrides.remove("GONVEX_MODULE_HOST_BINARY");
        let runtime = Runtime::new(Config::perf_from_lookup(|name| {
            overrides.get(name).cloned()
        })?);
        runtime.start().await?;
        Ok(Self {
            runtime,
            _host: host,
        })
    }

    pub async fn session(
        &self,
        project: &str,
        tenant: &str,
        account: &str,
    ) -> FixtureResult<TenantSession> {
        let control = self
            .runtime
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or("no control plane")?;
        Ok(control
            .tenant_session_for_account(project, tenant, account)
            .await?)
    }

    /// Production legacy page iterator, JSON encoding and no-context deflate.
    /// No socket/auth/send wait is included; bytes are immediately discarded.
    pub fn encode_wire(messages: Vec<ServerMessage>) -> (usize, usize) {
        let mut json_bytes = 0;
        let mut wire_bytes = 0;
        let mut encode = |message: ServerMessage| {
            let bytes = serde_json::to_vec(&message).unwrap();
            json_bytes += bytes.len();
            wire_bytes += gonvex_websocket::perf_compress_text(&bytes, 12, 1024)
                .unwrap()
                .map_or(bytes.len(), |compressed| compressed.len());
        };
        for message in messages {
            if matches!(message, ServerMessage::ReplicaSnapshot { .. }) {
                for page in crate::replica::SnapshotPages::new(message) {
                    encode(page);
                }
            } else {
                encode(message);
            }
        }
        (json_bytes, wire_bytes)
    }

    pub async fn shutdown(self) {
        self.runtime.shutdown().await;
    }
}
