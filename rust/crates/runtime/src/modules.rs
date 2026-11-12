//! Loaded TypeScript module contracts keyed by project.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use gonvex_module_host::protocol::{
    ActivateRequest, FunctionWire, JavaScriptWire, LoadRequest, ModuleArtifactWire,
    ReducerBundleWire, ResponsePayload,
};
use gonvex_store::RuntimeManifestRecord;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::RwLock;

use crate::module_host::{ModuleHost, ModuleHostError};

#[derive(Clone, Debug)]
pub struct FunctionDefinition {
    pub kind: String,
    pub internal: bool,
    pub public: bool,
    pub delivery: String,
    pub action_profile: String,
    pub action_capabilities: Value,
    pub replica: Option<ReplicaCollectionDefinition>,
    pub dependencies: Value,
    pub live_query_plan: Option<crate::live_query::LiveQueryPlan>,
    pub interactive: bool,
    pub classification: String,
    pub description: String,
    pub tags: Vec<String>,
    pub confirmation: String,
    pub args_schema: Value,
    pub result_schema: Value,
    /// Per-function profiler slot (`gonvex_profiler::function`), assigned
    /// once per (project, path) at module install.
    pub profile_slot: u16,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplicaCollectionDefinition {
    pub table: String,
    pub key: String,
    pub columns: Vec<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub equal_filters: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub exclude_when_set: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub visibility_tables: Vec<String>,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub visibility_plan_hash: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub order_by: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub order_direction: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mode: String,
    #[serde(default, skip_serializing_if = "is_zero_usize")]
    pub max_rows: usize,
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub max_bytes: i64,
    #[serde(default, rename = "retentionMs", skip_serializing_if = "is_zero_i64")]
    pub retention_ms: i64,
}

fn is_zero_usize(value: &usize) -> bool {
    *value == 0
}

fn is_zero_i64(value: &i64) -> bool {
    *value == 0
}

#[derive(Clone, Debug)]
pub struct ProjectModule {
    pub project_id: String,
    pub generation: u64,
    pub artifact_hash: String,
    pub client_contract: Option<u64>,
    pub functions: BTreeMap<String, FunctionDefinition>,
    pub manifest_functions: Value,
    pub(crate) replica_epochs: Arc<std::sync::Mutex<BTreeMap<String, String>>>,
    pub schema: Value,
    pub visibility: BTreeMap<String, crate::visibility::VisibilityPlan>,
    /// Optional `permissionGrants` projection backing `ctx.permissions`.
    pub permission_grants: Option<crate::permission_bits::PermissionGrantSource>,
    pub invitation_acceptance_reducer: String,
    pub migrations: Vec<gonvex_store::SqlMigration>,
    pub crons: Vec<CronSpec>,
}

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct CronSpec {
    pub name: String,
    pub function: String,
    #[serde(default)]
    pub args: Value,
    pub scope: String,
    #[serde(default)]
    pub interval_ms: Option<u64>,
    #[serde(default)]
    pub expression: Option<String>,
    /// Optional pre-check: one read-only SQL statement evaluated on a tenant
    /// reader before each run, with `$1` bound to the run's due time (Unix
    /// ms). The run enters V8 only when the first column of the first row is
    /// truthy (not NULL, 0, '' or absent). Errors fail open (the run happens).
    #[serde(default)]
    pub due_when: Option<String>,
    /// Profiler cron-table slot, assigned once at module install.
    #[serde(skip)]
    pub slot: u16,
}

#[derive(Default)]
pub struct ModuleRegistry {
    projects: RwLock<BTreeMap<String, Arc<ProjectModule>>>,
    failures: RwLock<BTreeMap<String, String>>,
    activation_gate: Arc<RwLock<()>>,
}

#[derive(Clone)]
pub struct ModuleCallLease {
    module: Arc<ProjectModule>,
    _activation: Arc<OwnedRwLockReadGuard<()>>,
}

impl ModuleCallLease {
    /// The leased module itself (shared, no copy).
    pub fn shared(&self) -> Arc<ProjectModule> {
        self.module.clone()
    }
}

impl std::ops::Deref for ModuleCallLease {
    type Target = ProjectModule;

    fn deref(&self) -> &Self::Target {
        &self.module
    }
}

#[derive(Debug, Error)]
pub enum ModuleRegistryError {
    #[error("project {project:?} has an invalid module artifact: {message}")]
    InvalidArtifact { project: String, message: String },
    #[error(transparent)]
    Host(#[from] ModuleHostError),
}

type ExtractedArtifact = (
    ModuleArtifactWire,
    BTreeMap<String, FunctionDefinition>,
    BTreeMap<String, crate::visibility::VisibilityPlan>,
);

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn install(
        &self,
        host: &ModuleHost,
        record: RuntimeManifestRecord,
    ) -> Result<Arc<ProjectModule>, ModuleRegistryError> {
        let validated = validate_manifest_record(&record)?;
        self.install_validated(host, &record, validated).await
    }

    /// Loads and activates a manifest that [`validate_manifest_record`]
    /// accepted. A push validates before it provisions tenants and installs
    /// that result instead of decoding and hashing the artifact again.
    pub async fn install_validated(
        &self,
        host: &ModuleHost,
        record: &RuntimeManifestRecord,
        validated: ValidatedManifest,
    ) -> Result<Arc<ProjectModule>, ModuleRegistryError> {
        let project = record.project_id.clone();
        let ValidatedManifest {
            migrations,
            client_contract,
            crons,
            permission_grants,
            artifact,
            functions,
            visibility,
        } = validated;
        let loaded = host
            .load(LoadRequest {
                module_id: project.clone(),
                generation: None,
                artifact,
            })
            .await?;
        let generation = match loaded {
            ResponsePayload::Loaded { generation, .. } => generation,
            _ => return Err(ModuleHostError::UnexpectedResponse.into()),
        };
        // Calls hold a read lease from manifest selection through module-host
        // completion. The write lease makes the host generation switch and
        // the signed runtime manifest one atomic dispatch boundary. Existing
        // calls finish on the old generation, and no new call can observe a
        // host/manifest mismatch in the short activation window.
        let _activation = self.activation_gate.write().await;
        match host
            .activate(ActivateRequest {
                module_id: project.clone(),
                generation,
                drain_ms: Some(30_000),
            })
            .await?
        {
            ResponsePayload::Activated { .. } => {}
            _ => return Err(ModuleHostError::UnexpectedResponse.into()),
        }
        let installed = Arc::new(ProjectModule {
            project_id: project.clone(),
            generation,
            artifact_hash: record.module_hash.clone(),
            client_contract,
            functions,
            replica_epochs: Default::default(),
            manifest_functions: record
                .manifest
                .get("functions")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
            schema: record
                .manifest
                .get("schema")
                .cloned()
                .unwrap_or_else(|| Value::Object(Map::new())),
            visibility,
            permission_grants,
            invitation_acceptance_reducer: record
                .manifest
                .get("module")
                .and_then(|module| module.get("invitationAcceptanceReducer"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .trim()
                .to_owned(),
            migrations,
            crons,
        });
        self.projects
            .write()
            .await
            .insert(project.clone(), installed.clone());
        self.failures.write().await.remove(&project);
        Ok(installed)
    }

    pub async fn record_failure(&self, project: &str, error: &ModuleRegistryError) {
        self.failures
            .write()
            .await
            .insert(project.to_owned(), error.to_string());
    }

    pub async fn project(&self, project: &str) -> Option<Arc<ProjectModule>> {
        self.projects.read().await.get(project).cloned()
    }

    pub async fn project_for_call(&self, project: &str) -> Option<ModuleCallLease> {
        let activation = Arc::clone(&self.activation_gate).read_owned().await;
        let module = self.projects.read().await.get(project).cloned()?;
        Some(ModuleCallLease {
            module,
            _activation: Arc::new(activation),
        })
    }

    pub async fn counts(&self) -> (usize, usize) {
        (
            self.projects.read().await.len(),
            self.failures.read().await.len(),
        )
    }

    /// Registers a module without loading it into a module host, for tests of
    /// HTTP surfaces that only read the installed manifest.
    #[cfg(test)]
    pub(crate) async fn insert_for_test(&self, module: ProjectModule) {
        self.projects
            .write()
            .await
            .insert(module.project_id.clone(), Arc::new(module));
    }
}

fn crons_from_module(
    module: &Map<String, Value>,
    project: &str,
) -> Result<Vec<CronSpec>, ModuleRegistryError> {
    let invalid = |message: String| ModuleRegistryError::InvalidArtifact {
        project: project.to_owned(),
        message,
    };
    let values = module
        .get("crons")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    let mut crons: Vec<CronSpec> = serde_json::from_value(values)
        .map_err(|error| invalid(format!("invalid crons: {error}")))?;
    let mut names = std::collections::BTreeSet::new();
    for cron in &crons {
        if cron.name.trim().is_empty() || cron.function.trim().is_empty() {
            return Err(invalid("cron name and function are required".to_owned()));
        }
        if !names.insert(cron.name.trim().to_owned()) {
            return Err(invalid(format!("duplicate cron name {:?}", cron.name)));
        }
        if !matches!(cron.scope.as_str(), "project" | "tenant") {
            return Err(invalid(format!("cron {:?} has invalid scope", cron.name)));
        }
        if cron.interval_ms.is_some() == cron.expression.is_some()
            || cron.interval_ms == Some(0)
            || cron
                .expression
                .as_deref()
                .is_some_and(|value| value.trim().is_empty())
        {
            return Err(invalid(format!(
                "cron {:?} requires exactly one positive intervalMs or expression",
                cron.name
            )));
        }
    }
    for cron in &mut crons {
        if let Some(statement) = cron.due_when.as_deref() {
            if statement.trim().is_empty() {
                cron.due_when = None;
            } else {
                crate::host_calls::admit_read_statement(statement)
                    .map_err(|error| invalid(format!("cron {:?} dueWhen: {error}", cron.name)))?;
            }
        }
        cron.slot = gonvex_profiler::cron::register(cron.name.trim());
    }
    Ok(crons)
}

/// Everything a manifest record yields once validated. Migrations are
/// decoded and checksummed here, once per artifact; tenants share them.
pub struct ValidatedManifest {
    pub(crate) migrations: Vec<gonvex_store::SqlMigration>,
    client_contract: Option<u64>,
    crons: Vec<CronSpec>,
    permission_grants: Option<crate::permission_bits::PermissionGrantSource>,
    artifact: ModuleArtifactWire,
    functions: BTreeMap<String, FunctionDefinition>,
    visibility: BTreeMap<String, crate::visibility::VisibilityPlan>,
}

pub(crate) fn validate_manifest_record(
    record: &RuntimeManifestRecord,
) -> Result<ValidatedManifest, ModuleRegistryError> {
    let project = record.project_id.clone();
    let module = record
        .manifest
        .get("module")
        .and_then(Value::as_object)
        .ok_or_else(|| ModuleRegistryError::InvalidArtifact {
            project: project.clone(),
            message: "manifest has no TypeScript module".to_owned(),
        })?;
    let migrations =
        migrations_from_module(module).map_err(|message| ModuleRegistryError::InvalidArtifact {
            project: project.clone(),
            message,
        })?;
    let client_contract = client_contract_from_module(module).map_err(|message| {
        ModuleRegistryError::InvalidArtifact {
            project: project.clone(),
            message,
        }
    })?;
    let crons = crons_from_module(module, &project)?;
    let permission_grants = module
        .get("permissionGrants")
        .filter(|value| !value.is_null())
        .map(|value| {
            serde_json::from_value::<crate::permission_bits::PermissionGrantSource>(value.clone())
                .map_err(|error| error.to_string())
                .and_then(|source| source.validate().map(|()| source))
        })
        .transpose()
        .map_err(|message| ModuleRegistryError::InvalidArtifact {
            project: project.clone(),
            message: format!("permissionGrants: {message}"),
        })?;
    let (artifact, functions, visibility) = artifact_from_manifest(record)?;
    for cron in &crons {
        let function =
            functions
                .get(&cron.function)
                .ok_or_else(|| ModuleRegistryError::InvalidArtifact {
                    project: project.clone(),
                    message: format!(
                        "cron {:?} targets unknown function {:?}",
                        cron.name, cron.function
                    ),
                })?;
        if !matches!(function.kind.as_str(), "reducer" | "action") {
            return Err(ModuleRegistryError::InvalidArtifact {
                project: project.clone(),
                message: format!("cron {:?} must target a Reducer or Action", cron.name),
            });
        }
    }
    Ok(ValidatedManifest {
        migrations,
        client_contract,
        crons,
        permission_grants,
        artifact,
        functions,
        visibility,
    })
}

/// Validates the exact standalone module object emitted by the TypeScript CLI
/// and returns its canonical hash. This powers release and migration tooling
/// without starting V8 or connecting to PostgreSQL.
pub fn verify_standalone_module_artifact(module: Value) -> Result<String, ModuleRegistryError> {
    let hash = module
        .get("hash")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim()
        .to_owned();
    let record = RuntimeManifestRecord {
        project_id: "artifact-verification".to_owned(),
        module_hash: hash.clone(),
        manifest: serde_json::json!({"module":module}),
    };
    validate_manifest_record(&record)?;
    Ok(hash)
}

fn migrations_from_module(
    module: &Map<String, Value>,
) -> Result<Vec<gonvex_store::SqlMigration>, String> {
    let Some(files) = module.get("files").and_then(Value::as_object) else {
        return Ok(Vec::new());
    };
    let mut migrations = Vec::new();
    for (path, encoded) in files {
        let file = Path::new(path);
        if file.parent() != Some(Path::new("migrations"))
            || file.extension().and_then(|value| value.to_str()) != Some("sql")
        {
            continue;
        }
        let name = file
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("invalid migration path {path:?}"))?;
        if name.len() < 10
            || !name.as_bytes()[..4].iter().all(u8::is_ascii_digit)
            || name.as_bytes().get(4) != Some(&b'_')
            || !name.ends_with(".sql")
            || !name[5..name.len() - 4]
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        {
            return Err(format!(
                "migration {name:?} must be named NNNN_description.sql"
            ));
        }
        let encoded = encoded
            .as_str()
            .ok_or_else(|| format!("migration {name} must be base64 text"))?;
        let source = String::from_utf8(
            STANDARD
                .decode(encoded)
                .map_err(|error| format!("decode migration {name}: {error}"))?,
        )
        .map_err(|_| format!("migration {name} must be UTF-8"))?;
        let mut scope = None;
        let mut no_transaction = false;
        for line in source.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Some(directive) = line.strip_prefix("--") else {
                break;
            };
            let directive = directive.trim();
            if let Some(value) = directive.strip_prefix("gonvex:scope") {
                scope = Some(match value.trim() {
                    "tenant" => gonvex_store::MigrationScope::Tenant,
                    "control-plane" => gonvex_store::MigrationScope::ControlPlane,
                    value => return Err(format!("migration {name}: invalid scope {value:?}")),
                });
            } else if directive == "gonvex:no-transaction" {
                no_transaction = true;
            }
        }
        let scope = scope.ok_or_else(|| {
            format!(
                "migration {name}: missing required -- gonvex:scope tenant|control-plane directive"
            )
        })?;
        migrations.push(gonvex_store::SqlMigration::new(
            name.to_owned(),
            scope,
            no_transaction,
            source,
        ));
    }
    migrations.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(migrations)
}

impl ProjectModule {
    /// Application contract compatibility is independent of the compiled artifact bytes.
    pub fn accepts_client_artifact(&self, expected: &str, contract: Option<u64>) -> bool {
        expected == self.artifact_hash
            || (self.client_contract.is_some() && contract == self.client_contract)
    }
    pub fn replica_directive(
        &self,
        tenant_id: &str,
        database_url: &str,
        account_id: &str,
        permissions: &Value,
    ) -> gonvex_protocol::ReplicaDirective {
        // A deployed module is immutable. Hash its full schema and function
        // manifest once per database, not once per subscription on every open.
        // Keep the exact existing epoch bytes so persisted replicas can resume.
        let epoch = {
            let mut epochs = self
                .replica_epochs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if let Some(epoch) = epochs.get(database_url) {
                epoch.clone()
            } else {
                let epoch = hash_json(&serde_json::json!({
                    "protocolVersion": 1,
                    "project": self.project_id,
                    "database": database_url,
                    "functions": self.manifest_functions,
                    "schema": self.schema,
                    "moduleHash": self.artifact_hash,
                }));
                if epochs.len() >= 1024 {
                    epochs.clear();
                }
                epochs.insert(database_url.to_owned(), epoch.clone());
                epoch
            }
        };
        let permissions_hash = hash_json(permissions);
        let account_id = if account_id.trim().is_empty() {
            "anonymous"
        } else {
            account_id
        };
        let scope = hash_json(&serde_json::json!({
            "protocolVersion": 1,
            "projectId": self.project_id,
            "tenantId": tenant_id,
            "accountId": account_id,
            "permissionsHash": permissions_hash,
            "epoch": epoch,
        }));
        let visibility_scope = hash_json(&serde_json::json!({
            "protocolVersion": 1,
            "kind": "replica-visibility",
            "projectId": self.project_id,
            "tenantId": tenant_id,
            "accountId": account_id,
            "permissionsHash": permissions_hash,
        }));
        gonvex_protocol::ReplicaDirective {
            protocol_version: 1,
            scope,
            visibility_scope,
            epoch,
        }
    }
}

fn hash_json(value: &Value) -> String {
    let digest = Sha256::digest(serde_json::to_vec(value).unwrap_or_default());
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn artifact_from_manifest(
    record: &RuntimeManifestRecord,
) -> Result<ExtractedArtifact, ModuleRegistryError> {
    let invalid = |message: String| ModuleRegistryError::InvalidArtifact {
        project: record.project_id.clone(),
        message,
    };
    let module = record
        .manifest
        .get("module")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("manifest has no TypeScript module".to_owned()))?;
    let hash = text(module, "hash")?;
    if hash != record.module_hash {
        return Err(invalid(format!(
            "stored module hash {:?} does not match artifact hash {:?}",
            record.module_hash, hash
        )));
    }
    let canonical_hash = canonical_artifact_hash(module)
        .map_err(|message| invalid(format!("canonical artifact hash: {message}")))?;
    if hash != canonical_hash {
        return Err(invalid(format!(
            "module artifact hash {hash:?} does not match canonical contract hash {canonical_hash:?}"
        )));
    }
    let javascript: JavaScriptWire = serde_json::from_value(
        module
            .get("javascript")
            .cloned()
            .ok_or_else(|| invalid("artifact has no JavaScript bundle".to_owned()))?,
    )
    .map_err(|error| invalid(error.to_string()))?;
    let reducer_bundle: Option<ReducerBundleWire> = module
        .get("reducerBundle")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| invalid(format!("reducerBundle: {error}")))?;
    let function_values = module
        .get("functions")
        .and_then(Value::as_object)
        .ok_or_else(|| invalid("artifact functions must be an object".to_owned()))?;
    let mut function_wires = Vec::with_capacity(function_values.len());
    let mut definitions = BTreeMap::new();
    for (path, value) in function_values {
        let function = value
            .as_object()
            .ok_or_else(|| invalid(format!("function {path:?} must be an object")))?;
        let kind = function
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        if !matches!(kind.as_str(), "query" | "reducer" | "action") {
            return Err(invalid(format!(
                "function {path:?} has unknown kind {kind:?}"
            )));
        }
        let internal = function
            .get("internal")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| invalid(format!("function {path:?} internal must be a boolean")))
            })
            .transpose()?
            .unwrap_or(false);
        let public = function
            .get("public")
            .map(|value| {
                value
                    .as_bool()
                    .ok_or_else(|| invalid(format!("function {path:?} public must be a boolean")))
            })
            .transpose()?
            .unwrap_or(false);
        if public
            && (internal
                || (kind == "query"
                    && (function.contains_key("replica")
                        || function
                            .get("delivery")
                            .and_then(Value::as_str)
                            .unwrap_or("oneShot")
                            != "oneShot"))
                || (kind == "reducer"
                    && (function.get("interactive").and_then(Value::as_bool) != Some(false)
                        || function
                            .get("offline")
                            .and_then(|v| v.get("mode"))
                            .and_then(Value::as_str)
                            == Some("allowed")
                        || function.contains_key("localExecution")
                        || function.contains_key("optimistic"))))
        {
            return Err(invalid(format!(
                "public function {path:?} must be non-internal, one-shot and server-only"
            )));
        }
        let declared_interactive = function
            .get("interactive")
            .map(|value| {
                value.as_bool().ok_or_else(|| {
                    invalid(format!("function {path:?} interactive must be a boolean"))
                })
            })
            .transpose()?;
        let declared_classification = function
            .get("classification")
            .map(|value| {
                value.as_str().ok_or_else(|| {
                    invalid(format!("function {path:?} classification must be a string"))
                })
            })
            .transpose()?;
        let interactive = match (declared_interactive, declared_classification) {
            (Some(value), _) => value,
            (None, Some("interactive")) => true,
            (None, Some("system" | "internal")) => false,
            (None, Some(value)) => {
                return Err(invalid(format!(
                    "function {path:?} has unknown classification {value:?}"
                )))
            }
            (None, None) => !internal && kind != "action",
        };
        let expected_classification = if internal {
            "internal"
        } else if interactive {
            "interactive"
        } else {
            "system"
        };
        let classification = declared_classification.unwrap_or(expected_classification);
        if classification != expected_classification {
            return Err(invalid(format!(
                "function {path:?} classification does not match its internal and interactive flags"
            )));
        }
        let description = function
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let agent = function.get("agent").and_then(Value::as_object);
        let mut tags = agent
            .and_then(|value| value.get("tags"))
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        tags.sort();
        tags.dedup();
        let confirmation = agent
            .and_then(|value| value.get("confirmation"))
            .and_then(Value::as_str)
            .unwrap_or("none")
            .to_owned();
        if !matches!(confirmation.as_str(), "none" | "required" | "destructive") {
            return Err(invalid(format!(
                "function {path:?} has an invalid agent confirmation"
            )));
        }
        let delivery = function
            .get("delivery")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        let mut metadata = Map::new();
        for name in [
            "public",
            "actionProfile",
            "offline",
            "optimistic",
            "interactive",
            "classification",
            "description",
            "agent",
            "actionCapabilities",
        ] {
            if let Some(value) = function.get(name) {
                metadata.insert(name.to_owned(), value.clone());
            }
        }
        function_wires.push(FunctionWire {
            path: path.clone(),
            kind: kind.clone(),
            internal,
            delivery: nonempty(&delivery),
            handler: optional_text(function, "handler"),
            export: optional_text(function, "export"),
            file: optional_text(function, "file"),
            args: function.get("args").cloned(),
            result: function.get("result").cloned(),
            metadata,
        });
        definitions.insert(
            path.clone(),
            FunctionDefinition {
                public,
                kind,
                internal,
                delivery,
                action_profile: function
                    .get("actionProfile")
                    .and_then(Value::as_str)
                    .unwrap_or("standard")
                    .to_owned(),
                action_capabilities: function
                    .get("actionCapabilities")
                    .cloned()
                    .unwrap_or(Value::Null),
                replica: function
                    .get("replica")
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|error| invalid(format!("function {path:?} replica: {error}")))?,
                dependencies: function.get("dependencies").cloned().unwrap_or(Value::Null),
                live_query_plan: function
                    .get("dependencies")
                    .and_then(|value| value.get("liveQueryPlan"))
                    .cloned()
                    .map(serde_json::from_value)
                    .transpose()
                    .map_err(|error| {
                        invalid(format!("function {path:?} live query plan: {error}"))
                    })?,
                interactive,
                classification: classification.to_owned(),
                description,
                tags,
                confirmation,
                args_schema: function
                    .get("args")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"kind":"object","fields":{}})),
                result_schema: function
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| serde_json::json!({"kind":"any"})),
                profile_slot: gonvex_profiler::function::register(&record.project_id, path),
            },
        );
    }
    function_wires.sort_by(|left, right| left.path.cmp(&right.path));
    if let Some(bundle) = &reducer_bundle {
        for path in &bundle.functions {
            match definitions.get(path) {
                Some(definition) if matches!(definition.kind.as_str(), "query" | "reducer") => {}
                Some(_) => {
                    return Err(invalid(format!(
                        "reducerBundle lists {path:?}, which is not a Query or Reducer"
                    )))
                }
                None => {
                    return Err(invalid(format!(
                        "reducerBundle lists unknown function {path:?}"
                    )))
                }
            }
        }
    }
    let visibility: BTreeMap<String, crate::visibility::VisibilityPlan> = module
        .get("visibility")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| invalid(format!("visibility plans: {error}")))?
        .unwrap_or_default();
    crate::visibility::validate_manifest(&visibility)
        .map_err(|error| invalid(error.to_string()))?;
    let legacy = visibility
        .values()
        .filter(|plan| plan.version != Some(2))
        .count();
    if legacy > 0 {
        tracing::warn!(legacy_plans = legacy, "DEPRECATED legacy visibility manifest: data-table dependencies can cause SQL fan-out; migrate to version 2 masks before legacy rejection is enabled");
    }
    for (table, plan) in &visibility {
        if &plan.table != table {
            return Err(invalid(format!(
                "visibility plan key {table:?} does not match table {:?}",
                plan.table
            )));
        }
        plan.validate()
            .map_err(|error| invalid(error.to_string()))?;
    }
    for (path, definition) in &definitions {
        if let Some(replica) = &definition.replica {
            if definition.kind != "query" || definition.delivery != "replica" {
                return Err(invalid(format!(
                    "function {path:?} declares Replica metadata but is not a replica Query"
                )));
            }
            if replica.retention_ms < 0 || replica.retention_ms > 30 * 24 * 60 * 60 * 1_000 {
                return Err(invalid(format!(
                    "function {path:?} retentionMs must be between 0 and 2592000000"
                )));
            }
            if replica.max_rows > 100_000 || replica.max_bytes > 256 << 20 {
                return Err(invalid(format!(
                    "function {path:?} Replica budget exceeds the runtime maximum"
                )));
            }
        }
        if let Some(plan) = &definition.live_query_plan {
            plan.validate()
                .map_err(|error| invalid(format!("function {path:?}: {error}")))?;
        }
    }
    Ok((
        ModuleArtifactWire {
            language: module
                .get("language")
                .and_then(Value::as_str)
                .unwrap_or("typescript")
                .to_owned(),
            entrypoint: text(module, "entrypoint")?,
            hash,
            javascript,
            reducer_bundle,
            functions: function_wires,
            metadata: Map::new(),
        },
        definitions,
        visibility,
    ))
}

#[doc(hidden)]
pub fn canonical_artifact_hash(module: &Map<String, Value>) -> Result<String, String> {
    let javascript = module
        .get("javascript")
        .and_then(Value::as_object)
        .ok_or_else(|| "artifact has no JavaScript bundle".to_owned())?;
    // The signed contract is streamed into the hash as canonical JSON. It
    // borrows the module's values: building it as one JSON value copied every
    // file (migration history included) and then its text, twice per push.
    let present = |key: &str, default: Value| {
        module
            .get(key)
            .map_or(std::borrow::Cow::Owned(default), std::borrow::Cow::Borrowed)
    };
    let mut contract = BTreeMap::<&str, std::borrow::Cow<'_, Value>>::new();
    contract.insert("generation", present("generation", Value::Null));
    contract.insert(
        "language",
        present("language", Value::String("typescript".to_owned())),
    );
    contract.insert("entrypoint", present("entrypoint", Value::Null));
    contract.insert("files", present("files", serde_json::json!({})));
    contract.insert("functions", present("functions", serde_json::json!({})));
    contract.insert("visibility", present("visibility", serde_json::json!({})));
    contract.insert("crons", present("crons", serde_json::json!([])));
    contract.insert(
        "invitationAcceptanceReducer",
        present("invitationAcceptanceReducer", Value::String(String::new())),
    );
    contract.insert(
        "javascript",
        std::borrow::Cow::Owned(serde_json::json!({
            "path":javascript.get("path").cloned().unwrap_or(Value::Null),
            "hash":javascript.get("hash").and_then(Value::as_str).unwrap_or("").trim().to_ascii_lowercase(),
        })),
    );
    // A split artifact signs its reducer bundle too. Unsplit artifacts keep
    // their existing hash because the key is absent.
    if let Some(bundle) = module.get("reducerBundle") {
        let bundle = bundle
            .as_object()
            .ok_or_else(|| "reducerBundle must be an object".to_owned())?;
        contract.insert(
            "reducerBundle",
            std::borrow::Cow::Owned(serde_json::json!({
                "path": bundle.get("path").cloned().unwrap_or(Value::Null),
                "hash": bundle.get("hash").and_then(Value::as_str).unwrap_or("").trim().to_ascii_lowercase(),
                "functions": bundle.get("functions").cloned().unwrap_or_else(|| serde_json::json!([])),
            })),
        );
    }
    // Declared only by modules that use `ctx.permissions`; absent keeps every
    // earlier artifact hash unchanged.
    if let Some(grants) = module
        .get("permissionGrants")
        .filter(|value| !value.is_null())
    {
        contract.insert("permissionGrants", std::borrow::Cow::Borrowed(grants));
    }
    let mut hasher = Sha256::new();
    write_canonical_entries(
        contract.iter().map(|(key, value)| (*key, value.as_ref())),
        &mut hasher,
    )
    .map_err(|error| error.to_string())?;
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

/// Canonical JSON: no whitespace, object keys in byte order, strings and
/// numbers as serde_json writes them.
fn write_canonical(value: &Value, out: &mut impl std::io::Write) -> std::io::Result<()> {
    match value {
        Value::Null => out.write_all(b"null"),
        Value::Bool(value) => out.write_all(if *value { b"true" } else { b"false" }),
        Value::Number(value) => write!(out, "{value}"),
        Value::String(value) => serde_json::to_writer(out, value).map_err(std::io::Error::from),
        Value::Array(values) => {
            out.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    out.write_all(b",")?;
                }
                write_canonical(value, out)?;
            }
            out.write_all(b"]")
        }
        Value::Object(object) => {
            let mut entries = object
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            write_canonical_entries(entries.into_iter(), out)
        }
    }
}

/// Writes `{key:value,...}` for entries already in canonical key order.
fn write_canonical_entries<'a>(
    entries: impl Iterator<Item = (&'a str, &'a Value)>,
    out: &mut impl std::io::Write,
) -> std::io::Result<()> {
    out.write_all(b"{")?;
    for (index, (key, value)) in entries.enumerate() {
        if index > 0 {
            out.write_all(b",")?;
        }
        serde_json::to_writer(&mut *out, key).map_err(std::io::Error::from)?;
        out.write_all(b":")?;
        write_canonical(value, out)?;
    }
    out.write_all(b"}")
}

fn text(object: &Map<String, Value>, key: &str) -> Result<String, ModuleRegistryError> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| ModuleRegistryError::InvalidArtifact {
            project: "unknown".to_owned(),
            message: format!("artifact {key} is required"),
        })
}

fn optional_text(object: &Map<String, Value>, key: &str) -> Option<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn nonempty(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoding before it was streamed, kept as the reference: artifact
    /// hashes signed by earlier runtimes and CLIs must not change.
    fn reference_canonical_json(value: &Value) -> String {
        match value {
            Value::Array(values) => format!(
                "[{}]",
                values
                    .iter()
                    .map(reference_canonical_json)
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            Value::Object(object) => {
                let mut entries = object.iter().collect::<Vec<_>>();
                entries.sort_by(|(left, _), (right, _)| left.cmp(right));
                let entries = entries
                    .into_iter()
                    .map(|(key, value)| {
                        format!(
                            "{}:{}",
                            serde_json::to_string(key).unwrap(),
                            reference_canonical_json(value)
                        )
                    })
                    .collect::<Vec<_>>();
                format!("{{{}}}", entries.join(","))
            }
            Value::String(value) => serde_json::to_string(value).unwrap(),
            other => other.to_string(),
        }
    }

    #[test]
    fn streamed_canonical_hash_matches_the_reference_encoding() {
        let value = serde_json::json!({
            "b": [1, 2.5, -3, 1e300, u64::MAX, true, false, null, "\u{e9}\u{1}\"\\\n\u{2028}"],
            "a": {"z": {}, "y": [], "\u{e9}": 1, "A": 2, "": "empty"},
            "files": {"migrations/0001_x.sql": "LS0gZ29udmV4OnNjb3BlIHRlbmFudA=="},
        });
        let mut streamed = Vec::new();
        write_canonical(&value, &mut streamed).unwrap();
        assert_eq!(
            String::from_utf8(streamed).unwrap(),
            reference_canonical_json(&value)
        );
        let module = serde_json::json!({
            "generation": 2,
            "entrypoint": "index.ts",
            "files": value["files"].clone(),
            "functions": {"tasks:list": {"kind": "query"}},
            "javascript": {"path": "module.js", "hash": " ABCD ", "source": "ignored"},
            "reducerBundle": {"path": "r.js", "hash": "EF", "functions": ["tasks:save"]},
            "permissionGrants": {"grants": []},
        });
        let contract = serde_json::json!({
            "generation": 2,
            "language": "typescript",
            "entrypoint": "index.ts",
            "files": value["files"].clone(),
            "functions": {"tasks:list": {"kind": "query"}},
            "visibility": {},
            "crons": [],
            "invitationAcceptanceReducer": "",
            "javascript": {"path": "module.js", "hash": "abcd"},
            "reducerBundle": {"path": "r.js", "hash": "ef", "functions": ["tasks:save"]},
            "permissionGrants": {"grants": []},
        });
        let expected: String = Sha256::digest(reference_canonical_json(&contract).as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        assert_eq!(
            canonical_artifact_hash(module.as_object().unwrap()).unwrap(),
            expected
        );
    }

    #[test]
    fn cached_replica_epochs_preserve_hashes_and_tenant_identity() {
        let module = ProjectModule {
            project_id: "project".into(),
            generation: 1,
            artifact_hash: "artifact".into(),
            client_contract: None,
            functions: BTreeMap::new(),
            manifest_functions: serde_json::json!({"tasks": {"kind": "reducer"}}),
            schema: serde_json::json!({"tasks": {"id": "text"}}),
            replica_epochs: Default::default(),
            visibility: BTreeMap::new(),
            permission_grants: None,
            invitation_acceptance_reducer: String::new(),
            migrations: Vec::new(),
            crons: Vec::new(),
        };
        let expected = hash_json(&serde_json::json!({
            "protocolVersion": 1, "project": module.project_id, "database": "database-a",
            "functions": module.manifest_functions, "schema": module.schema, "moduleHash": module.artifact_hash,
        }));
        let first = module.replica_directive("tenant-a", "database-a", "account-a", &Value::Null);
        assert_eq!(first.epoch, expected);
        for _ in 0..300 {
            assert_eq!(
                module
                    .replica_directive("tenant-a", "database-a", "account-a", &Value::Null)
                    .epoch,
                expected
            );
        }
        assert_eq!(module.replica_epochs.lock().unwrap().len(), 1);
        let other_account =
            module.replica_directive("tenant-a", "database-a", "account-b", &Value::Null);
        assert_eq!(first.epoch, other_account.epoch);
        assert_ne!(first.scope, other_account.scope);
        let other_database =
            module.replica_directive("tenant-b", "database-b", "account-a", &Value::Null);
        assert_ne!(first.epoch, other_database.epoch);
        assert_eq!(module.replica_epochs.lock().unwrap().len(), 2);
    }

    #[test]
    fn extracts_stored_typescript_artifact_without_rewriting_nulls() {
        let mut manifest = serde_json::json!({
            "module": {
                "generation":8,
                "language": "typescript",
                "hash": "pending",
                "entrypoint": "gonvex/index.ts",
                "files":{},
                "visibility":{},
                "crons":[],
                "invitationAcceptanceReducer":"",
                "javascript": {"path":"gonvex/_build/module.js","hash":"abc","code":"eA=="},
                "functions": {
                    "tasks.find": {
                        "kind": "query",
                        "handler": "tasks_find",
                        "file":"gonvex/index.ts",
                        "args": {"kind":"literal","value":{"literal":null}}
                    }
                }
            }
        });
        let canonical =
            canonical_artifact_hash(manifest["module"].as_object().expect("module")).unwrap();
        manifest["module"]["hash"] = Value::String(canonical.clone());
        let record = RuntimeManifestRecord {
            project_id: "project-a".to_owned(),
            module_hash: canonical,
            manifest,
        };
        let (artifact, functions, _) = artifact_from_manifest(&record).unwrap();
        assert_eq!(
            artifact.functions[0].args.as_ref().unwrap()["value"]["literal"],
            Value::Null
        );
        assert_eq!(functions["tasks.find"].kind, "query");
        // The per-function profiler slot is assigned at install, per project.
        let slot = functions["tasks.find"].profile_slot;
        assert_ne!(slot, gonvex_profiler::function::NONE);
        assert_eq!(
            gonvex_profiler::function::name(slot).unwrap(),
            ("project-a".to_owned(), "tasks.find".to_owned())
        );
        let (_, again, _) = artifact_from_manifest(&record).unwrap();
        assert_eq!(again["tasks.find"].profile_slot, slot);
    }

    #[test]
    fn public_manifest_admission_is_explicit_and_server_only() {
        for entry in [
            serde_json::json!({"kind":"query","public":true,"internal":true}),
            serde_json::json!({"kind":"query","public":true,"delivery":"live"}),
            serde_json::json!({"kind":"query","public":true,"delivery":"replica"}),
            serde_json::json!({"kind":"reducer","public":true}),
            serde_json::json!({"kind":"reducer","public":true,"interactive":false,"localExecution":1}),
            serde_json::json!({"kind":"reducer","public":true,"interactive":false,"offline":{"mode":"allowed"}}),
            serde_json::json!({"kind":"action","public":"true"}),
        ] {
            let mut module = serde_json::json!({"generation":1,"language":"typescript","entrypoint":"test.js","files":{},"functions":{"test":entry},"javascript":{"path":"test.js","hash":"abc","code":"eA=="}});
            let hash = canonical_artifact_hash(module.as_object().unwrap()).unwrap();
            module["hash"] = serde_json::json!(hash);
            let record = RuntimeManifestRecord {
                project_id: "project".into(),
                module_hash: hash,
                manifest: serde_json::json!({"module":module}),
            };
            assert!(artifact_from_manifest(&record).is_err());
        }
        let mut module = serde_json::json!({"generation":1,"language":"typescript","entrypoint":"test.js","files":{},"functions":{"test":{"kind":"reducer","public":true,"interactive":false,"classification":"system"}},"javascript":{"path":"test.js","hash":"abc","code":"eA=="}});
        let hash = canonical_artifact_hash(module.as_object().unwrap()).unwrap();
        module["hash"] = serde_json::json!(hash);
        let record = RuntimeManifestRecord {
            project_id: "project".into(),
            module_hash: hash,
            manifest: serde_json::json!({"module":module}),
        };
        let (artifact, functions, _) = artifact_from_manifest(&record).unwrap();
        assert!(functions["test"].public);
        assert_eq!(
            artifact.functions[0].metadata["public"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn preserves_signed_interactive_metadata_and_portable_schemas() {
        let mut module = serde_json::json!({
            "generation":8,
            "language":"typescript",
            "hash":"pending",
            "entrypoint":"gonvex/index.ts",
            "files":{},"visibility":{},"crons":[],"invitationAcceptanceReducer":"",
            "javascript":{"path":"gonvex/_build/module.js","hash":"abc","code":"eA=="},
            "functions":{
                "tasks.start":{
                    "kind":"reducer","handler":"start","file":"gonvex/tasks.ts",
                    "interactive":true,"classification":"interactive",
                    "description":"Start a task",
                    "agent":{"tags":["tasks","workflow"],"confirmation":"required"},
                    "args":{"kind":"object","fields":{"taskId":{"kind":"id","entity":"tasks"}}},
                    "result":{"kind":"object","fields":{"ok":{"kind":"boolean"}}}
                },
                "callbacks.receive":{
                    "kind":"action","handler":"receive","file":"gonvex/callbacks.ts","actionProfile":"agent",
                    "actionCapabilities":{"networkOrigins":["https://example.com"]},
                    "classification":"system",
                    "args":{"kind":"object","fields":{}},"result":{"kind":"any"}
                },
                "advancedVoice.getCheckpointCapability":{
                    "kind":"query","handler":"get_checkpoint","file":"gonvex/advancedVoice.ts",
                    "classification":"system",
                    "args":{"kind":"object","fields":{}},"result":{"kind":"any"}
                },
                "advancedVoice.clearCheckpointCapability":{
                    "kind":"reducer","handler":"clear_checkpoint","file":"gonvex/advancedVoice.ts",
                    "classification":"system",
                    "args":{"kind":"object","fields":{}},"result":{"kind":"any"}
                }
            }
        });
        let canonical = canonical_artifact_hash(module.as_object().unwrap()).unwrap();
        module["hash"] = Value::String(canonical.clone());
        let record = RuntimeManifestRecord {
            project_id: "project-a".to_owned(),
            module_hash: canonical,
            manifest: serde_json::json!({"module":module}),
        };
        let (artifact, functions, _) = artifact_from_manifest(&record).unwrap();
        assert_eq!(functions["callbacks.receive"].action_profile, "agent");
        assert_eq!(
            artifact
                .functions
                .iter()
                .find(|f| f.path == "callbacks.receive")
                .unwrap()
                .metadata["actionCapabilities"]["networkOrigins"],
            serde_json::json!(["https://example.com"])
        );
        assert_eq!(
            artifact
                .functions
                .iter()
                .find(|f| f.path == "callbacks.receive")
                .unwrap()
                .metadata["actionProfile"],
            "agent"
        );
        let start = &functions["tasks.start"];
        assert!(start.interactive);
        assert_eq!(start.classification, "interactive");
        assert_eq!(start.description, "Start a task");
        assert_eq!(start.tags, vec!["tasks", "workflow"]);
        assert_eq!(start.confirmation, "required");
        assert_eq!(start.args_schema["fields"]["taskId"]["entity"], "tasks");
        assert!(!functions["callbacks.receive"].interactive);
        assert!(!functions["advancedVoice.getCheckpointCapability"].interactive);
        assert_eq!(
            functions["advancedVoice.getCheckpointCapability"].classification,
            "system"
        );
        assert!(!functions["advancedVoice.clearCheckpointCapability"].interactive);
        assert_eq!(
            functions["advancedVoice.clearCheckpointCapability"].classification,
            "system"
        );
    }
}

fn client_contract_from_module(module: &Map<String, Value>) -> Result<Option<u64>, String> {
    let Some(encoded) = module
        .get("files")
        .and_then(|f| f.get("client-contract.json"))
    else {
        return Ok(None);
    };
    let bytes = STANDARD
        .decode(encoded.as_str().ok_or("invalid client contract encoding")?)
        .map_err(|e| e.to_string())?;
    let contract: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let version = contract
        .get("version")
        .and_then(Value::as_u64)
        .filter(|v| *v > 0)
        .ok_or("invalid client contract version")?;
    if contract.get("offlineMaxAgeMs") != Some(&Value::Null)
        && contract
            .get("offlineMaxAgeMs")
            .and_then(Value::as_u64)
            .filter(|v| *v > 0)
            .is_none()
    {
        return Err("invalid offline window".into());
    }
    Ok(Some(version))
}

#[cfg(test)]
mod offline_contract_tests {
    use super::*;
    #[test]
    fn unlimited_offline_policy_is_valid_but_missing_or_invalid_window_is_not() {
        for (window, valid) in [
            (Value::Null, true),
            (serde_json::json!(604800000), true),
            (serde_json::json!(0), false),
            (serde_json::json!(-1), false),
            (serde_json::json!("forever"), false),
        ] {
            let policy = serde_json::json!({"version": 1, "offlineMaxAgeMs": window});
            let module = serde_json::json!({"files": {"client-contract.json": STANDARD.encode(serde_json::to_vec(&policy).unwrap())}});
            assert_eq!(
                client_contract_from_module(module.as_object().unwrap()).is_ok(),
                valid
            );
        }
        let policy = serde_json::json!({"version": 1});
        let module = serde_json::json!({"files": {"client-contract.json": STANDARD.encode(serde_json::to_vec(&policy).unwrap())}});
        assert!(client_contract_from_module(module.as_object().unwrap()).is_err());
    }
}

#[cfg(test)]
#[path = "visibility_deprecation_tests.rs"]
mod visibility_deprecation_tests;
