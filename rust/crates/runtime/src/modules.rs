//! Loaded TypeScript module contracts keyed by project.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use gonvex_module_host::protocol::{
    ActivateRequest, FunctionWire, JavaScriptWire, LoadRequest, ModuleArtifactWire, ResponsePayload,
};
use gonvex_postgres::RuntimeManifestRecord;
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
    pub schema: Value,
    pub visibility: BTreeMap<String, crate::visibility::VisibilityPlan>,
    pub invitation_acceptance_reducer: String,
    pub migrations: Vec<gonvex_postgres::SqlMigration>,
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
        let project = record.project_id.clone();
        let module = record
            .manifest
            .get("module")
            .and_then(Value::as_object)
            .ok_or_else(|| ModuleRegistryError::InvalidArtifact {
                project: project.clone(),
                message: "manifest has no TypeScript module".to_owned(),
            })?;
        let migrations = migrations_from_module(module).map_err(|message| {
            ModuleRegistryError::InvalidArtifact {
                project: project.clone(),
                message,
            }
        })?;
        let client_contract = client_contract_from_module(module).map_err(|message| ModuleRegistryError::InvalidArtifact { project: project.clone(), message })?;
        let crons = crons_from_module(module, &project)?;
        let (artifact, functions, visibility) = artifact_from_manifest(&record)?;
        for cron in &crons {
            let function = functions.get(&cron.function).ok_or_else(|| {
                ModuleRegistryError::InvalidArtifact {
                    project: project.clone(),
                    message: format!(
                        "cron {:?} targets unknown function {:?}",
                        cron.name, cron.function
                    ),
                }
            })?;
            if !matches!(function.kind.as_str(), "reducer" | "action") {
                return Err(ModuleRegistryError::InvalidArtifact {
                    project: project.clone(),
                    message: format!("cron {:?} must target a Reducer or Action", cron.name),
                });
            }
        }
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
            artifact_hash: record.module_hash,
            client_contract,
            functions,
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
    let crons: Vec<CronSpec> = serde_json::from_value(values)
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
    Ok(crons)
}

pub(crate) fn validate_manifest_record(
    record: &RuntimeManifestRecord,
) -> Result<Vec<gonvex_postgres::SqlMigration>, ModuleRegistryError> {
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
    artifact_from_manifest(record)?;
    Ok(migrations)
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
) -> Result<Vec<gonvex_postgres::SqlMigration>, String> {
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
                    "tenant" => gonvex_postgres::MigrationScope::Tenant,
                    "control-plane" => gonvex_postgres::MigrationScope::ControlPlane,
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
        migrations.push(gonvex_postgres::SqlMigration::new(
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
        let epoch = hash_json(&serde_json::json!({
            "protocolVersion": 1,
            "project": self.project_id,
            "database": database_url,
            "functions": self.manifest_functions,
            "schema": self.schema,
            "moduleHash": self.artifact_hash,
        }));
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
            .and_then(Value::as_bool)
            .unwrap_or(false);
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
            "offline",
            "optimistic",
            "interactive",
            "classification",
            "description",
            "agent",
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
            },
        );
    }
    function_wires.sort_by(|left, right| left.path.cmp(&right.path));
    let visibility: BTreeMap<String, crate::visibility::VisibilityPlan> = module
        .get("visibility")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|error| invalid(format!("visibility plans: {error}")))?
        .unwrap_or_default();
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
            functions: function_wires,
            metadata: Map::new(),
        },
        definitions,
        visibility,
    ))
}

fn canonical_artifact_hash(module: &Map<String, Value>) -> Result<String, String> {
    let javascript = module
        .get("javascript")
        .and_then(Value::as_object)
        .ok_or_else(|| "artifact has no JavaScript bundle".to_owned())?;
    let contract = serde_json::json!({
        "generation":module.get("generation").cloned().unwrap_or(Value::Null),
        "language":module.get("language").cloned().unwrap_or(Value::String("typescript".to_owned())),
        "entrypoint":module.get("entrypoint").cloned().unwrap_or(Value::Null),
        "files":module.get("files").cloned().unwrap_or_else(|| serde_json::json!({})),
        "functions":module.get("functions").cloned().unwrap_or_else(|| serde_json::json!({})),
        "visibility":module.get("visibility").cloned().unwrap_or_else(|| serde_json::json!({})),
        "crons":module.get("crons").cloned().unwrap_or_else(|| serde_json::json!([])),
        "invitationAcceptanceReducer":module.get("invitationAcceptanceReducer").cloned().unwrap_or(Value::String(String::new())),
        "javascript":{
            "path":javascript.get("path").cloned().unwrap_or(Value::Null),
            "hash":javascript.get("hash").and_then(Value::as_str).unwrap_or("").trim().to_ascii_lowercase(),
        },
    });
    let bytes = canonical_json(&contract)?.into_bytes();
    Ok(Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn canonical_json(value: &Value) -> Result<String, String> {
    match value {
        Value::Null => Ok("null".to_owned()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Number(value) => Ok(value.to_string()),
        Value::String(value) => serde_json::to_string(value).map_err(|error| error.to_string()),
        Value::Array(values) => {
            let values = values
                .iter()
                .map(canonical_json)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(format!("[{}]", values.join(",")))
        }
        Value::Object(object) => {
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            let entries = entries
                .into_iter()
                .map(|(key, value)| {
                    Ok(format!(
                        "{}:{}",
                        serde_json::to_string(key).map_err(|error| error.to_string())?,
                        canonical_json(value)?
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            Ok(format!("{{{}}}", entries.join(",")))
        }
    }
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
                    "kind":"action","handler":"receive","file":"gonvex/callbacks.ts",
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
        let (_, functions, _) = artifact_from_manifest(&record).unwrap();
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
    let Some(encoded) = module.get("files").and_then(|f| f.get("client-contract.json")) else { return Ok(None); };
    let bytes = STANDARD.decode(encoded.as_str().ok_or("invalid client contract encoding")?).map_err(|e| e.to_string())?;
    let contract: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let version = contract.get("version").and_then(Value::as_u64).filter(|v| *v > 0).ok_or("invalid client contract version")?;
    if contract.get("offlineMaxAgeMs") != Some(&Value::Null) && contract.get("offlineMaxAgeMs").and_then(Value::as_u64).filter(|v| *v > 0).is_none() { return Err("invalid offline window".into()); }
    Ok(Some(version))
}

#[cfg(test)]
mod offline_contract_tests {
    use super::*;
    #[test]
    fn unlimited_offline_policy_is_valid_but_missing_or_invalid_window_is_not() {
        for (window, valid) in [(Value::Null, true), (serde_json::json!(604800000), true), (serde_json::json!(0), false), (serde_json::json!(-1), false), (serde_json::json!("forever"), false)] {
            let policy = serde_json::json!({"version": 1, "offlineMaxAgeMs": window});
            let module = serde_json::json!({"files": {"client-contract.json": STANDARD.encode(serde_json::to_vec(&policy).unwrap())}});
            assert_eq!(client_contract_from_module(module.as_object().unwrap()).is_ok(), valid);
        }
        let policy = serde_json::json!({"version": 1});
        let module = serde_json::json!({"files": {"client-contract.json": STANDARD.encode(serde_json::to_vec(&policy).unwrap())}});
        assert!(client_contract_from_module(module.as_object().unwrap()).is_err());
    }
}
