use std::collections::BTreeSet;
use std::fs;
use std::io::{self, Read};
use std::path::{Component, Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, SystemTime};

use deno_ast::{EmitOptions, MediaType, ParseParams, TranspileModuleOptions, TranspileOptions};
use duckdb::types::{Value, ValueRef};
use duckdb::{params_from_iter, Config};
use gonvex_module_runtime::{
    BoxFuture, Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse,
    Invocation, InvocationContext, ModuleArtifact, ModuleEngine, ModuleHost, ModuleLanguage,
    ModuleManifest,
};
use gonvex_module_runtime_v8::{initialize_v8_platform, V8Config, V8ModuleEngine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue};

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Request {
    version: u32,
    root: PathBuf,
    allow_unconfined: bool,
    code: String,
    duckdb: bool,
    #[serde(default)]
    imports: Vec<Import>,
    max_heap_bytes: u64,
    max_file_bytes: u64,
    max_workspace_bytes: u64,
    max_output_bytes: usize,
    max_rows: usize,
    duckdb_memory_bytes: u64,
    timeout_ms: u64,
    worker_uid: u32,
    worker_gid: u32,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Import {
    alias: String,
    path: String,
    #[serde(default, rename = "tables")]
    _tables: Vec<JsonValue>,
}

#[derive(Debug, Serialize)]
struct Response {
    ok: bool,
    result: JsonValue,
    error: String,
    logs: Vec<LogLine>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct LogLine {
    level: String,
    message: String,
}

struct WorkerHost {
    root: PathBuf,
    files: PathBuf,
    max_file_bytes: u64,
    max_workspace_bytes: u64,
    max_rows: usize,
    duckdb: Option<Mutex<duckdb::Connection>>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let response = match run().await {
        Ok((result, logs)) => Response {
            ok: true,
            result,
            error: String::new(),
            logs,
        },
        Err(error) => Response {
            ok: false,
            result: JsonValue::Null,
            error,
            logs: vec![],
        },
    };
    let encoded = serde_json::to_vec(&response).unwrap_or_else(|error| {
        format!(r#"{{"ok":false,"result":null,"error":"encode response: {error}","logs":[]}}"#)
            .into_bytes()
    });
    print!("{}", String::from_utf8_lossy(&encoded));
}

async fn run() -> Result<(JsonValue, Vec<LogLine>), String> {
    let mut input = Vec::new();
    io::stdin()
        .take(2 * 1024 * 1024)
        .read_to_end(&mut input)
        .map_err(|error| error.to_string())?;
    let request: Request = serde_json::from_slice(&input)
        .map_err(|error| format!("invalid worker request: {error}"))?;
    if request.version != 1 {
        return Err(format!("unsupported worker protocol {}", request.version));
    }
    if request.timeout_ms == 0 {
        return Err("worker timeout is required".to_owned());
    }

    isolate_filesystem(
        &request.root,
        request.allow_unconfined,
        request.worker_uid,
        request.worker_gid,
    )?;
    apply_resource_limits(&request)?;
    initialize_v8_platform();

    let host = WorkerHost::new(&request, Path::new("."))?;
    let javascript = transpile(&request.code, request.duckdb)?;
    let contract = FunctionContract {
        path: "run".to_owned(),
        kind: FunctionKind::Action,
        internal: true,
        delivery: None,
        args_schema: Some(json!({"kind":"any"})),
        result_schema: Some(json!({"kind":"any"})),
        metadata: Map::from_iter([("export".to_owned(), json!("run"))]),
    };
    let artifact = ModuleArtifact {
        manifest: ModuleManifest {
            module_id: "gonvex-sandbox".to_owned(),
            generation: 1,
            language: ModuleLanguage::TypeScript,
            artifact_hash: "sandbox-worker".to_owned(),
            functions: vec![contract],
            metadata: Map::new(),
        },
        payload: javascript.into_bytes(),
    };
    let engine = V8ModuleEngine::from_artifact(
        artifact,
        V8Config {
            max_heap_bytes: usize::try_from(request.max_heap_bytes).unwrap_or(64 << 20),
            execution_timeout: Duration::from_millis(request.timeout_ms),
            max_result_bytes: request.max_output_bytes,
            recycle_after_calls: 0,
            isolate_pool_size: 1,
        },
    )
    .map_err(|error| error.to_string())?;

    install_network_seccomp()?;
    let invocation = Invocation {
        function: "run".to_owned(),
        kind: FunctionKind::Action,
        args: b"null".to_vec(),
        context: InvocationContext {
            project_id: "sandbox".to_owned(),
            tenant_id: "sandbox".to_owned(),
            generation: 1,
            capabilities: Capabilities {
                sandbox: true,
                ..Capabilities::default()
            },
            now_unix_ms: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            deadline: Some(SystemTime::now() + Duration::from_millis(request.timeout_ms)),
            ..InvocationContext::default()
        },
    };
    let result = engine
        .invoke(&host, invocation)
        .await
        .map_err(|error| error.to_string())?;
    let value: JsonValue = serde_json::from_slice(&result.value)
        .map_err(|error| format!("sandbox returned invalid JSON: {error}"))?;
    let object = value
        .as_object()
        .ok_or_else(|| "sandbox result envelope is invalid".to_owned())?;
    let logs: Vec<LogLine> =
        serde_json::from_value(object.get("logs").cloned().unwrap_or_else(|| json!([])))
            .map_err(|error| error.to_string())?;
    Ok((
        object.get("result").cloned().unwrap_or(JsonValue::Null),
        logs,
    ))
}

fn transpile(code: &str, duckdb_enabled: bool) -> Result<String, String> {
    let duckdb_binding = if duckdb_enabled {
        r#"const duckdb = Object.freeze({
    query: (statement: string, parameters: unknown[] = []) => ctx.sandbox.__worker.query(statement, parameters),
    register: (name: string, rows: Record<string, unknown>[]) => ctx.sandbox.__worker.register(name, rows),
  });"#
    } else {
        ""
    };
    let wrapped = format!(
        r#"
export async function run(ctx: any): Promise<any> {{
  const __lines: Array<{{level: string, message: string}}> = [];
  const __format = (value: unknown): string => {{ try {{ return typeof value === "string" ? value : JSON.stringify(value); }} catch {{ return String(value); }} }};
  const __log = (level: string, values: unknown[]) => {{ if (__lines.length < 500) __lines.push({{level, message: values.map(__format).join(" ").slice(0, 16384)}}); }};
  const console = Object.freeze({{
    log: (...values: unknown[]) => __log("log", values),
    info: (...values: unknown[]) => __log("log", values),
    warn: (...values: unknown[]) => __log("warn", values),
    error: (...values: unknown[]) => __log("error", values),
  }});
  const files = Object.freeze({{
    readText: (path: string) => ctx.sandbox.__worker.readText(path),
    writeText: (path: string, content: string) => ctx.sandbox.__worker.writeText(path, content),
    list: () => ctx.sandbox.__worker.listFiles(),
  }});
  {duckdb_binding}
  const result = await (async (): Promise<unknown> => {{
{code}
  }})();
  return {{result, logs: __lines.slice(0, 500)}};
}}
"#
    );
    let program = deno_ast::parse_program(ParseParams {
        specifier: deno_ast::ModuleSpecifier::parse("file:///sandbox.ts")
            .map_err(|error| error.to_string())?,
        text: wrapped.into(),
        media_type: MediaType::TypeScript,
        capture_tokens: false,
        maybe_syntax: None,
        scope_analysis: false,
    })
    .map_err(|error| format!("TypeScript parse failed: {error}"))?;
    program
        .transpile(
            &TranspileOptions::default(),
            &TranspileModuleOptions::default(),
            &EmitOptions::default(),
        )
        .map(|value| value.into_source().text)
        .map_err(|error| format!("TypeScript transpile failed: {error}"))
}

impl WorkerHost {
    fn new(request: &Request, root: &Path) -> Result<Self, String> {
        let duckdb = if request.duckdb {
            let memory = format!("{}B", request.duckdb_memory_bytes);
            let config = Config::default()
                .enable_autoload_extension(false)
                .map_err(|error| error.to_string())?
                .enable_external_access(true)
                .map_err(|error| error.to_string())?
                .max_memory(&memory)
                .map_err(|error| error.to_string())?
                .threads(1)
                .map_err(|error| error.to_string())?;
            let connection =
                duckdb::Connection::open_with_flags(root.join("analysis.duckdb"), config)
                    .map_err(|error| error.to_string())?;
            for import in &request.imports {
                let alias = identifier(&import.alias)?;
                if !safe_relative(Path::new(&import.path)) {
                    return Err("DuckDB import escapes the workspace".to_owned());
                }
                let path = root
                    .join(&import.path)
                    .to_string_lossy()
                    .replace('\'', "''");
                connection
                    .execute_batch(&format!("ATTACH '{path}' AS \"{alias}\" (READ_ONLY)"))
                    .map_err(|error| format!("attach {alias}: {error}"))?;
            }
            connection.execute_batch("SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; SET allow_community_extensions=false; SET allow_persistent_secrets=false; SET enable_external_access=false; SET lock_configuration=true;").map_err(|error| format!("secure DuckDB: {error}"))?;
            Some(Mutex::new(connection))
        } else {
            None
        };
        Ok(Self {
            root: root.to_path_buf(),
            files: root.join("files"),
            max_file_bytes: request.max_file_bytes,
            max_workspace_bytes: request.max_workspace_bytes,
            max_rows: request.max_rows,
            duckdb,
        })
    }

    fn dispatch(&self, operation: &str, payload: &[u8]) -> Result<JsonValue, String> {
        let payload: JsonValue =
            serde_json::from_slice(payload).map_err(|error| error.to_string())?;
        match operation {
            "worker.readText" => self.read_text(required_string(&payload, "path")?),
            "worker.writeText" => self.write_text(
                required_string(&payload, "path")?,
                required_string(&payload, "content")?,
            ),
            "worker.listFiles" => self.list_files(),
            "worker.query" => self.query(
                required_string(&payload, "statement")?,
                payload
                    .get("parameters")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .unwrap_or_default(),
            ),
            "worker.register" => self.register(
                required_string(&payload, "name")?,
                payload
                    .get("rows")
                    .and_then(JsonValue::as_array)
                    .cloned()
                    .ok_or_else(|| "rows must be an array".to_owned())?,
            ),
            _ => Err(format!(
                "unsupported sandbox worker operation {operation:?}"
            )),
        }
    }

    fn read_text(&self, name: &str) -> Result<JsonValue, String> {
        let path = file_path(&self.files, name)?;
        let metadata = fs::metadata(&path).map_err(|error| error.to_string())?;
        if metadata.len() > self.max_file_bytes {
            return Err("sandbox file exceeds read limit".to_owned());
        }
        let value = fs::read_to_string(path).map_err(|error| error.to_string())?;
        Ok(JsonValue::String(value))
    }

    fn write_text(&self, name: &str, content: &str) -> Result<JsonValue, String> {
        if content.len() as u64 > self.max_file_bytes {
            return Err("sandbox file exceeds write limit".to_owned());
        }
        let path = file_path(&self.files, name)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let current = directory_size(&self.root)?;
        let old = fs::metadata(&path).map(|value| value.len()).unwrap_or(0);
        if current
            .saturating_sub(old)
            .saturating_add(content.len() as u64)
            > self.max_workspace_bytes
        {
            return Err("sandbox workspace byte limit exceeded".to_owned());
        }
        fs::write(&path, content).map_err(|error| error.to_string())?;
        Ok(json!({"path": name, "size": content.len()}))
    }

    fn list_files(&self) -> Result<JsonValue, String> {
        let mut files = Vec::new();
        list_relative(&self.files, &self.files, &mut files)?;
        files.sort();
        Ok(json!(files))
    }

    fn query(&self, statement: &str, parameters: Vec<JsonValue>) -> Result<JsonValue, String> {
        let connection = self
            .duckdb
            .as_ref()
            .ok_or_else(|| "DuckDB was not enabled for this sandbox".to_owned())?
            .lock()
            .map_err(|_| "DuckDB lock poisoned".to_owned())?;
        let values = parameters
            .iter()
            .map(json_to_duck)
            .collect::<Result<Vec<_>, _>>()?;
        let mut prepared = connection
            .prepare(statement)
            .map_err(|error| error.to_string())?;
        let mut rows = prepared
            .query(params_from_iter(values.iter()))
            .map_err(|error| error.to_string())?;
        let columns = rows
            .as_ref()
            .map(|statement| statement.column_names())
            .unwrap_or_default();
        let mut output = Vec::new();
        let mut truncated = false;
        while let Some(row) = rows.next().map_err(|error| error.to_string())? {
            if output.len() >= self.max_rows {
                truncated = true;
                break;
            }
            let mut record = Map::new();
            for (index, name) in columns.iter().enumerate() {
                record.insert(
                    name.clone(),
                    duck_to_json(row.get_ref(index).map_err(|error| error.to_string())?),
                );
            }
            output.push(JsonValue::Object(record));
        }
        Ok(
            json!({"columns": columns, "rowCount": output.len(), "rows": output, "truncated": truncated}),
        )
    }

    fn register(&self, name: &str, rows: Vec<JsonValue>) -> Result<JsonValue, String> {
        let name = identifier(name)?;
        if rows.len() > 100_000 {
            return Err("DuckDB register is limited to 100000 rows".to_owned());
        }
        let objects = rows
            .iter()
            .map(|row| {
                row.as_object()
                    .ok_or_else(|| "every registered row must be an object".to_owned())
            })
            .collect::<Result<Vec<_>, _>>()?;
        let columns = objects
            .iter()
            .flat_map(|row| row.keys().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if columns.is_empty() {
            return Err("DuckDB register requires at least one column".to_owned());
        }
        for column in &columns {
            identifier(column)?;
        }
        let types = columns
            .iter()
            .map(|column| infer_type(&objects, column))
            .collect::<Vec<_>>();
        let connection = self
            .duckdb
            .as_ref()
            .ok_or_else(|| "DuckDB was not enabled for this sandbox".to_owned())?
            .lock()
            .map_err(|_| "DuckDB lock poisoned".to_owned())?;
        let definitions = columns
            .iter()
            .zip(types.iter())
            .map(|(column, kind)| format!("\"{column}\" {kind}"))
            .collect::<Vec<_>>()
            .join(", ");
        connection.execute_batch(&format!("DROP TABLE IF EXISTS \"{name}\"; CREATE TABLE \"{name}\" ({definitions}); BEGIN TRANSACTION;")).map_err(|error| error.to_string())?;
        let markers = (0..columns.len())
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(",");
        let insert = format!("INSERT INTO \"{name}\" VALUES ({markers})");
        let result = (|| {
            let mut prepared = connection
                .prepare(&insert)
                .map_err(|error| error.to_string())?;
            for row in &objects {
                let values = columns
                    .iter()
                    .zip(types.iter())
                    .map(|(column, kind)| register_value(row.get(column), kind))
                    .collect::<Result<Vec<_>, _>>()?;
                prepared
                    .execute(params_from_iter(values.iter()))
                    .map_err(|error| error.to_string())?;
            }
            Ok::<(), String>(())
        })();
        if let Err(error) = result {
            let _ = connection.execute_batch("ROLLBACK");
            return Err(error);
        }
        connection
            .execute_batch("COMMIT")
            .map_err(|error| error.to_string())?;
        Ok(json!({"table": name, "rows": rows.len(), "columns": columns}))
    }
}

impl ModuleHost for WorkerHost {
    fn call<'a>(
        &'a self,
        _context: &'a InvocationContext,
        call: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async move {
            let HostCall::Sandbox { operation, payload } = call else {
                return Err(HostError::CapabilityDenied("sandbox"));
            };
            let value = self
                .dispatch(&operation, &payload)
                .map_err(HostError::Failed)?;
            serde_json::to_vec(&value)
                .map(|value| HostResponse { value })
                .map_err(|error| HostError::Failed(error.to_string()))
        })
    }
}

fn required_string<'a>(value: &'a JsonValue, key: &str) -> Result<&'a str, String> {
    value
        .get(key)
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("{key} must be a non-empty string"))
}

fn identifier(value: &str) -> Result<String, String> {
    let valid = value.len() <= 63
        && value.chars().enumerate().all(|(index, character)| {
            character == '_'
                || character.is_ascii_alphabetic()
                || (index > 0 && character.is_ascii_digit())
        });
    if valid {
        Ok(value.to_owned())
    } else {
        Err(format!("invalid DuckDB identifier {value:?}"))
    }
}

fn safe_relative(path: &Path) -> bool {
    !path.is_absolute()
        && path
            .components()
            .all(|part| matches!(part, Component::Normal(_)))
}

fn file_path(root: &Path, name: &str) -> Result<PathBuf, String> {
    let relative = Path::new(name);
    if !safe_relative(relative) {
        return Err("sandbox path must be relative and cannot escape the workspace".to_owned());
    }
    Ok(root.join(relative))
}

fn directory_size(root: &Path) -> Result<u64, String> {
    let mut total = 0_u64;
    for entry in fs::read_dir(root).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let metadata = entry.metadata().map_err(|error| error.to_string())?;
        total = total.saturating_add(if metadata.is_dir() {
            directory_size(&entry.path())?
        } else {
            metadata.len()
        });
    }
    Ok(total)
}

fn list_relative(root: &Path, current: &Path, output: &mut Vec<String>) -> Result<(), String> {
    if !current.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(current).map_err(|error| error.to_string())? {
        let entry = entry.map_err(|error| error.to_string())?;
        let path = entry.path();
        if entry
            .file_type()
            .map_err(|error| error.to_string())?
            .is_dir()
        {
            list_relative(root, &path, output)?;
        } else {
            output.push(
                path.strip_prefix(root)
                    .map_err(|error| error.to_string())?
                    .to_string_lossy()
                    .replace('\\', "/"),
            );
        }
    }
    Ok(())
}

fn json_to_duck(value: &JsonValue) -> Result<Value, String> {
    Ok(match value {
        JsonValue::Null => Value::Null,
        JsonValue::Bool(value) => Value::Boolean(*value),
        JsonValue::Number(value) if value.is_i64() => {
            Value::BigInt(value.as_i64().unwrap_or_default())
        }
        JsonValue::Number(value) => Value::Double(
            value
                .as_f64()
                .ok_or_else(|| "number is not representable".to_owned())?,
        ),
        JsonValue::String(value) => Value::Text(value.clone()),
        value => Value::Text(value.to_string()),
    })
}

fn duck_to_json(value: ValueRef<'_>) -> JsonValue {
    match value {
        ValueRef::Null => JsonValue::Null,
        ValueRef::Boolean(value) => json!(value),
        ValueRef::TinyInt(value) => json!(value),
        ValueRef::SmallInt(value) => json!(value),
        ValueRef::Int(value) => json!(value),
        ValueRef::BigInt(value) => json!(value),
        ValueRef::HugeInt(value) => i64::try_from(value)
            .map(|value| json!(value))
            .unwrap_or_else(|_| JsonValue::String(value.to_string())),
        ValueRef::UTinyInt(value) => json!(value),
        ValueRef::USmallInt(value) => json!(value),
        ValueRef::UInt(value) => json!(value),
        ValueRef::UBigInt(value) => json!(value),
        ValueRef::Float(value) => json!(value),
        ValueRef::Double(value) => json!(value),
        ValueRef::Decimal(value) => JsonValue::String(value.to_string()),
        ValueRef::Text(value) => JsonValue::String(String::from_utf8_lossy(value).into_owned()),
        ValueRef::Blob(value) => JsonValue::String(format!("<{} bytes>", value.len())),
        other => JsonValue::String(format!("{other:?}")),
    }
}

fn infer_type(rows: &[&Map<String, JsonValue>], column: &str) -> &'static str {
    let mut kind = "NULL";
    for value in rows
        .iter()
        .filter_map(|row| row.get(column))
        .filter(|value| !value.is_null())
    {
        let current = match value {
            JsonValue::Bool(_) => "BOOLEAN",
            JsonValue::Number(number) if number.is_i64() => "BIGINT",
            JsonValue::Number(_) => "DOUBLE",
            _ => "VARCHAR",
        };
        kind = match (kind, current) {
            ("NULL", value) => value,
            (left, right) if left == right => left,
            ("BIGINT", "DOUBLE") | ("DOUBLE", "BIGINT") => "DOUBLE",
            _ => "VARCHAR",
        };
    }
    if kind == "NULL" {
        "VARCHAR"
    } else {
        kind
    }
}

fn register_value(value: Option<&JsonValue>, kind: &str) -> Result<Value, String> {
    let Some(value) = value else {
        return Ok(Value::Null);
    };
    if value.is_null() {
        return Ok(Value::Null);
    }
    match kind {
        "BOOLEAN" => value
            .as_bool()
            .map(Value::Boolean)
            .ok_or_else(|| "boolean column contains another type".to_owned()),
        "BIGINT" => value
            .as_i64()
            .map(Value::BigInt)
            .ok_or_else(|| "integer column contains another type".to_owned()),
        "DOUBLE" => value
            .as_f64()
            .map(Value::Double)
            .ok_or_else(|| "number column contains another type".to_owned()),
        _ => Ok(Value::Text(
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string()),
        )),
    }
}

fn isolate_filesystem(
    root: &Path,
    allow_unconfined: bool,
    worker_uid: u32,
    worker_gid: u32,
) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::geteuid() == 0 {
            let root = std::ffi::CString::new(root.as_os_str().as_encoded_bytes())
                .map_err(|_| "sandbox root contains NUL".to_owned())?;
            if libc::chroot(root.as_ptr()) != 0 || libc::chdir(c"/".as_ptr()) != 0 {
                return Err(format!("chroot sandbox: {}", io::Error::last_os_error()));
            }
            if worker_uid == 0 || worker_gid == 0 {
                return Err("sandbox worker uid and gid must be non-root".to_owned());
            }
            if libc::setgroups(0, std::ptr::null()) != 0
                || libc::setgid(worker_gid) != 0
                || libc::setuid(worker_uid) != 0
            {
                return Err(format!(
                    "drop sandbox privileges: {}",
                    io::Error::last_os_error()
                ));
            }
            return Ok(());
        }
    }
    if !allow_unconfined {
        return Err("sandbox worker requires chroot isolation; unconfined mode is for local development only".to_owned());
    }
    std::env::set_current_dir(root).map_err(|error| error.to_string())
}

fn apply_resource_limits(request: &Request) -> Result<(), String> {
    #[cfg(unix)]
    unsafe {
        for (resource, limit) in [
            (libc::RLIMIT_CORE, 0),
            (libc::RLIMIT_FSIZE, request.max_workspace_bytes),
            (libc::RLIMIT_NOFILE, 64),
        ] {
            let value = libc::rlimit {
                rlim_cur: limit,
                rlim_max: limit,
            };
            if libc::setrlimit(resource, &value) != 0 {
                return Err(format!(
                    "set sandbox resource limit: {}",
                    io::Error::last_os_error()
                ));
            }
        }
    }
    Ok(())
}

fn install_network_seccomp() -> Result<(), String> {
    #[cfg(target_os = "linux")]
    unsafe {
        const SECCOMP_SET_MODE_FILTER: libc::c_uint = 1;
        const SECCOMP_FILTER_FLAG_TSYNC: libc::c_ulong = 1;
        const BPF_LD: u16 = 0x00;
        const BPF_W: u16 = 0x00;
        const BPF_ABS: u16 = 0x20;
        const BPF_JMP: u16 = 0x05;
        const BPF_JEQ: u16 = 0x10;
        const BPF_K: u16 = 0x00;
        const BPF_RET: u16 = 0x06;
        const ALLOW: u32 = 0x7fff0000;
        const ERRNO: u32 = 0x00050000;
        const KILL_PROCESS: u32 = 0x80000000;
        #[cfg(target_arch = "x86_64")]
        const AUDIT_ARCH: u32 = 0xc000003e;
        #[cfg(target_arch = "aarch64")]
        const AUDIT_ARCH: u32 = 0xc00000b7;
        let denied = [
            libc::SYS_socket,
            libc::SYS_connect,
            libc::SYS_bind,
            libc::SYS_listen,
            libc::SYS_accept,
            libc::SYS_accept4,
            libc::SYS_execve,
            libc::SYS_execveat,
            libc::SYS_ptrace,
            libc::SYS_mount,
            libc::SYS_umount2,
            libc::SYS_unshare,
            libc::SYS_setns,
            libc::SYS_fork,
            libc::SYS_vfork,
            libc::SYS_io_uring_setup,
            libc::SYS_io_uring_enter,
            libc::SYS_io_uring_register,
            libc::SYS_bpf,
            libc::SYS_perf_event_open,
            libc::SYS_keyctl,
            libc::SYS_add_key,
            libc::SYS_request_key,
        ];
        let mut filters = vec![
            libc::sock_filter {
                code: BPF_LD | BPF_W | BPF_ABS,
                jt: 0,
                jf: 0,
                k: 4,
            },
            libc::sock_filter {
                code: BPF_JMP | BPF_JEQ | BPF_K,
                jt: 1,
                jf: 0,
                k: AUDIT_ARCH,
            },
            libc::sock_filter {
                code: BPF_RET | BPF_K,
                jt: 0,
                jf: 0,
                k: KILL_PROCESS,
            },
            libc::sock_filter {
                code: BPF_LD | BPF_W | BPF_ABS,
                jt: 0,
                jf: 0,
                k: 0,
            },
        ];
        for syscall in denied {
            filters.push(libc::sock_filter {
                code: BPF_JMP | BPF_JEQ | BPF_K,
                jt: 0,
                jf: 1,
                k: syscall as u32,
            });
            filters.push(libc::sock_filter {
                code: BPF_RET | BPF_K,
                jt: 0,
                jf: 0,
                k: ERRNO | libc::EPERM as u32,
            });
        }
        filters.push(libc::sock_filter {
            code: BPF_RET | BPF_K,
            jt: 0,
            jf: 0,
            k: ALLOW,
        });
        let program = libc::sock_fprog {
            len: filters.len() as u16,
            filter: filters.as_mut_ptr(),
        };
        if libc::prctl(libc::PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0 {
            return Err(io::Error::last_os_error().to_string());
        }
        if libc::syscall(
            libc::SYS_seccomp,
            SECCOMP_SET_MODE_FILTER,
            SECCOMP_FILTER_FLAG_TSYNC,
            &program,
        ) != 0
        {
            return Err(format!(
                "install sandbox seccomp: {}",
                io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typescript_is_wrapped_with_only_requested_duckdb_binding() {
        let without = transpile("const value: number = 3; return value;", false).unwrap();
        assert!(!without.contains("const duckdb ="));
        assert!(without.contains("return value"));

        let with = transpile("return await duckdb.query('select 1')", true).unwrap();
        assert!(with.contains("const duckdb ="));
        assert!(with.contains("worker.query"));
    }

    #[test]
    fn paths_and_identifiers_fail_closed() {
        assert!(file_path(Path::new("files"), "../secret").is_err());
        assert!(file_path(Path::new("files"), "/etc/passwd").is_err());
        assert!(identifier("safe_table_1").is_ok());
        assert!(identifier("tasks; attach '/tmp/x'").is_err());
    }

    #[test]
    fn tenant_rows_get_stable_duckdb_types() {
        let first = Map::from_iter([
            ("count".to_owned(), json!(1)),
            ("name".to_owned(), json!("A")),
        ]);
        let second = Map::from_iter([
            ("count".to_owned(), json!(2.5)),
            ("name".to_owned(), JsonValue::Null),
        ]);
        let rows = vec![&first, &second];
        assert_eq!(infer_type(&rows, "count"), "DOUBLE");
        assert_eq!(infer_type(&rows, "name"), "VARCHAR");
    }

    #[test]
    fn optional_duckdb_registers_and_analyzes_tenant_rows() {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "gonvex-sandbox-worker-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("files")).unwrap();
        fs::create_dir_all(root.join("imports")).unwrap();
        let request = Request {
            version: 1,
            root: root.clone(),
            allow_unconfined: true,
            code: String::new(),
            duckdb: true,
            imports: vec![],
            max_heap_bytes: 64 << 20,
            max_file_bytes: 1 << 20,
            max_workspace_bytes: 16 << 20,
            max_output_bytes: 1 << 20,
            max_rows: 100,
            duckdb_memory_bytes: 64 << 20,
            timeout_ms: 10_000,
            worker_uid: 65534,
            worker_gid: 65534,
        };
        let host = WorkerHost::new(&request, &root).unwrap();
        host.dispatch("worker.register", br#"{"name":"tasks","rows":[{"status":"open","amount":2},{"status":"open","amount":3}]}"#).unwrap();
        let result = host
            .dispatch(
                "worker.query",
                br#"{"statement":"select sum(amount) as total from tasks","parameters":[]}"#,
            )
            .unwrap();
        assert_eq!(result["rows"][0]["total"], 5);
        drop(host);
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn approved_spreadsheet_artifact_is_attached_read_only() {
        let nonce = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "gonvex-sandbox-import-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(root.join("files")).unwrap();
        fs::create_dir_all(root.join("imports")).unwrap();
        let import_path = root.join("imports/workbook.duckdb");
        let source = duckdb::Connection::open(&import_path).unwrap();
        source
            .execute_batch("CREATE TABLE sales(region VARCHAR, amount BIGINT); INSERT INTO sales VALUES ('north', 7), ('south', 4);")
            .unwrap();
        drop(source);
        let request = Request {
            version: 1,
            root: root.clone(),
            allow_unconfined: true,
            code: String::new(),
            duckdb: true,
            imports: vec![Import {
                alias: "workbook".to_owned(),
                path: "imports/workbook.duckdb".to_owned(),
                _tables: vec![],
            }],
            max_heap_bytes: 64 << 20,
            max_file_bytes: 1 << 20,
            max_workspace_bytes: 16 << 20,
            max_output_bytes: 1 << 20,
            max_rows: 100,
            duckdb_memory_bytes: 64 << 20,
            timeout_ms: 10_000,
            worker_uid: 65534,
            worker_gid: 65534,
        };
        let host = WorkerHost::new(&request, &root).unwrap();
        let result = host
            .dispatch(
                "worker.query",
                br#"{"statement":"select sum(amount) as total from workbook.sales","parameters":[]}"#,
            )
            .unwrap();
        assert_eq!(result["rows"][0]["total"], 11);
        assert!(host
            .dispatch(
                "worker.query",
                br#"{"statement":"insert into workbook.sales values ('blocked', 1)","parameters":[]}"#,
            )
            .is_err());
        assert!(host
            .dispatch(
                "worker.query",
                br#"{"statement":"select * from read_csv_auto('/etc/passwd')","parameters":[]}"#,
            )
            .is_err());
        drop(host);
        fs::remove_dir_all(root).unwrap();
    }
}
