//! Trusted project-data browser used by the Gonvex operator dashboard.
//!
//! This is intentionally not an application invocation surface. It requires
//! operator authorization, resolves physical tenant databases inside the host,
//! hides runtime-owned tables, and never returns a database URL.

use std::collections::{BTreeMap, BTreeSet};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, patch, post};
use axum::{Json, Router};
use gonvex_store::rows::{json_object_expr, table_columns, ColumnInfo, JsonKind};
use gonvex_store::TenantTransaction;
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::types::Json as SqlJson;
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};

use crate::{operations, Runtime};

pub fn router() -> Router<Runtime> {
    Router::new()
        .route("/dev/data/tables", get(list_tables))
        .route(
            "/dev/data/tables/{table}/rows",
            get(read_rows).post(insert_row),
        )
        .route(
            "/dev/data/tables/{table}/rows/{row}",
            patch(update_row).delete(delete_row),
        )
        .route("/dev/data/references/replace", post(replace_references))
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DataQuery {
    #[serde(default)]
    project: String,
    #[serde(default)]
    tenant: String,
    #[serde(default)]
    limit: i64,
    #[serde(default)]
    offset: i64,
    #[serde(default)]
    search: String,
    #[serde(default)]
    sort: String,
    #[serde(default)]
    direction: String,
    #[serde(default)]
    filters: String,
    #[serde(default)]
    columns: String,
    #[serde(default)]
    count: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RowsFilter {
    column: String,
    #[serde(default)]
    operator: String,
    #[serde(default)]
    value: String,
    #[serde(default)]
    value_to: String,
}

fn project(headers: &HeaderMap, query: &DataQuery) -> String {
    headers
        .get("x-gonvex-project-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(query.project.trim())
        .to_owned()
}

fn tenant(headers: &HeaderMap, query: &DataQuery) -> String {
    headers
        .get("x-gonvex-tenant-id")
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(query.tenant.trim())
        .to_owned()
}

fn internal_table(name: &str) -> bool {
    name == "members"
        || name == "telemetry_events"
        || name.starts_with("gonvex_")
        || name.starts_with("_gonvex_")
}

fn valid_ident(value: &str) -> bool {
    let mut bytes = value.bytes();
    matches!(bytes.next(), Some(b'a'..=b'z' | b'A'..=b'Z' | b'_'))
        && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn quote_ident(value: &str) -> Result<String, &'static str> {
    valid_ident(value)
        .then(|| format!("\"{}\"", value.replace('"', "\"\"")))
        .ok_or("invalid identifier")
}

fn error(status: StatusCode, message: impl Into<String>) -> Response {
    (status, Json(json!({"error":message.into()}))).into_response()
}

async fn admit(
    runtime: &Runtime,
    database_url: &str,
) -> Result<gonvex_store::AdmissionPermit, Response> {
    runtime
        .inner
        .pools
        .admit(database_url)
        .await
        .map_err(|cause| error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()))
}

async fn authorize_read(
    runtime: &Runtime,
    headers: &HeaderMap,
    project: &str,
) -> Result<(), Response> {
    operations::authorize_project_resource(runtime, headers, project, "projects:read", false)
        .await
        .map(|_| ())
}

async fn authorize_write(
    runtime: &Runtime,
    headers: &HeaderMap,
    project: &str,
) -> Result<(), Response> {
    operations::authorize_project_resource(runtime, headers, project, "projects:update", true)
        .await
        .map(|_| ())
}

/// Resolves the selected project or tenant database inside the host.
async fn data_database(runtime: &Runtime, project: &str, tenant: &str) -> Result<String, Response> {
    if project.trim().is_empty() {
        return Err(error(StatusCode::BAD_REQUEST, "project is required"));
    }
    let configured = if tenant.is_empty() {
        runtime.inner.config.project_database_urls.get(project)
    } else {
        runtime
            .inner
            .config
            .tenant_database_urls
            .get(&format!("{project}:{tenant}"))
    };
    if let Some(database_url) = configured {
        return Ok(database_url.to_owned());
    }
    let control = runtime
        .inner
        .control_plane
        .read()
        .await
        .clone()
        .ok_or_else(|| {
            error(
                StatusCode::SERVICE_UNAVAILABLE,
                "Control Plane is unavailable",
            )
        })?;
    let route = control
        .resolve_tenant(project, if tenant.is_empty() { project } else { tenant })
        .await
        .map_err(|_| error(StatusCode::NOT_FOUND, "tenant database was not found"))?;
    Ok(route.database_url)
}

/// The read-only pool for browsing. Each statement sees one WAL snapshot.
async fn data_reader(runtime: &Runtime, database_url: &str) -> Result<SqlitePool, Response> {
    runtime
        .inner
        .pools
        .reader(database_url)
        .await
        .map_err(|cause| error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()))
}

/// Operator edits are tenant writes: they run in a `TenantTransaction` so the
/// change feed revisions them like any Reducer write.
async fn data_writer(runtime: &Runtime, database_url: &str) -> Result<TenantTransaction, Response> {
    gonvex_store::begin_transaction(&runtime.inner.pools, database_url, false)
        .await
        .map_err(|cause| error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()))
}

async fn reader_columns(pool: &SqlitePool, table: &str) -> Result<Vec<ColumnInfo>, String> {
    let mut connection = pool.acquire().await.map_err(|cause| cause.to_string())?;
    table_columns(&mut *connection, table)
        .await
        .map_err(|cause| cause.to_string())
}

fn column_kinds(columns: &[ColumnInfo]) -> Vec<(String, JsonKind)> {
    columns
        .iter()
        .map(|column| (column.name.clone(), column.kind()))
        .collect()
}

/// The text form a filter compares against. Booleans read as `true`/`false`
/// so operators filter them by the value the grid shows.
fn text_expr(quoted: &str, kind: JsonKind) -> String {
    match kind {
        JsonKind::Boolean => format!(
            "(CASE WHEN {quoted} IS NULL THEN NULL WHEN {quoted} THEN 'true' ELSE 'false' END)"
        ),
        _ => format!("CAST({quoted} AS TEXT)"),
    }
}

/// The SQL value for column `name` read from the JSON object bound as `$1`,
/// coerced by the column's declared type (replaces `jsonb_populate_record`).
fn json_input_expr(name: &str, kind: JsonKind) -> String {
    let path = format!("'$.{name}'");
    match kind {
        JsonKind::Json => {
            format!("CASE json_type($1,{path}) WHEN 'null' THEN NULL ELSE $1 -> {path} END")
        }
        JsonKind::Boolean => format!(
            "CASE json_type($1,{path}) WHEN 'true' THEN 1 WHEN 'false' THEN 0 \
             WHEN 'text' THEN CASE lower(json_extract($1,{path})) \
               WHEN 'true' THEN 1 WHEN 't' THEN 1 WHEN 'false' THEN 0 WHEN 'f' THEN 0 \
               ELSE json_extract($1,{path}) END \
             ELSE json_extract($1,{path}) END"
        ),
        _ => format!("json_extract($1,{path})"),
    }
}

async fn list_tables(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
) -> Response {
    let project = project(&headers, &query);
    if let Err(response) = authorize_read(&runtime, &headers, &project).await {
        return response;
    }
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let _admission = match admit(&runtime, &database_url).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let pool = match data_reader(&runtime, &database_url).await {
        Ok(pool) => pool,
        Err(response) => return response,
    };
    // `sqlite_stat1` (written by ANALYZE/optimize) supplies cheap estimates.
    // Tables it does not cover are counted exactly.
    let rows = match sqlx::query(
        r#"SELECT m.name AS name,
                  (SELECT json_group_array(c.name) FROM
                     (SELECT name FROM pragma_table_info(m.name) ORDER BY cid) c) AS columns
           FROM sqlite_schema m
           WHERE m.type='table' AND m.name NOT LIKE 'sqlite\_%' ESCAPE '\'
           ORDER BY m.name"#,
    )
    .fetch_all(&pool)
    .await
    {
        Ok(rows) => rows,
        Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
    };
    let has_stats: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_schema WHERE type='table' AND name='sqlite_stat1')",
    )
    .fetch_one(&pool)
    .await
    .unwrap_or(false);
    let estimates: BTreeMap<String, i64> = if has_stats {
        sqlx::query("SELECT tbl, stat FROM sqlite_stat1 WHERE stat IS NOT NULL")
            .fetch_all(&pool)
            .await
            .unwrap_or_default()
            .into_iter()
            .filter_map(|row| {
                let table: String = row.try_get("tbl").ok()?;
                let stat: String = row.try_get("stat").ok()?;
                let count = stat.split_whitespace().next()?.parse::<i64>().ok()?;
                Some((table, count))
            })
            .collect()
    } else {
        BTreeMap::new()
    };
    let mut tables = Vec::new();
    for row in rows {
        let name: String = row.get("name");
        if internal_table(&name) {
            continue;
        }
        let columns: Vec<String> = row
            .try_get::<SqlJson<Vec<String>>, _>("columns")
            .map(|columns| columns.0)
            .unwrap_or_default();
        let (count, estimated) = match estimates.get(&name) {
            Some(count) => (*count, true),
            None => match quote_ident(&name) {
                Ok(quoted) => (
                    sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {quoted}"))
                        .fetch_one(&pool)
                        .await
                        .unwrap_or(0),
                    false,
                ),
                Err(_) => (0, true),
            },
        };
        tables.push(json!({"name":name,"columns":columns,
            "rowCount":count,"rowCountEstimated":estimated}));
    }
    Json(json!({"tables":tables})).into_response()
}

fn requested_columns(raw: &str, all: &[String]) -> Result<Vec<String>, &'static str> {
    if raw.trim().is_empty() {
        return Ok(all.to_vec());
    }
    let allowed: BTreeSet<&str> = all.iter().map(String::as_str).collect();
    let mut seen = BTreeSet::new();
    let mut selected = Vec::new();
    for column in raw
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if !allowed.contains(column) || !valid_ident(column) {
            return Err("invalid selected column");
        }
        if seen.insert(column.to_owned()) {
            selected.push(column.to_owned());
        }
    }
    Ok(if selected.is_empty() {
        all.to_vec()
    } else {
        selected
    })
}

fn append_where<'a>(
    builder: &mut QueryBuilder<'a, Sqlite>,
    all: &BTreeMap<&str, JsonKind>,
    search: &'a str,
    filters: &'a [RowsFilter],
) -> Result<(), &'static str> {
    let mut has_clause = false;
    let prefix = |builder: &mut QueryBuilder<'a, Sqlite>, has_clause: &mut bool| {
        builder.push(if *has_clause { " AND " } else { " WHERE " });
        *has_clause = true;
    };
    // SQLite LIKE is ASCII case-insensitive, matching ILIKE for identifiers
    // and codes. Backslash stays the escape character as it was in Postgres.
    const LIKE: &str = ",'') LIKE ";
    const ESCAPE: &str = " ESCAPE '\\'";
    if !search.trim().is_empty() && !all.is_empty() {
        prefix(builder, &mut has_clause);
        builder.push('(');
        for (index, (column, kind)) in all.iter().enumerate() {
            if index > 0 {
                builder.push(" OR ");
            }
            builder
                .push("COALESCE(")
                .push(text_expr(&quote_ident(column)?, *kind))
                .push(LIKE)
                .push_bind(format!("%{}%", search.trim()))
                .push(ESCAPE);
        }
        builder.push(')');
    }
    for filter in filters
        .iter()
        .filter(|filter| !filter.column.trim().is_empty())
    {
        let column = filter.column.trim();
        let Some(kind) = all.get(column).copied() else {
            return Err("invalid filter column");
        };
        if !valid_ident(column) {
            return Err("invalid filter column");
        }
        prefix(builder, &mut has_clause);
        let quoted = quote_ident(column)?;
        let text = text_expr(&quoted, kind);
        match filter.operator.as_str() {
            "empty" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NULL OR ")
                    .push(&text)
                    .push("='')");
            }
            "notEmpty" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NOT NULL AND ")
                    .push(&text)
                    .push("<>'')");
            }
            "equals" => {
                builder.push(&text).push("=").push_bind(&filter.value);
            }
            "notEquals" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NULL OR ")
                    .push(&text)
                    .push("<>")
                    .push_bind(&filter.value)
                    .push(')');
            }
            "startsWith" => {
                builder
                    .push("COALESCE(")
                    .push(&text)
                    .push(LIKE)
                    .push_bind(format!("{}%", filter.value))
                    .push(ESCAPE);
            }
            "endsWith" => {
                builder
                    .push("COALESCE(")
                    .push(&text)
                    .push(LIKE)
                    .push_bind(format!("%{}", filter.value))
                    .push(ESCAPE);
            }
            "notContains" => {
                builder
                    .push("COALESCE(")
                    .push(&text)
                    .push(",'') NOT LIKE ")
                    .push_bind(format!("%{}%", filter.value))
                    .push(ESCAPE);
            }
            "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" => {
                let operator = match filter.operator.as_str() {
                    "lessThan" => "<",
                    "lessThanOrEqual" => "<=",
                    "greaterThan" => ">",
                    _ => ">=",
                };
                builder
                    .push(&text)
                    .push(" ")
                    .push(operator)
                    .push(' ')
                    .push_bind(&filter.value);
            }
            "inRange" => {
                builder.push('(').push(&quoted).push(" IS NOT NULL");
                if !filter.value.trim().is_empty() {
                    builder
                        .push(" AND ")
                        .push(&text)
                        .push(" >= ")
                        .push_bind(&filter.value);
                }
                if !filter.value_to.trim().is_empty() {
                    builder
                        .push(" AND ")
                        .push(&text)
                        .push(" <= ")
                        .push_bind(&filter.value_to);
                }
                builder.push(')');
            }
            "oneOf" => {
                let values: Vec<String> =
                    serde_json::from_str(&filter.value).map_err(|_| "invalid oneOf filter")?;
                if values.is_empty() {
                    builder.push("1");
                } else {
                    builder.push("COALESCE(").push(&text).push(",'') IN (");
                    let mut separated = builder.separated(',');
                    for value in values {
                        separated.push_bind(value);
                    }
                    builder.push(')');
                }
            }
            _ => {
                builder
                    .push("COALESCE(")
                    .push(&text)
                    .push(LIKE)
                    .push_bind(format!("%{}%", filter.value))
                    .push(ESCAPE);
            }
        }
    }
    Ok(())
}

async fn read_rows(
    State(runtime): State<Runtime>,
    Path(table): Path<String>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
) -> Response {
    let project = project(&headers, &query);
    if let Err(response) = authorize_read(&runtime, &headers, &project).await {
        return response;
    }
    if internal_table(&table) {
        return error(StatusCode::NOT_FOUND, "table not found");
    }
    if !valid_ident(&table) {
        return error(StatusCode::BAD_REQUEST, "invalid table name");
    }
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let _admission = match admit(&runtime, &database_url).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let pool = match data_reader(&runtime, &database_url).await {
        Ok(pool) => pool,
        Err(response) => return response,
    };
    let infos = match reader_columns(&pool, &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        Ok(_) => return error(StatusCode::BAD_REQUEST, "table does not exist"),
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause),
    };
    let all: Vec<String> = infos.iter().map(|column| column.name.clone()).collect();
    let selected = match requested_columns(&query.columns, &all) {
        Ok(columns) => columns,
        Err(message) => return error(StatusCode::BAD_REQUEST, message),
    };
    let filters: Vec<RowsFilter> = if query.filters.trim().is_empty() {
        Vec::new()
    } else {
        match serde_json::from_str(&query.filters) {
            Ok(filters) => filters,
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        }
    };
    let all_set: BTreeSet<&str> = all.iter().map(String::as_str).collect();
    let all_kinds: BTreeMap<&str, JsonKind> = infos
        .iter()
        .map(|column| (column.name.as_str(), column.kind()))
        .collect();
    let quoted_table = quote_ident(&table).expect("validated identifier");
    let exact_total = query.count != "false" && query.count != "estimate";
    let total = if exact_total {
        let mut count = QueryBuilder::<Sqlite>::new(format!("SELECT count(*) FROM {quoted_table}"));
        if let Err(message) = append_where(&mut count, &all_kinds, &query.search, &filters) {
            return error(StatusCode::BAD_REQUEST, message);
        }
        match count.build_query_scalar::<i64>().fetch_one(&pool).await {
            Ok(total) => total,
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        }
    } else {
        0
    };
    let limit = if query.limit <= 0 {
        100
    } else {
        query.limit.min(1_000)
    };
    let offset = query.offset.max(0);
    let selected_kinds = selected
        .iter()
        .map(|column| (column.clone(), all_kinds[column.as_str()]))
        .collect::<Vec<_>>();
    let mut rows = QueryBuilder::<Sqlite>::new("SELECT ");
    rows.push(json_object_expr("t", &selected_kinds))
        .push(" AS row FROM ")
        .push(&quoted_table)
        .push(" t");
    if let Err(message) = append_where(&mut rows, &all_kinds, &query.search, &filters) {
        return error(StatusCode::BAD_REQUEST, message);
    }
    let sort = if query.sort.trim().is_empty() {
        if all_set.contains("created_at") {
            "created_at"
        } else if all_set.contains("id") {
            "id"
        } else {
            &all[0]
        }
    } else {
        query.sort.trim()
    };
    if !all_set.contains(sort) || !valid_ident(sort) {
        return error(StatusCode::BAD_REQUEST, "invalid sort column");
    }
    let direction = if query.direction.eq_ignore_ascii_case("desc")
        || (query.sort.is_empty() && sort == "created_at")
    {
        " DESC"
    } else {
        " ASC"
    };
    rows.push(" ORDER BY t.")
        .push(quote_ident(sort).expect("validated sort"))
        .push(direction)
        .push(" LIMIT ")
        .push_bind(limit)
        .push(" OFFSET ")
        .push_bind(offset);
    let result = match rows.build().fetch_all(&pool).await {
        Ok(rows) => rows
            .into_iter()
            .filter_map(|row| {
                row.try_get::<SqlJson<Value>, _>("row")
                    .ok()
                    .map(|value| value.0)
            })
            .collect::<Vec<_>>(),
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    let inferred_total = if exact_total {
        total
    } else {
        offset
            + result.len() as i64
            + if result.len() as i64 == limit {
                limit
            } else {
                0
            }
    };
    Json(json!({"table":table,"columns":selected,"rows":result,"total":inferred_total,"offset":offset,"limit":limit})).into_response()
}

async fn insert_row(
    State(runtime): State<Runtime>,
    Path(table): Path<String>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
    Json(payload): Json<Value>,
) -> Response {
    let project = project(&headers, &query);
    if let Err(response) = authorize_write(&runtime, &headers, &project).await {
        return response;
    }
    if internal_table(&table) {
        return error(StatusCode::NOT_FOUND, "table not found");
    }
    if !valid_ident(&table) {
        return error(StatusCode::BAD_REQUEST, "invalid table name");
    }
    let Some(values) = payload.as_object() else {
        return error(StatusCode::BAD_REQUEST, "row must be an object");
    };
    if values.is_empty() {
        return error(StatusCode::BAD_REQUEST, "no values provided");
    }
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let mut transaction = match data_writer(&runtime, &database_url).await {
        Ok(transaction) => transaction,
        Err(response) => return response,
    };
    let all = match table_columns(&mut **transaction.transaction(), &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        _ => return error(StatusCode::BAD_REQUEST, "table does not exist"),
    };
    let kinds: BTreeMap<&str, JsonKind> = all
        .iter()
        .map(|column| (column.name.as_str(), column.kind()))
        .collect();
    let mut names = values
        .keys()
        .filter(|name| kinds.contains_key(name.as_str()) && valid_ident(name))
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    if names.len() != values.len() || names.is_empty() {
        return error(StatusCode::BAD_REQUEST, "invalid column name");
    }
    let quoted_table = quote_ident(&table).expect("validated table");
    let quoted_names = names
        .iter()
        .map(|name| quote_ident(name).expect("validated column"))
        .collect::<Vec<_>>();
    let inputs = names
        .iter()
        .map(|name| json_input_expr(name, kinds[name.as_str()]))
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {quoted_table} ({}) VALUES ({}) RETURNING {} AS row",
        quoted_names.join(","),
        inputs.join(","),
        json_object_expr("", &column_kinds(&all))
    );
    let result = sqlx::query(&sql)
        .bind(payload.to_string())
        .fetch_optional(&mut **transaction.transaction())
        .await;
    let row = match result {
        Ok(Some(row)) => match row.try_get::<SqlJson<Value>, _>("row") {
            Ok(row) => row.0,
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        },
        Ok(None) => return error(StatusCode::BAD_REQUEST, "insert did not return a row"),
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    if let Err(cause) = transaction.commit().await {
        return error(StatusCode::BAD_REQUEST, cause.to_string());
    }
    (StatusCode::CREATED, Json(json!({"table":table,"row":row}))).into_response()
}

async fn update_row(
    State(runtime): State<Runtime>,
    Path((table, row_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
    Json(payload): Json<Value>,
) -> Response {
    let project = project(&headers, &query);
    if let Err(response) = authorize_write(&runtime, &headers, &project).await {
        return response;
    }
    if internal_table(&table) {
        return error(StatusCode::NOT_FOUND, "table not found");
    }
    if !valid_ident(&table) {
        return error(StatusCode::BAD_REQUEST, "invalid table name");
    }
    let Some(values) = payload.as_object() else {
        return error(StatusCode::BAD_REQUEST, "row patch must be an object");
    };
    if values.is_empty() {
        return error(StatusCode::BAD_REQUEST, "at least one value is required");
    }
    if values
        .keys()
        .any(|column| matches!(column.as_str(), "_id" | "id" | "tenantId" | "tenant_id"))
    {
        return error(
            StatusCode::BAD_REQUEST,
            "identity and tenant columns cannot be changed",
        );
    }
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let mut transaction = match data_writer(&runtime, &database_url).await {
        Ok(transaction) => transaction,
        Err(response) => return response,
    };
    let all = match table_columns(&mut **transaction.transaction(), &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        _ => return error(StatusCode::BAD_REQUEST, "table does not exist"),
    };
    let kinds: BTreeMap<&str, JsonKind> = all
        .iter()
        .map(|column| (column.name.as_str(), column.kind()))
        .collect();
    let allowed: BTreeSet<&str> = kinds.keys().copied().collect();
    let id_column = if allowed.contains("_id") {
        "_id"
    } else if allowed.contains("id") {
        "id"
    } else {
        return error(
            StatusCode::BAD_REQUEST,
            "table has no supported identity column",
        );
    };
    let mut names = values
        .keys()
        .filter(|name| allowed.contains(name.as_str()) && valid_ident(name))
        .cloned()
        .collect::<Vec<_>>();
    names.sort();
    if names.len() != values.len() {
        return error(StatusCode::BAD_REQUEST, "invalid column name");
    }
    let quoted_table = quote_ident(&table).expect("validated table");
    let assignments = names
        .iter()
        .map(|name| {
            let quoted = quote_ident(name).expect("validated column");
            format!("{quoted}={}", json_input_expr(name, kinds[name.as_str()]))
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "UPDATE {quoted_table} SET {} WHERE CAST({} AS TEXT)=$2 RETURNING {} AS row",
        assignments.join(","),
        quote_ident(id_column).expect("validated id"),
        json_object_expr("", &column_kinds(&all))
    );
    changed_row(transaction, &table, &sql, Some(payload), &row_id, "update").await
}

async fn delete_row(
    State(runtime): State<Runtime>,
    Path((table, row_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
) -> Response {
    let project = project(&headers, &query);
    if !operations::admin_key_matches(&runtime, &headers) {
        return error(StatusCode::FORBIDDEN, "runtime admin key is required");
    }
    if project.is_empty() || internal_table(&table) {
        return error(StatusCode::NOT_FOUND, "table not found");
    }
    if !valid_ident(&table) {
        return error(StatusCode::BAD_REQUEST, "invalid table name");
    }
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let mut transaction = match data_writer(&runtime, &database_url).await {
        Ok(transaction) => transaction,
        Err(response) => return response,
    };
    let all = match table_columns(&mut **transaction.transaction(), &table).await {
        Ok(columns) => columns,
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    let id_column = if all.iter().any(|column| column.name == "_id") {
        "_id"
    } else if all.iter().any(|column| column.name == "id") {
        "id"
    } else {
        return error(
            StatusCode::BAD_REQUEST,
            "table has no supported identity column",
        );
    };
    let quoted_table = quote_ident(&table).expect("validated table");
    let sql = format!(
        "DELETE FROM {quoted_table} WHERE CAST({} AS TEXT)=$2 RETURNING {} AS row",
        quote_ident(id_column).expect("validated id"),
        json_object_expr("", &column_kinds(&all))
    );
    changed_row(transaction, &table, &sql, None, &row_id, "delete").await
}

/// Runs one row write (`$1` = JSON patch when present, `$2` = row id) and
/// commits it, returning the changed row as JSON.
async fn changed_row(
    mut transaction: TenantTransaction,
    table: &str,
    sql: &str,
    payload: Option<Value>,
    row_id: &str,
    action: &str,
) -> Response {
    let mut statement = sqlx::query(sql);
    statement = match payload {
        Some(payload) => statement.bind(payload.to_string()),
        None => statement.bind(Option::<String>::None),
    };
    let result = statement
        .bind(row_id)
        .fetch_optional(&mut **transaction.transaction())
        .await;
    let row = match result {
        Ok(Some(row)) => match row.try_get::<SqlJson<Value>, _>("row") {
            Ok(row) => row.0,
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        },
        Ok(None) => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("{action} did not find a row"),
            )
        }
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    if let Err(cause) = transaction.commit().await {
        return error(StatusCode::BAD_REQUEST, cause.to_string());
    }
    Json(json!({"table":table,"row":row})).into_response()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReplaceRequest {
    replacements: BTreeMap<String, String>,
    #[serde(default)]
    dry_run: bool,
}

async fn replace_references(
    State(runtime): State<Runtime>,
    headers: HeaderMap,
    Query(query): Query<DataQuery>,
    Json(request): Json<ReplaceRequest>,
) -> Response {
    if !operations::admin_key_matches(&runtime, &headers) {
        return error(StatusCode::FORBIDDEN, "runtime admin key is required");
    }
    if request.replacements.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "at least one replacement is required",
        );
    }
    if request.replacements.len() > 10_000 {
        return error(StatusCode::BAD_REQUEST, "too many replacements");
    }
    if request
        .replacements
        .iter()
        .any(|(source, replacement)| source.trim().is_empty() || replacement.trim().is_empty())
    {
        return error(StatusCode::BAD_REQUEST, "replacement ids cannot be empty");
    }
    let replacements = request
        .replacements
        .iter()
        .filter(|(source, replacement)| source.trim() != replacement.trim())
        .collect::<Vec<_>>();
    if replacements.is_empty() {
        return error(
            StatusCode::BAD_REQUEST,
            "at least one replacement is required",
        );
    }
    let project = project(&headers, &query);
    let database_url = match data_database(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(database_url) => database_url,
        Err(response) => return response,
    };
    let mut transaction = match data_writer(&runtime, &database_url).await {
        Ok(transaction) => transaction,
        Err(response) => return response,
    };
    let map: BTreeMap<String, String> = replacements
        .iter()
        .map(|(source, replacement)| (source.trim().to_owned(), replacement.trim().to_owned()))
        .collect();
    match replace_in_transaction(&mut transaction, &map, request.dry_run).await {
        Ok((text_rows, json_rows, changed)) => {
            if let Err(cause) = transaction.commit().await {
                return error(StatusCode::BAD_REQUEST, cause.to_string());
            }
            Json(json!({"textRows":text_rows,"jsonRows":json_rows,"columns":changed,"dryRun":request.dry_run})).into_response()
        }
        Err(cause) => error(StatusCode::BAD_REQUEST, cause),
    }
}

const REPLACEMENTS_TABLE: &str = "temp.\"_gonvex_reference_replacements\"";

async fn replace_in_transaction(
    transaction: &mut TenantTransaction,
    replacements: &BTreeMap<String, String>,
    dry_run: bool,
) -> Result<(i64, i64, Vec<Value>), String> {
    let connection = &mut **transaction.transaction();
    let text = |cause: sqlx::Error| cause.to_string();
    // DDL is transactional in SQLite, so a failed request leaves no temp table;
    // a successful one drops it before commit.
    sqlx::query(&format!("DROP TABLE IF EXISTS {REPLACEMENTS_TABLE}"))
        .execute(&mut *connection)
        .await
        .map_err(text)?;
    sqlx::query(&format!(
        "CREATE TEMP TABLE \"_gonvex_reference_replacements\" (source_id TEXT PRIMARY KEY, replacement_id TEXT NOT NULL)"
    ))
    .execute(&mut *connection)
    .await
    .map_err(text)?;
    for (source, replacement) in replacements {
        sqlx::query(&format!(
            "INSERT INTO {REPLACEMENTS_TABLE}(source_id,replacement_id) VALUES($1,$2)"
        ))
        .bind(source)
        .bind(replacement)
        .execute(&mut *connection)
        .await
        .map_err(text)?;
    }
    let column_rows = sqlx::query(
        r#"SELECT m.name AS table_name, c.name AS column_name, lower(c.type) AS data_type
           FROM sqlite_schema m JOIN pragma_table_xinfo(m.name) c
           WHERE m.type='table' AND m.name<>'members'
             AND m.name NOT LIKE 'sqlite\_%' ESCAPE '\'
             AND m.name NOT LIKE '\_gonvex\_%' ESCAPE '\'
             AND m.name NOT LIKE 'gonvex\_%' ESCAPE '\'
             AND c.hidden=0
           ORDER BY m.name, c.cid"#,
    )
    .fetch_all(&mut *connection)
    .await
    .map_err(text)?;
    let mut text_rows = 0_i64;
    let mut json_rows = 0_i64;
    let mut changed = Vec::new();
    for row in column_rows {
        let table: String = row.get("table_name");
        let column: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        let kind = JsonKind::from_declared(&data_type);
        let is_json = kind == JsonKind::Json;
        let is_text = !is_json
            && (data_type.contains("text")
                || data_type.contains("char")
                || data_type.contains("clob"));
        if !(is_json || is_text)
            || matches!(column.as_str(), "_id" | "id" | "tenantId" | "tenant_id")
        {
            continue;
        }
        let (Ok(table_ident), Ok(column_ident)) = (quote_ident(&table), quote_ident(&column))
        else {
            continue;
        };
        let affected = if is_json {
            // A JSON value can hold an id only as a JSON string, so a cheap
            // substring test on the quoted id selects candidate rows.
            let candidates = format!(
                "SELECT rowid AS gonvex_rowid, {column_ident} AS value FROM {table_ident} \
                 WHERE {column_ident} IS NOT NULL AND EXISTS (SELECT 1 FROM {REPLACEMENTS_TABLE} replacements \
                 WHERE instr(CAST({column_ident} AS TEXT), json_quote(replacements.source_id))>0)"
            );
            let rows = sqlx::query(&candidates)
                .fetch_all(&mut *connection)
                .await
                .map_err(text)?;
            if !dry_run {
                let update = format!("UPDATE {table_ident} SET {column_ident}=$1 WHERE rowid=$2");
                for candidate in &rows {
                    let raw: String = candidate.try_get("value").map_err(text)?;
                    let Ok(value) = serde_json::from_str::<Value>(&raw) else {
                        continue;
                    };
                    let next = replace_json_references(value, replacements);
                    sqlx::query(&update)
                        .bind(next.to_string())
                        .bind(candidate.get::<i64, _>("gonvex_rowid"))
                        .execute(&mut *connection)
                        .await
                        .map_err(text)?;
                }
            }
            rows.len() as i64
        } else if dry_run {
            sqlx::query_scalar::<_, i64>(&format!(
                "SELECT count(*) FROM {table_ident} target JOIN {REPLACEMENTS_TABLE} replacements \
                 ON target.{column_ident}=replacements.source_id"
            ))
            .fetch_one(&mut *connection)
            .await
            .map_err(text)?
        } else {
            sqlx::query(&format!(
                "UPDATE {table_ident} SET {column_ident}=replacements.replacement_id \
                 FROM {REPLACEMENTS_TABLE} replacements \
                 WHERE {table_ident}.{column_ident}=replacements.source_id"
            ))
            .execute(&mut *connection)
            .await
            .map(|result| result.rows_affected() as i64)
            .map_err(text)?
        };
        if is_json {
            json_rows += affected;
        } else {
            text_rows += affected;
        }
        if affected > 0 {
            changed
                .push(json!({"table":table,"column":column,"dataType":data_type,"rows":affected}));
        }
    }
    sqlx::query(&format!("DROP TABLE {REPLACEMENTS_TABLE}"))
        .execute(&mut *connection)
        .await
        .map_err(text)?;
    Ok((text_rows, json_rows, changed))
}

/// Replaces every JSON string equal to a source id (replaces the former
/// `pg_temp.gonvex_replace_references` plpgsql function).
fn replace_json_references(value: Value, replacements: &BTreeMap<String, String>) -> Value {
    match value {
        Value::String(text) => match replacements.get(&text) {
            Some(replacement) => Value::String(replacement.clone()),
            None => Value::String(text),
        },
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| replace_json_references(item, replacements))
                .collect(),
        ),
        Value::Object(object) => Value::Object(
            object
                .into_iter()
                .map(|(key, item)| (key, replace_json_references(item, replacements)))
                .collect(),
        ),
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifiers_and_internal_tables_fail_closed() {
        assert!(valid_ident("task_rows"));
        assert!(!valid_ident("task-rows"));
        assert!(internal_table("gonvex_runtime_projects"));
        assert!(internal_table("_gonvex_files"));
        assert!(internal_table("members"));
        assert!(!internal_table("tasks"));
    }

    #[test]
    fn json_references_replace_exact_strings_only() {
        let replacements = BTreeMap::from([("old".to_owned(), "new".to_owned())]);
        let value = json!({"a":"old","b":["old","older",{"c":"old"}],"d":1,"old":"keep-key"});
        assert_eq!(
            replace_json_references(value, &replacements),
            json!({"a":"new","b":["new","older",{"c":"new"}],"d":1,"old":"keep-key"})
        );
    }
}
