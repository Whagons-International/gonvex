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
use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::types::Json as SqlJson;
use sqlx::{PgPool, Postgres, QueryBuilder, Row};

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

async fn admit(runtime: &Runtime) -> Result<tokio::sync::OwnedSemaphorePermit, Response> {
    runtime
        .inner
        .pools
        .admit()
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

async fn data_pool(
    runtime: &Runtime,
    project: &str,
    tenant: &str,
) -> Result<Option<PgPool>, Response> {
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
        return runtime
            .inner
            .pools
            .pool(database_url)
            .await
            .map(Some)
            .map_err(|cause| error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()));
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
    runtime
        .inner
        .pools
        .pool(&route.database_url)
        .await
        .map(Some)
        .map_err(|cause| error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()))
}

async fn columns(pool: &PgPool, table: &str) -> Result<Vec<String>, sqlx::Error> {
    sqlx::query_scalar::<_, String>(
        "SELECT column_name FROM information_schema.columns WHERE table_schema=current_schema() AND table_name=$1 ORDER BY ordinal_position",
    )
    .bind(table)
    .fetch_all(pool)
    .await
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => return Json(json!({"tables":[]})).into_response(),
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let names = match sqlx::query_scalar::<_, String>(
        "SELECT table_name FROM information_schema.tables WHERE table_schema=current_schema() AND table_type='BASE TABLE' ORDER BY table_name",
    )
    .fetch_all(&pool)
    .await
    {
        Ok(names) => names,
        Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
    };
    let mut tables = Vec::new();
    for name in names.into_iter().filter(|name| !internal_table(name)) {
        let table_columns = match columns(&pool, &name).await {
            Ok(columns) => columns,
            Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
        };
        let quoted = match quote_ident(&name) {
            Ok(quoted) => quoted,
            Err(message) => return error(StatusCode::BAD_REQUEST, message),
        };
        let row_count =
            match sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {quoted}"))
                .fetch_one(&pool)
                .await
            {
                Ok(count) => count,
                Err(cause) => return error(StatusCode::SERVICE_UNAVAILABLE, cause.to_string()),
            };
        tables.push(json!({"name":name,"columns":table_columns,"rowCount":row_count}));
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
    builder: &mut QueryBuilder<'a, Postgres>,
    all: &BTreeSet<&str>,
    search: &'a str,
    filters: &'a [RowsFilter],
) -> Result<(), &'static str> {
    let mut has_clause = false;
    let prefix = |builder: &mut QueryBuilder<'a, Postgres>, has_clause: &mut bool| {
        builder.push(if *has_clause { " AND " } else { " WHERE " });
        *has_clause = true;
    };
    if !search.trim().is_empty() && !all.is_empty() {
        prefix(builder, &mut has_clause);
        builder.push('(');
        for (index, column) in all.iter().enumerate() {
            if index > 0 {
                builder.push(" OR ");
            }
            builder
                .push("COALESCE(")
                .push(quote_ident(column)?)
                .push("::text,'') ILIKE ")
                .push_bind(format!("%{}%", search.trim()));
        }
        builder.push(')');
    }
    for filter in filters
        .iter()
        .filter(|filter| !filter.column.trim().is_empty())
    {
        let column = filter.column.trim();
        if !all.contains(column) || !valid_ident(column) {
            return Err("invalid filter column");
        }
        prefix(builder, &mut has_clause);
        let quoted = quote_ident(column)?;
        match filter.operator.as_str() {
            "empty" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NULL OR ")
                    .push(&quoted)
                    .push("::text='')");
            }
            "notEmpty" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NOT NULL AND ")
                    .push(&quoted)
                    .push("::text<>'')");
            }
            "equals" => {
                builder
                    .push(&quoted)
                    .push("::text=")
                    .push_bind(&filter.value);
            }
            "notEquals" => {
                builder
                    .push("(")
                    .push(&quoted)
                    .push(" IS NULL OR ")
                    .push(&quoted)
                    .push("::text<>")
                    .push_bind(&filter.value)
                    .push(')');
            }
            "startsWith" => {
                builder
                    .push("COALESCE(")
                    .push(&quoted)
                    .push("::text,'') ILIKE ")
                    .push_bind(format!("{}%", filter.value));
            }
            "endsWith" => {
                builder
                    .push("COALESCE(")
                    .push(&quoted)
                    .push("::text,'') ILIKE ")
                    .push_bind(format!("%{}", filter.value));
            }
            "notContains" => {
                builder
                    .push("COALESCE(")
                    .push(&quoted)
                    .push("::text,'') NOT ILIKE ")
                    .push_bind(format!("%{}%", filter.value));
            }
            "lessThan" | "lessThanOrEqual" | "greaterThan" | "greaterThanOrEqual" => {
                let operator = match filter.operator.as_str() {
                    "lessThan" => "<",
                    "lessThanOrEqual" => "<=",
                    "greaterThan" => ">",
                    _ => ">=",
                };
                builder
                    .push(&quoted)
                    .push("::text ")
                    .push(operator)
                    .push(' ')
                    .push_bind(&filter.value);
            }
            "inRange" => {
                builder.push('(').push(&quoted).push(" IS NOT NULL");
                if !filter.value.trim().is_empty() {
                    builder
                        .push(" AND ")
                        .push(&quoted)
                        .push("::text >= ")
                        .push_bind(&filter.value);
                }
                if !filter.value_to.trim().is_empty() {
                    builder
                        .push(" AND ")
                        .push(&quoted)
                        .push("::text <= ")
                        .push_bind(&filter.value_to);
                }
                builder.push(')');
            }
            "oneOf" => {
                let values: Vec<String> =
                    serde_json::from_str(&filter.value).map_err(|_| "invalid oneOf filter")?;
                if values.is_empty() {
                    builder.push("TRUE");
                } else {
                    builder
                        .push("COALESCE(")
                        .push(&quoted)
                        .push("::text,'') IN (");
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
                    .push(&quoted)
                    .push("::text,'') ILIKE ")
                    .push_bind(format!("%{}%", filter.value));
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => {
            return Json(
                json!({"table":table,"columns":[],"rows":[],"total":0,"offset":0,"limit":100}),
            )
            .into_response()
        }
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let all = match columns(&pool, &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        Ok(_) => return error(StatusCode::BAD_REQUEST, "table does not exist"),
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
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
    let quoted_table = quote_ident(&table).expect("validated identifier");
    let exact_total = query.count != "false" && query.count != "estimate";
    let total = if exact_total {
        let mut count =
            QueryBuilder::<Postgres>::new(format!("SELECT count(*) FROM {quoted_table}"));
        if let Err(message) = append_where(&mut count, &all_set, &query.search, &filters) {
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
    let mut rows = QueryBuilder::<Postgres>::new("SELECT jsonb_build_object(");
    for (index, column) in selected.iter().enumerate() {
        if index > 0 {
            rows.push(',');
        }
        rows.push_bind(column)
            .push(",t.")
            .push(quote_ident(column).expect("validated column"));
    }
    rows.push(") AS row FROM ").push(&quoted_table).push(" t");
    if let Err(message) = append_where(&mut rows, &all_set, &query.search, &filters) {
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => return error(StatusCode::BAD_REQUEST, "database URL is not configured"),
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let all = match columns(&pool, &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        _ => return error(StatusCode::BAD_REQUEST, "table does not exist"),
    };
    let allowed: BTreeSet<&str> = all.iter().map(String::as_str).collect();
    let mut names = values
        .keys()
        .filter(|name| allowed.contains(name.as_str()))
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
    let select_names = quoted_names
        .iter()
        .map(|name| format!("r.{name}"))
        .collect::<Vec<_>>();
    let sql = format!(
        "INSERT INTO {quoted_table} ({}) SELECT {} FROM jsonb_populate_record(NULL::{quoted_table},$1::jsonb) r RETURNING to_jsonb({quoted_table}.*) AS row",
        quoted_names.join(","), select_names.join(",")
    );
    match sqlx::query(&sql)
        .bind(SqlJson(payload))
        .fetch_optional(&pool)
        .await
    {
        Ok(Some(row)) => match row.try_get::<SqlJson<Value>, _>("row") {
            Ok(row) => (
                StatusCode::CREATED,
                Json(json!({"table":table,"row":row.0})),
            )
                .into_response(),
            Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
        },
        Ok(None) => error(StatusCode::BAD_REQUEST, "insert did not return a row"),
        Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
    }
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => return error(StatusCode::BAD_REQUEST, "database URL is not configured"),
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let all = match columns(&pool, &table).await {
        Ok(columns) if !columns.is_empty() => columns,
        _ => return error(StatusCode::BAD_REQUEST, "table does not exist"),
    };
    let allowed: BTreeSet<&str> = all.iter().map(String::as_str).collect();
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
        .filter(|name| allowed.contains(name.as_str()))
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
            format!("{quoted}=r.{quoted}")
        })
        .collect::<Vec<_>>();
    let sql = format!(
        "UPDATE {quoted_table} t SET {} FROM jsonb_populate_record(NULL::{quoted_table},$1::jsonb) r WHERE t.{}::text=$2 RETURNING to_jsonb(t.*) AS row",
        assignments.join(","), quote_ident(id_column).expect("validated id")
    );
    changed_row(&pool, &table, &sql, payload, &row_id, "update").await
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => return error(StatusCode::BAD_REQUEST, "database URL is not configured"),
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let all = match columns(&pool, &table).await {
        Ok(columns) => columns,
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    let id_column = if all.iter().any(|column| column == "_id") {
        "_id"
    } else if all.iter().any(|column| column == "id") {
        "id"
    } else {
        return error(
            StatusCode::BAD_REQUEST,
            "table has no supported identity column",
        );
    };
    let quoted_table = quote_ident(&table).expect("validated table");
    let sql = format!(
        "DELETE FROM {quoted_table} WHERE {}::text=$1 RETURNING to_jsonb({quoted_table}.*) AS row",
        quote_ident(id_column).expect("validated id")
    );
    match sqlx::query(&sql).bind(&row_id).fetch_optional(&pool).await {
        Ok(Some(row)) => match row.try_get::<SqlJson<Value>, _>("row") {
            Ok(row) => Json(json!({"table":table,"row":row.0})).into_response(),
            Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
        },
        Ok(None) => error(StatusCode::BAD_REQUEST, "delete did not find a row"),
        Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
    }
}

async fn changed_row(
    pool: &PgPool,
    table: &str,
    sql: &str,
    payload: Value,
    row_id: &str,
    action: &str,
) -> Response {
    match sqlx::query(sql)
        .bind(SqlJson(payload))
        .bind(row_id)
        .fetch_optional(pool)
        .await
    {
        Ok(Some(row)) => match row.try_get::<SqlJson<Value>, _>("row") {
            Ok(row) => Json(json!({"table":table,"row":row.0})).into_response(),
            Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
        },
        Ok(None) => error(
            StatusCode::BAD_REQUEST,
            format!("{action} did not find a row"),
        ),
        Err(cause) => error(StatusCode::BAD_REQUEST, cause.to_string()),
    }
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
    let pool = match data_pool(&runtime, &project, &tenant(&headers, &query)).await {
        Ok(Some(pool)) => pool,
        Ok(None) => return error(StatusCode::BAD_REQUEST, "database URL is not configured"),
        Err(response) => return response,
    };
    let _admission = match admit(&runtime).await {
        Ok(admission) => admission,
        Err(response) => return response,
    };
    let mut transaction = match pool.begin().await {
        Ok(transaction) => transaction,
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    if let Err(cause) = sqlx::query("CREATE TEMP TABLE \"_gonvex_reference_replacements\" (source_id text PRIMARY KEY,replacement_id text NOT NULL) ON COMMIT DROP").execute(&mut *transaction).await {
        return error(StatusCode::BAD_REQUEST, cause.to_string());
    }
    for (source, replacement) in replacements {
        if let Err(cause) = sqlx::query("INSERT INTO \"_gonvex_reference_replacements\"(source_id,replacement_id) VALUES($1,$2)").bind(source.trim()).bind(replacement.trim()).execute(&mut *transaction).await {
            return error(StatusCode::BAD_REQUEST, cause.to_string());
        }
    }
    let function = r#"CREATE OR REPLACE FUNCTION pg_temp.gonvex_replace_references(value jsonb) RETURNS jsonb LANGUAGE plpgsql STABLE AS $$
DECLARE kind text; scalar text; mapped text; next_value jsonb;
BEGIN
 IF value IS NULL THEN RETURN NULL; END IF; kind:=jsonb_typeof(value);
 IF kind='string' THEN scalar:=value #>> '{}'; SELECT replacement_id INTO mapped FROM "_gonvex_reference_replacements" WHERE source_id=scalar; RETURN CASE WHEN mapped IS NULL THEN value ELSE to_jsonb(mapped) END;
 ELSIF kind='array' THEN SELECT COALESCE(jsonb_agg(pg_temp.gonvex_replace_references(item.value) ORDER BY item.ordinality),'[]'::jsonb) INTO next_value FROM jsonb_array_elements(value) WITH ORDINALITY AS item(value,ordinality); RETURN next_value;
 ELSIF kind='object' THEN SELECT COALESCE(jsonb_object_agg(item.key,pg_temp.gonvex_replace_references(item.value)),'{}'::jsonb) INTO next_value FROM jsonb_each(value) AS item(key,value); RETURN next_value;
 END IF; RETURN value;
END $$"#;
    if let Err(cause) = sqlx::query(function).execute(&mut *transaction).await {
        return error(StatusCode::BAD_REQUEST, cause.to_string());
    }
    let column_rows = match sqlx::query("SELECT table_name,column_name,data_type FROM information_schema.columns WHERE table_schema=current_schema() AND table_name<>'members' AND table_name NOT LIKE '\\_gonvex\\_%' ESCAPE '\\' AND table_name NOT LIKE 'gonvex\\_%' ESCAPE '\\' AND is_generated='NEVER' AND data_type IN ('text','character varying','character','json','jsonb') ORDER BY table_name,ordinal_position").fetch_all(&mut *transaction).await {
        Ok(rows) => rows,
        Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
    };
    let mut text_rows = 0_i64;
    let mut json_rows = 0_i64;
    let mut changed = Vec::new();
    for row in column_rows {
        let table: String = row.get("table_name");
        let column: String = row.get("column_name");
        let data_type: String = row.get("data_type");
        if matches!(column.as_str(), "_id" | "id" | "tenantId" | "tenant_id") {
            continue;
        }
        let table_ident = quote_ident(&table).expect("database identifier");
        let column_ident = quote_ident(&column).expect("database identifier");
        let affected = if matches!(data_type.as_str(), "json" | "jsonb") {
            let condition = format!("{column_ident} IS NOT NULL AND EXISTS (SELECT 1 FROM \"_gonvex_reference_replacements\" replacements WHERE {column_ident}::text LIKE '%'||to_jsonb(replacements.source_id)::text||'%')");
            if request.dry_run {
                sqlx::query_scalar::<_, i64>(&format!(
                    "SELECT count(*) FROM {table_ident} WHERE {condition}"
                ))
                .fetch_one(&mut *transaction)
                .await
            } else {
                let value = if data_type == "json" {
                    format!("pg_temp.gonvex_replace_references({column_ident}::jsonb)::json")
                } else {
                    format!("pg_temp.gonvex_replace_references({column_ident}::jsonb)")
                };
                sqlx::query(&format!(
                    "UPDATE {table_ident} SET {column_ident}={value} WHERE {condition}"
                ))
                .execute(&mut *transaction)
                .await
                .map(|result| result.rows_affected() as i64)
            }
        } else if request.dry_run {
            sqlx::query_scalar::<_, i64>(&format!("SELECT count(*) FROM {table_ident} target JOIN \"_gonvex_reference_replacements\" replacements ON target.{column_ident}=replacements.source_id")).fetch_one(&mut *transaction).await
        } else {
            sqlx::query(&format!("UPDATE {table_ident} target SET {column_ident}=replacements.replacement_id FROM \"_gonvex_reference_replacements\" replacements WHERE target.{column_ident}=replacements.source_id")).execute(&mut *transaction).await.map(|result| result.rows_affected() as i64)
        };
        let affected = match affected {
            Ok(affected) => affected,
            Err(cause) => return error(StatusCode::BAD_REQUEST, cause.to_string()),
        };
        if matches!(data_type.as_str(), "json" | "jsonb") {
            json_rows += affected;
        } else {
            text_rows += affected;
        }
        if affected > 0 {
            changed
                .push(json!({"table":table,"column":column,"dataType":data_type,"rows":affected}));
        }
    }
    if let Err(cause) = transaction.commit().await {
        return error(StatusCode::BAD_REQUEST, cause.to_string());
    }
    Json(json!({"textRows":text_rows,"jsonRows":json_rows,"columns":changed,"dryRun":request.dry_run})).into_response()
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
}
