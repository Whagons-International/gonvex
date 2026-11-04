//! All-plans parity oracle for the Whagons v1 → v2 visibility migration.
//!
//! Inputs (`perf_fixture/extract_whagons_visibility.py`): the 340 legacy plans
//! Whagons staging deployed, the 340 version 2 masks that replace them, and
//! the tenant schema of every table either set names. A real store (Control
//! Plane, provisioned tenant file) is seeded with random rows whose columns
//! draw from a small shared id pool and from every literal the plans compare
//! with, so joins, grants, inheritance and flags hit often. Members get random
//! roles and permissions drawn from the plans.
//!
//! For every plan, member and seed:
//!
//! - **v1 SQL**: `visibility::resolve` (legacy sets materialized in SQL) and the
//!   snapshot query (`replica::query_rows`);
//! - **v1 interpreted**: `row_matches` on every row of the table;
//! - **v2 SQL**: parent ACL indexes (`ParentAclRegistry::ensure`),
//!   `resolve_with_parents`, the snapshot query;
//! - **v2 compiled**: `visibility_class::member_visibility` on every row
//!   (plans the compiler does not support fall back to `row_matches` and are
//!   counted);
//!
//! must admit exactly the same row keys. Every disagreement is collected and
//! reported per table, then fails the test.
//!
//!   cargo test -p gonvex-runtime --features perf-fixture --lib visibility_whagons_parity -- --nocapture
//!   WHAGONS_PARITY_SEEDS=8 WHAGONS_PARITY_ROWS=40 WHAGONS_PARITY_TABLES=tasks,taskNotes ...
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use gonvex_store::{
    ControlPlane, MigrationScope, PoolLimits, PoolRegistry, SqlMigration, TenantRoute,
    TenantSession,
};
use serde_json::{json, Value};

use crate::modules::ReplicaCollectionDefinition;
use crate::perf_fixture::data::Rng;
use crate::visibility::{self, ParentAclRegistry, VisibilityPlan};
use crate::visibility_class::{member_visibility, MemberHandle, RowView};

const PLANS: &str = include_str!("perf_fixture/whagons_visibility_all.json");
const SCHEMA: &str = include_str!("perf_fixture/whagons_schema_all.sql");
const MEMBERS: usize = 10;

fn env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn load(plans: &Value) -> BTreeMap<String, VisibilityPlan> {
    plans
        .as_object()
        .unwrap()
        .iter()
        .map(|(table, plan)| {
            let plan: VisibilityPlan = serde_json::from_value(plan.clone()).unwrap();
            plan.validate()
                .unwrap_or_else(|error| panic!("{table}: {error}"));
            (table.clone(), plan)
        })
        .collect()
}

/// Every literal a plan compares a column with, every role and permission.
#[derive(Default)]
struct Literals {
    columns: BTreeMap<String, Vec<Value>>,
    /// Every string literal of any column: correlated columns (a row's
    /// `powerupSlug` vs `plugins.slug`) only meet through this pool.
    strings: Vec<Value>,
    roles: BTreeSet<String>,
    permissions: BTreeSet<String>,
}

fn collect_literals(node: &Value, out: &mut Literals) {
    match node {
        Value::Object(map) => {
            match (
                map.get("operator").and_then(Value::as_str),
                map.get("value"),
            ) {
                (Some("role"), Some(Value::String(role))) => {
                    out.roles.insert(role.clone());
                }
                (Some("permission"), Some(Value::String(permission))) => {
                    out.permissions.insert(permission.clone());
                }
                _ => {}
            }
            if let (Some(Value::String(column)), Some(literal)) = (
                map.get("column"),
                map.get("value").and_then(|value| value.get("literal")),
            ) {
                let values = out.columns.entry(column.clone()).or_default();
                if !values.contains(literal) {
                    values.push(literal.clone());
                }
                if literal.is_string() && !out.strings.contains(literal) {
                    out.strings.push(literal.clone());
                }
            }
            for value in map.values() {
                collect_literals(value, out);
            }
        }
        Value::Array(values) => values.iter().for_each(|value| collect_literals(value, out)),
        _ => {}
    }
}

struct Column {
    name: String,
    kind: String,
    not_null: bool,
}

fn id(pool: usize, rng: &mut Rng) -> String {
    format!("k{}", rng.below(pool))
}

/// One random column value: literals of that column, shared ids, nulls.
fn value(column: &Column, literals: &Literals, pool: usize, rng: &mut Rng) -> Value {
    let kind = column.kind.to_ascii_uppercase();
    if let Some(values) = literals.columns.get(&column.name) {
        if rng.chance(0.55) {
            let literal = rng.pick(values).clone();
            if !(literal.is_null() && column.not_null) {
                return match literal {
                    Value::Bool(flag) => json!(flag as i64),
                    other => other,
                };
            }
        }
    }
    if !column.not_null && rng.chance(0.3) {
        return Value::Null;
    }
    match kind.as_str() {
        "BOOLEAN" => json!(rng.below(2) as i64),
        "BIGINT" | "INTEGER" => json!(1_770_000_000_000i64 + rng.below(5) as i64),
        "REAL" => json!(rng.below(3) as f64 + 0.5),
        "JSON" => match rng.below(4) {
            0 => json!("[]"),
            1 => json!("{}"),
            2 => json!(serde_json::to_string(&json!([id(pool, rng), id(pool, rng)])).unwrap()),
            _ => json!(serde_json::to_string(&json!({"key": id(pool, rng)})).unwrap()),
        },
        _ => match rng.below(10) {
            0 => json!(""),
            1 | 2 if !literals.strings.is_empty() => rng.pick(&literals.strings).clone(),
            _ => json!(id(pool, rng)),
        },
    }
}

fn bind<'q>(
    query: sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>>,
    value: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Sqlite, sqlx::sqlite::SqliteArguments<'q>> {
    match value {
        Value::Null => query.bind(None::<String>),
        Value::Bool(flag) => query.bind(*flag as i64),
        Value::Number(number) => match number.as_i64() {
            Some(int) => query.bind(int),
            None => query.bind(number.as_f64().unwrap_or_default()),
        },
        Value::String(text) => query.bind(text.as_str()),
        other => query.bind(other.to_string()),
    }
}

async fn columns(pools: &PoolRegistry, url: &str, table: &str) -> Vec<Column> {
    use sqlx::Row as _;
    sqlx::query("SELECT name, type, \"notnull\" FROM pragma_table_info($1) ORDER BY cid")
        .bind(table)
        .fetch_all(&pools.reader(url).await.unwrap())
        .await
        .unwrap()
        .into_iter()
        .map(|row| Column {
            name: row.get(0),
            kind: row.get(1),
            not_null: row.get::<i64, _>(2) != 0,
        })
        .collect()
}

/// A modified clone must not share the original's compiled/derived caches.
fn shared_mutation(plan: VisibilityPlan) -> VisibilityPlan {
    serde_json::from_value(serde_json::to_value(&plan).unwrap()).unwrap()
}

struct Mismatch {
    seed: u64,
    member: usize,
    role: String,
    what: &'static str,
    only_left: Vec<String>,
    only_right: Vec<String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn whagons_legacy_and_v2_plans_admit_identical_rows() {
    let source: Value = serde_json::from_str(PLANS).unwrap();
    let legacy = load(&source["legacy"]);
    let v2 = load(&source["v2"]);
    assert_eq!(
        legacy.keys().collect::<Vec<_>>(),
        v2.keys().collect::<Vec<_>>()
    );
    crate::visibility::validate_manifest(&v2).unwrap();
    let mut literals = Literals::default();
    collect_literals(&source, &mut literals);
    let only: Option<BTreeSet<String>> = std::env::var("WHAGONS_PARITY_TABLES")
        .ok()
        .map(|tables| tables.split(',').map(str::to_owned).collect());
    let seeds = env("WHAGONS_PARITY_SEEDS", 3) as u64;
    let rows_per_table = env("WHAGONS_PARITY_ROWS", 24);
    let pool = rows_per_table.max(MEMBERS + 2);
    let roles: Vec<String> = ["member", "admin"]
        .into_iter()
        .map(str::to_owned)
        .chain(literals.roles.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let permissions: Vec<String> = literals.permissions.iter().cloned().collect();

    let mut mismatches: BTreeMap<String, Vec<Mismatch>> = BTreeMap::new();
    let mut fallback_tables = BTreeSet::new();
    let mut checked = 0usize;
    let mut visible = [0usize; 2];
    // Per table: (rows seen, rows visible) over every member and seed, and
    // how many (member, seed) cases saw a strict, non-empty subset.
    let mut coverage: BTreeMap<String, (usize, usize, usize)> = BTreeMap::new();
    // Negative control: the tasks mask without its first `or` branch must
    // disagree with the legacy plan somewhere, or the data is too weak.
    let mutations: Vec<VisibilityPlan> = (0..v2["tasks"].predicate.children.len())
        .map(|branch| {
            let mut mutated = v2["tasks"].clone();
            mutated.predicate.children.remove(branch);
            shared_mutation(mutated)
        })
        .collect();
    let mut control_caught = vec![0usize; mutations.len()];
    for seed in 0..seeds {
        let directory = tempfile::tempdir().unwrap();
        let url = gonvex_store::database_url(&directory.path().join("tenant.db"));
        let pools = PoolRegistry::new(PoolLimits {
            acquire_timeout: Duration::from_secs(30),
            ..PoolLimits::default()
        });
        let control = ControlPlane::connect(&url, pools.clone(), BTreeMap::new())
            .await
            .unwrap();
        let route = TenantRoute {
            project_id: "whagons".into(),
            tenant_id: format!("parity-{seed}"),
            database_url: url.clone(),
        };
        control
            .clone()
            .provision_tenant_database(
                route.clone(),
                vec![SqlMigration::new(
                    "0000_whagons_visibility.sql".into(),
                    MigrationScope::Tenant,
                    false,
                    format!("-- gonvex:scope tenant\n{SCHEMA}"),
                )],
            )
            .await
            .unwrap();

        // Seed every table plus the members.
        let mut rng = Rng::new(0x7157_2026 ^ (seed.wrapping_mul(0x9e37_79b9_7f4a_7c15)));
        let mut tables: BTreeSet<String> = SCHEMA
            .lines()
            .filter_map(|line| line.strip_prefix("CREATE TABLE IF NOT EXISTS \""))
            .filter_map(|rest| rest.split('"').next())
            .map(str::to_owned)
            .collect();
        tables.insert("members".into());
        let mut transaction = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        let mut member_roles = Vec::new();
        for table in &tables {
            let columns = columns(&pools, &url, table).await;
            let count = if table == "members" {
                MEMBERS
            } else {
                rows_per_table
            };
            for index in 0..count {
                let mut row = BTreeMap::new();
                for column in &columns {
                    row.insert(
                        column.name.clone(),
                        value(column, &literals, pool, &mut rng),
                    );
                }
                // Keys: k0.. shared by every table (members are k0..k9).
                for key in ["_id", "id"] {
                    if row.contains_key(key) {
                        row.insert(key.to_owned(), json!(format!("k{index}")));
                    }
                }
                if table == "members" {
                    let role = rng.pick(&roles).clone();
                    let mut granted = serde_json::Map::new();
                    for permission in &permissions {
                        if rng.chance(0.3) {
                            granted.insert(permission.clone(), json!(true));
                        }
                    }
                    row.insert("account_id".into(), json!(format!("acct-{index}")));
                    row.insert("status".into(), json!("active"));
                    row.insert("role".into(), json!(role));
                    row.insert(
                        "permissions".into(),
                        json!(Value::Object(granted).to_string()),
                    );
                    member_roles.push(role);
                }
                let names: Vec<&String> = row.keys().collect();
                let statement = format!(
                    "INSERT OR IGNORE INTO \"{table}\" ({}) VALUES ({})",
                    names
                        .iter()
                        .map(|name| format!("\"{name}\""))
                        .collect::<Vec<_>>()
                        .join(","),
                    (1..=names.len())
                        .map(|i| format!("${i}"))
                        .collect::<Vec<_>>()
                        .join(",")
                );
                let mut query = sqlx::query(&statement);
                for value in row.values() {
                    query = bind(query, value);
                }
                query
                    .execute(&mut **transaction.transaction())
                    .await
                    .unwrap_or_else(|error| panic!("seed {table}: {error}"));
            }
        }
        transaction.commit().await.unwrap();

        let feeds = crate::change_feed::ChangeFeedHub::new(pools.clone());
        let registry = ParentAclRegistry::default();
        let metrics = crate::visibility_metrics::VisibilityMetrics::default();
        let clock = crate::change_feed::read_clock(&pools, &route)
            .await
            .unwrap();
        for (table, legacy_plan) in &legacy {
            if only.as_ref().is_some_and(|only| !only.contains(table)) {
                continue;
            }
            let v2_plan = &v2[table];
            let table_columns = columns(&pools, &url, table).await;
            let definition: ReplicaCollectionDefinition = serde_json::from_value(json!({
                "table": table, "key": v2_plan.key,
                "columns": table_columns.iter().map(|c| c.name.clone()).collect::<Vec<_>>(),
            }))
            .unwrap();
            let public: VisibilityPlan = serde_json::from_value(json!({
                "table": table, "key": v2_plan.key, "sets": {}, "where": {"operator": "public"}
            }))
            .unwrap();
            for member in 0..MEMBERS {
                let mut session = TenantSession::anonymous(route.clone());
                session.identity.account.id = format!("acct-{member}");
                session.member.id = format!("k{member}");
                session.member.account_id = format!("acct-{member}");
                session.member.status = "active".into();
                session.member.role = member_roles[member].clone();
                let key_of = |row: &Value| -> String {
                    match &row[&v2_plan.key] {
                        Value::String(text) => text.clone(),
                        other => other.to_string(),
                    }
                };
                let keys = |rows: &[Value]| rows.iter().map(key_of).collect::<BTreeSet<_>>();

                // Legacy: sets in SQL, snapshot SQL, interpreted rows.
                let mut transaction = control
                    .begin_tenant_transaction(&route, true)
                    .await
                    .unwrap();
                let resolved_v1 =
                    visibility::resolve(&mut transaction, &session, legacy_plan, clock.revision)
                        .await
                        .unwrap_or_else(|error| panic!("{table} v1 resolve: {error}"));
                let (rows_v1, _) = crate::replica::query_rows(
                    &mut transaction,
                    &definition,
                    legacy_plan,
                    &resolved_v1,
                    &json!({}),
                )
                .await
                .unwrap_or_else(|error| panic!("{table} v1 snapshot: {error}"));
                let (all, _) = crate::replica::query_rows(
                    &mut transaction,
                    &definition,
                    &public,
                    &resolved_v1,
                    &json!({}),
                )
                .await
                .unwrap();
                transaction.commit().await.unwrap();
                let v1_sql = keys(&rows_v1);
                let v1_rows: BTreeSet<String> = all
                    .iter()
                    .filter(|row| visibility::row_matches(legacy_plan, &resolved_v1, row))
                    .map(key_of)
                    .collect();

                // Version 2: parent indexes, principal/grant/derived sets.
                let prepared = registry
                    .ensure(&feeds, &control, &session, &v2, &[v2_plan])
                    .await
                    .unwrap_or_else(|error| panic!("{table} v2 indexes: {error}"));
                let mut transaction = control
                    .begin_tenant_transaction(&route, true)
                    .await
                    .unwrap();
                let resolved_v2 = visibility::resolve_with_parents(
                    &mut transaction,
                    &session,
                    v2_plan,
                    clock.revision,
                    &v2,
                    &prepared,
                    &metrics,
                )
                .await
                .unwrap_or_else(|error| panic!("{table} v2 resolve: {error}"));
                let (rows_v2, _) = crate::replica::query_rows(
                    &mut transaction,
                    &definition,
                    v2_plan,
                    &resolved_v2,
                    &json!({}),
                )
                .await
                .unwrap_or_else(|error| panic!("{table} v2 snapshot: {error}"));
                transaction.commit().await.unwrap();
                let v2_sql = keys(&rows_v2);
                let handle = member_visibility(v2_plan, &resolved_v2, &url);
                let v2_compiled: BTreeSet<String> = all
                    .iter()
                    .filter(|row| match &handle {
                        MemberHandle::Compiled(compiled) => compiled.visible(RowView::Value(row)),
                        MemberHandle::Fallback => {
                            fallback_tables.insert(table.clone());
                            visibility::row_matches(v2_plan, &resolved_v2, row)
                        }
                    })
                    .map(key_of)
                    .collect();

                for (branch, mutated) in mutations.iter().enumerate() {
                    if table != "tasks" {
                        break;
                    }
                    let mut transaction = control
                        .begin_tenant_transaction(&route, true)
                        .await
                        .unwrap();
                    let resolved = visibility::resolve_with_parents(
                        &mut transaction,
                        &session,
                        mutated,
                        clock.revision,
                        &v2,
                        &prepared,
                        &metrics,
                    )
                    .await
                    .unwrap();
                    let (rows, _) = crate::replica::query_rows(
                        &mut transaction,
                        &definition,
                        mutated,
                        &resolved,
                        &json!({}),
                    )
                    .await
                    .unwrap();
                    transaction.commit().await.unwrap();
                    control_caught[branch] += (keys(&rows) != v1_sql) as usize;
                }
                let entry = coverage.entry(table.clone()).or_default();
                entry.0 += all.len();
                entry.1 += v1_sql.len();
                entry.2 += (!v1_sql.is_empty() && v1_sql.len() < all.len()) as usize;
                checked += all.len();
                visible[0] += v1_sql.len();
                visible[1] += v2_sql.len();
                for (what, left, right) in [
                    ("v1 SQL vs v2 SQL", &v1_sql, &v2_sql),
                    ("v1 SQL vs v1 interpreted", &v1_sql, &v1_rows),
                    ("v2 SQL vs v2 compiled", &v2_sql, &v2_compiled),
                ] {
                    if left != right {
                        mismatches.entry(table.clone()).or_default().push(Mismatch {
                            seed,
                            member,
                            role: member_roles[member].clone(),
                            what,
                            only_left: left.difference(right).cloned().collect(),
                            only_right: right.difference(left).cloned().collect(),
                        });
                    }
                }
            }
        }
        feeds.shutdown();
        drop(control);
        pools.close().await;
    }
    eprintln!(
        "whagons parity: {} plans x {MEMBERS} members x {seeds} seeds, {checked} row checks, {} v1 / {} v2 visible; compiled fallback on {:?}",
        legacy.len(),
        visible[0],
        visible[1],
        fallback_tables
    );
    let never = coverage
        .iter()
        .filter(|(_, c)| c.1 == 0)
        .map(|(t, _)| t.as_str())
        .collect::<Vec<_>>();
    let always = coverage
        .iter()
        .filter(|(_, c)| c.0 > 0 && c.1 == c.0)
        .map(|(t, _)| t.as_str())
        .collect::<Vec<_>>();
    let partial = coverage.values().filter(|c| c.2 > 0).count();
    eprintln!(
        "coverage: {partial}/{} tables had a member seeing a strict non-empty subset; never visible: {never:?}; always all visible: {always:?}; negative controls (tasks mask minus top-level branch i) caught in {control_caught:?} cases",
        coverage.len()
    );
    if only.as_ref().is_none_or(|only| only.contains("tasks")) {
        let missed: Vec<usize> = (0..control_caught.len())
            .filter(|&i| control_caught[i] == 0)
            .collect();
        eprintln!("negative controls undetected (tasks branches): {missed:?}");
        assert!(
            missed.len() * 2 < control_caught.len(),
            "most mutated tasks masks went undetected: the random data is too weak"
        );
    }
    for (table, cases) in &mismatches {
        eprintln!("MISMATCH {table}: {} cases", cases.len());
        for case in cases.iter().take(4) {
            eprintln!(
                "  seed {} member k{} ({}) {}: only left {:?}, only right {:?}",
                case.seed, case.member, case.role, case.what, case.only_left, case.only_right
            );
        }
    }
    assert!(
        mismatches.is_empty(),
        "{} tables disagree: {:?}",
        mismatches.len(),
        mismatches.keys().collect::<Vec<_>>()
    );
}
