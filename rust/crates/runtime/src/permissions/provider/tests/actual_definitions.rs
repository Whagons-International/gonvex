//! Actual pinned source definitions on an explicitly synthetic physical schema.
//! These tests prove SQL window equivalence here, not production-copy coverage.
use super::*;

fn fixture() -> Value {
    serde_json::from_str(include_str!(
        "../../../../testdata/whagons-source-policy-fixture.json"
    ))
    .unwrap()
}
fn loaded(f: &Value) -> ProjectModule {
    let mut m = module(json!({"operator":"public"}));
    let template = m.functions["owned.list"].clone();
    m.schema = f["moduleSchema"].clone();
    m.visibility = serde_json::from_value(f["visibility"].clone()).unwrap();
    m.functions = f["functions"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(path, v)| {
            let mut function = template.clone();
            function.replica = Some(serde_json::from_value(v["replica"].clone()).unwrap());
            function.args_schema = v["args"].clone();
            function.result_schema = v["result"].clone();
            function.dependencies = v.get("dependencies").cloned().unwrap_or(Value::Null);
            (path.clone(), function)
        })
        .collect();
    m
}
fn ident(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}
fn synthetic_type(pg: &str) -> &'static str {
    match pg {
        "text" | "timestamp with time zone" => "TEXT",
        "boolean" => "BOOLEAN",
        "jsonb" => "JSON",
        "bigint" | "integer" => "INTEGER",
        "double precision" | "numeric" => "REAL",
        _ => panic!("unhandled synthetic source type {pg}"),
    }
}
fn value(pg: &str, row: usize) -> Value {
    match pg {
        "text" => json!(format!("v-{}", row / 2)),
        "timestamp with time zone" => json!(format!("2026-11-05T00:00:{:02}.123Z", row / 2)),
        "boolean" => json!(row.is_multiple_of(2)),
        "jsonb" => json!(
            [
                "null",
                "{\"nested\":[1,true,null]}",
                "[1,2]",
                "\"text\"",
                "0",
                "false",
                "{}",
                "[]"
            ][row % 8]
        ),
        "bigint" | "integer" => json!((row / 2) as i64),
        "double precision" | "numeric" => json!((row / 2) as f64 + 0.25),
        _ => panic!("unhandled synthetic value type"),
    }
}
async fn schema(
    tx: &mut TenantTransaction,
    f: &Value,
    m: &ProjectModule,
) -> BTreeMap<String, Value> {
    let tables = m
        .visibility
        .iter()
        .filter(|(_, p)| expression(p).is_ok())
        .map(|(t, _)| t.clone())
        .collect::<BTreeSet<_>>();
    let permissions = m
        .visibility
        .values()
        .filter_map(|p| expression(p).ok())
        .flat_map(|p| {
            p.permissions()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .map(|s| (s, Value::Bool(true)))
        .collect::<serde_json::Map<_, _>>();
    let mut first = BTreeMap::new();
    for table in tables {
        let meta = &f["sourceSchema"][&table];
        let columns = meta["columns"].as_object().unwrap();
        let key = meta["key"].as_str().unwrap();
        let ddl = format!(
            "CREATE TABLE {}({})",
            ident(&table),
            columns
                .iter()
                .map(|(n, c)| format!(
                    "{} {}{}{}",
                    ident(n),
                    synthetic_type(c["type"].as_str().unwrap()),
                    if n == key { " PRIMARY KEY" } else { "" },
                    if c["nullable"] == false {
                        " NOT NULL"
                    } else {
                        ""
                    }
                ))
                .collect::<Vec<_>>()
                .join(",")
        );
        sqlx::query(&ddl)
            .execute(&mut **tx.transaction())
            .await
            .unwrap();
        let excludes = m
            .functions
            .values()
            .filter_map(|f| f.replica.as_ref())
            .filter(|d| d.table == table)
            .flat_map(|d| d.exclude_when_set.clone())
            .collect::<BTreeSet<_>>();
        let policy = expression(&m.visibility[&table]).unwrap();
        for row in 0..8 {
            let mut vals = serde_json::Map::new();
            for (name, col) in columns {
                let mut v = value(col["type"].as_str().unwrap(), row);
                if row == 3 && col["nullable"] == true {
                    v = Value::Null;
                }
                if name == key {
                    v = json!(format!("k-{row:02}"));
                }
                if excludes.contains(name) {
                    v = if row % 3 == 2 {
                        value(col["type"].as_str().unwrap(), row)
                    } else {
                        Value::Null
                    };
                }
                if policy.contexts().iter().any(|(col, _)| *col == name) {
                    v = json!(["alice", "bob", "", " alice", "ALICE", "é", "é", "alice"][row]);
                    if row == 2 && col["nullable"] == true && name != key {
                        v = Value::Null;
                    }
                    if name == key && (row == 2 || row == 7) {
                        v = json!(format!("other-{row}"));
                    }
                }
                if table == "members" {
                    match name.as_str() {
                        "id" => {
                            v = json!(if row == 0 {
                                "alice".into()
                            } else if row == 1 {
                                "bob".into()
                            } else {
                                format!("member-{row}")
                            })
                        }
                        "account_id" => {
                            v = json!(if row == 0 {
                                "a".into()
                            } else if row == 1 {
                                "b".into()
                            } else {
                                format!("account-{row}")
                            })
                        }
                        "status" => v = json!(if row < 2 { "active" } else { "inactive" }),
                        "role" => v = json!(if row == 0 { "system_admin" } else { "member" }),
                        "permissions" => {
                            v = json!(if row == 0 {
                                serde_json::to_string(&permissions).unwrap()
                            } else {
                                "{}".into()
                            })
                        }
                        _ => {}
                    }
                }
                vals.insert(name.clone(), v);
            }
            if row == 0 {
                first.insert(table.clone(), Value::Object(vals.clone()));
            }
            let sql = format!(
                "INSERT INTO {}({}) VALUES({})",
                ident(&table),
                vals.keys().map(|k| ident(k)).collect::<Vec<_>>().join(","),
                (1..=vals.len())
                    .map(|i| format!("?{i}"))
                    .collect::<Vec<_>>()
                    .join(",")
            );
            crate::sql_parameters::Parameters::scalars(&vals.values().cloned().collect::<Vec<_>>())
                .unwrap()
                .bind(sqlx::query(&sql))
                .execute(&mut **tx.transaction())
                .await
                .unwrap();
        }
    }
    first
}
fn row(r: crate::replica::ScannedRow) -> (String, String, String) {
    (
        r.key.to_string(),
        serde_json::to_string(&r.row).unwrap(),
        r.hash.to_string(),
    )
}
#[tokio::test]
#[expect(clippy::await_holding_lock, reason = "process-local fixture opt-in")]
async fn actual_public_and_self_definitions_match_v2_order_projection_filters_budgets_and_hashes() {
    let _guard = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
    let _env = Env(std::env::var_os("GONVEX_PERMISSION_PRIMITIVES_LOCAL"));
    std::env::set_var("GONVEX_PERMISSION_PRIMITIVES_LOCAL", "1");
    std::fs::create_dir_all(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let dir = tempfile::tempdir_in(indexes::LOCAL_FIXTURE_ROOT).unwrap();
    let url = gonvex_store::database_url(&dir.path().join("actual-definitions.db"));
    let pools = PoolRegistry::new(PoolLimits::default());
    let f = fixture();
    let m = loaded(&f);
    let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
        .await
        .unwrap();
    sqlx::raw_sql(gonvex_store::feed::SYNC_INFRASTRUCTURE_SQL)
        .execute(&mut **tx.transaction())
        .await
        .unwrap();
    let first = schema(&mut tx, &f, &m).await;
    indexes::install_local(&mut tx).await.unwrap();
    tx.commit().await.unwrap();
    let registry = ModuleRegistry::new();
    registry.install_permission_fixture(m).await;
    let lease = registry.project_for_call("fixture-project").await.unwrap();
    let mut session = TenantSession::anonymous(TenantRoute {
        project_id: "fixture-project".into(),
        tenant_id: "tenant".into(),
        database_url: url.clone(),
    });
    session.identity.account.id = "a".into();
    session.member.id = "alice".into();
    let mut counts = BTreeMap::<String, usize>::new();
    for (path, function) in &lease.functions {
        let definition = function.replica.as_ref().unwrap();
        let plan = &lease.visibility[&definition.table];
        if expression(plan).is_err() {
            continue;
        }
        let mut tx = gonvex_store::begin_transaction(&pools, &url, false)
            .await
            .unwrap();
        let Compilation::Complete(cert) =
            CompleteProviderCertificate::compile(&lease, &mut tx, &session, path)
                .await
                .unwrap()
        else {
            panic!("not compiled {path}")
        };
        let reg = cert.install(&mut tx).await.unwrap();
        tx.commit().await.unwrap();
        let args = Value::Object(
            definition
                .equal_filters
                .iter()
                .map(|(col, arg)| (arg.clone(), first[&definition.table][col].clone()))
                .collect(),
        );
        let mut tx = gonvex_store::begin_transaction(&pools, &url, true)
            .await
            .unwrap();
        let resolved = crate::visibility::resolve(&mut tx, &session, plan, 0)
            .await
            .unwrap();
        let mut expected = Vec::new();
        let expected_window =
            crate::replica::scan_window(&mut tx, definition, plan, &resolved, &args, |r| {
                expected.push(row(r));
                Ok(())
            })
            .await
            .unwrap();
        match reg.fresh(&lease, &mut tx, &session, &args).await.unwrap() {
            Fresh::Indexed(mut scan) => {
                let mut actual = Vec::new();
                let (scan_tx, scan_definition, prepared, scan_args) = scan.parts();
                let actual_window = crate::replica::permission::visit_indexed_collection_window(
                    scan_tx,
                    scan_definition,
                    prepared,
                    scan_args,
                    |r| {
                        actual.push(row(r));
                        Ok(())
                    },
                )
                .await
                .unwrap();
                assert_eq!(actual, expected, "exact rows/messages {path}");
                assert_eq!(actual_window, expected_window, "window {path}");
                *counts
                    .entry(format!("{}:indexed", plan.predicate.operator))
                    .or_default() += 1;
            }
            Fresh::NormalV2(Fallback::UnorderedSource) => {
                assert!(definition.order_by.is_empty());
                *counts
                    .entry(format!("{}:unordered-v2", plan.predicate.operator))
                    .or_default() += 1;
            }
            _ => panic!("unexpected eligibility {path}"),
        }
        tx.commit().await.unwrap();
    }
    assert_eq!(
        counts,
        BTreeMap::from([
            ("public:indexed".into(), 61),
            ("public:unordered-v2".into(), 82),
            ("eqContext:indexed".into(), 20),
            ("eqContext:unordered-v2".into(), 20),
            ("permission:indexed".into(), 60),
            ("permission:unordered-v2".into(), 61),
            ("role:indexed".into(), 1),
            ("role:unordered-v2".into(), 1),
            ("or:indexed".into(), 42),
            ("or:unordered-v2".into(), 79)
        ])
    );
    eprintln!(
        "synthetic actual-definition window coverage {counts:?}; production-copy qualified=0"
    );
}
