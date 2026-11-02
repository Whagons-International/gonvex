//! Derived grants belong to the same SQL snapshot as the rows they authorize.
//! The shared indexes are only for subsequent change-feed evaluation.

use super::*;

pub(super) async fn resolve_derived(
    transaction: &mut TenantTransaction,
    plan: &VisibilityPlan,
    resolved: &mut ResolvedVisibility,
) -> Result<usize, VisibilityError> {
    let mut statements = 0;
    let mut pending = active_sets(plan, &resolved.role)
        .into_iter()
        .filter(|name| plan.sets[name].kind == "derived")
        .collect::<BTreeSet<_>>();
    while !pending.is_empty() {
        let name = pending
            .iter()
            .find(|name| !pending.contains(&plan.sets[*name].from))
            .cloned()
            .ok_or_else(|| VisibilityError::Invalid("cyclic derived visibility sets".into()))?;
        let set = &plan.sets[&name];
        let source = resolved.sets.get(&set.from).ok_or_else(|| {
            VisibilityError::Invalid(format!(
                "mask principal set {:?} was not resolved",
                set.from
            ))
        })?;
        if source.is_empty() {
            resolved.sets.insert(name.clone(), BTreeSet::new());
            pending.remove(&name);
            continue;
        }
        let mut values = Vec::new();
        let mut predicates = vec![tokens_predicate(
            &set.key,
            &set.from,
            resolved,
            &mut values,
        )?];
        for constraint in &set.constraints {
            let predicate = if !constraint.context.is_empty() {
                let value = resolved.direct.get(&constraint.context).ok_or_else(|| {
                    VisibilityError::Invalid(format!("unresolved context {:?}", constraint.context))
                })?;
                values.push(Value::String(value.clone()));
                format!("r.{}::text = ${}", quote(&constraint.column)?, values.len())
            } else if !constraint.in_set.is_empty() {
                tokens_predicate(
                    &constraint.column,
                    &constraint.in_set,
                    resolved,
                    &mut values,
                )?
            } else {
                compile_literal_equality(
                    "r",
                    &constraint.column,
                    visibility_literal(constraint.value.as_ref())?,
                    &mut values,
                )?
            };
            predicates.push(predicate);
        }
        let statement = format!(
            "SELECT DISTINCT r.{}::text FROM {} AS r WHERE {}",
            quote(&set.select)?,
            quote(&set.table)?,
            predicates.join(" AND "),
        );
        let parameters = Parameters::scalars(&values).map_err(VisibilityError::Invalid)?;
        let statement_key = parameters.statement(&statement);
        let rows = parameters
            .bind(sqlx::query(&statement_key))
            .fetch_all(&mut **transaction.transaction())
            .await?;
        statements += 1;
        resolved.sets.insert(
            name.clone(),
            rows.into_iter()
                .filter_map(|row| row.get::<Option<String>, _>(0))
                .collect(),
        );
        pending.remove(&name);
    }
    resolved.fingerprint = visibility_fingerprint(
        plan,
        &resolved.direct,
        &resolved.role,
        &resolved.permissions,
        &resolved.sets,
    );
    Ok(statements)
}

fn tokens_predicate(
    column: &str,
    name: &str,
    resolved: &ResolvedVisibility,
    values: &mut Vec<Value>,
) -> Result<String, VisibilityError> {
    let tokens = resolved.sets.get(name).ok_or_else(|| {
        VisibilityError::Invalid(format!("mask principal set {name:?} was not resolved"))
    })?;
    values.push(serde_json::to_value(tokens).expect("string tokens serialize"));
    Ok(format!(
        "r.{}::text = ANY(ARRAY(SELECT jsonb_array_elements_text(${}::jsonb)))",
        quote(column)?,
        values.len(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use gonvex_postgres::{ControlPlane, PoolLimits, PoolRegistry, TenantRoute};
    use sqlx::{postgres::PgPoolOptions, PgPool};

    struct Fixture {
        admin: PgPool,
        pool: PgPool,
        control: ControlPlane,
        session: TenantSession,
        schema: String,
    }

    impl Fixture {
        async fn new() -> Option<Self> {
            let Ok(url) = std::env::var("GONVEX_TEST_POSTGRES_URL") else {
                eprintln!("GONVEX_TEST_POSTGRES_URL unset; skipping derived snapshot test");
                return None;
            };
            let admin = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .unwrap();
            let schema = format!("derived_snapshot_{}", uuid::Uuid::new_v4().simple());
            sqlx::query(&format!("CREATE SCHEMA \"{schema}\""))
                .execute(&admin)
                .await
                .unwrap();
            let separator = if url.contains('?') { '&' } else { '?' };
            let scoped = format!("{url}{separator}options=-csearch_path%3D{schema}");
            let control = ControlPlane::connect(
                &scoped,
                PoolRegistry::new(PoolLimits::default()),
                BTreeMap::new(),
            )
            .await
            .unwrap();
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&scoped)
                .await
                .unwrap();
            sqlx::raw_sql(
                r#"
                CREATE TABLE members(id text PRIMARY KEY, account_id text, status text,
                                     role text, permissions jsonb);
                INSERT INTO members VALUES('alice','account','active','member','{}');
                CREATE TABLE tasks(id text PRIMARY KEY, "workspaceId" text, "ownerId" text,
                                   "deletedAt" bigint);
                INSERT INTO tasks VALUES('task','a','alice',NULL);
                CREATE TABLE assignments(id text PRIMARY KEY, "memberId" text, "taskId" text);
                INSERT INTO assignments VALUES('grant','alice','task');
                CREATE TABLE workspaces(id text PRIMARY KEY, "organizationId" text);
                INSERT INTO workspaces VALUES('a','org-a'),('b','org-b');
                CREATE TABLE organizations(id text PRIMARY KEY);
                INSERT INTO organizations VALUES('org-a'),('org-b');
            "#,
            )
            .execute(&pool)
            .await
            .unwrap();
            let route = TenantRoute {
                project_id: "project".into(),
                tenant_id: "tenant".into(),
                database_url: scoped,
            };
            let mut session = TenantSession::anonymous(route);
            session.identity.account.id = "account".into();
            Some(Self {
                admin,
                pool,
                control,
                session,
                schema,
            })
        }

        async fn close(self) {
            self.pool.close().await;
            sqlx::query(&format!("DROP SCHEMA \"{}\" CASCADE", self.schema))
                .execute(&self.admin)
                .await
                .unwrap();
            self.admin.close().await;
        }
    }

    fn plan(chained: bool) -> VisibilityPlan {
        let mut value = serde_json::json!({
            "version":2,"permissionTables":[],"table":"workspaces","key":"id",
            "sets":{
                "assigned":{"kind":"grant","table":"assignments","select":"taskId",
                    "where":[{"column":"memberId","context":"member.id"}]},
                "zSpaces":{"kind":"derived","from":"assigned","table":"tasks",
                    "key":"id","select":"workspaceId","where":[
                        {"column":"ownerId","context":"member.id"},
                        {"column":"deletedAt","value":{"literal":null}}
                    ]}
            },
            "where":{"operator":"inSet","column":"id","set":"zSpaces"}
        });
        if chained {
            value["table"] = serde_json::json!("organizations");
            value["sets"]["aOrganizations"] = serde_json::json!({
                "kind":"derived","from":"zSpaces","table":"workspaces","key":"id",
                "select":"organizationId"
            });
            value["where"]["set"] = serde_json::json!("aOrganizations");
        }
        let plan: VisibilityPlan = serde_json::from_value(value).unwrap();
        plan.validate().unwrap();
        plan
    }

    async fn snapshot_ignores_index(ahead: bool) {
        let Some(fixture) = Fixture::new().await else {
            return;
        };
        let plan = plan(false);
        if !ahead {
            sqlx::query("UPDATE tasks SET \"workspaceId\"='b'")
                .execute(&fixture.pool)
                .await
                .unwrap();
        }
        let mut transaction = fixture
            .control
            .begin_tenant_transaction(&fixture.session.route, true)
            .await
            .unwrap();
        let mut resolved = resolve_member(&mut transaction, &fixture.session, &plan, 1, true)
            .await
            .unwrap();
        if ahead {
            sqlx::query("UPDATE tasks SET \"workspaceId\"='b'")
                .execute(&fixture.pool)
                .await
                .unwrap();
        }
        let stale_space = if ahead { "b" } else { "a" };
        resolved.indexes.insert(
            "zSpaces".into(),
            Arc::new(std::sync::RwLock::new(ParentAclIndex::from_snapshot(
                if ahead { 2 } else { 0 },
                BTreeMap::from([(
                    "task".into(),
                    serde_json::json!({
                        "id":"task","workspaceId":stale_space,"ownerId":"alice","deletedAt":null
                    }),
                )]),
            ))),
        );
        // This control is the old snapshot path. Both races produce the wrong grant.
        let mut old = resolved.clone();
        recompute_derived(&plan, &mut old);
        let expected = BTreeSet::from([if ahead { "a".into() } else { "b".into() }]);
        assert_ne!(old.sets["zSpaces"], expected);
        assert_eq!(
            resolve_derived(&mut transaction, &plan, &mut resolved)
                .await
                .unwrap(),
            1
        );
        assert_eq!(resolved.sets["zSpaces"], expected);
        let mut values = Vec::new();
        let predicate = compile_predicate(&plan, &resolved, "r", &mut values).unwrap();
        let parameters = Parameters::scalars(&values).unwrap();
        let query = format!("SELECT id FROM workspaces r WHERE {predicate}");
        let statement = parameters.statement(&query);
        let visible: BTreeSet<String> = parameters
            .bind(sqlx::query(&statement))
            .fetch_all(&mut **transaction.transaction())
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get(0))
            .collect();
        assert_eq!(visible, expected);
        transaction.rollback().await.unwrap();
        fixture.close().await;
    }

    #[tokio::test]
    async fn derived_snapshot_ignores_a_stale_index() {
        snapshot_ignores_index(false).await;
    }

    #[tokio::test]
    async fn derived_snapshot_ignores_an_index_ahead_of_repeatable_read() {
        snapshot_ignores_index(true).await;
    }

    #[tokio::test]
    async fn derived_snapshot_orders_chains_and_applies_all_constraints() {
        let Some(fixture) = Fixture::new().await else {
            return;
        };
        let mut plan = plan(true);
        plan.sets
            .get_mut("zSpaces")
            .unwrap()
            .constraints
            .push(VisibilityConstraint {
                table: String::new(),
                column: "id".into(),
                context: String::new(),
                value: None,
                in_set: "assigned".into(),
            });
        sqlx::raw_sql(
            r#"
            INSERT INTO tasks VALUES('other','b','bob',NULL),('deleted','b','alice',1);
            INSERT INTO assignments VALUES('other-grant','alice','other'),
                                          ('deleted-grant','alice','deleted');
        "#,
        )
        .execute(&fixture.pool)
        .await
        .unwrap();
        let mut transaction = fixture
            .control
            .begin_tenant_transaction(&fixture.session.route, true)
            .await
            .unwrap();
        let mut resolved = resolve_member(&mut transaction, &fixture.session, &plan, 1, true)
            .await
            .unwrap();
        assert_eq!(
            resolve_derived(&mut transaction, &plan, &mut resolved)
                .await
                .unwrap(),
            2
        );
        assert_eq!(resolved.sets["zSpaces"], BTreeSet::from(["a".into()]));
        assert_eq!(
            resolved.sets["aOrganizations"],
            BTreeSet::from(["org-a".into()])
        );
        resolved.sets.get_mut("assigned").unwrap().clear();
        assert_eq!(
            resolve_derived(&mut transaction, &plan, &mut resolved)
                .await
                .unwrap(),
            0,
            "empty source chains issue no SQL"
        );
        assert!(resolved.sets["zSpaces"].is_empty());
        assert!(resolved.sets["aOrganizations"].is_empty());
        transaction.rollback().await.unwrap();
        fixture.close().await;
    }
}
