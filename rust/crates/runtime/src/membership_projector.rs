//! Resumable tenant membership directory projection.
//!
//! Tenant `members` is authoritative. Its trigger writes a tenant-local
//! outbox in the same commit. This worker copies that committed state to the
//! Control Plane and only then removes the outbox row. A crash at any point is
//! safe because the Control Plane upsert is revision guarded.

use std::time::Duration;

use sqlx::Row;
use tokio::sync::watch;

use crate::Runtime;

#[derive(Clone)]
pub struct MembershipProjector {
    shutdown: watch::Sender<bool>,
}

impl Default for MembershipProjector {
    fn default() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self { shutdown }
    }
}

impl MembershipProjector {
    pub fn start(&self, runtime: Runtime) {
        let mut shutdown = self.shutdown.subscribe();
        tokio::spawn(async move {
            loop {
                if *shutdown.borrow() {
                    return;
                }
                if let Err(error) = drain_all(&runtime).await {
                    tracing::warn!(%error, "project tenant membership directory");
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                }
            }
        });
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

async fn drain_all(runtime: &Runtime) -> Result<(), gonvex_postgres::DatabaseError> {
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return Ok(());
    };
    for project in control.runtime_projects().await? {
        let Some(project_id) = project.get("id").and_then(serde_json::Value::as_str) else {
            continue;
        };
        // A Control-Plane-only project has no tenant module and therefore no
        // tenant identity infrastructure to project. Querying every historical
        // route made one such database abort projection for all real app
        // projects on every worker pass.
        if runtime.inner.modules.project(project_id).await.is_none() {
            continue;
        }
        for route in control.tenant_routes(project_id).await? {
            match project_membership_outbox_once(&control, &route).await {
                Ok(true) => runtime.notify_control_changed(project_id),
                Ok(false) => {}
                Err(error) => {
                    tracing::warn!(
                        %error,
                        project_id,
                        tenant_id = route.tenant_id,
                        "project tenant membership directory route"
                    );
                }
            }
        }
    }
    Ok(())
}

#[doc(hidden)]
pub async fn project_membership_outbox_once(
    control: &gonvex_postgres::ControlPlane,
    route: &gonvex_postgres::TenantRoute,
) -> Result<bool, gonvex_postgres::DatabaseError> {
    let mut projected = false;
    loop {
        let mut tenant = control.begin_tenant_transaction(route, true).await?;
        let rows = sqlx::query(
            r#"SELECT account_id,member_id,status,membership_revision
               FROM _gonvex_control_plane_membership_outbox
               ORDER BY membership_revision,account_id LIMIT 1000"#,
        )
        .fetch_all(&mut **tenant.transaction())
        .await?;
        tenant.commit().await?;
        if rows.is_empty() {
            return Ok(projected);
        }
        for row in rows {
            let account_id: String = row.get("account_id");
            let member_id: String = row.get("member_id");
            let status: String = row.get("status");
            let revision: i64 = row.get("membership_revision");
            let mut directory = control.begin_control_transaction(false).await?;
            sqlx::query(
                r#"INSERT INTO account_tenant_index
                   (account_id,tenant_id,member_id,status,tenant_membership_revision,updated_at)
                   VALUES($1,$2,$3,$4,$5,now())
                   ON CONFLICT(account_id,tenant_id) DO UPDATE SET
                     member_id=EXCLUDED.member_id,status=EXCLUDED.status,
                     tenant_membership_revision=EXCLUDED.tenant_membership_revision,updated_at=now()
                   WHERE EXCLUDED.tenant_membership_revision >=
                         account_tenant_index.tenant_membership_revision"#,
            )
            .bind(&account_id)
            .bind(&route.tenant_id)
            .bind(&member_id)
            .bind(&status)
            .bind(revision)
            .execute(&mut **directory.transaction())
            .await?;
            directory.commit().await?;

            let mut acknowledge = control.begin_tenant_transaction(route, false).await?;
            sqlx::query(
                r#"DELETE FROM _gonvex_control_plane_membership_outbox
                   WHERE account_id=$1 AND membership_revision<=$2"#,
            )
            .bind(&account_id)
            .bind(revision)
            .execute(&mut **acknowledge.transaction())
            .await?;
            acknowledge.commit().await?;
            projected = true;
        }
        if projected {
            // Yield between bounded batches so a large import cannot monopolize
            // the global SQL admission controller.
            tokio::task::yield_now().await;
        }
    }
}
