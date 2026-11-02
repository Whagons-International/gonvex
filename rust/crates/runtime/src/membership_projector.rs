//! Resumable tenant membership directory projection.
//!
//! Tenant `members` is authoritative. Its trigger writes a tenant-local
//! outbox in the same commit. This worker copies that committed state to the
//! Control Plane and only then removes the outbox row. A crash at any point is
//! safe because the Control Plane upsert is revision guarded.

use std::sync::Arc;
use std::time::Duration;

use sqlx::Row;
use tokio::sync::{watch, Notify};

use crate::Runtime;

#[derive(Clone)]
pub struct MembershipProjector {
    shutdown: watch::Sender<bool>,
    wake: Arc<Notify>,
}

impl Default for MembershipProjector {
    fn default() -> Self {
        let (shutdown, _) = watch::channel(false);
        Self {
            shutdown,
            wake: Arc::new(Notify::new()),
        }
    }
}

impl MembershipProjector {
    pub fn start(&self, runtime: Runtime) {
        let mut shutdown = self.shutdown.subscribe();
        let projector = self.clone();
        tokio::spawn(async move {
            let mut delay = Duration::from_secs(2);
            loop {
                if *shutdown.borrow() {
                    return;
                }
                let projected = match drain_all(&runtime).await {
                    Ok(projected) => projected,
                    Err(error) => {
                        tracing::warn!(%error, "project tenant membership directory");
                        // Retry errors at the busy interval too.
                        true
                    }
                };
                delay = next_delay(delay, projected);
                tokio::select! {
                    _ = projector.wait(delay) => {}
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() { return; }
                    }
                }
            }
        });
    }

    /// Coalesces commits, including commits arriving while a pass is running.
    pub fn wake(&self) {
        self.wake.notify_one();
    }

    async fn wait(&self, delay: Duration) {
        tokio::select! {
            _ = tokio::time::sleep(delay) => {}
            _ = self.wake.notified() => {}
        }
    }

    pub fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

fn next_delay(previous: Duration, projected: bool) -> Duration {
    if projected {
        Duration::from_secs(2)
    } else {
        (previous * 2).min(Duration::from_secs(10))
    }
}

async fn drain_all(runtime: &Runtime) -> Result<bool, gonvex_store::DatabaseError> {
    let mut projected = false;
    let Some(control) = runtime.inner.control_plane.read().await.clone() else {
        return Ok(false);
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
                Ok(true) => {
                    projected = true;
                    runtime.notify_control_changed(project_id);
                }
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
    Ok(projected)
}

#[doc(hidden)]
pub async fn project_membership_outbox_once(
    control: &gonvex_store::ControlPlane,
    route: &gonvex_store::TenantRoute,
) -> Result<bool, gonvex_store::DatabaseError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_caps_and_resets_after_work() {
        let mut delay = Duration::from_secs(2);
        for seconds in [4, 8, 10, 10] {
            delay = next_delay(delay, false);
            assert_eq!(delay, Duration::from_secs(seconds));
        }
        delay = next_delay(delay, true);
        assert_eq!(delay, Duration::from_secs(2));
        assert_eq!(next_delay(delay, true), Duration::from_secs(2));
        assert_eq!(next_delay(delay, false), Duration::from_secs(4));
    }

    #[tokio::test]
    async fn wake_interrupts_idle_sleep() {
        let projector = MembershipProjector::default();
        let wait = projector.wait(Duration::from_secs(10));
        tokio::pin!(wait);
        tokio::select! {
            _ = &mut wait => panic!("idle sleep completed before a wake"),
            _ = tokio::task::yield_now() => {}
        }
        projector.clone().wake();
        tokio::time::timeout(Duration::from_secs(1), wait)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn wake_during_pass_is_retained_and_coalesced() {
        let projector = MembershipProjector::default();
        projector.wake();
        projector.wake();
        tokio::time::timeout(
            Duration::from_secs(1),
            projector.wait(Duration::from_secs(10)),
        )
        .await
        .unwrap();
        assert!(tokio::time::timeout(
            Duration::from_millis(20),
            projector.wait(Duration::from_secs(10))
        )
        .await
        .is_err());
    }
}
