//! Durable scheduled Reducer and Action execution.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cron::Schedule;
use gonvex_module_runtime::{InvocationChannel, InvocationProvenance};
use gonvex_postgres::{TenantRoute, TenantSession};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use tokio::sync::{watch, Mutex, Notify};
use uuid::Uuid;

use crate::execution::ExecutionAccess;
use crate::modules::CronSpec;
use crate::Runtime;

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    owner: String,
    stop: watch::Sender<bool>,
    wake: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

#[derive(Clone)]
struct ScheduledJob {
    id: String,
    project_id: String,
    tenant_id: String,
    function: String,
    args: Value,
    scheduled_for: DateTime<Utc>,
    cron_name: String,
    claim_token: String,
    provenance: Value,
}

impl Scheduler {
    pub fn new() -> Self {
        let (stop, _) = watch::channel(false);
        Self {
            inner: Arc::new(SchedulerInner {
                owner: format!("rust-runtime-{}", Uuid::new_v4()),
                stop,
                wake: Notify::new(),
                task: Mutex::new(None),
            }),
        }
    }

    pub fn start(&self, runtime: Runtime) {
        let scheduler = self.clone();
        tokio::spawn(async move {
            let mut task = scheduler.inner.task.lock().await;
            if task.is_some() {
                return;
            }
            let runner = scheduler.clone();
            *task = Some(tokio::spawn(async move { runner.run(runtime).await }));
        });
    }

    pub fn shutdown(&self) {
        let _ = self.inner.stop.send(true);
        self.inner.wake.notify_waiters();
    }

    async fn run(&self, runtime: Runtime) {
        let mut stop = self.inner.stop.subscribe();
        let mut last_outbox_scan = std::time::Instant::now() - std::time::Duration::from_secs(10);
        if let Err(error) = self.sync_all_crons(&runtime).await {
            tracing::error!(error = %error, "synchronize scheduled crons");
        }
        loop {
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(500)) => {}
                _ = self.inner.wake.notified() => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                }
            }
            if *stop.borrow() {
                break;
            }
            if last_outbox_scan.elapsed() >= std::time::Duration::from_secs(5) {
                runtime.drain_all_action_outboxes().await;
                last_outbox_scan = std::time::Instant::now();
            }
            let jobs = match self.claim_due(&runtime, 16).await {
                Ok(jobs) => jobs,
                Err(error) => {
                    tracing::error!(error = %error, "claim scheduled work");
                    continue;
                }
            };
            for job in jobs {
                let scheduler = self.clone();
                let runtime = runtime.clone();
                tokio::spawn(async move {
                    scheduler.execute(&runtime, job).await;
                });
            }
        }
    }

    pub async fn enqueue(
        &self,
        runtime: &Runtime,
        session: &TenantSession,
        function: &str,
        args: Value,
        run_at: DateTime<Utc>,
        provenance: Value,
    ) -> Result<String, String> {
        let module = runtime
            .inner
            .modules
            .project(&session.identity.project_id)
            .await
            .ok_or_else(|| "project has no active TypeScript module".to_owned())?;
        let definition = module
            .functions
            .get(function.trim())
            .ok_or_else(|| format!("scheduled function {function:?} is not registered"))?;
        if !matches!(definition.kind.as_str(), "reducer" | "action") {
            return Err("only Reducers and Actions can be scheduled".to_owned());
        }
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Err("scheduler storage is unavailable".to_owned());
        };
        let id = format!("job_{}", Uuid::new_v4());
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gonvex-cron-sync:' || $1))")
            .bind(&session.identity.project_id)
            .execute(&mut **transaction.transaction())
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query(
            r#"INSERT INTO gonvex_scheduled_jobs
               (id,project_id,tenant_id,function_path,args,run_at,scheduled_for,provenance)
               VALUES($1,$2,$3,$4,$5,$6,$6,$7)"#,
        )
        .bind(&id)
        .bind(&session.identity.project_id)
        .bind(&session.route.tenant_id)
        .bind(function.trim())
        .bind(Json(args))
        .bind(run_at)
        .bind(Json(provenance))
        .execute(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        self.inner.wake.notify_one();
        Ok(id)
    }

    async fn claim_due(&self, runtime: &Runtime, limit: i64) -> Result<Vec<ScheduledJob>, String> {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Ok(Vec::new());
        };
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        let rows = sqlx::query(
            r#"WITH candidates AS (
                 SELECT id FROM gonvex_scheduled_jobs
                 WHERE status='pending' AND run_at<=now()
                   AND (claim_token='' OR lease_until IS NULL OR lease_until<=now())
                 ORDER BY (cron_name<>'') ASC,run_at,id FOR UPDATE SKIP LOCKED LIMIT $1
               )
               UPDATE gonvex_scheduled_jobs jobs SET
                 claim_sequence=jobs.claim_sequence+1,
                 claim_token=$2 || ':' || (jobs.claim_sequence+1)::text,
                 lease_until=now()+interval '2 minutes',updated_at=now()
               FROM candidates WHERE jobs.id=candidates.id
               RETURNING jobs.id,jobs.project_id,jobs.tenant_id,jobs.function_path,
                         jobs.args,jobs.run_at,jobs.scheduled_for,jobs.cron_name,jobs.claim_token,jobs.provenance"#,
        )
        .bind(limit)
        .bind(&self.inner.owner)
        .fetch_all(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(rows
            .into_iter()
            .map(|row| ScheduledJob {
                id: row.get("id"),
                project_id: row.get("project_id"),
                tenant_id: row.get("tenant_id"),
                function: row.get("function_path"),
                args: row
                    .get::<Option<Json<Value>>, _>("args")
                    .map(|value| value.0)
                    .unwrap_or(Value::Null),
                scheduled_for: row.get("scheduled_for"),
                cron_name: row.get("cron_name"),
                claim_token: row.get("claim_token"),
                provenance: row.get("provenance"),
            })
            .collect())
    }

    async fn execute(&self, runtime: &Runtime, job: ScheduledJob) {
        let (stop_lease, mut lease_stopped) = watch::channel(false);
        let renewer = self.clone();
        let renewal_runtime = runtime.clone();
        let renewal_job = job.clone();
        let renewal = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(std::time::Duration::from_secs(30)) => {
                        if !renewer.renew_lease(&renewal_runtime, &renewal_job).await {
                            return;
                        }
                    }
                    changed = lease_stopped.changed() => {
                        if changed.is_err() || *lease_stopped.borrow() { return; }
                    }
                }
            }
        });
        let result = self.execute_job(runtime, &job).await;
        let _ = stop_lease.send(true);
        let _ = renewal.await;
        if let Err(error) = self.finish(runtime, &job, result.is_ok()).await {
            tracing::error!(job = %job.id, error = %error, "finish scheduled work");
        }
        if let Err(error) = result {
            tracing::error!(job = %job.id, function = %job.function, error = %error, "scheduled function failed");
        }
    }

    async fn renew_lease(&self, runtime: &Runtime, job: &ScheduledJob) -> bool {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return false;
        };
        let Ok(mut transaction) = control.begin_control_transaction(false).await else {
            return false;
        };
        let renewed = sqlx::query(
            r#"UPDATE gonvex_scheduled_jobs SET lease_until=now()+interval '2 minutes',updated_at=now()
               WHERE id=$1 AND status='pending' AND claim_token=$2"#,
        )
        .bind(&job.id)
        .bind(&job.claim_token)
        .execute(&mut **transaction.transaction())
        .await
        .is_ok_and(|result| result.rows_affected() == 1);
        if renewed {
            transaction.commit().await.is_ok()
        } else {
            let _ = transaction.rollback().await;
            false
        }
    }

    async fn execute_job(&self, runtime: &Runtime, job: &ScheduledJob) -> Result<(), String> {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Err("Control Plane is unavailable".to_owned());
        };
        let route = control
            .tenant_routes(&job.project_id)
            .await
            .map_err(|error| error.to_string())?
            .into_iter()
            .find(|route| route.tenant_id == job.tenant_id)
            .ok_or_else(|| "scheduled tenant is unavailable".to_owned())?;
        let mut provenance = serde_json::from_value::<InvocationProvenance>(job.provenance.clone())
            .unwrap_or_default();
        let session = if let Some(account_id) = provenance.actor_account_id.as_deref() {
            control
                .tenant_session_for_account(&job.project_id, &job.tenant_id, account_id)
                .await
                .map_err(|error| error.to_string())?
        } else {
            crate::execution::system_tenant_session(&job.project_id, route)
        };
        let module = runtime
            .inner
            .modules
            .project_for_call(&job.project_id)
            .await
            .ok_or_else(|| "project has no active module".to_owned())?;
        let definition = module
            .functions
            .get(&job.function)
            .ok_or_else(|| "scheduled function is no longer registered".to_owned())?;
        if provenance.root_command_id.trim().is_empty() {
            provenance = crate::execution::direct_provenance(
                &session,
                InvocationChannel::System,
                &job.id,
                &module.artifact_hash,
            );
        }
        provenance.parent_command_id = Some(provenance.command_id.clone());
        provenance.command_id = job.id.clone();
        provenance.channel = InvocationChannel::Scheduler;
        provenance.depth = provenance.depth.saturating_add(1);
        match definition.kind.as_str() {
            "reducer" => runtime
                .execute_tenant_reducer_with_access(
                    &session,
                    &job.id,
                    None,
                    &job.function,
                    job.args.clone(),
                    ExecutionAccess {
                        allow_internal: true,
                        provenance: Some(provenance),
                        module: Some(module.clone()),
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            "action" => runtime
                .execute_tenant_action_with_access(
                    &session,
                    &job.function,
                    job.args.clone(),
                    ExecutionAccess {
                        allow_internal: true,
                        provenance: Some(provenance),
                        module: Some(module.clone()),
                    },
                )
                .await
                .map(|_| ())
                .map_err(|error| error.to_string()),
            _ => Err("scheduled function must be a Reducer or Action".to_owned()),
        }
    }

    async fn finish(
        &self,
        runtime: &Runtime,
        job: &ScheduledJob,
        _success: bool,
    ) -> Result<(), String> {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Err("Control Plane is unavailable".to_owned());
        };
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        let result = sqlx::query(
            r#"UPDATE gonvex_scheduled_jobs SET status='completed',completed_at=now(),
                 claim_token='',lease_until=NULL,updated_at=now()
               WHERE id=$1 AND status='pending' AND claim_token=$2"#,
        )
        .bind(&job.id)
        .bind(&job.claim_token)
        .execute(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
        if result.rows_affected() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())?;
            return Err("scheduled work lease was lost".to_owned());
        }
        if !job.cron_name.is_empty() {
            if let Some(next) = self.next_cron_occurrence(runtime, job).await? {
                insert_cron_job(&mut transaction, job, next).await?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())
    }

    async fn next_cron_occurrence(
        &self,
        runtime: &Runtime,
        job: &ScheduledJob,
    ) -> Result<Option<DateTime<Utc>>, String> {
        let module = runtime
            .inner
            .modules
            .project(&job.project_id)
            .await
            .ok_or_else(|| "project module is unavailable".to_owned())?;
        let Some(spec) = module.crons.iter().find(|spec| spec.name == job.cron_name) else {
            return Ok(None);
        };
        next_occurrence(spec, job.scheduled_for)
    }

    async fn sync_all_crons(&self, runtime: &Runtime) -> Result<(), String> {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Ok(());
        };
        for project in control
            .runtime_projects()
            .await
            .map_err(|error| error.to_string())?
        {
            let Some(project_id) = project.get("id").and_then(Value::as_str) else {
                continue;
            };
            let Some(module) = runtime.inner.modules.project(project_id).await else {
                continue;
            };
            let routes = control
                .tenant_routes(project_id)
                .await
                .map_err(|error| error.to_string())?;
            self.sync_project(runtime, project_id, &module.crons, &routes)
                .await?;
        }
        Ok(())
    }

    pub async fn sync_project(
        &self,
        runtime: &Runtime,
        project: &str,
        specs: &[CronSpec],
        routes: &[TenantRoute],
    ) -> Result<(), String> {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return Ok(());
        };
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query(
            "DELETE FROM gonvex_scheduled_jobs WHERE project_id=$1 AND status='pending' AND cron_name<>''",
        )
        .bind(project)
        .execute(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?;
        let now = Utc::now();
        if specs.iter().any(|spec| spec.scope == "project") && routes.len() != 1 {
            transaction
                .rollback()
                .await
                .map_err(|error| error.to_string())?;
            return Err(format!(
                "project-scoped crons require exactly one project database route; project {project:?} has {} tenant routes",
                routes.len()
            ));
        }
        for spec in specs {
            let targets: Vec<&TenantRoute> = match spec.scope.as_str() {
                "tenant" => routes.iter().collect(),
                "project" => routes.first().into_iter().collect(),
                _ => Vec::new(),
            };
            let Some(next) = next_occurrence(spec, now)? else {
                continue;
            };
            for route in targets {
                let job = ScheduledJob {
                    id: cron_id(project, &route.tenant_id, &spec.name, next),
                    project_id: project.to_owned(),
                    tenant_id: route.tenant_id.clone(),
                    function: spec.function.clone(),
                    args: spec.args.clone(),
                    scheduled_for: next,
                    cron_name: spec.name.clone(),
                    claim_token: String::new(),
                    provenance: Value::Null,
                };
                insert_cron_job(&mut transaction, &job, next).await?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        self.inner.wake.notify_one();
        Ok(())
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Runtime {
    pub(crate) async fn enqueue_scheduled(
        &self,
        session: &TenantSession,
        function: &str,
        args: Value,
        run_at: DateTime<Utc>,
        provenance: &InvocationProvenance,
    ) -> Result<Value, String> {
        self.inner
            .scheduler
            .enqueue(
                self,
                session,
                function,
                args,
                run_at,
                serde_json::to_value(provenance).unwrap_or(Value::Null),
            )
            .await
            .map(Value::String)
    }
}

async fn insert_cron_job(
    transaction: &mut gonvex_postgres::TenantTransaction,
    job: &ScheduledJob,
    run_at: DateTime<Utc>,
) -> Result<(), String> {
    let id = cron_id(&job.project_id, &job.tenant_id, &job.cron_name, run_at);
    sqlx::query(
        r#"INSERT INTO gonvex_scheduled_jobs
           (id,project_id,tenant_id,function_path,args,run_at,scheduled_for,cron_name,provenance)
           VALUES($1,$2,$3,$4,$5,$6,$6,$7,$8) ON CONFLICT(id) DO NOTHING"#,
    )
    .bind(id)
    .bind(&job.project_id)
    .bind(&job.tenant_id)
    .bind(&job.function)
    .bind(Json(job.args.clone()))
    .bind(run_at)
    .bind(&job.cron_name)
    .bind(Json(job.provenance.clone()))
    .execute(&mut **transaction.transaction())
    .await
    .map_err(|error| error.to_string())?;
    Ok(())
}

fn next_occurrence(spec: &CronSpec, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, String> {
    if let Some(interval_ms) = spec.interval_ms {
        let interval = i64::try_from(interval_ms)
            .map_err(|_| format!("cron {:?} interval is too large", spec.name))?;
        return Ok(Some(after + ChronoDuration::milliseconds(interval)));
    }
    let expression = spec.expression.as_deref().unwrap_or("").trim();
    let fields = expression.split_whitespace().count();
    let normalized = if fields == 5 {
        format!("0 {expression}")
    } else {
        expression.to_owned()
    };
    let schedule = Schedule::from_str(&normalized)
        .map_err(|error| format!("cron {:?} has invalid expression: {error}", spec.name))?;
    Ok(schedule.after(&after).next())
}

fn cron_id(project: &str, tenant: &str, name: &str, at: DateTime<Utc>) -> String {
    let source = format!("{project}\0{tenant}\0{name}\0{}", at.to_rfc3339());
    let digest = Sha256::digest(source.as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("cron_{suffix}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interval_and_five_field_crons_have_a_next_occurrence() {
        let now = Utc::now();
        let interval = CronSpec {
            name: "interval".to_owned(),
            function: "jobs.run".to_owned(),
            args: Value::Null,
            scope: "tenant".to_owned(),
            interval_ms: Some(1_000),
            expression: None,
        };
        assert_eq!(
            next_occurrence(&interval, now).unwrap(),
            Some(now + ChronoDuration::seconds(1))
        );
        let expression = CronSpec {
            name: "daily".to_owned(),
            expression: Some("0 0 * * *".to_owned()),
            interval_ms: None,
            ..interval
        };
        assert!(next_occurrence(&expression, now).unwrap().is_some());
    }
}
