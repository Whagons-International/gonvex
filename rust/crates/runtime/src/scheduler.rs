//! Durable scheduled Reducer and Action execution.

use std::str::FromStr;
use std::sync::Arc;

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use cron::Schedule;
use gonvex_module_runtime::{InvocationChannel, InvocationProvenance};
use gonvex_store::{TenantRoute, TenantSession};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::types::Json;
use sqlx::Row;
use tokio::sync::{watch, Mutex, Notify};
use uuid::Uuid;

use crate::execution::ExecutionAccess;
use crate::modules::CronSpec;
use crate::Runtime;

/// Completed jobs are kept this long for the operator history, then deleted.
const COMPLETED_JOB_RETENTION: ChronoDuration = ChronoDuration::days(7);
const PRUNE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(600);
const PRUNE_BATCH: i64 = 5_000;
const PRUNE_MAX_BATCHES: usize = 20;
const CLAIM_BATCH: i64 = 16;
/// Longest idle sleep. Bounds how late the scheduler notices jobs inserted by
/// another runtime replica or leases that expired after a crash.
const MAX_IDLE_SLEEP: std::time::Duration = std::time::Duration::from_secs(30);
/// Reducers drain their own outbox right after commit; this sweep only picks up
/// retries and work abandoned by a crashed runtime.
const OUTBOX_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Earliest unclaimed run time. One probe per partial index, never a table scan.
const NEXT_DUE_SQL: &str = r#"SELECT LEAST(
                 (SELECT run_at FROM gonvex_scheduled_jobs
                  WHERE status='pending' AND cron_name='' AND claim_token=''
                  ORDER BY run_at,id LIMIT 1),
                 (SELECT run_at FROM gonvex_scheduled_jobs
                  WHERE status='pending' AND cron_name<>'' AND claim_token=''
                  ORDER BY run_at,id LIMIT 1))"#;

const CLAIM_DUE_SQL: &str = r#"WITH candidates AS (
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
                         jobs.args,jobs.run_at,jobs.scheduled_for,jobs.cron_name,jobs.claim_token,jobs.provenance"#;

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

struct ScheduleRequest {
    id: Option<String>,
    function: String,
    args: Value,
    run_at: DateTime<Utc>,
    provenance: Value,
    /// `scheduler.wakeAt`: merge with the tenant's unclaimed wake for the same
    /// function and args, keeping the earlier time.
    coalesce: bool,
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
        if let Err(error) = self.sync_all_crons(&runtime).await {
            tracing::error!(error = %error, "synchronize scheduled crons");
        }
        // An outbox sweep may spend minutes retrying external deliveries across
        // tenants. Poll one sweep worker alongside the timer instead of awaiting
        // it before claiming jobs. Keeping this future pinned also prevents
        // overlapping sweeps, and drops it when the scheduler shuts down.
        let outbox_scans = async {
            loop {
                runtime.drain_all_action_outboxes().await;
                tokio::time::sleep(OUTBOX_SWEEP_INTERVAL).await;
            }
        };
        tokio::pin!(outbox_scans);
        let mut last_prune: Option<std::time::Instant> = None;
        // Sleep until the next job is due instead of polling. Enqueues, cron
        // syncs and finished cron runs wake the loop early through `wake`.
        let mut idle = std::time::Duration::ZERO;
        loop {
            tokio::select! {
                _ = &mut outbox_scans => break,
                _ = tokio::time::sleep(idle) => {}
                _ = self.inner.wake.notified() => {}
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                }
            }
            if *stop.borrow() {
                break;
            }
            if last_prune.is_none_or(|at| at.elapsed() >= PRUNE_INTERVAL) {
                last_prune = Some(std::time::Instant::now());
                self.prune_completed(&runtime).await;
            }
            let jobs = match self.claim_due(&runtime, CLAIM_BATCH).await {
                Ok(jobs) => jobs,
                Err(error) => {
                    tracing::error!(error = %error, "claim scheduled work");
                    idle = std::time::Duration::from_secs(1);
                    continue;
                }
            };
            idle = if jobs.len() as i64 == CLAIM_BATCH {
                std::time::Duration::ZERO
            } else {
                self.until_next_due(&runtime).await
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

    async fn enqueue(
        &self,
        runtime: &Runtime,
        session: &TenantSession,
        request: ScheduleRequest,
    ) -> Result<String, String> {
        let ScheduleRequest {
            id,
            function,
            args,
            run_at,
            provenance,
            coalesce,
        } = request;
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
        let id = id.unwrap_or_else(|| format!("job_{}", Uuid::new_v4()));
        let mut transaction = control
            .begin_control_transaction(false)
            .await
            .map_err(|error| error.to_string())?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gonvex-cron-sync:' || $1))")
            .bind(&session.identity.project_id)
            .execute(&mut **transaction.transaction())
            .await
            .map_err(|error| error.to_string())?;
        let job = OneShotJob {
            id: &id,
            project_id: &session.identity.project_id,
            tenant_id: &session.route.tenant_id,
            function: function.trim(),
            args,
            run_at,
            provenance,
            coalesce,
        };
        let id = insert_one_shot_job(transaction.transaction(), job)
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
        let jobs = claim_due_jobs(transaction.transaction(), &self.inner.owner, limit)
            .await
            .map_err(|error| error.to_string())?;
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(jobs)
    }

    async fn until_next_due(&self, runtime: &Runtime) -> std::time::Duration {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return MAX_IDLE_SLEEP;
        };
        let next = match control.begin_control_transaction(true).await {
            Ok(mut transaction) => {
                let next = next_due_at(transaction.transaction()).await;
                let _ = transaction.rollback().await;
                next
            }
            Err(error) => Err(sqlx::Error::Protocol(error.to_string())),
        };
        match next {
            Ok(next) => idle_sleep(next, Utc::now()),
            Err(error) => {
                tracing::error!(error = %error, "find next scheduled work");
                std::time::Duration::from_secs(1)
            }
        }
    }

    async fn prune_completed(&self, runtime: &Runtime) {
        let Some(control) = runtime.inner.control_plane.read().await.clone() else {
            return;
        };
        let before = Utc::now() - COMPLETED_JOB_RETENTION;
        for _ in 0..PRUNE_MAX_BATCHES {
            let deleted = match control.begin_control_transaction(false).await {
                Ok(mut transaction) => {
                    let deleted =
                        prune_completed_jobs(transaction.transaction(), before, PRUNE_BATCH).await;
                    match deleted {
                        Ok(deleted) => transaction
                            .commit()
                            .await
                            .map(|_| deleted)
                            .map_err(|error| error.to_string()),
                        Err(error) => Err(error.to_string()),
                    }
                }
                Err(error) => Err(error.to_string()),
            };
            match deleted {
                Ok(deleted) if deleted < PRUNE_BATCH as u64 => return,
                Ok(_) => {}
                Err(error) => {
                    tracing::error!(error = %error, "prune completed scheduled work");
                    return;
                }
            }
        }
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
                        ..ExecutionAccess::default()
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
                        ..ExecutionAccess::default()
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
                insert_cron_job(transaction.transaction(), job, next)
                    .await
                    .map_err(|error| error.to_string())?;
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        self.inner.wake.notify_one();
        Ok(())
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
        next_cron_run(spec, job.scheduled_for, Utc::now())
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
                insert_cron_job(transaction.transaction(), &job, next)
                    .await
                    .map_err(|error| error.to_string())?;
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
        coalesce: bool,
    ) -> Result<Value, String> {
        self.inner
            .scheduler
            .enqueue(
                self,
                session,
                ScheduleRequest {
                    id: None,
                    function: function.to_owned(),
                    args,
                    run_at,
                    provenance: serde_json::to_value(provenance).unwrap_or(Value::Null),
                    coalesce,
                },
            )
            .await
            .map(Value::String)
    }

    // Mirrors the scheduled-outbox payload field for field.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue_scheduled_with_id(
        &self,
        session: &TenantSession,
        job_id: &str,
        function: &str,
        args: Value,
        run_at: DateTime<Utc>,
        provenance: &InvocationProvenance,
        coalesce: bool,
    ) -> Result<Value, String> {
        self.inner
            .scheduler
            .enqueue(
                self,
                session,
                ScheduleRequest {
                    id: Some(job_id.to_owned()),
                    function: function.to_owned(),
                    args,
                    run_at,
                    provenance: serde_json::to_value(provenance).unwrap_or(Value::Null),
                    coalesce,
                },
            )
            .await
            .map(Value::String)
    }
}

struct OneShotJob<'a> {
    id: &'a str,
    project_id: &'a str,
    tenant_id: &'a str,
    function: &'a str,
    args: Value,
    run_at: DateTime<Utc>,
    provenance: Value,
    coalesce: bool,
}

/// Inserts a `runAt`/`runAfter` job, or for `wakeAt` merges it into the
/// tenant's unclaimed wake for the same function and args (earlier time wins).
/// Returns the id of the job that will run. Re-promoting an outbox entry whose
/// job already exists is a no-op, whatever state that job is in.
async fn insert_one_shot_job(
    connection: &mut sqlx::PgConnection,
    job: OneShotJob<'_>,
) -> Result<String, sqlx::Error> {
    let wake_key = if job.coalesce {
        wake_key(job.function, &job.args)
    } else {
        String::new()
    };
    let inserted: Option<String> = sqlx::query_scalar(
        r#"INSERT INTO gonvex_scheduled_jobs
           (id,project_id,tenant_id,function_path,args,run_at,scheduled_for,provenance,wake_key)
           SELECT $1,$2,$3,$4,$5,$6,$6,$7,$8
           WHERE NOT EXISTS (SELECT 1 FROM gonvex_scheduled_jobs WHERE id=$1)
           ON CONFLICT (project_id,tenant_id,wake_key)
             WHERE status='pending' AND claim_token='' AND wake_key<>''
           DO UPDATE SET run_at=LEAST(gonvex_scheduled_jobs.run_at,EXCLUDED.run_at),
                         scheduled_for=LEAST(gonvex_scheduled_jobs.scheduled_for,EXCLUDED.scheduled_for),
                         updated_at=now()
           RETURNING id"#,
    )
    .bind(job.id)
    .bind(job.project_id)
    .bind(job.tenant_id)
    .bind(job.function)
    .bind(Json(job.args))
    .bind(job.run_at)
    .bind(Json(job.provenance))
    .bind(wake_key)
    .fetch_optional(connection)
    .await?;
    Ok(inserted.unwrap_or_else(|| job.id.to_owned()))
}

/// Identity of a wake: function plus args with object keys sorted, so the same
/// logical args always coalesce.
fn wake_key(function: &str, args: &Value) -> String {
    fn canonical(value: &Value, out: &mut String) {
        match value {
            Value::Object(map) => {
                let mut keys: Vec<&String> = map.keys().collect();
                keys.sort();
                out.push('{');
                for (index, key) in keys.into_iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    out.push_str(&Value::String(key.clone()).to_string());
                    out.push(':');
                    canonical(&map[key], out);
                }
                out.push('}');
            }
            Value::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    canonical(item, out);
                }
                out.push(']');
            }
            other => out.push_str(&other.to_string()),
        }
    }
    let mut source = format!("{function}\0");
    canonical(args, &mut source);
    let digest = Sha256::digest(source.as_bytes());
    let suffix = digest[..16]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("wake_{suffix}")
}

/// Inserts the next run of a cron. A run that is already pending for the same
/// cron and tenant wins: the unique pending-cron index rejects the duplicate.
async fn insert_cron_job(
    connection: &mut sqlx::PgConnection,
    job: &ScheduledJob,
    run_at: DateTime<Utc>,
) -> Result<(), sqlx::Error> {
    let id = cron_id(&job.project_id, &job.tenant_id, &job.cron_name, run_at);
    sqlx::query(
        r#"INSERT INTO gonvex_scheduled_jobs
           (id,project_id,tenant_id,function_path,args,run_at,scheduled_for,cron_name,provenance)
           VALUES($1,$2,$3,$4,$5,$6,$6,$7,$8) ON CONFLICT DO NOTHING"#,
    )
    .bind(id)
    .bind(&job.project_id)
    .bind(&job.tenant_id)
    .bind(&job.function)
    .bind(Json(job.args.clone()))
    .bind(run_at)
    .bind(&job.cron_name)
    .bind(Json(job.provenance.clone()))
    .execute(connection)
    .await?;
    Ok(())
}

async fn claim_due_jobs(
    connection: &mut sqlx::PgConnection,
    owner: &str,
    limit: i64,
) -> Result<Vec<ScheduledJob>, sqlx::Error> {
    let rows = sqlx::query(CLAIM_DUE_SQL)
        .bind(limit)
        .bind(owner)
        .fetch_all(connection)
        .await?;
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

async fn prune_completed_jobs(
    connection: &mut sqlx::PgConnection,
    before: DateTime<Utc>,
    limit: i64,
) -> Result<u64, sqlx::Error> {
    let result = sqlx::query(
        r#"DELETE FROM gonvex_scheduled_jobs WHERE id IN (
             SELECT id FROM gonvex_scheduled_jobs
             WHERE status='completed' AND completed_at<$1
             ORDER BY completed_at,id LIMIT $2
           )"#,
    )
    .bind(before)
    .bind(limit)
    .execute(connection)
    .await?;
    Ok(result.rows_affected())
}

async fn next_due_at(
    connection: &mut sqlx::PgConnection,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    sqlx::query_scalar(NEXT_DUE_SQL).fetch_one(connection).await
}

/// How long the scheduler may sleep before `next` is due, capped so work from
/// other replicas and expired leases are still noticed.
fn idle_sleep(next: Option<DateTime<Utc>>, now: DateTime<Utc>) -> std::time::Duration {
    let Some(next) = next else {
        return MAX_IDLE_SLEEP;
    };
    (next - now)
        .to_std()
        .unwrap_or(std::time::Duration::ZERO)
        .min(MAX_IDLE_SLEEP)
}

/// The run after `scheduled_for`, or the first one after `now` when the
/// scheduler fell behind. Missed runs are skipped, never replayed.
fn next_cron_run(
    spec: &CronSpec,
    scheduled_for: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Option<DateTime<Utc>>, String> {
    match next_occurrence(spec, scheduled_for)? {
        Some(next) if next <= now => next_occurrence(spec, now),
        next => Ok(next),
    }
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

    fn minute_cron() -> CronSpec {
        CronSpec {
            name: "every-minute".to_owned(),
            function: "jobs.run".to_owned(),
            args: Value::Null,
            scope: "tenant".to_owned(),
            interval_ms: None,
            expression: Some("* * * * *".to_owned()),
        }
    }

    #[test]
    fn a_cron_that_fell_behind_skips_missed_runs_instead_of_replaying_them() {
        let spec = minute_cron();
        let now = DateTime::parse_from_rfc3339("2026-11-02T17:30:20Z")
            .unwrap()
            .with_timezone(&Utc);
        let on_time = now - ChronoDuration::seconds(20);
        assert_eq!(
            next_cron_run(&spec, on_time, now).unwrap(),
            Some(on_time + ChronoDuration::minutes(1))
        );
        let six_days_late = now - ChronoDuration::days(6);
        let next = next_cron_run(&spec, six_days_late, now).unwrap().unwrap();
        assert!(next > now && next <= now + ChronoDuration::minutes(1));
    }

    #[test]
    fn the_scheduler_sleeps_until_the_next_job_but_never_past_the_idle_cap() {
        let now = Utc::now();
        assert_eq!(
            idle_sleep(Some(now + ChronoDuration::seconds(7)), now),
            std::time::Duration::from_secs(7)
        );
        assert_eq!(
            idle_sleep(Some(now - ChronoDuration::seconds(7)), now),
            std::time::Duration::ZERO
        );
        assert_eq!(
            idle_sleep(Some(now + ChronoDuration::hours(6)), now),
            MAX_IDLE_SLEEP
        );
        assert_eq!(idle_sleep(None, now), MAX_IDLE_SLEEP);
    }

    struct ControlFixture {
        pool: sqlx::PgPool,
    }

    /// A fresh Control Plane schema with the production DDL applied, or None when
    /// GONVEX_TEST_POSTGRES_URL is unset.
    async fn control_fixture(label: &str) -> Option<ControlFixture> {
        let base = std::env::var("GONVEX_TEST_POSTGRES_URL")
            .ok()
            .filter(|value| !value.trim().is_empty())?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let schema = format!("gonvex_scheduler_{label}_{nonce}");
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(&base)
            .await
            .unwrap();
        sqlx::query(&format!(r#"CREATE SCHEMA "{schema}""#))
            .execute(&admin)
            .await
            .unwrap();
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}options=-csearch_path%3D{schema}");
        let pools = gonvex_store::PoolRegistry::new(gonvex_store::PoolLimits::default());
        gonvex_store::ControlPlane::connect(&url, pools, Default::default())
            .await
            .unwrap();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        Some(ControlFixture { pool })
    }

    fn cron_job(tenant: &str) -> ScheduledJob {
        ScheduledJob {
            id: String::new(),
            project_id: "project".to_owned(),
            tenant_id: tenant.to_owned(),
            function: "slas.evaluate".to_owned(),
            args: Value::Null,
            scheduled_for: Utc::now(),
            cron_name: "slas".to_owned(),
            claim_token: String::new(),
            provenance: Value::Null,
        }
    }

    async fn pending_runs(pool: &sqlx::PgPool, tenant: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='slas' AND tenant_id=$1",
        )
        .bind(tenant)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Regression for the 2026-11-02 production incident: a scheduler that fell
    /// behind enqueued a new copy of every cron each minute, 2.5 million in six days.
    #[tokio::test]
    async fn a_cron_never_has_more_than_one_pending_run_per_tenant() {
        let Some(fixture) = control_fixture("backlog").await else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping scheduler backlog test");
            return;
        };
        let mut connection = fixture.pool.acquire().await.unwrap();
        let job = cron_job("tenant-a");
        let start = Utc::now() - ChronoDuration::hours(1);
        for minute in 0..60 {
            insert_cron_job(
                &mut connection,
                &job,
                start + ChronoDuration::minutes(minute),
            )
            .await
            .unwrap();
        }
        insert_cron_job(&mut connection, &cron_job("tenant-b"), start)
            .await
            .unwrap();
        assert_eq!(pending_runs(&fixture.pool, "tenant-a").await, 1);
        assert_eq!(pending_runs(&fixture.pool, "tenant-b").await, 1);

        // Finishing the pending run makes room for the next one, as finish() does.
        let claimed = claim_due_jobs(&mut connection, "owner", 16).await.unwrap();
        assert_eq!(claimed.len(), 2);
        sqlx::query("UPDATE gonvex_scheduled_jobs SET status='completed',completed_at=now() WHERE tenant_id='tenant-a'")
            .execute(&mut *connection)
            .await
            .unwrap();
        insert_cron_job(
            &mut connection,
            &job,
            Utc::now() + ChronoDuration::minutes(1),
        )
        .await
        .unwrap();
        assert_eq!(pending_runs(&fixture.pool, "tenant-a").await, 1);
    }

    /// Every claim used to scan the whole table, completed history included, so
    /// each claim got slower as the table grew until the scheduler fell behind.
    #[tokio::test]
    async fn claims_use_an_index_instead_of_scanning_job_history() {
        let Some(fixture) = control_fixture("claim").await else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping scheduler claim test");
            return;
        };
        sqlx::query(
            r#"INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name,status,completed_at)
               SELECT 'done_' || n,'project','tenant-' || (n % 30),'slas.evaluate',
                      now() - n * interval '1 second',now() - n * interval '1 second',
                      'slas','completed',now() - n * interval '1 second'
               FROM generate_series(1,100000) AS n"#,
        )
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query(
            r#"INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name)
               SELECT 'due_' || n,'project','tenant-' || n,'slas.evaluate',
                      now() - interval '1 minute',now() - interval '1 minute',
                      CASE WHEN n % 2 = 0 THEN 'slas' ELSE '' END
               FROM generate_series(1,200) AS n"#,
        )
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE gonvex_scheduled_jobs")
            .execute(&fixture.pool)
            .await
            .unwrap();
        let mut connection = fixture.pool.acquire().await.unwrap();
        let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {CLAIM_DUE_SQL}"))
            .bind(16_i64)
            .bind("owner")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        let plan = plan.join("\n");
        assert!(
            !plan.contains("Seq Scan on gonvex_scheduled_jobs"),
            "claim scans the job table:\n{plan}"
        );

        let plan: Vec<String> = sqlx::query_scalar(&format!("EXPLAIN {NEXT_DUE_SQL}"))
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        let plan = plan.join("\n");
        assert!(
            !plan.contains("Seq Scan on gonvex_scheduled_jobs"),
            "next-due lookup scans the job table:\n{plan}"
        );
        let next = next_due_at(&mut connection).await.unwrap().unwrap();
        assert!(next <= Utc::now() - ChronoDuration::seconds(59));

        let claimed = claim_due_jobs(&mut connection, "owner", 16).await.unwrap();
        assert_eq!(claimed.len(), 16);
        assert!(
            claimed.iter().all(|job| job.cron_name.is_empty()),
            "one-shot jobs are claimed before crons"
        );
    }

    #[tokio::test]
    async fn pruning_deletes_only_old_completed_jobs() {
        let Some(fixture) = control_fixture("prune").await else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping scheduler prune test");
            return;
        };
        sqlx::query(
            r#"INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,status,completed_at)
               VALUES
                 ('old_1','project','t','jobs.run',now() - interval '9 days',now() - interval '9 days','completed',now() - interval '9 days'),
                 ('old_2','project','t','jobs.run',now() - interval '8 days',now() - interval '8 days','completed',now() - interval '8 days'),
                 ('recent','project','t','jobs.run',now() - interval '1 day',now() - interval '1 day','completed',now() - interval '1 day'),
                 ('pending','project','t','jobs.run',now() - interval '9 days',now() - interval '9 days','pending',NULL)"#,
        )
        .execute(&fixture.pool)
        .await
        .unwrap();
        let mut connection = fixture.pool.acquire().await.unwrap();
        let before = Utc::now() - COMPLETED_JOB_RETENTION;
        assert_eq!(
            prune_completed_jobs(&mut connection, before, 1)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            prune_completed_jobs(&mut connection, before, 10)
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            prune_completed_jobs(&mut connection, before, 10)
                .await
                .unwrap(),
            0
        );
        let left: Vec<String> =
            sqlx::query_scalar("SELECT id FROM gonvex_scheduled_jobs ORDER BY id")
                .fetch_all(&mut *connection)
                .await
                .unwrap();
        assert_eq!(left, ["pending", "recent"]);
    }

    #[test]
    fn wake_keys_ignore_object_key_order_but_not_values() {
        let a = serde_json::json!({"tenant": "t", "ids": [1, 2]});
        let b: Value = serde_json::from_str(r#"{"ids":[1,2],"tenant":"t"}"#).unwrap();
        assert_eq!(wake_key("cleaning.tick", &a), wake_key("cleaning.tick", &b));
        assert_ne!(
            wake_key("cleaning.tick", &a),
            wake_key(
                "cleaning.tick",
                &serde_json::json!({"tenant": "t", "ids": [2, 1]})
            )
        );
        assert_ne!(
            wake_key("cleaning.tick", &a),
            wake_key("agents.dispatch", &a)
        );
    }

    fn wake<'a>(id: &'a str, run_at: DateTime<Utc>, coalesce: bool) -> OneShotJob<'a> {
        OneShotJob {
            id,
            project_id: "project",
            tenant_id: "tenant-a",
            function: "cleaning.tick",
            args: serde_json::json!({}),
            run_at,
            provenance: Value::Null,
            coalesce,
        }
    }

    async fn pending_wakes(pool: &sqlx::PgPool) -> Vec<(String, DateTime<Utc>)> {
        sqlx::query_as(
            "SELECT id,run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND function_path='cleaning.tick' ORDER BY run_at",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    /// The building block that replaces every-minute crons: every write may wake
    /// a due-work handler, yet at most one run waits per tenant.
    #[tokio::test]
    async fn wakes_coalesce_to_one_pending_run_at_the_earliest_time() {
        let Some(fixture) = control_fixture("wake").await else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping scheduler wake test");
            return;
        };
        let mut connection = fixture.pool.acquire().await.unwrap();
        let base = DateTime::parse_from_rfc3339("2030-01-01T10:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let first = insert_one_shot_job(&mut connection, wake("job_1", base, true))
            .await
            .unwrap();
        assert_eq!(first, "job_1");
        // A later wake does not postpone; an earlier one pulls the run forward.
        for (id, minutes) in [("job_2", 30), ("job_3", -20), ("job_4", 5)] {
            let kept = insert_one_shot_job(
                &mut connection,
                wake(id, base + ChronoDuration::minutes(minutes), true),
            )
            .await
            .unwrap();
            assert_eq!(kept, "job_1", "wake {id} joins the pending run");
        }
        assert_eq!(
            pending_wakes(&fixture.pool).await,
            [("job_1".to_owned(), base - ChronoDuration::minutes(20))]
        );

        // Plain runAt jobs never coalesce.
        insert_one_shot_job(&mut connection, wake("job_plain", base, false))
            .await
            .unwrap();
        assert_eq!(pending_wakes(&fixture.pool).await.len(), 2);
        sqlx::query("DELETE FROM gonvex_scheduled_jobs WHERE id='job_plain'")
            .execute(&mut *connection)
            .await
            .unwrap();

        // While job_1 runs, a new wake queues exactly one more run.
        sqlx::query("UPDATE gonvex_scheduled_jobs SET claim_token='owner:1',lease_until=now()+interval '2 minutes' WHERE id='job_1'")
            .execute(&mut *connection)
            .await
            .unwrap();
        for id in ["job_5", "job_6"] {
            insert_one_shot_job(&mut connection, wake(id, base, true))
                .await
                .unwrap();
        }
        let waiting: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM gonvex_scheduled_jobs WHERE status='pending' AND claim_token='' ORDER BY id",
        )
        .fetch_all(&mut *connection)
        .await
        .unwrap();
        assert_eq!(waiting, ["job_5"]);

        // Replaying an outbox promotion for a job that already ran is a no-op.
        sqlx::query("UPDATE gonvex_scheduled_jobs SET status='completed',completed_at=now(),claim_token='' WHERE id='job_1'")
            .execute(&mut *connection)
            .await
            .unwrap();
        let replayed = insert_one_shot_job(&mut connection, wake("job_1", base, true))
            .await
            .unwrap();
        assert_eq!(replayed, "job_1");
        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM gonvex_scheduled_jobs")
            .fetch_one(&mut *connection)
            .await
            .unwrap();
        assert_eq!(total, 2, "job_1 completed, job_5 waiting");
    }

    /// Databases that already piled up copies must still accept the unique index.
    #[tokio::test]
    async fn schema_upgrade_collapses_an_existing_cron_backlog() {
        let Some(fixture) = control_fixture("upgrade").await else {
            eprintln!("GONVEX_TEST_POSTGRES_URL is not set; skipping scheduler upgrade test");
            return;
        };
        sqlx::query("DROP INDEX gonvex_scheduled_jobs_one_pending_cron")
            .execute(&fixture.pool)
            .await
            .unwrap();
        sqlx::query(
            r#"INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name)
               SELECT 'copy_' || n,'project','tenant-a','slas.evaluate',
                      now() - n * interval '1 minute',now() - n * interval '1 minute','slas'
               FROM generate_series(1,500) AS n"#,
        )
        .execute(&fixture.pool)
        .await
        .unwrap();
        let schema: String = sqlx::query_scalar("SELECT current_schema()")
            .fetch_one(&fixture.pool)
            .await
            .unwrap();
        let base = std::env::var("GONVEX_TEST_POSTGRES_URL").unwrap();
        let separator = if base.contains('?') { '&' } else { '?' };
        let url = format!("{base}{separator}options=-csearch_path%3D{schema}");
        let pools = gonvex_store::PoolRegistry::new(gonvex_store::PoolLimits::default());
        gonvex_store::ControlPlane::connect(&url, pools, Default::default())
            .await
            .unwrap();
        let survivors: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='slas'",
        )
        .fetch_all(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(survivors, ["copy_1"], "the newest copy survives");
    }
}
