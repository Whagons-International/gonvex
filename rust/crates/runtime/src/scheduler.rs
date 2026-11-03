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

/// Earliest unclaimed run time (Unix ms). One probe per partial index, never a
/// table scan. `min` over the union skips a side that has no pending job.
const NEXT_DUE_SQL: &str = r#"SELECT min(run_at) FROM (
                 SELECT (SELECT run_at FROM gonvex_scheduled_jobs
                  WHERE status='pending' AND cron_name='' AND claim_token=''
                  ORDER BY run_at,id LIMIT 1) AS run_at
                 UNION ALL
                 SELECT (SELECT run_at FROM gonvex_scheduled_jobs
                  WHERE status='pending' AND cron_name<>'' AND claim_token=''
                  ORDER BY run_at,id LIMIT 1))"#;

/// Claims due work. The Control Plane has one writer connection, so the claim
/// is serialized without row locks; leases are two minutes in milliseconds.
const CLAIM_DUE_SQL: &str = r#"UPDATE gonvex_scheduled_jobs SET
                 claim_sequence=claim_sequence+1,
                 claim_token=$2 || ':' || CAST(claim_sequence+1 AS TEXT),
                 lease_until=CAST(unixepoch('subsec')*1000 AS INTEGER)+120000,
                 updated_at=CAST(unixepoch('subsec')*1000 AS INTEGER)
               WHERE id IN (
                 SELECT id FROM gonvex_scheduled_jobs
                 WHERE status='pending' AND run_at<=CAST(unixepoch('subsec')*1000 AS INTEGER)
                   AND (claim_token='' OR lease_until IS NULL
                        OR lease_until<=CAST(unixepoch('subsec')*1000 AS INTEGER))
                 ORDER BY (cron_name<>'') ASC,run_at,id LIMIT $1
               )
               RETURNING id,project_id,tenant_id,function_path,
                         args,run_at,scheduled_for,cron_name,claim_token,provenance"#;

fn millis_to_datetime(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

#[derive(Clone)]
pub struct Scheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    owner: String,
    stop: watch::Sender<bool>,
    wake: Notify,
    task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    lane: crate::scheduled_lane::ScheduledLane,
    stagger: Stagger,
}

/// Per-tenant stagger of cron runs.
///
/// Without it, every tenant's five-field cron fires at the same instant and
/// every interval cron is anchored to process start, so all tenants run each
/// cron together: a thundering herd every minute and a bigger one at :00.
///
/// With it, each (project, tenant, cron) gets a fixed offset from a hash:
/// * interval crons run at Unix-time phase `hash mod interval`, so runs stay
///   exactly one interval apart, tenants spread evenly over the interval, and
///   a restart neither shifts the phase nor runs a cron twice in one interval;
/// * five-field crons run `hash mod min(gap, spread)` after each nominal
///   occurrence, where `gap` is the time to the next occurrence and `spread`
///   is `GONVEX_SCHEDULER_CRON_SPREAD_MS` (default 10 minutes). A run is
///   therefore always inside its own period (never two runs per period) and
///   at most `min(period, spread) - 1 ms` late.
///
/// `GONVEX_SCHEDULER_STAGGER=0` restores nominal times.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Stagger {
    enabled: bool,
    spread_ms: i64,
}

impl Stagger {
    const DEFAULT_SPREAD_MS: i64 = 600_000;
    #[cfg(test)]
    const OFF: Self = Self {
        enabled: false,
        spread_ms: 0,
    };

    fn from_env() -> Self {
        let enabled = !matches!(
            std::env::var("GONVEX_SCHEDULER_STAGGER")
                .ok()
                .as_deref()
                .map(str::trim),
            Some("0" | "false" | "off")
        );
        let spread_ms = std::env::var("GONVEX_SCHEDULER_CRON_SPREAD_MS")
            .ok()
            .and_then(|value| value.trim().parse::<i64>().ok())
            .filter(|value| *value >= 0)
            .unwrap_or(Self::DEFAULT_SPREAD_MS);
        Self { enabled, spread_ms }
    }
}

/// The stable hash behind a tenant's cron offset.
fn stagger_key(project: &str, tenant: &str, cron: &str) -> u64 {
    let digest = Sha256::digest(format!("{project}\0{tenant}\0{cron}").as_bytes());
    u64::from_be_bytes(digest[..8].try_into().unwrap_or_default())
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
                lane: crate::scheduled_lane::ScheduledLane::new(
                    crate::scheduled_lane::LaneConfig::from_env(),
                ),
                stagger: Stagger::from_env(),
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
        // tenants, so it runs beside the claim loop instead of before it. It
        // must run in its own task: a sweep future pinned in this loop is only
        // polled inside `select!`, so while the loop body awaits claims it sat
        // suspended holding whatever it held (a pool connection, or the store
        // registry lock while opening a tenant file), and the claim then
        // waited on it forever. One task still means no overlapping sweeps.
        let sweeper = {
            let runtime = runtime.clone();
            let mut stop = self.inner.stop.subscribe();
            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = async {
                            runtime.drain_all_action_outboxes().await;
                            tokio::time::sleep(OUTBOX_SWEEP_INTERVAL).await;
                        } => {}
                        changed = stop.changed() => {
                            if changed.is_err() || *stop.borrow() { break; }
                        }
                    }
                }
            })
        };
        let mut last_prune: Option<std::time::Instant> = None;
        // Sleep until the next job is due instead of polling. Enqueues, cron
        // syncs and finished cron runs wake the loop early through `wake`.
        let mut idle = std::time::Duration::ZERO;
        loop {
            tokio::select! {
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
            // Claim only as many jobs as the lane can start now, so a claimed
            // job never waits behind the concurrency cap with its lease running.
            let permits = tokio::select! {
                permits = self.inner.lane.acquire_free() => permits,
                changed = stop.changed() => {
                    if changed.is_err() || *stop.borrow() { break; }
                    continue;
                }
            };
            let limit = (permits.len() as i64).clamp(1, CLAIM_BATCH);
            let jobs = match self.claim_due(&runtime, limit).await {
                Ok(jobs) => jobs,
                Err(error) => {
                    tracing::error!(error = %error, "claim scheduled work");
                    idle = std::time::Duration::from_secs(1);
                    continue;
                }
            };
            idle = if jobs.len() as i64 == limit {
                std::time::Duration::ZERO
            } else {
                self.until_next_due(&runtime).await
            };
            for (job, permit) in jobs.into_iter().zip(permits) {
                let scheduler = self.clone();
                let runtime = runtime.clone();
                self.inner.lane.spawn(async move {
                    scheduler.execute(&runtime, job).await;
                    drop(permit);
                });
            }
        }
        sweeper.abort();
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
        // The Control Plane writer is a single connection, so this transaction
        // already excludes a concurrent cron sync for the project.
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
        // One scheduled run per tenant at a time, and user calls first.
        let tenant = self.inner.lane.tenant_lock(&job.tenant_id);
        let tenant_turn = tenant.lock_owned().await;
        crate::scheduled_lane::yield_to_users(self.inner.lane.config().yield_limit).await;
        let started = gonvex_profiler::now_ns();
        let start_delay = (Utc::now() - job.scheduled_for)
            .num_nanoseconds()
            .unwrap_or(0)
            .max(0) as u64;
        let skip = self.due_check(runtime, &job).await == Some(false);
        let (result, mut sample) = if skip {
            (Ok(()), gonvex_profiler::cron::CronSample::default())
        } else {
            crate::cron_profile::scope(self.execute_job(runtime, &job)).await
        };
        drop(tenant_turn);
        sample.total_ns = gonvex_profiler::now_ns().saturating_sub(started);
        sample.start_delay_ns = start_delay;
        sample.error = result.is_err();
        sample.skipped = skip;
        gonvex_profiler::cron::record(cron_slot(runtime, &job).await, &sample);
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
            r#"UPDATE gonvex_scheduled_jobs
               SET lease_until=CAST(unixepoch('subsec')*1000 AS INTEGER)+120000,
                   updated_at=CAST(unixepoch('subsec')*1000 AS INTEGER)
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

    /// Evaluates the cron's `dueWhen` pre-check on a tenant reader. `None`
    /// when the cron has none or it failed (the run then happens).
    async fn due_check(&self, runtime: &Runtime, job: &ScheduledJob) -> Option<bool> {
        if job.cron_name.is_empty() {
            return None;
        }
        let module = runtime.inner.modules.project(&job.project_id).await?;
        let statement = module
            .crons
            .iter()
            .find(|spec| spec.name == job.cron_name)?
            .due_when
            .clone()?;
        let control = runtime.inner.control_plane.read().await.clone()?;
        let checked = async {
            let route = control
                .tenant_routes(&job.project_id)
                .await
                .map_err(|error| error.to_string())?
                .into_iter()
                .find(|route| route.tenant_id == job.tenant_id)
                .ok_or_else(|| "scheduled tenant is unavailable".to_owned())?;
            let mut transaction = control
                .begin_tenant_transaction(&route, true)
                .await
                .map_err(|error| error.to_string())?;
            let row = sqlx::query(statement.trim())
                .bind(Utc::now().timestamp_millis())
                .fetch_optional(&mut **transaction.transaction())
                .await
                .map_err(|error| error.to_string());
            let _ = transaction.rollback().await;
            Ok::<bool, String>(row?.is_some_and(|row| due_value_is_truthy(&row)))
        }
        .await;
        match checked {
            Ok(due) => Some(due),
            Err(error) => {
                tracing::warn!(cron = %job.cron_name, error = %error, "cron dueWhen failed; running the cron");
                None
            }
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
            r#"UPDATE gonvex_scheduled_jobs SET status='completed',
                 completed_at=CAST(unixepoch('subsec')*1000 AS INTEGER),
                 claim_token='',lease_until=NULL,
                 updated_at=CAST(unixepoch('subsec')*1000 AS INTEGER)
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
        let key = stagger_key(&job.project_id, &job.tenant_id, &spec.name);
        next_cron_run(spec, job.scheduled_for, Utc::now(), key, self.inner.stagger)
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
            for route in targets {
                let key = stagger_key(project, &route.tenant_id, &spec.name);
                let Some(next) = next_run(spec, now, key, self.inner.stagger)? else {
                    continue;
                };
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
    connection: &mut sqlx::SqliteConnection,
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
           DO UPDATE SET run_at=min(gonvex_scheduled_jobs.run_at,EXCLUDED.run_at),
                         scheduled_for=min(gonvex_scheduled_jobs.scheduled_for,EXCLUDED.scheduled_for),
                         updated_at=CAST(unixepoch('subsec')*1000 AS INTEGER)
           RETURNING id"#,
    )
    .bind(job.id)
    .bind(job.project_id)
    .bind(job.tenant_id)
    .bind(job.function)
    .bind(Json(job.args))
    .bind(job.run_at.timestamp_millis())
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
    connection: &mut sqlx::SqliteConnection,
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
    .bind(run_at.timestamp_millis())
    .bind(&job.cron_name)
    .bind(Json(job.provenance.clone()))
    .execute(connection)
    .await?;
    Ok(())
}

async fn claim_due_jobs(
    connection: &mut sqlx::SqliteConnection,
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
            scheduled_for: millis_to_datetime(row.get("scheduled_for")),
            cron_name: row.get("cron_name"),
            claim_token: row.get("claim_token"),
            provenance: row
                .get::<Option<Json<Value>>, _>("provenance")
                .map(|value| value.0)
                .unwrap_or(Value::Null),
        })
        .collect())
}

async fn prune_completed_jobs(
    connection: &mut sqlx::SqliteConnection,
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
    .bind(before.timestamp_millis())
    .bind(limit)
    .execute(connection)
    .await?;
    Ok(result.rows_affected())
}

async fn next_due_at(
    connection: &mut sqlx::SqliteConnection,
) -> Result<Option<DateTime<Utc>>, sqlx::Error> {
    let next: Option<i64> = sqlx::query_scalar(NEXT_DUE_SQL).fetch_one(connection).await?;
    Ok(next.map(millis_to_datetime))
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
    key: u64,
    stagger: Stagger,
) -> Result<Option<DateTime<Utc>>, String> {
    match next_run(spec, scheduled_for, key, stagger)? {
        Some(next) if next <= now => next_run(spec, now, key, stagger),
        next => Ok(next),
    }
}

/// `dueWhen` result: the first column of the first row is truthy.
fn due_value_is_truthy(row: &sqlx::sqlite::SqliteRow) -> bool {
    use sqlx::{TypeInfo, ValueRef};
    let Ok(value) = row.try_get_raw(0) else {
        return false;
    };
    if value.is_null() {
        return false;
    }
    match value.type_info().name() {
        "INTEGER" | "BOOLEAN" => row.try_get::<i64, _>(0).is_ok_and(|value| value != 0),
        "REAL" => row.try_get::<f64, _>(0).is_ok_and(|value| value != 0.0),
        "TEXT" => row
            .try_get::<String, _>(0)
            .is_ok_and(|value| !value.is_empty() && value != "0"),
        _ => true,
    }
}

/// The next run strictly after `after`, staggered per tenant (see [`Stagger`]).
fn next_run(
    spec: &CronSpec,
    after: DateTime<Utc>,
    key: u64,
    stagger: Stagger,
) -> Result<Option<DateTime<Utc>>, String> {
    if !stagger.enabled {
        return next_occurrence(spec, after);
    }
    if let Some(interval_ms) = spec.interval_ms {
        let interval = i64::try_from(interval_ms)
            .map_err(|_| format!("cron {:?} interval is too large", spec.name))?;
        let phase = (key % interval_ms) as i64;
        let after_ms = after.timestamp_millis();
        let next = after_ms - (after_ms - phase).rem_euclid(interval) + interval;
        return Ok(Some(millis_to_datetime(next)));
    }
    let schedule = parse_schedule(spec)?;
    // The latest nominal occurrence at or before `after` may still be due at
    // its staggered time.
    if let Some(previous) = latest_occurrence_at_or_before(&schedule, after) {
        if let Some(at) = staggered(&schedule, previous, key, stagger.spread_ms) {
            if at > after {
                return Ok(Some(at));
            }
        }
    }
    let Some(nominal) = schedule.after(&after).next() else {
        return Ok(None);
    };
    Ok(staggered(&schedule, nominal, key, stagger.spread_ms))
}

/// The latest nominal occurrence `<= at` (cron resolution is one second).
fn latest_occurrence_at_or_before(schedule: &Schedule, at: DateTime<Utc>) -> Option<DateTime<Utc>> {
    let mut earlier = schedule.after(&(at + ChronoDuration::seconds(1)));
    let mut previous = earlier.next_back();
    while previous.is_some_and(|occurrence| occurrence > at) {
        previous = earlier.next_back();
    }
    previous
}

/// `nominal` plus this tenant's offset inside `min(gap to the next nominal
/// occurrence, spread)`.
fn staggered(
    schedule: &Schedule,
    nominal: DateTime<Utc>,
    key: u64,
    spread_ms: i64,
) -> Option<DateTime<Utc>> {
    let gap = schedule
        .after(&nominal)
        .next()
        .map_or(spread_ms, |next| (next - nominal).num_milliseconds());
    let window = gap.min(spread_ms);
    if window <= 1 {
        return Some(nominal);
    }
    let offset = (key % window as u64) as i64;
    nominal.checked_add_signed(ChronoDuration::milliseconds(offset))
}

fn next_occurrence(spec: &CronSpec, after: DateTime<Utc>) -> Result<Option<DateTime<Utc>>, String> {
    if let Some(interval_ms) = spec.interval_ms {
        let interval = i64::try_from(interval_ms)
            .map_err(|_| format!("cron {:?} interval is too large", spec.name))?;
        return Ok(Some(after + ChronoDuration::milliseconds(interval)));
    }
    Ok(parse_schedule(spec)?.after(&after).next())
}

fn parse_schedule(spec: &CronSpec) -> Result<Schedule, String> {
    let expression = spec.expression.as_deref().unwrap_or("").trim();
    let fields = expression.split_whitespace().count();
    let normalized = if fields == 5 {
        format!("0 {expression}")
    } else {
        expression.to_owned()
    };
    Schedule::from_str(&normalized)
        .map_err(|error| format!("cron {:?} has invalid expression: {error}", spec.name))
}

/// The profiler cron-table slot of a claimed job (one-shot jobs share slot 0).
async fn cron_slot(runtime: &Runtime, job: &ScheduledJob) -> u16 {
    if job.cron_name.is_empty() || !gonvex_profiler::enabled() {
        return gonvex_profiler::cron::ONE_SHOT_SLOT;
    }
    match runtime.inner.modules.project(&job.project_id).await {
        Some(module) => module
            .crons
            .iter()
            .find(|spec| spec.name == job.cron_name)
            .map_or(gonvex_profiler::cron::ONE_SHOT_SLOT, |spec| spec.slot),
        None => gonvex_profiler::cron::ONE_SHOT_SLOT,
    }
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

    /// A fresh data directory under `GONVEX_TEST_DATA_DIR` when set (CI volume),
    /// otherwise a system tempdir.
    fn test_data_dir() -> tempfile::TempDir {
        match std::env::var("GONVEX_TEST_DATA_DIR")
            .ok()
            .filter(|value| !value.trim().is_empty())
        {
            Some(root) => {
                std::fs::create_dir_all(&root).unwrap();
                tempfile::tempdir_in(root).unwrap()
            }
            None => tempfile::tempdir().unwrap(),
        }
    }


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
            due_when: None,
            slot: 0,
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
            due_when: None,
            slot: 0,
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
            next_cron_run(&spec, on_time, now, 0, Stagger::OFF).unwrap(),
            Some(on_time + ChronoDuration::minutes(1))
        );
        let six_days_late = now - ChronoDuration::days(6);
        let next = next_cron_run(&spec, six_days_late, now, 0, Stagger::OFF).unwrap().unwrap();
        assert!(next > now && next <= now + ChronoDuration::minutes(1));
    }

    const STAGGER: Stagger = Stagger {
        enabled: true,
        spread_ms: Stagger::DEFAULT_SPREAD_MS,
    };

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text).unwrap().with_timezone(&Utc)
    }

    fn expression_cron(expression: &str) -> CronSpec {
        CronSpec {
            expression: Some(expression.to_owned()),
            ..minute_cron()
        }
    }

    fn interval_cron(interval_ms: u64) -> CronSpec {
        CronSpec {
            interval_ms: Some(interval_ms),
            expression: None,
            ..minute_cron()
        }
    }

    #[test]
    fn stagger_offsets_are_deterministic_and_spread_tenants() {
        let key = stagger_key("project", "tenant-a", "jobs.every-minute");
        assert_eq!(key, stagger_key("project", "tenant-a", "jobs.every-minute"));
        assert_ne!(key, stagger_key("project", "tenant-b", "jobs.every-minute"));
        assert_ne!(key, stagger_key("project", "tenant-a", "jobs.hourly"));
        let now = at("2026-11-03T12:59:59.500Z");
        for spec in [expression_cron("* * * * *"), interval_cron(60_000), expression_cron("0 * * * *")] {
            let first = next_run(&spec, now, key, STAGGER).unwrap();
            assert_eq!(first, next_run(&spec, now, key, STAGGER).unwrap(), "same inputs, same run");
            // 26 tenants no longer share one instant.
            let distinct: std::collections::BTreeSet<_> = (0..26)
                .map(|tenant| {
                    let key = stagger_key("project", &format!("tenant-{tenant}"), &spec.name);
                    next_run(&spec, now, key, STAGGER).unwrap().unwrap()
                })
                .collect();
            assert!(distinct.len() >= 24, "{:?}: {} distinct run times", spec.expression, distinct.len());
        }
    }

    /// Consecutive staggered runs of one tenant: exactly one per nominal
    /// period, each no earlier than its occurrence and less than
    /// min(period, spread) after it.
    #[test]
    fn staggered_runs_keep_the_period_and_bound_the_delay() {
        let cases: [(&str, i64, i64); 4] = [
            ("* * * * *", 60_000, 60_000),
            ("*/15 * * * *", 900_000, 600_000),
            ("0 * * * *", 3_600_000, 600_000),
            ("0 8 * * *", 86_400_000, 600_000),
        ];
        for tenant in 0..20 {
            for (expression, period, max_delay) in cases {
                let spec = expression_cron(expression);
                let schedule = parse_schedule(&spec).unwrap();
                let key = stagger_key("project", &format!("tenant-{tenant}"), &spec.name);
                let mut after = at("2026-11-03T07:59:00Z");
                let mut previous: Option<DateTime<Utc>> = None;
                for _ in 0..6 {
                    let run = next_run(&spec, after, key, STAGGER).unwrap().unwrap();
                    let nominal = latest_occurrence_at_or_before(&schedule, run).unwrap();
                    let delay = (run - nominal).num_milliseconds();
                    assert!((0..max_delay).contains(&delay), "{expression}: delay {delay} ms");
                    if let Some(previous) = previous {
                        assert_eq!((run - previous).num_milliseconds(), period, "{expression}");
                    }
                    previous = Some(run);
                    after = run;
                }
            }
        }
        // Interval crons: a fixed Unix-time phase, so runs are exactly one
        // interval apart and a restart lands on the same lattice.
        let spec = interval_cron(60_000);
        let key = stagger_key("project", "tenant-a", &spec.name);
        let first = next_run(&spec, at("2026-11-03T12:00:00Z"), key, STAGGER).unwrap().unwrap();
        assert!(first > at("2026-11-03T12:00:00Z") && first <= at("2026-11-03T12:01:00Z"));
        let second = next_run(&spec, first, key, STAGGER).unwrap().unwrap();
        assert_eq!((second - first).num_milliseconds(), 60_000);
        let after_restart = next_run(&spec, first + ChronoDuration::seconds(17), key, STAGGER)
            .unwrap()
            .unwrap();
        assert_eq!(after_restart, second, "a restart keeps the phase");
        // Off restores nominal times.
        assert_eq!(
            next_run(&expression_cron("0 * * * *"), at("2026-11-03T12:10:00Z"), key, Stagger::OFF).unwrap(),
            Some(at("2026-11-03T13:00:00Z"))
        );
    }

    /// A process that starts between an occurrence and this tenant's offset
    /// still runs that occurrence instead of skipping a whole period.
    #[test]
    fn startup_inside_the_stagger_window_keeps_the_current_occurrence() {
        let spec = expression_cron("0 * * * *");
        let key = (0..)
            .map(|tenant| stagger_key("project", &format!("tenant-{tenant}"), &spec.name))
            .find(|key| key % 600_000 > 300_000)
            .unwrap();
        let run = next_run(&spec, at("2026-11-03T12:05:00Z"), key, STAGGER).unwrap().unwrap();
        assert!(run > at("2026-11-03T12:05:00Z") && run < at("2026-11-03T12:10:00Z"), "{run}");
        let next = next_cron_run(&spec, run, run, key, STAGGER).unwrap().unwrap();
        assert_eq!((next - run).num_minutes(), 60);
        // Fell behind: the next run is in the future, never replayed.
        let now = run + ChronoDuration::hours(5);
        let late = next_cron_run(&spec, run, now, key, STAGGER).unwrap().unwrap();
        assert!(late > now && late <= now + ChronoDuration::hours(1));
    }

    #[tokio::test]
    async fn due_when_values_follow_sql_truthiness() {
        let fixture = control_fixture().await;
        for (sql, expected) in [
            ("SELECT 1", true),
            ("SELECT 0", false),
            ("SELECT NULL", false),
            ("SELECT ''", false),
            ("SELECT 'x'", true),
            ("SELECT 0.5", true),
            ("SELECT EXISTS (SELECT 1 WHERE 1 = 0)", false),
            ("SELECT 1 WHERE 0", false),
        ] {
            let row = sqlx::query(sql).fetch_optional(&fixture.reader).await.unwrap();
            assert_eq!(row.is_some_and(|row| due_value_is_truthy(&row)), expected, "{sql}");
        }
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

    const NOW: &str = "CAST(unixepoch('subsec')*1000 AS INTEGER)";

    struct ControlFixture {
        /// The Control Plane writer (one connection).
        pool: sqlx::SqlitePool,
        /// Read-only snapshots, usable while a test holds the writer.
        reader: sqlx::SqlitePool,
        url: String,
        _data: tempfile::TempDir,
    }

    /// A fresh SQLite Control Plane with the production DDL applied.
    async fn control_fixture() -> ControlFixture {
        let data = test_data_dir();
        let url = gonvex_store::database_url(&data.path().join("control.db"));
        let pools = gonvex_store::PoolRegistry::new(gonvex_store::PoolLimits::default());
        gonvex_store::ControlPlane::connect(&url, pools.clone(), Default::default())
            .await
            .unwrap();
        ControlFixture {
            pool: pools.pool(&url).await.unwrap(),
            reader: pools.reader(&url).await.unwrap(),
            url,
            _data: data,
        }
    }

    /// Sequence 1..=count for bulk fixtures (SQLite has no generate_series).
    fn series(count: i64) -> String {
        format!("WITH RECURSIVE series(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM series WHERE n<{count})")
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

    async fn pending_runs(pool: &sqlx::SqlitePool, tenant: &str) -> i64 {
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
        let fixture = control_fixture().await;
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
        assert_eq!(pending_runs(&fixture.reader, "tenant-a").await, 1);
        assert_eq!(pending_runs(&fixture.reader, "tenant-b").await, 1);

        // Finishing the pending run makes room for the next one, as finish() does.
        let claimed = claim_due_jobs(&mut connection, "owner", 16).await.unwrap();
        assert_eq!(claimed.len(), 2);
        sqlx::query(&format!("UPDATE gonvex_scheduled_jobs SET status='completed',completed_at={NOW} WHERE tenant_id='tenant-a'"))
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
        assert_eq!(pending_runs(&fixture.reader, "tenant-a").await, 1);
    }

    /// Every claim used to scan the whole table, completed history included, so
    /// each claim got slower as the table grew until the scheduler fell behind.
    #[tokio::test]
    async fn claims_use_an_index_instead_of_scanning_job_history() {
        let fixture = control_fixture().await;
        sqlx::query(&format!(
            r#"{} INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name,status,completed_at)
               SELECT 'done_' || n,'project','tenant-' || (n % 30),'slas.evaluate',
                      {NOW} - n * 1000,{NOW} - n * 1000,
                      'slas','completed',{NOW} - n * 1000
               FROM series"#,
            series(100_000)
        ))
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query(&format!(
            r#"{} INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name)
               SELECT 'due_' || n,'project','tenant-' || n,'slas.evaluate',
                      {NOW} - 60000,{NOW} - 60000,
                      CASE WHEN n % 2 = 0 THEN 'slas' ELSE '' END
               FROM series"#,
            series(200)
        ))
        .execute(&fixture.pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE gonvex_scheduled_jobs")
            .execute(&fixture.pool)
            .await
            .unwrap();
        let mut connection = fixture.pool.acquire().await.unwrap();
        // A full table scan reads "SCAN gonvex_scheduled_jobs" with no index.
        let table_scan = |plan: &[String]| {
            assert!(!plan.is_empty(), "EXPLAIN QUERY PLAN returned no rows");
            plan.iter().any(|line| {
                line.starts_with("SCAN gonvex_scheduled_jobs") && !line.contains("USING")
            })
        };
        let plan: Vec<String> = sqlx::query(&format!("EXPLAIN QUERY PLAN {CLAIM_DUE_SQL}"))
            .bind(16_i64)
            .bind("owner")
            .fetch_all(&mut *connection)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        assert!(!table_scan(&plan), "claim scans the job table:\n{}", plan.join("\n"));

        let plan: Vec<String> = sqlx::query(&format!("EXPLAIN QUERY PLAN {NEXT_DUE_SQL}"))
            .fetch_all(&mut *connection)
            .await
            .unwrap()
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect();
        assert!(
            !table_scan(&plan),
            "next-due lookup scans the job table:\n{}",
            plan.join("\n")
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
        let fixture = control_fixture().await;
        const DAY: i64 = 86_400_000;
        sqlx::query(&format!(
            r#"INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,status,completed_at)
               VALUES
                 ('old_1','project','t','jobs.run',{NOW} - 9*{DAY},{NOW} - 9*{DAY},'completed',{NOW} - 9*{DAY}),
                 ('old_2','project','t','jobs.run',{NOW} - 8*{DAY},{NOW} - 8*{DAY},'completed',{NOW} - 8*{DAY}),
                 ('recent','project','t','jobs.run',{NOW} - {DAY},{NOW} - {DAY},'completed',{NOW} - {DAY}),
                 ('pending','project','t','jobs.run',{NOW} - 9*{DAY},{NOW} - 9*{DAY},'pending',NULL)"#
        ))
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

    async fn pending_wakes(pool: &sqlx::SqlitePool) -> Vec<(String, DateTime<Utc>)> {
        sqlx::query_as::<_, (String, i64)>(
            "SELECT id,run_at FROM gonvex_scheduled_jobs WHERE status='pending' AND function_path='cleaning.tick' ORDER BY run_at",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .into_iter()
        .map(|(id, run_at)| (id, millis_to_datetime(run_at)))
        .collect()
    }

    /// The building block that replaces every-minute crons: every write may wake
    /// a due-work handler, yet at most one run waits per tenant.
    #[tokio::test]
    async fn wakes_coalesce_to_one_pending_run_at_the_earliest_time() {
        let fixture = control_fixture().await;
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
            pending_wakes(&fixture.reader).await,
            [("job_1".to_owned(), base - ChronoDuration::minutes(20))]
        );

        // Plain runAt jobs never coalesce.
        insert_one_shot_job(&mut connection, wake("job_plain", base, false))
            .await
            .unwrap();
        assert_eq!(pending_wakes(&fixture.reader).await.len(), 2);
        sqlx::query("DELETE FROM gonvex_scheduled_jobs WHERE id='job_plain'")
            .execute(&mut *connection)
            .await
            .unwrap();

        // While job_1 runs, a new wake queues exactly one more run.
        sqlx::query(&format!("UPDATE gonvex_scheduled_jobs SET claim_token='owner:1',lease_until={NOW}+120000 WHERE id='job_1'"))
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
        sqlx::query(&format!("UPDATE gonvex_scheduled_jobs SET status='completed',completed_at={NOW},claim_token='' WHERE id='job_1'"))
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
        let fixture = control_fixture().await;
        sqlx::query("DROP INDEX gonvex_scheduled_jobs_one_pending_cron")
            .execute(&fixture.pool)
            .await
            .unwrap();
        sqlx::query(&format!(
            r#"{} INSERT INTO gonvex_scheduled_jobs
                 (id,project_id,tenant_id,function_path,run_at,scheduled_for,cron_name)
               SELECT 'copy_' || n,'project','tenant-a','slas.evaluate',
                      {NOW} - n * 60000,{NOW} - n * 60000,'slas'
               FROM series"#,
            series(500)
        ))
        .execute(&fixture.pool)
        .await
        .unwrap();
        // A second start of the Control Plane reapplies the schema upgrade.
        let pools = gonvex_store::PoolRegistry::new(gonvex_store::PoolLimits::default());
        gonvex_store::ControlPlane::connect(&fixture.url, pools.clone(), Default::default())
            .await
            .unwrap();
        pools.close().await;
        let survivors: Vec<String> = sqlx::query_scalar(
            "SELECT id FROM gonvex_scheduled_jobs WHERE status='pending' AND cron_name='slas'",
        )
        .fetch_all(&fixture.pool)
        .await
        .unwrap();
        assert_eq!(survivors, ["copy_1"], "the newest copy survives");
    }
}
