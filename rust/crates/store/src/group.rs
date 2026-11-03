//! Group commit on a database's single writer.
//!
//! Every commit with `synchronous=FULL` waits for one WAL sync. Each database
//! has one writer, so commits run one after another and a 40 ms sync caps a
//! tenant at ~25 commits per second while the CPU idles. Write transactions
//! therefore share one SQLite transaction while others are waiting:
//!
//! * a member joins the open group (or opens one) inside its own savepoint;
//! * on commit it finalizes its own revision, releases its savepoint, and, if
//!   another write transaction is already waiting for the writer, leaves the
//!   group open for it and waits for the group's `COMMIT`;
//! * otherwise it commits at once, so an idle database adds no delay;
//! * the group commits with one sync and only then publishes every member's
//!   revision, in order. No caller is answered before its commit; a failed
//!   commit fails every member.
//!
//! A member rolls back only to its own savepoint. A member dropped without
//! finishing (a cancelled future) leaves partial writes in the shared
//! transaction that cannot be undone from `Drop`, so it aborts the open group
//! and every waiting member receives an error.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::{Sqlite, Transaction};
use tokio::sync::{oneshot, OwnedMutexGuard};

use crate::feed::{CommitNotice, CHANGE_FEED_CHANNEL};
use crate::registry::Database;
use crate::{DatabaseError, PoolRegistry};

const SAVEPOINT: &str = "gonvex_group_member";
/// Members per group before it commits regardless of waiters.
const MAX_MEMBERS: usize = 64;
/// Longest a group stays open for waiting writers before it commits.
const MAX_AGE: Duration = Duration::from_millis(25);
/// A group left open is committed by a flusher if no member arrives.
const FLUSH_AFTER: Duration = Duration::from_millis(5);

static GROUP_IDS: AtomicU64 = AtomicU64::new(1);
/// Groups committed by this process (observability and tests).
pub static GROUP_COMMITS: AtomicU64 = AtomicU64::new(0);

type Waiter = oneshot::Sender<Result<(), String>>;

pub(crate) struct WriteGroup {
    id: u64,
    transaction: Transaction<'static, Sqlite>,
    /// When the group first stayed open for a waiting writer.
    held_open: Option<Instant>,
    members: usize,
    waiters: Vec<Waiter>,
    notices: Vec<CommitNotice>,
}

/// One write transaction's membership in its database's group.
pub(crate) struct Member {
    guard: Option<OwnedMutexGuard<Option<WriteGroup>>>,
    database: Database,
    pools: PoolRegistry,
    database_url: Arc<str>,
}

impl Member {
    pub(crate) async fn join(
        pools: &PoolRegistry,
        database_url: &str,
    ) -> Result<Self, DatabaseError> {
        let database = pools.database(database_url).await?;
        database.group_waiting.fetch_add(1, Ordering::SeqCst);
        let locked =
            tokio::time::timeout(pools.acquire_timeout(), database.group.clone().lock_owned()).await;
        database.group_waiting.fetch_sub(1, Ordering::SeqCst);
        // Same contract as waiting for the writer connection itself.
        let mut guard = locked.map_err(|_| DatabaseError::Sql(sqlx::Error::PoolTimedOut))?;
        if guard.is_none() {
            let transaction = database.writer.begin().await?;
            *guard = Some(WriteGroup {
                id: GROUP_IDS.fetch_add(1, Ordering::Relaxed),
                transaction,
                held_open: None,
                members: 0,
                waiters: Vec::new(),
                notices: Vec::new(),
            });
        }
        let group = guard.as_mut().expect("group is open");
        if let Err(error) = sqlx::query(&format!("SAVEPOINT {SAVEPOINT}"))
            .execute(&mut *group.transaction)
            .await
        {
            // The connection is unusable: abort the group for everyone.
            let group = guard.take().expect("group is open");
            fail(group, &error.to_string());
            return Err(error.into());
        }
        group.members += 1;
        Ok(Self {
            guard: Some(guard),
            database,
            pools: pools.clone(),
            database_url: Arc::from(database_url.trim()),
        })
    }

    pub(crate) fn transaction(&mut self) -> &mut Transaction<'static, Sqlite> {
        &mut self
            .guard
            .as_mut()
            .and_then(|guard| guard.as_mut())
            .expect("member holds its group until it finishes")
            .transaction
    }

    /// Releases this member's work into the group and returns once the group
    /// has committed (or failed).
    pub(crate) async fn commit(mut self, notice: Option<CommitNotice>) -> Result<(), DatabaseError> {
        let mut guard = self.guard.take().expect("member holds its group");
        let group = guard.as_mut().expect("group is open");
        if let Err(error) = sqlx::query(&format!("RELEASE {SAVEPOINT}"))
            .execute(&mut *group.transaction)
            .await
        {
            let group = guard.take().expect("group is open");
            fail(group, &error.to_string());
            return Err(error.into());
        }
        if let Some(notice) = notice {
            group.notices.push(notice);
        }
        self.settle(guard).await
    }

    /// Undoes only this member's work; earlier members stay in the group.
    pub(crate) async fn rollback(mut self) -> Result<(), DatabaseError> {
        let mut guard = self.guard.take().expect("member holds its group");
        let group = guard.as_mut().expect("group is open");
        let undone = async {
            sqlx::query(&format!("ROLLBACK TO {SAVEPOINT}"))
                .execute(&mut *group.transaction)
                .await?;
            sqlx::query(&format!("RELEASE {SAVEPOINT}"))
                .execute(&mut *group.transaction)
                .await
        }
        .await;
        if let Err(error) = undone {
            let group = guard.take().expect("group is open");
            fail(group, &error.to_string());
            return Err(error.into());
        }
        group.members -= 1;
        if group.waiters.is_empty() && group.notices.is_empty() && group.members == 0 {
            // Nothing else in the group: end the transaction now.
            let group = guard.take().expect("group is open");
            group.transaction.rollback().await?;
            return Ok(());
        }
        // Earlier members are waiting for this group's COMMIT.
        self.settle_without_waiting(guard).await
    }

    /// Leaves the group open for a waiting writer, or commits it now.
    async fn settle(
        &mut self,
        mut guard: OwnedMutexGuard<Option<WriteGroup>>,
    ) -> Result<(), DatabaseError> {
        let group = guard.as_mut().expect("group is open");
        if self.should_wait(group) {
            let (sender, receiver) = oneshot::channel();
            group.waiters.push(sender);
            let id = group.id;
            drop(guard);
            self.schedule_flush(id);
            return match receiver.await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(message)) => Err(DatabaseError::GroupCommit(message)),
                Err(_) => Err(DatabaseError::GroupCommit("group commit was abandoned".into())),
            };
        }
        let group = guard.take().expect("group is open");
        drop(guard);
        commit_group(group, &self.pools, &self.database_url).await
    }

    /// After a rollback, commits earlier members unless another writer is
    /// waiting to join (it will settle the group).
    async fn settle_without_waiting(
        &mut self,
        mut guard: OwnedMutexGuard<Option<WriteGroup>>,
    ) -> Result<(), DatabaseError> {
        let group = guard.as_mut().expect("group is open");
        if self.should_wait(group) {
            let id = group.id;
            drop(guard);
            self.schedule_flush(id);
            return Ok(());
        }
        let group = guard.take().expect("group is open");
        drop(guard);
        commit_group(group, &self.pools, &self.database_url).await.or(Ok(()))
    }

    fn should_wait(&self, group: &mut WriteGroup) -> bool {
        let wait = self.database.group_waiting.load(Ordering::SeqCst) > 0
            && group.members < MAX_MEMBERS
            && group.held_open.is_none_or(|since| since.elapsed() < MAX_AGE);
        if wait && group.held_open.is_none() {
            group.held_open = Some(Instant::now());
        }
        wait
    }

    /// Commits the group if it is still open after FLUSH_AFTER (a waiting
    /// writer that was cancelled must not strand earlier members).
    fn schedule_flush(&self, id: u64) {
        let gate = self.database.group.clone();
        let pools = self.pools.clone();
        let url = self.database_url.clone();
        tokio::spawn(async move {
            tokio::time::sleep(FLUSH_AFTER).await;
            let mut guard = gate.lock_owned().await;
            if guard.as_ref().is_some_and(|group| group.id == id) {
                let group = guard.take().expect("group is open");
                drop(guard);
                let _ = commit_group(group, &pools, &url).await;
            }
        });
    }
}

impl Drop for Member {
    fn drop(&mut self) {
        // A member dropped without commit or rollback (a cancelled future)
        // may have written into the shared transaction: abort the group.
        if let Some(mut guard) = self.guard.take() {
            if let Some(group) = guard.take() {
                fail(group, "a write transaction in the commit group was cancelled");
            }
        }
    }
}

/// Commits the group with one sync, then publishes its revisions in order and
/// answers its waiting members.
async fn commit_group(
    group: WriteGroup,
    pools: &PoolRegistry,
    database_url: &str,
) -> Result<(), DatabaseError> {
    let WriteGroup {
        transaction,
        waiters,
        notices,
        ..
    } = group;
    match transaction.commit().await {
        Ok(()) => {
            GROUP_COMMITS.fetch_add(1, Ordering::Relaxed);
            for notice in &notices {
                let _ = pools
                    .notify(database_url, CHANGE_FEED_CHANNEL, &notice.payload())
                    .await;
            }
            for waiter in waiters {
                let _ = waiter.send(Ok(()));
            }
            Ok(())
        }
        Err(error) => {
            let message = error.to_string();
            for waiter in waiters {
                let _ = waiter.send(Err(message.clone()));
            }
            Err(error.into())
        }
    }
}

/// Drops the group's transaction (sqlx rolls it back) and fails its waiters.
fn fail(group: WriteGroup, message: &str) {
    for waiter in group.waiters {
        let _ = waiter.send(Err(message.to_owned()));
    }
    drop(group.transaction);
}

#[cfg(test)]
mod tests {
    use crate::feed::{capture_triggers_sql, SYNC_INFRASTRUCTURE_SQL};
    use crate::rows::table_columns;
    use crate::{begin_transaction, PoolLimits, PoolRegistry};
    use std::sync::atomic::Ordering;

    async fn database() -> (PoolRegistry, String, tempfile_dir::Dir) {
        let dir = tempfile_dir::Dir::new();
        let url = format!("sqlite://{}", dir.0.join("g.db").display());
        let pools = PoolRegistry::new(PoolLimits::default());
        let pool = pools.pool(&url).await.unwrap();
        let mut connection = pool.acquire().await.unwrap();
        crate::execute_script(&mut connection, SYNC_INFRASTRUCTURE_SQL).await.unwrap();
        crate::execute_script(&mut connection, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await.unwrap();
        let columns = table_columns(&mut connection, "t").await.unwrap();
        crate::execute_script(&mut connection, &capture_triggers_sql("t", "id", &columns)).await.unwrap();
        drop(connection);
        (pools, url, dir)
    }

    mod tempfile_dir {
        pub struct Dir(pub std::path::PathBuf);
        impl Dir {
            pub fn new() -> Self {
                let path = std::env::temp_dir().join(format!("group-{}", uuid::Uuid::new_v4().simple()));
                std::fs::create_dir_all(&path).unwrap();
                Self(path)
            }
        }
        impl Drop for Dir {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_writers_share_commits_and_keep_their_own_revisions() {
        let (pools, url, _dir) = database().await;
        let before = super::GROUP_COMMITS.load(Ordering::Relaxed);
        let mut tasks = Vec::new();
        for id in 0..40i64 {
            let (pools, url) = (pools.clone(), url.clone());
            tasks.push(tokio::spawn(async move {
                let mut tx = begin_transaction(&pools, &url, false).await.unwrap();
                tx.set_command_id(&format!("cmd-{id}")).await.unwrap();
                sqlx::query("INSERT INTO t VALUES ($1, 'x')").bind(id).execute(&mut **tx.transaction()).await.unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                tx.commit_with_revision().await.unwrap().unwrap()
            }));
        }
        let mut revisions = Vec::new();
        for task in tasks {
            revisions.push(task.await.unwrap());
        }
        let commits = super::GROUP_COMMITS.load(Ordering::Relaxed) - before;
        revisions.sort_unstable();
        revisions.dedup();
        assert_eq!(revisions.len(), 40, "each member keeps its own revision");
        assert!(commits < 40, "concurrent writers shared commits ({commits} commits for 40 writers)");
        let reader = pools.reader(&url).await.unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM t").fetch_one(&reader).await.unwrap();
        assert_eq!(rows, 40);
        let commands: i64 = sqlx::query_scalar("SELECT count(DISTINCT command_id) FROM _gonvex_sync_changes")
            .fetch_one(&reader)
            .await
            .unwrap();
        assert_eq!(commands, 40, "provenance stays per member");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_failed_member_rolls_back_alone() {
        let (pools, url, _dir) = database().await;
        let mut first = begin_transaction(&pools, &url, false).await.unwrap();
        sqlx::query("INSERT INTO t VALUES (1, 'kept')").execute(&mut **first.transaction()).await.unwrap();
        // A second writer waiting makes `first` leave the group open.
        let (pools2, url2) = (pools.clone(), url.clone());
        let second = tokio::spawn(async move {
            let mut tx = begin_transaction(&pools2, &url2, false).await.unwrap();
            sqlx::query("INSERT INTO t VALUES (2, 'undone')").execute(&mut **tx.transaction()).await.unwrap();
            tx.mark_failed("test failure");
            tx.commit().await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        first.commit().await.unwrap();
        assert!(second.await.unwrap().is_err());
        let reader = pools.reader(&url).await.unwrap();
        let values: Vec<String> = sqlx::query_scalar("SELECT v FROM t ORDER BY id").fetch_all(&reader).await.unwrap();
        assert_eq!(values, vec!["kept".to_owned()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_cancelled_member_aborts_its_group_without_partial_writes() {
        let (pools, url, _dir) = database().await;
        let mut first = begin_transaction(&pools, &url, false).await.unwrap();
        sqlx::query("INSERT INTO t VALUES (1, 'a')").execute(&mut **first.transaction()).await.unwrap();
        let (pools2, url2) = (pools.clone(), url.clone());
        let cancelled = tokio::spawn(async move {
            let mut tx = begin_transaction(&pools2, &url2, false).await.unwrap();
            sqlx::query("INSERT INTO t VALUES (2, 'b')").execute(&mut **tx.transaction()).await.unwrap();
            // Never finishes: dropped when the task is aborted.
            std::future::pending::<()>().await;
            drop(tx);
        });
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        // `first` sees a waiter and leaves the group open for it.
        let committing = tokio::spawn(async move { first.commit().await });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        cancelled.abort();
        let result = committing.await.unwrap();
        assert!(result.is_err(), "the waiting member learns its group was aborted");
        let reader = pools.reader(&url).await.unwrap();
        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM t").fetch_one(&reader).await.unwrap();
        assert_eq!(rows, 0, "nothing partial is committed");
        // The writer is usable again.
        let mut next = begin_transaction(&pools, &url, false).await.unwrap();
        sqlx::query("INSERT INTO t VALUES (3, 'c')").execute(&mut **next.transaction()).await.unwrap();
        next.commit().await.unwrap();
    }
}
