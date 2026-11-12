//! Compaction after a foreign-key rebuild.
//!
//! A `-- gonvex:foreign-key-rebuild` migration copies every table it rebuilds
//! and drops the old one. SQLite keeps the old pages in the file as free
//! pages, so the live rows end up spread over a much larger file: scans and
//! replica snapshot builds read sparser pages, and boots after a restore read
//! more bytes, until the file is vacuumed. One Whagons rebuild of 409 tables
//! left 42% of each tenant file free and made snapshot builds 25-45% slower.
//!
//! After a provisioning batch that applied a rebuild, [`after_rebuild`]
//! vacuums the file once when free pages are a large share of it. It runs
//! outside every migration transaction while the caller holds the schema
//! lock and the database's single writer, so no write can interleave. The
//! VACUUM uses its own short-lived connection: changing `temp_store` on the
//! writer would drop the writer's TEMP objects, and the writer's in-memory
//! temp store would hold the whole live database in RAM during the copy.
//!
//! Best effort: the migrations are already committed, so a VACUUM that fails
//! or is skipped only leaves the free pages in place (logged; an operator can
//! VACUUM the file later).

use std::path::Path;
use std::str::FromStr;
use std::time::{Duration, Instant};

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Connection};

use crate::DatabaseError;

/// VACUUM only when free pages are more than this share of the file...
const MIN_FREE_FRACTION: f64 = 0.2;
/// ...and more than this many bytes.
const MIN_FREE_BYTES: u64 = 8 << 20;
/// Largest live size (bytes) vacuumed automatically
/// (`GONVEX_REBUILD_VACUUM_MAX_BYTES`): VACUUM rewrites every live page, so
/// this bounds the time the writer is held and the temporary file's size.
const DEFAULT_MAX_LIVE_BYTES: u64 = 2 << 30;
/// How long the final checkpoint waits for a concurrent one to finish.
const CHECKPOINT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    Keep,
    Vacuum,
    TooLarge,
}

pub(crate) fn decide(page_size: u64, pages: u64, free: u64, max_live_bytes: u64) -> Decision {
    let free_bytes = free.saturating_mul(page_size);
    if pages == 0
        || free_bytes <= MIN_FREE_BYTES
        || (free as f64) <= MIN_FREE_FRACTION * pages as f64
    {
        Decision::Keep
    } else if pages.saturating_sub(free).saturating_mul(page_size) > max_live_bytes {
        Decision::TooLarge
    } else {
        Decision::Vacuum
    }
}

fn max_live_bytes() -> u64 {
    std::env::var("GONVEX_REBUILD_VACUUM_MAX_BYTES")
        .ok()
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(DEFAULT_MAX_LIVE_BYTES)
}

/// Vacuums `path` when [`decide`] says so. Call only after a batch that
/// applied a foreign-key rebuild, holding the schema lock and the writer.
pub(crate) async fn after_rebuild(path: &Path) {
    let started = Instant::now();
    let name = path.display();
    match vacuum(path).await {
        Ok(None) => {}
        Ok(Some((before, after))) => eprintln!(
            "gonvex-store: compacted {name} after a foreign-key rebuild: {before} -> {after} bytes in {} ms",
            started.elapsed().as_millis()
        ),
        Err(error) => eprintln!(
            "gonvex-store: compaction of {name} after a foreign-key rebuild failed after {} ms; free pages stay until the file is vacuumed: {error}",
            started.elapsed().as_millis()
        ),
    }
}

/// Returns the file sizes before and after, or `None` when nothing ran.
async fn vacuum(path: &Path) -> Result<Option<(u64, u64)>, DatabaseError> {
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))?
        .busy_timeout(Duration::from_secs(5))
        .with_regexp()
        // The copy is built in a temporary file, not in memory.
        .pragma("temp_store", "FILE");
    let mut connection = options.connect().await?;
    let result = async {
        let (page_size, pages, free): (i64, i64, i64) = sqlx::query_as(
            "SELECT (SELECT page_size FROM pragma_page_size),(SELECT page_count FROM pragma_page_count),(SELECT freelist_count FROM pragma_freelist_count)",
        )
        .fetch_one(&mut connection)
        .await?;
        let (page_size, pages, free) = (page_size as u64, pages as u64, free as u64);
        match decide(page_size, pages, free, max_live_bytes()) {
            Decision::Keep => return Ok(None),
            Decision::TooLarge => {
                eprintln!(
                    "gonvex-store: not compacting {} after a foreign-key rebuild: {} live bytes exceed GONVEX_REBUILD_VACUUM_MAX_BYTES ({} free pages stay)",
                    path.display(),
                    (pages - free) * page_size,
                    free
                );
                return Ok(None);
            }
            Decision::Vacuum => {}
        }
        sqlx::query("VACUUM").execute(&mut connection).await?;
        // Copy the rewritten pages from the WAL into the file, which
        // truncates it, and empty the WAL. The background checkpointer may
        // be copying the migration's frames at this moment (it owns the
        // checkpoint lock, so this one reports busy): wait for it. Readers
        // holding an older snapshot only delay the truncation to a later
        // checkpoint.
        let deadline = Instant::now() + CHECKPOINT_WAIT;
        loop {
            let (busy, log, copied): (i64, i64, i64) =
                sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
                    .fetch_one(&mut connection)
                    .await?;
            if (busy == 0 && log == copied) || Instant::now() >= deadline {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let after: i64 = sqlx::query_scalar("SELECT page_count FROM pragma_page_count")
            .fetch_one(&mut connection)
            .await?;
        Ok(Some((pages * page_size, after as u64 * page_size)))
    }
    .await;
    let _ = connection.close().await;
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1 << 20;

    #[test]
    fn vacuums_only_large_free_shares_within_the_size_bound() {
        let page = 4096;
        let pages = |bytes: u64| bytes / page;
        // 42% free of 94 MB, as the Whagons rebuild left a tenant.
        assert_eq!(
            decide(
                page,
                pages(94 * MIB),
                pages(40 * MIB),
                DEFAULT_MAX_LIVE_BYTES
            ),
            Decision::Vacuum
        );
        // A large share but only a few MB: not worth a rewrite.
        assert_eq!(
            decide(
                page,
                pages(10 * MIB),
                pages(5 * MIB),
                DEFAULT_MAX_LIVE_BYTES
            ),
            Decision::Keep
        );
        // Many MB but a small share of a large file.
        assert_eq!(
            decide(
                page,
                pages(1000 * MIB),
                pages(100 * MIB),
                DEFAULT_MAX_LIVE_BYTES
            ),
            Decision::Keep
        );
        // Live bytes above the bound.
        assert_eq!(
            decide(page, pages(300 * MIB), pages(100 * MIB), 100 * MIB),
            Decision::TooLarge
        );
        assert_eq!(decide(page, 0, 0, DEFAULT_MAX_LIVE_BYTES), Decision::Keep);
    }
}
