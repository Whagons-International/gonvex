//! Module artifacts as the runtime loads them at boot.
//!
//! A module carries every migration it ever shipped, base64-encoded in its
//! manifest. Whagons' history is 13 files, 19 MB of manifest text: reading,
//! parsing, hashing and decoding it again cost every boot about 0.1 s and
//! 15 MB of memory, although only a tenant that has not applied a migration
//! ever needs its body.
//!
//! `gonvex_runtime_manifests` keeps the complete manifest as pushed (older
//! runtimes boot from it). Alongside it, `gonvex_runtime_artifacts` keeps the
//! same manifest without migration bodies, the migrations' names, flags and
//! checksums, and the SHA-256 of that text; `gonvex_runtime_migration_bodies`
//! keeps each body once, by checksum, using the bounded artifact encoding when
//! smaller than plain SQL. Existing plain entries remain readable and are not
//! rewritten on boot. A runtime predating the encoding refuses a pending encoded
//! body at its checksum check; deploy the decoding runtime before new artifacts. A boot whose stored artifact still
//! matches the manifest row (same module hash and `updated_at`) verifies the
//! text's SHA-256, parses only it and provisions with bodiless migrations
//! ([`SqlMigration::stored`]): tenants compare checksums as before, and a
//! tenant that must apply a migration loads its body by checksum, verified,
//! before applying it ([`ControlPlane::load_pending_bodies`]).

use std::collections::{BTreeSet, HashSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use sqlx::{Row, SqliteConnection};

use crate::provision::sql_checksum;
use crate::{ControlPlane, DatabaseError, MigrationScope, RuntimeManifestRecord, SqlMigration};

const MANIFEST_HEADS_SQL: &str = r#"SELECT project_id, module_hash, updated_at
    FROM gonvex_runtime_manifests WHERE module_hash <> '' ORDER BY updated_at, project_id"#;

/// The identity of a project's stored manifest row, read without the
/// manifest itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeManifestHead {
    pub project_id: String,
    pub module_hash: String,
    pub updated_at: i64,
}

/// A project's module artifact without its migration bodies.
#[derive(Clone, Debug)]
pub struct StoredArtifact {
    pub project_id: String,
    pub module_hash: String,
    /// The manifest without migration bodies, as stored.
    pub manifest_text: String,
    /// Bodiless migrations ([`SqlMigration::stored`]), in manifest order.
    pub migrations: Vec<SqlMigration>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoredMigration {
    name: String,
    scope: MigrationScope,
    no_transaction: bool,
    foreign_key_rebuild: bool,
    checksum: String,
}

// An array envelope preserves the existing table's JSON-array constraint.
// Earlier, unverified arrays of entries are cache misses and are rebuilt.
type StoredMigrations = (Vec<StoredMigration>, String);

fn migration_metadata_sha256(entries: &[StoredMigration]) -> Result<String, DatabaseError> {
    let text = serde_json::to_string(entries).map_err(|error| protocol(error.to_string()))?;
    Ok(manifest_sha256(&text))
}

/// Lowercase hex SHA-256 of a stored manifest text.
pub fn manifest_sha256(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn protocol(message: impl Into<String>) -> DatabaseError {
    sqlx::Error::Protocol(message.into()).into()
}

impl ControlPlane {
    /// Every project's stored manifest identity, oldest first (the order
    /// [`ControlPlane::runtime_manifests`] returns).
    pub async fn runtime_manifest_heads(&self) -> Result<Vec<RuntimeManifestHead>, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let rows = sqlx::query(MANIFEST_HEADS_SQL)
            .fetch_all(&self.reader)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| RuntimeManifestHead {
                project_id: row.get("project_id"),
                module_hash: row.get("module_hash"),
                updated_at: row.get("updated_at"),
            })
            .collect())
    }

    /// The complete stored manifest of one project.
    pub async fn runtime_manifest(
        &self,
        project_id: &str,
    ) -> Result<Option<RuntimeManifestRecord>, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(
            "SELECT project_id, module_hash, manifest FROM gonvex_runtime_manifests WHERE project_id=$1",
        )
        .bind(project_id)
        .fetch_optional(&self.reader)
        .await?;
        Ok(row.map(|row| RuntimeManifestRecord {
            project_id: row.get("project_id"),
            module_hash: row.get("module_hash"),
            manifest: row.get("manifest"),
        }))
    }

    /// The stored artifact of `head`, when it still describes that manifest
    /// row and its text is intact. `None` means: boot from the manifest row
    /// and store the artifact again.
    pub async fn stored_artifact(
        &self,
        head: &RuntimeManifestHead,
    ) -> Result<Option<StoredArtifact>, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        let row = sqlx::query(
            r#"SELECT manifest_sha256, manifest, migrations FROM gonvex_runtime_artifacts
               WHERE project_id=$1 AND module_hash=$2 AND manifest_updated_at=$3"#,
        )
        .bind(&head.project_id)
        .bind(&head.module_hash)
        .bind(head.updated_at)
        .fetch_optional(&self.reader)
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let sha256: String = row.get("manifest_sha256");
        let manifest_text: String = row.get("manifest");
        if manifest_sha256(&manifest_text) != sha256 {
            eprintln!(
                "gonvex-store: the stored artifact of project {} does not match its digest; loading the complete manifest",
                head.project_id
            );
            return Ok(None);
        }
        // Flags and checksums are executable migration metadata too. A damaged
        // cache must fall back to the fully verified manifest, never change
        // transaction or rebuild semantics. Older cache formats also rebuild.
        let Ok((migrations, metadata_sha256)) =
            serde_json::from_str::<StoredMigrations>(row.get("migrations"))
        else {
            return Ok(None);
        };
        if migration_metadata_sha256(&migrations)? != metadata_sha256 {
            return Ok(None);
        }
        Ok(Some(StoredArtifact {
            project_id: head.project_id.clone(),
            module_hash: head.module_hash.clone(),
            manifest_text,
            migrations: migrations
                .into_iter()
                .map(|migration| {
                    SqlMigration::stored(
                        migration.name,
                        migration.scope,
                        migration.no_transaction,
                        migration.foreign_key_rebuild,
                        migration.checksum,
                    )
                })
                .collect(),
        }))
    }

    /// Whether the project's stored artifact is exactly this one and the
    /// manifest row has not changed since it was stored: saving it again
    /// would rewrite identical rows.
    pub async fn stored_artifact_is_current(
        &self,
        project_id: &str,
        module_hash: &str,
        manifest_sha256: &str,
    ) -> Result<bool, DatabaseError> {
        let _admission = self.pools.admit(&self.database_url).await?;
        Ok(sqlx::query_scalar(
            r#"SELECT EXISTS(SELECT 1 FROM gonvex_runtime_artifacts a
                 JOIN gonvex_runtime_manifests m ON m.project_id=a.project_id
                 WHERE a.project_id=$1 AND a.module_hash=$2 AND a.manifest_sha256=$3
                   AND m.module_hash=a.module_hash AND m.updated_at=a.manifest_updated_at)"#,
        )
        .bind(project_id)
        .bind(module_hash)
        .bind(manifest_sha256)
        .fetch_one(&self.reader)
        .await?)
    }

    /// Stores the bodies of `migrations` that are loaded and not stored yet.
    pub async fn store_migration_bodies(
        &self,
        migrations: &[SqlMigration],
    ) -> Result<(), DatabaseError> {
        let checksums = migrations
            .iter()
            .filter(|migration| migration.sql().is_some())
            .map(|migration| migration.checksum.as_str())
            .collect::<BTreeSet<_>>();
        if checksums.is_empty() {
            return Ok(());
        }
        let _admission = self.pools.admit(&self.database_url).await?;
        let stored: HashSet<String> = sqlx::query_scalar(
            "SELECT checksum FROM gonvex_runtime_migration_bodies WHERE checksum IN (SELECT value FROM json_each($1))",
        )
        .bind(serde_json::to_string(&checksums).map_err(|error| protocol(error.to_string()))?)
        .fetch_all(&self.reader)
        .await?
        .into_iter()
        .collect();
        let mut missing = migrations
            .iter()
            .filter(|migration| !stored.contains(&migration.checksum))
            .filter_map(|migration| Some((migration.checksum.as_str(), migration.sql()?)))
            .collect::<Vec<_>>();
        missing.sort_unstable();
        missing.dedup_by_key(|(checksum, _)| *checksum);
        if missing.is_empty() {
            return Ok(());
        }
        let mut transaction = self.pool.begin().await?;
        for (checksum, sql) in missing {
            let sql = crate::migration_encoding::encode_stored(sql).map_err(protocol)?;
            sqlx::query(
                "INSERT INTO gonvex_runtime_migration_bodies(checksum,sql) VALUES($1,$2) ON CONFLICT(checksum) DO NOTHING",
            )
            .bind(checksum)
            .bind(sql.as_ref())
            .execute(&mut *transaction)
            .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    /// Records `artifact` as the stored form of the project's manifest row
    /// written at `manifest_updated_at`, and drops bodies no stored artifact
    /// references. Store its bodies first ([`Self::store_migration_bodies`]).
    pub async fn save_stored_artifact(
        &self,
        artifact: &StoredArtifact,
        manifest_updated_at: i64,
    ) -> Result<(), DatabaseError> {
        let migrations = artifact
            .migrations
            .iter()
            .map(|migration| StoredMigration {
                name: migration.name.clone(),
                scope: migration.scope,
                no_transaction: migration.no_transaction,
                foreign_key_rebuild: migration.foreign_key_rebuild(),
                checksum: migration.checksum.clone(),
            })
            .collect::<Vec<_>>();
        let checksum = migration_metadata_sha256(&migrations)?;
        let migrations = serde_json::to_string(&(migrations, checksum))
            .map_err(|error| protocol(error.to_string()))?;
        let _admission = self.pools.admit(&self.database_url).await?;
        let mut transaction = self.pool.begin().await?;
        let missing: Option<String> = sqlx::query_scalar(
            r#"SELECT json_extract(value,'$.name') FROM json_each($1,'$[0]')
               WHERE json_extract(value,'$.checksum') NOT IN (SELECT checksum FROM gonvex_runtime_migration_bodies)
               LIMIT 1"#,
        )
        .bind(&migrations)
        .fetch_optional(&mut *transaction)
        .await?;
        if let Some(name) = missing {
            return Err(protocol(format!(
                "stored artifact references migration {name} without a stored body"
            )));
        }
        sqlx::query(
            r#"INSERT INTO gonvex_runtime_artifacts(project_id,module_hash,manifest_updated_at,manifest_sha256,manifest,migrations)
               VALUES($1,$2,$3,$4,$5,$6)
               ON CONFLICT(project_id) DO UPDATE SET module_hash=excluded.module_hash,
                 manifest_updated_at=excluded.manifest_updated_at,manifest_sha256=excluded.manifest_sha256,
                 manifest=excluded.manifest,migrations=excluded.migrations"#,
        )
        .bind(&artifact.project_id)
        .bind(&artifact.module_hash)
        .bind(manifest_updated_at)
        .bind(manifest_sha256(&artifact.manifest_text))
        .bind(&artifact.manifest_text)
        .bind(&migrations)
        .execute(&mut *transaction)
        .await?;
        sqlx::query(
            r#"DELETE FROM gonvex_runtime_migration_bodies WHERE checksum NOT IN (
                 SELECT json_extract(m.value,'$.checksum')
                 FROM gonvex_runtime_artifacts a, json_each(
                   CASE WHEN json_type(a.migrations,'$[0]')='array'
                     THEN json_extract(a.migrations,'$[0]') ELSE a.migrations END) m)"#,
        )
        .execute(&mut *transaction)
        .await?;
        transaction.commit().await?;
        Ok(())
    }

    /// Loads the bodies of `scope`'s migrations that `ledger`'s database has
    /// not recorded and that `migrations` carries without a body. Each body
    /// must match its checksum.
    pub(crate) async fn load_pending_bodies(
        &self,
        ledger: &mut SqliteConnection,
        mut migrations: Vec<SqlMigration>,
        scope: MigrationScope,
    ) -> Result<Vec<SqlMigration>, DatabaseError> {
        if migrations
            .iter()
            .all(|migration| migration.scope != scope || migration.sql().is_some())
        {
            return Ok(migrations);
        }
        let recorded: HashSet<String> =
            if crate::provision::table_exists(ledger, "gonvex_migrations").await? {
                sqlx::query_scalar("SELECT name FROM gonvex_migrations")
                    .fetch_all(&mut *ledger)
                    .await?
                    .into_iter()
                    .collect()
            } else {
                HashSet::new()
            };
        let mut remaining = crate::migration_encoding::MAX_DECODED_BYTES;
        for migration in migrations.iter_mut().filter(|migration| {
            migration.scope == scope
                && migration.sql().is_none()
                && !recorded.contains(&migration.name)
        }) {
            let sql: Option<String> = sqlx::query_scalar(
                "SELECT sql FROM gonvex_runtime_migration_bodies WHERE checksum=$1",
            )
            .bind(&migration.checksum)
            .fetch_optional(&self.reader)
            .await?;
            let sql = sql.ok_or_else(|| {
                protocol(format!(
                    "migration {} has no stored body; push the module again",
                    migration.name
                ))
            })?;
            let sql =
                crate::migration_encoding::decode_stored(sql, &mut remaining).map_err(protocol)?;
            if sql_checksum(&sql) != migration.checksum {
                return Err(protocol(format!(
                    "stored body of migration {} does not match its checksum",
                    migration.name
                )));
            }
            *migration = SqlMigration::new(
                migration.name.clone(),
                migration.scope,
                migration.no_transaction,
                sql,
            );
        }
        Ok(migrations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction_commit_tests::TestDir;
    use crate::{PoolLimits, PoolRegistry, TenantRoute};

    struct Fixture {
        _dir: TestDir,
        pools: PoolRegistry,
        control: ControlPlane,
        route: TenantRoute,
    }

    async fn fixture(prefix: &str) -> Fixture {
        let dir = TestDir::new(prefix);
        let data = crate::DataDir::new(&dir.0);
        let pools = PoolRegistry::new(PoolLimits::default());
        let control = ControlPlane::connect(&data.control_url(), pools.clone(), Default::default())
            .await
            .unwrap();
        let route = TenantRoute {
            project_id: "project".to_owned(),
            tenant_id: "tenant".to_owned(),
            database_url: data.tenant_url("project", "tenant").unwrap(),
        };
        Fixture {
            _dir: dir,
            pools,
            control,
            route,
        }
    }

    fn migration(name: &str, sql: &str) -> SqlMigration {
        SqlMigration::new(
            name.to_owned(),
            MigrationScope::Tenant,
            false,
            sql.to_owned(),
        )
    }

    fn record(hash: &str) -> RuntimeManifestRecord {
        RuntimeManifestRecord {
            project_id: "project".to_owned(),
            module_hash: hash.to_owned(),
            manifest: serde_json::json!({"module":{"hash":hash}}),
        }
    }

    async fn store(f: &Fixture, hash: &str, migrations: &[SqlMigration]) -> RuntimeManifestHead {
        let updated_at = f
            .control
            .save_runtime_manifest(&record(hash))
            .await
            .unwrap();
        f.control.store_migration_bodies(migrations).await.unwrap();
        let artifact = StoredArtifact {
            project_id: "project".to_owned(),
            module_hash: hash.to_owned(),
            manifest_text: format!(r#"{{"module":{{"hash":"{hash}"}}}}"#),
            migrations: migrations.to_vec(),
        };
        f.control
            .save_stored_artifact(&artifact, updated_at)
            .await
            .unwrap();
        let heads = f.control.runtime_manifest_heads().await.unwrap();
        assert_eq!(heads.len(), 1);
        heads.into_iter().next().unwrap()
    }

    async fn bodies(f: &Fixture) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM gonvex_runtime_migration_bodies")
            .fetch_one(&f.control.reader)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn compressed_archive_preserves_sql_and_accepts_existing_plain_bodies() {
        let f = fixture("gonvex-encoded-archive").await;
        let sql = format!(
            "{}CREATE TABLE archived(id TEXT PRIMARY KEY);",
            "-- café ☀\r\n".repeat(10_000)
        );
        let migration = migration("0001_archive.sql", &sql);
        let head = store(&f, "hash", std::slice::from_ref(&migration)).await;
        let encoded: String = sqlx::query_scalar("SELECT sql FROM gonvex_runtime_migration_bodies")
            .fetch_one(&f.control.reader)
            .await
            .unwrap();
        assert!(encoded.starts_with("deflate-raw-v1:"));
        assert!(encoded.len() < sql.len() / 10);
        assert_ne!(
            sql_checksum(&encoded),
            migration.checksum,
            "an older runtime must refuse encoded SQL at its checksum check"
        );
        let stored = f.control.stored_artifact(&head).await.unwrap().unwrap();
        f.control
            .clone()
            .provision_tenant_database(f.route.clone(), stored.migrations.clone())
            .await
            .unwrap();
        let mut ledger = f.control.pool.acquire().await.unwrap();
        let decoded = f
            .control
            .load_pending_bodies(
                &mut ledger,
                stored.migrations.clone(),
                MigrationScope::Tenant,
            )
            .await
            .unwrap();
        assert_eq!(decoded[0].sql(), Some(sql.as_str()));
        drop(ledger);

        // Existing archives are not rewritten on a repeated push, and remain
        // valid inputs to a runtime that now stores compressed bodies.
        sqlx::query("UPDATE gonvex_runtime_migration_bodies SET sql=$1")
            .bind(&sql)
            .execute(&f.control.pool)
            .await
            .unwrap();
        f.control
            .store_migration_bodies(std::slice::from_ref(&migration))
            .await
            .unwrap();
        let plain: String = sqlx::query_scalar("SELECT sql FROM gonvex_runtime_migration_bodies")
            .fetch_one(&f.control.reader)
            .await
            .unwrap();
        assert_eq!(plain, sql);
        let mut ledger = f.control.pool.acquire().await.unwrap();
        let decoded = f
            .control
            .load_pending_bodies(&mut ledger, stored.migrations, MigrationScope::Tenant)
            .await
            .unwrap();
        assert_eq!(decoded[0].sql(), Some(sql.as_str()));
        drop(ledger);
        f.pools.close().await;
    }

    #[tokio::test]
    async fn corrupt_encoded_archive_cannot_apply_or_change_a_migration() {
        let f = fixture("gonvex-corrupt-archive").await;
        let migration = migration("0001_archive.sql", "CREATE TABLE forbidden(id INTEGER);");
        let head = store(&f, "hash", std::slice::from_ref(&migration)).await;
        let stored = f.control.stored_artifact(&head).await.unwrap().unwrap();
        for invalid in [
            "deflate-raw-v1:67108865:AA==".to_owned(),
            "deflate-raw-v1:5:y0jNyckHAA==".to_owned(),
        ] {
            sqlx::query("UPDATE gonvex_runtime_migration_bodies SET sql=$1")
                .bind(invalid)
                .execute(&f.control.pool)
                .await
                .unwrap();
            assert!(f
                .control
                .clone()
                .provision_tenant_database(f.route.clone(), stored.migrations.clone())
                .await
                .is_err());
            let reader = f.pools.pool(&f.route.database_url).await.unwrap();
            let count: i64 =
                sqlx::query_scalar("SELECT count(*) FROM sqlite_schema WHERE name='forbidden'")
                    .fetch_one(&reader)
                    .await
                    .unwrap();
            assert_eq!(count, 0);
        }
        f.pools.close().await;
    }

    #[tokio::test]
    async fn legacy_cache_entries_are_rebuilt_and_keep_their_bodies_until_replaced() {
        let f = fixture("gonvex-stored-legacy").await;
        let first = migration("0001_a.sql", "CREATE TABLE a(id TEXT PRIMARY KEY);");
        let head = store(&f, "hash-1", std::slice::from_ref(&first)).await;
        sqlx::query(
            "UPDATE gonvex_runtime_artifacts SET migrations=json_extract(migrations,'$[0]')",
        )
        .execute(&f.control.pool)
        .await
        .unwrap();
        assert!(f.control.stored_artifact(&head).await.unwrap().is_none());
        sqlx::query("INSERT INTO gonvex_runtime_artifacts SELECT 'legacy-project',module_hash,manifest_updated_at,manifest_sha256,manifest,migrations FROM gonvex_runtime_artifacts WHERE project_id='project'")
            .execute(&f.control.pool).await.unwrap();
        let second = migration("0002_b.sql", "CREATE TABLE b(id TEXT PRIMARY KEY);");
        store(&f, "hash-2", std::slice::from_ref(&second)).await;
        assert_eq!(bodies(&f).await, 2);
        sqlx::query("DELETE FROM gonvex_runtime_artifacts WHERE project_id='legacy-project'")
            .execute(&f.control.pool)
            .await
            .unwrap();
        store(&f, "hash-3", std::slice::from_ref(&second)).await;
        assert_eq!(bodies(&f).await, 1);
    }

    #[tokio::test]
    async fn a_stored_artifact_describes_only_its_manifest_row_and_intact_text() {
        let f = fixture("gonvex-stored-artifact").await;
        let first = migration("0001_a.sql", "CREATE TABLE a(id TEXT PRIMARY KEY);");
        let head = store(&f, "hash-1", std::slice::from_ref(&first)).await;
        let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {MANIFEST_HEADS_SQL}"))
            .fetch_all(&f.control.reader)
            .await
            .unwrap();
        assert!(
            plan.iter()
                .any(|row| row.get::<String, _>("detail").contains("COVERING INDEX")),
            "reading manifest heads must not visit the large manifest rows"
        );
        let stored = f.control.stored_artifact(&head).await.unwrap().unwrap();
        assert_eq!(stored.migrations, vec![first.without_body()]);
        assert!(f
            .control
            .stored_artifact_is_current(
                "project",
                "hash-1",
                &manifest_sha256(&stored.manifest_text)
            )
            .await
            .unwrap());

        // A manifest row saved by an older runtime (same hash, new time).
        sqlx::query("UPDATE gonvex_runtime_manifests SET updated_at=updated_at+1")
            .execute(&f.control.pool)
            .await
            .unwrap();
        let moved = f.control.runtime_manifest_heads().await.unwrap().remove(0);
        assert!(f.control.stored_artifact(&moved).await.unwrap().is_none());
        assert!(!f
            .control
            .stored_artifact_is_current(
                "project",
                "hash-1",
                &manifest_sha256(&stored.manifest_text)
            )
            .await
            .unwrap());

        // Damaged text is never installed.
        let head = store(&f, "hash-1", std::slice::from_ref(&first)).await;
        sqlx::query("UPDATE gonvex_runtime_artifacts SET manifest='{\"module\":{}}'")
            .execute(&f.control.pool)
            .await
            .unwrap();
        assert!(f.control.stored_artifact(&head).await.unwrap().is_none());

        // Valid JSON with altered execution flags is still a corrupt cache.
        let head = store(&f, "hash-1", std::slice::from_ref(&first)).await;
        sqlx::query("UPDATE gonvex_runtime_artifacts SET migrations=json_set(migrations,'$[0][0].noTransaction',json('true'))")
            .execute(&f.control.pool).await.unwrap();
        assert!(f.control.stored_artifact(&head).await.unwrap().is_none());

        // Bodies no stored artifact references are dropped.
        let second = migration("0002_b.sql", "CREATE TABLE b(id TEXT PRIMARY KEY);");
        store(&f, "hash-2", &[first.clone(), second.clone()]).await;
        assert_eq!(bodies(&f).await, 2);
        store(&f, "hash-3", std::slice::from_ref(&second)).await;
        assert_eq!(bodies(&f).await, 1);

        // An artifact whose body was never stored is refused.
        let unstored = migration("0003_c.sql", "CREATE TABLE c(id TEXT PRIMARY KEY);");
        let error = f
            .control
            .save_stored_artifact(
                &StoredArtifact {
                    project_id: "project".to_owned(),
                    module_hash: "hash-4".to_owned(),
                    manifest_text: "{}".to_owned(),
                    migrations: vec![unstored],
                },
                0,
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("0003_c.sql"), "{error}");
        f.pools.close().await;
    }

    #[tokio::test]
    async fn provisioning_loads_and_verifies_only_the_bodies_a_tenant_needs() {
        let f = fixture("gonvex-stored-bodies").await;
        let first = migration("0001_a.sql", "CREATE TABLE a(id TEXT PRIMARY KEY);");
        let second = migration("0002_b.sql", "CREATE TABLE b(id TEXT PRIMARY KEY);");
        f.control
            .clone()
            .provision_tenant_database(f.route.clone(), vec![first.clone()])
            .await
            .unwrap();
        store(&f, "hash", &[first.clone(), second.clone()]).await;
        // 0001 is recorded (its body is not read); 0002 is applied from the
        // stored body.
        sqlx::query("DELETE FROM gonvex_runtime_migration_bodies WHERE checksum=$1")
            .bind(&first.checksum)
            .execute(&f.control.pool)
            .await
            .unwrap();
        let stored = vec![first.without_body(), second.without_body()];
        f.control
            .clone()
            .provision_tenant_database(f.route.clone(), stored.clone())
            .await
            .unwrap();
        let writer = f.pools.pool(&f.route.database_url).await.unwrap();
        let applied: i64 = sqlx::query_scalar("SELECT count(*) FROM gonvex_migrations")
            .fetch_one(&writer)
            .await
            .unwrap();
        assert_eq!(applied, 2);

        // A fresh tenant needs every body; one that does not match its
        // checksum is refused before anything runs.
        sqlx::query("INSERT INTO gonvex_runtime_migration_bodies(checksum,sql) VALUES($1,'DROP TABLE members;')")
            .bind(&first.checksum)
            .execute(&f.control.pool)
            .await
            .unwrap();
        let fresh = TenantRoute {
            tenant_id: "fresh".to_owned(),
            database_url: crate::DataDir::new(&f._dir.0)
                .tenant_url("project", "fresh")
                .unwrap(),
            ..f.route.clone()
        };
        let error = f
            .control
            .clone()
            .provision_tenant_database(fresh.clone(), stored.clone())
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("does not match its checksum"),
            "{error}"
        );
        sqlx::query("DELETE FROM gonvex_runtime_migration_bodies WHERE checksum=$1")
            .bind(&first.checksum)
            .execute(&f.control.pool)
            .await
            .unwrap();
        let error = f
            .control
            .clone()
            .provision_tenant_database(fresh, stored)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("no stored body"), "{error}");
        f.pools.close().await;
    }
}
