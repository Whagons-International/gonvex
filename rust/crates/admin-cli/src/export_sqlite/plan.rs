//! Which Postgres databases become which SQLite files, and the Control Plane
//! routing columns rewritten to point at them.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use gonvex_store::{DataDir, MigrationScope, SqlMigration};
use serde::Serialize;

use crate::identity::Result;

/// A `gonvex_runtime_projects` row's routing fields in Postgres.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceProject {
    pub id: String,
    pub database_url: String,
}

/// A `gonvex_runtime_tenants` row's routing fields in Postgres.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceTenant {
    pub relationship_id: String,
    pub project_id: String,
    pub tenant_id: String,
    pub database_url: String,
    /// `deleted_at IS NOT NULL OR status = 'deleted'`.
    pub deleted: bool,
}

/// New `database_url` / `database_name` for one Control Plane row.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RouteRewrite {
    pub database_url: String,
    pub database_name: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DatabaseKind {
    Project,
    Tenant,
}

/// One Postgres database exported to one SQLite file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DatabasePlan {
    pub kind: DatabaseKind,
    pub project_id: String,
    /// Live tenants routed to this database (sorted).
    pub tenant_ids: Vec<String>,
    pub source_url: String,
    pub target_url: String,
    pub target_path: PathBuf,
}

#[derive(Debug, Default)]
pub struct RoutePlan {
    /// By project id.
    pub projects: BTreeMap<String, RouteRewrite>,
    /// By tenant relationship id.
    pub tenants: BTreeMap<String, RouteRewrite>,
    pub databases: Vec<DatabasePlan>,
}

fn normalized(url: &str) -> String {
    url.trim().to_owned()
}

fn relative_name(data: &DataDir, path: &Path) -> String {
    data.relative(path)
        .unwrap_or_else(|| path.to_path_buf())
        .display()
        .to_string()
}

fn rewrite(data: &DataDir, path: &Path) -> RouteRewrite {
    RouteRewrite {
        database_url: gonvex_store::database_url(path),
        database_name: relative_name(data, path),
    }
}

/// Maps every distinct source database to a file in `data`:
///
/// - a project's own database (`gonvex_runtime_projects.database_url`) is
///   `projects/{project}/project.db`; a tenant routed to that same database
///   (single-database projects) uses it too;
/// - any other tenant database is `projects/{project}/tenants/{tenant}.db`.
///
/// Two live tenants routed to one database cannot be split into per-tenant
/// files and are refused. Rows with an empty URL keep it empty. Deleted
/// tenants are rewritten (no Postgres URL survives) but never exported.
pub fn plan_routes(
    data: &DataDir,
    projects: &[SourceProject],
    tenants: &[SourceTenant],
) -> Result<RoutePlan> {
    let mut plan = RoutePlan::default();
    // source URL -> index into plan.databases
    let mut by_source: BTreeMap<String, usize> = BTreeMap::new();
    let mut project_source: BTreeMap<&str, String> = BTreeMap::new();
    for project in projects {
        let source = normalized(&project.database_url);
        if source.is_empty() {
            plan.projects.insert(
                project.id.clone(),
                RouteRewrite {
                    database_url: String::new(),
                    database_name: String::new(),
                },
            );
            continue;
        }
        let path = data.project_path(&project.id)?;
        plan.projects
            .insert(project.id.clone(), rewrite(data, &path));
        project_source.insert(&project.id, source.clone());
        if let Some(existing) = by_source.get(&source) {
            let other = &plan.databases[*existing];
            return Err(format!(
                "projects {:?} and {:?} use the same Postgres database; each project needs its own",
                other.project_id, project.id
            )
            .into());
        }
        by_source.insert(source.clone(), plan.databases.len());
        plan.databases.push(DatabasePlan {
            kind: DatabaseKind::Project,
            project_id: project.id.clone(),
            tenant_ids: Vec::new(),
            target_url: gonvex_store::database_url(&path),
            target_path: path,
            source_url: source,
        });
    }
    let mut ordered = tenants.iter().collect::<Vec<_>>();
    ordered.sort_by(|left, right| {
        (left.deleted, &left.project_id, &left.tenant_id).cmp(&(
            right.deleted,
            &right.project_id,
            &right.tenant_id,
        ))
    });
    let empty = RouteRewrite {
        database_url: String::new(),
        database_name: String::new(),
    };
    // Source URLs owned by a deleted tenant only (rewritten, never exported).
    let mut deleted_only: BTreeMap<String, String> = BTreeMap::new();
    for tenant in ordered {
        let source = normalized(&tenant.database_url);
        if source.is_empty() {
            plan.tenants
                .insert(tenant.relationship_id.clone(), empty.clone());
            continue;
        }
        if let Some(index) = by_source.get(&source).copied() {
            let database = &mut plan.databases[index];
            if database.project_id != tenant.project_id {
                return Err(format!(
                    "tenant {:?} of project {:?} uses the database of project {:?}",
                    tenant.tenant_id, tenant.project_id, database.project_id
                )
                .into());
            }
            if tenant.deleted {
                // `database_name` is unique per project, so a deleted row
                // cannot share the live route; it keeps no route at all.
                plan.tenants
                    .insert(tenant.relationship_id.clone(), empty.clone());
                continue;
            }
            if let Some(first) = database.tenant_ids.first() {
                return Err(format!(
                    "tenants {:?} and {:?} of project {:?} share one Postgres database; it cannot be split into per-tenant SQLite files",
                    first, tenant.tenant_id, tenant.project_id
                )
                .into());
            }
            database.tenant_ids.push(tenant.tenant_id.clone());
            let route = rewrite(data, &database.target_path);
            plan.tenants.insert(tenant.relationship_id.clone(), route);
            continue;
        }
        if tenant.deleted {
            let route = match deleted_only.entry(source) {
                std::collections::btree_map::Entry::Occupied(_) => empty.clone(),
                std::collections::btree_map::Entry::Vacant(slot) => {
                    let path = data.tenant_path(&tenant.project_id, &tenant.tenant_id)?;
                    slot.insert(tenant.tenant_id.clone());
                    rewrite(data, &path)
                }
            };
            plan.tenants.insert(tenant.relationship_id.clone(), route);
            continue;
        }
        let path = data.tenant_path(&tenant.project_id, &tenant.tenant_id)?;
        by_source.insert(source.clone(), plan.databases.len());
        plan.tenants
            .insert(tenant.relationship_id.clone(), rewrite(data, &path));
        plan.databases.push(DatabasePlan {
            kind: DatabaseKind::Tenant,
            project_id: tenant.project_id.clone(),
            tenant_ids: vec![tenant.tenant_id.clone()],
            target_url: gonvex_store::database_url(&path),
            target_path: path,
            source_url: source,
        });
    }
    Ok(plan)
}

impl DatabasePlan {
    /// `--project` / `--tenant` filters (empty = everything).
    pub fn selected(&self, projects: &[String], tenants: &[String]) -> bool {
        (projects.is_empty() || projects.contains(&self.project_id))
            && (tenants.is_empty() || self.tenant_ids.iter().any(|id| tenants.contains(id)))
    }
}

/// Applies `FROM=TO` prefix rewrites to a source URL (for example a Docker
/// network host name that is not reachable from the operator's machine).
pub fn map_source_url(url: &str, maps: &[(String, String)]) -> String {
    for (from, to) in maps {
        if let Some(rest) = url.strip_prefix(from.as_str()) {
            return format!("{to}{rest}");
        }
    }
    url.to_owned()
}

/// A Postgres URL with its password replaced, safe for reports and logs.
pub fn redact_url(url: &str) -> String {
    let Some((scheme, rest)) = url.split_once("://") else {
        return url.to_owned();
    };
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let (authority, tail) = rest.split_at(authority_end);
    let Some((credentials, host)) = authority.rsplit_once('@') else {
        return url.to_owned();
    };
    let user = credentials.split(':').next().unwrap_or("");
    let masked = if credentials.contains(':') {
        format!("{user}:***")
    } else {
        user.to_owned()
    };
    let tail = redact_query_password(tail);
    format!("{scheme}://{masked}@{host}{tail}")
}

fn redact_query_password(tail: &str) -> String {
    let Some((path, query)) = tail.split_once('?') else {
        return tail.to_owned();
    };
    let query = query
        .split('&')
        .map(|pair| match pair.split_once('=') {
            Some((key, _)) if key.eq_ignore_ascii_case("password") => format!("{key}=***"),
            _ => pair.to_owned(),
        })
        .collect::<Vec<_>>()
        .join("&");
    format!("{path}?{query}")
}

/// Reads `NNNN_description.sql` migrations from a directory exactly as the
/// runtime reads them from a module artifact (`-- gonvex:scope` and
/// `-- gonvex:no-transaction` header directives), sorted by name. Other files
/// (for example `migrations.lock.json`) are ignored.
pub fn load_migrations(directory: &Path) -> Result<Vec<SqlMigration>> {
    let mut migrations = Vec::new();
    let entries = std::fs::read_dir(directory)
        .map_err(|error| format!("read migrations {}: {error}", directory.display()))?;
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|value| value.to_str()) != Some("sql") || !path.is_file() {
            continue;
        }
        let name = path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| format!("invalid migration path {}", path.display()))?
            .to_owned();
        let source = std::fs::read_to_string(&path)
            .map_err(|error| format!("read migration {name}: {error}"))?;
        migrations.push(parse_migration(&name, source)?);
    }
    migrations.sort_by(|left, right| left.name.cmp(&right.name));
    Ok(migrations)
}

pub fn parse_migration(name: &str, source: String) -> Result<SqlMigration> {
    if name.len() < 10
        || !name.as_bytes()[..4].iter().all(u8::is_ascii_digit)
        || name.as_bytes().get(4) != Some(&b'_')
        || !name.ends_with(".sql")
        || !name[5..name.len() - 4]
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(format!("migration {name:?} must be named NNNN_description.sql").into());
    }
    let mut scope = None;
    let mut no_transaction = false;
    for line in source.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(directive) = line.strip_prefix("--") else {
            break;
        };
        let directive = directive.trim();
        if let Some(value) = directive.strip_prefix("gonvex:scope") {
            scope = Some(match value.trim() {
                "tenant" => MigrationScope::Tenant,
                "control-plane" => MigrationScope::ControlPlane,
                value => return Err(format!("migration {name}: invalid scope {value:?}").into()),
            });
        } else if directive == "gonvex:no-transaction" {
            no_transaction = true;
        }
    }
    let scope = scope.ok_or_else(|| {
        format!("migration {name}: missing required -- gonvex:scope tenant|control-plane directive")
    })?;
    Ok(SqlMigration::new(
        name.to_owned(),
        scope,
        no_transaction,
        source,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn project(id: &str, url: &str) -> SourceProject {
        SourceProject {
            id: id.into(),
            database_url: url.into(),
        }
    }

    fn tenant(project: &str, id: &str, url: &str, deleted: bool) -> SourceTenant {
        SourceTenant {
            relationship_id: format!("{project}:{id}"),
            project_id: project.into(),
            tenant_id: id.into(),
            database_url: url.into(),
            deleted,
        }
    }

    #[test]
    fn routes_rewrite_to_data_dir_files() {
        let data = DataDir::new("/srv/data");
        let plan = plan_routes(
            &data,
            &[
                project("single", "postgres://u:p@db/single"),
                project("multi", "postgres://u:p@db/multi"),
                project("empty", ""),
            ],
            &[
                tenant("single", "single", " postgres://u:p@db/single ", false),
                tenant("multi", "acme", "postgres://u:p@db/multi_acme", false),
                tenant("multi", "beta", "postgres://u:p@db/multi_beta", false),
                tenant("multi", "gone", "postgres://u:p@db/multi_gone", true),
                tenant("multi", "nourl", "", false),
            ],
        )
        .unwrap();
        assert_eq!(
            plan.projects["single"],
            RouteRewrite {
                database_url: "sqlite:///srv/data/projects/single/project.db".into(),
                database_name: "projects/single/project.db".into(),
            }
        );
        assert_eq!(plan.projects["empty"].database_url, "");
        assert_eq!(
            plan.tenants["single:single"].database_url,
            "sqlite:///srv/data/projects/single/project.db"
        );
        assert_eq!(
            plan.tenants["multi:acme"],
            RouteRewrite {
                database_url: "sqlite:///srv/data/projects/multi/tenants/acme.db".into(),
                database_name: "projects/multi/tenants/acme.db".into(),
            }
        );
        // Deleted tenants are rewritten (no Postgres URL or password survives)
        // but their databases are not exported.
        assert_eq!(
            plan.tenants["multi:gone"].database_url,
            "sqlite:///srv/data/projects/multi/tenants/gone.db"
        );
        assert_eq!(plan.tenants["multi:nourl"].database_url, "");
        let exported = plan
            .databases
            .iter()
            .map(|database| (database.target_url.as_str(), database.tenant_ids.clone()))
            .collect::<Vec<_>>();
        assert_eq!(
            exported,
            vec![
                (
                    "sqlite:///srv/data/projects/single/project.db",
                    vec!["single".to_owned()]
                ),
                ("sqlite:///srv/data/projects/multi/project.db", vec![]),
                (
                    "sqlite:///srv/data/projects/multi/tenants/acme.db",
                    vec!["acme".to_owned()]
                ),
                (
                    "sqlite:///srv/data/projects/multi/tenants/beta.db",
                    vec!["beta".to_owned()]
                ),
            ]
        );
        let acme = &plan.databases[2];
        assert!(acme.selected(&[], &[]));
        assert!(acme.selected(&["multi".into()], &["acme".into()]));
        assert!(!acme.selected(&["single".into()], &[]));
        assert!(!acme.selected(&[], &["beta".into()]));
    }

    #[test]
    fn shared_tenant_databases_and_unsafe_ids_are_refused() {
        let data = DataDir::new("/srv/data");
        let shared = plan_routes(
            &data,
            &[project("p", "postgres://db/p")],
            &[
                tenant("p", "a", "postgres://db/shared", false),
                tenant("p", "b", "postgres://db/shared", false),
            ],
        );
        assert!(shared.unwrap_err().to_string().contains("share one"));
        let traversal = plan_routes(
            &data,
            &[project("p", "postgres://db/p")],
            &[tenant("p", "../x", "postgres://db/x", false)],
        );
        assert!(traversal.is_err());
        let cross = plan_routes(
            &data,
            &[
                project("p", "postgres://db/p"),
                project("q", "postgres://db/q"),
            ],
            &[tenant("q", "a", "postgres://db/p", false)],
        );
        assert!(cross.is_err());
    }

    #[test]
    fn source_urls_are_mapped_and_redacted() {
        let maps = vec![(
            "postgres://u:p@postgres:5432".to_owned(),
            "postgres://u:p@127.0.0.1:15432".to_owned(),
        )];
        assert_eq!(
            map_source_url("postgres://u:p@postgres:5432/tenant_a", &maps),
            "postgres://u:p@127.0.0.1:15432/tenant_a"
        );
        assert_eq!(
            map_source_url("postgres://other/x", &maps),
            "postgres://other/x"
        );
        assert_eq!(
            redact_url("postgres://gonvex:s3cret@db:5432/control?sslmode=disable&password=x"),
            "postgres://gonvex:***@db:5432/control?sslmode=disable&password=***"
        );
        assert_eq!(redact_url("postgres://db/control"), "postgres://db/control");
        assert_eq!(
            redact_url("postgres://user@db/control"),
            "postgres://user@db/control"
        );
    }

    #[test]
    fn migrations_parse_like_the_runtime() {
        let migration =
            parse_migration("0001_init.sql", "-- gonvex:scope tenant\n-- gonvex:no-transaction\nCREATE TABLE t(id TEXT PRIMARY KEY);".into())
                .unwrap();
        assert_eq!(migration.scope, MigrationScope::Tenant);
        assert!(migration.no_transaction);
        assert!(parse_migration("0001_init.sql", "CREATE TABLE t(id TEXT);".into()).is_err());
        assert!(parse_migration("init.sql", "-- gonvex:scope tenant\n".into()).is_err());
        let control = parse_migration(
            "0002_cp.sql",
            "-- gonvex:scope control-plane\nSELECT 1;".into(),
        )
        .unwrap();
        assert_eq!(control.scope, MigrationScope::ControlPlane);
    }
}
