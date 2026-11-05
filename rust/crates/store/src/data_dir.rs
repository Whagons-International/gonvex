//! Data directory layout and the single-owner lock.
//!
//! ```text
//! {data}/.gonvex.lock                              exclusive runtime lock
//! {data}/control.db                                Control Plane
//! {data}/projects/{project}/project.db             project (single-database) file
//! {data}/projects/{project}/tenants/{tenant}.db    tenant shard
//! ```

use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write as _;
use std::path::{Component, Path, PathBuf};

use crate::registry::database_url;
use crate::DatabaseError;

/// Environment variable naming the data directory.
pub const DATA_DIR_ENV: &str = "GONVEX_DATA_DIR";
/// Data directory used when [`DATA_DIR_ENV`] is unset.
pub const DEFAULT_DATA_DIR: &str = "./data";
/// Lock file name inside the data directory.
pub const LOCK_FILE: &str = ".gonvex.lock";

/// An absolute data directory and the database paths inside it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataDir {
    root: PathBuf,
}

impl DataDir {
    /// Uses `root` (relative paths resolve against the current directory).
    pub fn new(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref();
        let absolute = if root.is_absolute() {
            root.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|dir| dir.join(root))
                .unwrap_or_else(|_| root.to_path_buf())
        };
        Self {
            root: normalize(&absolute),
        }
    }

    /// Accepts a directory path or a `sqlite://` URL naming it. A URL or path
    /// that names a `.db` file (for example the Control Plane URL) selects the
    /// directory that contains it.
    pub fn parse(value: &str) -> Self {
        let trimmed = value.trim();
        let path = trimmed
            .strip_prefix("sqlite://")
            .or_else(|| trimmed.strip_prefix("sqlite:"))
            .unwrap_or(trimmed);
        let path = Path::new(path.split('?').next().unwrap_or(path));
        let directory = if path.extension().is_some_and(|extension| extension == "db") {
            path.parent().unwrap_or(path)
        } else {
            path
        };
        Self::new(if directory.as_os_str().is_empty() {
            Path::new(".")
        } else {
            directory
        })
    }

    /// `GONVEX_DATA_DIR`, or `./data` when unset or empty.
    pub fn from_env() -> Self {
        let configured = std::env::var(DATA_DIR_ENV).unwrap_or_default();
        let configured = configured.trim();
        Self::parse(if configured.is_empty() {
            DEFAULT_DATA_DIR
        } else {
            configured
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn lock_path(&self) -> PathBuf {
        self.root.join(LOCK_FILE)
    }

    pub fn control_path(&self) -> PathBuf {
        self.root.join("control.db")
    }

    pub fn control_url(&self) -> String {
        database_url(&self.control_path())
    }

    /// `{data}/projects/{project}`.
    pub fn project_dir(&self, project_id: &str) -> Result<PathBuf, DatabaseError> {
        Ok(self
            .root
            .join("projects")
            .join(path_segment("project id", project_id)?))
    }

    pub fn project_path(&self, project_id: &str) -> Result<PathBuf, DatabaseError> {
        Ok(self.project_dir(project_id)?.join("project.db"))
    }

    pub fn project_url(&self, project_id: &str) -> Result<String, DatabaseError> {
        Ok(database_url(&self.project_path(project_id)?))
    }

    pub fn tenant_path(&self, project_id: &str, tenant_id: &str) -> Result<PathBuf, DatabaseError> {
        Ok(self
            .project_dir(project_id)?
            .join("tenants")
            .join(format!("{}.db", path_segment("tenant id", tenant_id)?)))
    }

    pub fn tenant_url(&self, project_id: &str, tenant_id: &str) -> Result<String, DatabaseError> {
        Ok(database_url(&self.tenant_path(project_id, tenant_id)?))
    }

    /// The path of `path` relative to the data directory, if it lies inside it.
    pub fn relative(&self, path: &Path) -> Option<PathBuf> {
        normalize(path)
            .strip_prefix(&self.root)
            .ok()
            .map(Path::to_path_buf)
    }

    /// Takes the exclusive data-directory lock, creating the directory if
    /// needed. Fails with [`DatabaseError::DataDirLocked`] while another
    /// process (or another handle in this process) holds it. The lock is
    /// released when the returned guard is dropped or the process exits.
    pub fn lock(&self) -> Result<DataDirLock, DatabaseError> {
        std::fs::create_dir_all(&self.root).map_err(io_error)?;
        let path = self.lock_path();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .map_err(io_error)?;
        match file.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(DatabaseError::DataDirLocked(
                    self.root.display().to_string(),
                ))
            }
            Err(TryLockError::Error(error)) => return Err(io_error(error)),
        }
        // Diagnostics only: the flock, not the file contents, is the lock.
        let _ = file.set_len(0);
        let _ = writeln!(file, "{}", std::process::id());
        let _ = file.sync_all();
        Ok(DataDirLock { file, path })
    }
}

/// Holds the exclusive data-directory lock until dropped.
#[derive(Debug)]
pub struct DataDirLock {
    file: File,
    path: PathBuf,
}

impl DataDirLock {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DataDirLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

fn io_error(error: std::io::Error) -> DatabaseError {
    DatabaseError::Io(error.to_string())
}

/// A project or tenant id used as one path component. Ids are generated or
/// validated identifiers; anything that could traverse or hide is refused.
fn path_segment<'a>(kind: &str, value: &'a str) -> Result<&'a str, DatabaseError> {
    let valid = !value.is_empty()
        && value.len() <= 200
        && !value.starts_with('.')
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if valid {
        Ok(value)
    } else {
        Err(DatabaseError::Io(format!(
            "{kind} {value:?} cannot name a database file"
        )))
    }
}

/// Removes `.` components and resolves `..` lexically.
fn normalize(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }
    normalized
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_follows_the_storage_contract() {
        let data = DataDir::new("/srv/gonvex/./data");
        assert_eq!(data.root(), Path::new("/srv/gonvex/data"));
        assert_eq!(data.control_url(), "sqlite:///srv/gonvex/data/control.db");
        assert_eq!(
            data.project_url("p1").unwrap(),
            "sqlite:///srv/gonvex/data/projects/p1/project.db"
        );
        assert_eq!(
            data.tenant_url("p1", "t-1").unwrap(),
            "sqlite:///srv/gonvex/data/projects/p1/tenants/t-1.db"
        );
        assert_eq!(
            DataDir::parse("sqlite:///srv/gonvex/data/control.db").root(),
            Path::new("/srv/gonvex/data")
        );
        assert_eq!(
            DataDir::parse("sqlite:///srv/gonvex/data").root(),
            Path::new("/srv/gonvex/data")
        );
        for invalid in ["", "..", "../x", "a/b", ".hidden", "a\\b"] {
            assert!(data.tenant_path("p1", invalid).is_err(), "{invalid:?}");
            assert!(data.project_path(invalid).is_err(), "{invalid:?}");
        }
        assert_eq!(
            data.relative(Path::new("/srv/gonvex/data/projects/p1/project.db")),
            Some(PathBuf::from("projects/p1/project.db"))
        );
        assert_eq!(data.relative(Path::new("/srv/other.db")), None);
    }
}
