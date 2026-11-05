//! Versioned disk backend for the private indexed snapshot experiment.
//! Default-off transport is connection-scoped and requires a sealed source proof.
//! No raw-content-hash endpoint or normal provisioning enables this module.
//! Callers must hold the authoritative commit/admission lease while using
//! `publish`/`read`; a digest is never an authorization capability.
#![allow(dead_code)]
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};

const FORMAT: u32 = 1;
// Exactly the websocket level-1, 12-bit, raw RFC7692 no-context compressor.
const ENCODING: &str = "json-lines-pmd-level1-window12-no-context-v1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Identity {
    pub canonical_set: Vec<u8>,
    pub source_authority: Vec<u8>,
    pub module: String,
    pub permission_generation: i64,
    pub permission_serial: i64,
    pub encoding: String,
}
impl Identity {
    pub fn new(
        exact: &crate::permissions::model::ExactPermissionSet,
        module: String,
        generation: i64,
        serial: i64,
        source_authority: Vec<u8>,
    ) -> Self {
        Self {
            canonical_set: exact.canonical.clone(),
            source_authority,
            module,
            permission_generation: generation,
            permission_serial: serial,
            encoding: ENCODING.into(),
        }
    }
    fn key(&self) -> String {
        hash(&serde_json::to_vec(self).expect("identity"))
    }
}
#[derive(Clone, Debug)]
pub(crate) struct Authority {
    pub identity: Identity,
    pub epoch: String,
    pub revision: u64,
    pub retained_revision: u64,
    pub index_ready: bool,
    pub dependency_complete: bool,
    pub active_account: bool,
    pub active_member: bool,
    pub supported_shared_collection: bool,
    pub now_ms: u64,
    pub valid_until_ms: u64,
}
impl Authority {
    fn admits(&self, identity: &Identity) -> bool {
        self.index_ready
            && self.dependency_complete
            && self.active_account
            && self.active_member
            && self.supported_shared_collection
            && self.valid_until_ms > self.now_ms
            && self.identity == *identity
            && identity.encoding == ENCODING
            && !identity.canonical_set.is_empty()
            && identity.canonical_set.len() <= (1 << 20)
            && !identity.source_authority.is_empty()
            && identity.source_authority.len() <= (1 << 20)
            && !identity.module.is_empty()
            && identity.permission_generation > 0
    }
}
#[derive(Clone)]
pub(crate) struct Limits {
    pub disk_bytes: u64,
    pub manifests: usize,
    pub queued: usize,
    pub piece_bytes: usize,
    pub pieces: usize,
    pub ttl_ms: u64,
    pub rebuild_rows: u64,
    pub rebuild_bytes: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            disk_bytes: 256 << 20,
            manifests: 128,
            queued: 16,
            piece_bytes: 256 << 10,
            pieces: 1024,
            ttl_ms: 3_600_000,
            rebuild_rows: 10_000,
            rebuild_bytes: 8 << 20,
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Piece {
    pub hash: String,
    pub raw_hash: String,
    pub compressed: bool,
    pub bytes: u64,
    pub raw_bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
    pub format: u32,
    pub identity: Identity,
    pub epoch: String,
    pub revision: u64,
    pub expires_ms: u64,
    pub pieces: Vec<Piece>,
    pub rows: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    manifest: Manifest,
    checksum: String,
}
#[derive(Default)]
struct State {
    queued: VecDeque<Identity>,
    pending: BTreeSet<String>,
}
pub(crate) struct Store {
    _root_lock: File,
    build_slot: Arc<tokio::sync::Semaphore>,
    queue_slots: Arc<tokio::sync::Semaphore>,
    root: PathBuf,
    limits: Limits,
    state: Mutex<State>,
}
fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}
fn invalid() -> io::Error {
    io::Error::other("starter rejected; use normal open")
}
fn valid_hash(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl Store {
    /// One store per process/data root; runtime data-dir ownership is required.
    /// Directory must be private (artifacts can contain authorized row data).
    pub fn open(root: &Path, limits: Limits) -> io::Result<Arc<Self>> {
        if limits.piece_bytes == 0
            || limits.pieces == 0
            || limits.queued == 0
            || limits.manifests == 0
        {
            return Err(invalid());
        }
        fs::create_dir_all(root)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(root, fs::Permissions::from_mode(0o700))?;
        }
        let root_lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(root.join(".store.lock"))?;
        root_lock.try_lock().map_err(io::Error::other)?;
        for child in ["pieces", "manifests", "tmp"] {
            fs::create_dir_all(root.join(child))?;
        }
        let this = Arc::new(Self {
            _root_lock: root_lock,
            build_slot: Arc::new(tokio::sync::Semaphore::new(1)),
            queue_slots: Arc::new(tokio::sync::Semaphore::new(limits.queued)),
            root: root.to_path_buf(),
            limits,
            state: Mutex::new(State::default()),
        });
        // Interrupted builds have no manifest; remove their temporary files.
        for entry in fs::read_dir(this.root.join("tmp"))? {
            fs::remove_file(entry?.path())?;
        }
        this.cleanup(0)?;
        Ok(this)
    }
    /// Called only AFTER the normal first-load/miss. Never builds in open.
    /// Current cache cursors must bypass this method entirely.
    pub fn enqueue_after_miss(&self, authority: &Authority) -> bool {
        if !authority.admits(&authority.identity) {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        if state.pending.len() >= self.limits.queued
            || !state.pending.insert(authority.identity.key())
        {
            return false;
        }
        state.queued.push_back(authority.identity.clone());
        true
    }
    /// Background worker obtains one singleflight job. Dropping it cancels and
    /// releases ownership, including error paths and early returns.
    pub fn next(self: &Arc<Self>) -> Option<Build> {
        let identity = self.state.lock().unwrap().queued.pop_front()?;
        Some(Build {
            store: self.clone(),
            identity,
            cancelled: Arc::new(AtomicBool::new(false)),
            manifest: None,
            buffer: Vec::new(),
            staged: Vec::new(),
        })
    }
    pub fn try_queue_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        self.queue_slots.clone().try_acquire_owned().ok()
    }
    pub async fn build_permit(&self) -> io::Result<tokio::sync::OwnedSemaphorePermit> {
        self.build_slot
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| invalid())
    }
    pub fn take(self: &Arc<Self>, identity: &Identity) -> Option<Build> {
        let mut state = self.state.lock().unwrap();
        let index = state.queued.iter().position(|i| i == identity)?;
        let identity = state.queued.remove(index)?;
        Some(Build {
            store: self.clone(),
            identity,
            cancelled: Arc::new(AtomicBool::new(false)),
            manifest: None,
            buffer: Vec::new(),
            staged: Vec::new(),
        })
    }
    fn load(&self, identity: &Identity) -> io::Result<Manifest> {
        let bytes = read_bounded(&self.root.join("manifests").join(identity.key()), 4 << 20)?;
        let envelope: Envelope = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if hash(&serde_json::to_vec(&envelope.manifest)?) != envelope.checksum {
            return Err(invalid());
        }
        Ok(envelope.manifest)
    }
    fn usable(&self, m: &Manifest, a: &Authority) -> bool {
        a.admits(&m.identity)
            && m.format == FORMAT
            && m.identity == a.identity
            && m.epoch == a.epoch
            && m.revision >= a.retained_revision
            && m.revision <= a.revision
            && m.expires_ms > a.now_ms
            && m.pieces.len() <= self.limits.pieces
    }
    /// Streams one bounded encoded piece at a time. `current` must obtain fresh
    /// account/member/permission authority under the caller's admission barrier;
    /// the barrier stays held through `visit`. Never exposes a filesystem URL.
    pub fn read(
        &self,
        identity: &Identity,
        mut current: impl FnMut() -> io::Result<Authority>,
        mut visit: impl FnMut(&Piece, &[u8]) -> io::Result<()>,
    ) -> io::Result<Manifest> {
        let _guard = self.state.lock().unwrap();
        let m = self.load(identity)?;
        if m.identity != *identity || !self.usable(&m, &current()?) {
            return Err(invalid());
        }
        // Validate ALL pieces before delivering any bytes. A corrupt later piece
        // cannot leave a partially hydrated bootstrap advertised as complete.
        for p in &m.pieces {
            self.read_piece(p)?;
        }
        for p in &m.pieces {
            let bytes = self.read_piece(p)?;
            if !self.usable(&m, &current()?) {
                return Err(invalid());
            }
            visit(p, &bytes)?;
        }
        if !self.usable(&m, &current()?) {
            return Err(invalid());
        }
        Ok(m)
    }
    /// Authenticated manifest-scoped lookup. A content hash by itself is never
    /// accepted as authority, even when the corresponding file exists on disk.
    pub fn read_selected_piece(
        &self,
        identity: &Identity,
        requested_hash: &str,
        current: impl FnMut() -> io::Result<Authority>,
        mut visit: impl FnMut(&Piece, &[u8]) -> io::Result<()>,
    ) -> io::Result<Manifest> {
        let mut found = false;
        let manifest = self.read(identity, current, |piece, bytes| {
            if piece.hash == requested_hash && !found {
                found = true;
                visit(piece, bytes)?;
            }
            Ok(())
        })?;
        if !found {
            return Err(invalid());
        }
        Ok(manifest)
    }
    /// Transport-only read after the complete offered format was verified.
    /// The exact immutable manifest is pinned, and every selected piece still
    /// gets fresh authority + encoded integrity before its bounded enqueue.
    pub(crate) fn read_pinned_piece(
        &self,
        expected: &Manifest,
        index: usize,
        mut current: impl FnMut() -> io::Result<Authority>,
        mut visit: impl FnMut(&Piece, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        let _guard = self.state.lock().unwrap();
        let actual = self.load(&expected.identity)?;
        if serde_json::to_vec(&actual)? != serde_json::to_vec(expected)?
            || !self.usable(&actual, &current()?)
        {
            return Err(invalid());
        }
        let piece = actual.pieces.get(index).ok_or_else(invalid)?;
        let bytes = self.read_piece(piece)?;
        if !self.usable(&actual, &current()?) {
            return Err(invalid());
        }
        visit(piece, &bytes)
    }
    fn read_piece(&self, p: &Piece) -> io::Result<Vec<u8>> {
        if !valid_hash(&p.hash)
            || !valid_hash(&p.raw_hash)
            || p.raw_bytes > self.limits.piece_bytes as u64
            || p.bytes > self.limits.piece_bytes as u64
        {
            return Err(invalid());
        }
        let bytes = read_bounded(
            &self.root.join("pieces").join(&p.hash),
            self.limits.piece_bytes,
        )?;
        if bytes.len() as u64 != p.bytes || hash(&bytes) != p.hash {
            return Err(invalid());
        }
        Ok(bytes)
    }
    pub fn should_rebuild(
        &self,
        m: &Manifest,
        a: &Authority,
        changed_rows: u64,
        changed_bytes: u64,
    ) -> bool {
        !self.usable(m, a)
            || changed_rows > self.limits.rebuild_rows
            || changed_bytes > self.limits.rebuild_bytes
    }
    pub fn cleanup(&self, now_ms: u64) -> io::Result<()> {
        let _guard = self.state.lock().unwrap();
        self.cleanup_locked(now_ms)
    }
    fn cleanup_locked(&self, now_ms: u64) -> io::Result<()> {
        let mut reachable = BTreeSet::new();
        for entry in fs::read_dir(self.root.join("manifests"))? {
            let path = entry?.path();
            let m = read_bounded(&path, 4 << 20).ok().and_then(|b| {
                serde_json::from_slice::<Envelope>(&b)
                    .ok()
                    .filter(|e| {
                        serde_json::to_vec(&e.manifest)
                            .is_ok_and(|bytes| hash(&bytes) == e.checksum)
                    })
                    .map(|e| e.manifest)
            });
            match m {
                Some(m)
                    if m.format == FORMAT
                        && m.expires_ms > now_ms
                        && m.pieces.len() <= self.limits.pieces
                        && m.pieces.iter().all(|p| valid_hash(&p.hash)) =>
                {
                    reachable.extend(m.pieces.into_iter().map(|p| p.hash));
                }
                _ => {
                    fs::remove_file(path)?;
                }
            }
        }
        for entry in fs::read_dir(self.root.join("pieces"))? {
            let path = entry?.path();
            if !reachable.contains(&path.file_name().unwrap().to_string_lossy().into_owned()) {
                fs::remove_file(path)?;
            }
        }
        Ok(())
    }
    fn disk_bytes(&self) -> io::Result<u64> {
        let mut size = 0u64;
        for dir in ["pieces", "manifests", "tmp"] {
            for entry in fs::read_dir(self.root.join(dir))? {
                size = size.saturating_add(entry?.metadata()?.len());
            }
        }
        Ok(size)
    }
}
fn read_bounded(path: &Path, max: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    File::open(path)?
        .take(max as u64 + 1)
        .read_to_end(&mut bytes)?;
    if bytes.len() > max {
        return Err(invalid());
    }
    Ok(bytes)
}
fn atomic(root: &Path, target: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = root.join("tmp").join(uuid::Uuid::new_v4().to_string());
    let result = (|| {
        let mut file = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&tmp, target)?;
        File::open(target.parent().unwrap())?.sync_all()
    })();
    let _ = fs::remove_file(tmp);
    result
}
pub(crate) struct Build {
    store: Arc<Store>,
    identity: Identity,
    cancelled: Arc<AtomicBool>,
    manifest: Option<Manifest>,
    buffer: Vec<u8>,
    staged: Vec<(Piece, PathBuf)>,
}
impl Build {
    pub fn cancellation(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }
    pub fn begin(&mut self, snapshot: &Authority) -> io::Result<()> {
        if !snapshot.admits(&self.identity) || self.manifest.is_some() {
            return Err(invalid());
        }
        self.manifest = Some(Manifest {
            format: FORMAT,
            identity: self.identity.clone(),
            epoch: snapshot.epoch.clone(),
            revision: snapshot.revision,
            expires_ms: snapshot
                .now_ms
                .saturating_add(self.store.limits.ttl_ms)
                .min(snapshot.valid_until_ms),
            pieces: Vec::new(),
            rows: 0,
        });
        Ok(())
    }
    /// SQL snapshot visitor feeds projected rows; never clones a full table.
    pub fn row(&mut self, row: &impl Serialize) -> io::Result<()> {
        self.check()?;
        let mut bytes = Vec::new();
        serde_json::to_writer(
            CappedWriter {
                bytes: &mut bytes,
                max: self.store.limits.piece_bytes.saturating_sub(1),
            },
            row,
        )?;
        bytes.push(b'\n');
        if bytes.len() > self.store.limits.piece_bytes {
            return Err(invalid());
        }
        if self.buffer.len() + bytes.len() > self.store.limits.piece_bytes {
            self.flush()?;
        }
        self.buffer.extend_from_slice(&bytes);
        self.manifest.as_mut().ok_or_else(invalid)?.rows += 1;
        Ok(())
    }
    fn check(&self) -> io::Result<()> {
        if self.cancelled.load(Ordering::Acquire) || self.manifest.is_none() {
            Err(invalid())
        } else {
            Ok(())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        self.check()?;
        if self.buffer.is_empty() {
            return Ok(());
        }
        if self.staged.len() >= self.store.limits.pieces {
            return Err(invalid());
        }
        let encoded = gonvex_websocket::bench_compress_text(&self.buffer, 12)?;
        let compressed = encoded.is_some();
        let bytes = encoded.as_deref().unwrap_or(&self.buffer);
        let p = Piece {
            hash: hash(bytes),
            raw_hash: hash(&self.buffer),
            compressed,
            bytes: bytes.len() as u64,
            raw_bytes: self.buffer.len() as u64,
        };
        // Serialize disk accounting with publication and GC; staging is bounded.
        let _guard = self.store.state.lock().unwrap();
        if self.store.disk_bytes()?.saturating_add(p.bytes) > self.store.limits.disk_bytes {
            return Err(invalid());
        }
        let path = self
            .store
            .root
            .join("tmp")
            .join(uuid::Uuid::new_v4().to_string());
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        self.staged.push((p, path));
        self.buffer.clear();
        Ok(())
    }
    /// Caller holds writer/module/account admission barriers. Authority is read
    /// AFTER compression and again immediately before atomic manifest rename.
    pub fn publish(
        mut self,
        mut current: impl FnMut() -> io::Result<Authority>,
    ) -> io::Result<Manifest> {
        self.flush()?;
        self.check()?;
        let store = self.store.clone();
        let _guard = store.state.lock().unwrap();
        let mut m = self.manifest.take().ok_or_else(invalid)?;
        m.pieces = self.staged.iter().map(|(p, _)| p.clone()).collect();
        if !store.usable(&m, &current()?) {
            return Err(invalid());
        }
        let target = store.root.join("manifests").join(self.identity.key());
        let count = fs::read_dir(store.root.join("manifests"))?.count();
        if !target.exists() && count >= store.limits.manifests {
            return Err(invalid());
        }
        for (p, path) in &self.staged {
            let dest = store.root.join("pieces").join(&p.hash);
            // Replace even an existing corrupt file; content is deterministic.
            fs::rename(path, dest)?;
        }
        File::open(store.root.join("pieces"))?.sync_all()?;
        let bytes = serde_json::to_vec(&Envelope {
            checksum: hash(&serde_json::to_vec(&m)?),
            manifest: m.clone(),
        })?;
        if bytes.len() > 4 << 20
            || store.disk_bytes()?.saturating_add(bytes.len() as u64) > store.limits.disk_bytes
        {
            return Err(invalid());
        }
        if self.cancelled.load(Ordering::Acquire) || !store.usable(&m, &current()?) {
            return Err(invalid());
        }
        atomic(&store.root, &target, &bytes)?;
        store.cleanup_locked(current()?.now_ms)?;
        Ok(m)
    }
}
impl Drop for Build {
    fn drop(&mut self) {
        for (_, path) in &self.staged {
            let _ = fs::remove_file(path);
        }
        self.store
            .state
            .lock()
            .unwrap()
            .pending
            .remove(&self.identity.key());
    }
}
#[cfg(test)]
mod tests;

struct CappedWriter<'a> {
    bytes: &'a mut Vec<u8>,
    max: usize,
}
impl Write for CappedWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self.bytes.len().saturating_add(bytes.len()) > self.max {
            return Err(invalid());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(crate) mod format;

pub(crate) mod provider;

#[cfg(test)]
pub(crate) fn capture_allocations(start: bool) -> (u64, u64) {
    tests::capture_all(start)
}
