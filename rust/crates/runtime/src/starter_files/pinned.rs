//! Copied encoded input, not an authority or a validated-artifact certificate.
//! File stamps only invalidate existing copied bytes; they never bless new bytes.
use super::*;
use std::os::unix::fs::MetadataExt;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Stamp {
    dev: u64,
    ino: u64,
    len: u64,
    mtime: (i64, i64),
    ctime: (i64, i64),
}
impl Stamp {
    fn from(m: fs::Metadata) -> io::Result<Self> {
        if !m.is_file() {
            return Err(invalid());
        }
        Ok(Self {
            dev: m.dev(),
            ino: m.ino(),
            len: m.len(),
            mtime: (m.mtime(), m.mtime_nsec()),
            ctime: (m.ctime(), m.ctime_nsec()),
        })
    }
    fn path(&self, path: &Path) -> io::Result<()> {
        if Self::from(fs::symlink_metadata(path)?)? != *self {
            return Err(invalid());
        }
        Ok(())
    }
}
struct Encoded {
    bytes: Arc<[u8]>,
    stamp: Stamp,
}
/// All fields private, no Deserialize, no decoded row table. Only the transport's
/// fresh-SQL factory can promote this integrity-checked input into a proof.
pub(crate) struct Input {
    manifest: Arc<Manifest>,
    header: Stamp,
    pieces: Vec<Encoded>,
    retained_charge: usize,
}
fn read_fd(
    path: &Path,
    max: usize,
    mut charge: impl FnMut(usize) -> io::Result<()>,
) -> io::Result<(Vec<u8>, Stamp)> {
    let mut file = File::open(path)?;
    let before = Stamp::from(file.metadata()?)?;
    let len: usize = before.len.try_into().map_err(|_| invalid())?;
    if len > max {
        return Err(invalid());
    }
    // Charge before the read buffer, using metadata from this actual descriptor.
    charge(len.checked_add(1).ok_or_else(invalid)?)?;
    let mut bytes = Vec::with_capacity(len + 1);
    (&mut file).take((len as u64) + 1).read_to_end(&mut bytes)?;
    if bytes.len() != len || before != Stamp::from(file.metadata()?)? {
        return Err(invalid());
    }
    before.path(path)?;
    Ok((bytes, before))
}
impl Store {
    pub(crate) fn pin_input(
        &self,
        a: &Authority,
        mut charge: impl FnMut(usize) -> io::Result<()>,
    ) -> io::Result<Input> {
        let _guard = self.state.lock().unwrap();
        let path = self.root.join("manifests").join(a.identity.key());
        // The header can hold escaped source identity bytes. Eight times input
        // covers its owned parse/clone and the input buffer; reserve before parse.
        let (bytes, header) = read_fd(&path, 4 << 20, |n| {
            charge(n.checked_mul(8).ok_or_else(invalid)?)
        })?;
        let envelope: Envelope = serde_json::from_slice(&bytes)?;
        let m = envelope.manifest;
        if hash(&serde_json::to_vec(&m)?) != envelope.checksum || !self.usable(&m, a) {
            return Err(invalid());
        }
        let raw = m
            .pieces
            .iter()
            .try_fold(0usize, |n, p| n.checked_add(p.raw_bytes.try_into().ok()?))
            .ok_or_else(invalid)?;
        let encoded = m
            .pieces
            .iter()
            .try_fold(0usize, |n, p| n.checked_add(p.bytes.try_into().ok()?))
            .ok_or_else(invalid)?;
        if raw > format::MAX_TOTAL as usize
            || encoded > format::MAX_TOTAL as usize
            || m.rows > format::MAX_ROWS
        {
            return Err(invalid());
        }
        for p in &m.pieces {
            if !valid_hash(&p.hash)
                || !valid_hash(&p.raw_hash)
                || p.bytes > format::MAX_PIECE as u64
                || p.raw_bytes > format::MAX_PIECE as u64
            {
                return Err(invalid());
            }
        }
        // Bound retained header/piece metadata and Arc allocations, plus factory
        // scratch: one inflate/JSON value, canonical row, key set, and Vec->Arc copy.
        let retained = bytes
            .len()
            .checked_mul(8)
            .and_then(|n| n.checked_add(encoded))
            .and_then(|n| n.checked_add(m.pieces.len() * 512 + 1024))
            .ok_or_else(invalid)?;
        let peak = retained
            .checked_add(raw)
            .and_then(|n| n.checked_add(m.rows as usize * 128))
            .and_then(|n| n.checked_add(format::MAX_PIECE * 32))
            .ok_or_else(invalid)?;
        charge(peak)?;
        let mut pieces = Vec::with_capacity(m.pieces.len());
        for p in &m.pieces {
            let (bytes, stamp) = read_fd(
                &self.root.join("pieces").join(&p.hash),
                p.bytes as usize,
                |_| Ok(()),
            )?;
            diagnostics::add(diagnostics::Site::EncodedReadBytes, bytes.len() as u64);
            if bytes.len() as u64 != p.bytes || hash(&bytes) != p.hash {
                return Err(invalid());
            }
            pieces.push(Encoded {
                bytes: Arc::from(bytes),
                stamp,
            });
        }
        let input = Input {
            manifest: Arc::new(m),
            header,
            pieces,
            retained_charge: retained,
        };
        input.current(self, a)?;
        Ok(input)
    }
}
impl Input {
    pub(crate) fn manifest(&self) -> &Arc<Manifest> {
        &self.manifest
    }
    pub(crate) fn retained_charge(&self) -> usize {
        self.retained_charge
    }
    pub(crate) fn bytes(&self, index: usize) -> io::Result<&[u8]> {
        Ok(&self.pieces.get(index).ok_or_else(invalid)?.bytes)
    }
    pub(crate) fn current(&self, store: &Store, a: &Authority) -> io::Result<()> {
        if !store.usable(&self.manifest, a) {
            return Err(invalid());
        }
        self.header.path(
            &store
                .root
                .join("manifests")
                .join(self.manifest.identity.key()),
        )?;
        for (p, encoded) in self.manifest.pieces.iter().zip(&self.pieces) {
            encoded
                .stamp
                .path(&store.root.join("pieces").join(&p.hash))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn dir() -> tempfile::TempDir {
        tempfile::tempdir_in("/mnt/data/work/gonvex-starter-perf").unwrap()
    }
    #[test]
    fn starter_read_fd_uses_open_descriptor_and_rejects_replacement_and_symlink() {
        let d = dir();
        let path = d.path().join("encoded");
        fs::write(&path, b"first").unwrap();
        let mut admitted = 0;
        let (bytes, stamp) = read_fd(&path, 5, |n| {
            admitted = n;
            Ok(())
        })
        .unwrap();
        assert!(admitted >= bytes.len());
        let replacement = d.path().join("new");
        fs::write(&replacement, b"first").unwrap();
        fs::rename(replacement, &path).unwrap();
        assert!(stamp.path(&path).is_err());
        fs::remove_file(&path).unwrap();
        assert!(stamp.path(&path).is_err());
        let target = d.path().join("target");
        fs::write(&target, b"first").unwrap();
        std::os::unix::fs::symlink(&target, &path).unwrap();
        assert!(read_fd(&path, 5, |_| Ok(())).is_err());
        assert!(read_fd(&target, 4, |_| panic!("oversized input allocated")).is_err());
    }
    #[test]
    fn starter_read_fd_charges_before_read_and_detects_mutation_during_charge() {
        let d = dir();
        let path = d.path().join("encoded");
        fs::write(&path, b"first").unwrap();
        assert!(read_fd(&path, 5, |_| Err(io::Error::other("budget denied"))).is_err());
        let result = read_fd(&path, 5, |_| {
            fs::write(&path, b"other").unwrap();
            Ok(())
        });
        assert!(result.is_err(), "actual FD metadata changed while reading");
        // Pathname replacement at the admission boundary must also fail even
        // though the open FD still contains unchanged correctly sized bytes.
        let result = read_fd(&path, 5, |_| {
            let p = d.path().join("new");
            fs::write(&p, b"other").unwrap();
            fs::rename(p, &path).unwrap();
            Ok(())
        });
        assert!(result.is_err());
    }
    #[test]
    fn starter_copied_arc_never_changes_when_same_length_disk_bytes_restore_mtime() {
        let d = dir();
        let path = d.path().join("encoded");
        fs::write(&path, b"first").unwrap();
        let modified = fs::metadata(&path).unwrap().modified().unwrap();
        let (bytes, stamp) = read_fd(&path, 5, |_| Ok(())).unwrap();
        let copied: Arc<[u8]> = bytes.into();
        fs::write(&path, b"other").unwrap();
        File::options()
            .write(true)
            .open(&path)
            .unwrap()
            .set_times(fs::FileTimes::new().set_modified(modified))
            .unwrap();
        assert!(
            stamp.path(&path).is_err(),
            "restored mtime must not hide ctime mutation"
        );
        assert_eq!(&*copied, b"first");
    }
}
