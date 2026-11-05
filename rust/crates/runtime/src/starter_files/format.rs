//! Bounded verification of the REAL disk v1 format before transport admission.
//! This produces integrity metadata, never an authorization capability.
use super::{hash, invalid, Manifest, Piece, ENCODING, FORMAT};
use crate::modules::ReplicaCollectionDefinition;
use gonvex_protocol::replica_digest::ReplicaSetDigest;
use serde_json::Value;
use std::{collections::BTreeSet, io};
#[path = "strict_json.rs"]
mod strict_json;

pub(crate) const MAX_PIECE: usize = 256 << 10;
pub(crate) const MAX_TOTAL: u64 = 32 << 20;
pub(crate) const MAX_ROWS: u64 = 100_000;

pub(crate) fn decode(piece: &Piece, bytes: &[u8]) -> io::Result<Vec<u8>> {
    if bytes.len() > MAX_PIECE
        || bytes.len() as u64 != piece.bytes
        || piece.raw_bytes > MAX_PIECE as u64
        || hash(bytes) != piece.hash
    {
        return Err(invalid());
    }
    let raw = if piece.compressed {
        let mut input = Vec::with_capacity(bytes.len() + 4);
        input.extend_from_slice(bytes);
        input.extend_from_slice(&[0, 0, 255, 255]);
        // Bound allocation AND output before invoking the inflater. One extra
        // byte detects forged expanded lengths without allocating the bomb.
        let mut out = vec![0; piece.raw_bytes as usize + 1];
        let mut codec = flate2::Decompress::new_with_window_bits(false, 12);
        codec
            .decompress(&input, &mut out, flate2::FlushDecompress::Sync)
            .map_err(io::Error::other)?;
        if codec.total_out() != piece.raw_bytes || codec.total_in() != input.len() as u64 {
            return Err(invalid());
        }
        out.truncate(piece.raw_bytes as usize);
        out
    } else {
        bytes.to_vec()
    };
    if raw.len() as u64 != piece.raw_bytes || hash(&raw) != piece.raw_hash {
        return Err(invalid());
    }
    Ok(raw)
}

pub(crate) struct Verifier {
    definition: ReplicaCollectionDefinition,
    next: usize,
    rows: u64,
    bytes: u64,
    keys: BTreeSet<String>,
    digest: ReplicaSetDigest,
}
#[derive(Clone, Debug)]
pub(crate) struct Verified {
    pub rows: u64,
    pub bytes: u64,
    pub digest: String,
}
impl Verifier {
    pub fn new(definition: &ReplicaCollectionDefinition, manifest: &Manifest) -> io::Result<Self> {
        if manifest.format != FORMAT
            || manifest.identity.encoding != ENCODING
            || manifest.rows > MAX_ROWS
            || manifest.pieces.len() > 1024
            || manifest
                .pieces
                .iter()
                .try_fold(0u64, |n, p| n.checked_add(p.raw_bytes))
                .is_none_or(|n| n > MAX_TOTAL)
            || !definition.equal_filters.is_empty()
            || definition.max_bytes < 0
            || definition.key.is_empty()
            || definition.columns.iter().collect::<BTreeSet<_>>().len() != definition.columns.len()
            || !definition.columns.contains(&definition.key)
            || !definition.columns.contains(&definition.order_by)
        {
            return Err(invalid());
        }
        Ok(Self {
            definition: definition.clone(),
            next: 0,
            rows: 0,
            bytes: 0,
            keys: BTreeSet::new(),
            digest: ReplicaSetDigest::default(),
        })
    }
    pub fn piece(
        &mut self,
        manifest: &Manifest,
        index: usize,
        bytes: &[u8],
        mut row: impl FnMut(&Value) -> io::Result<String>,
    ) -> io::Result<()> {
        if index != self.next {
            return Err(invalid());
        }
        let piece = manifest.pieces.get(index).ok_or_else(invalid)?;
        let raw = decode(piece, bytes)?;
        if raw.is_empty() || raw.last() != Some(&b'\n') {
            return Err(invalid());
        }
        for line in raw[..raw.len() - 1].split(|b| *b == b'\n') {
            if line.is_empty() || line.len() > MAX_PIECE {
                return Err(invalid());
            }
            let value: Value = strict_json::parse(line)?;
            let object = value.as_object().ok_or_else(invalid)?;
            if object.len() != self.definition.columns.len()
                || object.keys().any(|k| !self.definition.columns.contains(k))
            {
                return Err(invalid());
            }
            let key = value
                .get(&self.definition.key)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty() && !s.contains('\0'))
                .ok_or_else(invalid)?;
            self.rows = self
                .rows
                .checked_add(1)
                .filter(|n| *n <= MAX_ROWS)
                .ok_or_else(invalid)?;
            self.bytes = self
                .bytes
                .checked_add(line.len() as u64)
                .filter(|n| *n <= MAX_TOTAL)
                .ok_or_else(invalid)?;
            if !self.keys.insert(key.into())
                || (self.definition.max_rows > 0 && self.rows > self.definition.max_rows as u64)
                || (self.definition.max_bytes > 0 && self.bytes > self.definition.max_bytes as u64)
            {
                return Err(invalid());
            }
            let row_hash = row(&value)?;
            self.digest.add(&row_hash).map_err(io::Error::other)?;
        }
        self.next += 1;
        Ok(())
    }
    pub fn finish(self, manifest: &Manifest) -> io::Result<Verified> {
        if self.next != manifest.pieces.len() || self.rows != manifest.rows {
            return Err(invalid());
        }
        Ok(Verified {
            rows: self.rows,
            bytes: self.bytes,
            digest: self.digest.legacy_digest(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::starter_files::{Authority, Identity, Limits, Store};
    use serde_json::json;
    #[test]
    fn verifies_real_disk_projection_and_rejects_inflation_bomb_before_output() {
        let definition:ReplicaCollectionDefinition=serde_json::from_value(json!({"table":"tasks","key":"id","columns":["id","body"],"orderBy":"id","orderDirection":"asc","mode":"window","maxRows":10})).unwrap();
        let dir = tempfile::tempdir_in("/mnt/data/work/gonvex-starter-bootstrap").unwrap();
        let store = Store::open(dir.path(), Limits::default()).unwrap();
        let authority = Authority {
            identity: Identity {
                canonical_set: vec![1],
                source_authority: vec![2],
                module: "fixture".into(),
                permission_generation: 1,
                permission_serial: 1,
                encoding: ENCODING.into(),
            },
            epoch: "e".into(),
            revision: 1,
            retained_revision: 0,
            index_ready: true,
            dependency_complete: true,
            active_account: true,
            active_member: true,
            supported_shared_collection: true,
            now_ms: 1,
            valid_until_ms: 1000,
        };
        assert!(store.enqueue_after_miss(&authority));
        let mut build = store.next().unwrap();
        build.begin(&authority).unwrap();
        build
            .row(&json!({"id":"a","body":"x".repeat(10000)}))
            .unwrap();
        let manifest = build.publish(|| Ok(authority.clone())).unwrap();
        let mut v = Verifier::new(&definition, &manifest).unwrap();
        store
            .read(
                &authority.identity,
                || Ok(authority.clone()),
                |_, b| {
                    v.piece(&manifest, 0, b, |row| {
                        Ok(hash(crate::replica::stable_json_text(row).as_bytes()))
                    })
                },
            )
            .unwrap();
        assert_eq!(v.finish(&manifest).unwrap().rows, 1);
        store
            .read(
                &authority.identity,
                || Ok(authority.clone()),
                |p, b| {
                    let mut forged = p.clone();
                    forged.raw_bytes = 1;
                    assert!(decode(&forged, b).is_err());
                    Ok(())
                },
            )
            .unwrap();
        let mut bad = definition;
        bad.columns = vec!["id".into()];
        let mut v = Verifier::new(&bad, &manifest).unwrap();
        assert!(store
            .read(
                &authority.identity,
                || Ok(authority.clone()),
                |_, b| v.piece(&manifest, 0, b, |_| panic!("private field accepted"))
            )
            .is_err());
    }
}
