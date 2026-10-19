//! Constant-size, reversible row-hash summaries for negotiated Replica sync.
//! Row hashes include identity and the canonical authorized projection. Storage
//! must subtract the previous hash atomically before replacing a row.

const BUCKETS: usize = 256;
const RECEIPT_BYTES: usize = 16 + (BUCKETS + 1) * 16 + 32;
const MAX_COUNT: u64 = 9_007_199_254_740_991;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSetDigest {
    sums: [u128; BUCKETS + 1],
    legacy: [u8; 32],
    rows: u64,
}
impl Default for ReplicaSetDigest {
    fn default() -> Self {
        Self {
            sums: [0; BUCKETS + 1],
            legacy: [0; 32],
            rows: 0,
        }
    }
}
impl ReplicaSetDigest {
    pub fn count(&self) -> u64 {
        self.rows
    }
    pub fn add(&mut self, hash: &str) -> Result<(), &'static str> {
        self.change(hash, false)
    }
    pub fn remove(&mut self, hash: &str) -> Result<(), &'static str> {
        self.change(hash, true)
    }
    fn change(&mut self, hash: &str, remove: bool) -> Result<(), &'static str> {
        if hash.len() != 64
            || !hash
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("Replica row hash must be canonical SHA-256 hex");
        }
        let count = if remove {
            self.rows.checked_sub(1)
        } else {
            self.rows.checked_add(1).filter(|n| *n <= MAX_COUNT)
        }
        .ok_or("Replica digest count is invalid")?;
        let mut bytes = [0u8; 32];
        for (i, byte) in bytes.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16).unwrap();
        }
        let value = u128::from_be_bytes(bytes[..16].try_into().unwrap());
        let bucket = usize::from(bytes[31]) + 1;
        for index in [0, bucket] {
            self.sums[index] = if remove {
                self.sums[index].wrapping_sub(value)
            } else {
                self.sums[index].wrapping_add(value)
            };
        }
        for (out, byte) in self.legacy.iter_mut().zip(bytes) {
            *out ^= byte;
        }
        self.rows = count;
        Ok(())
    }
    pub fn digest(&self) -> String {
        format!("sum128-v1:{:032x}", self.sums[0])
    }
    pub fn legacy_digest(&self) -> String {
        format!(
            "xor1:{}",
            self.legacy
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>()
        )
    }
    pub fn bucket(&self, bucket: usize) -> Option<String> {
        self.sums
            .get(bucket.checked_add(1)?)
            .map(|sum| format!("{sum:032x}"))
    }
    /// Exact receipt shared with the TypeScript implementation: version,
    /// reserved word, binary64 safe count, 257 little-endian sums, XOR bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = vec![0; RECEIPT_BYTES];
        bytes[..4].copy_from_slice(&1u32.to_le_bytes());
        bytes[8..16].copy_from_slice(&(self.rows as f64).to_le_bytes());
        for (i, sum) in self.sums.iter().enumerate() {
            bytes[16 + i * 16..32 + i * 16].copy_from_slice(&sum.to_le_bytes());
        }
        bytes[RECEIPT_BYTES - 32..].copy_from_slice(&self.legacy);
        bytes
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, &'static str> {
        if bytes.len() != RECEIPT_BYTES {
            return Err("Invalid replica digest receipt length");
        }
        if bytes[..4] != 1u32.to_le_bytes() || bytes[4..8] != [0; 4] {
            return Err("Unsupported replica digest receipt format");
        }
        let count = f64::from_le_bytes(bytes[8..16].try_into().unwrap());
        if !count.is_finite() || count < 0.0 || count.fract() != 0.0 || count > MAX_COUNT as f64 {
            return Err("Invalid replica digest receipt count");
        }
        let mut result = Self {
            rows: count as u64,
            ..Self::default()
        };
        for (i, sum) in result.sums.iter_mut().enumerate() {
            *sum = u128::from_le_bytes(bytes[16 + i * 16..32 + i * 16].try_into().unwrap());
        }
        result.legacy.copy_from_slice(&bytes[RECEIPT_BYTES - 32..]);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn reversible_carries_buckets_and_legacy_parity() {
        let mut digest = ReplicaSetDigest::default();
        let high = format!("{}{}", "ff".repeat(16), "00".repeat(16));
        let low = format!("{}01{}01", "00".repeat(15), "00".repeat(15));
        digest.add(&high).unwrap();
        digest.add(&low).unwrap();
        assert_eq!(digest.digest(), format!("sum128-v1:{}", "00".repeat(16)));
        assert_eq!(digest.bucket(0).unwrap(), "ff".repeat(16));
        assert_eq!(digest.bucket(1).unwrap(), format!("{}01", "00".repeat(15)));
        assert_eq!(digest.count(), 2);
        let bytes = digest.encode();
        assert_eq!(bytes.len(), 4160);
        assert_eq!(
            &bytes[..16],
            &[1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64]
        );
        assert_eq!(ReplicaSetDigest::decode(&bytes).unwrap(), digest);
        digest.remove(&high).unwrap();
        assert_eq!(digest.legacy_digest(), format!("xor1:{low}"));
        digest.remove(&low).unwrap();
        assert_eq!(digest, ReplicaSetDigest::default());
        assert!(digest.bucket(256).is_none());
    }
    #[test]
    fn invalid_input_never_changes_state() {
        let mut digest = ReplicaSetDigest::default();
        let before = digest.clone();
        for bad in ["bad".to_owned(), "AA".repeat(32), "é".repeat(32)] {
            assert!(digest.add(&bad).is_err());
            assert_eq!(digest, before);
        }
        assert!(digest.remove(&"00".repeat(32)).is_err());
        assert_eq!(digest, before);
        let mut receipt = digest.encode();
        receipt[4] = 1;
        assert!(ReplicaSetDigest::decode(&receipt).is_err());
        let mut receipt = digest.encode();
        receipt[8..16].copy_from_slice(&f64::NAN.to_le_bytes());
        assert!(ReplicaSetDigest::decode(&receipt).is_err());
        assert!(ReplicaSetDigest::decode(&[0]).is_err());
    }
    #[test]
    fn shared_typescript_receipt_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../packages/protocol/fixtures/replica-set-digest.json"
        ))
        .unwrap();
        let mut digest = ReplicaSetDigest::default();
        for mutation in fixture["mutations"].as_array().unwrap() {
            let hash = mutation["hash"].as_str().unwrap();
            if mutation["op"] == "add" {
                digest.add(hash).unwrap();
            } else {
                digest.remove(hash).unwrap();
            }
        }
        assert_eq!(digest.digest(), fixture["digest"].as_str().unwrap());
        assert_eq!(digest.legacy_digest(), fixture["legacy"].as_str().unwrap());
        assert_eq!(digest.count(), fixture["count"].as_u64().unwrap());
        let encoded = digest
            .encode()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        assert_eq!(encoded, fixture["receipt"].as_str().unwrap());
    }
}
