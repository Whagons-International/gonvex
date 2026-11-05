//! Event-local encode-once bookkeeping, NOT a live delivery adapter.
//!
//! There is deliberately no global cache or network enqueue API. The eventual
//! feed adapter must hold a commit/admission barrier across validation+enqueue,
//! invalidate at revoke/expiry commit, and create a fresh batch per event. This
//! helper alone does not establish that ordering and MUST NOT enable a route.
use std::{collections::HashMap, sync::Arc};

use super::query::ReadIdentity;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum Representation {
    LegacyJson,
    NativeRows,
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) enum Isolation {
    /// Only a normalized policy with no unresolved member-specific projection
    /// may use this. Equal role names are never a proof of this condition.
    ExactNormalizedSet,
    MemberProjection(String),
}
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct EncodingKey {
    /// Full canonical bytes retain scope and user-specific atom identity.
    exact: Vec<u8>,
    generation: i64,
    change_serial: i64,
    source_revision: i64,
    /// Trusted canonical collection definition, args/filter/order/budgets and
    /// projected fields. Never replace this with a role, table name or hash.
    collection_contract: Vec<u8>,
    representation: Representation,
    isolation: Isolation,
}
impl EncodingKey {
    pub fn new(
        identity: &ReadIdentity,
        collection_contract: Vec<u8>,
        representation: Representation,
        isolation: Isolation,
    ) -> Result<Self, &'static str> {
        if collection_contract.is_empty() || identity.generation <= 0 {
            return Err("complete admitted collection contract is required");
        }
        if let Isolation::MemberProjection(member) = &isolation {
            if member != &identity.principal {
                return Err("member projection identity mismatch");
            }
        }
        Ok(Self {
            exact: identity.exact.canonical.clone(),
            generation: identity.generation,
            change_serial: identity.change_serial,
            source_revision: identity.source_revision,
            collection_contract,
            representation,
            isolation,
        })
    }
}

#[derive(Default)]
pub(super) struct EventEncodingBatch {
    payloads: HashMap<EncodingKey, Arc<[u8]>>,
}
impl EventEncodingBatch {
    /// Payload only. Subscription IDs, cursors, ACKs, windows and backpressure
    /// remain outside this batch and cannot enter the shared encoded bytes.
    pub fn encode_once(
        &mut self,
        key: EncodingKey,
        encode: impl FnOnce() -> Arc<[u8]>,
    ) -> Arc<[u8]> {
        Arc::clone(self.payloads.entry(key).or_insert_with(encode))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::permissions::{
        model::{ExactPermissionSet, PermissionScope},
        query::ReadIdentity,
    };
    #[test]
    fn payload_reuse_requires_exact_collection_member_and_index_identity() {
        let scope = PermissionScope {
            project: "p".into(),
            database_instance: "db".into(),
            tenant: "t".into(),
            epoch: "e".into(),
            table: "tasks".into(),
            collection: "tasks.list".into(),
            policy_version: "v1".into(),
            projection_version: "v1".into(),
            semantics_version: "normalized-atoms-binary-v1".into(),
        };
        let mut identity = ReadIdentity {
            exact: ExactPermissionSet::new(&scope, &[]).unwrap(),
            generation: 1,
            change_serial: 1,
            source_revision: 4,
            principal: "alice".into(),
        };
        let mut batch = EventEncodingBatch::default();
        let key = EncodingKey::new(
            &identity,
            b"args=A;projection=name;order=id;budget=10".to_vec(),
            Representation::LegacyJson,
            Isolation::ExactNormalizedSet,
        )
        .unwrap();
        let one = batch.encode_once(key.clone(), || Arc::from(&b"payload"[..]));
        let same = batch.encode_once(key, || panic!("identical payload should encode once"));
        assert!(Arc::ptr_eq(&one, &same));
        let other = EncodingKey::new(
            &identity,
            b"args=B;projection=name;order=id;budget=10".to_vec(),
            Representation::LegacyJson,
            Isolation::ExactNormalizedSet,
        )
        .unwrap();
        assert!(!Arc::ptr_eq(
            &one,
            &batch.encode_once(other, || Arc::from(&b"other"[..]))
        ));
        assert!(EncodingKey::new(
            &identity,
            b"member fields".to_vec(),
            Representation::LegacyJson,
            Isolation::MemberProjection("bob".into())
        )
        .is_err());
        identity.change_serial += 1;
        let revoked = EncodingKey::new(
            &identity,
            b"args=A;projection=name;order=id;budget=10".to_vec(),
            Representation::LegacyJson,
            Isolation::ExactNormalizedSet,
        )
        .unwrap();
        assert!(!Arc::ptr_eq(
            &one,
            &batch.encode_once(revoked, || Arc::from(&b"new generation"[..]))
        ));
    }
}
