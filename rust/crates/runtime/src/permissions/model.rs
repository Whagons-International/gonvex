use sha2::{Digest, Sha256};

/// Trusted routing identity, never supplied by a client. Epoch alone does not
/// identify a database: independently created/copied files may share an epoch.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct PermissionScope {
    pub project: String,
    pub database_instance: String,
    pub tenant: String,
    pub epoch: String,
    pub table: String,
    pub collection: String,
    pub policy_version: String,
    pub projection_version: String,
    pub semantics_version: String,
}

impl PermissionScope {
    pub fn validate(&self) -> Result<(), InvalidPrimitive> {
        if [
            &self.project,
            &self.database_instance,
            &self.tenant,
            &self.epoch,
            &self.table,
            &self.collection,
            &self.policy_version,
            &self.projection_version,
            &self.semantics_version,
        ]
        .into_iter()
        .any(String::is_empty)
        {
            return Err(InvalidPrimitive::EmptyIdentity);
        }
        Ok(())
    }
    pub fn canonical(&self) -> Result<Vec<u8>, InvalidPrimitive> {
        self.validate()?;
        let mut bytes = b"gonvex-permission-scope-v1".to_vec();
        for value in [
            &self.project,
            &self.database_instance,
            &self.tenant,
            &self.epoch,
            &self.table,
            &self.collection,
            &self.policy_version,
            &self.projection_version,
            &self.semantics_version,
        ] {
            field(&mut bytes, value.as_bytes());
        }
        Ok(bytes)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Atom {
    pub namespace: String,
    /// TEXT scalar resource: blank is an exact value, distinct from NULL.
    /// Member/row identities are validated separately at their bindings.
    pub resource: String,
    /// User-specific atoms remain distinct even when the shared roles agree.
    pub principal: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum InvalidPrimitive {
    EmptyIdentity,
    EmptyConjunction,
    WrongUniverse,
    Unsupported(String),
    IndexNotReady,
}

fn field(out: &mut Vec<u8>, value: &[u8]) {
    out.extend_from_slice(&(value.len() as u64).to_be_bytes());
    out.extend_from_slice(value);
}

impl Atom {
    pub fn canonical(&self) -> Result<Vec<u8>, InvalidPrimitive> {
        if self.namespace.is_empty() || self.principal.as_ref().is_some_and(String::is_empty) {
            return Err(InvalidPrimitive::EmptyIdentity);
        }
        let mut out = Vec::new();
        field(&mut out, self.namespace.as_bytes());
        field(&mut out, self.resource.as_bytes());
        out.push(u8::from(self.principal.is_some()));
        if let Some(principal) = &self.principal {
            field(&mut out, principal.as_bytes());
        }
        Ok(out)
    }
}

/// Digest accelerates lookup; canonical bytes remain the equality authority.
/// It is not a role hash, nor a hash of currently visible row IDs. Revision
/// belongs in the snapshot/delivery key, not this stable permission-set key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExactPermissionSet {
    pub canonical: Vec<u8>,
    pub digest: [u8; 32],
}

impl ExactPermissionSet {
    pub fn new(scope: &PermissionScope, atoms: &[Atom]) -> Result<Self, InvalidPrimitive> {
        Self::from_keys(
            scope,
            atoms
                .iter()
                .map(Atom::canonical)
                .collect::<Result<Vec<_>, _>>()?,
        )
    }

    pub fn from_keys(
        scope: &PermissionScope,
        mut keys: Vec<Vec<u8>>,
    ) -> Result<Self, InvalidPrimitive> {
        if keys.iter().any(Vec::is_empty) {
            return Err(InvalidPrimitive::EmptyIdentity);
        }
        let mut canonical = b"gonvex-permission-set-v1".to_vec();
        field(&mut canonical, &scope.canonical()?);
        keys.sort();
        keys.dedup();
        canonical.extend_from_slice(&(keys.len() as u64).to_be_bytes());
        for key in keys {
            field(&mut canonical, &key);
        }
        let digest = Sha256::digest(&canonical).into();
        Ok(Self { canonical, digest })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IndexPhase {
    Backfilling,
    Ready,
    Failed,
}

/// Read from the index metadata inside the SAME SQLite snapshot as admission
/// and row selection. This value alone is not an executable read capability.
#[derive(Clone, Debug)]
pub(crate) struct IndexWatermark {
    pub scope: PermissionScope,
    pub generation: u64,
    pub phase: IndexPhase,
    pub covered_revision: u64,
}

impl IndexWatermark {
    pub fn check(
        &self,
        scope: &PermissionScope,
        generation: u64,
        snapshot_revision: u64,
    ) -> Result<(), InvalidPrimitive> {
        scope.validate()?;
        if self.scope != *scope
            || self.phase != IndexPhase::Ready
            || generation == 0
            || self.generation != generation
            || self.covered_revision != snapshot_revision
        {
            return Err(InvalidPrimitive::IndexNotReady);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> PermissionScope {
        PermissionScope {
            project: "project".into(),
            database_instance: "instance".into(),
            tenant: "tenant".into(),
            epoch: "epoch".into(),
            table: "tasks".into(),
            collection: "tasks.list".into(),
            policy_version: "policy-1".into(),
            projection_version: "projection-1".into(),
            semantics_version: "normalized-atoms-binary-v1".into(),
        }
    }
    fn atom(principal: &str) -> Atom {
        Atom {
            namespace: "owner".into(),
            resource: "task".into(),
            principal: Some(principal.into()),
        }
    }

    #[test]
    fn exact_sets_are_order_independent_and_preserve_user_and_scope_identity() {
        let a = atom("alice");
        let b = atom("bob");
        let scope = scope();
        assert_eq!(
            ExactPermissionSet::new(&scope, &[a.clone(), b.clone(), a.clone()]),
            ExactPermissionSet::new(&scope, &[b.clone(), a.clone()])
        );
        assert_ne!(
            ExactPermissionSet::new(&scope, &[a.clone()]),
            ExactPermissionSet::new(&scope, &[b])
        );
        let mut other = scope.clone();
        other.database_instance = "other-instance".into();
        assert_ne!(
            ExactPermissionSet::new(&scope, &[a.clone()]),
            ExactPermissionSet::new(&other, &[a])
        );
    }

    #[test]
    fn incomplete_stale_future_or_wrong_scope_index_is_rejected() {
        let scope = scope();
        let mut watermark = IndexWatermark {
            scope: scope.clone(),
            generation: 1,
            phase: IndexPhase::Ready,
            covered_revision: 9,
        };
        assert!(watermark.check(&scope, 1, 9).is_ok());
        for revision in [8, 10] {
            assert_eq!(
                watermark.check(&scope, 1, revision),
                Err(InvalidPrimitive::IndexNotReady)
            );
        }
        assert_eq!(
            watermark.check(&scope, 2, 9),
            Err(InvalidPrimitive::IndexNotReady)
        );
        watermark.phase = IndexPhase::Backfilling;
        assert_eq!(
            watermark.check(&scope, 1, 9),
            Err(InvalidPrimitive::IndexNotReady)
        );
        watermark.phase = IndexPhase::Ready;
        watermark.scope.project = "other-project".into();
        assert_eq!(
            watermark.check(&scope, 1, 9),
            Err(InvalidPrimitive::IndexNotReady)
        );
    }
}

#[cfg(test)]
mod scalar_tests {
    use super::*;
    #[test]
    fn blank_resource_is_exact_and_not_a_missing_identity() {
        let blank = Atom {
            namespace: "owner".into(),
            resource: String::new(),
            principal: None,
        };
        let space = Atom {
            resource: " ".into(),
            ..blank.clone()
        };
        assert_ne!(blank.canonical().unwrap(), space.canonical().unwrap());
        assert!(Atom {
            namespace: String::new(),
            ..blank
        }
        .canonical()
        .is_err());
    }
}
