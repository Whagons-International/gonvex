//! Process-only proof. No deserializer, public constructor, decoded row table or
//! SQL-membership memo. EVERY offer independently supplies fresh SQL equality.
use super::*;
use crate::starter_files::{pinned::Input, Authority};
use std::collections::VecDeque;

pub(super) struct ValidatedArtifact {
    input: Input,
    context: Registered,
    verified: format::Verified,
    // Last field: encoded bytes and context drop before the charge is released.
    _charge: Arc<Reservation>,
}
#[derive(Default)]
pub(super) struct Cache(VecDeque<Arc<ValidatedArtifact>>);
impl Cache {
    #[cfg(test)]
    pub(super) fn clear(&mut self) {
        self.0.clear();
    }
    #[cfg(test)]
    pub(super) fn len(&self) -> usize {
        self.0.len()
    }
    /// Called with try_lock under the writer-ordered fresh admission barrier.
    /// Contention falls back; no unbounded factory waiters or network awaits.
    pub(super) fn get_or_validate(
        &mut self,
        r: &Registered,
        fresh: &Authority,
        rows: u64,
        digest: &str,
    ) -> io::Result<Arc<ValidatedArtifact>> {
        self.0
            .retain(|a| a.input.current(&a.context.store, fresh).is_ok());
        if let Some(i) = self.0.iter().position(|a| {
            a.matches(r, fresh) && a.verified.rows == rows && a.verified.digest == digest
        }) {
            let a = self.0.remove(i).unwrap();
            self.0.push_back(a.clone());
            return Ok(a);
        }
        // Cache eviction cannot release bytes still held by active offers.
        while self.0.len() >= 8 {
            self.0.pop_front();
        }
        // Context retains existing immutable registrations. Charge their complete
        // source/schema/proof bytes as well, even when their Arc is shared.
        let reg = &r.registration;
        let context = reg.schema.len()
            + reg.artifact_hash.len()
            + reg.binding.authority.len()
            + reg
                .binding
                .scope
                .canonical()
                .map_err(|_| io::Error::other("artifact scope"))?
                .len()
            + reg
                .source_schemas
                .iter()
                .map(|(a, b)| a.len() + b.len() + 128)
                .sum::<usize>()
            + reg
                .source_guards
                .iter()
                .map(|(a, b, c)| a.len() + b.len() + c.len() + 192)
                .sum::<usize>()
            + r.proof.manifest().len()
            + r.proof.plan().len()
            + r.proof.definition().len()
            + r.column_types
                .iter()
                .map(|(a, b)| a.len() + b.len() + 128)
                .sum::<usize>()
            + (1 << 20);
        let lease = reserve(context)
            .or_else(|_| {
                self.0.clear();
                reserve(context)
            })
            .map_err(|_| io::Error::other("artifact context budget"))?;
        let input = r.store.pin_input(fresh, |n| {
            lease.resize(
                context
                    .checked_add(n)
                    .ok_or_else(|| io::Error::other("artifact charge overflow"))?,
            )
        })?;
        let timing = Scope::new(Site::OfferFormat);
        let mut v = format::Verifier::new(&reg.definition, input.manifest())?;
        for i in 0..input.manifest().pieces.len() {
            v.piece(input.manifest(), i, input.bytes(i)?, |row| {
                checked_hash(row, &r.column_types)
            })?;
        }
        let verified = v.finish(input.manifest())?;
        crate::starter_files::diagnostics::add(Site::FormatRows, verified.rows);
        drop(timing);
        if verified.rows != rows || verified.digest != digest {
            return Err(io::Error::other(
                "starter differs from fresh authorized SQL window",
            ));
        }
        input.current(&r.store, fresh)?;
        lease.resize(
            context
                .checked_add(input.retained_charge())
                .ok_or_else(|| io::Error::other("artifact charge overflow"))?,
        )?;
        let a = Arc::new(ValidatedArtifact {
            input,
            context: r.clone(),
            verified,
            _charge: lease,
        });
        self.0.push_back(a.clone());
        Ok(a)
    }
}
impl ValidatedArtifact {
    fn matches(&self, r: &Registered, fresh: &Authority) -> bool {
        let a = &self.context.registration;
        let b = &r.registration;
        // The immutable registration pointer additionally pins the expression
        // closure and prevents reuse across reinstalls. Exact bytes still bind
        // loaded provider/function, physical schema/source/set and all budgets.
        Arc::ptr_eq(a, b)
            && Arc::ptr_eq(&self.context.store, &r.store)
            && a.generation == b.generation
            && a.artifact_hash == b.artifact_hash
            && a.schema == b.schema
            && a.source_schemas == b.source_schemas
            && a.source_guards == b.source_guards
            && a.binding.scope == b.binding.scope
            && a.binding.universe == b.binding.universe
            && a.binding.authority == b.binding.authority
            && self.context.proof.manifest() == r.proof.manifest()
            && self.context.proof.plan() == r.proof.plan()
            && self.context.proof.definition() == r.proof.definition()
            && self.context.column_types == r.column_types
            && self.input.manifest().identity == fresh.identity
    }
    pub(super) fn manifest(&self) -> &Arc<Manifest> {
        self.input.manifest()
    }
    pub(super) fn verified(&self) -> &format::Verified {
        &self.verified
    }
    pub(super) fn charge(&self) -> usize {
        self._charge.bytes()
    }
    pub(super) fn bytes<'a>(
        &'a self,
        r: &Registered,
        fresh: &Authority,
        index: usize,
    ) -> io::Result<&'a [u8]> {
        if !self.matches(r, fresh) {
            return Err(io::Error::other("artifact authority changed"));
        }
        self.input.current(&r.store, fresh)?;
        self.input.bytes(index)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn starter_charge_releases_only_last_owner_and_failed_growth_is_atomic() {
        let _lock = crate::permissions::LOCAL_TEST_ENV.lock().unwrap();
        let before = OFFER_BYTES.load(Ordering::Acquire);
        let charge = reserve(4096).unwrap();
        charge.resize(8192).unwrap();
        assert!(charge.resize(GLOBAL_LIMIT + 1).is_err());
        assert_eq!(charge.bytes(), 8192);
        let active = charge.clone();
        drop(charge);
        assert_eq!(OFFER_BYTES.load(Ordering::Acquire), before + 8192);
        drop(active);
        assert_eq!(OFFER_BYTES.load(Ordering::Acquire), before);
    }
}
