//! Rust host-side module generation lifecycle.
//!
//! This crate is intentionally independent of HTTP, Postgres, and an engine
//! implementation. The production module host embeds these primitives while
//! the Rust network/database host retains transaction ownership.

mod generation;

use std::collections::BTreeMap;
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use gonvex_module_runtime::{
    BoxFuture, Invocation, InvocationResult, ModuleEngine, ModuleError, ModuleHost,
};
use thiserror::Error;

struct GenerationEntry {
    engine: Arc<dyn ModuleEngine>,
    in_flight: Mutex<usize>,
    drained: Condvar,
    retiring: Mutex<bool>,
}

impl GenerationEntry {
    fn new(engine: Arc<dyn ModuleEngine>) -> Self {
        Self {
            engine,
            in_flight: Mutex::new(0),
            drained: Condvar::new(),
            retiring: Mutex::new(false),
        }
    }

    fn acquire(self: &Arc<Self>) -> Option<GenerationLease> {
        let retiring = self.retiring.lock().expect("generation retirement lock");
        if *retiring {
            return None;
        }
        let mut in_flight = self.in_flight.lock().expect("generation call lock");
        *in_flight += 1;
        drop(in_flight);
        drop(retiring);
        Some(GenerationLease {
            entry: Arc::clone(self),
        })
    }

    fn retire(&self) {
        *self.retiring.lock().expect("generation retirement lock") = true;
        if *self.in_flight.lock().expect("generation call lock") == 0 {
            self.drained.notify_all();
        }
    }

    fn wait_for_drain(&self, timeout: Option<Duration>) -> bool {
        let mut in_flight = self.in_flight.lock().expect("generation call lock");
        if let Some(timeout) = timeout {
            let deadline = Instant::now() + timeout;
            while *in_flight != 0 {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return false;
                }
                let (next, result) = self
                    .drained
                    .wait_timeout(in_flight, remaining)
                    .expect("generation drain wait");
                in_flight = next;
                if result.timed_out() && *in_flight != 0 {
                    return false;
                }
            }
            true
        } else {
            while *in_flight != 0 {
                in_flight = self.drained.wait(in_flight).expect("generation drain wait");
            }
            true
        }
    }
}

pub struct GenerationLease {
    entry: Arc<GenerationEntry>,
}

impl GenerationLease {
    pub fn engine(&self) -> &Arc<dyn ModuleEngine> {
        &self.entry.engine
    }

    pub fn generation(&self) -> u64 {
        self.entry.engine.manifest().generation
    }
}

impl Drop for GenerationLease {
    fn drop(&mut self) {
        let mut in_flight = self.entry.in_flight.lock().expect("generation call lock");
        *in_flight = in_flight.saturating_sub(1);
        if *in_flight == 0 {
            self.entry.drained.notify_all();
        }
    }
}

pub struct RetiredGeneration {
    generation: u64,
    entry: Arc<GenerationEntry>,
}

impl RetiredGeneration {
    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn wait_for_drain(&self, timeout: Option<Duration>) -> bool {
        self.entry.wait_for_drain(timeout)
    }
}

#[derive(Debug, Error)]
pub enum RegistryError {
    #[error("module generation {0} is not newer than the active generation")]
    NonMonotonicGeneration(u64),
    #[error("module generation counter is exhausted")]
    GenerationExhausted,
    #[error("module generation {0} was never loaded")]
    UnknownGeneration(u64),
    #[error("no active module generation")]
    Empty,
    #[error("module invocation failed: {0}")]
    Invocation(#[from] ModuleError),
}

pub struct GenerationRegistry {
    active: RwLock<Option<Arc<GenerationEntry>>>,
    generations: Mutex<BTreeMap<u64, Arc<GenerationEntry>>>,
}

impl Default for GenerationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl GenerationRegistry {
    pub fn new() -> Self {
        Self {
            active: RwLock::new(None),
            generations: Mutex::new(BTreeMap::new()),
        }
    }

    /// Atomically makes `engine` the target for new calls. Existing leases keep
    /// the old engine alive until their calls complete.
    pub fn activate(
        &self,
        engine: Arc<dyn ModuleEngine>,
    ) -> Result<Option<RetiredGeneration>, RegistryError> {
        let generation = engine.manifest().generation;
        let mut active = self.active.write().expect("active generation lock");
        if let Some(current) = active.as_ref() {
            let current_generation = current.engine.manifest().generation;
            if generation <= current_generation {
                return Err(RegistryError::NonMonotonicGeneration(generation));
            }
            current.retire();
        }
        let next = Arc::new(GenerationEntry::new(engine));
        let previous = active.replace(Arc::clone(&next));
        self.generations
            .lock()
            .expect("generation registry lock")
            .insert(generation, next);
        Ok(previous.map(|entry| RetiredGeneration {
            generation: entry.engine.manifest().generation,
            entry,
        }))
    }

    pub fn acquire(&self) -> Result<GenerationLease, RegistryError> {
        let active = self.active.read().expect("active generation lock");
        let entry = active.as_ref().ok_or(RegistryError::Empty)?;
        entry.acquire().ok_or(RegistryError::Empty)
    }

    pub fn active_generation(&self) -> Option<u64> {
        self.active
            .read()
            .expect("active generation lock")
            .as_ref()
            .map(|entry| entry.engine.manifest().generation)
    }

    /// Drops retired engine references once their in-flight calls have drained.
    /// A timeout leaves the generation retained, making a forced shutdown an
    /// explicit host policy instead of silently dropping an invocation.
    ///
    /// Waiting happens with no registry lock held: draining can take as long as
    /// the slowest in-flight call, and blocking `activate` for that whole window
    /// would make publishing a generation wait on the one it replaces.
    pub fn reap(&self, timeout: Option<Duration>) -> Vec<u64> {
        // Read the active generation before taking the registry lock: `activate`
        // takes `active` and then `generations`, so acquiring them the other way
        // round here would be a lock-order inversion.
        let active_generation = self.active_generation();
        let retired: Vec<(u64, Arc<GenerationEntry>)> = {
            let generations = self.generations.lock().expect("generation registry lock");
            generations
                .iter()
                .filter(|(generation, _)| Some(**generation) != active_generation)
                .map(|(generation, entry)| (*generation, Arc::clone(entry)))
                .collect()
        };
        let mut drained = Vec::new();
        for (generation, entry) in retired {
            if entry.wait_for_drain(timeout) {
                drained.push(generation);
            }
        }
        if !drained.is_empty() {
            let mut generations = self.generations.lock().expect("generation registry lock");
            for generation in &drained {
                generations.remove(generation);
            }
        }
        drained
    }

    /// Retires the active generation and waits for every generation to drain.
    /// Returns false when a call was still running at the deadline, which the
    /// host reports rather than silently abandoning the invocation.
    pub fn drain(&self, timeout: Option<Duration>) -> bool {
        if let Some(entry) = self.active.write().expect("active generation lock").take() {
            entry.retire();
        }
        let entries: Vec<Arc<GenerationEntry>> = self
            .generations
            .lock()
            .expect("generation registry lock")
            .values()
            .map(Arc::clone)
            .collect();
        let mut drained = true;
        for entry in entries {
            drained &= entry.wait_for_drain(timeout);
        }
        if drained {
            self.generations
                .lock()
                .expect("generation registry lock")
                .clear();
        }
        drained
    }
}

pub struct ModuleHostRuntime {
    pub registry: Arc<GenerationRegistry>,
}

impl ModuleHostRuntime {
    pub fn new(registry: Arc<GenerationRegistry>) -> Self {
        Self { registry }
    }

    pub fn invoke<'a>(
        &'a self,
        host: &'a dyn ModuleHost,
        invocation: Invocation,
    ) -> BoxFuture<'a, Result<InvocationResult, RegistryError>> {
        Box::pin(async move {
            let lease = self.registry.acquire()?;
            let result = lease.engine().invoke(host, invocation).await;
            result.map_err(RegistryError::Invocation)
        })
    }
}

/// Every module the host serves, keyed by module id (one per Gonvex project).
///
/// One process serves every project: an engine is per module generation, not
/// per tenant, and tenancy travels on the invocation context instead. A module
/// is loaded into a staging slot first and becomes reachable only when it is
/// activated, so a generation that fails to load or to warm never serves a
/// call.
#[derive(Default)]
pub struct ModuleRegistry {
    modules: Mutex<BTreeMap<String, Arc<ModuleSlot>>>,
    // Issued numbers outlive an unloaded slot. A delayed load or activation
    // must never collide with a new incarnation of the same module.
    issued: Mutex<BTreeMap<String, u64>>,
}

struct ModuleSlot {
    registry: Arc<GenerationRegistry>,
    /// Loaded but not yet activated generations, highest generation last.
    staged: Mutex<BTreeMap<u64, Arc<dyn ModuleEngine>>>,
}

impl ModuleSlot {
    fn new() -> Self {
        Self {
            registry: Arc::new(GenerationRegistry::new()),
            staged: Mutex::new(BTreeMap::new()),
        }
    }
}

impl ModuleRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    fn slot(&self, module_id: &str) -> Arc<ModuleSlot> {
        let mut modules = self.modules.lock().expect("module registry lock");
        Arc::clone(
            modules
                .entry(module_id.to_owned())
                .or_insert_with(|| Arc::new(ModuleSlot::new())),
        )
    }

    /// Reserves the next generation for a module. The host allocates before it
    /// builds an engine, because the generation is part of the engine's own
    /// manifest, and a caller that supplies its own generation must still be
    /// strictly ahead of everything this process has issued or activated.
    pub fn reserve_generation(
        &self,
        module_id: &str,
        requested: Option<u64>,
    ) -> Result<u64, RegistryError> {
        let slot = self.slot(module_id);
        let mut counters = self.issued.lock().expect("module generation lock");
        let issued = counters.entry(module_id.to_owned()).or_default();
        let generation = generation::next_generation(
            *issued,
            slot.registry.active_generation().unwrap_or(0),
            requested,
        )
        .ok_or(match requested {
            Some(value) => RegistryError::NonMonotonicGeneration(value),
            None => RegistryError::GenerationExhausted,
        })?;
        *issued = generation;
        Ok(generation)
    }

    /// Holds a loaded engine until it is activated. Staging a generation twice
    /// replaces the earlier engine, which is what a retried load should do.
    pub fn stage(&self, module_id: &str, generation: u64, engine: Arc<dyn ModuleEngine>) {
        let slot = self.slot(module_id);
        slot.staged
            .lock()
            .expect("module staging lock")
            .insert(generation, engine);
    }

    /// Atomically makes a staged generation the target for new calls. Calls
    /// already running on the previous generation finish on it.
    pub fn activate(
        &self,
        module_id: &str,
        generation: u64,
    ) -> Result<Option<RetiredGeneration>, RegistryError> {
        let slot = self.slot(module_id);
        let engine = slot
            .staged
            .lock()
            .expect("module staging lock")
            .remove(&generation)
            .ok_or(RegistryError::UnknownGeneration(generation))?;
        // Use the same issued -> active lock order as reserve_generation.
        // Recording a manually staged generation must be atomic with activation
        // so an unload/reload cannot allocate below it in between these steps.
        let mut counters = self.issued.lock().expect("module generation lock");
        let actual_generation = engine.manifest().generation;
        let retired = slot.registry.activate(engine)?;
        let issued = counters.entry(module_id.to_owned()).or_default();
        *issued = (*issued).max(actual_generation);
        drop(counters);
        // Anything still staged below the new generation can never be activated
        // now that activation is strictly monotonic.
        slot.staged
            .lock()
            .expect("module staging lock")
            .retain(|staged, _| *staged > generation);
        Ok(retired)
    }

    pub fn acquire(&self, module_id: &str) -> Result<GenerationLease, RegistryError> {
        self.slot(module_id).registry.acquire()
    }

    pub fn active_generation(&self, module_id: &str) -> Option<u64> {
        self.modules
            .lock()
            .expect("module registry lock")
            .get(module_id)
            .and_then(|slot| slot.registry.active_generation())
    }

    /// Drops retired generations of one module whose calls have finished.
    pub fn reap(&self, module_id: &str, timeout: Option<Duration>) -> Vec<u64> {
        self.slot(module_id).registry.reap(timeout)
    }

    /// Retires a module entirely, waiting for its calls within `timeout`.
    pub fn unload(&self, module_id: &str, timeout: Option<Duration>) -> bool {
        let slot = self
            .modules
            .lock()
            .expect("module registry lock")
            .remove(module_id);
        match slot {
            Some(slot) => {
                slot.staged.lock().expect("module staging lock").clear();
                slot.registry.drain(timeout)
            }
            None => true,
        }
    }

    pub fn module_ids(&self) -> Vec<String> {
        self.modules
            .lock()
            .expect("module registry lock")
            .keys()
            .cloned()
            .collect()
    }

    /// Bounded shutdown: every module is retired and every in-flight call is
    /// given `timeout` to finish. False means at least one call was still
    /// running, which the caller reports instead of pretending it drained.
    pub fn shutdown(&self, timeout: Option<Duration>) -> bool {
        let slots: Vec<Arc<ModuleSlot>> = {
            let mut modules = self.modules.lock().expect("module registry lock");
            let slots = modules.values().map(Arc::clone).collect();
            modules.clear();
            slots
        };
        let mut drained = true;
        for slot in slots {
            slot.staged.lock().expect("module staging lock").clear();
            drained &= slot.registry.drain(timeout);
        }
        drained
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    use gonvex_module_runtime::{ModuleLanguage, ModuleManifest};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Engine {
        manifest: ModuleManifest,
        drops: Arc<AtomicUsize>,
    }

    impl Drop for Engine {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl ModuleEngine for Engine {
        fn manifest(&self) -> &ModuleManifest {
            &self.manifest
        }

        fn invoke<'a>(
            &'a self,
            _host: &'a dyn ModuleHost,
            _invocation: Invocation,
        ) -> BoxFuture<'a, Result<InvocationResult, ModuleError>> {
            Box::pin(async { Err(ModuleError::Unsupported("test engine".into())) })
        }
    }

    fn engine(generation: u64, drops: &Arc<AtomicUsize>) -> Arc<dyn ModuleEngine> {
        Arc::new(Engine {
            manifest: ModuleManifest {
                module_id: "m".into(),
                generation,
                language: ModuleLanguage::TypeScript,
                artifact_hash: "test".into(),
                functions: Vec::new(),
                metadata: Default::default(),
            },
            drops: Arc::clone(drops),
        })
    }

    #[test]
    fn automatic_reservation_rejects_exhaustion_without_poisoning() {
        let registry = ModuleRegistry::new();
        assert_eq!(
            registry.reserve_generation("m", Some(u64::MAX)).unwrap(),
            u64::MAX
        );
        assert!(matches!(
            registry.reserve_generation("m", None),
            Err(RegistryError::GenerationExhausted)
        ));
        assert!(matches!(
            registry.reserve_generation("m", Some(u64::MAX)),
            Err(RegistryError::NonMonotonicGeneration(u64::MAX))
        ));
    }

    #[test]
    fn reservations_survive_unload_and_shutdown() {
        let registry = ModuleRegistry::new();
        assert_eq!(registry.reserve_generation("m", None).unwrap(), 1);
        assert!(registry.unload("m", None));
        assert_eq!(registry.reserve_generation("m", None).unwrap(), 2);
        assert!(registry.shutdown(None));
        assert_eq!(registry.reserve_generation("m", None).unwrap(), 3);
        assert_eq!(registry.reserve_generation("other", None).unwrap(), 1);
    }

    #[test]
    fn failed_reservation_preserves_the_counter() {
        let registry = ModuleRegistry::new();
        assert_eq!(registry.reserve_generation("m", Some(10)).unwrap(), 10);
        assert!(registry.reserve_generation("m", Some(9)).is_err());
        assert_eq!(registry.reserve_generation("m", None).unwrap(), 11);
    }

    #[test]
    fn manually_staged_generation_survives_unload_in_the_issued_ledger() {
        let registry = ModuleRegistry::new();
        let drops = Arc::new(AtomicUsize::new(0));
        // Stage keys and engine manifests are distinct inputs to the library.
        registry.stage("m", 2, engine(42, &drops));
        registry.activate("m", 2).unwrap();
        assert!(registry.unload("m", None));
        assert_eq!(registry.reserve_generation("m", None).unwrap(), 43);
    }

    #[test]
    fn concurrent_reservations_are_unique() {
        let registry = Arc::new(ModuleRegistry::new());
        let workers: Vec<_> = (0..4)
            .map(|_| {
                let registry = Arc::clone(&registry);
                std::thread::spawn(move || {
                    (0..128)
                        .map(|_| registry.reserve_generation("m", None).unwrap())
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let mut generations: Vec<_> = workers
            .into_iter()
            .flat_map(|worker| worker.join().unwrap())
            .collect();
        generations.sort_unstable();
        assert_eq!(generations, (1..=512).collect::<Vec<_>>());
    }

    #[test]
    fn activation_preserves_old_leases_until_release() {
        let drops = Arc::new(AtomicUsize::new(0));
        let registry = GenerationRegistry::new();
        assert!(registry.activate(engine(1, &drops)).unwrap().is_none());
        let old = registry.acquire().unwrap();
        let retired = registry.activate(engine(2, &drops)).unwrap().unwrap();
        assert_eq!(old.generation(), 1);
        assert_eq!(registry.acquire().unwrap().generation(), 2);
        assert!(!retired.wait_for_drain(Some(Duration::ZERO)));
        assert!(registry.reap(Some(Duration::ZERO)).is_empty());
        drop(old);
        assert!(retired.wait_for_drain(Some(Duration::ZERO)));
        assert_eq!(registry.reap(Some(Duration::ZERO)), vec![1]);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(retired);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn unload_reports_a_live_lease_and_releases_the_engine_afterwards() {
        let drops = Arc::new(AtomicUsize::new(0));
        let registry = ModuleRegistry::new();
        registry.stage("m", 1, engine(1, &drops));
        registry.activate("m", 1).unwrap();
        let lease = registry.acquire("m").unwrap();
        assert!(!registry.unload("m", Some(Duration::ZERO)));
        assert!(matches!(registry.acquire("m"), Err(RegistryError::Empty)));
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(lease);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
