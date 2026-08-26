//! Rust host-side module generation lifecycle.
//!
//! This crate is intentionally independent of HTTP, Postgres, and an engine
//! implementation. The production module host embeds these primitives while
//! the Rust network/database host retains transaction ownership.

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
}

struct ModuleSlot {
    registry: Arc<GenerationRegistry>,
    /// Loaded but not yet activated generations, highest generation last.
    staged: Mutex<BTreeMap<u64, Arc<dyn ModuleEngine>>>,
    /// Monotonic generation counter, never reused within one host process.
    issued: Mutex<u64>,
}

impl ModuleSlot {
    fn new() -> Self {
        Self {
            registry: Arc::new(GenerationRegistry::new()),
            staged: Mutex::new(BTreeMap::new()),
            issued: Mutex::new(0),
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
        let mut issued = slot.issued.lock().expect("module generation lock");
        let floor = (*issued).max(slot.registry.active_generation().unwrap_or(0));
        let generation = match requested {
            Some(requested) if requested <= floor => {
                return Err(RegistryError::NonMonotonicGeneration(requested))
            }
            Some(requested) => requested,
            None => floor + 1,
        };
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
        let retired = slot.registry.activate(engine)?;
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
