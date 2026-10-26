//! Isolate pooling and recycling.
//!
//! `isolate_pool_size` bounds live isolates and therefore concurrent module
//! calls: a permit is taken before an isolate is leased, so load sheds by
//! queueing rather than by creating unbounded V8 heaps.

use std::sync::{Arc, Mutex};

use gonvex_module_runtime::ModuleError;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use crate::isolate::{IsolateWorker, ModuleSource, WorkerCall};
use crate::V8Config;

struct PooledIsolate {
    worker: IsolateWorker,
    calls: usize,
}

pub(crate) struct IsolatePool {
    source: Arc<ModuleSource>,
    config: V8Config,
    idle: Mutex<Vec<PooledIsolate>>,
    permits: Arc<Semaphore>,
}

impl IsolatePool {
    pub(crate) fn new(source: Arc<ModuleSource>, config: V8Config) -> Self {
        let size = config.isolate_pool_size.max(1);
        Self {
            source,
            config,
            idle: Mutex::new(Vec::new()),
            permits: Arc::new(Semaphore::new(size)),
        }
    }

    pub(crate) async fn acquire(&self) -> Result<IsolateLease<'_>, ModuleError> {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| ModuleError::Unsupported("module isolate pool is closed".to_owned()))?;
        // The guard is dropped before the await below: an isolate is started
        // outside the lock so a slow bundle load never blocks other calls.
        let pooled = self.idle.lock().expect("isolate pool lock").pop();
        match pooled {
            Some(pooled) => Ok(IsolateLease {
                pool: self,
                pooled: Some(pooled),
                permit: Some(permit),
            }),
            None => {
                let (worker, ready) =
                    IsolateWorker::spawn(Arc::clone(&self.source), self.config.clone())?;
                // This guard owns both worker and permit across the await. A
                // canceled or failed load retires through the same path as a
                // canceled call, holding capacity until the native heap is gone.
                let lease = IsolateLease {
                    pool: self,
                    pooled: Some(PooledIsolate { worker, calls: 0 }),
                    permit: Some(permit),
                };
                match ready.await {
                    Ok(Ok(())) => Ok(lease),
                    Ok(Err(error)) => Err(error),
                    Err(_) => Err(ModuleError::Execution(
                        "module isolate stopped before it finished loading".to_owned(),
                    )),
                }
            }
        }
    }

    /// Dropping a `PooledIsolate` closes its call channel, which ends the
    /// worker loop and tears the isolate down. `recycle_after_calls` of 0 means
    /// an isolate is never reused.
    fn release(&self, pooled: PooledIsolate, healthy: bool, permit: OwnedSemaphorePermit) {
        if !healthy || pooled.calls >= self.config.recycle_after_calls {
            pooled.worker.retire(permit);
            return;
        }
        let mut idle = self.idle.lock().expect("isolate pool lock");
        if idle.len() < self.config.isolate_pool_size.max(1) {
            idle.push(pooled);
        } else {
            drop(idle);
            pooled.worker.retire(permit);
        }
    }
}

pub(crate) struct IsolateLease<'a> {
    pool: &'a IsolatePool,
    pooled: Option<PooledIsolate>,
    permit: Option<OwnedSemaphorePermit>,
}

impl IsolateLease<'_> {
    pub(crate) fn dispatch(&self, call: WorkerCall) -> Result<(), ModuleError> {
        self.pooled
            .as_ref()
            .expect("leased isolate is present until the lease ends")
            .worker
            .send(call)
    }

    /// Returns the isolate to the pool after it served one call.
    pub(crate) fn finish(mut self, healthy: bool) {
        if let Some(mut pooled) = self.pooled.take() {
            pooled.calls += 1;
            self.pool.release(
                pooled,
                healthy,
                self.permit.take().expect("leased permit is present"),
            );
        }
    }

    /// Returns an isolate that ran nothing, so prewarming does not spend a call
    /// from the isolate's recycle budget.
    pub(crate) fn release_unused(mut self) {
        if let Some(pooled) = self.pooled.take() {
            self.pool.release(
                pooled,
                true,
                self.permit.take().expect("leased permit is present"),
            );
        }
    }
}

impl Drop for IsolateLease<'_> {
    fn drop(&mut self) {
        // A lease that ends without `finish` means the call was cancelled or the
        // isolate died mid-call. Retire it: it may still be running JavaScript
        // for the abandoned invocation.
        if let Some(pooled) = self.pooled.take() {
            self.pool.release(
                pooled,
                false,
                self.permit.take().expect("leased permit is present"),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::isolate::CallSpec;
    use gonvex_module_runtime::{
        Capabilities, FunctionContract, FunctionKind, HostCall, HostResponse, Invocation,
        InvocationContext,
    };
    use serde_json::{json, Map};
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    #[test]
    fn canceled_unbounded_call_disposes_cpu_and_pending_host_work() {
        crate::host_call_tests::run_v8_test(async {
            for cpu in [false, true] {
                let source = Arc::new(ModuleSource::new("unbounded-cancellation",
                    "export async function run(ctx) { await ctx.tools.entered({}); while (true) {} }".into()).unwrap());
                let probe = Arc::clone(&source.probe);
                let pool = IsolatePool::new(source, V8Config::default());
                let lease = pool.acquire().await.unwrap();
                let (entered, release) = probe.hold_teardown();
                let (host, mut requests) = tokio::sync::mpsc::unbounded_channel();
                let (reply, _receive) = tokio::sync::oneshot::channel();
                let capabilities = Capabilities {
                    action_tools: true,
                    ..Default::default()
                };
                lease
                    .dispatch(WorkerCall {
                        reply,
                        spec: CallSpec {
                            contract: FunctionContract {
                                path: "run".into(),
                                kind: FunctionKind::Action,
                                internal: false,
                                delivery: None,
                                args_schema: Some(json!({"kind":"any"})),
                                result_schema: Some(json!({"kind":"any"})),
                                metadata: Map::from_iter([("export".into(), json!("run"))]),
                            },
                            invocation: Invocation {
                                function: "run".into(),
                                kind: FunctionKind::Action,
                                args: b"null".to_vec(),
                                context: InvocationContext {
                                    generation: 1,
                                    capabilities: capabilities.clone(),
                                    action_tools: vec!["entered".into()],
                                    ..Default::default()
                                },
                            },
                            capabilities,
                            now_unix_ms: 0,
                            timeout: None,
                            max_result_bytes: 1024,
                            host,
                        },
                    })
                    .unwrap();
                let request = tokio::time::timeout(Duration::from_secs(1), requests.recv())
                    .await
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(request.call, HostCall::ToolInvoke { ref tool, .. } if tool == "entered")
                );
                if cpu {
                    request
                        .reply
                        .send(Ok(HostResponse {
                            value: b"null".to_vec(),
                        }))
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                drop(lease);
                assert_eq!(pool.permits.available_permits(), 0);
                tokio::time::timeout(Duration::from_secs(1), async move {
                    loop {
                        match entered.try_recv() {
                            Ok(()) => break,
                            Err(std::sync::mpsc::TryRecvError::Empty) => {
                                tokio::time::sleep(Duration::from_millis(1)).await
                            }
                            Err(error) => {
                                panic!("canceled invocation did not reach disposal: {error}")
                            }
                        }
                    }
                })
                .await
                .expect("cancellation must stop a call without an execution deadline");
                assert_eq!(pool.permits.available_permits(), 0);
                release.send(()).unwrap();
                let permit = tokio::time::timeout(
                    Duration::from_secs(1),
                    Arc::clone(&pool.permits).acquire_owned(),
                )
                .await
                .unwrap()
                .unwrap();
                assert_eq!(probe.stopped.load(Ordering::SeqCst), 1);
                drop(permit);
            }
        });
    }

    #[test]
    fn initialization_deadline_disposes_cpu_and_async_initializers() {
        crate::host_call_tests::run_v8_test(async {
            for code in [
                "while (true) {}",
                "await new Promise(resolve => setTimeout(resolve, 1000));",
            ] {
                let source =
                    Arc::new(ModuleSource::new("bounded-initialization", code.into()).unwrap());
                let probe = Arc::clone(&source.probe);
                let pool = IsolatePool::new(
                    source,
                    V8Config {
                        initialization_timeout: Duration::from_millis(30),
                        ..Default::default()
                    },
                );
                for expected in 1..=3 {
                    let result = tokio::time::timeout(Duration::from_millis(500), pool.acquire())
                        .await
                        .expect("initialization must be bounded even when no handler runs");
                    assert!(
                        matches!(result, Err(ModuleError::BudgetExceeded(_))),
                        "wrong initialization result class"
                    );
                    let permit = tokio::time::timeout(
                        Duration::from_secs(1),
                        Arc::clone(&pool.permits).acquire_owned(),
                    )
                    .await
                    .expect("timed-out initialization must dispose before returning capacity")
                    .unwrap();
                    assert_eq!(probe.started.load(Ordering::SeqCst), expected);
                    assert_eq!(probe.stopped.load(Ordering::SeqCst), expected);
                    drop(permit);
                }
            }
        });
    }

    #[test]
    fn initialization_budget_rejects_zero_and_unrepresentable_deadlines() {
        crate::host_call_tests::run_v8_test(async {
            for timeout in [Duration::ZERO, Duration::MAX] {
                let source = Arc::new(
                    ModuleSource::new("initialization-budget", "export const ready = true;".into())
                        .unwrap(),
                );
                let pool = IsolatePool::new(
                    source,
                    V8Config {
                        initialization_timeout: timeout,
                        ..Default::default()
                    },
                );
                let result = pool.acquire().await;
                if timeout.is_zero() {
                    assert!(matches!(result, Err(ModuleError::BudgetExceeded(_))));
                } else {
                    assert!(matches!(result, Err(ModuleError::Unsupported(_))));
                }
            }
        });
    }

    #[test]
    fn canceled_startup_holds_capacity_until_native_disposal() {
        crate::host_call_tests::run_v8_test(async {
            let source =
                Arc::new(ModuleSource::new("startup-capacity", "while (true) {}".into()).unwrap());
            let probe = Arc::clone(&source.probe);
            let (entered, release) = probe.hold_teardown();
            let pool = IsolatePool::new(source, V8Config::default());
            assert!(
                tokio::time::timeout(Duration::from_millis(50), pool.acquire())
                    .await
                    .is_err()
            );
            assert_eq!(
                pool.permits.available_permits(),
                0,
                "canceled startup released pool capacity before native disposal"
            );
            tokio::time::timeout(Duration::from_secs(1), async move {
                loop {
                    match entered.try_recv() {
                        Ok(()) => break,
                        Err(std::sync::mpsc::TryRecvError::Empty) => {
                            tokio::time::sleep(Duration::from_millis(1)).await
                        }
                        Err(error) => panic!("initializer did not reach disposal: {error}"),
                    }
                }
            })
            .await
            .expect("cancellation must stop synchronous initialization");
            assert!(
                tokio::time::timeout(Duration::from_millis(20), pool.acquire())
                    .await
                    .is_err()
            );
            assert_eq!(
                probe.started.load(Ordering::SeqCst),
                1,
                "a canceled initializer must keep its only capacity slot until teardown finishes"
            );
            release.send(()).unwrap();
            let permit = tokio::time::timeout(
                Duration::from_secs(1),
                Arc::clone(&pool.permits).acquire_owned(),
            )
            .await
            .expect("native disposal must return capacity")
            .unwrap();
            assert_eq!(probe.stopped.load(Ordering::SeqCst), 1);
            drop(permit);
        });
    }
}
