//! Scheduled work versus user calls on one isolate (docs/performance/cron-writer.md):
//! the scheduled time slice, the priority queue in front of the isolate, and
//! the optional scheduled isolate.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gonvex_module_runtime::{
    BoxFuture, Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse,
    Invocation, InvocationChannel, InvocationContext, InvocationInfo, ModuleArtifact, ModuleEngine,
    ModuleError, ModuleHost, ModuleLanguage, ModuleManifest,
};
use serde_json::{json, Map};

use crate::host_call_tests::run_v8_test;
use crate::{V8Config, V8ModuleEngine};

/// `spin(n)` runs `n` loop iterations, forever when `n < 0` (Reducers see a
/// frozen `Date.now()`, so time cannot bound the loop); `gate()` awaits one
/// host call.
const MODULE: &str = r#"
export function spin(ctx, n) { if (n < 0) { while (true) {} } let x = 0; for (let i = 0; i < n; i++) x += i; return 'spun'; }
export async function gate(ctx) { await ctx.db.query('SELECT 1', []); return 'opened'; }
"#;

fn build(config: V8Config) -> V8ModuleEngine {
    let contract = |path: &str| FunctionContract {
        path: path.into(),
        kind: FunctionKind::Reducer,
        internal: false,
        delivery: None,
        args_schema: Some(json!({"kind":"any"})),
        result_schema: Some(json!({"kind":"any"})),
        metadata: Map::from_iter([("export".into(), json!(path))]),
    };
    V8ModuleEngine::from_artifact(
        ModuleArtifact {
            manifest: ModuleManifest {
                module_id: "scheduling".into(),
                generation: 1,
                language: ModuleLanguage::TypeScript,
                artifact_hash: "test".into(),
                functions: vec![contract("spin"), contract("gate")],
                metadata: Map::new(),
            },
            payload: MODULE.as_bytes().to_vec(),
        },
        config,
    )
    .unwrap()
}

fn invocation(function: &str, args: &str, channel: InvocationChannel, command: &str) -> Invocation {
    Invocation {
        function: function.into(),
        kind: FunctionKind::Reducer,
        args: args.as_bytes().to_vec(),
        context: InvocationContext {
            generation: 1,
            capabilities: Capabilities {
                db_read: true,
                ..Default::default()
            },
            invocation: InvocationInfo {
                channel,
                root_channel: channel,
                command_id: command.into(),
                root_command_id: command.into(),
                ..Default::default()
            },
            ..Default::default()
        },
    }
}

/// Records which invocation reached its first host call, in order; the call
/// named `held` waits until `open` is released.
struct GateHost {
    order: Mutex<Vec<String>>,
    open: tokio::sync::Semaphore,
    held: Mutex<Option<String>>,
}

impl Default for GateHost {
    fn default() -> Self {
        Self {
            order: Mutex::default(),
            open: tokio::sync::Semaphore::new(0),
            held: Mutex::default(),
        }
    }
}

impl ModuleHost for GateHost {
    fn call<'a>(
        &'a self,
        context: &'a InvocationContext,
        _: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async move {
            let command = context.invocation.command_id.clone();
            self.order.lock().unwrap().push(command.clone());
            if self.held.lock().unwrap().as_deref() == Some(command.as_str()) {
                self.open.acquire().await.unwrap().forget();
            }
            Ok(HostResponse {
                value: b"[]".to_vec(),
            })
        })
    }
}

#[test]
fn scheduled_reducer_outrunning_its_slice_gets_an_error_that_says_to_chunk() {
    run_v8_test(async {
        let engine = build(V8Config {
            scheduled_execution_timeout: Duration::from_millis(100),
            ..Default::default()
        });
        let host = GateHost::default();
        let error = engine
            .invoke(&host, invocation("spin", "-1", InvocationChannel::Scheduler, "cron"))
            .await
            .expect_err("a scheduled Reducer is cut at its slice");
        let ModuleError::BudgetExceeded(message) = error else {
            panic!("wrong error class: {error:?}");
        };
        assert!(message.contains("scheduled reducer spin exceeded its 100 ms time slice"), "{message}");
        assert!(message.contains("ctx.scheduler.runAfter"), "{message}");
        // The same work from a user, or within the slice, runs.
        let user = engine
            .invoke(&host, invocation("spin", "100000", InvocationChannel::Ui, "user"))
            .await
            .unwrap();
        assert_eq!(user.value, br#""spun""#);
        let short = engine
            .invoke(&host, invocation("spin", "10", InvocationChannel::Scheduler, "short"))
            .await
            .unwrap();
        assert_eq!(short.value, br#""spun""#);
        // A zero slice turns the guard off: only the ordinary ceiling applies.
        let error = build(V8Config {
            scheduled_execution_timeout: Duration::ZERO,
            execution_timeout: Duration::from_millis(200),
            ..Default::default()
        })
        .invoke(&host, invocation("spin", "-1", InvocationChannel::Scheduler, "cron"))
        .await
        .expect_err("the execution ceiling still applies");
        assert!(
            matches!(&error, ModuleError::BudgetExceeded(message) if message.contains("200 ms execution deadline")),
            "{error:?}"
        );
    });
}

#[test]
fn queued_user_calls_take_the_isolate_before_queued_scheduled_work() {
    run_v8_test(async {
        for priority in [true, false] {
            let engine = Arc::new(build(V8Config {
                isolate_priority: priority,
                ..Default::default()
            }));
            let host = Arc::new(GateHost::default());
            *host.held.lock().unwrap() = Some("holder".into());
            // A user call holds the only isolate inside a host call.
            let holder = tokio::spawn({
                let (engine, host) = (Arc::clone(&engine), Arc::clone(&host));
                async move {
                    engine
                        .invoke(&*host, invocation("gate", "null", InvocationChannel::Ui, "holder"))
                        .await
                }
            });
            while host.order.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // Then a cron, an outbox call and a user call queue, in that order.
            let mut queued = Vec::new();
            for (channel, command) in [
                (InvocationChannel::Scheduler, "cron"),
                (InvocationChannel::System, "outbox"),
                (InvocationChannel::Ui, "user"),
            ] {
                let (engine, host) = (Arc::clone(&engine), Arc::clone(&host));
                queued.push(tokio::spawn(async move {
                    engine
                        .invoke(&*host, invocation("gate", "null", channel, command))
                        .await
                }));
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            host.open.add_permits(1);
            holder.await.unwrap().unwrap();
            for call in queued {
                call.await.unwrap().unwrap();
            }
            let order = host.order.lock().unwrap().clone();
            let expected = if priority {
                ["holder", "user", "outbox", "cron"]
            } else {
                ["holder", "cron", "outbox", "user"]
            };
            assert_eq!(order, expected, "priority {priority}");
        }
    });
}

#[test]
fn scheduled_isolate_keeps_user_calls_off_a_running_cron() {
    run_v8_test(async {
        for separate in [true, false] {
            let engine = Arc::new(build(V8Config {
                scheduled_isolate: separate,
                scheduled_execution_timeout: Duration::ZERO,
                ..Default::default()
            }));
            let host = Arc::new(GateHost::default());
            *host.held.lock().unwrap() = Some("cron".into());
            // A cron is inside a long host call (a big write) on its isolate.
            let cron = tokio::spawn({
                let (engine, host) = (Arc::clone(&engine), Arc::clone(&host));
                async move {
                    engine
                        .invoke(&*host, invocation("gate", "null", InvocationChannel::Scheduler, "cron"))
                        .await
                }
            });
            while host.order.lock().unwrap().is_empty() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            let user = tokio::time::timeout(
                Duration::from_millis(if separate { 5_000 } else { 300 }),
                engine.invoke(&*host, invocation("spin", "1", InvocationChannel::Ui, "user")),
            )
            .await;
            if separate {
                assert_eq!(user.expect("user call runs while the cron holds its own isolate").unwrap().value, br#""spun""#);
            } else {
                assert!(user.is_err(), "with one shared isolate the user call waits for the cron");
            }
            host.open.add_permits(1);
            cron.await.unwrap().unwrap();
            let (_, on_demand) = engine.idle_isolates();
            assert_eq!(on_demand, usize::from(separate), "the scheduled isolate is pooled until idle");
        }
    });
}
