use super::*;
use gonvex_module_runtime::{FunctionKind, HostCall, HostError, HostResponse, InvocationContext};
struct Host;
impl ModuleHost for Host {
    fn call<'a>(
        &'a self,
        _: &'a InvocationContext,
        _: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async { Err(HostError::Failed("unexpected host call".into())) })
    }
}
#[test]
fn getter_handler_lookup_spends_execution_budget() {
    crate::host_call_tests::run_v8_test(async {
        let engine = V8ModuleEngine::from_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "lookup-budget".into(),
                    generation: 1,
                    language: ModuleLanguage::TypeScript,
                    artifact_hash: "test".into(),
                    functions: vec![FunctionContract {
                        path: "handlers.run".into(),
                        kind: FunctionKind::Query,
                        internal: false,
                        delivery: None,
                        args_schema: Some(serde_json::json!({"kind":"any"})),
                        result_schema: Some(serde_json::json!({"kind":"any"})),
                        metadata: Default::default(),
                    }],
                    metadata: Default::default(),
                },
                payload: br#"export const handlers = { get run() { while (true) {} } };"#.to_vec(),
            },
            V8Config {
                execution_timeout: Duration::from_millis(30),
                ..Default::default()
            },
        )
        .unwrap();
        engine.prewarm().await.unwrap();
        let cloned = engine.clone();
        let mut call = tokio::spawn(async move {
            cloned
                .invoke(
                    &Host,
                    Invocation {
                        function: "handlers.run".into(),
                        kind: FunctionKind::Query,
                        args: b"null".to_vec(),
                        context: InvocationContext::default(),
                    },
                )
                .await
        });
        let result = tokio::time::timeout(Duration::from_secs(1), &mut call).await;
        let observed = format!("{result:?}");
        if result.is_err() {
            call.abort();
            let _ = call.await;
        }
        drop(engine);
        assert!(
            matches!(result, Ok(Ok(Err(ModuleError::BudgetExceeded(_))))),
            "getter lookup escaped its 30 ms budget: {observed}"
        );
    });
}
fn engine(source: &str, timeout: Duration, paths: &[&str]) -> V8ModuleEngine {
    V8ModuleEngine::from_artifact(
        ModuleArtifact {
            manifest: ModuleManifest {
                module_id: "lookup-cleanup".into(),
                generation: 1,
                language: ModuleLanguage::TypeScript,
                artifact_hash: "test".into(),
                functions: paths
                    .iter()
                    .map(|path| FunctionContract {
                        path: (*path).into(),
                        kind: FunctionKind::Query,
                        internal: false,
                        delivery: None,
                        args_schema: Some(serde_json::json!({"kind":"any"})),
                        result_schema: Some(serde_json::json!({"kind":"any"})),
                        metadata: Default::default(),
                    })
                    .collect(),
                metadata: Default::default(),
            },
            payload: source.as_bytes().to_vec(),
        },
        V8Config {
            execution_timeout: timeout,
            ..Default::default()
        },
    )
    .unwrap()
}
fn invoke(path: &str) -> Invocation {
    Invocation {
        function: path.into(),
        kind: FunctionKind::Query,
        args: b"null".to_vec(),
        context: InvocationContext::default(),
    }
}
#[test]
fn missing_handler_disarms_before_reuse() {
    crate::host_call_tests::run_v8_test(async {
        let timeout = Duration::from_millis(50);
        let engine = engine(
            "let n=0; export function run(){return ++n}",
            timeout,
            &["missing", "run"],
        );
        engine.prewarm().await.unwrap();
        assert!(matches!(
            engine.invoke(&Host, invoke("missing")).await,
            Err(ModuleError::FunctionNotFound(_))
        ));
        tokio::time::sleep(timeout + Duration::from_millis(50)).await;
        assert_eq!(
            engine.invoke(&Host, invoke("run")).await.unwrap().value,
            b"1"
        );
        assert_eq!(
            engine.invoke(&Host, invoke("run")).await.unwrap().value,
            b"2"
        );
    });
}
#[test]
fn getter_timeout_retires_worker() {
    crate::host_call_tests::run_v8_test(async {
        let engine=engine("let n=0; export const handlers={get run(){n++;while(true){}}}; export function okay(){return n}",Duration::from_millis(30),&["handlers.run","okay"]);
        engine.prewarm().await.unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(1),
            engine.invoke(&Host, invoke("handlers.run")),
        )
        .await
        .expect("lookup stayed blocked");
        assert!(matches!(result, Err(ModuleError::BudgetExceeded(_))));
        assert_eq!(
            engine.invoke(&Host, invoke("okay")).await.unwrap().value,
            b"0",
            "timed-out getter's worker was reused"
        );
    });
}
#[test]
fn zero_and_unrepresentable_execution_deadlines_fail_without_panicking() {
    crate::host_call_tests::run_v8_test(async {
        for timeout in [Duration::ZERO, Duration::MAX] {
            let engine = engine("export function run(){return 1}", timeout, &["run"]);
            engine.prewarm().await.unwrap();
            let result = engine.invoke(&Host, invoke("run")).await;
            if timeout.is_zero() {
                assert!(matches!(result, Err(ModuleError::BudgetExceeded(_))));
            } else {
                assert!(matches!(result, Err(ModuleError::Unsupported(_))));
            }
        }
    });
}
