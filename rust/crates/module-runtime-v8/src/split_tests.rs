//! Split artifacts: Queries/Reducers run from the reducer bundle, resolved by
//! exact path; everything else runs from the complete bundle, whose isolates
//! start on demand and are released when idle.

use super::*;
use crate::host_call_tests::run_v8_test;
use gonvex_module_runtime::{
    FunctionContract, FunctionKind, HostCall, HostError, HostResponse, InvocationContext,
    ModuleLanguage, ModuleManifest,
};
use serde_json::{json, Map};

struct NoHost;

impl ModuleHost for NoHost {
    fn call<'a>(
        &'a self,
        _: &'a InvocationContext,
        _: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async { Err(HostError::Failed("no host calls in split tests".into())) })
    }
}

fn contract(path: &str, kind: FunctionKind, export: &str) -> FunctionContract {
    FunctionContract {
        path: path.into(),
        kind,
        internal: false,
        delivery: None,
        args_schema: Some(json!({"kind":"any"})),
        result_schema: Some(json!({"kind":"any"})),
        metadata: Map::from_iter([("export".into(), json!(export))]),
    }
}

/// The complete bundle answers "complete:<path>" and the reducer bundle
/// "reducer:<path>", so a test can see which bundle served each call. The
/// reducer bundle also exports a top-level `get` that a non-path resolver
/// would pick for `tasks.get` (export name "get").
const COMPLETE: &str = r#"
    const def = (tag) => ({ run: async () => "complete:" + tag });
    export const tasks = { get: def("tasks.get"), rename: def("tasks.rename") };
    export const assistant = { ask: def("assistant.ask") };
"#;
const REDUCERS: &str = r#"
    const def = (tag) => ({ run: async () => "reducer:" + tag });
    const get = def("tasks.get");
    const rename = def("tasks.rename");
    export const shadow = def("shadow");
    export { get as "tasks.get", rename as "tasks.rename", shadow as get };
"#;

fn engine(reducer_functions: Vec<&str>, config: V8Config) -> Result<V8ModuleEngine, ModuleError> {
    V8ModuleEngine::from_split_artifact(
        ModuleArtifact {
            manifest: ModuleManifest {
                module_id: "split".into(),
                generation: 1,
                language: ModuleLanguage::TypeScript,
                artifact_hash: "test".into(),
                functions: vec![
                    contract("tasks.get", FunctionKind::Query, "get"),
                    contract("tasks.rename", FunctionKind::Reducer, "rename"),
                    contract("assistant.ask", FunctionKind::Action, "ask"),
                ],
                metadata: Map::new(),
            },
            payload: COMPLETE.as_bytes().to_vec(),
        },
        Some(ReducerBundle {
            code: REDUCERS.as_bytes().to_vec(),
            functions: reducer_functions.into_iter().map(str::to_owned).collect(),
        }),
        config,
    )
}

async fn call(engine: &V8ModuleEngine, path: &str, kind: FunctionKind) -> String {
    let result = engine
        .invoke(
            &NoHost,
            Invocation {
                function: path.into(),
                kind,
                args: b"null".to_vec(),
                context: InvocationContext {
                    generation: 1,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    serde_json::from_slice::<String>(&result.value).unwrap()
}

#[test]
fn split_engine_routes_by_bundle_and_resolves_reducer_functions_by_path() {
    run_v8_test(async {
        let engine = engine(vec!["tasks.get", "tasks.rename"], V8Config::default()).unwrap();
        assert!(engine.is_split());
        engine.prewarm().await.unwrap();
        // Load-time validation evaluated the complete bundle and released it.
        assert_eq!(engine.inner.pools[0].idle_count(), 0);
        assert_eq!(engine.inner.reducer.as_ref().unwrap().pools[0].idle_count(), 1);

        assert_eq!(call(&engine, "tasks.get", FunctionKind::Query).await, "reducer:tasks.get");
        assert_eq!(call(&engine, "tasks.rename", FunctionKind::Reducer).await, "reducer:tasks.rename");
        assert_eq!(call(&engine, "assistant.ask", FunctionKind::Action).await, "complete:assistant.ask");
        assert_eq!(engine.inner.pools[0].idle_count(), 1, "the Action isolate started on demand");
        assert_eq!(engine.release_idle_on_demand(Duration::ZERO), 1);
        assert_eq!(engine.inner.pools[0].idle_count(), 0);
        // Releasing never touches the warm Query/Reducer isolate.
        assert_eq!(engine.inner.reducer.as_ref().unwrap().pools[0].idle_count(), 1);
        assert_eq!(call(&engine, "assistant.ask", FunctionKind::Action).await, "complete:assistant.ask");
    });
}

#[test]
fn unlisted_queries_and_reducers_run_from_the_complete_bundle() {
    run_v8_test(async {
        let engine = engine(vec!["tasks.get"], V8Config::default()).unwrap();
        engine.prewarm().await.unwrap();
        assert_eq!(call(&engine, "tasks.get", FunctionKind::Query).await, "reducer:tasks.get");
        assert_eq!(call(&engine, "tasks.rename", FunctionKind::Reducer).await, "complete:tasks.rename");
    });
}

#[test]
fn idle_on_demand_isolates_are_released_by_the_reaper() {
    run_v8_test(async {
        let engine = engine(
            vec!["tasks.get", "tasks.rename"],
            V8Config {
                on_demand_idle_ttl: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap();
        engine.prewarm().await.unwrap();
        call(&engine, "assistant.ask", FunctionKind::Action).await;
        assert_eq!(engine.inner.pools[0].idle_count(), 1);
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while engine.inner.pools[0].idle_count() > 0 {
            assert!(std::time::Instant::now() < deadline, "idle Action isolate was never released");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    });
}

#[test]
fn split_artifacts_reject_actions_and_unknown_functions_in_the_reducer_bundle() {
    for listed in [vec!["assistant.ask"], vec!["tasks.missing"]] {
        assert!(matches!(
            engine(listed, V8Config::default()),
            Err(ModuleError::InvalidArtifact(_))
        ));
    }
}

#[test]
fn a_complete_bundle_that_fails_to_evaluate_fails_the_split_load() {
    run_v8_test(async {
        let engine = V8ModuleEngine::from_split_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "split-broken".into(),
                    generation: 1,
                    language: ModuleLanguage::TypeScript,
                    artifact_hash: "test".into(),
                    functions: vec![contract("tasks.get", FunctionKind::Query, "get")],
                    metadata: Map::new(),
                },
                payload: b"throw new Error('broken complete bundle');".to_vec(),
            },
            Some(ReducerBundle {
                code: b"const get = { run: async () => 1 }; export { get as \"tasks.get\" };".to_vec(),
                functions: vec!["tasks.get".into()],
            }),
            V8Config::default(),
        )
        .unwrap();
        assert!(engine.prewarm().await.is_err());
    });
}
