use std::time::{SystemTime, UNIX_EPOCH};

use gonvex_module_runtime::{
    BoxFuture, Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse,
    Invocation, InvocationContext, ModuleArtifact, ModuleEngine, ModuleHost, ModuleLanguage,
    ModuleManifest,
};
use serde_json::{json, Map, Value};

use crate::{V8Config, V8ModuleEngine};

const VECTORS: &str = include_str!("../../../../packages/module-sdk/runtime-contract/vectors.json");
const RUNNER: &str = include_str!("../../../../packages/module-sdk/runtime-contract/runner.js");

struct NoExternalServices;
impl ModuleHost for NoExternalServices {
    fn call<'a>(
        &'a self,
        _: &'a InvocationContext,
        _: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async {
            Err(HostError::Failed(
                "contract case reached an external service".to_owned(),
            ))
        })
    }
}

fn wall_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn published_runtime_contract_vectors_run_in_the_real_isolate() {
    crate::host_call_tests::run_v8_test(async {
        let suite: Value = serde_json::from_str(VECTORS).unwrap();
        let vectors = suite["cases"].as_array().unwrap();
        let functions = vectors
            .iter()
            .map(|vector| {
                let mut metadata = Map::from_iter([("export".into(), json!("contract"))]);
                if let Some(capabilities) = vector.get("capabilities") {
                    metadata.insert("actionCapabilities".into(), capabilities.clone());
                }
                FunctionContract {
                    path: vector["id"].as_str().unwrap().to_owned(),
                    kind: serde_json::from_value(vector["kind"].clone()).unwrap(),
                    internal: false,
                    delivery: None,
                    args_schema: Some(
                        vector
                            .get("argsSchema")
                            .cloned()
                            .unwrap_or_else(|| json!({"kind":"any"})),
                    ),
                    result_schema: Some(json!({"kind":"any"})),
                    metadata,
                }
            })
            .collect();
        let engine = V8ModuleEngine::from_artifact(
            ModuleArtifact {
                manifest: ModuleManifest {
                    module_id: "published-runtime-contract".into(),
                    generation: 1,
                    language: ModuleLanguage::TypeScript,
                    artifact_hash: "runtime-contract-v1".into(),
                    functions,
                    metadata: Map::new(),
                },
                payload: RUNNER.as_bytes().to_vec(),
            },
            V8Config::default(),
        )
        .unwrap();
        // Repeat in the same pool. Captured globals, random counters and key
        // caches must not carry an invocation's state into its successor.
        for _ in 0..2 {
            for vector in vectors {
                let id = vector["id"].as_str().unwrap();
                let kind: FunctionKind = serde_json::from_value(vector["kind"].clone()).unwrap();
                let start = wall_time();
                let outcome = engine
                    .invoke(
                        &NoExternalServices,
                        Invocation {
                            function: id.to_owned(),
                            kind,
                            args: serde_json::to_vec(vector.get("input").unwrap_or(vector))
                                .unwrap(),
                            context: InvocationContext {
                                now_unix_ms: vector
                                    .get("now")
                                    .and_then(Value::as_u64)
                                    .unwrap_or(1792324800123),
                                intent_entropy: vector
                                    .get("intentEntropy")
                                    .and_then(Value::as_str)
                                    .map(str::to_owned),
                                generation: 1,
                                capabilities: Capabilities {
                                    network: true,
                                    ..Default::default()
                                },
                                ..Default::default()
                            },
                        },
                    )
                    .await;
                let end = wall_time();
                if let Some(expected) = vector.get("expectedError").and_then(Value::as_str) {
                    let error = outcome.expect_err(id).to_string();
                    assert!(
                        error.contains(expected),
                        "{id}: expected {expected:?}, got {error}"
                    );
                    continue;
                }
                let outcome = outcome.unwrap_or_else(|error| panic!("{id}: {error}"));
                let value: Value = serde_json::from_slice(&outcome.value).unwrap();
                if vector["expectActionGlobals"] == true {
                    for clock in ["now", "constructed"] {
                        let actual = value[clock].as_u64().unwrap();
                        assert!(
                            (start..=end).contains(&actual),
                            "{id}: {clock}={actual} outside real wall clock {start}..={end}"
                        );
                    }
                    assert!((0.0..1.0).contains(&value["random"].as_f64().unwrap()));
                    assert_eq!(value["bytes"], 32);
                    assert_eq!(value["uuid"], true);
                } else {
                    assert_eq!(value, vector["expected"], "{id}");
                }
            }
        }
    });
}
