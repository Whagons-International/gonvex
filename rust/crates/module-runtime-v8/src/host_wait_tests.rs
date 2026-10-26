use super::*;
use gonvex_module_runtime::{
    Capabilities, FunctionContract, FunctionKind, HostCall, HostError, HostResponse,
    InvocationContext, ModuleLanguage, ModuleManifest,
};
use serde_json::json;
struct PendingHost {
    entered: tokio::sync::Notify,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
}
struct PendingDrop(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for PendingDrop {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
impl ModuleHost for PendingHost {
    fn call<'a>(
        &'a self,
        _: &'a InvocationContext,
        _: HostCall,
    ) -> BoxFuture<'a, Result<HostResponse, HostError>> {
        Box::pin(async move {
            let _guard = PendingDrop(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending().await
        })
    }
}
async fn check(raced: bool) {
    let engine=V8ModuleEngine::from_artifact(ModuleArtifact {
   manifest:ModuleManifest {module_id:"pending-host".into(),generation:1,language:ModuleLanguage::TypeScript,artifact_hash:"test".into(),
    functions:vec![FunctionContract{path:"run".into(),kind:FunctionKind::Query,internal:false,delivery:None,args_schema:Some(json!({"kind":"any"})),result_schema:Some(json!({"kind":"any"})),metadata:Default::default()}],metadata:Default::default()},
   payload:if raced {br#"export async function run(ctx) {return await Promise.race([ctx.db.query('hold'),new Promise(resolve=>setTimeout(()=>resolve(42),10))]);}"#.to_vec()}
    else {br#"export async function run(ctx) {return await ctx.db.query('hold');}"#.to_vec()}
  },V8Config{execution_timeout:Duration::from_millis(if raced {5000}else{200}),..Default::default()}).unwrap();
    engine.prewarm().await.unwrap();
    let host = Arc::new(PendingHost {
        entered: tokio::sync::Notify::new(),
        dropped: Default::default(),
    });
    let mut task = tokio::spawn({
        let engine = engine.clone();
        let host = host.clone();
        async move {
            engine
                .invoke(
                    &*host,
                    Invocation {
                        function: "run".into(),
                        kind: FunctionKind::Query,
                        args: b"null".to_vec(),
                        context: InvocationContext {
                            capabilities: Capabilities {
                                db_read: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(3), host.entered.notified())
        .await
        .expect("host did not enter");
    let result = tokio::time::timeout(Duration::from_secs(3), &mut task).await;
    let observed = format!("{result:?}");
    if result.is_err() {
        task.abort();
        let _ = task.await;
    }
    drop(engine);
    assert_eq!(
        host.dropped.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "pending host future not cancelled"
    );
    if raced {
        assert!(
            matches!(result,Ok(Ok(Ok(ref reply))) if reply.value==b"42"),
            "native completion blocked by pending serial host: {observed}"
        );
    } else {
        assert!(
            matches!(result, Ok(Ok(Err(ModuleError::BudgetExceeded(_))))),
            "native deadline blocked by pending serial host: {observed}"
        );
    }
}
#[test]
fn pending_serial_host_cannot_swallow_deadline() {
    crate::host_call_tests::run_v8_test(check(false));
}
#[test]
fn pending_serial_host_cannot_swallow_native_result() {
    crate::host_call_tests::run_v8_test(check(true));
}
