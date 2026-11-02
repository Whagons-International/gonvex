use super::*;

async fn wait_file(path: &Path) -> String {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(path).await {
                if !value.is_empty() {
                    break value;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "native retirement coordinator did not publish {}",
            path.display()
        )
    })
}

async fn publish(path: &Path, value: Value) {
    let temporary = path.with_extension("tmp");
    tokio::fs::write(&temporary, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    tokio::fs::rename(temporary, path).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the parent-owned native exit coordinator"]
async fn native_child_exit_retains_capacity_until_retirement() {
    use std::os::unix::fs::PermissionsExt as _;
    let directory = std::env::var_os("GONVEX_SANDBOX_RETIREMENT_DIR")
        .map(PathBuf::from)
        .expect("native sandbox retirement requires its coordinator");
    let mut premature = Vec::new();
    for mode in [
        "abort-response",
        "abort-write",
        "cancel-write",
        "timeout-write",
    ] {
        let case = directory.join(mode);
        tokio::fs::create_dir_all(&case).await.unwrap();
        let root = case.join("workspace");
        tokio::fs::create_dir_all(&root).await.unwrap();
        let worker = root.join("worker.sh");
        let quoted = case.to_string_lossy().replace('\'', "'\\''");
        let consume = if mode == "abort-response" {
            "while IFS= read -r line; do :; done\n"
        } else {
            ""
        };
        tokio::fs::write(&worker, format!(
            "#!/bin/sh\n{consume}if [ -e '{quoted}/worker.pid' ]; then marker='{quoted}/replacement.pid'; else marker='{quoted}/worker.pid'; fi\nprintf '%s' \"$$\" > \"$marker\"\nexec sleep 60\n"
        )).await.unwrap();
        tokio::fs::set_permissions(&worker, std::fs::Permissions::from_mode(0o700))
            .await
            .unwrap();
        let manager = SandboxManager::new(SandboxConfig {
            root: root.clone(),
            concurrency: 1,
            worker_binary: Some(worker),
            ..SandboxConfig::default()
        });
        let scope = Scope {
            project: "project".into(),
            tenant: "tenant".into(),
            account: "alice".into(),
        };
        let first = manager
            .create(scope.clone(), serde_json::json!({}), false)
            .await
            .unwrap();
        let first = first["sandboxId"].as_str().unwrap().to_owned();
        let second = manager
            .create(scope.clone(), serde_json::json!({}), false)
            .await
            .unwrap();
        let second = second["sandboxId"].as_str().unwrap().to_owned();
        let runner = manager.clone();
        let owner = scope.clone();
        let sandbox = first.clone();
        let mut task = tokio::spawn(async move {
            runner.run(owner, serde_json::json!({"sandboxId":sandbox,
                "code":if mode == "abort-response" { "1".to_owned() } else { "x".repeat(512 << 10) },
                "timeoutMs":if mode == "timeout-write" { 1 } else { 30_000 }})).await
        });
        let pid: u32 = wait_file(&case.join("worker.pid")).await.parse().unwrap();
        assert!(pid > 1);
        publish(
            &case.join("request.json"),
            serde_json::json!({"pid":pid,"parent":std::process::id(),"mode":mode}),
        )
        .await;
        let ready: Value =
            serde_json::from_str(&wait_file(&case.join("attached.json")).await).unwrap();
        assert_eq!(ready["pid"], pid);
        assert_eq!(manager.inner.admission.available_permits(), 0);
        if mode.starts_with("abort") {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else if mode == "cancel-write" {
            let execution = manager.inner.workspaces.lock().await[&first]
                .active
                .clone()
                .unwrap();
            manager
                .cancel(
                    scope.clone(),
                    serde_json::json!({"sandboxId":first,"executionId":execution}),
                )
                .await
                .unwrap();
            let answer = tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(answer["status"], "cancelled");
        } else {
            let answer = tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            assert_eq!(answer["error"], "sandbox execution timed out");
        }
        let held: Value = serde_json::from_str(&wait_file(&case.join("held.json")).await).unwrap();
        assert_eq!(held["pid"], pid);
        assert_eq!(held["exitEventHeld"], true);
        assert_eq!(held["nativeMappingsRetained"], true);
        let available_while_held = manager.inner.admission.available_permits();
        let runner = manager.clone();
        let owner = scope.clone();
        let sandbox = second.clone();
        let replacement = tokio::spawn(async move {
            runner
                .run(
                    owner,
                    serde_json::json!({"sandboxId":sandbox,"code":"1","timeoutMs":30_000}),
                )
                .await
        });
        let second_marker = case.join("replacement.pid");
        if available_while_held > 0 {
            // Require actual overlapping native children for the bad-policy
            // witness, rather than inferring overlap from an integer alone.
            wait_file(&second_marker).await;
        } else {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        let replacement_started_while_held = second_marker.exists();
        if available_while_held > 0 || replacement_started_while_held {
            premature.push(mode);
        }
        publish(
            &case.join("observed.json"),
            serde_json::json!({
                "mode":mode,"pid":pid,"availableWhileNativeExitHeld":available_while_held,
                "replacementStartedWhileNativeExitHeld":replacement_started_while_held,
            }),
        )
        .await;
        publish(
            &case.join("continue.json"),
            serde_json::json!({"release":true}),
        )
        .await;
        wait_file(&case.join("reaped.json")).await;
        let replacement_pid: u32 = wait_file(&second_marker).await.parse().unwrap();
        assert!(replacement_pid > 1 && replacement_pid != pid);
        replacement.abort();
        assert!(replacement.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let states = manager.inner.workspaces.lock().await;
                let clear = states[&first].active.is_none() && states[&second].active.is_none();
                drop(states);
                if clear
                    && manager.inner.admission.available_permits() == 1
                    && !Path::new(&format!("/proc/{pid}")).exists()
                    && !Path::new(&format!("/proc/{replacement_pid}")).exists()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("native retirement leaked child, reservation or capacity");
        let permit = manager.inner.admission.try_acquire().unwrap();
        assert!(manager.inner.admission.try_acquire().is_err());
        drop(permit);
        manager
            .destroy(scope.clone(), serde_json::json!({"sandboxId":first}))
            .await
            .unwrap();
        manager
            .destroy(scope, serde_json::json!({"sandboxId":second}))
            .await
            .unwrap();
        manager.shutdown().await;
        let _ = tokio::fs::remove_dir_all(root).await;
        publish(&case.join("complete.json"), serde_json::json!({"complete":true,"replacementProgressedAfterExit":true,"exactCapacity":true})).await;
    }
    assert!(
        premature.is_empty(),
        "sandbox admission was reused before native child exit: {premature:?}"
    );
}
