use super::*;

#[tokio::test]
async fn cancelled_admission_releases_workspace_execution_ownership() {
    for cancelled in [false, true] {
        let root = std::env::temp_dir().join(format!("gonvex-sandbox-cancel-{}", Uuid::new_v4()));
        let manager = SandboxManager::new(SandboxConfig {
            root: root.clone(),
            concurrency: 1,
            worker_binary: None,
            ..SandboxConfig::default()
        });
        let scope = Scope {
            project: "project".into(),
            tenant: "tenant".into(),
            account: "alice".into(),
        };
        let created = manager
            .create(scope.clone(), serde_json::json!({}), false)
            .await
            .unwrap();
        let id = created["sandboxId"].as_str().unwrap().to_owned();
        let permit = manager.inner.admission.acquire().await.unwrap();
        let runner = manager.clone();
        let owner = scope.clone();
        let sandbox = id.clone();
        let task = tokio::spawn(async move {
            runner
                .run(owner, serde_json::json!({"sandboxId":sandbox,"code":"1"}))
                .await
        });
        let execution_id = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Some(active) = manager.inner.workspaces.lock().await[&id].active.clone() {
                    break active;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("sandbox did not enter its actual admission wait");
        assert_eq!(manager.inner.admission.available_permits(), 0);
        assert!(!task.is_finished(), "held admission did not retain the run");
        if cancelled {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            drop(permit);
            let result = task.await.unwrap().unwrap();
            assert_eq!(result["status"], "failed");
            assert_eq!(result["error"], "sandbox worker binary is not configured");
            // Acquire the exact same capacity for the checks shared with abort.
            let reacquired = manager.inner.admission.try_acquire().unwrap();
            assert!(manager.inner.admission.try_acquire().is_err());
            drop(reacquired);
            manager
                .destroy(scope, serde_json::json!({"sandboxId":id}))
                .await
                .unwrap();
            manager.shutdown().await;
            let _ = tokio::fs::remove_dir_all(root).await;
            continue;
        }
        let cleaned = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let workspaces = manager.inner.workspaces.lock().await;
                let workspace = &workspaces[&id];
                let execution = &workspace.executions[&execution_id];
                if workspace.active.is_none() {
                    assert_eq!(execution.status, "cancelled");
                    assert!(execution.finished_at.is_some());
                    break;
                }
                drop(workspaces);
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
        // Cleanup before failing, including the failing previous-source run.
        drop(permit);
        let available = manager.inner.admission.try_acquire().unwrap();
        assert!(manager.inner.admission.try_acquire().is_err());
        drop(available);
        let destroyed = manager
            .destroy(scope, serde_json::json!({"sandboxId":id}))
            .await;
        manager.shutdown().await;
        let _ = tokio::fs::remove_dir_all(root).await;
        assert!(
            cleaned,
            "cancelled sandbox admission leaked workspace execution ownership"
        );
        assert!(
            destroyed.is_ok(),
            "cancelled sandbox remained permanently active"
        );
    }
}

#[tokio::test]
async fn cancellation_cleanup_cannot_clear_a_different_execution_or_workspace() {
    let root = std::env::temp_dir().join(format!("gonvex-sandbox-identity-{}", Uuid::new_v4()));
    let manager = SandboxManager::new(SandboxConfig {
        root: root.clone(),
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
    let second = manager
        .create(scope, serde_json::json!({}), false)
        .await
        .unwrap();
    let first = first["sandboxId"].as_str().unwrap();
    let second = second["sandboxId"].as_str().unwrap();
    {
        let mut workspaces = manager.inner.workspaces.lock().await;
        for id in [first, second] {
            workspaces.get_mut(id).unwrap().active = Some("new-run".into());
        }
        cancel_active_execution(&mut workspaces, first, "old-run");
        cancel_active_execution(&mut workspaces, "absent", "new-run");
        assert_eq!(workspaces[first].active.as_deref(), Some("new-run"));
        assert_eq!(workspaces[second].active.as_deref(), Some("new-run"));
        cancel_active_execution(&mut workspaces, first, "new-run");
        assert!(workspaces[first].active.is_none());
        assert_eq!(workspaces[second].active.as_deref(), Some("new-run"));
        // Repeated cleanup is a no-op, including the absent execution record.
        cancel_active_execution(&mut workspaces, first, "new-run");
        assert!(workspaces[first].active.is_none());
    }
    manager.shutdown().await;
    let _ = tokio::fs::remove_dir_all(root).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn cancelling_a_native_worker_releases_its_execution_reservation() {
    use std::os::unix::fs::PermissionsExt as _;
    let root =
        std::env::temp_dir().join(format!("gonvex-sandbox-worker-cancel-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let witness = root.join("worker.pid");
    let worker = root.join("worker.sh");
    let quoted = witness.to_string_lossy().replace('\'', "'\\''");
    tokio::fs::write(&worker, format!(
        "#!/bin/sh\nwhile IFS= read -r line; do :; done\nprintf '%s' \"$$\" > '{quoted}'\nexec sleep 60\n"
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
    let created = manager
        .create(scope.clone(), serde_json::json!({}), false)
        .await
        .unwrap();
    let id = created["sandboxId"].as_str().unwrap().to_owned();
    let runner = manager.clone();
    let owner = scope.clone();
    let sandbox = id.clone();
    let task = tokio::spawn(async move {
        runner
            .run(owner, serde_json::json!({"sandboxId":sandbox,"code":"1"}))
            .await
    });
    let pid = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(value) = tokio::fs::read_to_string(&witness).await {
                if let Ok(pid) = value.parse::<u32>() {
                    break pid;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    if pid.is_err() {
        task.abort();
        let _ = task.await;
        manager.shutdown().await;
        let _ = tokio::fs::remove_dir_all(root).await;
        panic!("actual sandbox worker did not consume its request and start");
    }
    let pid = pid.unwrap();
    assert!(pid > 1);
    assert_eq!(manager.inner.admission.available_permits(), 0);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    let cleaned = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let workspaces = manager.inner.workspaces.lock().await;
            let workspace = &workspaces[&id];
            if workspace.active.is_none() && !std::path::Path::new(&format!("/proc/{pid}")).exists()
            {
                assert!(workspace
                    .executions
                    .values()
                    .all(|run| run.status == "cancelled" && run.finished_at.is_some()));
                break;
            }
            drop(workspaces);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_ok();
    let available = manager.inner.admission.try_acquire().unwrap();
    assert!(manager.inner.admission.try_acquire().is_err());
    drop(available);
    let destroyed = manager
        .destroy(scope, serde_json::json!({"sandboxId":id}))
        .await;
    manager.shutdown().await;
    let _ = tokio::fs::remove_dir_all(root).await;
    assert!(
        cleaned,
        "cancelled native sandbox worker retained its process or execution ownership"
    );
    assert!(destroyed.is_ok());
}

#[tokio::test]
async fn explicit_cancel_finishes_queued_execution_before_admission_is_available() {
    let root =
        std::env::temp_dir().join(format!("gonvex-sandbox-queued-cancel-{}", Uuid::new_v4()));
    let manager = SandboxManager::new(SandboxConfig {
        root: root.clone(),
        concurrency: 1,
        worker_binary: None,
        ..SandboxConfig::default()
    });
    let scope = Scope {
        project: "project".into(),
        tenant: "tenant".into(),
        account: "alice".into(),
    };
    let created = manager
        .create(scope.clone(), serde_json::json!({}), false)
        .await
        .unwrap();
    let id = created["sandboxId"].as_str().unwrap().to_owned();
    let gate = manager.inner.admission.acquire().await.unwrap();
    let runner = manager.clone();
    let owner = scope.clone();
    let sandbox = id.clone();
    let mut task = tokio::spawn(async move {
        runner
            .run(owner, serde_json::json!({"sandboxId":sandbox,"code":"1"}))
            .await
    });
    let execution = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(active) = manager.inner.workspaces.lock().await[&id].active.clone() {
                break active;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!task.is_finished());
    let ack = manager
        .cancel(
            scope.clone(),
            serde_json::json!({"sandboxId":id,"executionId":execution}),
        )
        .await
        .unwrap();
    assert_eq!(ack["executionId"], execution);
    let answer = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
    let terminated_while_held = answer.is_ok();
    if let Ok(answer) = answer {
        let answer = answer.unwrap().unwrap();
        assert_eq!(answer["status"], "cancelled");
        assert_eq!(answer["error"], "sandbox execution cancelled");
    } else {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(manager.inner.admission.available_permits(), 0);
    drop(gate);
    // Let existing abort cleanup finish too, so a negative run leaves no state.
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if manager.inner.workspaces.lock().await[&id].active.is_none() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let permit = manager.inner.admission.try_acquire().unwrap();
    assert!(manager.inner.admission.try_acquire().is_err());
    drop(permit);
    let destroyed = manager
        .destroy(scope, serde_json::json!({"sandboxId":id}))
        .await;
    manager.shutdown().await;
    let _ = tokio::fs::remove_dir_all(root).await;
    assert!(
        terminated_while_held,
        "explicit sandbox cancellation waited for occupied admission"
    );
    assert!(destroyed.is_ok());
}

#[cfg(target_os = "linux")]
async fn blocked_native_request_write(cancelled: bool) {
    use std::os::unix::fs::PermissionsExt as _;
    {
        let root =
            std::env::temp_dir().join(format!("gonvex-sandbox-stdin-cancel-{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&root).await.unwrap();
        let witness = root.join("worker.pid");
        let worker = root.join("worker.sh");
        let quoted = witness.to_string_lossy().replace('\'', "'\\''");
        tokio::fs::write(
            &worker,
            format!("#!/bin/sh\nprintf '%s' \"$$\" > '{quoted}'\nexec sleep 60\n"),
        )
        .await
        .unwrap();
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
        let created = manager
            .create(scope.clone(), serde_json::json!({}), false)
            .await
            .unwrap();
        let id = created["sandboxId"].as_str().unwrap().to_owned();
        let runner = manager.clone();
        let owner = scope.clone();
        let sandbox = id.clone();
        let mut task = tokio::spawn(async move {
            runner.run(owner, serde_json::json!({"sandboxId":sandbox,"code":"x".repeat(512 << 10),"timeoutMs":if cancelled { 30_000 } else { 1 }})).await
        });
        let pid = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(value) = tokio::fs::read_to_string(&witness).await {
                    if let Ok(pid) = value.parse::<u32>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert!(pid > 1);
        assert!(!task.is_finished());
        let execution = manager.inner.workspaces.lock().await[&id]
            .active
            .clone()
            .unwrap();
        if cancelled {
            manager
                .cancel(
                    scope.clone(),
                    serde_json::json!({"sandboxId":id,"executionId":execution}),
                )
                .await
                .unwrap();
        }
        let answer = tokio::time::timeout(Duration::from_secs(5), &mut task).await;
        let terminated = answer.is_ok();
        if let Ok(answer) = answer {
            let answer = answer.unwrap().unwrap();
            assert_eq!(
                answer["status"],
                if cancelled { "cancelled" } else { "failed" }
            );
            assert_eq!(
                answer["error"],
                if cancelled {
                    "sandbox execution cancelled"
                } else {
                    "sandbox execution timed out"
                }
            );
        } else {
            task.abort();
            let _ = task.await;
        }
        let stopped = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !std::path::Path::new(&format!("/proc/{pid}")).exists()
                    && manager.inner.workspaces.lock().await[&id].active.is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .is_ok();
        let permit = manager.inner.admission.try_acquire().unwrap();
        assert!(manager.inner.admission.try_acquire().is_err());
        drop(permit);
        let destroyed = manager
            .destroy(scope, serde_json::json!({"sandboxId":id}))
            .await;
        manager.shutdown().await;
        let _ = tokio::fs::remove_dir_all(root).await;
        assert!(
            stopped,
            "blocked stdin fixture child or reservation survived cleanup"
        );
        assert!(destroyed.is_ok());
        assert!(
            terminated,
            "sandbox cancel or timeout did not cover blocked request write, cancelled={cancelled}"
        );
    }
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn explicit_cancel_covers_a_blocked_native_request_write() {
    blocked_native_request_write(true).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn explicit_cancel_timeout_covers_a_blocked_native_request_write() {
    blocked_native_request_write(false).await;
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn explicit_cancel_boundary_preserves_an_uncancelled_worker_response() {
    use std::os::unix::fs::PermissionsExt as _;
    let root =
        std::env::temp_dir().join(format!("gonvex-sandbox-worker-success-{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&root).await.unwrap();
    let worker = root.join("worker.sh");
    tokio::fs::write(&worker, "#!/bin/sh\nwhile IFS= read -r line; do :; done\nprintf '%s' '{\"ok\":true,\"result\":{\"answer\":42},\"logs\":[]}'\n").await.unwrap();
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
    let created = manager
        .create(scope.clone(), serde_json::json!({}), false)
        .await
        .unwrap();
    let id = created["sandboxId"].as_str().unwrap().to_owned();
    let answer = tokio::time::timeout(
        Duration::from_secs(5),
        manager.run(
            scope.clone(),
            serde_json::json!({"sandboxId":id,"code":"1"}),
        ),
    )
    .await;
    let no_active = manager.inner.workspaces.lock().await[&id].active.is_none();
    let permit = manager.inner.admission.try_acquire().unwrap();
    assert!(manager.inner.admission.try_acquire().is_err());
    drop(permit);
    let destroyed = manager
        .destroy(scope, serde_json::json!({"sandboxId":id}))
        .await;
    manager.shutdown().await;
    let _ = tokio::fs::remove_dir_all(root).await;
    let answer = answer
        .expect("uncancelled native worker did not finish")
        .unwrap();
    assert_eq!(answer["status"], "succeeded");
    assert_eq!(answer["result"], serde_json::json!({"answer":42}));
    assert_eq!(answer["logs"], serde_json::json!([]));
    assert!(no_active && destroyed.is_ok());
}

#[cfg(target_os = "linux")]
#[tokio::test]
async fn retirement_wait_error_closes_admission_and_wakes_queued_work() {
    use std::os::unix::process::ExitStatusExt as _;
    for failed in [false, true] {
        let admission = Arc::new(Semaphore::new(1));
        let permit = admission.clone().acquire_owned().await.unwrap();
        let pending = admission.clone().acquire_owned();
        tokio::pin!(pending);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut pending)
                .await
                .is_err()
        );
        let result = if failed {
            Err(std::io::Error::other("injected child wait failure"))
        } else {
            Ok(std::process::ExitStatus::from_raw(0))
        };
        finish_child_retirement(result, permit);
        let answer = tokio::time::timeout(Duration::from_secs(5), pending)
            .await
            .unwrap();
        if failed {
            assert!(answer.is_err());
            assert!(admission.is_closed());
            assert_eq!(admission.available_permits(), 1);
        } else {
            let permit = answer.unwrap();
            assert!(!admission.is_closed());
            assert!(admission.try_acquire().is_err());
            drop(permit);
            let permit = admission.try_acquire().unwrap();
            assert!(admission.try_acquire().is_err());
            drop(permit);
        }
    }
}
