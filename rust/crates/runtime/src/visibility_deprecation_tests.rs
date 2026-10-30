use super::*;
use std::io::{self, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct CapturedLog(Arc<Mutex<Vec<u8>>>);

impl Write for CapturedLog {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn legacy_admission_warns_and_version_two_does_not() {
    for legacy in [true, false] {
        let output = Arc::new(Mutex::new(Vec::new()));
        let sink = output.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::WARN)
            .with_writer(move || CapturedLog(sink.clone()))
            .finish();
        let plan = if legacy {
            serde_json::json!({"table":"tasks","key":"id","sets":{},"where":{"operator":"public"}})
        } else {
            serde_json::json!({"version":2,"permissionTables":[],"table":"tasks","key":"id","sets":{},"where":{"operator":"public"}})
        };
        let mut manifest = serde_json::json!({"module":{
            "generation":1,"language":"typescript","hash":"pending",
            "entrypoint":"test.js","files":{},"functions":{},"crons":[],
            "visibility":{"tasks":plan},
            "javascript":{"path":"test.js","hash":"abc","code":"eA=="}
        }});
        let hash = canonical_artifact_hash(manifest["module"].as_object().unwrap()).unwrap();
        manifest["module"]["hash"] = Value::String(hash.clone());
        let record = RuntimeManifestRecord {
            project_id: "visibility-log-test".into(),
            module_hash: hash,
            manifest,
        };
        tracing::subscriber::with_default(subscriber, || {
            artifact_from_manifest(&record)
                .expect("both legacy and mask manifests remain admissible");
        });
        let log = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        if legacy {
            assert!(
                log.contains("DEPRECATED legacy visibility manifest"),
                "{log}"
            );
            assert!(log.contains("legacy_plans=1"), "{log}");
            assert!(log.contains("migrate to version 2 masks"), "{log}");
        } else {
            assert!(!log.contains("DEPRECATED"), "{log}");
        }
    }
}
