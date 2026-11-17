//! Turning a wire artifact into something the V8 engine will run.
//!
//! Two things happen before any JavaScript is evaluated: the bundle is decoded
//! and its SHA-256 is checked against the hash the build recorded, and the
//! declarative function metadata is lowered into `FunctionContract`s. The
//! `handler` and `export` names the build captured land in
//! `FunctionContract.metadata`, which is exactly what the engine's export
//! resolution reads, so a module's entry points come from its manifest rather
//! than from guessing at bundle shapes.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use gonvex_module_runtime::{FunctionContract, ModuleArtifact, ModuleLanguage, ModuleManifest};
use sha2::{Digest, Sha256};

use crate::protocol::{codes, parse_kind, FunctionSummary, ModuleArtifactWire, WireError};
use gonvex_module_runtime_v8::ReducerBundle;

pub struct DecodedArtifact {
    pub artifact: ModuleArtifact,
    /// The split Query/Reducer bundle, verified against its recorded hash.
    pub reducer_bundle: Option<ReducerBundle>,
    pub summaries: Vec<FunctionSummary>,
}

pub fn decode(
    module_id: &str,
    generation: u64,
    wire: ModuleArtifactWire,
) -> Result<DecodedArtifact, WireError> {
    let language = wire.language.trim().to_ascii_lowercase();
    if !language.is_empty() && language != "typescript" {
        return Err(WireError::new(
            codes::INVALID_ARTIFACT,
            format!("module {module_id} declares language {language}, but this host runs TypeScript modules"),
        ));
    }

    let code = BASE64
        .decode(wire.javascript.code.as_bytes())
        .map_err(|err| {
            WireError::new(
                codes::INVALID_ARTIFACT,
                format!("module {module_id} JavaScript is not valid base64: {err}"),
            )
        })?;
    if code.is_empty() {
        return Err(WireError::new(
            codes::INVALID_ARTIFACT,
            format!("module {module_id} has an empty JavaScript bundle"),
        ));
    }
    // The hash is what makes the artifact self-describing: a bundle that does
    // not match the manifest it arrived with never reaches an isolate.
    let expected = wire.javascript.hash.trim().to_ascii_lowercase();
    if expected.is_empty() {
        return Err(WireError::new(
            codes::INVALID_ARTIFACT,
            format!("module {module_id} JavaScript has no hash to verify"),
        ));
    }
    let actual = hex_digest(&code);
    if actual != expected {
        return Err(WireError::new(
            codes::ARTIFACT_HASH_MISMATCH,
            format!(
                "module {module_id} JavaScript hash {actual} does not match the manifest hash {expected}"
            ),
        ));
    }
    if std::str::from_utf8(&code).is_err() {
        return Err(WireError::new(
            codes::INVALID_ARTIFACT,
            format!("module {module_id} JavaScript is not valid UTF-8"),
        ));
    }

    let reducer_bundle = match wire.reducer_bundle {
        None => None,
        Some(bundle) => {
            let code = BASE64.decode(bundle.code.as_bytes()).map_err(|err| {
                WireError::new(
                    codes::INVALID_ARTIFACT,
                    format!("module {module_id} reducer bundle is not valid base64: {err}"),
                )
            })?;
            let expected = bundle.hash.trim().to_ascii_lowercase();
            let actual = hex_digest(&code);
            if expected.is_empty() || actual != expected {
                return Err(WireError::new(
                    codes::ARTIFACT_HASH_MISMATCH,
                    format!(
                        "module {module_id} reducer bundle hash {actual} does not match the manifest hash {expected}"
                    ),
                ));
            }
            Some(ReducerBundle {
                code,
                functions: bundle.functions,
            })
        }
    };

    let mut functions = Vec::with_capacity(wire.functions.len());
    let mut summaries = Vec::with_capacity(wire.functions.len());
    for function in wire.functions {
        let path = function.path.trim().to_owned();
        if path.is_empty() {
            return Err(WireError::new(
                codes::INVALID_ARTIFACT,
                format!("module {module_id} declares a function with no path"),
            ));
        }
        let kind = parse_kind(function.kind.trim()).ok_or_else(|| {
            WireError::new(
                codes::INVALID_ARTIFACT,
                format!(
                    "module {module_id} function {path} has unknown kind {}",
                    function.kind
                ),
            )
        })?;

        let mut metadata = function.metadata;
        insert_text(&mut metadata, "handler", function.handler.as_deref());
        insert_text(&mut metadata, "export", function.export.as_deref());
        insert_text(&mut metadata, "file", function.file.as_deref());
        insert_text(&mut metadata, "delivery", function.delivery.as_deref());
        if function.internal {
            metadata.insert("internal".to_owned(), serde_json::Value::Bool(true));
        }

        summaries.push(FunctionSummary {
            path: path.clone(),
            kind: function.kind.trim().to_owned(),
            internal: function.internal,
            delivery: function.delivery.clone(),
        });
        functions.push(FunctionContract {
            path,
            kind,
            internal: function.internal,
            delivery: function.delivery,
            args_schema: function.args,
            result_schema: function.result,
            metadata,
        });
    }

    let mut metadata = wire.metadata;
    insert_text(&mut metadata, "entrypoint", Some(wire.entrypoint.as_str()));
    insert_text(
        &mut metadata,
        "javascriptPath",
        Some(wire.javascript.path.as_str()),
    );

    let artifact_hash = if wire.hash.trim().is_empty() {
        actual
    } else {
        wire.hash.trim().to_owned()
    };
    Ok(DecodedArtifact {
        artifact: ModuleArtifact {
            manifest: ModuleManifest {
                module_id: module_id.to_owned(),
                generation,
                language: ModuleLanguage::TypeScript,
                artifact_hash,
                functions,
                metadata,
            },
            payload: code,
        },
        reducer_bundle,
        summaries,
    })
}

fn insert_text(
    metadata: &mut serde_json::Map<String, serde_json::Value>,
    key: &str,
    value: Option<&str>,
) {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    metadata.insert(key.to_owned(), serde_json::Value::String(value.to_owned()));
}

fn hex_digest(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest {
        encoded.push_str(&format!("{byte:02x}"));
    }
    encoded
}
