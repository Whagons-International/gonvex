//! The supported WebCrypto contract. Cryptographic operations belong to ring;
//! this module only validates WebCrypto parameters and translates key formats.
use std::collections::BTreeSet;

use base64::{engine::general_purpose, Engine as _};
use deno_core::{
    anyhow::{anyhow, Result},
    cppgc::GarbageCollected,
    op2,
};
use pkcs1::der::Decode;
use ring::{
    aead, digest, hmac,
    rand::SystemRandom,
    signature::{self, KeyPair},
};
use serde::Deserialize;
use serde_json::{json, Value};

enum Material {
    Hmac(hmac::Key),
    Aes(Box<aead::LessSafeKey>),
    RsaPrivate(signature::RsaKeyPair),
    RsaPublic { n: Vec<u8>, e: Vec<u8> },
    EdPrivate(signature::Ed25519KeyPair),
    EdPublic(Vec<u8>),
}

/// A native opaque object owned by V8's garbage collector, never a JS byte array
/// or an accumulating integer-handle registry. It survives caller key caches.
pub(crate) struct CryptoKey {
    material: Material,
    name: String,
    usages: BTreeSet<String>,
    info: Value,
}
impl GarbageCollected for CryptoKey {}

fn error(name: &str, message: &str) -> deno_core::anyhow::Error {
    anyhow!("{name}: {message}")
}
fn data_error() -> deno_core::anyhow::Error {
    error("DataError", "invalid key data")
}
fn operation_error() -> deno_core::anyhow::Error {
    error("OperationError", "cryptographic operation failed")
}
fn unsupported() -> deno_core::anyhow::Error {
    error("NotSupportedError", "unsupported algorithm or key format")
}

fn name(value: &Value) -> Result<String> {
    value
        .as_str()
        .or_else(|| value.get("name").and_then(Value::as_str))
        .map(str::to_ascii_uppercase)
        .ok_or_else(unsupported)
}
fn bytes(value: &Value) -> Result<Vec<u8>> {
    serde_json::from_value(value.clone()).map_err(|_| data_error())
}
fn jwk_bytes(value: &Value, field: &str) -> Result<Vec<u8>> {
    general_purpose::URL_SAFE_NO_PAD
        .decode(
            value
                .get(field)
                .and_then(Value::as_str)
                .ok_or_else(data_error)?,
        )
        .map_err(|_| data_error())
}
fn require_kty(value: &Value, expected: &str) -> Result<()> {
    if value["kty"] == expected {
        Ok(())
    } else {
        Err(data_error())
    }
}
fn rsa_public(der: &[u8]) -> Result<(Vec<u8>, Vec<u8>)> {
    let key = pkcs1::RsaPublicKey::from_der(der).map_err(|_| data_error())?;
    Ok((
        key.modulus.as_bytes().to_vec(),
        key.public_exponent.as_bytes().to_vec(),
    ))
}
fn spki_bytes(der: &[u8], oid: &str) -> Result<Vec<u8>> {
    let key = spki::SubjectPublicKeyInfoRef::try_from(der).map_err(|_| data_error())?;
    if key.algorithm.oid.to_string() != oid {
        return Err(data_error());
    }
    key.subject_public_key
        .as_bytes()
        .map(<[u8]>::to_vec)
        .ok_or_else(data_error)
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Import {
    format: String,
    data: Value,
    algorithm: Value,
    extractable: bool,
    usages: Vec<String>,
}

#[op2]
#[cppgc]
pub(crate) fn op_gonvex_crypto_import(#[string] request: String) -> Result<CryptoKey> {
    let request: Import = serde_json::from_str(&request).map_err(|_| data_error())?;
    let algorithm = name(&request.algorithm)?;
    let hash = if matches!(algorithm.as_str(), "HMAC" | "RSASSA-PKCS1-V1_5") {
        Some(name(&request.algorithm["hash"])?)
    } else {
        None
    };
    if algorithm == "HMAC" && !matches!(hash.as_deref(), Some("SHA-1" | "SHA-256"))
        || algorithm == "RSASSA-PKCS1-V1_5" && hash.as_deref() != Some("SHA-256")
    {
        return Err(unsupported());
    }
    let jwk = request.format == "jwk";
    if jwk {
        if request.data["ext"] == false && request.extractable {
            return Err(data_error());
        }
        if let Some(ops) = request.data.get("key_ops") {
            let ops = ops.as_array().ok_or_else(data_error)?;
            let mut seen = BTreeSet::new();
            for op in ops {
                if !seen.insert(op.as_str().ok_or_else(data_error)?) {
                    return Err(data_error());
                }
            }
            if request
                .usages
                .iter()
                .any(|usage| !seen.contains(usage.as_str()))
            {
                return Err(data_error());
            }
        }
        if let Some(usage) = request.data.get("use") {
            let expected = if algorithm == "AES-GCM" { "enc" } else { "sig" };
            if usage != expected {
                return Err(data_error());
            }
        }
    }
    let mut info = json!({"name": if algorithm == "RSASSA-PKCS1-V1_5" { "RSASSA-PKCS1-v1_5" } else if algorithm == "ED25519" { "Ed25519" } else { algorithm.as_str() }});
    if let Some(hash) = &hash {
        info["hash"] = json!({"name":hash});
    }
    let (material, kind, jwk_alg) = match algorithm.as_str() {
        "HMAC" | "AES-GCM" => {
            let raw = match request.format.as_str() {
                "raw" => bytes(&request.data)?,
                "jwk" => {
                    require_kty(&request.data, "oct")?;
                    jwk_bytes(&request.data, "k")?
                }
                _ => return Err(unsupported()),
            };
            info["length"] = json!(raw.len() * 8);
            if algorithm == "HMAC" {
                if raw.is_empty() {
                    return Err(data_error());
                }
                if let Some(length) = request.algorithm.get("length") {
                    // WebCrypto permits at most one final partial octet.
                    let length = length.as_u64().ok_or_else(data_error)?;
                    if length == 0
                        || length > (raw.len() * 8) as u64
                        || length <= (raw.len().saturating_sub(1) * 8) as u64
                    {
                        return Err(data_error());
                    }
                    if length % 8 != 0 {
                        return Err(unsupported());
                    }
                }
                let (alg, jwk_alg) = if hash.as_deref() == Some("SHA-1") {
                    (hmac::HMAC_SHA1_FOR_LEGACY_USE_ONLY, "HS1")
                } else {
                    (hmac::HMAC_SHA256, "HS256")
                };
                (Material::Hmac(hmac::Key::new(alg, &raw)), "secret", jwk_alg)
            } else {
                let (alg, jwk_alg) = match raw.len() {
                    16 => (&aead::AES_128_GCM, "A128GCM"),
                    32 => (&aead::AES_256_GCM, "A256GCM"),
                    _ => return Err(data_error()),
                };
                let key = aead::UnboundKey::new(alg, &raw).map_err(|_| data_error())?;
                (
                    Material::Aes(Box::new(aead::LessSafeKey::new(key))),
                    "secret",
                    jwk_alg,
                )
            }
        }
        "RSASSA-PKCS1-V1_5" => {
            let private = request.format == "pkcs8" || jwk && request.data.get("d").is_some();
            let material = if private {
                let key = if jwk {
                    require_kty(&request.data, "RSA")?;
                    signature::RsaKeyPair::from_components(&ring::rsa::KeyPairComponents {
                        public_key: ring::rsa::PublicKeyComponents {
                            n: jwk_bytes(&request.data, "n")?,
                            e: jwk_bytes(&request.data, "e")?,
                        },
                        d: jwk_bytes(&request.data, "d")?,
                        p: jwk_bytes(&request.data, "p")?,
                        q: jwk_bytes(&request.data, "q")?,
                        dP: jwk_bytes(&request.data, "dp")?,
                        dQ: jwk_bytes(&request.data, "dq")?,
                        qInv: jwk_bytes(&request.data, "qi")?,
                    })
                    .map_err(|_| data_error())?
                } else {
                    signature::RsaKeyPair::from_pkcs8(&bytes(&request.data)?)
                        .map_err(|_| data_error())?
                };
                let (n, e) = rsa_public(key.public_key().as_ref())?;
                info["modulusLength"] =
                    json!(n.len() * 8 - n.first().map_or(0, |byte| byte.leading_zeros() as usize));
                info["publicExponent"] = json!(e);
                Material::RsaPrivate(key)
            } else {
                let (n, e) = match request.format.as_str() {
                    "spki" => {
                        rsa_public(&spki_bytes(&bytes(&request.data)?, "1.2.840.113549.1.1.1")?)?
                    }
                    "jwk" => {
                        require_kty(&request.data, "RSA")?;
                        (
                            jwk_bytes(&request.data, "n")?,
                            jwk_bytes(&request.data, "e")?,
                        )
                    }
                    _ => return Err(unsupported()),
                };
                if n.is_empty() || e.is_empty() {
                    return Err(data_error());
                }
                info["modulusLength"] = json!(n.len() * 8 - n[0].leading_zeros() as usize);
                info["publicExponent"] = json!(e);
                Material::RsaPublic { n, e }
            };
            (
                material,
                if private { "private" } else { "public" },
                "RS256",
            )
        }
        "ED25519" => {
            let private = request.format == "pkcs8" || jwk && request.data.get("d").is_some();
            let material = if private {
                let key = if jwk {
                    require_kty(&request.data, "OKP")?;
                    if request.data["crv"] != "Ed25519" {
                        return Err(data_error());
                    }
                    signature::Ed25519KeyPair::from_seed_and_public_key(
                        &jwk_bytes(&request.data, "d")?,
                        &jwk_bytes(&request.data, "x")?,
                    )
                    .map_err(|_| data_error())?
                } else {
                    signature::Ed25519KeyPair::from_pkcs8_maybe_unchecked(&bytes(&request.data)?)
                        .map_err(|_| data_error())?
                };
                Material::EdPrivate(key)
            } else {
                let raw = match request.format.as_str() {
                    "raw" => bytes(&request.data)?,
                    "spki" => spki_bytes(&bytes(&request.data)?, "1.3.101.112")?,
                    "jwk" => {
                        require_kty(&request.data, "OKP")?;
                        if request.data["crv"] != "Ed25519" {
                            return Err(data_error());
                        }
                        jwk_bytes(&request.data, "x")?
                    }
                    _ => return Err(unsupported()),
                };
                if raw.len() != 32 {
                    return Err(data_error());
                }
                Material::EdPublic(raw)
            };
            (
                material,
                if private { "private" } else { "public" },
                "EdDSA",
            )
        }
        _ => return Err(unsupported()),
    };
    if jwk && request.data.get("alg").is_some_and(|alg| alg != jwk_alg) {
        return Err(data_error());
    }
    let allowed: &[&str] = match (&material, kind) {
        (Material::Hmac(_), _) => &["sign", "verify"],
        (Material::Aes(_), _) => &["encrypt", "decrypt"],
        (_, "private") => &["sign"],
        _ => &["verify"],
    };
    if request
        .usages
        .iter()
        .any(|usage| !allowed.contains(&usage.as_str()))
        || kind != "public" && request.usages.is_empty()
    {
        return Err(error("SyntaxError", "invalid key usages"));
    }
    let mut usages = BTreeSet::new();
    let ordered_usages = request
        .usages
        .into_iter()
        .filter(|usage| usages.insert(usage.clone()))
        .collect::<Vec<_>>();
    Ok(CryptoKey {
        material,
        name: algorithm,
        usages,
        info: json!({"type":kind,"extractable":request.extractable,"algorithm":info,"usages":ordered_usages}),
    })
}

#[op2]
#[string]
pub(crate) fn op_gonvex_crypto_key_info(#[cppgc] key: &CryptoKey) -> String {
    key.info.to_string()
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Operation {
    operation: String,
    algorithm: Value,
    data: Vec<u8>,
    #[serde(default)]
    signature: Vec<u8>,
    #[serde(default)]
    iv: Vec<u8>,
    #[serde(default)]
    additional_data: Vec<u8>,
    #[serde(default)]
    tag_length: Option<u16>,
}

#[op2]
#[string]
pub(crate) fn op_gonvex_crypto_operation(
    #[cppgc] key: &CryptoKey,
    #[string] request: String,
) -> Result<String> {
    let request: Operation = serde_json::from_str(&request).map_err(|_| operation_error())?;
    if !key.usages.contains(&request.operation) || name(&request.algorithm)? != key.name {
        return Err(error(
            "InvalidAccessError",
            "key algorithm or usage does not permit this operation",
        ));
    }
    let output = match (&key.material, request.operation.as_str()) {
        (Material::Hmac(key), "sign") => json!({"bytes":hmac::sign(key, &request.data).as_ref()}),
        (Material::Hmac(key), "verify") => {
            json!({"verified":hmac::verify(key, &request.data, &request.signature).is_ok()})
        }
        (Material::RsaPrivate(key), "sign") => {
            let mut signature = vec![0; key.public().modulus_len()];
            key.sign(
                &signature::RSA_PKCS1_SHA256,
                &SystemRandom::new(),
                &request.data,
                &mut signature,
            )
            .map_err(|_| operation_error())?;
            json!({"bytes":signature})
        }
        (Material::RsaPublic { n, e }, "verify") => {
            json!({"verified":ring::rsa::PublicKeyComponents { n, e }.verify(&signature::RSA_PKCS1_2048_8192_SHA256, &request.data, &request.signature).is_ok()})
        }
        (Material::EdPrivate(key), "sign") => json!({"bytes":key.sign(&request.data).as_ref()}),
        (Material::EdPublic(key), "verify") => {
            json!({"verified":signature::UnparsedPublicKey::new(&signature::ED25519, key).verify(&request.data, &request.signature).is_ok()})
        }
        (Material::Aes(key), "encrypt" | "decrypt") => {
            if request.tag_length.unwrap_or(128) != 128 {
                return Err(unsupported());
            }
            let nonce = aead::Nonce::try_assume_unique_for_key(&request.iv)
                .map_err(|_| operation_error())?;
            let mut data = request.data;
            let aad = aead::Aad::from(request.additional_data);
            if request.operation == "encrypt" {
                key.seal_in_place_append_tag(nonce, aad, &mut data)
                    .map_err(|_| operation_error())?;
            } else {
                let length = key
                    .open_in_place(nonce, aad, &mut data)
                    .map_err(|_| operation_error())?
                    .len();
                data.truncate(length);
            }
            json!({"bytes":data})
        }
        _ => {
            return Err(error(
                "InvalidAccessError",
                "key type does not permit this operation",
            ))
        }
    };
    Ok(output.to_string())
}

#[op2]
#[string]
pub(crate) fn op_gonvex_crypto_digest(
    #[string] algorithm: String,
    #[string] input: String,
) -> Result<String> {
    let input: Vec<u8> = serde_json::from_str(&input).map_err(|_| operation_error())?;
    let algorithm = match algorithm.as_str() {
        "SHA-1" => &digest::SHA1_FOR_LEGACY_USE_ONLY,
        "SHA-256" => &digest::SHA256,
        "SHA-384" => &digest::SHA384,
        "SHA-512" => &digest::SHA512,
        _ => return Err(unsupported()),
    };
    Ok(json!(digest::digest(algorithm, &input).as_ref()).to_string())
}

#[op2]
#[string]
pub(crate) fn op_gonvex_base64(encode: bool, #[string] input: String) -> Result<String> {
    if encode {
        let bytes = input
            .chars()
            .map(|value| u8::try_from(u32::from(value)))
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|_| error("InvalidCharacterError", "btoa requires a byte string"))?;
        Ok(general_purpose::STANDARD.encode(bytes))
    } else {
        let input = input
            .chars()
            .filter(|c| !matches!(c, '\t' | '\n' | '\u{000c}' | '\r' | ' '))
            .collect::<String>();
        if input.contains('=') && input.len() % 4 != 0 {
            return Err(error("InvalidCharacterError", "invalid base64 padding"));
        }
        let config = general_purpose::GeneralPurposeConfig::new()
            .with_decode_padding_mode(base64::engine::DecodePaddingMode::Indifferent)
            .with_decode_allow_trailing_bits(true);
        let bytes = general_purpose::GeneralPurpose::new(&base64::alphabet::STANDARD, config)
            .decode(input)
            .map_err(|_| error("InvalidCharacterError", "invalid base64"))?;
        Ok(bytes.into_iter().map(char::from).collect())
    }
}
