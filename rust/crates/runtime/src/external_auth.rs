//! Project-scoped external identity verification.

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use thiserror::Error;
use url::Url;

struct CachedKeys {
    expires_at: Instant,
    keys: Arc<JwkSet>,
}

fn key_cache() -> &'static tokio::sync::RwLock<BTreeMap<String, CachedKeys>> {
    static CACHE: OnceLock<tokio::sync::RwLock<BTreeMap<String, CachedKeys>>> = OnceLock::new();
    CACHE.get_or_init(|| tokio::sync::RwLock::new(BTreeMap::new()))
}

#[derive(Clone, Debug)]
pub struct ExternalAuthConfiguration {
    pub provider: String,
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub firebase_tenant_id: String,
    pub signup_mode: String,
    pub firebase_project_id: String,
    pub firebase_admin_credentials: Option<String>,
}

#[derive(Clone, Debug)]
pub struct VerifiedExternalIdentity {
    pub provider: String,
    pub issuer: String,
    pub subject: String,
    pub email: String,
    pub email_verified: bool,
    pub name: String,
    pub picture: String,
    pub sign_in_provider: String,
    pub auth_time: i64,
}

#[derive(Debug, Error)]
pub enum ExternalAuthError {
    #[error("external identity token is invalid")]
    InvalidToken,
    #[error("external identity token issuer is invalid")]
    WrongIssuer,
    #[error("external identity token audience is invalid")]
    WrongAudience,
    #[error("external identity token is expired")]
    Expired,
    #[error("external identity token subject is missing")]
    MissingSubject,
    #[error("Firebase tenant does not match this auth realm")]
    FirebaseTenantMismatch,
    #[error("external identity JWKS endpoint is unsafe")]
    UnsafeJwks,
    #[error("external identity keys are unavailable")]
    KeysUnavailable,
    #[error("Firebase account is disabled or its sessions were revoked")]
    FirebaseAccountRevoked,
    #[error("Firebase account status is unavailable")]
    FirebaseAccountUnavailable,
}

pub async fn verify_external_token(
    configuration: &ExternalAuthConfiguration,
    token: &str,
) -> Result<VerifiedExternalIdentity, ExternalAuthError> {
    if token.trim().is_empty() {
        return Err(ExternalAuthError::InvalidToken);
    }
    let header = decode_header(token).map_err(|_| ExternalAuthError::InvalidToken)?;
    if header.alg != Algorithm::RS256 {
        return Err(ExternalAuthError::InvalidToken);
    }
    let key_id = header.kid.ok_or(ExternalAuthError::InvalidToken)?;
    let mut set = jwks(&configuration.jwks_url, false).await?;
    if set.find(&key_id).is_none() {
        set = jwks(&configuration.jwks_url, true).await?;
    }
    let jwk = set.find(&key_id).ok_or(ExternalAuthError::InvalidToken)?;
    let key = DecodingKey::from_jwk(jwk).map_err(|_| ExternalAuthError::InvalidToken)?;
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[configuration.issuer.as_str()]);
    validation.set_audience(&[configuration.audience.as_str()]);
    validation.set_required_spec_claims(&["exp", "iat", "sub", "iss", "aud"]);
    validation.leeway = 30;
    let claims = decode::<Value>(token, &key, &validation).map_err(|error| {
        use jsonwebtoken::errors::ErrorKind;
        match error.kind() {
            ErrorKind::ExpiredSignature => ExternalAuthError::Expired,
            ErrorKind::InvalidIssuer => ExternalAuthError::WrongIssuer,
            ErrorKind::InvalidAudience => ExternalAuthError::WrongAudience,
            _ => ExternalAuthError::InvalidToken,
        }
    })?;
    let claims = claims.claims;
    let subject = claims
        .get("sub")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or(ExternalAuthError::MissingSubject)?;
    let now = chrono::Utc::now().timestamp();
    let issued_at = claims
        .get("iat")
        .and_then(Value::as_i64)
        .ok_or(ExternalAuthError::InvalidToken)?;
    if issued_at > now + 60 {
        return Err(ExternalAuthError::InvalidToken);
    }
    let firebase = claims.get("firebase").and_then(Value::as_object);
    let auth_time = if configuration.provider == "firebase" {
        let auth_time = claims
            .get("auth_time")
            .and_then(Value::as_i64)
            .ok_or(ExternalAuthError::InvalidToken)?;
        if auth_time > now + 60 {
            return Err(ExternalAuthError::InvalidToken);
        }
        let actual_tenant = firebase
            .and_then(|value| value.get("tenant"))
            .and_then(Value::as_str)
            .unwrap_or("");
        if actual_tenant != configuration.firebase_tenant_id {
            return Err(ExternalAuthError::FirebaseTenantMismatch);
        }
        auth_time
    } else {
        claims
            .get("auth_time")
            .and_then(Value::as_i64)
            .unwrap_or(issued_at)
    };
    let identity = VerifiedExternalIdentity {
        provider: configuration.provider.clone(),
        issuer: configuration.issuer.clone(),
        subject: subject.to_owned(),
        email: claims
            .get("email")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_lowercase(),
        email_verified: claims.get("email_verified").is_some_and(|value| {
            value.as_bool().unwrap_or_else(|| {
                value
                    .as_str()
                    .is_some_and(|value| value.eq_ignore_ascii_case("true"))
            })
        }),
        name: claims
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned(),
        picture: claims
            .get("picture")
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned(),
        sign_in_provider: firebase
            .and_then(|value| value.get("sign_in_provider"))
            .and_then(Value::as_str)
            .unwrap_or(&configuration.provider)
            .to_owned(),
        auth_time,
    };
    if let Some(credentials) = configuration.firebase_admin_credentials.as_deref() {
        verify_firebase_account(configuration, credentials, &identity).await?;
    }
    Ok(identity)
}

async fn jwks(url: &str, force: bool) -> Result<Arc<JwkSet>, ExternalAuthError> {
    if !force {
        if let Some(keys) = key_cache()
            .read()
            .await
            .get(url)
            .filter(|entry| entry.expires_at > Instant::now())
            .map(|entry| entry.keys.clone())
        {
            return Ok(keys);
        }
    }
    let (client, parsed) = safe_client(url).await?;
    let response = client
        .get(parsed)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|_| ExternalAuthError::KeysUnavailable)?
        .error_for_status()
        .map_err(|_| ExternalAuthError::KeysUnavailable)?;
    let cache_control = response
        .headers()
        .get(reqwest::header::CACHE_CONTROL)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    let max_age = if cache_control.split(',').any(|part| {
        matches!(
            part.trim().to_ascii_lowercase().as_str(),
            "no-store" | "no-cache"
        )
    }) {
        Duration::ZERO
    } else {
        cache_max_age(cache_control)
            .unwrap_or(Duration::from_secs(5 * 60))
            .clamp(Duration::from_secs(30), Duration::from_secs(60 * 60))
    };
    let keys = Arc::new(
        response
            .json::<JwkSet>()
            .await
            .map_err(|_| ExternalAuthError::KeysUnavailable)?,
    );
    if !max_age.is_zero() {
        key_cache().write().await.insert(
            url.to_owned(),
            CachedKeys {
                expires_at: Instant::now() + max_age,
                keys: keys.clone(),
            },
        );
    }
    Ok(keys)
}

#[derive(Deserialize)]
struct FirebaseAdminCredentials {
    project_id: String,
    client_email: String,
    private_key: String,
}

#[derive(Serialize)]
struct ServiceAccountClaims<'a> {
    iss: &'a str,
    scope: &'static str,
    aud: &'static str,
    iat: i64,
    exp: i64,
}

async fn verify_firebase_account(
    configuration: &ExternalAuthConfiguration,
    credentials_json: &str,
    identity: &VerifiedExternalIdentity,
) -> Result<(), ExternalAuthError> {
    let credentials: FirebaseAdminCredentials = serde_json::from_str(credentials_json)
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    if credentials.project_id != configuration.firebase_project_id
        || credentials.client_email.trim().is_empty()
        || credentials.private_key.trim().is_empty()
    {
        return Err(ExternalAuthError::FirebaseAccountUnavailable);
    }
    let now = chrono::Utc::now().timestamp();
    let assertion = encode(
        &Header::new(Algorithm::RS256),
        &ServiceAccountClaims {
            iss: &credentials.client_email,
            scope: "https://www.googleapis.com/auth/identitytoolkit",
            aud: "https://oauth2.googleapis.com/token",
            iat: now,
            exp: now + 300,
        },
        &EncodingKey::from_rsa_pem(credentials.private_key.as_bytes())
            .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?,
    )
    .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    let form = url::form_urlencoded::Serializer::new(String::new())
        .append_pair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
        .append_pair("assertion", &assertion)
        .finish();
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(8))
        .build()
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    let token_response = client
        .post("https://oauth2.googleapis.com/token")
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/x-www-form-urlencoded",
        )
        .body(form)
        .send()
        .await
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    if !token_response.status().is_success() {
        return Err(ExternalAuthError::FirebaseAccountUnavailable);
    }
    let access_token = token_response
        .json::<Value>()
        .await
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?
        .get("access_token")
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ExternalAuthError::FirebaseAccountUnavailable)?
        .to_owned();
    let lookup_url = format!(
        "https://identitytoolkit.googleapis.com/v1/projects/{}/accounts:lookup",
        url::form_urlencoded::byte_serialize(configuration.firebase_project_id.as_bytes())
            .collect::<String>()
    );
    let response = client
        .post(lookup_url)
        .bearer_auth(access_token)
        .json(&serde_json::json!({"localId":[identity.subject]}))
        .send()
        .await
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    if !response.status().is_success() {
        return Err(ExternalAuthError::FirebaseAccountUnavailable);
    }
    let body = response
        .json::<Value>()
        .await
        .map_err(|_| ExternalAuthError::FirebaseAccountUnavailable)?;
    let user = body
        .get("users")
        .and_then(Value::as_array)
        .and_then(|users| {
            users.iter().find(|user| {
                user.get("localId").and_then(Value::as_str) == Some(identity.subject.as_str())
            })
        })
        .ok_or(ExternalAuthError::FirebaseAccountRevoked)?;
    if user
        .get("disabled")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(ExternalAuthError::FirebaseAccountRevoked);
    }
    let valid_since = user
        .get("validSince")
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<i64>().ok())
        .unwrap_or(0);
    if identity.auth_time < valid_since {
        return Err(ExternalAuthError::FirebaseAccountRevoked);
    }
    Ok(())
}

fn cache_max_age(value: &str) -> Option<Duration> {
    value.split(',').find_map(|part| {
        part.trim()
            .strip_prefix("max-age=")
            .and_then(|seconds| seconds.parse::<u64>().ok())
            .map(Duration::from_secs)
    })
}

async fn safe_client(url: &str) -> Result<(reqwest::Client, Url), ExternalAuthError> {
    let url = Url::parse(url).map_err(|_| ExternalAuthError::UnsafeJwks)?;
    if url.scheme() != "https" || url.username() != "" || url.password().is_some() {
        return Err(ExternalAuthError::UnsafeJwks);
    }
    let host = url.host_str().ok_or(ExternalAuthError::UnsafeJwks)?;
    if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
        return Err(ExternalAuthError::UnsafeJwks);
    }
    let port = url.port_or_known_default().unwrap_or(443);
    let addresses = tokio::net::lookup_host((host, port))
        .await
        .map_err(|_| ExternalAuthError::KeysUnavailable)?
        .collect::<Vec<_>>();
    let address = addresses
        .into_iter()
        .find(|address| public_ip(address.ip()))
        .ok_or(ExternalAuthError::UnsafeJwks)?;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .resolve(host, SocketAddr::new(address.ip(), port))
        .build()
        .map_err(|_| ExternalAuthError::KeysUnavailable)?;
    Ok((client, url))
}

fn public_ip(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            !address.is_private()
                && !address.is_loopback()
                && !address.is_link_local()
                && !address.is_broadcast()
                && !address.is_documentation()
                && !address.is_unspecified()
        }
        IpAddr::V6(address) => {
            !address.is_loopback()
                && !address.is_unspecified()
                && !address.is_unique_local()
                && !address.is_unicast_link_local()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn private_addresses_are_rejected() {
        assert!(!public_ip("127.0.0.1".parse().unwrap()));
        assert!(!public_ip("10.2.3.4".parse().unwrap()));
        assert!(!public_ip("::1".parse().unwrap()));
        assert!(public_ip("8.8.8.8".parse().unwrap()));
    }

    #[test]
    fn jwks_cache_control_is_bounded_by_the_caller() {
        assert_eq!(
            cache_max_age("public, max-age=3600"),
            Some(Duration::from_secs(3600))
        );
        assert_eq!(cache_max_age("no-store"), None);
    }
}
