//! Tenant-scoped S3-compatible storage owned by the Rust host.
//!
//! Modules receive opaque file ids and host calls. They never receive bucket
//! credentials, object keys from another tenant, or a database selector.

use std::collections::BTreeMap;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use reqwest::{Method, Response};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::Row;
use url::Url;
use uuid::Uuid;

use crate::config::StorageConfig;
use crate::Runtime;
use gonvex_postgres::TenantSession;

const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
const MAX_DIRECT_STORE_BYTES: usize = 128 << 20;
const DEFAULT_UPLOAD_SECONDS: u64 = 15 * 60;
const DEFAULT_DOWNLOAD_SECONDS: u64 = 10 * 60;

const FILES_TABLE_DDL: &str = r#"CREATE TABLE IF NOT EXISTS _gonvex_files (
  id TEXT PRIMARY KEY,
  tenant_id TEXT NOT NULL,
  owner_id TEXT NOT NULL DEFAULT '',
  bucket TEXT NOT NULL,
  object_key TEXT NOT NULL,
  content_type TEXT NOT NULL DEFAULT '',
  size_bytes BIGINT NOT NULL DEFAULT 0,
  checksum TEXT NOT NULL DEFAULT '',
  visibility TEXT NOT NULL DEFAULT 'private',
  status TEXT NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  uploaded_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ
)"#;

#[derive(Clone)]
pub struct StorageManager {
    config: StorageConfig,
    client: reqwest::Client,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FileMetadata {
    id: String,
    tenant_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    owner_id: String,
    bucket: String,
    object_key: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    content_type: String,
    size: i64,
    #[serde(skip_serializing_if = "String::is_empty")]
    checksum: String,
    visibility: String,
    status: String,
    created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    uploaded_at: Option<DateTime<Utc>>,
}

#[derive(Default, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StorageRequest {
    #[serde(default)]
    file_id: String,
    #[serde(default)]
    ttl_ms: i64,
    #[serde(default)]
    content_base64: String,
    #[serde(default)]
    content_type: String,
    #[serde(default)]
    size: i64,
    #[serde(default)]
    visibility: String,
    #[serde(default)]
    owner_id: String,
    #[serde(default)]
    expires_ms: i64,
}

impl StorageManager {
    pub fn new(config: StorageConfig) -> Self {
        Self {
            config,
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("static storage client configuration is valid"),
        }
    }

    pub fn configured(&self) -> bool {
        self.config.configured()
    }

    pub async fn call(
        self,
        runtime: Runtime,
        session: TenantSession,
        operation: String,
        payload: Value,
    ) -> Result<Value, String> {
        self.require_configured()?;
        let control = runtime
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "Control Plane is unavailable".to_owned())?;
        let request: StorageRequest = if payload.is_null() {
            StorageRequest::default()
        } else {
            serde_json::from_value(payload)
                .map_err(|error| format!("invalid storage payload: {error}"))?
        };
        self.ensure_table(&control, &session).await?;
        match operation.trim() {
            "generateUploadUrl" => {
                let file_id = Uuid::new_v4().simple().to_string();
                let key = self.object_key(&session, &file_id);
                let visibility = visibility(&request.visibility);
                let owner = owner(&session, &request.owner_id);
                let mut transaction = control
                    .begin_tenant_transaction(&session.route, false)
                    .await
                    .map_err(|error| error.to_string())?;
                sqlx::query(r#"INSERT INTO _gonvex_files
                    (id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,visibility,status)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending')"#)
                    .bind(&file_id)
                    .bind(&session.route.tenant_id)
                    .bind(owner)
                    .bind(&self.config.bucket)
                    .bind(&key)
                    .bind(&request.content_type)
                    .bind(request.size.max(0))
                    .bind(visibility)
                    .execute(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
                transaction
                    .commit()
                    .await
                    .map_err(|error| error.to_string())?;
                let expires = millis_or_default(request.expires_ms, DEFAULT_UPLOAD_SECONDS);
                let (url, method) = if self.config.public_base_url.is_empty() {
                    (self.presign(Method::PUT, &key, expires)?, "PUT")
                } else {
                    (self.proxy_url(&key, expires, true), "POST")
                };
                let mut headers = BTreeMap::new();
                if !request.content_type.is_empty() {
                    headers.insert("Content-Type", request.content_type);
                }
                Ok(serde_json::json!({
                    "fileId": file_id,
                    "url": url,
                    "method": method,
                    "headers": headers,
                }))
            }
            "getUrl" => {
                let metadata = self
                    .metadata(&control, &session, &request.file_id, true)
                    .await?;
                let url = if !self.config.public_base_url.is_empty() {
                    self.proxy_url(&metadata.object_key, DEFAULT_DOWNLOAD_SECONDS, false)
                } else if metadata.visibility == "public" {
                    self.object_url(&metadata.object_key)?.to_string()
                } else {
                    self.presign(Method::GET, &metadata.object_key, DEFAULT_DOWNLOAD_SECONDS)?
                };
                Ok(Value::String(url))
            }
            "generateDownloadUrl" => {
                let metadata = self
                    .metadata(&control, &session, &request.file_id, false)
                    .await?;
                let expires = millis_or_default(request.ttl_ms, DEFAULT_DOWNLOAD_SECONDS);
                let url = if self.config.public_base_url.is_empty() {
                    self.presign(Method::GET, &metadata.object_key, expires)?
                } else {
                    self.proxy_url(&metadata.object_key, expires, false)
                };
                Ok(Value::String(url))
            }
            "getMetadata" => serde_json::to_value(
                self.metadata(&control, &session, &request.file_id, true)
                    .await?,
            )
            .map_err(|error| error.to_string()),
            "delete" => {
                let metadata = self
                    .metadata(&control, &session, &request.file_id, false)
                    .await?;
                self.object_request(Method::DELETE, &metadata.object_key, Vec::new(), "")
                    .await?;
                let mut transaction = control
                    .begin_tenant_transaction(&session.route, false)
                    .await
                    .map_err(|error| error.to_string())?;
                sqlx::query("DELETE FROM _gonvex_files WHERE id=$1 AND tenant_id=$2")
                    .bind(&request.file_id)
                    .bind(&session.route.tenant_id)
                    .execute(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
                transaction
                    .commit()
                    .await
                    .map_err(|error| error.to_string())?;
                Ok(Value::Null)
            }
            "store" => {
                let content = STANDARD
                    .decode(request.content_base64.as_bytes())
                    .map_err(|_| "storage content is not valid base64".to_owned())?;
                if content.len() > MAX_DIRECT_STORE_BYTES {
                    return Err(format!(
                        "storage content exceeds the {} byte limit",
                        MAX_DIRECT_STORE_BYTES
                    ));
                }
                let file_id = Uuid::new_v4().simple().to_string();
                let key = self.object_key(&session, &file_id);
                self.object_request(Method::PUT, &key, content.clone(), &request.content_type)
                    .await?;
                let checksum = hex_sha256(&content);
                let visibility = visibility(&request.visibility);
                let owner = owner(&session, &request.owner_id);
                let mut transaction = control
                    .begin_tenant_transaction(&session.route, false)
                    .await
                    .map_err(|error| error.to_string())?;
                let row = sqlx::query(r#"INSERT INTO _gonvex_files
                    (id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,checksum,visibility,status,uploaded_at)
                    VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'uploaded',now())
                    RETURNING id,tenant_id,owner_id,bucket,object_key,content_type,size_bytes,
                              checksum,visibility,status,created_at,uploaded_at"#)
                    .bind(&file_id)
                    .bind(&session.route.tenant_id)
                    .bind(owner)
                    .bind(&self.config.bucket)
                    .bind(&key)
                    .bind(&request.content_type)
                    .bind(i64::try_from(content.len()).unwrap_or(i64::MAX))
                    .bind(checksum)
                    .bind(visibility)
                    .fetch_one(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
                let metadata = metadata_from_row(&row);
                transaction
                    .commit()
                    .await
                    .map_err(|error| error.to_string())?;
                serde_json::to_value(metadata).map_err(|error| error.to_string())
            }
            other => Err(format!("unknown storage operation {other:?}")),
        }
    }

    pub async fn read_file(
        self,
        runtime: Runtime,
        session: TenantSession,
        file_id: String,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, FileMetadata), String> {
        self.require_configured()?;
        let control = runtime
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "Control Plane is unavailable".to_owned())?;
        self.ensure_table(&control, &session).await?;
        let metadata = self.metadata(&control, &session, &file_id, true).await?;
        if metadata.visibility == "private" && metadata.owner_id != session.identity.account.id {
            return Err("file not found".to_owned());
        }
        if metadata.status != "uploaded" {
            return Err("file is not uploaded".to_owned());
        }
        let response = self
            .signed_request(Method::GET, &metadata.object_key, Vec::new(), "")?
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if !response.status().is_success() {
            return Err(format!("object storage returned {}", response.status()));
        }
        if response.content_length().unwrap_or_default() > max_bytes as u64 {
            return Err("stored file exceeds the sandbox import limit".to_owned());
        }
        let bytes = response.bytes().await.map_err(|error| error.to_string())?;
        if bytes.len() > max_bytes {
            return Err("stored file exceeds the sandbox import limit".to_owned());
        }
        Ok((bytes.to_vec(), metadata))
    }

    pub fn verify_proxy(&self, key: &str, expires: i64, signature: &str, upload: bool) -> bool {
        if expires <= Utc::now().timestamp() || signature.is_empty() {
            return false;
        }
        let expected = self.proxy_signature(key, expires, upload);
        constant_time(expected.as_bytes(), signature.as_bytes())
    }

    pub async fn proxy_get(&self, key: &str, range: Option<&str>) -> Result<Response, String> {
        let mut request = self.signed_request(Method::GET, key, Vec::new(), "")?;
        if let Some(range) = range.filter(|range| !range.trim().is_empty()) {
            request = request.header("range", range);
        }
        request.send().await.map_err(|error| error.to_string())
    }

    pub async fn proxy_put(
        &self,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), String> {
        self.object_request(Method::PUT, key, body, content_type)
            .await
    }

    pub async fn list_project_files(
        &self,
        runtime: &Runtime,
        project_id: &str,
        limit: usize,
    ) -> Result<Vec<Value>, String> {
        self.require_configured()?;
        let control = runtime
            .inner
            .control_plane
            .read()
            .await
            .clone()
            .ok_or_else(|| "Control Plane is unavailable".to_owned())?;
        let mut files = Vec::new();
        for route in control
            .tenant_routes(project_id)
            .await
            .map_err(|error| error.to_string())?
        {
            let mut transaction = control
                .begin_tenant_transaction(&route, true)
                .await
                .map_err(|error| error.to_string())?;
            let exists: bool =
                sqlx::query_scalar("SELECT to_regclass('public._gonvex_files') IS NOT NULL")
                    .fetch_one(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
            if !exists {
                transaction
                    .commit()
                    .await
                    .map_err(|error| error.to_string())?;
                continue;
            }
            let remaining = i64::try_from(limit.saturating_sub(files.len())).unwrap_or(i64::MAX);
            if remaining == 0 {
                transaction
                    .commit()
                    .await
                    .map_err(|error| error.to_string())?;
                break;
            }
            let rows = sqlx::query(
                r#"SELECT id,object_key,size_bytes,content_type,created_at,uploaded_at
                   FROM _gonvex_files WHERE deleted_at IS NULL AND status='uploaded'
                   ORDER BY COALESCE(uploaded_at,created_at) DESC LIMIT $1"#,
            )
            .bind(remaining)
            .fetch_all(&mut **transaction.transaction())
            .await
            .map_err(|error| error.to_string())?;
            for row in rows {
                let key = row.get::<String, _>("object_key");
                files.push(serde_json::json!({
                    "id":row.get::<String,_>("id"),
                    "key":key,
                    "size":row.get::<i64,_>("size_bytes"),
                    "contentType":row.get::<String,_>("content_type"),
                    "uploadedAt":row.get::<Option<DateTime<Utc>>,_>("uploaded_at")
                        .unwrap_or_else(||row.get::<DateTime<Utc>,_>("created_at")),
                    "url":self.proxy_url(&key,DEFAULT_DOWNLOAD_SECONDS,false),
                }));
            }
            transaction
                .commit()
                .await
                .map_err(|error| error.to_string())?;
        }
        files.sort_by(|left, right| {
            right
                .get("uploadedAt")
                .and_then(Value::as_str)
                .cmp(&left.get("uploadedAt").and_then(Value::as_str))
        });
        files.truncate(limit);
        Ok(files)
    }

    fn require_configured(&self) -> Result<(), String> {
        self.configured()
            .then_some(())
            .ok_or_else(|| "storage is not configured".to_owned())
    }

    async fn ensure_table(
        &self,
        control: &gonvex_postgres::ControlPlane,
        session: &TenantSession,
    ) -> Result<(), String> {
        let mut transaction = control
            .begin_tenant_transaction(&session.route, false)
            .await
            .map_err(|error| error.to_string())?;
        for statement in [
            FILES_TABLE_DDL,
            "CREATE INDEX IF NOT EXISTS _gonvex_files_tenant_idx ON _gonvex_files (tenant_id)",
            "CREATE INDEX IF NOT EXISTS _gonvex_files_owner_idx ON _gonvex_files (owner_id)",
        ] {
            sqlx::query(statement)
                .execute(&mut **transaction.transaction())
                .await
                .map_err(|error| error.to_string())?;
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())
    }

    async fn metadata(
        &self,
        control: &gonvex_postgres::ControlPlane,
        session: &TenantSession,
        file_id: &str,
        finalize: bool,
    ) -> Result<FileMetadata, String> {
        if file_id.trim().is_empty() {
            return Err("fileId is required".to_owned());
        }
        let mut transaction = control
            .begin_tenant_transaction(&session.route, false)
            .await
            .map_err(|error| error.to_string())?;
        let row = sqlx::query(
            r#"SELECT id,tenant_id,owner_id,bucket,object_key,content_type,
                    size_bytes,checksum,visibility,status,created_at,uploaded_at
                FROM _gonvex_files WHERE id=$1 AND tenant_id=$2"#,
        )
        .bind(file_id)
        .bind(&session.route.tenant_id)
        .fetch_optional(&mut **transaction.transaction())
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| "file not found".to_owned())?;
        let mut metadata = metadata_from_row(&row);
        if finalize && metadata.status == "pending" {
            if let Some((size, content_type, checksum)) = self.head(&metadata.object_key).await? {
                let row = sqlx::query(r#"UPDATE _gonvex_files SET status='uploaded',size_bytes=$2,
                        content_type=COALESCE(NULLIF($3,''),content_type),checksum=$4,uploaded_at=now()
                    WHERE id=$1 RETURNING id,tenant_id,owner_id,bucket,object_key,content_type,
                        size_bytes,checksum,visibility,status,created_at,uploaded_at"#)
                    .bind(file_id)
                    .bind(size)
                    .bind(content_type)
                    .bind(checksum)
                    .fetch_one(&mut **transaction.transaction())
                    .await
                    .map_err(|error| error.to_string())?;
                metadata = metadata_from_row(&row);
            }
        }
        transaction
            .commit()
            .await
            .map_err(|error| error.to_string())?;
        Ok(metadata)
    }

    async fn head(&self, key: &str) -> Result<Option<(i64, String, String)>, String> {
        let response = self
            .signed_request(Method::HEAD, key, Vec::new(), "")?
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(format!("object storage returned {}", response.status()));
        }
        let size = response
            .headers()
            .get("content-length")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .unwrap_or(0);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .to_owned();
        let checksum = response
            .headers()
            .get("etag")
            .and_then(|value| value.to_str().ok())
            .unwrap_or("")
            .trim_matches('"')
            .to_owned();
        Ok(Some((size, content_type, checksum)))
    }

    async fn object_request(
        &self,
        method: Method,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<(), String> {
        let response = self
            .signed_request(method, key, body, content_type)?
            .send()
            .await
            .map_err(|error| error.to_string())?;
        if response.status().is_success() || response.status() == reqwest::StatusCode::NOT_FOUND {
            Ok(())
        } else {
            Err(format!("object storage returned {}", response.status()))
        }
    }

    fn signed_request(
        &self,
        method: Method,
        key: &str,
        body: Vec<u8>,
        content_type: &str,
    ) -> Result<reqwest::RequestBuilder, String> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let url = self.object_url(key)?;
        let host = url_host(&url);
        let payload_hash = if body.is_empty() {
            EMPTY_SHA256.to_owned()
        } else {
            hex_sha256(&body)
        };
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method.as_str(),
            url.path(),
            canonical_headers,
            signed_headers,
            payload_hash
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.config.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex_sha256(canonical_request.as_bytes())
        );
        let signature = self.sign(&date, string_to_sign.as_bytes());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers}, Signature={}",
            self.config.access_key_id,
            hex(&signature)
        );
        let mut request = self
            .client
            .request(method, url)
            .header("host", host)
            .header("x-amz-content-sha256", payload_hash)
            .header("x-amz-date", amz_date)
            .header("authorization", authorization);
        if !content_type.is_empty() {
            request = request.header("content-type", content_type);
        }
        if !body.is_empty() {
            request = request.body(body);
        }
        Ok(request)
    }

    fn presign(&self, method: Method, key: &str, seconds: u64) -> Result<String, String> {
        let now = Utc::now();
        let amz_date = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date = now.format("%Y%m%d").to_string();
        let mut url = self.object_url(key)?;
        let host = url_host(&url);
        let scope = format!("{date}/{}/s3/aws4_request", self.config.region);
        let mut query = BTreeMap::from([
            ("X-Amz-Algorithm", "AWS4-HMAC-SHA256".to_owned()),
            (
                "X-Amz-Credential",
                format!("{}/{}", self.config.access_key_id, scope),
            ),
            ("X-Amz-Date", amz_date.clone()),
            ("X-Amz-Expires", seconds.clamp(1, 604_800).to_string()),
            ("X-Amz-SignedHeaders", "host".to_owned()),
        ]);
        let canonical_query = query
            .iter()
            .map(|(key, value)| format!("{}={}", aws_encode(key), aws_encode(value)))
            .collect::<Vec<_>>()
            .join("&");
        let canonical_request = format!(
            "{}\n{}\n{}\nhost:{}\n\nhost\nUNSIGNED-PAYLOAD",
            method.as_str(),
            url.path(),
            canonical_query,
            host
        );
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex_sha256(canonical_request.as_bytes())
        );
        query.insert(
            "X-Amz-Signature",
            hex(&self.sign(&date, string_to_sign.as_bytes())),
        );
        url.set_query(Some(
            &query
                .iter()
                .map(|(key, value)| format!("{}={}", aws_encode(key), aws_encode(value)))
                .collect::<Vec<_>>()
                .join("&"),
        ));
        Ok(url.to_string())
    }

    fn sign(&self, date: &str, message: &[u8]) -> Vec<u8> {
        let date_key = hmac_sha256(
            format!("AWS4{}", self.config.secret_access_key).as_bytes(),
            date.as_bytes(),
        );
        let region_key = hmac_sha256(&date_key, self.config.region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"s3");
        let signing_key = hmac_sha256(&service_key, b"aws4_request");
        hmac_sha256(&signing_key, message)
    }

    fn object_url(&self, key: &str) -> Result<Url, String> {
        let mut endpoint = Url::parse(self.config.endpoint.trim_end_matches('/'))
            .map_err(|error| format!("invalid S3 endpoint: {error}"))?;
        let path = if self.config.force_path_style {
            format!("/{}/{}", aws_encode(&self.config.bucket), encode_key(key))
        } else {
            let host = endpoint
                .host_str()
                .ok_or_else(|| "S3 endpoint has no host".to_owned())?;
            endpoint
                .set_host(Some(&format!("{}.{}", self.config.bucket, host)))
                .map_err(|_| "invalid S3 bucket host".to_owned())?;
            format!("/{}", encode_key(key))
        };
        endpoint.set_path(&path);
        Ok(endpoint)
    }

    fn object_key(&self, session: &TenantSession, file_id: &str) -> String {
        format!(
            "{}/{}/{}",
            safe_segment(&session.identity.project_id),
            safe_segment(&session.route.tenant_id),
            file_id
        )
    }

    fn proxy_url(&self, key: &str, seconds: u64, upload: bool) -> String {
        let expires = Utc::now().timestamp() + i64::try_from(seconds).unwrap_or(i64::MAX);
        let signature = self.proxy_signature(key, expires, upload);
        format!(
            "{}/storage/{}?exp={expires}&sig={signature}{}",
            self.config.public_base_url.trim_end_matches('/'),
            encode_key(key),
            if upload { "&upload=1" } else { "" }
        )
    }

    fn proxy_signature(&self, key: &str, expires: i64, upload: bool) -> String {
        let domain = if upload {
            "gonvex-storage-upload:"
        } else {
            "gonvex-storage:"
        };
        hex(&hmac_sha256(
            format!("{domain}{}", self.config.secret_access_key).as_bytes(),
            format!("{key}\n{expires}").as_bytes(),
        ))
    }
}

fn metadata_from_row(row: &sqlx::postgres::PgRow) -> FileMetadata {
    FileMetadata {
        id: row.get("id"),
        tenant_id: row.get("tenant_id"),
        owner_id: row.get("owner_id"),
        bucket: row.get("bucket"),
        object_key: row.get("object_key"),
        content_type: row.get("content_type"),
        size: row.get("size_bytes"),
        checksum: row.get("checksum"),
        visibility: row.get("visibility"),
        status: row.get("status"),
        created_at: row.get("created_at"),
        uploaded_at: row.get("uploaded_at"),
    }
}

fn visibility(value: &str) -> &'static str {
    match value.trim() {
        "tenant" => "tenant",
        "public" => "public",
        _ => "private",
    }
}

fn owner(session: &TenantSession, requested: &str) -> String {
    if requested.trim().is_empty() {
        session.identity.account.id.clone()
    } else {
        requested.trim().to_owned()
    }
}

fn millis_or_default(milliseconds: i64, default_seconds: u64) -> u64 {
    if milliseconds <= 0 {
        default_seconds
    } else {
        u64::try_from(milliseconds)
            .unwrap_or(u64::MAX)
            .div_ceil(1_000)
    }
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts arbitrary key lengths");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn hex_sha256(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn aws_encode(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (byte as char).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
}

fn encode_key(value: &str) -> String {
    value
        .split('/')
        .map(aws_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn safe_segment(value: &str) -> String {
    value
        .trim()
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '_'
            }
        })
        .collect::<String>()
        .trim_matches(|character| character == '_' || character == '.')
        .to_owned()
}

fn url_host(url: &Url) -> String {
    let host = url.host_str().unwrap_or_default();
    match url.port() {
        Some(port) => format!("{host}:{port}"),
        None => host.to_owned(),
    }
}

fn constant_time(left: &[u8], right: &[u8]) -> bool {
    use subtle::ConstantTimeEq as _;
    left.len() == right.len() && bool::from(left.ct_eq(right))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_keys_are_tenant_namespaced_and_paths_are_encoded() {
        assert_eq!(encode_key("project/a b/file"), "project/a%20b/file");
        assert_eq!(safe_segment("../tenant one"), "tenant_one");
    }

    #[test]
    fn proxy_signatures_are_method_scoped_and_constant_time_checked() {
        let manager = StorageManager::new(StorageConfig {
            secret_access_key: "secret".to_owned(),
            ..StorageConfig::default()
        });
        let get = manager.proxy_signature("p/t/f", 100, false);
        let put = manager.proxy_signature("p/t/f", 100, true);
        assert_ne!(get, put);
        assert!(constant_time(get.as_bytes(), get.as_bytes()));
    }
}
