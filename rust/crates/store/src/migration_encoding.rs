//! Versioned, bounded migration SQL encoding shared by artifact ingestion and
//! the cold migration-body archive. Checksums always describe decoded SQL.
use std::borrow::Cow;
use std::io::Write;

use base64::{engine::general_purpose::STANDARD, Engine as _};

pub const MAX_DECODED_BYTES: usize = 64 << 20;
const PREFIX: &str = "deflate-raw-v1:";

pub fn decode_file(encoded: &str, remaining: &mut usize) -> Result<Vec<u8>, String> {
    let bytes = if let Some(compressed) = encoded.strip_prefix("deflate-raw-v1:") {
        let (length, payload) = compressed
            .split_once(':')
            .ok_or("missing decoded migration length")?;
        if length.is_empty() || !length.bytes().all(|b| b.is_ascii_digit()) {
            return Err("invalid decoded migration length".into());
        }
        let length: usize = length
            .parse()
            .map_err(|_| "invalid decoded migration length")?;
        if length > *remaining {
            return Err("decoded migrations exceed the artifact byte budget".into());
        }
        let compressed = STANDARD.decode(payload).map_err(|e| e.to_string())?;
        // One extra byte detects a false length without allowing unbounded expansion.
        let mut output = Vec::with_capacity(length + 1);
        let mut decoder = flate2::Decompress::new(false);
        let status = decoder
            .decompress_vec(&compressed, &mut output, flate2::FlushDecompress::Finish)
            .map_err(|e| format!("invalid compressed migration: {e}"))?;
        if status != flate2::Status::StreamEnd
            || decoder.total_in() != compressed.len() as u64
            || output.len() != length
        {
            return Err("compressed migration must have exactly its declared length and one complete stream".into());
        }
        output
    } else {
        STANDARD.decode(encoded).map_err(|e| e.to_string())?
    };
    *remaining = remaining
        .checked_sub(bytes.len())
        .ok_or("decoded migrations exceed the artifact byte budget")?;
    Ok(bytes)
}

/// Keep readable SQL when encoding would grow it. Stored plain SQL remains
/// compatible; a compressed body uses the same explicit envelope as artifacts.
pub(crate) fn encode_stored(sql: &str) -> Result<Cow<'_, str>, String> {
    if sql.len() > MAX_DECODED_BYTES {
        return Err("decoded migrations exceed the artifact byte budget".into());
    }
    let mut encoder = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder
        .write_all(sql.as_bytes())
        .map_err(|e| e.to_string())?;
    let body = encoder.finish().map_err(|e| e.to_string())?;
    let encoded = format!("{PREFIX}{}:{}", sql.len(), STANDARD.encode(body));
    Ok(if encoded.len() < sql.len() {
        Cow::Owned(encoded)
    } else {
        Cow::Borrowed(sql)
    })
}

pub(crate) fn decode_stored(sql: String, remaining: &mut usize) -> Result<String, String> {
    if sql.starts_with(PREFIX) {
        String::from_utf8(decode_file(&sql, remaining)?)
            .map_err(|_| "migration must be UTF-8".into())
    } else {
        *remaining = remaining
            .checked_sub(sql.len())
            .ok_or("decoded migrations exceed the artifact byte budget")?;
        Ok(sql)
    }
}
