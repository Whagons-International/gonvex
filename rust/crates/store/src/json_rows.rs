//! The row type of streamed snapshot and live-query reads.
//!
//! A row is the UTF-8 JSON text SQLite produced for it (`json_object(...)`
//! over the collection's columns, `rows::json_object_expr`), exactly as
//! stored in the result column: valid JSON, not re-encoded and not parsed.
//! Rows arrive in [`JsonRowBatch`]es (one allocation per batch of up to 128
//! rows / 256 KiB) from [`JsonRows`], which `TenantTransaction::stream_json_rows`
//! returns. Consumers borrow each row as `&[u8]` and decide how to
//! materialize it (`serde_json::from_slice`, a raw-value wrapper, or copying
//! the bytes into an encode-once frame). NULL rows are skipped.

use std::sync::Arc;

use crate::DatabaseError;

/// Rows of a query's first column (JSON text, e.g. from `json_object`),
/// packed into one buffer. NULL rows are skipped.
#[derive(Debug, Default)]
pub struct JsonRowBatch {
    data: Vec<u8>,
    ends: Vec<usize>,
}

impl JsonRowBatch {
    pub(crate) fn with_capacity(rows: usize, bytes: usize) -> Self {
        Self {
            data: Vec::with_capacity(bytes),
            ends: Vec::with_capacity(rows),
        }
    }

    pub(crate) fn push(&mut self, row: &[u8]) {
        self.data.extend_from_slice(row);
        self.ends.push(self.data.len());
    }

    pub fn len(&self) -> usize {
        self.ends.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ends.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.data.len()
    }

    /// Each row's JSON text, in query order.
    pub fn rows(&self) -> impl Iterator<Item = &[u8]> + '_ {
        let mut start = 0;
        self.ends.iter().map(move |end| {
            let row = &self.data[start..*end];
            start = *end;
            row
        })
    }
}

/// Row batches from [`TenantTransaction::stream_json_rows`].
pub struct JsonRows<'t> {
    pub(crate) receiver: tokio::sync::mpsc::Receiver<Result<JsonRowBatch, DatabaseError>>,
    pub(crate) stop: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) _transaction: std::marker::PhantomData<&'t mut crate::TenantTransaction>,
}

impl JsonRows<'_> {
    /// The next batch; `None` after the last row.
    pub async fn next_batch(&mut self) -> Option<Result<JsonRowBatch, DatabaseError>> {
        self.receiver.recv().await
    }
}

impl Drop for JsonRows<'_> {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        self.receiver.close();
    }
}
