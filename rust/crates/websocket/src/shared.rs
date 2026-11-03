//! Encode once, compress once, one frame per connection per tick.
//!
//! The server negotiates `server_no_context_takeover` for every
//! permessage-deflate agreement (`negotiation.rs`), so a message's compressed
//! bytes depend only on its payload, the server window and the compression
//! level, never on what the connection sent before. A [`SharedText`] is a JSON
//! segment encoded once and shared (`Arc`) by every connection that sends it;
//! its DEFLATE form is computed at most once per negotiated window.
//!
//! A batched message is assembled from segments. Each compressed segment is a
//! complete, byte-aligned run of non-final DEFLATE blocks ending with the
//! sync-flush empty stored block, and it never references bytes before its own
//! start, so segments concatenate into one valid raw DEFLATE stream (RFC 1951
//! 3.2.3/3.2.4): the inflater's window may contain earlier segments, but no
//! distance reaches them. Small per-connection segments (envelope, separators,
//! subscription bindings) are written as stored blocks. The assembled message
//! ends with an empty stored block whose `00 00 ff ff` tail is removed, exactly
//! the RFC 7692 7.2.1 shape a single sync-flushed message has.
use bytes::{Bytes, BytesMut};
use flate2::{Compress, Compression, FlushCompress};
use std::{cell::RefCell, io, sync::Arc, sync::OnceLock};

pub(crate) const COMPRESSION_LEVEL: u32 = 1;
/// Windows `negotiation.rs` can select for the server (9..=12); 13..=15 are
/// accepted here for benchmarks and future negotiation.
const WINDOWS: usize = 7;
const SYNC_TRAILER: [u8; 4] = [0, 0, 255, 255];

thread_local! {
    // One reusable compressor per window and thread: `reset` restores the
    // fresh state without the ~370 KiB allocation of a new zlib stream.
    static COMPRESSORS: RefCell<[Option<Compress>; WINDOWS]> = const { RefCell::new([None, None, None, None, None, None, None]) };
}

fn window_index(window_bits: u8) -> io::Result<usize> {
    match window_bits {
        9..=15 => Ok(usize::from(window_bits - 9)),
        _ => Err(io::Error::other("unsupported DEFLATE window")),
    }
}

/// Appends `payload` as one no-context-takeover DEFLATE segment, keeping the
/// sync-flush trailer (`00 00 ff ff`). Uses this thread's reusable compressor.
pub(crate) fn deflate_segment(
    payload: &[u8],
    window_bits: u8,
    out: &mut Vec<u8>,
) -> io::Result<()> {
    let index = window_index(window_bits)?;
    COMPRESSORS.with(|cell| {
        let mut compressors = cell.borrow_mut();
        let codec = compressors[index].get_or_insert_with(|| {
            Compress::new_with_window_bits(Compression::new(COMPRESSION_LEVEL), false, window_bits)
        });
        codec.reset();
        let result = deflate_with(codec, payload, out);
        if result.is_err() {
            // Never reuse a stream whose state is unknown.
            compressors[index] = None;
        }
        result
    })
}

pub(crate) fn deflate_with(
    codec: &mut Compress,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> io::Result<()> {
    let start = out.len();
    out.reserve(payload.len() / 3 + 64);
    let mut consumed = 0usize;
    loop {
        if out.capacity() - out.len() < 64 {
            out.reserve(out.capacity().max(4096));
        }
        let before_in = codec.total_in();
        let before_out = codec.total_out();
        codec
            .compress_vec(&payload[consumed..], out, FlushCompress::Sync)
            .map_err(io::Error::other)?;
        consumed += (codec.total_in() - before_in) as usize;
        let written = (codec.total_out() - before_out) as usize;
        // A sync flush is complete once all input is consumed and the
        // compressor stopped with output space left (or had nothing left).
        if consumed == payload.len() && (out.len() < out.capacity() || written == 0) {
            break;
        }
        if written == 0 && codec.total_in() == before_in && out.len() < out.capacity() {
            return Err(io::Error::other("compressor stalled"));
        }
    }
    if !out[start..].ends_with(&SYNC_TRAILER) {
        return Err(io::Error::other("missing deflate trailer"));
    }
    Ok(())
}

/// Appends `raw` as byte-aligned non-final stored blocks (RFC 1951 3.2.4).
pub(crate) fn stored_blocks(raw: &[u8], out: &mut Vec<u8>) {
    for chunk in raw.chunks(u16::MAX as usize) {
        let len = chunk.len() as u16;
        out.push(0);
        out.extend_from_slice(&len.to_le_bytes());
        out.extend_from_slice(&(!len).to_le_bytes());
        out.extend_from_slice(chunk);
    }
}

/// A JSON text segment encoded once and shared by every recipient.
pub struct SharedText {
    text: Bytes,
    deflated: [OnceLock<Bytes>; WINDOWS],
}

impl std::fmt::Debug for SharedText {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedText")
            .field("len", &self.text.len())
            .finish()
    }
}

impl SharedText {
    /// `text` is UTF-8 (a `String`), so plain transports never re-validate it.
    pub fn new(text: String) -> Self {
        Self {
            text: Bytes::from(text),
            deflated: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.text.len()
    }

    pub fn is_empty(&self) -> bool {
        self.text.is_empty()
    }

    pub fn text(&self) -> &[u8] {
        &self.text
    }

    /// Heap bytes held: the text plus every compressed form built so far.
    pub fn heap_bytes(&self) -> usize {
        self.text.len()
            + self
                .deflated
                .iter()
                .filter_map(OnceLock::get)
                .map(Bytes::len)
                .sum::<usize>()
    }

    /// Number of compressed forms built (0 until a compressed transport sends it).
    pub fn compressed_forms(&self) -> usize {
        self.deflated
            .iter()
            .filter(|form| form.get().is_some())
            .count()
    }

    /// The segment's DEFLATE form for `window_bits`, built at most once. Ends
    /// with the sync trailer. Falls back to stored blocks when compression does
    /// not help or fails.
    pub fn deflated(&self, window_bits: u8) -> &Bytes {
        let index = window_index(window_bits).expect("negotiated window is 9..=15");
        self.deflated[index].get_or_init(|| {
            let mut out = Vec::new();
            match deflate_segment(&self.text, window_bits, &mut out) {
                Ok(()) if out.len() < self.text.len() + 5 => {}
                _ => {
                    out.clear();
                    stored_blocks(&self.text, &mut out);
                    out.extend_from_slice(&[0, 0, 0, 255, 255]);
                }
            }
            Bytes::from(out)
        })
    }
}

/// One piece of a batched message.
#[derive(Clone, Debug)]
pub enum Segment {
    /// Encoded once, shared by every connection that sends it.
    Shared(Arc<SharedText>),
    /// Small, connection-specific bytes (envelope, separators, bindings).
    Inline(Bytes),
}

impl Segment {
    pub fn len(&self) -> usize {
        match self {
            Self::Shared(text) => text.len(),
            Self::Inline(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn raw(&self) -> &[u8] {
        match self {
            Self::Shared(text) => text.text(),
            Self::Inline(bytes) => bytes,
        }
    }
}

/// A message being assembled from segments, sent as one WebSocket text frame.
#[derive(Debug, Default)]
struct PendingBatch {
    segments: Vec<Segment>,
    tail: Option<Bytes>,
    suffix: &'static [u8],
    raw_len: usize,
}

impl PendingBatch {
    fn total_len(&self) -> usize {
        self.raw_len + self.tail.as_ref().map_or(0, Bytes::len) + self.suffix.len()
    }

    fn parts(&self) -> impl Iterator<Item = &[u8]> {
        self.segments
            .iter()
            .map(Segment::raw)
            .chain(self.tail.as_deref())
            .chain(std::iter::once(self.suffix))
    }

    /// The uncompressed message text.
    fn text(&self) -> BytesMut {
        let mut text = BytesMut::with_capacity(self.total_len());
        for part in self.parts() {
            text.extend_from_slice(part);
        }
        text
    }

    /// The compressed message body (RSV1 set), assembled from shared
    /// compressed segments without compressing anything per connection.
    fn deflated(&self, window_bits: u8) -> BytesMut {
        let mut out = Vec::with_capacity(self.total_len() / 2 + 64);
        let mut inline = Vec::new();
        for segment in &self.segments {
            match segment {
                Segment::Shared(text) => {
                    stored_blocks(&inline, &mut out);
                    inline.clear();
                    out.extend_from_slice(text.deflated(window_bits));
                }
                Segment::Inline(bytes) => inline.extend_from_slice(bytes),
            }
        }
        if let Some(tail) = &self.tail {
            inline.extend_from_slice(tail);
        }
        inline.extend_from_slice(self.suffix);
        stored_blocks(&inline, &mut out);
        // Final empty stored block, minus the `00 00 ff ff` the receiver appends.
        out.push(0);
        BytesMut::from(&out[..])
    }
}

/// A batched message ready for the wire.
#[derive(Debug)]
pub struct BatchFrame {
    pub bytes: BytesMut,
    /// `bytes` is a permessage-deflate body (send with RSV1).
    pub compressed: bool,
}

/// At most one message being assembled from segments.
#[derive(Debug, Default)]
pub struct OutboundBatch {
    pending: Option<PendingBatch>,
}

impl OutboundBatch {
    pub fn is_open(&self) -> bool {
        self.pending.is_some()
    }

    /// Uncompressed bytes of the pending message (0 when none).
    pub fn len(&self) -> usize {
        self.pending.as_ref().map_or(0, PendingBatch::total_len)
    }

    pub fn is_empty(&self) -> bool {
        self.pending.is_none()
    }

    /// Starts a message: `prefix` now, `suffix` when it is taken. The caller
    /// pushes segments that make the whole text valid JSON.
    pub fn open(&mut self, prefix: Bytes, suffix: &'static [u8]) {
        debug_assert!(self.pending.is_none(), "a batch is already open");
        self.pending = Some(PendingBatch {
            raw_len: prefix.len(),
            segments: vec![Segment::Inline(prefix)],
            tail: None,
            suffix,
        });
    }

    pub fn push(&mut self, segment: Segment) {
        let batch = self.pending.as_mut().expect("open before push");
        batch.raw_len += segment.len();
        batch.segments.push(segment);
    }

    /// Replaces the bytes written after every segment and before the suffix.
    pub fn set_tail(&mut self, tail: Option<Bytes>) {
        self.pending.as_mut().expect("open before set_tail").tail = tail;
    }

    /// Takes the pending message. `deflate` is the connection's negotiated
    /// (server window, threshold); below the threshold the text is sent plain.
    pub fn take_frame(&mut self, deflate: Option<(u8, usize)>) -> Option<BatchFrame> {
        let batch = self.pending.take()?;
        Some(match deflate {
            Some((window_bits, threshold)) if batch.total_len() >= threshold => BatchFrame {
                bytes: batch.deflated(window_bits),
                compressed: true,
            },
            _ => BatchFrame {
                bytes: batch.text(),
                compressed: false,
            },
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inflate::inflate_message;

    fn batch(segments: Vec<Segment>, tail: Option<&'static str>) -> PendingBatch {
        PendingBatch {
            raw_len: segments.iter().map(Segment::len).sum(),
            segments,
            tail: tail.map(|tail| Bytes::from_static(tail.as_bytes())),
            suffix: b"]}",
        }
    }

    #[test]
    fn assembled_segments_inflate_to_the_concatenated_text() {
        let shared = Arc::new(SharedText::new(
            serde_json::json!({"type":"replica.delta","rows":["x".repeat(3000), "y".repeat(500)]})
                .to_string(),
        ));
        let tiny = Arc::new(SharedText::new("{}".to_owned()));
        let incompressible = Arc::new(SharedText::new(
            serde_json::to_string(
                &(0..4000u32)
                    .map(|i| char::from(b'!' + (i.wrapping_mul(2654435761) >> 25) as u8 % 90))
                    .collect::<String>(),
            )
            .unwrap(),
        ));
        for window in 9..=15 {
            let pending = batch(
                vec![
                    Segment::Inline(Bytes::from_static(b"{\"items\":[")),
                    Segment::Shared(shared.clone()),
                    Segment::Inline(Bytes::from_static(b",")),
                    Segment::Shared(tiny.clone()),
                    Segment::Inline(Bytes::from_static(b",")),
                    Segment::Shared(incompressible.clone()),
                    Segment::Inline(Bytes::from_static(b",")),
                    Segment::Shared(shared.clone()),
                ],
                Some(",{\"type\":\"replica.watermark\",\"revision\":9}"),
            );
            let text = pending.text();
            let compressed = pending.deflated(window);
            assert!(compressed.len() < text.len());
            assert_eq!(
                inflate_message(&compressed, window, 1 << 20).unwrap(),
                text.to_vec()
            );
            serde_json::from_slice::<serde_json::Value>(&text).unwrap();
        }
        // Each window was compressed once and is reused.
        assert_eq!(shared.compressed_forms(), 7);
    }

    #[test]
    fn segment_compression_matches_a_fresh_compressor() {
        let payload = "abc".repeat(5000);
        for window in 9..=15 {
            let mut first = Vec::new();
            deflate_segment(payload.as_bytes(), window, &mut first).unwrap();
            let mut fresh =
                Compress::new_with_window_bits(Compression::new(COMPRESSION_LEVEL), false, window);
            let mut expected = Vec::new();
            deflate_with(&mut fresh, payload.as_bytes(), &mut expected).unwrap();
            // A reset compressor produces the same bytes as a new one.
            let mut second = Vec::new();
            deflate_segment(payload.as_bytes(), window, &mut second).unwrap();
            assert_eq!(first, expected);
            assert_eq!(second, expected);
            // Same bytes as the pre-reuse implementation (8 KiB scratch loop).
            let mut codec =
                Compress::new_with_window_bits(Compression::new(COMPRESSION_LEVEL), false, window);
            let mut old = Vec::new();
            let mut scratch = [0; 8192];
            loop {
                let before_in = codec.total_in();
                let before_out = codec.total_out();
                codec
                    .compress(
                        &payload.as_bytes()[before_in as usize..],
                        &mut scratch,
                        FlushCompress::Sync,
                    )
                    .unwrap();
                let written = (codec.total_out() - before_out) as usize;
                old.extend_from_slice(&scratch[..written]);
                if codec.total_in() as usize == payload.len() && written < scratch.len() {
                    break;
                }
            }
            assert_eq!(old, expected);
        }
    }

    #[test]
    fn large_inline_runs_split_into_stored_blocks() {
        let big = Bytes::from(vec![b'a'; 200_000]);
        let pending = batch(vec![Segment::Inline(big)], None);
        let compressed = pending.deflated(12);
        assert_eq!(
            inflate_message(&compressed, 12, 1 << 20).unwrap(),
            pending.text().to_vec()
        );
    }
}
