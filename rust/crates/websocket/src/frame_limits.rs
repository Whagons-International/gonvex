//! Enforce the existing per-frame bound before Ratchet allocates a payload.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

/// Larger masked data frames reach Ratchet as fragments of at most this many
/// payload bytes. Ratchet keeps its frame read buffer at the size of the
/// largest frame it has read, for the life of the connection; re-fragmenting
/// bounds that buffer. Fragments are a multiple of four bytes, so every
/// fragment starts at mask offset zero and keeps the client's masking key.
pub(crate) const READ_FRAGMENT: usize = 4096;

/// Writes smaller than this are coalesced before reaching the socket.
pub(crate) const WRITE_COALESCE: usize = 4096;

pub(crate) struct FrameLimits<S> {
    inner: S,
    write_buf: Vec<u8>,
    written: usize,
    max_frame: usize,
    /// The wire header being parsed.
    header: [u8; 14],
    read: usize,
    header_len: usize,
    /// The header handed to Ratchet: the wire header or a fragment header.
    out: [u8; 14],
    out_len: usize,
    sent: usize,
    /// Payload bytes left in the frame Ratchet is reading.
    remaining: u64,
    /// Payload bytes of the wire frame after the current fragment.
    rest: u64,
    fin: bool,
    mask: [u8; 4],
}

impl<S> FrameLimits<S> {
    pub(crate) fn new(inner: S, max_frame: usize) -> Self {
        Self {
            inner,
            write_buf: Vec::new(),
            written: 0,
            max_frame,
            header: [0; 14],
            read: 0,
            header_len: 2,
            out: [0; 14],
            out_len: 0,
            sent: 0,
            remaining: 0,
            rest: 0,
            fin: false,
            mask: [0; 4],
        }
    }

    /// Emit a header for `length` payload bytes, masked with the wire key.
    fn fragment(&mut self, first: u8, length: u64) {
        self.out[0] = first;
        let mut len = 2;
        if length < 126 {
            self.out[1] = 0x80 | length as u8;
        } else if length <= u16::MAX as u64 {
            self.out[1] = 0x80 | 126;
            self.out[2..4].copy_from_slice(&(length as u16).to_be_bytes());
            len = 4;
        } else {
            self.out[1] = 0x80 | 127;
            self.out[2..10].copy_from_slice(&length.to_be_bytes());
            len = 10;
        }
        self.out[len..len + 4].copy_from_slice(&self.mask);
        self.out_len = len + 4;
        self.remaining = length;
    }

    /// Start the next continuation fragment of a split wire frame.
    fn next_fragment(&mut self) {
        let length = self.rest.min(READ_FRAGMENT as u64);
        self.rest -= length;
        let fin = if self.rest == 0 && self.fin { 0x80 } else { 0 };
        self.fragment(fin, length);
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for FrameLimits<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        dst: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if dst.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if this.sent < this.out_len {
                let count = dst.remaining().min(this.out_len - this.sent);
                dst.put_slice(&this.out[this.sent..this.sent + count]);
                this.sent += count;
                if this.sent == this.out_len {
                    this.sent = 0;
                    this.out_len = 0;
                }
                return Poll::Ready(Ok(()));
            }
            if this.remaining > 0 {
                let limit = dst.remaining().min(this.remaining as usize);
                let mut child = ReadBuf::new(dst.initialize_unfilled_to(limit));
                return match Pin::new(&mut this.inner).poll_read(cx, &mut child) {
                    Poll::Ready(Ok(())) => {
                        let n = child.filled().len();
                        this.remaining -= n as u64;
                        dst.advance(n);
                        Poll::Ready(Ok(()))
                    }
                    result => result,
                };
            }
            if this.rest > 0 {
                this.next_fragment();
                continue;
            }
            while this.read < this.header_len {
                let mut child = ReadBuf::new(&mut this.header[this.read..this.header_len]);
                match Pin::new(&mut this.inner).poll_read(cx, &mut child) {
                    Poll::Ready(Ok(())) => {
                        let n = child.filled().len();
                        if n == 0 {
                            return Poll::Ready(if this.read == 0 {
                                Ok(())
                            } else {
                                Err(io::ErrorKind::UnexpectedEof.into())
                            });
                        }
                        this.read += n;
                    }
                    result => return result,
                }
                if this.header_len == 2 && this.read == 2 {
                    let first = this.header[0];
                    let second = this.header[1];
                    // RSV1 is defined only for the first data frame of a message.
                    if first & 0x40 != 0 && (first & 0x0f == 0 || first & 0x08 != 0) {
                        return Poll::Ready(Err(io::Error::other(
                            "RSV1 on control or continuation",
                        )));
                    }
                    this.header_len += match second & 0x7f {
                        126 => 2,
                        127 => 8,
                        _ => 0,
                    };
                    if second & 0x80 != 0 {
                        this.header_len += 4;
                    }
                }
            }
            let header_len = this.header_len;
            this.read = 0;
            this.header_len = 2;
            let length = match this.header[1] & 0x7f {
                126 => u16::from_be_bytes(this.header[2..4].try_into().unwrap()) as u64,
                127 => u64::from_be_bytes(this.header[2..10].try_into().unwrap()),
                n => n as u64,
            };
            if length > this.max_frame as u64 {
                return Poll::Ready(Err(io::Error::other("WebSocket frame exceeds limit")));
            }
            let first = this.header[0];
            let masked = this.header[1] & 0x80 != 0;
            // Only masked data frames are split; anything else reaches Ratchet
            // unchanged and keeps Ratchet's own validation and errors.
            if masked && first & 0x0f <= 2 && length > READ_FRAGMENT as u64 {
                this.mask
                    .copy_from_slice(&this.header[header_len - 4..header_len]);
                this.fin = first & 0x80 != 0;
                this.rest = length - READ_FRAGMENT as u64;
                this.fragment(first & 0x7f, READ_FRAGMENT as u64);
            } else {
                this.out[..header_len].copy_from_slice(&this.header[..header_len]);
                this.out_len = header_len;
                this.remaining = length;
            }
        }
    }
}

impl<S: AsyncWrite + Unpin> FrameLimits<S> {
    /// Write out coalesced bytes, then free the buffer: an idle connection
    /// holds no write buffer.
    fn poll_write_buffer(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        while self.written < self.write_buf.len() {
            match Pin::new(&mut self.inner).poll_write(cx, &self.write_buf[self.written..]) {
                Poll::Ready(Ok(0)) => return Poll::Ready(Err(io::ErrorKind::WriteZero.into())),
                Poll::Ready(Ok(n)) => self.written += n,
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => return Poll::Pending,
            }
        }
        self.written = 0;
        self.write_buf = Vec::new();
        Poll::Ready(Ok(()))
    }
}

// Same policy as `tokio::io::BufWriter::with_capacity(WRITE_COALESCE)`: small
// frame headers and payloads go out in one write (no delayed-ACK stall), large
// payloads bypass the buffer. Unlike BufWriter, the buffer exists only while
// a frame is being written.
impl<S: AsyncWrite + Unpin> AsyncWrite for FrameLimits<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if this.write_buf.len() + data.len() > WRITE_COALESCE {
            match this.poll_write_buffer(cx) {
                Poll::Ready(Ok(())) => {}
                other => return other.map(|result| result.map(|()| 0)),
            }
        }
        if data.len() >= WRITE_COALESCE {
            return Pin::new(&mut this.inner).poll_write(cx, data);
        }
        if this.write_buf.capacity() == 0 {
            this.write_buf.reserve_exact(WRITE_COALESCE);
        }
        this.write_buf.extend_from_slice(data);
        Poll::Ready(Ok(data.len()))
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.poll_write_buffer(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.inner).poll_flush(cx),
            other => other,
        }
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match this.poll_write_buffer(cx) {
            Poll::Ready(Ok(())) => Pin::new(&mut this.inner).poll_shutdown(cx),
            other => other,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn headers_are_bounded_before_reading_payload() {
        for header in [
            vec![0x81, 126, 0x10, 0x01],
            vec![0x81, 127, 0, 0, 0, 1, 0, 0, 0, 0],
            vec![0xc9, 0],
            vec![0xc0, 0],
        ] {
            let mut stream = FrameLimits::new(header.as_slice(), 4096);
            assert!(stream.read_u8().await.is_err());
        }
    }

    #[tokio::test]
    async fn small_reads_preserve_headers_and_frame_boundaries() {
        let bytes = [
            0x81, 0x82, 1, 2, 3, 4, b'x', b'y', 0x89, 0x80, 1, 2, 3, 4, 0x81, 0,
        ];
        let mut stream = FrameLimits::new(bytes.as_slice(), 4096);
        for byte in bytes {
            assert_eq!(stream.read_u8().await.unwrap(), byte);
        }
        assert!(stream.read_u8().await.is_err());
    }

    #[tokio::test]
    async fn large_masked_data_frames_are_refragmented_with_the_same_key() {
        let key = [9, 8, 7, 6];
        let payload = (0..READ_FRAGMENT * 2 + 5)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<_>>();
        let mut wire = vec![0xc1, 0x80 | 126];
        wire.extend_from_slice(&(payload.len() as u16).to_be_bytes());
        wire.extend_from_slice(&key);
        wire.extend(payload.iter().enumerate().map(|(i, b)| b ^ key[i % 4]));
        wire.extend_from_slice(&[0x89, 0x80, 1, 2, 3, 4]);
        let mut stream = FrameLimits::new(wire.as_slice(), 1 << 20);
        let mut output = Vec::new();
        stream.read_to_end(&mut output).await.unwrap();
        let mut frames = Vec::new();
        let mut at = 0;
        while at < output.len() {
            let first = output[at];
            let (length, offset) = match output[at + 1] & 0x7f {
                126 => (
                    u16::from_be_bytes([output[at + 2], output[at + 3]]) as usize,
                    4,
                ),
                n => (n as usize, 2),
            };
            assert_ne!(output[at + 1] & 0x80, 0, "fragments stay masked");
            let mask = &output[at + offset..at + offset + 4];
            let data = output[at + offset + 4..at + offset + 4 + length]
                .iter()
                .enumerate()
                .map(|(i, b)| b ^ mask[i % 4])
                .collect::<Vec<_>>();
            frames.push((first, data));
            at += offset + 4 + length;
        }
        let headers = frames.iter().map(|(first, _)| *first).collect::<Vec<_>>();
        // RSV1 and the opcode stay on the first fragment, FIN on the last.
        assert_eq!(headers, [0x41, 0x00, 0x80, 0x89]);
        assert_eq!(frames[0].1.len(), READ_FRAGMENT);
        assert_eq!(frames[2].1.len(), 5);
        let joined = frames[..3]
            .iter()
            .flat_map(|(_, data)| data.clone())
            .collect::<Vec<_>>();
        assert_eq!(joined, payload);
    }

    #[tokio::test]
    async fn writes_coalesce_small_frames_and_free_the_buffer_after_flush() {
        use tokio::io::AsyncWriteExt;
        let mut stream = FrameLimits::new(Vec::<u8>::new(), 4096);
        stream.write_all(&[0x81, 3]).await.unwrap();
        stream.write_all(b"abc").await.unwrap();
        assert!(stream.inner.is_empty(), "header and payload are coalesced");
        stream.flush().await.unwrap();
        assert_eq!(stream.inner, [0x81, 3, b'a', b'b', b'c']);
        assert_eq!(stream.write_buf.capacity(), 0);
        let large = vec![7; WRITE_COALESCE + 1];
        stream.write_all(&[0x82, 126, 0x10, 0x01]).await.unwrap();
        stream.write_all(&large).await.unwrap();
        stream.flush().await.unwrap();
        assert_eq!(stream.inner.len(), 5 + 4 + large.len());
        assert_eq!(&stream.inner[5..9], [0x82, 126, 0x10, 0x01]);
        assert_eq!(stream.write_buf.capacity(), 0);
    }
}
