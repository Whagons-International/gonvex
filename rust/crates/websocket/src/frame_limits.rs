//! Enforce the existing per-frame bound before Ratchet allocates a payload.
use std::{
    io,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

pub(crate) struct FrameLimits<S> {
    inner: S,
    max_frame: usize,
    header: [u8; 14],
    read: usize,
    sent: usize,
    header_len: usize,
    remaining: u64,
}

impl<S> FrameLimits<S> {
    pub(crate) fn new(inner: S, max_frame: usize) -> Self {
        Self {
            inner,
            max_frame,
            header: [0; 14],
            read: 0,
            sent: 0,
            header_len: 2,
            remaining: 0,
        }
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
        if this.remaining > 0 {
            let limit = dst.remaining().min(this.remaining as usize);
            let mut child = ReadBuf::new(dst.initialize_unfilled_to(limit));
            match Pin::new(&mut this.inner).poll_read(cx, &mut child) {
                Poll::Ready(Ok(())) => {
                    let n = child.filled().len();
                    this.remaining -= n as u64;
                    dst.advance(n);
                    return Poll::Ready(Ok(()));
                }
                result => return result,
            }
        }
        loop {
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
            }
            if this.header_len == 2 {
                let first = this.header[0];
                let second = this.header[1];
                // RSV1 is defined only for the first data frame of a message.
                if first & 0x40 != 0 && (first & 0x0f == 0 || first & 0x08 != 0) {
                    return Poll::Ready(Err(io::Error::other("RSV1 on control or continuation")));
                }
                this.header_len += match second & 0x7f {
                    126 => 2,
                    127 => 8,
                    _ => 0,
                };
                if second & 0x80 != 0 {
                    this.header_len += 4;
                }
                if this.read < this.header_len {
                    continue;
                }
            }
            break;
        }
        let length = match this.header[1] & 0x7f {
            126 => u16::from_be_bytes(this.header[2..4].try_into().unwrap()) as u64,
            127 => u64::from_be_bytes(this.header[2..10].try_into().unwrap()),
            n => n as u64,
        };
        if length > this.max_frame as u64 {
            return Poll::Ready(Err(io::Error::other("WebSocket frame exceeds limit")));
        }
        let count = dst.remaining().min(this.header_len - this.sent);
        dst.put_slice(&this.header[this.sent..this.sent + count]);
        this.sent += count;
        if this.sent == this.header_len {
            this.remaining = length;
            this.sent = 0;
            this.read = 0;
            this.header_len = 2;
        }
        Poll::Ready(Ok(()))
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FrameLimits<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, data)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
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
}
