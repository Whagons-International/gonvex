//! Bounded local framing.
//!
//! Every frame is a 4-byte big-endian length followed by exactly that many
//! bytes of UTF-8 JSON. The length prefix is what makes the protocol bounded:
//! a reader knows a frame's size before it allocates for it and refuses
//! anything past `max_frame_bytes` instead of growing until the process dies.
//! Nothing here is line-delimited, so a JSON string containing a newline is
//! ordinary data rather than a framing hazard.

use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Generous enough for a large module bundle, small enough that one bad frame
/// cannot exhaust the host's memory.
pub const DEFAULT_MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

const HEADER_BYTES: usize = 4;

#[derive(Debug, thiserror::Error)]
pub enum FrameError {
    #[error("module host peer closed the connection")]
    Closed,
    #[error("frame of {size} bytes exceeds the {limit} byte frame limit")]
    TooLarge { size: usize, limit: usize },
    #[error("frame is not valid JSON: {0}")]
    Malformed(String),
    #[error("module host transport failed: {0}")]
    Io(#[from] io::Error),
}

impl FrameError {
    /// True once the connection can no longer be used, so the caller stops
    /// reading instead of spinning on a broken stream.
    pub fn is_fatal(&self) -> bool {
        !matches!(self, Self::Malformed(_))
    }
}

pub async fn read_frame<R>(reader: &mut R, limit: usize) -> Result<Vec<u8>, FrameError>
where
    R: AsyncRead + Unpin,
{
    let mut header = [0u8; HEADER_BYTES];
    if let Err(err) = reader.read_exact(&mut header).await {
        return Err(match err.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Closed,
            _ => FrameError::Io(err),
        });
    }
    let size = u32::from_be_bytes(header) as usize;
    // Reject before allocation. Oversized payloads are deliberately not drained.
    checked_frame_length(size, limit)?;
    let mut payload = vec![0u8; size];
    if let Err(err) = reader.read_exact(&mut payload).await {
        return Err(match err.kind() {
            io::ErrorKind::UnexpectedEof => FrameError::Closed,
            _ => FrameError::Io(err),
        });
    }
    Ok(payload)
}

pub async fn write_frame<W>(writer: &mut W, payload: &[u8], limit: usize) -> Result<(), FrameError>
where
    W: AsyncWrite + Unpin,
{
    let length = checked_frame_length(payload.len(), limit)?;
    let header = length.to_be_bytes();
    writer.write_all(&header).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

#[inline]
fn frame_length(size: usize, limit: usize) -> Option<u32> {
    let wire_limit = limit.min(u32::MAX as usize);
    if size == 0 || size > wire_limit {
        None
    } else {
        Some(size as u32)
    }
}

fn checked_frame_length(size: usize, limit: usize) -> Result<u32, FrameError> {
    match frame_length(size, limit) {
        Some(length) => Ok(length),
        None if size == 0 => Err(FrameError::Malformed("frame is empty".to_owned())),
        None => Err(FrameError::TooLarge {
            size,
            limit: limit.min(u32::MAX as usize),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[test]
    fn frame_lengths_fit_the_wire_and_reader_contract() {
        assert!(matches!(
            checked_frame_length(0, 64),
            Err(FrameError::Malformed(_))
        ));
        assert_eq!(checked_frame_length(64, 64).unwrap(), 64);
        assert!(matches!(
            checked_frame_length(65, 64),
            Err(FrameError::TooLarge { .. })
        ));
        assert_eq!(
            checked_frame_length(u32::MAX as usize, usize::MAX).unwrap(),
            u32::MAX
        );
        #[cfg(target_pointer_width = "64")]
        assert!(matches!(
            checked_frame_length(u32::MAX as usize + 1, usize::MAX),
            Err(FrameError::TooLarge { .. })
        ));
    }

    #[tokio::test]
    async fn rejected_frame_writes_no_bytes() {
        for (payload, limit) in [(&b""[..], 64), (&b"oversized"[..], 1)] {
            let (mut writer, mut reader) = tokio::io::duplex(64);
            let result = write_frame(&mut writer, payload, limit).await;
            assert!(if payload.is_empty() {
                matches!(result, Err(FrameError::Malformed(_)))
            } else {
                matches!(result, Err(FrameError::TooLarge { .. }))
            });
            writer.shutdown().await.unwrap();
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.is_empty());
        }
    }

    #[tokio::test]
    async fn rejected_header_never_polls_the_payload() {
        struct HeaderOnly {
            header: [u8; 4],
            read: bool,
        }
        impl AsyncRead for HeaderOnly {
            fn poll_read(
                self: std::pin::Pin<&mut Self>,
                _: &mut std::task::Context<'_>,
                buffer: &mut tokio::io::ReadBuf<'_>,
            ) -> std::task::Poll<io::Result<()>> {
                let reader = self.get_mut();
                assert!(
                    !reader.read,
                    "invalid headers must not request payload bytes"
                );
                buffer.put_slice(&reader.header);
                reader.read = true;
                std::task::Poll::Ready(Ok(()))
            }
        }
        for size in [0_u32, 65] {
            let mut reader = HeaderOnly {
                header: size.to_be_bytes(),
                read: false,
            };
            let result = read_frame(&mut reader, 64).await;
            assert!(if size == 0 {
                matches!(result, Err(FrameError::Malformed(_)))
            } else {
                matches!(
                    result,
                    Err(FrameError::TooLarge {
                        size: 65,
                        limit: 64
                    })
                )
            });
        }
    }

    #[tokio::test]
    async fn admitted_json_round_trips_with_newlines_and_unicode() {
        let (mut writer, mut reader) = tokio::io::duplex(256);
        let payload = "{\"message\":\"café\\nready\"}".as_bytes();
        write_frame(&mut writer, payload, payload.len())
            .await
            .unwrap();
        assert_eq!(
            read_frame(&mut reader, payload.len()).await.unwrap(),
            payload
        );
    }
}
