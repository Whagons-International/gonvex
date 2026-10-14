//! Bounded raw DEFLATE decoding, allocated only at the end of a message.
use flate2::{Decompress, FlushDecompress, Status};
use std::io;

pub(crate) fn inflate_message(input: &[u8], window: u8, limit: usize) -> io::Result<Vec<u8>> {
    // A 9-bit inflater can receive an 8-bit peer window. flate2 has no 8-bit API.
    let mut codec = Decompress::new_with_window_bits(false, window.max(9));
    let mut output = Vec::new();
    let mut open = false;
    for (part, trailer) in [(input, false), (&[0, 0, 255, 255][..], true)] {
        if trailer && !open {
            return Err(io::Error::other("missing deflate sync block"));
        }
        let mut offset = 0;
        loop {
            let mut scratch = [0; 8192];
            let writable = limit
                .saturating_sub(output.len())
                .saturating_add(1)
                .min(scratch.len());
            let before_in = codec.total_in();
            let before_out = codec.total_out();
            let status = codec
                .decompress(
                    &part[offset..],
                    &mut scratch[..writable],
                    FlushDecompress::None,
                )
                .map_err(io::Error::other)?;
            let consumed = (codec.total_in() - before_in) as usize;
            let written = (codec.total_out() - before_out) as usize;
            if written > limit.saturating_sub(output.len()) {
                return Err(io::Error::other("expanded message exceeds limit"));
            }
            output.extend_from_slice(&scratch[..written]);
            offset += consumed;
            if status == Status::StreamEnd {
                // RFC 7692 permits BFINAL streams inside one message. Their
                // padded next block starts a new raw stream, not a new message.
                if trailer {
                    return Err(io::Error::other("final block in deflate trailer"));
                }
                codec = Decompress::new_with_window_bits(false, window.max(9));
                open = false;
            } else if consumed > 0 {
                open = true;
            }
            if consumed == 0 && written == 0 {
                if offset != part.len() {
                    return Err(io::Error::other("inflater stalled"));
                }
                break;
            }
        }
    }
    // Complete a synthetic empty final block. A valid PMD sync-flush boundary
    // consumes it without output and reaches StreamEnd; partial streams fail.
    let before_in = codec.total_in();
    let before_out = codec.total_out();
    let status = codec
        .decompress(&[1, 0, 0, 255, 255], &mut [0; 1], FlushDecompress::Finish)
        .map_err(io::Error::other)?;
    if status != Status::StreamEnd
        || codec.total_in() - before_in != 5
        || codec.total_out() != before_out
    {
        return Err(io::Error::other("incomplete deflate message"));
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arbitrary_payloads_never_panic_or_exceed_the_limit() {
        let mut seed = 42u32;
        for size in (0..128).cycle().take(4096) {
            let mut input = Vec::with_capacity(size);
            for _ in 0..size {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                input.push(seed as u8);
            }
            if let Ok(decoded) = inflate_message(&input, 8 + (seed % 8) as u8, 128) {
                assert!(decoded.len() <= 128);
            }
        }
    }

    #[test]
    fn rfc_vectors_and_malformed_streams() {
        assert_eq!(inflate_message(&[0], 15, 0).unwrap(), b"");
        assert_eq!(
            inflate_message(&[0xf2, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00], 15, 5).unwrap(),
            b"Hello"
        );
        let final_stream = [0xf3, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x00];
        assert_eq!(inflate_message(&final_stream, 15, 5).unwrap(), b"Hello");
        let mut combined = final_stream[..7].to_vec();
        combined.extend_from_slice(&final_stream);
        assert_eq!(inflate_message(&combined, 15, 10).unwrap(), b"HelloHello");
        for bad in [&[][..], &[255][..], &final_stream[..7], &[0xf2, 0x48][..]] {
            assert!(inflate_message(bad, 15, 100).is_err(), "{bad:?}");
        }
        assert!(inflate_message(&final_stream, 15, 4).is_err());
    }
}
