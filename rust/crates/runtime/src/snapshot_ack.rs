//! Snapshot ACK progress, retained-credit release and frame admission.

use std::collections::VecDeque;

pub(crate) struct AckProgress {
    pub(crate) sent: Option<u64>,
    pub(crate) acked: Option<u64>,
}

/// Return whether a valid cumulative ACK advances the acknowledged sequence.
/// Invalid and repeated ACKs leave both progress fields unchanged.
#[inline]
pub(crate) fn record_ack(progress: &mut AckProgress, sequence: u64) -> Result<bool, ()> {
    match progress.sent {
        None => return Err(()),
        Some(sent) if sequence > sent => return Err(()),
        Some(_) => {}
    }
    match progress.acked {
        Some(acked) if sequence <= acked => return Ok(false),
        _ => {}
    }
    progress.acked = Some(sequence);
    Ok(true)
}

/// Remove exactly the leading records covered by a cumulative durable ACK.
/// Both current and retired snapshot queues use this operation. The caller
/// validates the ACK's identity and sent high-water mark before releasing it.
// Pinned Verus has VecDeque len/index/pop_front specs, but no is_empty/front
// specs. Keep the ordinary prefix loop and allow only this one len comparison.
#[allow(clippy::len_zero)]
pub(crate) fn release_acked_prefix(frames: &mut VecDeque<(u64, usize)>, sequence: u64) {
    while frames.len() > 0 {
        let (sent, _) = frames[0];
        if sent > sequence {
            break;
        }
        frames.pop_front();
    }
}

/// Return exact post-admission credits, or reject without overflowing usize.
/// Limits are parameters so the policy remains explicit at its call site.
pub(crate) fn admit_frame(
    count: usize,
    bytes: usize,
    frame_bytes: usize,
    frame_limit: usize,
    byte_limit: usize,
) -> Option<(usize, usize)> {
    if count >= frame_limit || bytes > byte_limit {
        return None;
    }
    if frame_bytes > byte_limit - bytes {
        return None;
    }
    Some((count + 1, bytes + frame_bytes))
}
