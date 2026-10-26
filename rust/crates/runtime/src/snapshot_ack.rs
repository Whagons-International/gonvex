//! Sequence admission for a current snapshot. Delivery owns the frame credits.

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
