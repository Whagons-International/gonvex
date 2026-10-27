//! Full-range Duration nanoseconds. Native Duration and clock adapters live in budget.rs.
// Keep these plain Rust bodies extractable by verification/verus/verify_budgets.py.

/// `(u64::MAX + 1) * 1_000_000_000 - 1`, exactly Duration::MAX.as_nanos().
pub(crate) const MAX_DURATION_NANOS: u128 = 18_446_744_073_709_551_615_999_999_999;

/// Invalid numeric inputs fail before subtraction, including inputs beyond Duration::MAX.
pub(crate) fn remaining_nanos(invocation: Option<u128>, elapsed: u128) -> Result<Option<u128>, ()> {
    if elapsed > MAX_DURATION_NANOS {
        return Err(());
    }
    match invocation {
        None => Ok(None),
        Some(limit) => {
            if limit > MAX_DURATION_NANOS || elapsed >= limit {
                Err(())
            } else {
                Ok(Some(limit - elapsed))
            }
        }
    }
}

/// The execution ceiling starts at dispatch, so elapsed only spends invocation budget.
pub(crate) fn execution_nanos(
    invocation: Option<u128>,
    ceiling: Option<u128>,
    elapsed: u128,
) -> Result<Option<u128>, ()> {
    if let Some(limit) = ceiling {
        if limit > MAX_DURATION_NANOS {
            return Err(());
        }
    }
    let remaining = match remaining_nanos(invocation, elapsed) {
        Ok(value) => value,
        Err(()) => return Err(()),
    };
    Ok(match (ceiling, remaining) {
        (None, None) => None,
        (Some(limit), None) | (None, Some(limit)) => Some(limit),
        (Some(limit), Some(left)) => Some(if limit < left { limit } else { left }),
    })
}
