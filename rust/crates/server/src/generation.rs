// Keep allocation arithmetic independent of registry locks and engine effects.
// verification/verus/verify_generation.py checks this exact function body.
pub(crate) fn next_generation(issued: u64, active: u64, requested: Option<u64>) -> Option<u64> {
    let floor = if issued > active { issued } else { active };
    match requested {
        Some(value) => {
            if value > floor {
                Some(value)
            } else {
                None
            }
        }
        None => {
            if floor == u64::MAX {
                None
            } else {
                Some(floor + 1)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::next_generation;

    #[test]
    fn boundaries_preserve_both_high_water_marks() {
        assert_eq!(next_generation(0, 0, None), Some(1));
        assert_eq!(next_generation(10, 20, None), Some(21));
        assert_eq!(next_generation(20, 10, Some(20)), None);
        assert_eq!(next_generation(20, 10, Some(21)), Some(21));
        assert_eq!(next_generation(u64::MAX - 1, 0, None), Some(u64::MAX));
        assert_eq!(next_generation(u64::MAX, 0, None), None);
        assert_eq!(next_generation(0, u64::MAX, None), None);
        assert_eq!(next_generation(0, 0, Some(0)), None);
    }
}
