use std::time::{Duration, Instant, SystemTime};

use gonvex_module_runtime::ModuleError;

#[path = "budget_core.rs"]
mod numeric;

// Verified core outputs are bounded by Duration::MAX. Native as_nanos/new and
// this conversion remain an explicit boundary, checked by adapter regressions.
fn duration_from_nanos(nanos: u128) -> Duration {
    debug_assert!(nanos <= numeric::MAX_DURATION_NANOS);
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
}

/// The absolute invocation deadline is captured once, before queue admission.
/// Later wall-clock changes cannot give the call a fresh deadline. The
/// configured execution ceiling starts when an isolate is ready to dispatch.
pub(crate) struct CallBudget {
    admitted: Instant,
    invocation: Option<Duration>,
    ceiling: Option<Duration>,
}

fn expired() -> ModuleError {
    ModuleError::BudgetExceeded("invocation deadline elapsed before the module started".to_owned())
}

impl CallBudget {
    pub(crate) fn capture(
        deadline: Option<SystemTime>,
        ceiling: Option<Duration>,
    ) -> Result<Self, ModuleError> {
        let admitted = Instant::now();
        let invocation = deadline
            .map(|deadline| {
                deadline
                    .duration_since(SystemTime::now())
                    .map_err(|_| expired())
            })
            .transpose()?;
        Ok(Self {
            admitted,
            invocation,
            ceiling,
        })
    }

    fn remaining_after(&self, elapsed: Duration) -> Result<Option<Duration>, ModuleError> {
        numeric::remaining_nanos(
            self.invocation.map(|value| value.as_nanos()),
            elapsed.as_nanos(),
        )
        .map(|remaining| remaining.map(duration_from_nanos))
        .map_err(|()| expired())
    }

    pub(crate) fn invocation_remaining(&self) -> Result<Option<Duration>, ModuleError> {
        self.remaining_after(self.admitted.elapsed())
    }

    fn execution_after(&self, elapsed: Duration) -> Result<Option<Duration>, ModuleError> {
        numeric::execution_nanos(
            self.invocation.map(|value| value.as_nanos()),
            self.ceiling.map(|value| value.as_nanos()),
            elapsed.as_nanos(),
        )
        .map(|timeout| timeout.map(duration_from_nanos))
        .map_err(|()| expired())
    }

    pub(crate) fn execution_timeout(&self) -> Result<Option<Duration>, ModuleError> {
        self.execution_after(self.admitted.elapsed())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn budget(invocation: Option<u64>, ceiling: Option<u64>) -> CallBudget {
        CallBudget {
            admitted: Instant::now(),
            invocation: invocation.map(Duration::from_millis),
            ceiling: ceiling.map(Duration::from_millis),
        }
    }

    #[test]
    fn dispatch_uses_only_the_deadline_remainder() {
        assert_eq!(
            budget(Some(100), Some(500))
                .execution_after(Duration::from_millis(60))
                .unwrap(),
            Some(Duration::from_millis(40))
        );
        assert_eq!(
            budget(Some(100), Some(10))
                .execution_after(Duration::from_millis(60))
                .unwrap(),
            Some(Duration::from_millis(10))
        );
        assert!(matches!(
            budget(Some(100), None).execution_after(Duration::from_millis(100)),
            Err(ModuleError::BudgetExceeded(_))
        ));
    }

    #[test]
    fn no_invocation_deadline_does_not_spend_the_execution_ceiling_in_queue() {
        assert_eq!(
            budget(None, Some(10))
                .execution_after(Duration::from_secs(60))
                .unwrap(),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            budget(None, None)
                .execution_after(Duration::from_secs(60))
                .unwrap(),
            None
        );
    }

    #[test]
    fn far_future_deadline_does_not_overflow_an_instant_addition() {
        let value = CallBudget {
            admitted: Instant::now(),
            invocation: Some(Duration::MAX),
            ceiling: Some(Duration::from_secs(10)),
        };
        assert_eq!(
            value.execution_after(Duration::from_secs(1)).unwrap(),
            Some(Duration::from_secs(10))
        );
    }

    fn precise_budget(invocation: Option<Duration>, ceiling: Option<Duration>) -> CallBudget {
        CallBudget {
            admitted: Instant::now(),
            invocation,
            ceiling,
        }
    }

    #[test]
    fn native_nanosecond_adapter_round_trips_full_duration_range() {
        assert_eq!(Duration::MAX.as_nanos(), numeric::MAX_DURATION_NANOS);
        for seconds in [0, 1, 2, u32::MAX as u64, u64::MAX - 1, u64::MAX] {
            for nanos in [0, 1, 999, 999_999, 1_000_000, 999_999_998, 999_999_999] {
                let value = Duration::new(seconds, nanos);
                assert_eq!(duration_from_nanos(value.as_nanos()), value);
            }
        }
    }

    #[test]
    fn submillisecond_remainder_and_second_borrow_are_exact() {
        let one_ns = Duration::from_nanos(1);
        let value = precise_budget(Some(Duration::new(1, 3)), None);
        assert_eq!(
            value
                .remaining_after(Duration::new(0, 999_999_999))
                .unwrap(),
            Some(Duration::from_nanos(4))
        );
        assert_eq!(
            value.execution_after(Duration::new(1, 2)).unwrap(),
            Some(one_ns)
        );
        for elapsed in [Duration::new(1, 3), Duration::new(1, 4), Duration::MAX] {
            assert!(matches!(
                value.remaining_after(elapsed),
                Err(ModuleError::BudgetExceeded(_))
            ));
        }
        assert_eq!(
            precise_budget(Some(Duration::from_nanos(999)), Some(one_ns))
                .execution_after(Duration::from_nanos(998))
                .unwrap(),
            Some(one_ns)
        );
    }

    #[test]
    fn zero_and_maximum_keep_native_duration_semantics() {
        let zero = Duration::ZERO;
        let max = Duration::MAX;
        assert!(matches!(
            precise_budget(Some(zero), None).execution_after(zero),
            Err(ModuleError::BudgetExceeded(_))
        ));
        assert!(matches!(
            precise_budget(Some(max), Some(zero)).execution_after(max),
            Err(ModuleError::BudgetExceeded(_))
        ));
        assert_eq!(
            precise_budget(None, Some(zero))
                .execution_after(max)
                .unwrap(),
            Some(zero)
        );
        assert_eq!(
            precise_budget(Some(max), Some(zero))
                .execution_after(zero)
                .unwrap(),
            Some(zero)
        );
        assert_eq!(
            precise_budget(None, None).execution_after(max).unwrap(),
            None
        );
        assert_eq!(
            precise_budget(None, Some(max))
                .execution_after(max)
                .unwrap(),
            Some(max)
        );
        assert_eq!(
            precise_budget(Some(max), None)
                .execution_after(zero)
                .unwrap(),
            Some(max)
        );
        assert_eq!(
            precise_budget(Some(max), Some(max))
                .execution_after(Duration::from_nanos(1))
                .unwrap(),
            Some(Duration::new(u64::MAX, 999_999_998))
        );
        assert_eq!(
            precise_budget(Some(max), None)
                .remaining_after(Duration::from_secs(u64::MAX))
                .unwrap(),
            Some(Duration::new(0, 999_999_999))
        );
    }

    #[test]
    fn adapter_matches_native_duration_selection_at_boundaries() {
        let values = [
            Duration::ZERO,
            Duration::from_nanos(1),
            Duration::from_nanos(999),
            Duration::new(0, 999_999_999),
            Duration::new(1, 0),
            Duration::new(1, 1),
            Duration::new(u64::MAX, 0),
            Duration::MAX,
        ];
        let options = [None]
            .into_iter()
            .chain(values.into_iter().map(Some))
            .collect::<Vec<_>>();
        for invocation in &options {
            for ceiling in &options {
                for elapsed in values {
                    let expected_remaining = invocation
                        .map(|limit| {
                            limit
                                .checked_sub(elapsed)
                                .filter(|left| !left.is_zero())
                                .ok_or(())
                        })
                        .transpose();
                    let expected_execution =
                        expected_remaining.map(|remaining| match (*ceiling, remaining) {
                            (None, None) => None,
                            (Some(limit), None) | (None, Some(limit)) => Some(limit),
                            (Some(limit), Some(left)) => Some(limit.min(left)),
                        });
                    let value = precise_budget(*invocation, *ceiling);
                    assert_eq!(
                        value.remaining_after(elapsed).map_err(|_| ()),
                        expected_remaining
                    );
                    assert_eq!(
                        value.execution_after(elapsed).map_err(|_| ()),
                        expected_execution
                    );
                }
            }
        }
    }

    #[test]
    fn numeric_invalid_inputs_return_errors_without_overflow() {
        let too_large = numeric::MAX_DURATION_NANOS + 1;
        for invalid in [too_large, u128::MAX] {
            assert_eq!(numeric::remaining_nanos(Some(invalid), 0), Err(()));
            assert_eq!(numeric::remaining_nanos(None, invalid), Err(()));
            assert_eq!(numeric::execution_nanos(None, Some(invalid), 0), Err(()));
            assert_eq!(numeric::execution_nanos(Some(0), None, invalid), Err(()));
        }
    }
}
