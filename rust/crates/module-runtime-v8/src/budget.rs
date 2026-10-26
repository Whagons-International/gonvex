use std::time::{Duration, Instant, SystemTime};

use gonvex_module_runtime::ModuleError;

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
        self.invocation
            .map(|limit| {
                limit
                    .checked_sub(elapsed)
                    .filter(|left| !left.is_zero())
                    .ok_or_else(expired)
            })
            .transpose()
    }

    pub(crate) fn invocation_remaining(&self) -> Result<Option<Duration>, ModuleError> {
        self.remaining_after(self.admitted.elapsed())
    }

    fn execution_after(&self, elapsed: Duration) -> Result<Option<Duration>, ModuleError> {
        Ok(match (self.ceiling, self.remaining_after(elapsed)?) {
            (None, None) => None,
            (Some(limit), None) | (None, Some(limit)) => Some(limit),
            (Some(limit), Some(remaining)) => Some(limit.min(remaining)),
        })
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
}
