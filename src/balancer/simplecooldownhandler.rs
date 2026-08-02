use std::time::{Duration, Instant};

use crate::balancer::traits::{ConnectResult, ConnectResult::Failure, CooldownHandler};
use crate::errors::ProxyError;
use crate::state::{BackendStatus, BackendStatus::Alive, BackendStatus::Drain, TargetState};

/// An extremely simple cooldown handler that simply uses a constant time to skip calling a backend.
pub struct SimpleCooldownHandler {
    targets: TargetState,
}
impl SimpleCooldownHandler {
    const COOLDOWN: Duration = Duration::from_secs(120);

    pub fn new(targets: TargetState) -> Self {
        SimpleCooldownHandler { targets }
    }
}
impl CooldownHandler for SimpleCooldownHandler {
    fn get_target_status(&self, target: &str) -> Result<BackendStatus, ProxyError> {
        let status = **self.targets.get_target_status(target)?;
        Ok(status)
    }

    /// Always update cooldown on failure. Remove a cooldown if we connected successfully during
    /// one.
    fn report_connection_attempt(
        &self,
        target: &str,
        result: ConnectResult,
    ) -> Result<(), ProxyError> {
        let now = Instant::now();
        if result == Failure {
            self.targets.update_target_status(
                target,
                Alive {
                    cool_until: Some(Instant::now() + Self::COOLDOWN),
                },
            )?;
        }
        let Alive {
            cool_until: Some(until),
        } = self.get_target_status(target)?
        else {
            return Ok(());
        };
        if until > now {
            self.targets
                .update_target_status(target, Alive { cool_until: None })?;
        }
        Ok(())
    }

    fn drain(&self, target: &str) -> Result<(), ProxyError> {
        self.targets.update_target_status(target, Drain)
    }
}
