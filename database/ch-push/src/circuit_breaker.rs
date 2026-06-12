use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::stats::DeadLetterStats;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    fn as_u64(self) -> u64 {
        match self {
            CircuitState::Closed => 0,
            CircuitState::Open => 1,
            CircuitState::HalfOpen => 2,
        }
    }
}

pub struct CircuitBreaker {
    state: CircuitState,
    consecutive_failures: u32,
    opened_at: Option<Instant>,
    failure_threshold: u32,
    cooldown: Duration,
    stats: Arc<DeadLetterStats>,
}

impl CircuitBreaker {
    pub fn new(stats: Arc<DeadLetterStats>, failure_threshold: u32, cooldown: Duration) -> Self {
        let breaker = Self {
            state: CircuitState::Closed,
            consecutive_failures: 0,
            opened_at: None,
            failure_threshold,
            cooldown,
            stats,
        };
        breaker.update_state_metric();
        breaker
    }

    fn update_state_metric(&self) {
        self.stats.circuit_state.store(self.state.as_u64(), Ordering::Relaxed);
    }

    fn set_state(&mut self, state: CircuitState) {
        if self.state != state {
            self.state = state;
            self.update_state_metric();
        }
    }

    pub fn allow(&mut self) -> bool {
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => {
                if let Some(opened_at) = self.opened_at {
                    if opened_at.elapsed() >= self.cooldown {
                        self.opened_at = None;
                        self.consecutive_failures = 0;
                        self.set_state(CircuitState::HalfOpen);
                        true
                    } else {
                        false
                    }
                } else {
                    false
                }
            }
            CircuitState::HalfOpen => true,
        }
    }

    pub fn on_success(&mut self) {
        self.consecutive_failures = 0;
        self.opened_at = None;
        self.set_state(CircuitState::Closed);
    }

    pub fn on_failure(&mut self) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.state == CircuitState::HalfOpen || self.consecutive_failures >= self.failure_threshold {
            self.opened_at = Some(Instant::now());
            self.set_state(CircuitState::Open);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_breaker() -> CircuitBreaker {
        let stats = Arc::new(DeadLetterStats::default());
        CircuitBreaker::new(stats, 3, Duration::from_secs(30))
    }

    fn make_test_breaker_with_config(threshold: u32, cooldown_secs: u64) -> CircuitBreaker {
        let stats = Arc::new(DeadLetterStats::default());
        CircuitBreaker::new(stats, threshold, Duration::from_secs(cooldown_secs))
    }

    #[test]
    fn circuit_breaker_starts_closed() {
        let mut breaker = make_test_breaker();
        assert_eq!(breaker.state, CircuitState::Closed);
        assert!(breaker.allow());
    }

    #[test]
    fn circuit_breaker_opens_after_threshold_failures() {
        let mut breaker = make_test_breaker_with_config(3, 30);

        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Closed);
        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Closed);
        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Open);
    }

    #[test]
    fn circuit_breaker_blocks_when_open() {
        let mut breaker = make_test_breaker_with_config(1, 30);
        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Open);
        assert!(!breaker.allow());
    }

    #[test]
    fn circuit_breaker_half_open_after_cooldown() {
        let mut breaker = make_test_breaker_with_config(1, 0);
        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Open);

        std::thread::sleep(Duration::from_millis(10));
        assert!(breaker.allow());
        assert_eq!(breaker.state, CircuitState::HalfOpen);
    }

    #[test]
    fn circuit_breaker_closes_on_success() {
        let mut breaker = make_test_breaker_with_config(1, 0);
        breaker.on_failure();
        std::thread::sleep(Duration::from_millis(10));
        breaker.allow();

        breaker.on_success();
        assert_eq!(breaker.state, CircuitState::Closed);
        assert_eq!(breaker.consecutive_failures, 0);
    }

    #[test]
    fn circuit_breaker_reopens_on_half_open_failure() {
        let mut breaker = make_test_breaker_with_config(1, 0);
        breaker.on_failure();
        std::thread::sleep(Duration::from_millis(10));
        breaker.allow();
        assert_eq!(breaker.state, CircuitState::HalfOpen);

        breaker.on_failure();
        assert_eq!(breaker.state, CircuitState::Open);
    }

    #[test]
    fn circuit_breaker_resets_failures_on_success() {
        let mut breaker = make_test_breaker_with_config(5, 30);
        breaker.on_failure();
        breaker.on_failure();
        assert_eq!(breaker.consecutive_failures, 2);

        breaker.on_success();
        assert_eq!(breaker.consecutive_failures, 0);
    }

    #[test]
    fn circuit_breaker_state_metric_updates() {
        let mut breaker = make_test_breaker_with_config(1, 0);
        assert_eq!(breaker.stats.circuit_state.load(Ordering::Relaxed), 0);

        breaker.on_failure();
        assert_eq!(breaker.stats.circuit_state.load(Ordering::Relaxed), 1);

        std::thread::sleep(Duration::from_millis(10));
        breaker.allow();
        assert_eq!(breaker.stats.circuit_state.load(Ordering::Relaxed), 2);

        breaker.on_success();
        assert_eq!(breaker.stats.circuit_state.load(Ordering::Relaxed), 0);
    }
}
