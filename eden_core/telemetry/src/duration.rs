use std::time::Duration;

/// Duration in nanoseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationNanos(pub u64);

/// Duration in microseconds
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct DurationMicros(pub u64);

/// Duration in milliseconds
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct DurationMillis(pub f64);

impl From<Duration> for DurationNanos {
    fn from(d: Duration) -> Self {
        Self(d.as_nanos() as u64)
    }
}

impl From<Duration> for DurationMicros {
    fn from(d: Duration) -> Self {
        Self(d.as_micros() as u64)
    }
}

impl From<Duration> for DurationMillis {
    fn from(d: Duration) -> Self {
        Self(d.as_secs_f64() * 1000.0)
    }
}

impl From<DurationNanos> for DurationMicros {
    fn from(d: DurationNanos) -> Self {
        Self(d.0 / 1_000)
    }
}

impl From<DurationNanos> for DurationMillis {
    fn from(d: DurationNanos) -> Self {
        Self(d.0 as f64 / 1_000_000.0)
    }
}

impl From<DurationMicros> for DurationMillis {
    fn from(d: DurationMicros) -> Self {
        Self(d.0 as f64 / 1_000.0)
    }
}

// === Accessor methods ===

impl DurationNanos {
    pub const fn new(nanos: u64) -> Self {
        Self(nanos)
    }

    pub const fn as_nanos(&self) -> u64 {
        self.0
    }

    pub fn as_micros(&self) -> u64 {
        self.0 / 1_000
    }

    pub fn as_millis(&self) -> f64 {
        self.0 as f64 / 1_000_000.0
    }
}

impl DurationMicros {
    pub const fn new(micros: u64) -> Self {
        Self(micros)
    }

    pub const fn as_micros(&self) -> u64 {
        self.0
    }

    pub fn as_millis(&self) -> f64 {
        self.0 as f64 / 1_000.0
    }

    pub fn as_nanos(&self) -> u64 {
        self.0.saturating_mul(1_000)
    }
}

impl DurationMillis {
    pub const fn new(millis: f64) -> Self {
        Self(millis)
    }

    pub fn as_millis(&self) -> f64 {
        self.0
    }

    pub fn as_micros(&self) -> u64 {
        (self.0 * 1_000.0) as u64
    }

    pub fn as_nanos(&self) -> u64 {
        (self.0 * 1_000_000.0) as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_std_duration() {
        let std_dur = Duration::from_millis(1500); // 1.5 seconds

        let nanos: DurationNanos = std_dur.into();
        let micros: DurationMicros = std_dur.into();
        let millis: DurationMillis = std_dur.into();

        assert_eq!(nanos.0, 1_500_000_000);
        assert_eq!(micros.0, 1_500_000);
        assert_eq!(millis.0, 1500.0);
    }

    #[test]
    fn test_nanos_to_smaller_units() {
        let nanos = DurationNanos(1_234_567_890);

        let micros: DurationMicros = nanos.into();
        let millis: DurationMillis = nanos.into();

        assert_eq!(micros.0, 1_234_567);
        assert_eq!(millis.0, 1234.567890);
    }

    #[test]
    fn test_micros_to_millis() {
        let micros = DurationMicros(5_000);
        let millis: DurationMillis = micros.into();

        assert_eq!(millis.0, 5.0);
    }

    #[test]
    fn test_accessor_methods() {
        let nanos = DurationNanos::new(2_000_000);

        assert_eq!(nanos.as_nanos(), 2_000_000);
        assert_eq!(nanos.as_micros(), 2_000);
        assert_eq!(nanos.as_millis(), 2.0);
    }

    #[test]
    fn test_comparisons() {
        let a = DurationNanos(1000);
        let b = DurationNanos(2000);

        assert!(a < b);
        assert!(b > a);
        assert_eq!(a, DurationNanos(1000));
    }

    #[test]
    fn test_precision_safety() {
        // This demonstrates that we prevent accidental precision loss
        let millis = DurationMillis(1.5);

        // These conversions are explicit and clear about precision
        assert_eq!(millis.as_micros(), 1_500);
        assert_eq!(millis.as_nanos(), 1_500_000);
    }

    #[test]
    fn test_saturation() {
        let huge_micros = DurationMicros(u64::MAX);
        let nanos = huge_micros.as_nanos();

        // Should saturate rather than overflow
        assert_eq!(nanos, u64::MAX);
    }
}
