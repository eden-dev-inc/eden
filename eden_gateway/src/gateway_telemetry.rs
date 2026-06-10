use std::time::Instant;

pub(crate) struct GatewayTelemetry;

impl GatewayTelemetry {
    pub(crate) fn elapsed_since_us(start: Instant) -> u64 {
        start.elapsed().as_micros().min(u64::MAX as u128) as u64
    }
}
