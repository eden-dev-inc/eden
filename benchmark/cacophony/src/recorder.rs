use std::collections::HashMap;
use std::time::Instant;

use hdrhistogram::Histogram;
use serde::Serialize;
use tokio::sync::mpsc;

use crate::arrival::CommandType;

/// A completed command with all timing information.
pub struct CompletedCommand {
    /// When the arrival scheduler said this command should fire.
    pub scheduled_time: Instant,
    /// When the command was actually written to the socket.
    pub send_time: Instant,
    /// When the response was fully read from the socket.
    pub recv_time: Instant,
    pub connection_id: u64,
    pub command_type: CommandType,
    pub key: String,
    /// Number of SET commands planned for this key in the generated phase.
    pub planned_set_count: u16,
    /// For SET commands: the value that was written.
    pub set_value: Option<Vec<u8>>,
    /// RESP request bytes successfully handed to the socket writer.
    pub request_wire_bytes: u64,
    /// RESP response bytes read from the socket, including framing.
    pub response_wire_bytes: u64,
    /// Application payload bytes read from bulk-string responses.
    pub response_payload_bytes: u64,
    pub outcome: CommandOutcome,
}

pub enum CommandOutcome {
    /// GET returned a value.
    GetHit(Vec<u8>),
    /// GET returned nil.
    GetMiss,
    /// SET returned OK.
    SetOk,
    /// Redis returned an error string.
    RedisError(String),
    /// Connection or protocol error.
    ConnectionError(String),
}

/// Per-phase summary output.
#[derive(Serialize)]
pub struct PhaseSummary {
    pub completed: u64,
    pub errors: u64,
    pub redis_errors: u64,
    pub connection_errors: u64,
    pub integrity_failures: u64,
    pub integrity_race_suspects: u64,
    pub integrity_checked_hits: u64,
    pub integrity_unchecked_hits: u64,
    pub request_wire_bytes: u64,
    pub response_wire_bytes: u64,
    pub response_payload_bytes: u64,
    /// Top error messages by frequency (up to 10).
    pub top_errors: Vec<ErrorCount>,
    /// Latency for successful commands only.
    pub service_latency_us: LatencySummary,
    pub sojourn_latency_us: LatencySummary,
    pub queue_delay_us: LatencySummary,
    /// Latency for error commands only (redis + connection errors).
    pub error_service_latency_us: LatencySummary,
}

#[derive(Serialize)]
pub struct ErrorCount {
    pub message: String,
    pub count: u64,
}

#[derive(Serialize)]
pub struct LatencySummary {
    pub min: u64,
    pub mean: f64,
    pub p50: u64,
    pub p95: u64,
    pub p99: u64,
    pub p999: u64,
    pub max: u64,
    pub count: u64,
}

impl LatencySummary {
    fn from_histogram(h: &Histogram<u64>) -> Self {
        if h.is_empty() {
            return Self {
                min: 0,
                mean: 0.0,
                p50: 0,
                p95: 0,
                p99: 0,
                p999: 0,
                max: 0,
                count: 0,
            };
        }
        Self {
            min: h.min(),
            mean: h.mean(),
            p50: h.value_at_quantile(0.50),
            p95: h.value_at_quantile(0.95),
            p99: h.value_at_quantile(0.99),
            p999: h.value_at_quantile(0.999),
            max: h.max(),
            count: h.len(),
        }
    }
}

/// Records command outcomes and computes per-phase summaries.
///
/// # Integrity checking
///
/// The recorder can be primed with planned SET counts before dispatch starts.
/// For keys with exactly one planned SET, a GET hit after that SET completes
/// can be checked strictly against the observed value. Keys with multiple
/// planned SETs are ambiguous under concurrent connections, so mismatches are
/// reported as race suspects rather than hard failures.
///
/// - **GET on a key never SET by this process**: not flagged. We have no
///   way to know what value (if any) should be there.
/// - **GET returning nil for a key we SET**: not flagged. Could be TTL,
///   eviction, or a bug; we cannot distinguish those here.
/// - **GET returning a different value on a multi-SET key**: counted as
///   `integrity_race_suspects`, not `integrity_failures`. With concurrent
///   connections and uniform key distribution, Redis may serialize writes in
///   an order the recorder cannot infer from response arrival order alone.
///
/// This makes `integrity_failures` a stricter wrong-value signal for
/// deterministic keys while keeping the old race-prone signal visible under
/// `integrity_race_suspects`.
pub struct Recorder {
    /// HDR histograms for successful commands (in microseconds).
    service: Histogram<u64>,
    sojourn: Histogram<u64>,
    queue_delay: Histogram<u64>,
    /// HDR histogram for error commands (service latency only).
    error_service: Histogram<u64>,
    completed: u64,
    errors: u64,
    redis_errors: u64,
    connection_errors: u64,
    integrity_failures: u64,
    integrity_race_suspects: u64,
    integrity_checked_hits: u64,
    integrity_unchecked_hits: u64,
    request_wire_bytes: u64,
    response_wire_bytes: u64,
    response_payload_bytes: u64,
    /// Expected values for integrity checking: key -> last SET value.
    expected: HashMap<String, Vec<u8>>,
    /// Error message frequency for the top error strings.
    error_strings: HashMap<String, u64>,
}

impl Default for Recorder {
    fn default() -> Self {
        Self::new()
    }
}

impl Recorder {
    pub fn new() -> Self {
        // 1μs to 60s range, 3 significant digits.
        let hist = || Histogram::<u64>::new_with_bounds(1, 60_000_000, 3).expect("histogram bounds");
        Self {
            service: hist(),
            sojourn: hist(),
            queue_delay: hist(),
            error_service: hist(),
            completed: 0,
            errors: 0,
            redis_errors: 0,
            connection_errors: 0,
            integrity_failures: 0,
            integrity_race_suspects: 0,
            integrity_checked_hits: 0,
            integrity_unchecked_hits: 0,
            request_wire_bytes: 0,
            response_wire_bytes: 0,
            response_payload_bytes: 0,
            expected: HashMap::new(),
            error_strings: HashMap::new(),
        }
    }

    /// Run the recorder as a task, consuming events until the channel closes.
    /// Returns the phase summary.
    pub async fn run(mut self, mut rx: mpsc::UnboundedReceiver<CompletedCommand>) -> PhaseSummary {
        while let Some(cmd) = rx.recv().await {
            self.record(cmd);
        }
        self.summarize()
    }

    fn record(&mut self, cmd: CompletedCommand) {
        self.request_wire_bytes += cmd.request_wire_bytes;
        self.response_wire_bytes += cmd.response_wire_bytes;
        self.response_payload_bytes += cmd.response_payload_bytes;

        // Latency calculations in microseconds.
        let service_us = cmd.recv_time.duration_since(cmd.send_time).as_micros() as u64;
        let sojourn_us = cmd.recv_time.duration_since(cmd.scheduled_time).as_micros() as u64;
        let queue_us = cmd.send_time.duration_since(cmd.scheduled_time).as_micros() as u64;

        match &cmd.outcome {
            CommandOutcome::SetOk => {
                self.completed += 1;
                if let Some(val) = cmd.set_value {
                    self.expected.insert(cmd.key, val);
                }
            }
            CommandOutcome::GetHit(received) => {
                self.completed += 1;
                match (cmd.planned_set_count, self.expected.get(&cmd.key)) {
                    (1, Some(expected)) => {
                        self.integrity_checked_hits += 1;
                        if received != expected {
                            self.integrity_failures += 1;
                        }
                    }
                    (count, Some(expected)) => {
                        if count == 0 {
                            self.integrity_unchecked_hits += 1;
                            if received != expected {
                                self.integrity_race_suspects += 1;
                            }
                        } else if count > 1 && received != expected {
                            self.integrity_race_suspects += 1;
                        }
                    }
                    (_, None) => {
                        self.integrity_unchecked_hits += 1;
                    }
                }
            }
            CommandOutcome::GetMiss => {
                self.completed += 1;
                // nil for an unknown key is fine; nil for a key we SET is
                // ambiguous (could be TTL, eviction, or a bug). Don't flag
                // it as an integrity failure here.
            }
            CommandOutcome::RedisError(e) => {
                self.errors += 1;
                self.redis_errors += 1;
                *self.error_strings.entry(e.clone()).or_insert(0) += 1;
            }
            CommandOutcome::ConnectionError(e) => {
                self.errors += 1;
                self.connection_errors += 1;
                *self.error_strings.entry(e.clone()).or_insert(0) += 1;
            }
        }

        // Route latencies to the correct histogram set.
        let clamp = |v: u64| v.clamp(1, 60_000_000);
        let is_error = matches!(cmd.outcome, CommandOutcome::RedisError(_) | CommandOutcome::ConnectionError(_));
        if is_error {
            let _ = self.error_service.record(clamp(service_us));
        } else {
            let _ = self.service.record(clamp(service_us));
            let _ = self.sojourn.record(clamp(sojourn_us));
            let _ = self.queue_delay.record(clamp(queue_us));
        }
    }

    fn summarize(self) -> PhaseSummary {
        // Top 10 error messages by frequency.
        let mut top_errors: Vec<ErrorCount> =
            self.error_strings.into_iter().map(|(message, count)| ErrorCount { message, count }).collect();
        top_errors.sort_by(|a, b| b.count.cmp(&a.count));
        top_errors.truncate(10);

        PhaseSummary {
            completed: self.completed,
            errors: self.errors,
            redis_errors: self.redis_errors,
            connection_errors: self.connection_errors,
            integrity_failures: self.integrity_failures,
            integrity_race_suspects: self.integrity_race_suspects,
            integrity_checked_hits: self.integrity_checked_hits,
            integrity_unchecked_hits: self.integrity_unchecked_hits,
            request_wire_bytes: self.request_wire_bytes,
            response_wire_bytes: self.response_wire_bytes,
            response_payload_bytes: self.response_payload_bytes,
            top_errors,
            service_latency_us: LatencySummary::from_histogram(&self.service),
            sojourn_latency_us: LatencySummary::from_histogram(&self.sojourn),
            queue_delay_us: LatencySummary::from_histogram(&self.queue_delay),
            error_service_latency_us: LatencySummary::from_histogram(&self.error_service),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cmd(
        command_type: CommandType,
        key: &str,
        planned_set_count: u16,
        set_value: Option<&[u8]>,
        outcome: CommandOutcome,
    ) -> CompletedCommand {
        let now = Instant::now();
        CompletedCommand {
            scheduled_time: now,
            send_time: now,
            recv_time: now,
            connection_id: 0,
            command_type,
            key: key.to_string(),
            planned_set_count,
            set_value: set_value.map(ToOwned::to_owned),
            request_wire_bytes: 0,
            response_wire_bytes: 0,
            response_payload_bytes: 0,
            outcome,
        }
    }

    #[test]
    fn planned_stale_value_is_race_suspect_not_integrity_failure() {
        let mut recorder = Recorder::new();

        recorder.record(cmd(CommandType::Set, "k", 2, Some(b"newer"), CommandOutcome::SetOk));
        recorder.record(cmd(CommandType::Get, "k", 2, None, CommandOutcome::GetHit(b"older".to_vec())));

        let summary = recorder.summarize();
        assert_eq!(summary.integrity_failures, 0);
        assert_eq!(summary.integrity_race_suspects, 1);
        assert_eq!(summary.integrity_checked_hits, 0);
        assert_eq!(summary.integrity_unchecked_hits, 0);
    }

    #[test]
    fn single_set_mismatch_is_integrity_failure() {
        let mut recorder = Recorder::new();

        recorder.record(cmd(CommandType::Set, "k", 1, Some(b"expected"), CommandOutcome::SetOk));
        recorder.record(cmd(CommandType::Get, "k", 1, None, CommandOutcome::GetHit(b"alien".to_vec())));

        let summary = recorder.summarize();
        assert_eq!(summary.integrity_failures, 1);
        assert_eq!(summary.integrity_race_suspects, 0);
        assert_eq!(summary.integrity_checked_hits, 1);
    }

    #[test]
    fn unplanned_mismatch_is_race_suspect_not_integrity_failure() {
        let mut recorder = Recorder::new();

        recorder.record(cmd(CommandType::Set, "k", 0, Some(b"expected"), CommandOutcome::SetOk));
        recorder.record(cmd(CommandType::Get, "k", 0, None, CommandOutcome::GetHit(b"other".to_vec())));

        let summary = recorder.summarize();
        assert_eq!(summary.integrity_failures, 0);
        assert_eq!(summary.integrity_race_suspects, 1);
        assert_eq!(summary.integrity_checked_hits, 0);
        assert_eq!(summary.integrity_unchecked_hits, 1);
    }
}
