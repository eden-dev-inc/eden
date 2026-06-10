use eden_gateway_core::response::{GatewayMirrorResponseMode, GatewayResponsePolicySpec, GatewayResponseProfile, WireResponseObserver};
use memchr::memchr_iter;

#[derive(Debug, Default)]
pub struct RedisResponseErrorScanner {
    observed: usize,
    prev_prev: u8,
    prev: u8,
    contains_error: bool,
}

impl WireResponseObserver for RedisResponseErrorScanner {
    fn observe(&mut self, bytes: &[u8]) {
        if bytes.is_empty() || self.contains_error {
            self.observed = self.observed.saturating_add(bytes.len());
            return;
        }

        for pos in memchr_iter(b'-', bytes) {
            if self.is_error_frame_start(bytes, pos) {
                self.contains_error = true;
                break;
            }
        }

        self.update_tail(bytes);
        self.observed = self.observed.saturating_add(bytes.len());
    }
}

impl RedisResponseErrorScanner {
    pub fn contains_error(&self) -> bool {
        self.contains_error
    }

    fn is_error_frame_start(&self, bytes: &[u8], pos: usize) -> bool {
        if pos == 0 {
            return self.observed == 0 || (self.prev_prev == b'\r' && self.prev == b'\n');
        }

        if pos == 1 {
            return self.prev == b'\r' && bytes[0] == b'\n';
        }

        bytes[pos - 2] == b'\r' && bytes[pos - 1] == b'\n'
    }

    fn update_tail(&mut self, bytes: &[u8]) {
        match bytes.len() {
            0 => {}
            1 => {
                self.prev_prev = self.prev;
                self.prev = bytes[0];
            }
            len => {
                self.prev_prev = bytes[len - 2];
                self.prev = bytes[len - 1];
            }
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct RedisGatewayResponseProfile;

impl GatewayResponseProfile for RedisGatewayResponseProfile {
    type Observer = RedisResponseErrorScanner;

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponsePolicySpec::new("redis", Some(GatewayMirrorResponseMode::DrainOnly))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn response_error_scanner_detects_error_at_stream_start() {
        let mut scanner = RedisResponseErrorScanner::default();

        scanner.observe(b"-ERR bad\r\n");

        assert!(scanner.contains_error());
    }

    #[test]
    fn response_error_scanner_detects_error_after_complete_frame() {
        let mut scanner = RedisResponseErrorScanner::default();

        scanner.observe(b"$5\r\nvalue\r\n-ERR bad\r\n");

        assert!(scanner.contains_error());
    }

    #[test]
    fn response_error_scanner_detects_split_error_boundary() {
        let mut scanner = RedisResponseErrorScanner::default();

        scanner.observe(b"$5\r\nvalue\r");
        scanner.observe(b"\n-ERR bad\r\n");

        assert!(scanner.contains_error());
    }

    #[test]
    fn response_error_scanner_ignores_hyphens_inside_payloads() {
        let mut scanner = RedisResponseErrorScanner::default();

        scanner.observe(b"$11\r\nvalue-error\r\n+OK\r\n");

        assert!(!scanner.contains_error());
    }

    #[test]
    fn response_error_scanner_ignores_hyphen_after_non_frame_newline() {
        let mut scanner = RedisResponseErrorScanner::default();

        scanner.observe(b"$13\r\nvalue\n-error\r\n");

        assert!(!scanner.contains_error());
    }
}
