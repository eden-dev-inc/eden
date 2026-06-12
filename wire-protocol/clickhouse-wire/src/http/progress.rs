//! HTTP progress header parsing for ClickHouse.
//!
//! ClickHouse reports query progress via X-ClickHouse-Progress headers.

use crate::error::ClickhouseWireError;

/// Progress information from X-ClickHouse-Progress header.
///
/// The header value is JSON like:
/// `{"read_rows":"100","read_bytes":"1000","total_rows_to_read":"10000"}`
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HttpProgress {
    /// Number of rows read.
    pub read_rows: u64,
    /// Number of bytes read.
    pub read_bytes: u64,
    /// Total rows to read (estimate).
    pub total_rows_to_read: u64,
    /// Number of rows written.
    pub written_rows: u64,
    /// Number of bytes written.
    pub written_bytes: u64,
    /// Elapsed time in nanoseconds.
    pub elapsed_ns: u64,
}

impl HttpProgress {
    /// Create new empty progress.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse progress from JSON string.
    ///
    /// This is a simple parser that doesn't require a JSON library.
    /// Format: `{"key":"value",...}`
    pub fn parse_json(json: &str) -> Result<Self, ClickhouseWireError> {
        let mut progress = Self::new();

        // Simple JSON parsing without external dependencies
        let json = json.trim();
        if !json.starts_with('{') || !json.ends_with('}') {
            return Err(ClickhouseWireError::InvalidHeader {
                header: "X-ClickHouse-Progress".to_string(),
                value: json.to_string(),
            });
        }

        // Remove braces and split by comma
        let inner = &json[1..json.len() - 1];
        for pair in inner.split(',') {
            let pair = pair.trim();
            if pair.is_empty() {
                continue;
            }

            // Split by colon
            let mut parts = pair.splitn(2, ':');
            let key = parts.next().unwrap_or("").trim().trim_matches('"');
            let value = parts.next().unwrap_or("").trim().trim_matches('"');

            match key {
                "read_rows" => progress.read_rows = value.parse().unwrap_or(0),
                "read_bytes" => progress.read_bytes = value.parse().unwrap_or(0),
                "total_rows_to_read" => progress.total_rows_to_read = value.parse().unwrap_or(0),
                "written_rows" => progress.written_rows = value.parse().unwrap_or(0),
                "written_bytes" => progress.written_bytes = value.parse().unwrap_or(0),
                "elapsed_ns" => progress.elapsed_ns = value.parse().unwrap_or(0),
                _ => {}
            }
        }

        Ok(progress)
    }

    /// Get completion percentage (if total_rows is known).
    pub fn completion_percent(&self) -> Option<f64> {
        if self.total_rows_to_read > 0 {
            Some((self.read_rows as f64 / self.total_rows_to_read as f64) * 100.0)
        } else {
            None
        }
    }

    /// Get elapsed time in seconds.
    pub fn elapsed_seconds(&self) -> f64 {
        self.elapsed_ns as f64 / 1_000_000_000.0
    }

    /// Accumulate another progress update.
    pub fn accumulate(&mut self, other: &HttpProgress) {
        self.read_rows += other.read_rows;
        self.read_bytes += other.read_bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;

        if other.total_rows_to_read > 0 {
            self.total_rows_to_read = other.total_rows_to_read;
        }
        if other.elapsed_ns > 0 {
            self.elapsed_ns = other.elapsed_ns;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_json_basic() {
        let json = r#"{"read_rows":"100","read_bytes":"5000","total_rows_to_read":"1000"}"#;
        let progress = HttpProgress::parse_json(json).unwrap();

        assert_eq!(progress.read_rows, 100);
        assert_eq!(progress.read_bytes, 5000);
        assert_eq!(progress.total_rows_to_read, 1000);
    }

    #[test]
    fn test_parse_json_all_fields() {
        let json = r#"{"read_rows":"50","read_bytes":"2500","total_rows_to_read":"100","written_rows":"0","written_bytes":"0","elapsed_ns":"1000000000"}"#;
        let progress = HttpProgress::parse_json(json).unwrap();

        assert_eq!(progress.read_rows, 50);
        assert_eq!(progress.read_bytes, 2500);
        assert_eq!(progress.total_rows_to_read, 100);
        assert_eq!(progress.elapsed_ns, 1_000_000_000);
    }

    #[test]
    fn test_completion_percent() {
        let progress = HttpProgress { read_rows: 50, total_rows_to_read: 100, ..Default::default() };

        let percent = progress.completion_percent().unwrap();
        assert!((percent - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_elapsed_seconds() {
        let progress = HttpProgress { elapsed_ns: 2_500_000_000, ..Default::default() };

        assert!((progress.elapsed_seconds() - 2.5).abs() < 0.001);
    }

    #[test]
    fn test_invalid_json() {
        let result = HttpProgress::parse_json("not json");
        assert!(result.is_err());
    }

    #[test]
    fn test_accumulate() {
        let mut total = HttpProgress::new();
        let update = HttpProgress {
            read_rows: 100,
            read_bytes: 5000,
            total_rows_to_read: 1000,
            ..Default::default()
        };

        total.accumulate(&update);
        assert_eq!(total.read_rows, 100);
        assert_eq!(total.total_rows_to_read, 1000);

        total.accumulate(&update);
        assert_eq!(total.read_rows, 200);
        assert_eq!(total.total_rows_to_read, 1000); // Not accumulated
    }
}
