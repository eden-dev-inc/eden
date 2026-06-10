//! Lightweight RESP boundary scanner for pipelined response parsing.
//!
//! Scans concatenated RESP response bytes and returns element boundaries
//! with error detection. Used to attribute per-command response bytes and
//! error flags in pipelined batches.
//!
//! Returns `None` on malformed or truncated input; callers fall back to
//! even division of total bytes.

/// A single RESP element boundary.
#[derive(Debug, Clone)]
pub(crate) struct RespElement<'a> {
    /// Number of bytes this element occupies in the response buffer.
    pub len: usize,
    /// Whether this element is a RESP error (`-ERR ...`).
    pub is_error: bool,
    /// Whether this element is a nil array (`*-1\r\n`), indicating a WATCH-aborted EXEC.
    pub is_nil_array: bool,
    /// For error responses, the error keyword (e.g. "MOVED", "ERR", "CANCELED").
    /// Used by the pipeline path to classify REDIRECT vs CLIENT_ERROR.
    pub error_kind: Option<&'a str>,
}

/// Maximum recursion depth for nested arrays.
const MAX_DEPTH: usize = 8;

pub(crate) struct RespScanner;

impl RespScanner {
    /// Scan concatenated RESP response bytes and return per-element boundaries.
    ///
    /// `cmd_count` is the expected number of top-level elements. Returns `None`
    /// if the input is malformed, truncated, or doesn't contain exactly
    /// `cmd_count` elements.
    pub(crate) fn scan(buf: &[u8], cmd_count: usize) -> Option<Vec<RespElement<'_>>> {
        let mut elements = Vec::with_capacity(cmd_count);
        let mut pos = 0;

        for _ in 0..cmd_count {
            if pos >= buf.len() {
                return None;
            }
            let start = pos;
            let is_error = buf[pos] == b'-';
            // Detect nil array: *-1\r\n (WATCH-aborted EXEC response)
            let is_nil_array = if buf[pos] == b'*' {
                Self::parse_int(buf, pos + 1).map(|(count, _)| count < 0).unwrap_or(false)
            } else {
                false
            };
            // Extract error keyword for error classification (REDIRECT, CLIENT_ERROR).
            let error_kind = if is_error { Self::extract_error_kind(buf, start) } else { None };
            pos = Self::skip_element(buf, pos, 0)?;
            elements.push(RespElement { len: pos - start, is_error, is_nil_array, error_kind });
        }

        Some(elements)
    }

    /// Extract the error keyword from a RESP error line (`-MOVED ...`, `-ERR ...`).
    ///
    /// Returns the first space-delimited token after the `-` prefix, which is the
    /// error kind used by `ErrorCategory::from_error_kind()` to classify redirects
    /// and client cancellations.
    fn extract_error_kind(buf: &[u8], start: usize) -> Option<&str> {
        // Error format: -KEYWORD message\r\n
        let after_dash = start + 1;
        let line_end = buf[after_dash..].iter().position(|&b| b == b'\r' || b == b' ')?;
        std::str::from_utf8(&buf[after_dash..after_dash + line_end]).ok()
    }

    /// Skip a single RESP element starting at `pos`, returning the position
    /// after the element. Returns `None` on malformed input.
    fn skip_element(buf: &[u8], pos: usize, depth: usize) -> Option<usize> {
        if pos >= buf.len() || depth > MAX_DEPTH {
            return None;
        }

        match buf[pos] {
            // Simple string (+), Error (-), Integer (:)
            b'+' | b'-' | b':' | b',' | b'(' => Self::skip_to_crlf(buf, pos + 1),

            // Null (_), Boolean (#)
            b'_' | b'#' => Self::skip_to_crlf(buf, pos + 1),

            // Bulk string ($)
            b'$' | b'=' | b'!' => {
                let (len, after_len) = Self::parse_int(buf, pos + 1)?;
                if len < 0 {
                    // Null blob string / blob error: $-1\r\n, =-1\r\n, !-1\r\n
                    Some(after_len)
                } else {
                    let data_end = after_len + len as usize;
                    // Need data_end + 2 for trailing \r\n
                    if data_end + 2 > buf.len() {
                        return None;
                    }
                    if buf[data_end] != b'\r' || buf[data_end + 1] != b'\n' {
                        return None;
                    }
                    Some(data_end + 2)
                }
            }

            // Array (*) and RESP3 push/set containers.
            b'*' | b'>' | b'~' => {
                let (count, mut cur) = Self::parse_int(buf, pos + 1)?;
                if count < 0 {
                    // Null array: *-1\r\n
                    return Some(cur);
                }
                for _ in 0..count {
                    cur = Self::skip_element(buf, cur, depth + 1)?;
                }
                Some(cur)
            }

            // RESP3 map and attribute.
            b'%' | b'|' => {
                let (count, mut cur) = Self::parse_int(buf, pos + 1)?;
                if count < 0 {
                    return Some(cur);
                }
                for _ in 0..(count as usize * 2) {
                    cur = Self::skip_element(buf, cur, depth + 1)?;
                }
                if buf[pos] == b'|' {
                    cur = Self::skip_element(buf, cur, depth + 1)?;
                }
                Some(cur)
            }

            _ => None,
        }
    }

    /// Skip to the end of a \r\n terminated line. Returns position after \r\n.
    fn skip_to_crlf(buf: &[u8], start: usize) -> Option<usize> {
        let mut i = start;
        while i + 1 < buf.len() {
            if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                return Some(i + 2);
            }
            i += 1;
        }
        None
    }

    /// Parse a signed integer from RESP, terminated by \r\n.
    /// Returns (value, position after \r\n).
    fn parse_int(buf: &[u8], start: usize) -> Option<(i64, usize)> {
        let crlf_pos = {
            let mut i = start;
            loop {
                if i + 1 >= buf.len() {
                    return None;
                }
                if buf[i] == b'\r' && buf[i + 1] == b'\n' {
                    break i;
                }
                i += 1;
            }
        };

        if start == crlf_pos {
            return None;
        }

        let mut pos = start;
        let negative = buf[pos] == b'-';
        if negative {
            pos += 1;
            if pos == crlf_pos {
                return None;
            }
        }

        let mut value = 0_i64;
        while pos < crlf_pos {
            let digit = buf[pos].checked_sub(b'0')?;
            if digit > 9 {
                return None;
            }
            value = value.checked_mul(10)?.checked_add(i64::from(digit))?;
            pos += 1;
        }

        if negative {
            value = value.checked_neg()?;
        }

        Some((value, crlf_pos + 2))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy)]
    enum Resp3LoadProfile {
        Consistent,
        Variable,
        Malicious,
    }

    impl Resp3LoadProfile {
        fn label(self) -> &'static str {
            match self {
                Self::Consistent => "consistent",
                Self::Variable => "variable",
                Self::Malicious => "malicious",
            }
        }
    }

    fn resp3_load_profiles() -> [Resp3LoadProfile; 3] {
        [
            Resp3LoadProfile::Consistent,
            Resp3LoadProfile::Variable,
            Resp3LoadProfile::Malicious,
        ]
    }

    fn resp3_only_buffer(profile: Resp3LoadProfile) -> (Vec<u8>, usize) {
        match profile {
            Resp3LoadProfile::Consistent => (b"_\r\n_\r\n_\r\n".to_vec(), 3),
            Resp3LoadProfile::Variable => (b"#t\r\n,1.5\r\n~2\r\n+one\r\n+two\r\n".to_vec(), 3),
            Resp3LoadProfile::Malicious => {
                let mut buf = b"%2\r\n+key\r\n+value\r\n+other\r\n=14\r\ntxt:bigpayload\r\n".to_vec();
                buf.extend_from_slice(b">2\r\n+message\r\n+payload\r\n");
                (buf, 2)
            }
        }
    }

    #[test]
    fn test_simple_strings() {
        // Two simple string responses: +OK\r\n+OK\r\n
        let buf = b"+OK\r\n+OK\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].len, 5);
        assert!(!elems[0].is_error);
        assert_eq!(elems[1].len, 5);
    }

    #[test]
    fn test_error_detection() {
        // Error response followed by OK
        let buf = b"-ERR something\r\n+OK\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert_eq!(elems.len(), 2);
        assert!(elems[0].is_error);
        assert!(!elems[1].is_error);
    }

    #[test]
    fn test_bulk_strings() {
        // $3\r\nfoo\r\n$-1\r\n
        let buf = b"$3\r\nfoo\r\n$-1\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].len, 9); // $3\r\nfoo\r\n
        assert_eq!(elems[1].len, 5); // $-1\r\n
    }

    #[test]
    fn test_integer() {
        let buf = b":42\r\n:0\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].len, 5);
        assert_eq!(elems[1].len, 4);
    }

    #[test]
    fn test_array() {
        // *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
        let buf = b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n";
        let elems = RespScanner::scan(buf, 1).unwrap();
        assert_eq!(elems.len(), 1);
        assert_eq!(elems[0].len, buf.len());
        assert!(!elems[0].is_error);
    }

    #[test]
    fn test_null_array() {
        let buf = b"*-1\r\n+OK\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert_eq!(elems.len(), 2);
        assert_eq!(elems[0].len, 5);
        assert_eq!(elems[1].len, 5);
    }

    #[test]
    fn test_mixed_types() {
        // +OK\r\n-ERR bad\r\n:100\r\n$5\r\nhello\r\n
        let buf = b"+OK\r\n-ERR bad\r\n:100\r\n$5\r\nhello\r\n";
        let elems = RespScanner::scan(buf, 4).unwrap();
        assert_eq!(elems.len(), 4);
        assert!(!elems[0].is_error);
        assert!(elems[1].is_error);
        assert!(!elems[2].is_error);
        assert!(!elems[3].is_error);
    }

    #[test]
    fn test_truncated_returns_none() {
        let buf = b"+OK\r\n$3\r\nfo"; // truncated bulk string
        assert!(RespScanner::scan(buf, 2).is_none());
    }

    #[test]
    fn test_wrong_count_returns_none() {
        let buf = b"+OK\r\n";
        assert!(RespScanner::scan(buf, 2).is_none());
    }

    #[test]
    fn test_empty_buf_returns_none() {
        assert!(RespScanner::scan(b"", 1).is_none());
    }

    #[test]
    fn test_zero_count() {
        let elems = RespScanner::scan(b"+OK\r\n", 0).unwrap();
        assert!(elems.is_empty());
    }

    #[test]
    fn test_error_kind_moved() {
        let buf = b"-MOVED 3999 127.0.0.1:6381\r\n+OK\r\n";
        let elems = RespScanner::scan(buf, 2).unwrap();
        assert!(elems[0].is_error);
        assert_eq!(elems[0].error_kind, Some("MOVED"));
        assert!(!elems[1].is_error);
        assert_eq!(elems[1].error_kind, None);
    }

    #[test]
    fn test_error_kind_ask() {
        let buf = b"-ASK 3999 127.0.0.1:6381\r\n";
        let elems = RespScanner::scan(buf, 1).unwrap();
        assert!(elems[0].is_error);
        assert_eq!(elems[0].error_kind, Some("ASK"));
    }

    #[test]
    fn test_error_kind_err() {
        let buf = b"-ERR unknown command\r\n";
        let elems = RespScanner::scan(buf, 1).unwrap();
        assert!(elems[0].is_error);
        assert_eq!(elems[0].error_kind, Some("ERR"));
    }

    #[test]
    fn resp3_only_buffers_currently_return_none() {
        for profile in resp3_load_profiles() {
            let (buf, count) = resp3_only_buffer(profile);
            assert!(
                RespScanner::scan(&buf, count).is_some(),
                "RESP3-only {} buffers should now produce per-command scan boundaries",
                profile.label()
            );
        }
    }
}
