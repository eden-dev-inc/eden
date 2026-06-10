use actix_web::http::header::HeaderMap;
use actix_web::web;
use std::str::FromStr;

pub const ORG_ID_HEADER: &str = "X-Org-Id";
pub const ORG_UUID_HEADER: &str = "X-Org-Uuid";

pub const HEADER_MAX_ATTEMPTS: &str = "X-Eden-Max-Attempts";
pub const DEFAULT_MAX_ATTEMPTS: u8 = 1;
pub const HEADER_RETRY_DELAY: &str = "X-Eden-Retry-Delay";
pub const DEFAULT_RETRY_DELAY: u64 = 0;
pub const HEADER_MAX_TIMEOUT: &str = "X-Eden-Max-Timeout";
pub const DEFAULT_MAX_TIMEOUT: u64 = 10_000;
pub const HEADER_VERBOSE: &str = "X-Eden-Verbose";
pub const DEFAULT_VERBOSE: bool = false;
pub const HEADER_OUTPUT: &str = "X-Eden-Output";
pub const DEFAULT_OUTPUT: bool = false;
pub const HEADER_TEST: &str = "X-Eden-Test";
pub const DEFAULT_TEST: bool = false;
pub const HEADER_MAX_CONCURRENT_CONNECTIONS: &str = "X-Eden-Max-Concurrent-Connections";
pub const DEFAULT_MAX_CONCURRENT_CONNECTIONS: u32 = 256;

pub fn from_headers<T: FromStr>(headers: &web::Header<HeaderMap>, header_name: &str, default_value: T) -> T {
    headers.get(header_name).and_then(|v| v.to_str().ok()).and_then(|s| s.parse().ok()).unwrap_or(default_value)
}
