use actix_web::http::header::HeaderMap;
use actix_web::web;
use borsh::{BorshDeserialize, BorshSerialize};
use opentelemetry::KeyValue;
use request::*;
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use utoipa::ToSchema;

/// The settings passed as part of each request
#[derive(Debug, Serialize, Deserialize, BorshDeserialize, BorshSerialize, PartialEq, Clone, Copy, ToSchema)]

pub struct EdenSettings {
    max_attempts: Option<u8>,                // if a request failed, sets the number of max retry attempts (default: 3)
    retry_delay: Option<u64>,                // time between retry attempts in milliseconds (default: 0)
    max_timeout: Option<u64>,                // length of time in milliseconds a request can run until it is timed out (default: 120,000)
    verbose: Option<bool>, // false (default) - limited output from endpoint, true - verbose output with additional requests metrics
    output: Option<bool>,  // false (default) - JSON, true - Bytes
    test: Option<bool>,    // false (default) - true, run as test
    max_concurrent_connections: Option<u32>, // max concurrent connections for interlays (default: 256)
}

impl From<&HeaderMap> for EdenSettings {
    fn from(header_map: &HeaderMap) -> EdenSettings {
        let mut settings = EdenSettings::default();

        if let Some(attempts) = settings.update_field(header_map, HEADER_MAX_ATTEMPTS) {
            settings.max_attempts.replace(attempts);
        }

        if let Some(delay) = settings.update_field(header_map, HEADER_RETRY_DELAY) {
            settings.retry_delay.replace(delay);
        }

        if let Some(timeout) = settings.update_field(header_map, HEADER_MAX_TIMEOUT) {
            settings.max_timeout.replace(timeout);
        }

        if let Some(verbose) = settings.update_field(header_map, HEADER_VERBOSE) {
            settings.verbose.replace(verbose);
        }

        if let Some(output) = settings.update_field(header_map, HEADER_OUTPUT) {
            settings.output.replace(output);
        }

        if let Some(test) = settings.update_field(header_map, HEADER_TEST) {
            settings.test.replace(test);
        }

        if let Some(max_concurrent_connections) = settings.update_field(header_map, HEADER_MAX_CONCURRENT_CONNECTIONS) {
            settings.max_concurrent_connections.replace(max_concurrent_connections);
        }

        settings
    }
}

impl EdenSettings {
    /// Get max attempts, with default 1 if none
    pub fn max_attempts(&self) -> u8 {
        self.max_attempts.map_or(DEFAULT_MAX_ATTEMPTS, |max| max)
    }

    pub fn max_attempts_header(headers: &HeaderMap) -> u8 {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_MAX_ATTEMPTS).unwrap_or(DEFAULT_MAX_ATTEMPTS)
    }

    /// Get retry delay, with default 0ms if none
    pub fn retry_delay(&self) -> u64 {
        self.retry_delay.map_or(DEFAULT_RETRY_DELAY, |delay| delay)
    }

    pub fn retry_delay_header(headers: &HeaderMap) -> u64 {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_RETRY_DELAY).unwrap_or(DEFAULT_RETRY_DELAY)
    }

    /// Get max timeout in milliseconds, with 10,000ms if none
    pub fn max_timeout(&self) -> u64 {
        self.max_timeout.map_or(DEFAULT_MAX_TIMEOUT, |timeout| timeout)
    }

    pub fn max_timeout_duration(&self) -> Duration {
        Duration::from_millis(self.max_timeout())
    }

    pub fn max_timeout_header(headers: &HeaderMap) -> u64 {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_MAX_TIMEOUT).unwrap_or(DEFAULT_MAX_TIMEOUT)
    }

    /// Get verbose mode, with default false if none
    pub fn verbose(&self) -> bool {
        self.verbose.map_or(DEFAULT_VERBOSE, |bool| bool)
    }

    pub fn verbose_header(headers: &HeaderMap) -> bool {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_VERBOSE).unwrap_or(DEFAULT_VERBOSE)
    }

    pub fn output(&self) -> bool {
        self.output.map_or(DEFAULT_OUTPUT, |bool| bool)
    }

    pub fn output_header(headers: &HeaderMap) -> bool {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_OUTPUT).unwrap_or(DEFAULT_OUTPUT)
    }

    pub fn test(&self) -> bool {
        self.output.map_or(DEFAULT_TEST, |bool| bool)
    }

    pub fn test_header(headers: &HeaderMap) -> bool {
        let mut settings = Self::default();
        settings.update_field(headers, HEADER_TEST).unwrap_or(DEFAULT_TEST)
    }

    /// Get max concurrent connections, with default 256 if none
    pub fn max_concurrent_connections(&self) -> u32 {
        self.max_concurrent_connections.map_or(DEFAULT_MAX_CONCURRENT_CONNECTIONS, |max| max)
    }

    pub fn labels(&self) -> Vec<KeyValue> {
        vec![
            self.max_attempts.as_ref().map(|v| KeyValue::new("max_attempts", v.to_string())),
            self.retry_delay.as_ref().map(|v| KeyValue::new("retry_delay", v.to_string())),
            self.max_timeout.as_ref().map(|v| KeyValue::new("max_timeout", v.to_string())),
            self.verbose.as_ref().map(|v| KeyValue::new("verbose", v.to_string())),
            self.output.as_ref().map(|v| KeyValue::new("output", v.to_string())),
            self.test.as_ref().map(|v| KeyValue::new("test", v.to_string())),
            self.max_concurrent_connections.as_ref().map(|v| KeyValue::new("max_concurrent_connections", v.to_string())),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    fn update_field<T>(&mut self, header_map: &HeaderMap, key: &str) -> Option<T>
    where
        T: FromStr,
    {
        header_map.get(key).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<T>().ok())
    }
}

impl Default for EdenSettings {
    fn default() -> EdenSettings {
        Self {
            max_attempts: Some(DEFAULT_MAX_ATTEMPTS),
            retry_delay: Some(DEFAULT_RETRY_DELAY),
            max_timeout: Some(DEFAULT_MAX_TIMEOUT),
            verbose: Some(DEFAULT_VERBOSE),
            output: Some(DEFAULT_OUTPUT),
            test: Some(DEFAULT_TEST),
            max_concurrent_connections: Some(DEFAULT_MAX_CONCURRENT_CONNECTIONS),
        }
    }
}

impl From<web::Header<HeaderMap>> for EdenSettings {
    fn from(headers: web::Header<HeaderMap>) -> EdenSettings {
        Self {
            max_attempts: Some(from_headers(&headers, HEADER_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS)),
            retry_delay: Some(from_headers(&headers, HEADER_RETRY_DELAY, DEFAULT_RETRY_DELAY)),
            max_timeout: Some(from_headers(&headers, HEADER_MAX_TIMEOUT, DEFAULT_MAX_TIMEOUT)),
            verbose: Some(from_headers(&headers, HEADER_VERBOSE, DEFAULT_VERBOSE)),
            output: Some(from_headers(&headers, HEADER_OUTPUT, DEFAULT_OUTPUT)),
            test: Some(from_headers(&headers, HEADER_TEST, DEFAULT_TEST)),
            max_concurrent_connections: Some(from_headers(&headers, HEADER_MAX_CONCURRENT_CONNECTIONS, DEFAULT_MAX_CONCURRENT_CONNECTIONS)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::http::header::{HeaderName, HeaderValue};

    #[test]
    fn max_timeout_header_accepts_zero_without_flooring() {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_bytes(HEADER_MAX_TIMEOUT.as_bytes()).expect("header name should parse"),
            HeaderValue::from_static("0"),
        );

        let settings = EdenSettings::from(&headers);
        assert_eq!(settings.max_timeout(), 0);
        assert_eq!(settings.max_timeout_duration(), Duration::ZERO);
    }

    #[test]
    fn max_timeout_defaults_when_header_is_absent() {
        let headers = HeaderMap::new();
        let settings = EdenSettings::from(&headers);

        assert_eq!(settings.max_timeout(), DEFAULT_MAX_TIMEOUT);
        assert_eq!(settings.max_timeout_duration(), Duration::from_millis(DEFAULT_MAX_TIMEOUT));
    }
}
