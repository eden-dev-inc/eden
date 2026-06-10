use crate::api::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use error::{EpError, ParseError};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, PartialEq, Eq, Hash, ToSchema, JsonSchema)]
#[serde(untagged)]
pub enum RedisKey {
    String(String),
    Integer(i64, String),
    Bytes(Vec<u8>),
}

impl RedisKey {
    /// Create a new string key
    pub fn new<S: Into<String>>(key: S) -> Self {
        RedisKey::String(key.into())
    }

    /// Create a new binary key
    pub fn bytes(key: Vec<u8>) -> Self {
        RedisKey::Bytes(key)
    }

    /// Get the key as bytes
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RedisKey::Bytes(b) => b.as_slice(),
            RedisKey::Integer(_, i) => i.as_bytes(),
            RedisKey::String(s) => s.as_bytes(),
        }
    }

    /// Get the key as a string, if valid UTF-8
    pub fn as_str(&self) -> Option<&str> {
        match self {
            RedisKey::Bytes(b) => std::str::from_utf8(b).ok(),
            RedisKey::Integer(_, i) => Some(i),
            RedisKey::String(s) => Some(s),
        }
    }

    /// Returns the length of the key in bytes
    pub fn len(&self) -> usize {
        self.as_bytes().len()
    }

    /// Returns true if the key is empty
    pub fn is_empty(&self) -> bool {
        self.as_bytes().is_empty()
    }

    /// Check if the key is a string variant
    pub fn is_string(&self) -> bool {
        matches!(self, RedisKey::String(_))
    }

    /// Check if the key is a binary variant
    pub fn is_binary(&self) -> bool {
        matches!(self, RedisKey::Bytes(_))
    }

    /// Check if the key starts with the given prefix
    pub fn has_prefix<P: AsRef<[u8]>>(&self, prefix: P) -> bool {
        self.as_bytes().starts_with(prefix.as_ref())
    }

    /// Add a prefix to the key, returning a new key
    pub fn with_prefix<P: AsRef<[u8]>>(&self, prefix: P) -> Self {
        let mut new_bytes = prefix.as_ref().to_vec();
        new_bytes.extend_from_slice(self.as_bytes());

        if self.is_string()
            && std::str::from_utf8(prefix.as_ref()).is_ok()
            && let Ok(s) = String::from_utf8(new_bytes.clone())
        {
            return RedisKey::String(s);
        }
        RedisKey::Bytes(new_bytes)
    }

    /// Strip a prefix from the key, returning the remainder if present
    pub fn strip_prefix<P: AsRef<[u8]>>(&self, prefix: P) -> Option<Self> {
        let prefix = prefix.as_ref();
        if !self.has_prefix(prefix) {
            return None;
        }

        let remainder = &self.as_bytes()[prefix.len()..];

        Some(if self.is_string() {
            RedisKey::String(String::from_utf8_lossy(remainder).into_owned())
        } else {
            RedisKey::Bytes(remainder.to_vec())
        })
    }

    /// Check if the key ends with the given suffix
    pub fn has_suffix<S: AsRef<[u8]>>(&self, suffix: S) -> bool {
        self.as_bytes().ends_with(suffix.as_ref())
    }

    /// Add a suffix to the key, returning a new key
    pub fn with_suffix<S: AsRef<[u8]>>(&self, suffix: S) -> Self {
        let mut new_bytes = self.as_bytes().to_vec();
        new_bytes.extend_from_slice(suffix.as_ref());

        if self.is_string()
            && std::str::from_utf8(suffix.as_ref()).is_ok()
            && let Ok(s) = String::from_utf8(new_bytes.clone())
        {
            return RedisKey::String(s);
        }
        RedisKey::Bytes(new_bytes)
    }

    /// Strip a suffix from the key, returning the remainder if present
    pub fn strip_suffix<S: AsRef<[u8]>>(&self, suffix: S) -> Option<Self> {
        let suffix = suffix.as_ref();
        if !self.has_suffix(suffix) {
            return None;
        }

        let end = self.as_bytes().len() - suffix.len();
        let remainder = &self.as_bytes()[..end];

        Some(if self.is_string() {
            RedisKey::String(String::from_utf8_lossy(remainder).into_owned())
        } else {
            RedisKey::Bytes(remainder.to_vec())
        })
    }

    /// Extract the content between a prefix and suffix
    /// e.g., "user:123:session" with prefix "user:" and suffix ":session" returns "123"
    pub fn extract_between<P: AsRef<[u8]>, S: AsRef<[u8]>>(&self, prefix: P, suffix: S) -> Option<Self> {
        self.strip_prefix(prefix)?.strip_suffix(suffix)
    }

    /// Split the key by a delimiter, returning all parts
    pub fn split(&self, delimiter: u8) -> Vec<Self> {
        self.as_bytes()
            .split(|&b| b == delimiter)
            .map(|part| {
                if self.is_string() {
                    RedisKey::String(String::from_utf8_lossy(part).into_owned())
                } else {
                    RedisKey::Bytes(part.to_vec())
                }
            })
            .collect()
    }

    /// Get a specific segment after splitting by delimiter (0-indexed)
    pub fn segment(&self, delimiter: u8, index: usize) -> Option<Self> {
        self.split(delimiter).into_iter().nth(index)
    }

    /// Join multiple keys with a delimiter
    pub fn join<I, K>(keys: I, delimiter: u8) -> Self
    where
        I: IntoIterator<Item = K>,
        K: AsRef<[u8]>,
    {
        let parts: Vec<Vec<u8>> = keys.into_iter().map(|k| k.as_ref().to_vec()).collect();

        if parts.is_empty() {
            return RedisKey::String(String::new());
        }

        let total_len = parts.iter().map(|p| p.len()).sum::<usize>() + parts.len() - 1;
        let mut result = Vec::with_capacity(total_len);

        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                result.push(delimiter);
            }
            result.extend_from_slice(part);
        }

        match String::from_utf8(result) {
            Ok(s) => RedisKey::String(s),
            Err(e) => RedisKey::Bytes(e.into_bytes()),
        }
    }

    /// Check if the key matches a glob-style pattern
    /// Supports: * (any chars), ? (single char)
    pub fn matches_pattern(&self, pattern: &str) -> bool {
        let key = match self.as_str() {
            Some(s) => s,
            None => return false,
        };
        glob_match(pattern, key)
    }
}

/// Simple glob matching for Redis-style patterns
fn glob_match(pattern: &str, text: &str) -> bool {
    let mut p_chars = pattern.chars().peekable();
    let mut t_chars = text.chars().peekable();

    let mut p_star: Option<std::iter::Peekable<std::str::Chars>> = None;
    let mut t_star: Option<std::iter::Peekable<std::str::Chars>> = None;

    loop {
        match (p_chars.peek(), t_chars.peek()) {
            (Some('*'), _) => {
                p_chars.next();
                p_star = Some(p_chars.clone());
                t_star = Some(t_chars.clone());
            }
            (Some('?'), Some(_)) => {
                p_chars.next();
                t_chars.next();
            }
            (Some(p), Some(t)) if p == t => {
                p_chars.next();
                t_chars.next();
            }
            (None, None) => return true,
            (None, Some(_)) | (Some(_), None) | (Some(_), Some(_)) => {
                if let (Some(ps), Some(mut ts)) = (p_star.clone(), t_star.clone()) {
                    ts.next();
                    if ts.peek().is_none() && p_chars.peek().is_some() {
                        return false;
                    }
                    t_star = Some(ts.clone());
                    p_chars = ps;
                    t_chars = ts;
                } else {
                    return false;
                }
            }
        }
    }
}

impl Display for RedisKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisKey::Bytes(b) => write!(f, "{}", String::from_utf8_lossy(b)),
            RedisKey::Integer(i, _) => write!(f, "{}", i),
            RedisKey::String(s) => write!(f, "{}", s),
        }
    }
}

impl Default for RedisKey {
    fn default() -> Self {
        RedisKey::String(String::new())
    }
}

impl AsRef<[u8]> for RedisKey {
    fn as_ref(&self) -> &[u8] {
        self.as_bytes()
    }
}

impl From<String> for RedisKey {
    fn from(s: String) -> Self {
        RedisKey::String(s)
    }
}

impl From<&String> for RedisKey {
    fn from(s: &String) -> Self {
        RedisKey::String(s.to_string())
    }
}

impl From<&str> for RedisKey {
    fn from(s: &str) -> Self {
        RedisKey::String(s.to_string())
    }
}

impl From<i64> for RedisKey {
    fn from(i: i64) -> Self {
        RedisKey::Integer(i, i.to_string())
    }
}

impl From<Vec<u8>> for RedisKey {
    fn from(b: Vec<u8>) -> Self {
        RedisKey::Bytes(b)
    }
}

impl From<&[u8]> for RedisKey {
    fn from(b: &[u8]) -> Self {
        RedisKey::Bytes(b.to_vec())
    }
}

/// Error type for invalid RedisJsonValue to RedisKey conversions
#[derive(Debug, Clone, PartialEq)]
pub enum KeyConversionError {
    InvalidType(&'static str),
    FloatNotAllowed,
}

impl std::fmt::Display for KeyConversionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KeyConversionError::InvalidType(t) => write!(f, "Cannot convert {} to RedisKey", t),
            KeyConversionError::FloatNotAllowed => {
                write!(f, "Float values cannot be used as Redis keys (precision issues)")
            }
        }
    }
}

impl std::error::Error for KeyConversionError {}

/// RedisKey -> RedisJsonValue (infallible)
impl From<RedisKey> for RedisJsonValue {
    fn from(key: RedisKey) -> Self {
        match key {
            RedisKey::Bytes(b) => RedisJsonValue::Bytes(b),
            RedisKey::Integer(i, _) => RedisJsonValue::Integer(i),
            RedisKey::String(s) => RedisJsonValue::String(s),
        }
    }
}

impl From<&RedisKey> for RedisJsonValue {
    fn from(key: &RedisKey) -> Self {
        RedisJsonValue::from(key.clone())
    }
}

/// RedisJsonValue -> RedisKey (fallible)
impl TryFrom<RedisJsonValue> for RedisKey {
    type Error = EpError;

    fn try_from(value: RedisJsonValue) -> Result<Self, Self::Error> {
        match value {
            RedisJsonValue::String(s) => Ok(RedisKey::String(s)),
            RedisJsonValue::Integer(i) => Ok(RedisKey::String(i.to_string())),
            RedisJsonValue::Bool(b) => Ok(RedisKey::String(b.to_string())),
            RedisJsonValue::Null => Ok(RedisKey::String("nil".to_string())),
            RedisJsonValue::Bytes(b) => Ok(RedisKey::Bytes(b)),
            RedisJsonValue::Float(_) => Err(EpError::Parse(ParseError::Custom("Failed to parse redis key from float".into()))),
            RedisJsonValue::Array(_) => Err(EpError::Parse(ParseError::Custom("Failed to parse redis key from array".into()))),
            RedisJsonValue::Object(_) => Err(EpError::Parse(ParseError::Custom("Failed to parse redis key from object".into()))),
        }
    }
}

impl TryFrom<&RedisJsonValue> for RedisKey {
    type Error = EpError;

    fn try_from(value: &RedisJsonValue) -> Result<Self, Self::Error> {
        RedisKey::try_from(value.clone())
    }
}

impl RedisKey {
    /// Convert from RedisJsonValue, stringifying any type (lossy)
    pub fn from_value_lossy(value: RedisJsonValue) -> Self {
        match value {
            RedisJsonValue::String(s) => RedisKey::String(s),
            RedisJsonValue::Integer(i) => RedisKey::String(i.to_string()),
            RedisJsonValue::Bool(b) => RedisKey::String(b.to_string()),
            RedisJsonValue::Null => RedisKey::String("nil".to_string()),
            RedisJsonValue::Float(f) => RedisKey::String(f.to_string()),
            RedisJsonValue::Bytes(b) => RedisKey::Bytes(b),
            RedisJsonValue::Array(a) => RedisKey::String(serde_json::to_string(&a).unwrap_or_default()),
            RedisJsonValue::Object(o) => RedisKey::String(serde_json::to_string(&o).unwrap_or_default()),
        }
    }

    /// Parse key as RedisJsonValue, detecting integers/bools/null
    pub fn to_value_parsed(&self) -> RedisJsonValue {
        let s = match self.as_str() {
            Some(s) => s,
            None => return RedisJsonValue::Bytes(self.as_bytes().to_vec()),
        };

        match s {
            "nil" | "null" => RedisJsonValue::Null,
            "true" => RedisJsonValue::Bool(true),
            "false" => RedisJsonValue::Bool(false),
            _ => s.parse::<i64>().map(RedisJsonValue::Integer).unwrap_or_else(|_| RedisJsonValue::String(s.to_string())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefix_operations() {
        let key = RedisKey::new("user:123:profile");

        assert!(key.has_prefix("user:"));
        assert!(!key.has_prefix("admin:"));

        let stripped = key.strip_prefix("user:").unwrap();
        assert_eq!(stripped.as_str(), Some("123:profile"));

        let prefixed = key.with_prefix("prod:");
        assert_eq!(prefixed.as_str(), Some("prod:user:123:profile"));
    }

    #[test]
    fn test_suffix_operations() {
        let key = RedisKey::new("user:123:profile");

        assert!(key.has_suffix(":profile"));
        assert!(!key.has_suffix(":session"));

        let stripped = key.strip_suffix(":profile").unwrap();
        assert_eq!(stripped.as_str(), Some("user:123"));

        let suffixed = key.with_suffix(":v2");
        assert_eq!(suffixed.as_str(), Some("user:123:profile:v2"));
    }

    #[test]
    fn test_extract_between() {
        let key = RedisKey::new("user:123:session");
        let extracted = key.extract_between("user:", ":session").unwrap();
        assert_eq!(extracted.as_str(), Some("123"));

        let no_match = key.extract_between("admin:", ":session");
        assert!(no_match.is_none());
    }

    #[test]
    fn test_split_and_segment() {
        let key = RedisKey::new("user:123:profile:settings");

        let parts = key.split(b':');
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[0].as_str(), Some("user"));
        assert_eq!(parts[2].as_str(), Some("profile"));

        let segment = key.segment(b':', 1).unwrap();
        assert_eq!(segment.as_str(), Some("123"));
    }

    #[test]
    fn test_join() {
        let key = RedisKey::join(["user", "123", "profile"], b':');
        assert_eq!(key.as_str(), Some("user:123:profile"));
    }

    #[test]
    fn test_pattern_matching() {
        let key = RedisKey::new("user:123:session");

        assert!(key.matches_pattern("user:*:session"));
        assert!(key.matches_pattern("user:???:session"));
        assert!(key.matches_pattern("*"));
        assert!(!key.matches_pattern("admin:*"));
    }

    #[test]
    fn test_binary_key() {
        let binary_key = RedisKey::bytes(vec![0x00, 0x01, 0x02, 0xFF]);

        assert!(binary_key.is_binary());
        assert_eq!(binary_key.len(), 4);
        assert!(binary_key.has_prefix([0x00, 0x01]));

        let stripped = binary_key.strip_prefix([0x00]).unwrap();
        assert_eq!(stripped.as_bytes(), &[0x01, 0x02, 0xFF]);
    }
}
