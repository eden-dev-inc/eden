//! Redis request identity, audit, and service-name helpers.

use super::*;

pub(crate) struct RedisRequestMetadata;

impl RedisRequestMetadata {
    /// FNV-1a hash for hot-key tracking. Returns non-zero.
    #[inline]
    #[cfg(test)]
    pub(crate) fn fnv1a_hash(bytes: &[u8]) -> u64 {
        let mut hash: u64 = 0xcbf29ce484222325;
        for &b in bytes {
            hash ^= b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        if hash == 0 { 1 } else { hash }
    }

    #[cfg(test)]
    pub(crate) fn normalize_service_name(value: &str) -> Option<String> {
        let trimmed = value.trim();
        if trimmed.is_empty() { None } else { Some(trimmed.to_string()) }
    }

    pub(super) fn value_to_string(value: &RedisJsonValue) -> Option<String> {
        match value {
            RedisJsonValue::String(s) => Some(s.clone()),
            RedisJsonValue::Bytes(bytes) => String::from_utf8(bytes.clone()).ok(),
            _ => None,
        }
    }

    #[cfg(test)]
    fn key_to_string(key: &RedisKey) -> Option<String> {
        key.as_str().map(|value| value.to_string()).or_else(|| Some(String::from_utf8_lossy(key.as_bytes()).to_string()))
    }

    #[cfg(test)]
    pub(crate) fn audit_key_from_args(command: &RedisApi, args: &[RedisJsonValue]) -> Option<String> {
        if let Ok(keys) = command.keys_from_args(args)
            && let Some(key) = keys.into_iter().next()
        {
            return Self::key_to_string(&key);
        }
        args.first().and_then(Self::value_to_string)
    }

    #[cfg(test)]
    pub(crate) fn audit_args_hash(command_bytes: &[u8]) -> u64 {
        let mut hasher = DefaultHasher::new();
        command_bytes.hash(&mut hasher);
        hasher.finish()
    }
}
