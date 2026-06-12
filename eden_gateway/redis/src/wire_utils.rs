//! TTL detection helpers shared across proxy paths.

use endpoints::endpoint::ep_redis::api::RedisJsonValue;

pub(crate) struct RedisTtl;

impl RedisTtl {
    /// True if the command carries a TTL (SETEX, PSETEX, or SET with EX/PX/EXAT/PXAT/KEEPTTL).
    #[inline]
    pub(crate) fn has_ttl_flag(cmd_upper: &str, args: &[RedisJsonValue]) -> bool {
        match cmd_upper {
            "SETEX" | "PSETEX" => true,
            "SET" => Self::args_contain_ttl_token(args),
            _ => false,
        }
    }

    /// TTL check when only the command name is available (no parsed args).
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn has_ttl_flag_cmd_only(cmd_upper: &str) -> bool {
        matches!(cmd_upper, "SETEX" | "PSETEX")
    }

    /// Scan SET args for TTL tokens. SET has at most 6 optional tokens after key+value.
    fn args_contain_ttl_token(args: &[RedisJsonValue]) -> bool {
        // SET key value [EX seconds | PX ms | EXAT ts | PXAT ts | KEEPTTL] [NX|XX] [GET]
        // Args[0] = key, Args[1] = value, optional tokens start at index 2.
        for arg in args.iter().skip(2) {
            let s = match arg {
                RedisJsonValue::String(s) => s.as_str(),
                RedisJsonValue::Bytes(b) => match std::str::from_utf8(b) {
                    Ok(s) => s,
                    Err(_) => continue,
                },
                _ => continue,
            };
            if s.eq_ignore_ascii_case("EX")
                || s.eq_ignore_ascii_case("PX")
                || s.eq_ignore_ascii_case("EXAT")
                || s.eq_ignore_ascii_case("PXAT")
                || s.eq_ignore_ascii_case("KEEPTTL")
            {
                return true;
            }
        }
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoints::endpoint::ep_redis::api::RedisApi;

    fn str_arg(s: &str) -> RedisJsonValue {
        RedisJsonValue::String(s.to_string())
    }

    #[test]
    fn test_setex_always_has_ttl() {
        assert!(RedisTtl::has_ttl_flag("SETEX", &[]));
        assert!(RedisTtl::has_ttl_flag("PSETEX", &[]));
    }

    #[test]
    fn test_set_with_ex() {
        let args = vec![str_arg("mykey"), str_arg("myval"), str_arg("EX"), str_arg("60")];
        assert!(RedisTtl::has_ttl_flag("SET", &args));
    }

    #[test]
    fn test_set_with_px() {
        let args = vec![str_arg("mykey"), str_arg("myval"), str_arg("px"), str_arg("5000")];
        assert!(RedisTtl::has_ttl_flag("SET", &args));
    }

    #[test]
    fn test_set_without_ttl() {
        let args = vec![str_arg("mykey"), str_arg("myval")];
        assert!(!RedisTtl::has_ttl_flag("SET", &args));
    }

    #[test]
    fn test_set_with_nx_only() {
        let args = vec![str_arg("mykey"), str_arg("myval"), str_arg("NX")];
        assert!(!RedisTtl::has_ttl_flag("SET", &args));
    }

    #[test]
    fn test_set_with_keepttl() {
        let args = vec![str_arg("mykey"), str_arg("myval"), str_arg("KEEPTTL")];
        assert!(RedisTtl::has_ttl_flag("SET", &args));
    }

    #[test]
    fn test_get_never_has_ttl() {
        assert!(!RedisTtl::has_ttl_flag("GET", &[]));
    }

    #[test]
    fn test_cmd_only_detection() {
        assert!(RedisTtl::has_ttl_flag_cmd_only("SETEX"));
        assert!(RedisTtl::has_ttl_flag_cmd_only("PSETEX"));
        assert!(!RedisTtl::has_ttl_flag_cmd_only("SET"));
        assert!(!RedisTtl::has_ttl_flag_cmd_only("GET"));
    }

    #[test]
    fn test_redis_api_display_is_uppercase() {
        assert_eq!(RedisApi::Set.to_string(), "SET");
        assert_eq!(RedisApi::Setex.to_string(), "SETEX");
        assert_eq!(RedisApi::Psetex.to_string(), "PSETEX");
    }
}
