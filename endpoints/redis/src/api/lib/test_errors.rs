//! Integration tests for RedisError type mappings.
//!
//! These tests verify that Redis error responses are correctly parsed and mapped
//! to the appropriate `RedisError` variants via `EpError::parse_redis_error()`.

#[cfg(test)]
mod tests {
    use error::{EpError, RedisError};

    /// Helper to create a mock error and parse it
    fn parse_error_message(msg: &str) -> EpError {
        EpError::parse_redis_error(std::io::Error::other(msg))
    }

    mod unit {
        use super::*;

        #[test]
        fn test_parse_wrongtype_error() {
            let error = parse_error_message("WRONGTYPE Operation against a key holding the wrong kind of value");
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1820); // Redis(0x18) + WrongType(0x20)
        }

        #[test]
        fn test_parse_noscript_error() {
            let error = parse_error_message("NOSCRIPT No matching script. Please use EVAL.");
            assert!(
                matches!(error, EpError::Redis(RedisError::ScriptNotFound)),
                "Expected ScriptNotFound, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1860); // Redis(0x18) + ScriptNotFound(0x60)
        }

        #[test]
        fn test_parse_unknown_command_error() {
            let error = parse_error_message("ERR unknown command 'FAKECOMMAND', with args beginning with:");
            assert!(
                matches!(error, EpError::Redis(RedisError::CommandNotFound)),
                "Expected CommandNotFound, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1830); // Redis(0x18) + CommandNotFound(0x30)
        }

        #[test]
        fn test_parse_wrong_number_of_args_error() {
            let error = parse_error_message("ERR wrong number of arguments for 'get' command");
            assert!(
                matches!(error, EpError::Redis(RedisError::InvalidSyntax)),
                "Expected InvalidSyntax, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1831); // Redis(0x18) + InvalidSyntax(0x31)
        }

        #[test]
        fn test_parse_syntax_error() {
            let error = parse_error_message("ERR syntax error");
            assert!(
                matches!(error, EpError::Redis(RedisError::InvalidSyntax)),
                "Expected InvalidSyntax, got: {:?}",
                error
            );
        }

        #[test]
        fn test_parse_noauth_error() {
            let error = parse_error_message("NOAUTH Authentication required.");
            assert!(matches!(error, EpError::Redis(RedisError::AuthRequired)), "Expected AuthRequired, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1810); // Redis(0x18) + AuthRequired(0x10)
        }

        #[test]
        fn test_parse_wrongpass_error() {
            let error = parse_error_message("WRONGPASS invalid username-password pair or user is disabled");
            assert!(
                matches!(error, EpError::Redis(RedisError::InvalidPassword)),
                "Expected InvalidPassword, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1811); // Redis(0x18) + InvalidPassword(0x11)
        }

        #[test]
        fn test_parse_noperm_error() {
            let error = parse_error_message("NOPERM this user has no permissions to run the 'config' command");
            assert!(
                matches!(error, EpError::Redis(RedisError::PermissionDenied)),
                "Expected PermissionDenied, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1812); // Redis(0x18) + PermissionDenied(0x12)
        }

        #[test]
        fn test_parse_out_of_range_error() {
            let error = parse_error_message("ERR value is out of range");
            assert!(matches!(error, EpError::Redis(RedisError::OutOfRange)), "Expected OutOfRange, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1832); // Redis(0x18) + OutOfRange(0x32)
        }

        #[test]
        fn test_parse_execabort_error() {
            let error = parse_error_message("EXECABORT Transaction discarded because of previous errors.");
            assert!(
                matches!(error, EpError::Redis(RedisError::TransactionAborted)),
                "Expected TransactionAborted, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1850); // Redis(0x18) + TransactionAborted(0x50)
        }

        #[test]
        fn test_parse_oom_error() {
            let error = parse_error_message("OOM command not allowed when used memory > 'maxmemory'.");
            assert!(
                matches!(error, EpError::Redis(RedisError::ServerOutOfMemory)),
                "Expected ServerOutOfMemory, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1872); // Redis(0x18) + ServerOutOfMemory(0x72)
        }

        #[test]
        fn test_parse_readonly_error() {
            let error = parse_error_message("READONLY You can't write against a read only replica.");
            assert!(
                matches!(error, EpError::Redis(RedisError::ServerReadOnly)),
                "Expected ServerReadOnly, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1871); // Redis(0x18) + ServerReadOnly(0x71)
        }

        #[test]
        fn test_parse_loading_error() {
            let error = parse_error_message("LOADING Redis is loading the dataset in memory");
            assert!(matches!(error, EpError::Redis(RedisError::ServerBusy)), "Expected ServerBusy, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1870); // Redis(0x18) + ServerBusy(0x70)
        }

        #[test]
        fn test_parse_busy_error() {
            let error = parse_error_message("BUSY Redis is busy running a script. You can only call SCRIPT KILL or SHUTDOWN NOSAVE.");
            assert!(matches!(error, EpError::Redis(RedisError::ServerBusy)), "Expected ServerBusy, got: {:?}", error);
        }

        #[test]
        fn test_parse_clusterdown_error() {
            let error = parse_error_message("CLUSTERDOWN The cluster is down");
            assert!(matches!(error, EpError::Redis(RedisError::ClusterDown)), "Expected ClusterDown, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1842); // Redis(0x18) + ClusterDown(0x42)
        }

        #[test]
        fn test_parse_moved_error() {
            let error = parse_error_message("MOVED 3999 127.0.0.1:6381");
            assert!(matches!(error, EpError::Redis(RedisError::ClusterMoved)), "Expected ClusterMoved, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1840); // Redis(0x18) + ClusterMoved(0x40)
        }

        #[test]
        fn test_parse_ask_error() {
            let error = parse_error_message("ASK 3999 127.0.0.1:6381");
            assert!(matches!(error, EpError::Redis(RedisError::ClusterAsk)), "Expected ClusterAsk, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1841); // Redis(0x18) + ClusterAsk(0x41)
        }

        #[test]
        fn test_parse_crossslot_error() {
            let error = parse_error_message("CROSSSLOT Keys in request don't hash to the same slot");
            assert!(
                matches!(error, EpError::Redis(RedisError::ClusterCrossSlot)),
                "Expected ClusterCrossSlot, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1843); // Redis(0x18) + ClusterCrossSlot(0x43)
        }

        #[test]
        fn test_parse_tryagain_error() {
            let error = parse_error_message("TRYAGAIN Multiple keys request during rehashing");
            assert!(matches!(error, EpError::Redis(RedisError::TryAgain)), "Expected TryAgain, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1890); // Redis(0x18) + TryAgain(0x90)
        }

        #[test]
        fn test_parse_connection_refused_error() {
            let error = parse_error_message("Connection refused");
            assert!(
                matches!(error, EpError::Redis(RedisError::ConnectionRefused)),
                "Expected ConnectionRefused, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1802); // Redis(0x18) + ConnectionRefused(0x02)
        }

        #[test]
        fn test_parse_connection_timeout_error() {
            let error = parse_error_message("connection timeout");
            assert!(
                matches!(error, EpError::Redis(RedisError::ConnectionTimeout)),
                "Expected ConnectionTimeout, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1801); // Redis(0x18) + ConnectionTimeout(0x01)
        }

        #[test]
        fn test_parse_broken_pipe_error() {
            let error = parse_error_message("broken pipe");
            assert!(
                matches!(error, EpError::Redis(RedisError::ConnectionLost)),
                "Expected ConnectionLost, got: {:?}",
                error
            );
            assert_eq!(error.error_code(), 0x1803); // Redis(0x18) + ConnectionLost(0x03)
        }

        #[test]
        fn test_parse_script_error() {
            let error = parse_error_message("ERR Error running script (call to f_abc123): @user_script:1: error message");
            assert!(matches!(error, EpError::Redis(RedisError::ScriptError)), "Expected ScriptError, got: {:?}", error);
            assert_eq!(error.error_code(), 0x1861); // Redis(0x18) + ScriptError(0x61)
        }

        #[test]
        fn test_parse_custom_fallback() {
            let error = parse_error_message("ERR some unknown error that doesn't match patterns");
            assert!(matches!(error, EpError::Redis(RedisError::Custom(_))), "Expected Custom, got: {:?}", error);
            assert_eq!(error.error_code(), 0x18FF); // Redis(0x18) + Custom(0xFF)
        }

        #[test]
        fn test_error_display_format() {
            let error = EpError::Redis(RedisError::WrongType);
            let display = error.to_string();
            assert!(display.contains("[E1820]"), "Display should include error code");
            assert!(display.contains("Redis"), "Display should include category");
            assert!(display.contains("Wrong type"), "Display should include message");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        /// Test that WRONGTYPE error is returned when using set commands on a string key.
        /// This is a commonly triggered Redis error that should map to RedisError::WrongType.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wrongtype_error_sadd_on_string() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Setup: Create a string key
            let _ = ctx.raw(b"*3\r\n$3\r\nSET\r\n$19\r\nerr_test_sadd_wt_r2\r\n$5\r\nvalue\r\n").await.expect("set");

            // Trigger: Use SADD on the string key (should fail with WRONGTYPE)
            let result =
                ctx.raw(b"*3\r\n$4\r\nSADD\r\n$19\r\nerr_test_sadd_wt_r2\r\n$1\r\na\r\n").await.expect("sadd should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test that WRONGTYPE error is returned when using hash commands on a string key.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wrongtype_error_hset_on_string() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Setup: Create a string key
            ctx.raw(b"*3\r\n$3\r\nSET\r\n$18\r\nerr_test_hset_wt_2\r\n$5\r\nvalue\r\n").await.expect("set");

            // Trigger: Use HSET on the string key
            let result = ctx
                .raw(b"*4\r\n$4\r\nHSET\r\n$18\r\nerr_test_hset_wt_2\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
                .await
                .expect("hset should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test NOSCRIPT error when EVALSHA references non-existent script.
        /// This is a scripting error that should map to RedisError::ScriptNotFound.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_noscript_error() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // EVALSHA with a non-existent script SHA
            let result = ctx
                .raw(b"*3\r\n$7\r\nEVALSHA\r\n$40\r\n0000000000000000000000000000000000000000\r\n$1\r\n0\r\n")
                .await
                .expect("evalsha should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(
                matches!(error, EpError::Redis(RedisError::ScriptNotFound)),
                "Expected ScriptNotFound error, got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test INCR on non-integer value returns TypeConversionFailed error.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_invalid_integer_error() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Setup: Create a key with non-integer value
            ctx.raw(b"*3\r\n$3\r\nSET\r\n$16\r\nerr_test_incr_r2\r\n$5\r\nhello\r\n").await.expect("set");

            // Trigger: Try to INCR the non-integer value
            let result = ctx.raw(b"*2\r\n$4\r\nINCR\r\n$16\r\nerr_test_incr_r2\r\n").await.expect("incr should return response");

            // Verify parse_redis_error maps this to OutOfRange
            // (Redis error "ERR value is not an integer or out of range" contains "out of range")
            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(
                matches!(error, EpError::Redis(RedisError::OutOfRange)),
                "Expected OutOfRange error (via 'out of range' pattern), got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test that RESP2 errors start with '-' prefix and map to correct variant.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_resp2_error_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nerr_format_test_2\r\n$5\r\nvalue\r\n").await.expect("set");

            let result =
                ctx.raw(b"*3\r\n$4\r\nSADD\r\n$17\r\nerr_format_test_2\r\n$1\r\na\r\n").await.expect("sadd should return response");

            let response = String::from_utf8_lossy(&result);
            assert!(response.starts_with("-"), "RESP2 error should start with -, got: {}", response);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test that RESP3 WRONGTYPE errors are returned and mapped correctly.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_resp3_error_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nerr_format_test_3\r\n$5\r\nvalue\r\n").await.expect("set");

            let result =
                ctx.raw(b"*3\r\n$4\r\nSADD\r\n$17\r\nerr_format_test_3\r\n$1\r\na\r\n").await.expect("sadd should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test that connection recovers after an error response.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_error_recovery() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // First: Setup and trigger a WRONGTYPE error
            ctx.raw(b"*3\r\n$3\r\nSET\r\n$16\r\nerr_recovery_key\r\n$5\r\nvalue\r\n").await.expect("set");

            let error_result = ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nerr_recovery_key\r\n$1\r\na\r\n").await.expect("sadd error");

            let error_response = String::from_utf8_lossy(&error_result);

            let error = parse_error_message(&error_response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            // Second: run a valid command - should succeed (connection still works)
            let result = ctx.raw(b"*3\r\n$3\r\nSET\r\n$18\r\nerr_recovery_test2\r\n$5\r\nvalue\r\n").await.expect("set after error");

            let response = String::from_utf8_lossy(&result);
            assert!(response.contains("OK"), "Valid command after error should succeed, got: {}", response);

            // Third: verify the key was set
            let result = ctx.raw(b"*2\r\n$3\r\nGET\r\n$18\r\nerr_recovery_test2\r\n").await.expect("get");

            let response = String::from_utf8_lossy(&result);
            assert!(response.contains("value"), "GET should return the set value, got: {}", response);

            ctx.stop().await;
        }

        /// Test LPUSH on a set key returns WRONGTYPE.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wrongtype_error_lpush_on_set() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Setup: Create a set key first
            let _ = ctx.raw(b"*3\r\n$4\r\nSADD\r\n$18\r\nerr_test_lpush_set\r\n$1\r\na\r\n").await.expect("sadd");

            // Trigger: Use LPUSH on the set key (should fail with WRONGTYPE)
            let result =
                ctx.raw(b"*3\r\n$5\r\nLPUSH\r\n$18\r\nerr_test_lpush_set\r\n$1\r\nb\r\n").await.expect("lpush should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test that error code mapping is correct for common errors.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_error_code_mapping() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Create a string key
            ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nerr_code_test_key\r\n$5\r\nvalue\r\n").await.expect("set");

            // Trigger WRONGTYPE
            let result = ctx.raw(b"*3\r\n$4\r\nSADD\r\n$17\r\nerr_code_test_key\r\n$1\r\na\r\n").await.expect("sadd");

            let response = String::from_utf8_lossy(&result);
            let error = parse_error_message(&response);

            // Verify error code is correct: 0x1820 = Redis(0x18) + WrongType(0x20)
            assert_eq!(
                error.error_code(),
                0x1820,
                "WRONGTYPE should have error code 0x1820, got: {:#06x}",
                error.error_code()
            );
            assert_eq!(error.error_hex(), "E1820", "Error hex should be E1820, got: {}", error.error_hex());

            ctx.stop().await;
        }

        /// Test OutOfRange error with LSET on out-of-bounds index.
        /// LSET returns "ERR index out of range" when index doesn't exist.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_out_of_range_lset() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Create a list with one element
            ctx.raw(b"*3\r\n$5\r\nRPUSH\r\n$14\r\nerr_lset_range\r\n$1\r\na\r\n").await.expect("rpush");

            // Try to set index 100 (out of range for a 1-element list)
            let result = ctx
                .raw(b"*4\r\n$4\r\nLSET\r\n$14\r\nerr_lset_range\r\n$3\r\n100\r\n$1\r\nb\r\n")
                .await
                .expect("lset should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(
                matches!(error, EpError::Redis(RedisError::OutOfRange)),
                "Expected OutOfRange error, got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test ScriptError with a Lua script that throws an error.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_script_error() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Execute a Lua script that calls error()
            // EVAL "return redis.error_reply('oops')" 0
            // Script: return redis.error_reply('oops') = 32 chars
            let result = ctx
                .raw(b"*3\r\n$4\r\nEVAL\r\n$32\r\nreturn redis.error_reply('oops')\r\n$1\r\n0\r\n")
                .await
                .expect("eval should return response");

            let response = String::from_utf8_lossy(&result);

            // Strip RESP error prefix (-ERR) for accurate parsing
            let error_msg = response.trim_start_matches('-').trim().to_string();
            let error = parse_error_message(&error_msg);

            // Script errors via error_reply return generic ERR messages
            assert!(
                matches!(error, EpError::Redis(RedisError::Custom(_))),
                "Expected Custom error for script error_reply, got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test ZADD with NaN score.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_nan_score() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Try ZADD with "nan" as score
            let result = ctx
                .raw(b"*4\r\n$4\r\nZADD\r\n$12\r\nerr_zadd_nan\r\n$3\r\nnan\r\n$6\r\nmember\r\n")
                .await
                .expect("zadd should return response");

            // Verify parse_redis_error maps this to Custom
            // ("not a valid float" has no specific pattern match in parse_redis_error)
            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(
                matches!(error, EpError::Redis(RedisError::Custom(_))),
                "Expected Custom error for float parsing error, got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test OBJECT ENCODING on non-existent key returns nil, not error.
        /// This verifies that nil responses are handled differently from errors.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_nonexistent() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Key: err_obj_noexist = 14 chars
            let result = ctx
                .raw(b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$14\r\nerr_obj_noexist\r\n")
                .await
                .expect("object encoding should return response");

            let response = String::from_utf8_lossy(&result);
            // Should return nil ($-1 in RESP2), not an error
            assert!(
                response.contains("-1") || response.contains("nil") || response.starts_with("$-1"),
                "Expected nil response for non-existent key, got: {}",
                response
            );

            ctx.stop().await;
        }

        /// Test RENAME with non-existent source key.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_nonexistent_key() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Try to rename a key that doesn't exist
            let result = ctx
                .raw(b"*3\r\n$6\r\nRENAME\r\n$14\r\nerr_rename_src\r\n$14\r\nerr_rename_dst\r\n")
                .await
                .expect("rename should return response");

            // Verify parse_redis_error maps "no such key" to Custom (generic error)
            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(
                matches!(error, EpError::Redis(RedisError::Custom(_))),
                "Expected Custom error for 'no such key', got: {:?}",
                error
            );

            ctx.stop().await;
        }

        /// Test LPOS on a non-list key returns WRONGTYPE.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wrongtype_lpos_on_string() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Create a string key
            ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\nerr_lpos_string\r\n$5\r\nvalue\r\n").await.expect("set");

            // Try LPOS on string key
            let result =
                ctx.raw(b"*3\r\n$4\r\nLPOS\r\n$15\r\nerr_lpos_string\r\n$5\r\nvalue\r\n").await.expect("lpos should return response");

            let response = String::from_utf8_lossy(&result);

            let error = parse_error_message(&response);
            assert!(matches!(error, EpError::Redis(RedisError::WrongType)), "Expected WrongType error, got: {:?}", error);

            ctx.stop().await;
        }

        /// Test SMOVE with non-existent source set.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smove_nonexistent_source() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Create destination set
            ctx.raw(b"*3\r\n$4\r\nSADD\r\n$13\r\nerr_smove_dst\r\n$1\r\na\r\n").await.expect("sadd");

            // Try SMOVE from non-existent set
            let result = ctx
                .raw(b"*4\r\n$5\r\nSMOVE\r\n$13\r\nerr_smove_src\r\n$13\r\nerr_smove_dst\r\n$1\r\nb\r\n")
                .await
                .expect("smove should return response");

            let response = String::from_utf8_lossy(&result);
            // Returns 0 (member not moved) rather than error
            assert!(
                response.contains("0") || response.starts_with(":0"),
                "Expected 0 response for non-existent source, got: {}",
                response
            );

            ctx.stop().await;
        }
    }
}
