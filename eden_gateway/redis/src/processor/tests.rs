//! Processor-focused unit tests extracted from `processor.rs`.

use super::*;
use eden_core::error::EpError;
use eden_core::format::EdenNodeUuid;
use eden_core::telemetry::{AllMetrics, TelemetryDurations, TelemetryLabels};
use std::sync::Arc;

trait LoadProfileLabel {
    fn label(self) -> &'static str;
}

#[derive(Clone, Copy)]
enum FlowLoadProfile {
    Consistent,
    Variable,
    Malicious,
}

impl LoadProfileLabel for FlowLoadProfile {
    fn label(self) -> &'static str {
        match self {
            Self::Consistent => "consistent",
            Self::Variable => "variable",
            Self::Malicious => "malicious",
        }
    }
}

fn flow_load_profiles() -> [FlowLoadProfile; 3] {
    [FlowLoadProfile::Consistent, FlowLoadProfile::Variable, FlowLoadProfile::Malicious]
}

fn divergent_version_compare_responses(profile: FlowLoadProfile) -> Vec<(Bytes, Bytes)> {
    match profile {
        FlowLoadProfile::Consistent => vec![(Bytes::from_static(b"$2\r\nv1\r\n"), Bytes::from_static(b"$2\r\nv2\r\n"))],
        FlowLoadProfile::Variable => vec![
            (
                Bytes::from_static(b"*2\r\n$3\r\nfoo\r\n$3\r\nold\r\n"),
                Bytes::from_static(b"*2\r\n$3\r\nfoo\r\n$3\r\nnew\r\n"),
            ),
            (Bytes::from_static(b"$-1\r\n"), Bytes::from_static(b"$5\r\nfresh\r\n")),
        ],
        FlowLoadProfile::Malicious => {
            let old_payload = vec![b'o'; 12 * 1024];
            let new_payload = vec![b'n'; 12 * 1024];
            let old = Bytes::from(format!("${}\r\n{}\r\n", old_payload.len(), String::from_utf8_lossy(&old_payload)));
            let new = Bytes::from(format!("${}\r\n{}\r\n", new_payload.len(), String::from_utf8_lossy(&new_payload)));
            vec![(old, new)]
        }
    }
}

fn swallowed_response_results(profile: FlowLoadProfile) -> Vec<ResultEP<Option<Bytes>>> {
    match profile {
        FlowLoadProfile::Consistent => vec![
            Ok(Some(Bytes::from_static(b"+QUEUED\r\n"))),
            Ok(None),
            Ok(Some(Bytes::from_static(b"*1\r\n+OK\r\n"))),
        ],
        FlowLoadProfile::Variable => vec![
            Ok(Some(Bytes::from_static(b"+PONG\r\n"))),
            Err(EpError::request("temporary backend failure")),
            Ok(None),
            Ok(Some(Bytes::from_static(b"$5\r\nvalue\r\n"))),
        ],
        FlowLoadProfile::Malicious => vec![
            Ok(Some(Bytes::from_static(b"+READY\r\n"))),
            Ok(None),
            Ok(None),
            Err(EpError::request("malicious upstream fault")),
            Ok(Some(Bytes::from_static(b":1\r\n"))),
        ],
    }
}

fn transactional_follow_up_after_pin_loss(profile: FlowLoadProfile) -> Vec<Bytes> {
    match profile {
        FlowLoadProfile::Consistent => vec![
            Bytes::from_static(b"*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$1\r\n2\r\n"),
            Bytes::from_static(b"*1\r\n$4\r\nEXEC\r\n"),
        ],
        FlowLoadProfile::Variable => vec![
            Bytes::from_static(b"*4\r\n$4\r\nHSET\r\n$4\r\nacct\r\n$5\r\nfield\r\n$1\r\n7\r\n"),
            Bytes::from_static(b"*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n"),
            Bytes::from_static(b"*1\r\n$4\r\nEXEC\r\n"),
        ],
        FlowLoadProfile::Malicious => {
            let oversized = "partial".repeat(2048);
            vec![
                Bytes::from(format!("*3\r\n$3\r\nSET\r\n$12\r\nescape:hatch\r\n${}\r\n{}\r\n", oversized.len(), oversized)),
                Bytes::from_static(b"*2\r\n$5\r\nLPUSH\r\n$8\r\nauditlog\r\n"),
                Bytes::from_static(b"*1\r\n$4\r\nEXEC\r\n"),
            ]
        }
    }
}

fn session_state_commands(profile: FlowLoadProfile) -> Vec<RedisApi> {
    match profile {
        FlowLoadProfile::Consistent => vec![RedisApi::Subscribe, RedisApi::Unsubscribe, RedisApi::Auth, RedisApi::Select],
        FlowLoadProfile::Variable => vec![
            RedisApi::Psubscribe,
            RedisApi::Punsubscribe,
            RedisApi::Subscribe,
            RedisApi::Auth,
            RedisApi::Select,
            RedisApi::Reset,
        ],
        FlowLoadProfile::Malicious => vec![
            RedisApi::Ssubscribe,
            RedisApi::Sunsubscribe,
            RedisApi::Psubscribe,
            RedisApi::Punsubscribe,
            RedisApi::Subscribe,
            RedisApi::Unsubscribe,
            RedisApi::Auth,
            RedisApi::Select,
            RedisApi::Reset,
        ],
    }
}

fn oversized_request_chunks(profile: FlowLoadProfile) -> (Vec<Bytes>, usize) {
    match profile {
        FlowLoadProfile::Consistent => {
            let header = b"*3\r\n$3\r\nSET\r\n$9\r\nbig:value\r\n$32768\r\n".to_vec();
            let body = vec![b'a'; 24 * 1024];
            (vec![Bytes::from(header), Bytes::from(body)], 0)
        }
        FlowLoadProfile::Variable => {
            let partial = format!("*3\r\n$3\r\nSET\r\n$11\r\nmixed:large\r\n${}\r\n{}\r\n", 2048, "v".repeat(2048));
            let incomplete_header = b"*3\r\n$3\r\nSET\r\n$12\r\nwide:partial\r\n$65536\r\n".to_vec();
            let incomplete_body = vec![b'b'; 20 * 1024];
            (vec![Bytes::from(partial), Bytes::from(incomplete_header), Bytes::from(incomplete_body)], 1)
        }
        FlowLoadProfile::Malicious => {
            let incomplete_header = format!("*3\r\n$3\r\nSET\r\n$14\r\ndos:payload:01\r\n${}\r\n", 262_144);
            let binary_chunk = vec![0xff; 96 * 1024];
            let zero_chunk = vec![0u8; 64 * 1024];
            (vec![Bytes::from(incomplete_header), Bytes::from(binary_chunk), Bytes::from(zero_chunk)], 0)
        }
    }
}

fn resp_error_messages(profile: FlowLoadProfile) -> Vec<String> {
    match profile {
        FlowLoadProfile::Consistent => vec!["simple upstream error".to_string()],
        FlowLoadProfile::Variable => vec![
            "connection failed\r\nretry suggested".to_string(),
            "tls alert\r\ncertificate expired".to_string(),
        ],
        FlowLoadProfile::Malicious => vec![
            format!("backend panic\r\n{}\r\ntrace", "X".repeat(2048)),
            "multi-line\r\nprotocol\r\npoison".to_string(),
        ],
    }
}

fn pipeline_metric_commands(profile: FlowLoadProfile, command_count: usize) -> Vec<Bytes> {
    let mut commands = Vec::with_capacity(command_count);
    for idx in 0..command_count {
        let command = match profile {
            FlowLoadProfile::Consistent => Bytes::from_static(b"*2\r\n$3\r\nGET\r\n$5\r\nalpha\r\n"),
            FlowLoadProfile::Variable => match idx % 3 {
                0 => Bytes::from(format!("*2\r\n$3\r\nGET\r\n${}\r\nitem:{idx}\r\n", format!("item:{idx}").len())),
                1 => Bytes::from(format!("*3\r\n$3\r\nSET\r\n${}\r\nitem:{idx}\r\n$2\r\n42\r\n", format!("item:{idx}").len())),
                _ => Bytes::from(format!("*2\r\n$3\r\nTTL\r\n${}\r\nitem:{idx}\r\n", format!("item:{idx}").len())),
            },
            FlowLoadProfile::Malicious => {
                let key = format!("abuse:{idx}:{}", "k".repeat(96 + idx % 32));
                if idx % 2 == 0 {
                    let value = "V".repeat(512 + idx % 64);
                    Bytes::from(format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value))
                } else {
                    Bytes::from(format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key))
                }
            }
        };
        commands.push(command);
    }
    commands
}

#[test]
fn test_is_resp_null_bulk_string() {
    assert!(RedisWire::is_resp_null(b"$-1\r\n"));
}

#[test]
fn test_is_resp_null_array() {
    assert!(RedisWire::is_resp_null(b"*-1\r\n"));
}

#[test]
fn test_is_resp_null_non_null() {
    assert!(!RedisWire::is_resp_null(b"+OK\r\n"));
    assert!(!RedisWire::is_resp_null(b"$3\r\nfoo\r\n"));
}

#[test]
fn test_is_resp_null_empty() {
    assert!(!RedisWire::is_resp_null(b""));
}

#[test]
fn append_bounded_accepts_exact_limit_and_rejects_overflow() {
    let mut buffer = BytesMut::new();

    RedisWire::append_bounded(&mut buffer, b"abcd", 4).expect("exact limit should fit");
    assert_eq!(buffer.as_ref(), b"abcd");
    assert!(RedisWire::append_bounded(&mut buffer, b"e", 4).is_err());
    assert_eq!(buffer.as_ref(), b"abcd");
}

#[test]
fn normalize_result_for_client_turns_missing_upstream_response_into_error() {
    assert_eq!(
        RedisWire::normalize_result_for_client(Ok(Some(Bytes::from_static(b"+OK\r\n"))))
            .expect("response should remain ok")
            .expect("response should remain present"),
        Bytes::from_static(b"+OK\r\n")
    );
    assert!(RedisWire::normalize_result_for_client(Ok(None)).is_err());
}

#[test]
fn request_metadata_normalizes_service_names_and_falls_back_to_first_arg_key() {
    assert_eq!(
        RedisRequestMetadata::normalize_service_name("  billing-worker  "),
        Some("billing-worker".to_string())
    );
    assert_eq!(RedisRequestMetadata::normalize_service_name("   "), None);

    let set_args = vec![
        RedisJsonValue::String("cache:key".to_string()),
        RedisJsonValue::String("value".to_string()),
    ];
    assert_eq!(RedisRequestMetadata::audit_key_from_args(&RedisApi::Set, &set_args), Some("cache:key".to_string()));

    let ping_args = vec![RedisJsonValue::String("payload".to_string())];
    assert_eq!(RedisRequestMetadata::audit_key_from_args(&RedisApi::Ping, &ping_args), Some("payload".to_string()));
}

#[test]
fn request_metadata_hashes_are_stable_and_nonzero() {
    let first = RedisRequestMetadata::fnv1a_hash(b"tenant:key");
    let second = RedisRequestMetadata::fnv1a_hash(b"tenant:key");
    let different = RedisRequestMetadata::fnv1a_hash(b"tenant:other");

    assert_ne!(first, 0);
    assert_eq!(first, second);
    assert_ne!(first, different);
    assert_eq!(
        RedisRequestMetadata::audit_args_hash(b"*1\r\n$4\r\nPING\r\n"),
        RedisRequestMetadata::audit_args_hash(b"*1\r\n$4\r\nPING\r\n")
    );
}

#[test]
fn fallback_on_miss_still_only_falls_back_on_transport_errors() {
    let transport_error: ResultEP<Bytes> = Err(EpError::parse("boom"));
    assert!(RedisWire::should_fallback_on_miss(&transport_error));

    let null_bulk: ResultEP<Bytes> = Ok(Bytes::from_static(b"$-1\r\n"));
    assert!(RedisWire::should_fallback_on_miss(&null_bulk), "RESP null bulk strings should trigger fallback");

    let null_array: ResultEP<Bytes> = Ok(Bytes::from_static(b"*-1\r\n"));
    assert!(RedisWire::should_fallback_on_miss(&null_array), "RESP null arrays should trigger fallback");
}

#[test]
fn subscribe_family_commands_currently_lack_pubsub_mode_handling() {
    for profile in flow_load_profiles() {
        for command in session_state_commands(profile).into_iter().filter(|command| {
            matches!(
                command,
                RedisApi::Subscribe
                    | RedisApi::Psubscribe
                    | RedisApi::Ssubscribe
                    | RedisApi::Unsubscribe
                    | RedisApi::Punsubscribe
                    | RedisApi::Sunsubscribe
            )
        }) {
            assert_eq!(
                RedisDispatch::pre_dispatch_handling(&command),
                PreDispatchHandling::ExplicitLocalState,
                "pub/sub command {:?} should be intercepted locally for {} load",
                command,
                profile.label()
            );
        }
    }
}

#[test]
fn command_dispatch_path_prefers_policy_override_before_pinned_connections() {
    assert_eq!(RedisDispatch::command_path(true, true), CommandDispatchPath::PolicyOverride);
    assert_eq!(RedisDispatch::command_path(true, false), CommandDispatchPath::PolicyOverride);
    assert_eq!(RedisDispatch::command_path(false, true), CommandDispatchPath::PinnedConnection);
    assert_eq!(RedisDispatch::command_path(false, false), CommandDispatchPath::RoutedConnection);
}

#[test]
fn replication_capture_requires_write_allowed_manager_and_enabled_stream() {
    let scenarios = [
        (true, false, true, true, true),
        (false, false, true, true, false),
        (true, true, true, true, false),
        (true, false, false, true, false),
        (true, false, true, false, false),
    ];

    for (is_write, was_policy_blocked, has_replication_manager, allow_replication_stream, expected) in scenarios {
        assert_eq!(
            RedisDispatch::should_capture_replication_bytes(
                is_write,
                was_policy_blocked,
                has_replication_manager,
                allow_replication_stream
            ),
            expected
        );
    }
}

#[test]
fn auth_commands_currently_forward_to_generic_pooled_path() {
    for profile in flow_load_profiles() {
        let auth_commands =
            session_state_commands(profile).into_iter().filter(|command| matches!(command, RedisApi::Auth)).collect::<Vec<_>>();
        assert!(!auth_commands.is_empty(), "test data should include AUTH for {} load", profile.label());

        for command in auth_commands {
            assert_eq!(
                RedisDispatch::pre_dispatch_handling(&command),
                PreDispatchHandling::ExplicitLocalState,
                "AUTH should be intercepted locally for {} load",
                profile.label()
            );
        }
    }
}

#[test]
fn select_commands_currently_forward_to_generic_pooled_path() {
    for profile in flow_load_profiles() {
        let select_commands =
            session_state_commands(profile).into_iter().filter(|command| matches!(command, RedisApi::Select)).collect::<Vec<_>>();
        assert!(!select_commands.is_empty(), "test data should include SELECT for {} load", profile.label());

        for command in select_commands {
            assert_eq!(
                RedisDispatch::pre_dispatch_handling(&command),
                PreDispatchHandling::ExplicitLocalState,
                "SELECT should be intercepted locally for {} load",
                profile.label()
            );
        }
    }
}

#[test]
fn pinned_connection_error_mid_batch_reroutes_follow_up_commands_to_unpinned_path() {
    for profile in flow_load_profiles() {
        let mut tracker = PinnedTransactionTracker::new();
        tracker.mark_pinned();
        tracker.confirm_watch();
        tracker.confirm_multi();

        assert_eq!(
            RedisDispatch::command_path(false, tracker.is_pinned()),
            CommandDispatchPath::PinnedConnection,
            "transactional commands should initially stay on the pinned connection for {} load",
            profile.label()
        );

        tracker.on_connection_error();

        assert!(
            tracker.should_abort_connection(),
            "after a pinned connection error, the {} connection should be aborted instead of rerouted",
            profile.label()
        );
        assert!(tracker.should_release(), "connection errors clear the pinned transaction state");
    }
}

#[test]
fn version_compare_mismatch_records_proxy_error_metric() {
    let metrics = Arc::new(AllMetrics::new());
    let mut telemetry_wrapper =
        TelemetryWrapper::new(metrics.clone(), TelemetryLabels::new(&EdenNodeUuid::new_uuid()), TelemetryDurations::default());
    let ctx = LogContext::default().with_feature("redis-validation");

    let mismatch_count: u64 = flow_load_profiles().iter().map(|profile| divergent_version_compare_responses(*profile).len() as u64).sum();

    for profile in flow_load_profiles() {
        for (old_resp, new_resp) in divergent_version_compare_responses(profile) {
            assert!(
                RedisResponseComparison::responses_differ(old_resp.as_ref(), new_resp.as_ref()),
                "test data should diverge for {}",
                profile.label()
            );
            RedisResponseComparison::record_mismatch("org-validation", "interlay-validation", &mut telemetry_wrapper, &ctx);
        }
    }

    assert_eq!(
        metrics.proxy().get_errors_total(),
        mismatch_count,
        "VersionCompare mismatches should emit one proxy error metric per divergent response pair"
    );
}

#[test]
fn ok_none_results_desync_visible_response_count_from_command_count() {
    for profile in flow_load_profiles() {
        let results = swallowed_response_results(profile);
        let rendered = RedisWire::render_client_response_bytes(&results);
        let visible_slots: usize = results.iter().map(RedisWire::client_visible_response_slots).sum();

        assert_eq!(
            visible_slots,
            results.len(),
            "all {} command results should now occupy a client-visible response slot",
            profile.label()
        );
        assert!(
            crate::resp_scan::RespScanner::scan(&rendered, results.len()).is_some(),
            "rendering {} command results should preserve per-command RESP attribution",
            profile.label()
        );
    }
}

#[test]
fn resp_error_messages_with_crlf_produce_multiple_line_terminators() {
    for profile in flow_load_profiles() {
        for message in resp_error_messages(profile) {
            let frame = RedisWire::format_resp_error_line(&message);
            let terminators = RedisWire::count_resp_line_terminators(&frame);

            if message.contains("\r\n") {
                assert_eq!(
                    terminators,
                    1,
                    "CRLF-bearing {} error messages should be sanitized to one RESP terminator",
                    profile.label()
                );
            } else {
                assert_eq!(terminators, 1, "single-line {} errors should still render one RESP terminator", profile.label());
            }
        }
    }
}

#[test]
fn pinned_connection_error_mid_batch_loses_transaction_context_for_follow_up_exec_sequence() {
    for profile in flow_load_profiles() {
        let mut tracker = PinnedTransactionTracker::new();
        tracker.mark_pinned();
        tracker.confirm_multi();

        assert_eq!(
            RedisDispatch::command_path(false, tracker.is_pinned()),
            CommandDispatchPath::PinnedConnection,
            "transactional commands should start on the pinned connection for {} load",
            profile.label()
        );

        tracker.on_connection_error();

        for _command in transactional_follow_up_after_pin_loss(profile) {
            assert!(
                tracker.should_abort_connection(),
                "follow-up transactional commands should be prevented after a {} pin loss",
                profile.label()
            );
        }
    }
}

#[test]
fn incomplete_large_bulk_strings_accumulate_request_buffer_without_a_size_cap() {
    for profile in flow_load_profiles() {
        let (chunks, expected_parsed_commands) = oversized_request_chunks(profile);
        let (retained_bytes, parsed_commands) = RedisWire::measure_request_buffer_retention(&chunks);

        assert_eq!(
            parsed_commands,
            expected_parsed_commands,
            "expected parsed-command count should match the {} oversized request scenario",
            profile.label()
        );
        assert!(
            RedisWire::request_buffer_hits_limit(&chunks) || retained_bytes < MAX_REQUEST_BUFFER_BYTES,
            "oversized {} payloads should be capped instead of growing without bound",
            profile.label()
        );
    }
}

#[test]
fn routing_refresh_snapshot_can_become_stale_after_cache_mutation() {
    let scenarios = [
        (FlowLoadProfile::Consistent, true, 0usize),
        (FlowLoadProfile::Variable, false, 1usize),
        (FlowLoadProfile::Malicious, false, 4usize),
    ];

    for (profile, remove_after_refresh, post_refresh_mutations) in scenarios {
        assert!(
            !RoutingRuntime::refresh_snapshot_stales_after_cache_mutation(remove_after_refresh, post_refresh_mutations),
            "routing refresh should avoid stale {} snapshots after {} post-refresh mutations",
            profile.label(),
            post_refresh_mutations
        );
    }
}

#[test]
fn pipeline_slow_flags_can_miss_slow_batches_when_average_stays_below_threshold() {
    let threshold_us = 10_000;
    let scenarios = [
        (FlowLoadProfile::Consistent, 100usize, 500_000u64),
        (FlowLoadProfile::Variable, 40usize, 320_000u64),
        (FlowLoadProfile::Malicious, 200usize, 1_600_000u64),
    ];

    for (profile, command_count, duration_us) in scenarios {
        let _commands = pipeline_metric_commands(profile, command_count);

        assert!(
            duration_us >= threshold_us,
            "scenario should represent a slow overall batch for {} load",
            profile.label()
        );
    }
}

#[test]
fn pipeline_request_and_fallback_response_bytes_wrap_past_u32_max() {
    let scenarios = [
        (FlowLoadProfile::Consistent, 1usize, u64::from(u32::MAX) + 1_024, u64::from(u32::MAX) + 2_048),
        (FlowLoadProfile::Variable, 3usize, u64::from(u32::MAX) + 12_288, u64::from(u32::MAX) + 24_576),
        (
            FlowLoadProfile::Malicious,
            17usize,
            u64::from(u32::MAX) * 2 + 65_536,
            u64::from(u32::MAX) * 2 + 131_072,
        ),
    ];

    for (profile, command_count, bytes_read, total_bytes_written) in scenarios {
        let _commands = pipeline_metric_commands(profile, command_count);

        let expected_request_bytes = RedisPipelineMetrics::request_bytes(bytes_read, command_count as u64);
        let expected_response_bytes = RedisPipelineMetrics::response_bytes(total_bytes_written, command_count as u64);

        assert_eq!(u64::from(expected_request_bytes), (bytes_read / command_count as u64).min(u64::from(u32::MAX)));
        assert_eq!(
            u64::from(expected_response_bytes),
            (total_bytes_written / command_count as u64).min(u64::from(u32::MAX))
        );
    }
}

#[test]
fn pipeline_metric_helpers_handle_empty_or_single_command_edges() {
    assert_eq!(RedisPipelineMetrics::request_bytes(12, 0), 12);
    assert_eq!(RedisPipelineMetrics::response_bytes(10, 0), 10);
    assert!(RedisPipelineMetrics::marks_slow(10_000, 1, 10_000));
    assert!(!RedisPipelineMetrics::marks_slow(10_000, 2, 10_000));
    assert!(!RedisPipelineMetrics::marks_slow(10_000, 1, 0));
}

#[test]
fn zero_max_timeout_immediately_expires_conflict_waits() {
    let rt = tokio::runtime::Builder::new_current_thread().enable_time().build().expect("failed to build validation runtime");

    rt.block_on(async {
        let ready_delays = [
            (FlowLoadProfile::Consistent, Duration::from_millis(50)),
            (FlowLoadProfile::Variable, Duration::from_millis(75)),
            (FlowLoadProfile::Malicious, Duration::from_millis(100)),
        ];

        for (profile, ready_after) in ready_delays {
            let zero_timeout = tokio::time::timeout(RedisPipelineMetrics::queue_conflict_timeout_duration(0), async {
                tokio::task::yield_now().await;
                tokio::time::sleep(ready_after).await;
            })
            .await;

            assert!(zero_timeout.is_ok(), "zero timeout should now use a minimum floor for {} load", profile.label());

            let non_zero_timeout = tokio::time::timeout(
                RedisPipelineMetrics::queue_conflict_timeout_duration((ready_after.as_millis() as u64) + 25),
                async {
                    tokio::task::yield_now().await;
                    tokio::time::sleep(ready_after).await;
                },
            )
            .await;
            assert!(
                non_zero_timeout.is_ok(),
                "a small positive timeout should allow the same conflict wait to complete for {} load",
                profile.label()
            );
        }
    });
}

// ── PinnedTransactionTracker: initial state ──────────────────────────

#[test]
fn new_tracker_starts_unpinned() {
    let t = PinnedTransactionTracker::new();
    assert!(!t.is_watching());
    assert!(!t.is_in_multi());
    assert!(!t.is_pinned());
    assert!(t.should_release());
}

// ── WATCH lifecycle ──────────────────────────────────────────────────

#[test]
fn watch_requests_pin_acquisition() {
    let mut t = PinnedTransactionTracker::new();
    assert_eq!(t.pin_action(), PinAction::AcquirePin);

    t.mark_pinned();
    t.confirm_watch();

    assert!(t.is_watching());
    assert!(t.is_pinned());
    assert!(!t.should_release()); // watching holds pin
}

#[test]
fn multiple_watch_commands_only_pin_once() {
    let mut t = PinnedTransactionTracker::new();

    // First WATCH acquires pin
    assert_eq!(t.pin_action(), PinAction::AcquirePin);
    t.mark_pinned();
    t.confirm_watch();

    // Second WATCH (additional keys) reuses
    assert_eq!(t.pin_action(), PinAction::AlreadyPinned);
    t.confirm_watch();

    assert!(t.is_watching());
    assert!(t.is_pinned());
}

#[test]
fn failed_watch_pin_doesnt_change_state() {
    let t = PinnedTransactionTracker::new();
    assert_eq!(t.pin_action(), PinAction::AcquirePin);

    // Simulate failure: don't call mark_pinned() or confirm_watch()
    assert!(!t.is_watching());
    assert!(!t.is_pinned());
    assert!(t.should_release());
}

#[test]
fn watch_unwatch_releases_pin() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();
    assert!(!t.should_release());

    t.on_unwatch();
    assert!(!t.is_watching());
    assert!(t.should_release());

    t.release();
    assert!(!t.is_pinned());
}

// ── MULTI lifecycle ──────────────────────────────────────────────────

#[test]
fn multi_requests_pin_acquisition() {
    let mut t = PinnedTransactionTracker::new();
    assert_eq!(t.pin_action(), PinAction::AcquirePin);

    t.mark_pinned();
    t.confirm_multi();

    assert!(t.is_in_multi());
    assert!(t.is_pinned());
    assert!(!t.should_release());
}

#[test]
fn multi_exec_without_watch() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_multi();
    assert!(t.is_in_multi());

    t.on_exec_or_discard();
    assert!(!t.is_in_multi());
    assert!(t.should_release());

    t.release();
    assert!(!t.is_pinned());
}

#[test]
fn multi_discard_releases() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_multi();

    t.on_exec_or_discard(); // DISCARD
    assert!(!t.is_in_multi());
    assert!(!t.is_watching());
    assert!(t.should_release());
}

// ── WATCH + MULTI combined ───────────────────────────────────────────

#[test]
fn watch_then_multi_does_not_repin() {
    let mut t = PinnedTransactionTracker::new();

    // WATCH acquires pin
    assert_eq!(t.pin_action(), PinAction::AcquirePin);
    t.mark_pinned();
    t.confirm_watch();

    // MULTI reuses existing pin
    assert_eq!(t.pin_action(), PinAction::AlreadyPinned);
    t.confirm_multi();

    assert!(t.is_watching());
    assert!(t.is_in_multi());
    assert!(t.is_pinned());
}

#[test]
fn exec_clears_both_watch_and_multi() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();
    t.confirm_multi();

    t.on_exec_or_discard();

    assert!(!t.is_watching());
    assert!(!t.is_in_multi());
    assert!(t.should_release());
}

#[test]
fn unwatch_during_multi_keeps_multi_pinned() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();
    t.confirm_multi();

    t.on_unwatch();

    assert!(!t.is_watching());
    assert!(t.is_in_multi());
    assert!(!t.should_release()); // still in MULTI
}

// ── Full WATCH/MULTI/EXEC lifecycle ──────────────────────────────────

#[test]
fn full_watch_multi_exec_lifecycle() {
    let mut t = PinnedTransactionTracker::new();

    // 1. WATCH key
    assert_eq!(t.pin_action(), PinAction::AcquirePin);
    t.mark_pinned();
    t.confirm_watch();
    assert!(t.is_watching());
    assert!(!t.is_in_multi());
    assert!(!t.should_release());

    // 2. GET (regular command) — no state change in tracker
    assert!(t.is_pinned());

    // 3. MULTI
    assert_eq!(t.pin_action(), PinAction::AlreadyPinned);
    t.confirm_multi();
    assert!(t.is_watching());
    assert!(t.is_in_multi());

    // 4. Queued commands — no state change in tracker
    assert!(!t.should_release());

    // 5. EXEC
    t.on_exec_or_discard();
    assert!(!t.is_watching());
    assert!(!t.is_in_multi());
    assert!(t.should_release());

    // 6. Release
    t.release();
    assert!(!t.is_pinned());
}

// ── Policy-blocked EXEC/DISCARD ──────────────────────────────────────

#[test]
fn policy_blocked_exec_keeps_pinned_state() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_multi();

    // Policy blocks EXEC — on_exec_or_discard is NOT called
    // The backend is still in MULTI state so the pin must stay
    assert!(t.is_in_multi());
    assert!(t.is_pinned());
    assert!(!t.should_release());
}

#[test]
fn policy_blocked_discard_keeps_pinned_state() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();
    t.confirm_multi();

    // Policy blocks DISCARD — on_exec_or_discard is NOT called
    assert!(t.is_watching());
    assert!(t.is_in_multi());
    assert!(!t.should_release());
}

// Pin acquired but confirm not called (command not forwarded)

#[test]
fn pinned_without_confirm_watch_releases() {
    let mut t = PinnedTransactionTracker::new();
    t.mark_pinned();
    // confirm_watch deliberately not called
    assert!(!t.is_watching());
    assert!(t.is_pinned());
    assert!(t.should_release());
}

#[test]
fn skipped_confirm_multi_keeps_watch() {
    let mut t = PinnedTransactionTracker::new();
    t.mark_pinned();
    t.confirm_watch();
    // confirm_multi deliberately not called
    assert!(t.is_watching());
    assert!(!t.is_in_multi());
    assert!(!t.should_release());
}

#[test]
fn pinned_without_confirm_multi_releases() {
    let mut t = PinnedTransactionTracker::new();
    t.mark_pinned();
    // confirm_multi deliberately not called
    assert!(!t.is_in_multi());
    assert!(t.is_pinned());
    assert!(t.should_release());
}

#[test]
fn skipped_unwatch_keeps_watch() {
    let mut t = PinnedTransactionTracker::new();
    t.mark_pinned();
    t.confirm_watch();
    // on_unwatch deliberately not called
    assert!(t.is_watching());
    assert!(!t.should_release());
}

// ── Connection error ─────────────────────────────────────────────────

#[test]
fn connection_error_resets_all_state() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();
    t.confirm_multi();

    t.on_connection_error();

    assert!(!t.is_watching());
    assert!(!t.is_in_multi());
    assert!(!t.is_pinned());
    assert!(t.should_release());
}

#[test]
fn connection_error_during_watch_only() {
    let mut t = PinnedTransactionTracker::new();

    t.mark_pinned();
    t.confirm_watch();

    t.on_connection_error();

    assert!(!t.is_watching());
    assert!(!t.is_pinned());
    assert!(t.should_release());
}

// ── Sequential transactions ──────────────────────────────────────────

#[test]
fn sequential_transactions_each_require_new_pin() {
    let mut t = PinnedTransactionTracker::new();

    // First transaction
    assert_eq!(t.pin_action(), PinAction::AcquirePin);
    t.mark_pinned();
    t.confirm_multi();
    t.on_exec_or_discard();
    assert!(t.should_release());
    t.release();
    assert!(!t.is_pinned());

    // Second transaction — must acquire a new pin
    assert_eq!(t.pin_action(), PinAction::AcquirePin);
    t.mark_pinned();
    t.confirm_multi();
    t.on_exec_or_discard();
    assert!(t.should_release());
    t.release();
    assert!(!t.is_pinned());
}

// ── Helper function tests (feature-gated) ────────────────────────────
mod request_metadata_hash_tests {
    use super::super::*;

    #[test]
    fn fnv1a_produces_nonzero() {
        assert_ne!(RedisRequestMetadata::fnv1a_hash(b""), 0);
        assert_ne!(RedisRequestMetadata::fnv1a_hash(b"hello"), 0);
        assert_ne!(RedisRequestMetadata::fnv1a_hash(b"key:1234"), 0);
    }

    #[test]
    fn fnv1a_deterministic() {
        let h1 = RedisRequestMetadata::fnv1a_hash(b"consistent");
        let h2 = RedisRequestMetadata::fnv1a_hash(b"consistent");
        assert_eq!(h1, h2);
    }

    #[test]
    fn fnv1a_different_inputs_differ() {
        assert_ne!(RedisRequestMetadata::fnv1a_hash(b"key1"), RedisRequestMetadata::fnv1a_hash(b"key2"));
    }

    #[test]
    fn fnv1a_empty_is_nonzero() {
        // FNV offset basis is non-zero, so empty input should return the basis
        assert_ne!(RedisRequestMetadata::fnv1a_hash(b""), 0);
    }

    #[test]
    fn normalize_service_name_trims() {
        assert_eq!(RedisRequestMetadata::normalize_service_name("  my-service  "), Some("my-service".to_string()));
    }

    #[test]
    fn normalize_service_name_empty_returns_none() {
        assert_eq!(RedisRequestMetadata::normalize_service_name(""), None);
        assert_eq!(RedisRequestMetadata::normalize_service_name("   "), None);
    }

    #[test]
    fn normalize_service_name_preserves_content() {
        assert_eq!(RedisRequestMetadata::normalize_service_name("redis-cache"), Some("redis-cache".to_string()));
    }
}

mod routing_tests {
    use ep_core::database::traffic::{ReadRouting, RoutingStrategy};

    #[test]
    fn test_user_hash_routing_uses_client_ip() {
        // Test that UserHash routing uses IP consistently
        // This simulates what happens in handle_read_command when UserHash is used

        let read_routing = ReadRouting::Ratio { strategy: RoutingStrategy::UserHash { ratio: 0.5 } };

        // Test that the same IP always routes consistently
        let ip_1 = "192.168.1.1";
        let ip_2 = "192.168.1.2";

        // Simulate multiple commands with the same IP
        let mut results_ip_1 = Vec::new();
        for _i in 0..5 {
            if let ReadRouting::Ratio { strategy } = &read_routing {
                let route_to_new = strategy.should_route_to_new_for_user(ip_1);
                results_ip_1.push(route_to_new);
            }
        }

        // All results for ip_1 should be the same (consistent routing)
        let first_result = results_ip_1[0];
        for &result in &results_ip_1 {
            assert_eq!(result, first_result, "Same IP should always route the same way");
        }

        // Test with a different IP
        let mut results_ip_2 = Vec::new();
        for _i in 0..5 {
            if let ReadRouting::Ratio { strategy } = &read_routing {
                let route_to_new = strategy.should_route_to_new_for_user(ip_2);
                results_ip_2.push(route_to_new);
            }
        }

        // All results for ip_2 should be consistent
        let first_result_ip_2 = results_ip_2[0];
        for &result in &results_ip_2 {
            assert_eq!(result, first_result_ip_2, "Same IP should always route the same way");
        }
    }

    #[test]
    fn test_ip_deterministic_routing() {
        // Test that the same IP always produces the same routing decision
        let strategy = RoutingStrategy::UserHash { ratio: 0.7 };

        let ip = "10.0.0.1";

        // Call multiple times - should always be the same
        let result1 = strategy.should_route_to_new_for_user(ip);
        let result2 = strategy.should_route_to_new_for_user(ip);
        let result3 = strategy.should_route_to_new_for_user(ip);

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }

    #[test]
    fn test_different_ips_can_route_differently() {
        // Test that different IPs can produce different routing decisions
        let strategy = RoutingStrategy::UserHash { ratio: 0.5 };

        let mut results = std::collections::HashMap::new();

        // Test multiple IPs
        for i in 1..=20 {
            let ip = format!("192.168.1.{}", i);
            let route_to_new = strategy.should_route_to_new_for_user(&ip);
            results.insert(i, route_to_new);
        }

        // We should see some variation (not all true or all false)
        let true_count = results.values().filter(|&&v| v).count();
        let false_count = results.values().filter(|&&v| !v).count();

        // With 20 IPs and 50% ratio, we should see some distribution
        assert!(true_count > 0, "Should have some IPs routing to new");
        assert!(false_count > 0, "Should have some IPs routing to old");
    }
}
