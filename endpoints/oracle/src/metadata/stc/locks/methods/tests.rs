#![allow(clippy::field_reassign_with_default)]

use super::*;

#[test]
fn test_lock_info_metrics_and_severity() {
    let mut lock_info = OracleLockInfo {
        total_active_locks: 100,
        waiting_sessions: 10,
        blocked_sessions: 5,
        avg_lock_wait_time: 25.0,
        blocked_session_percentage: 10.0,
        ..OracleLockInfo::default()
    };

    lock_info.calculate_derived_metrics();

    // total_lock_attempts = 100 + 10 = 110; efficiency = (110 - 10) / 110 * 100
    let expected = 100.0_f64 / 110.0 * 100.0;
    assert!((lock_info.lock_efficiency_ratio - expected).abs() < 0.01);
    assert!(matches!(lock_info.contention_severity, ContentionSeverity::Medium));
    assert_eq!(lock_info.lock_contention_ratio(), 10.0);
}

#[test]
fn test_lock_helpers_and_summary() {
    assert_eq!(OracleLockInfo::lock_mode_description(1), "Null (1)");
    assert_eq!(OracleLockInfo::lock_type_description("TM"), "Table Lock");
    assert_eq!(OracleLockInfo::format_wait_time(3665), "1h 1m 5s");

    let lock_info = OracleLockInfo {
        total_active_locks: 200,
        blocking_locks: 20,
        avg_lock_wait_time: 15.0,
        row_level_locks: 50,
        table_level_locks: 30,
        ddl_locks: 10,
        system_locks: 5,
        library_cache_locks: 3,
        dictionary_cache_locks: 2,
        other_locks: 1,
        blocking_chains: vec![OracleBlockingChain::default()],
        contended_objects: vec![OracleContentionHotspot::default()],
        performance_impact_score: 45.0,
        ..Default::default()
    };

    let distribution = lock_info.get_lock_distribution();
    assert_eq!(distribution.get("Row Level (TX)"), Some(&50));
    assert_eq!(distribution.get("Table Level (TM)"), Some(&30));

    let summary = OracleLockSummary::from_lock_info(&lock_info);
    assert_eq!(summary.total_locks, 200);
    assert_eq!(summary.blocking_percentage, 10.0);
    assert_eq!(summary.blocking_chains_count, 1);
}

#[test]
fn test_metadata_collection_interface() {
    let lock_info = OracleLockInfo::default();

    assert_eq!(lock_info.description(), "Oracle lock information and blocking analysis");
    assert_eq!(lock_info.category(), "locks");
    assert!(matches!(lock_info.interval(), SyncFrequency::High));
    assert!(lock_info.size() > 0);

    let requests = lock_info.request();
    assert!(requests.contains_key("lock_summary"));
    assert!(requests.contains_key("blocking_chains"));
    assert!(requests.contains_key("contended_objects"));
    assert_eq!(requests.len(), 9);
}
