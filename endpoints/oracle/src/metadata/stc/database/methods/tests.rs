#![allow(clippy::field_reassign_with_default)]

use super::*;

#[test]
fn test_database_health_and_metrics() {
    let mut stats = OracleDatabaseStats {
        database_size: 1_000_000_000,
        used_space: 800_000_000,
        free_space: 200_000_000,
        pga_aggregate_target: 500_000_000,
        pga_used: 400_000_000,
        buffer_cache_hit_ratio: 92.0,
        library_cache_hit_ratio: 99.0,
        soft_parse_ratio: 95.0,
        cpu_usage_percentage: 60.0,
        physical_reads_per_sec: 100.0,
        logical_reads_per_sec: 1000.0,
        ..OracleDatabaseStats::default()
    };

    assert_eq!(stats.space_utilization_percentage(), 80.0);
    assert_eq!(stats.pga_utilization_percentage(), 80.0);
    assert!(stats.is_library_cache_healthy(95.0));
    assert!(!stats.has_io_bottlenecks());

    let health = stats.is_database_healthy();
    assert!(matches!(health.overall_status, DatabaseHealthStatus::Warning));
    assert!(!health.issues.is_empty());

    stats.buffer_cache_hit_ratio = 98.0;
    let healthy = stats.is_database_healthy();
    assert!(matches!(healthy.overall_status, DatabaseHealthStatus::Healthy));

    stats.uptime_seconds = 3_661.0;
    assert_eq!(stats.uptime_human_readable(), "1h 1m 1s");

    stats.top_wait_events = vec![OracleWaitEventStats {
        event: "db file sequential read".to_string(),
        wait_class: "User I/O".to_string(),
        total_waits: 1000,
        total_timeouts: 0,
        time_waited: 50_000.0,
        average_wait: 50.0,
        pct_of_total_time: 25.0,
    }];
    assert!(stats.has_significant_waits(20.0));
}
