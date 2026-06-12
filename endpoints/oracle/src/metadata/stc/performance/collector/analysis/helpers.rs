use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn calculate_cpu_score(system_stats: &SystemStatistics) -> f64 {
        let mut score = 100.0_f64;

        if system_stats.cpu_utilization > 90.0 {
            score -= 30.0;
        } else if system_stats.cpu_utilization > 80.0 {
            score -= 20.0;
        } else if system_stats.cpu_utilization > 70.0 {
            score -= 10.0;
        }

        score.max(0.0)
    }

    pub(crate) fn calculate_memory_score(memory_util: &MemoryUtilization) -> f64 {
        let mut score: f64 = 100.0;

        if memory_util.sga_stats.utilization_pct > 95.0 {
            score -= 25.0;
        } else if memory_util.sga_stats.utilization_pct > 90.0 {
            score -= 15.0;
        }

        score.max(0.0_f64)
    }

    pub(crate) fn calculate_io_score(io_stats: &IoStatistics) -> f64 {
        let mut score = 100.0_f64;

        if io_stats.io_summary.avg_io_time_ms > 20.0 {
            score -= 30.0;
        } else if io_stats.io_summary.avg_io_time_ms > 10.0 {
            score -= 20.0;
        }

        score.max(0.0)
    }

    pub(crate) fn calculate_sql_score(sql_performance: &SqlPerformanceMetrics) -> f64 {
        let mut score = 100.0_f64;

        let avg_exec_time_ms = sql_performance.summary.avg_sql_execution_time / 1000.0;
        if avg_exec_time_ms > 1000.0 {
            score -= 25.0;
        } else if avg_exec_time_ms > 100.0 {
            score -= 15.0;
        }

        score.max(0.0)
    }

    pub(crate) fn analyze_wait_events(wait_events: &[WaitEventStat]) -> WaitEventsAnalysis {
        let mut analysis = WaitEventsAnalysis {
            top_wait_events: wait_events.iter().take(5).map(|we| we.event_name.clone()).collect(),
            ..Default::default()
        };

        let mut wait_classes: HashMap<String, f64> = HashMap::new();
        for event in wait_events {
            *wait_classes.entry(event.wait_class.clone()).or_insert(0.0) += event.pct_db_time;
        }
        analysis.wait_classes_distribution = wait_classes;

        analysis
    }

    pub(crate) fn identify_bottlenecks(
        system_stats: &SystemStatistics,
        _wait_events: &[WaitEventStat],
        _memory_util: &MemoryUtilization,
        _io_stats: &IoStatistics,
        _session_stats: &SessionStatistics,
    ) -> Vec<PerformanceBottleneck> {
        let mut bottlenecks = Vec::new();

        if system_stats.cpu_utilization > 80.0 {
            let mut metrics = HashMap::new();
            metrics.insert("cpu_utilization".to_string(), system_stats.cpu_utilization);

            bottlenecks.push(PerformanceBottleneck {
                bottleneck_type: BottleneckType::Cpu,
                severity: BottleneckSeverity::Major,
                description: format!("High CPU utilization detected at {:.1}%", system_stats.cpu_utilization),
                impact: "May cause slow response times and poor user experience".to_string(),
                recommendation: "Review top CPU consuming SQL statements".to_string(),
                affected_components: vec!["CPU".to_string()],
                metrics,
            });
        }

        bottlenecks
    }

    pub(crate) fn calculate_kpis(
        system_stats: &SystemStatistics,
        session_stats: &SessionStatistics,
        sql_performance: &SqlPerformanceMetrics,
    ) -> PerformanceKpis {
        PerformanceKpis {
            avg_response_time_ms: sql_performance.summary.avg_sql_execution_time / 1000.0,
            transactions_per_second: system_stats.commits_per_sec + system_stats.rollbacks_per_sec,
            concurrency_level: session_stats.active_sessions as f64,
            resource_utilization_pct: system_stats.cpu_utilization,
            availability_pct: 100.0,
            ..Default::default()
        }
    }
}
