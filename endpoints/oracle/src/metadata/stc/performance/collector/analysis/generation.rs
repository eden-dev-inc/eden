use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn generate_performance_analysis(
        system_stats: &SystemStatistics,
        wait_events: &[WaitEventStat],
        sql_performance: &SqlPerformanceMetrics,
        memory_util: &MemoryUtilization,
        io_stats: &IoStatistics,
        session_stats: &SessionStatistics,
    ) -> ResultEP<PerformanceAnalysis> {
        let mut analysis = PerformanceAnalysis {
            cpu_score: Self::calculate_cpu_score(system_stats),
            memory_score: Self::calculate_memory_score(memory_util),
            io_score: Self::calculate_io_score(io_stats),
            sql_score: Self::calculate_sql_score(sql_performance),

            ..Default::default()
        };

        analysis.overall_score =
            analysis.cpu_score * 0.25 + analysis.memory_score * 0.25 + analysis.io_score * 0.25 + analysis.sql_score * 0.25;

        analysis.wait_events_analysis = Self::analyze_wait_events(wait_events);
        analysis.bottlenecks = Self::identify_bottlenecks(system_stats, wait_events, memory_util, io_stats, session_stats);
        analysis.kpis = Self::calculate_kpis(system_stats, session_stats, sql_performance);

        Ok(analysis)
    }

    pub(crate) fn generate_recommendations(stats: &OraclePerformanceStats) -> ResultEP<Vec<PerformanceRecommendation>> {
        let mut recommendations = Vec::new();

        if stats.system_stats.cpu_utilization > 80.0 {
            recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Performance,
                priority: RecommendationPriority::High,
                title: "High CPU Utilization Detected".to_string(),
                description: format!(
                    "CPU utilization is at {:.1}%, which may indicate performance issues",
                    stats.system_stats.cpu_utilization
                ),
                rationale: "High CPU usage can lead to poor response times and user experience".to_string(),
                expected_benefit: "Reduced response times and improved system performance".to_string(),
                implementation_effort: ImplementationEffort::Medium,
                risk_level: RiskLevel::Medium,
                affected_metrics: vec!["CPU Utilization".to_string(), "Response Time".to_string()],
                action_items: vec![
                    "Review top SQL statements by CPU time".to_string(),
                    "Consider SQL tuning for high CPU consumers".to_string(),
                    "Evaluate hardware scaling options".to_string(),
                ],
            });
        }

        if stats.memory_utilization.sga_stats.utilization_pct > 90.0 {
            recommendations.push(PerformanceRecommendation {
                category: RecommendationCategory::Memory,
                priority: RecommendationPriority::High,
                title: "SGA Memory Pressure".to_string(),
                description: format!(
                    "SGA utilization is at {:.1}%, indicating memory pressure",
                    stats.memory_utilization.sga_stats.utilization_pct
                ),
                rationale: "High SGA utilization can lead to memory allocation failures".to_string(),
                expected_benefit: "Improved memory management and reduced allocation errors".to_string(),
                implementation_effort: ImplementationEffort::Low,
                risk_level: RiskLevel::Low,
                affected_metrics: vec!["SGA Utilization".to_string(), "Memory Efficiency".to_string()],
                action_items: vec![
                    "Review SGA target advisor recommendations".to_string(),
                    "Consider increasing SGA_TARGET parameter".to_string(),
                    "Analyze shared pool usage patterns".to_string(),
                ],
            });
        }

        Ok(recommendations)
    }

    pub(crate) fn generate_alerts(stats: &OraclePerformanceStats) -> ResultEP<Vec<PerformanceAlert>> {
        let mut alerts = Vec::new();
        let current_time = DateTimeWrapper::from(Utc::now());

        if stats.system_stats.cpu_utilization > 90.0 {
            alerts.push(PerformanceAlert {
                alert_type: AlertType::Performance,
                severity: AlertSeverity::Critical,
                message: "Critical CPU utilization level reached".to_string(),
                metric_name: "CPU Utilization".to_string(),
                current_value: stats.system_stats.cpu_utilization,
                threshold_value: 90.0,
                duration_seconds: 0,
                first_occurrence: current_time.clone(),
                recommended_action: "Immediately review top CPU consuming SQL statements".to_string(),
            });
        }

        Ok(alerts)
    }

    pub(crate) fn calculate_health_score(stats: &OraclePerformanceStats) -> ResultEP<f64> {
        let mut score = 100.0;

        if stats.system_stats.cpu_utilization > 80.0 {
            score -= (stats.system_stats.cpu_utilization - 80.0) * 0.5;
        }

        if stats.memory_utilization.sga_stats.utilization_pct > 90.0 {
            score -= (stats.memory_utilization.sga_stats.utilization_pct - 90.0) * 0.3;
        }

        if stats.io_statistics.io_summary.avg_io_time_ms > 5.0 {
            score -= (stats.io_statistics.io_summary.avg_io_time_ms - 5.0) * 2.0;
        }

        if stats.session_statistics.blocked_sessions > 0 {
            score -= stats.session_statistics.blocked_sessions as f64 * 2.0;
        }

        Ok(score.clamp(0.0, 100.0))
    }

    pub(crate) fn calculate_data_quality(stats: &OraclePerformanceStats) -> f64 {
        let mut quality_score = 100.0_f64;

        if stats.system_stats.db_time == 0 {
            quality_score -= 20.0;
        }
        if stats.wait_events.is_empty() {
            quality_score -= 15.0;
        }
        if stats.sql_performance.summary.total_sql_statements == 0 {
            quality_score -= 15.0;
        }

        quality_score.max(0.0)
    }
}
