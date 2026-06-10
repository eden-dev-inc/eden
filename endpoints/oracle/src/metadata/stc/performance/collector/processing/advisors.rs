use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn process_memory_advisors(rows: &[Row]) -> ResultEP<MemoryAdvisors> {
        let mut advisors = MemoryAdvisors::default();

        for row in rows {
            let advisor_type = row.get_string("advisor_type")?;
            let size_mb = row.get_u64("size_mb")?;
            let size_factor = row.get_f64("size_factor")?;
            let estd_db_time_factor = row.get_f64("estd_db_time_factor")?;
            let estd_physical_reads = row.get_u64("estd_physical_reads")?;

            let recommendation = AdvisorRecommendation {
                size_mb,
                size_factor,
                estd_physical_reads,
                estd_time: estd_db_time_factor,
                estd_pct_of_db_time_for_reads: 0.0,
            };

            match advisor_type.as_str() {
                "SGA_TARGET" => advisors.sga_target_advisor.push(recommendation),
                "PGA_TARGET" => advisors.pga_target_advisor.push(recommendation),
                _ => {}
            }
        }

        Ok(advisors)
    }

    pub(crate) fn process_workarea_stats(rows: &[Row]) -> ResultEP<WorkareaMemoryStats> {
        let mut workarea_stats = WorkareaMemoryStats::default();

        for row in rows {
            let optimal_executions = row.get_u64("optimal_executions")?;
            let onepass_executions = row.get_u64("onepass_executions")?;
            let multipass_executions = row.get_u64("multipasses_executions")?;
            let total_executions = row.get_u64("total_executions")?;

            workarea_stats.optimal_executions += optimal_executions;
            workarea_stats.onepass_executions += onepass_executions;
            workarea_stats.multipass_executions += multipass_executions;
            workarea_stats.total_executions += total_executions;
        }

        if workarea_stats.total_executions > 0 {
            workarea_stats.optimal_pct = workarea_stats.optimal_executions as f64 / workarea_stats.total_executions as f64 * 100.0;
            workarea_stats.onepass_pct = workarea_stats.onepass_executions as f64 / workarea_stats.total_executions as f64 * 100.0;
            workarea_stats.multipass_pct = workarea_stats.multipass_executions as f64 / workarea_stats.total_executions as f64 * 100.0;
        }

        Ok(workarea_stats)
    }
}
