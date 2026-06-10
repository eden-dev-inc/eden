use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn process_system_stats(rows: &[Row]) -> ResultEP<SystemStatistics> {
        let mut stats = SystemStatistics::default();
        let mut stat_map: HashMap<String, u64> = HashMap::new();

        for row in rows {
            let stat_name = row.get_string("stat_name")?;
            let stat_value = row.get_u64("stat_value")?;
            stat_map.insert(stat_name, stat_value);
        }

        stats.db_cpu_time = stat_map.get("CPU used by this session").copied().unwrap_or(0);
        stats.db_time = stat_map.get("DB time").copied().unwrap_or(1);
        stats.cpu_utilization = if stats.db_time > 0 {
            (stats.db_cpu_time as f64 / stats.db_time as f64) * 100.0
        } else {
            0.0
        };

        stats.parse_count_total = stat_map.get("parse count (total)").copied().unwrap_or(0);
        stats.parse_count_hard = stat_map.get("parse count (hard)").copied().unwrap_or(0);
        stats.execute_count = stat_map.get("execute count").copied().unwrap_or(0);

        let logical_reads = stat_map.get("session logical reads").copied().unwrap_or(0);
        let physical_reads = stat_map.get("physical reads").copied().unwrap_or(0);
        let _physical_writes = stat_map.get("physical writes").copied().unwrap_or(0);

        stats.buffer_cache_hit_ratio = if logical_reads > 0 {
            ((logical_reads - physical_reads) as f64 / logical_reads as f64) * 100.0
        } else {
            0.0
        };

        stats.commits_per_sec = stat_map.get("user commits").copied().unwrap_or(0) as f64;
        stats.rollbacks_per_sec = stat_map.get("user rollbacks").copied().unwrap_or(0) as f64;
        stats.redo_generation_rate = stat_map.get("redo size").copied().unwrap_or(0) as f64;

        Ok(stats)
    }

    pub(crate) fn process_wait_events(rows: &[Row]) -> ResultEP<Vec<WaitEventStat>> {
        let mut wait_events = Vec::new();

        for row in rows {
            let event_name = row.get_string("event")?;
            let wait_class = row.get_string("wait_class")?;
            let total_waits = row.get_u64("total_waits")?;
            let time_waited = row.get_u64("time_waited")?;
            let average_wait = row.get_f64("average_wait")?;
            let pct_db_time = row.get_f64("pct_db_time")?;

            let wait_event = WaitEventStat {
                event_name: event_name.clone(),
                wait_class: wait_class.clone(),
                total_waits,
                total_wait_time: time_waited,
                average_wait_time: average_wait,
                pct_db_time,
                waits_per_sec: 0.0,
                wait_time_per_sec: 0.0,
                severity: Self::classify_wait_event_severity(&event_name, pct_db_time),
                category: Self::classify_wait_event_category(&wait_class),
            };

            wait_events.push(wait_event);
        }

        Ok(wait_events)
    }

    pub(crate) fn process_sql_performance(rows: &[Row]) -> ResultEP<SqlPerformanceMetrics> {
        let mut sql_stats = Vec::new();
        let mut summary = SqlPerformanceSummary::default();

        for row in rows {
            let sql_id = row.get_string("sql_id")?;
            let sql_text = row.get_string("sql_text")?;
            let executions = row.get_u64("executions")?;
            let elapsed_time = row.get_u64("elapsed_time")?;
            let cpu_time = row.get_u64("cpu_time")?;
            let buffer_gets = row.get_u64("buffer_gets")?;
            let disk_reads = row.get_u64("disk_reads")?;
            let rows_processed = row.get_u64("rows_processed")?;
            let parse_calls = row.get_u64("parse_calls")?;
            let avg_elapsed_time = row.get_f64("avg_elapsed_time")?;
            let avg_cpu_time = row.get_f64("avg_cpu_time")?;

            let sql_stat = SqlStatistic {
                sql_id: sql_id.clone(),
                sql_text,
                executions,
                elapsed_time,
                cpu_time,
                avg_elapsed_time,
                avg_cpu_time,
                buffer_gets,
                disk_reads,
                rows_processed,
                parse_calls,
                optimizer_cost: row.get_opt_u64("optimizer_cost")?,
                first_load_time: row.get_datetime("first_load_time")?,
                last_active_time: row.get_datetime("last_active_time")?,
                performance_rating: Self::rate_sql_performance(avg_elapsed_time, buffer_gets, executions),
            };

            summary.total_sql_statements += 1;
            summary.total_executions += executions;
            summary.total_elapsed_time += elapsed_time;
            summary.total_cpu_time += cpu_time;

            sql_stats.push(sql_stat);
        }

        if summary.total_executions > 0 {
            summary.avg_sql_execution_time = summary.total_elapsed_time as f64 / summary.total_executions as f64;
        }

        let mut top_sql_by_elapsed = sql_stats.clone();
        top_sql_by_elapsed.sort_by(|a, b| b.elapsed_time.cmp(&a.elapsed_time));
        top_sql_by_elapsed.truncate(10);

        let mut top_sql_by_cpu = sql_stats.clone();
        top_sql_by_cpu.sort_by(|a, b| b.cpu_time.cmp(&a.cpu_time));
        top_sql_by_cpu.truncate(10);

        let mut top_sql_by_executions = sql_stats.clone();
        top_sql_by_executions.sort_by(|a, b| b.executions.cmp(&a.executions));
        top_sql_by_executions.truncate(10);

        let mut top_sql_by_buffer_gets = sql_stats;
        top_sql_by_buffer_gets.sort_by(|a, b| b.buffer_gets.cmp(&a.buffer_gets));
        top_sql_by_buffer_gets.truncate(10);

        Ok(SqlPerformanceMetrics {
            top_sql_by_elapsed,
            top_sql_by_cpu,
            top_sql_by_executions,
            top_sql_by_buffer_gets,
            summary,
        })
    }
}
