use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SqlPerformanceMetrics {
    /// Top SQL statements by elapsed time
    pub top_sql_by_elapsed: Vec<SqlStatistic>,
    /// Top SQL statements by CPU time
    pub top_sql_by_cpu: Vec<SqlStatistic>,
    /// Top SQL statements by executions
    pub top_sql_by_executions: Vec<SqlStatistic>,
    /// Top SQL statements by buffer gets
    pub top_sql_by_buffer_gets: Vec<SqlStatistic>,
    /// SQL performance summary
    pub summary: SqlPerformanceSummary,
}

/// Individual SQL statement statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SqlStatistic {
    /// SQL ID
    pub sql_id: String,
    /// SQL text (first 100 characters)
    pub sql_text: String,
    /// Number of executions
    pub executions: u64,
    /// Total elapsed time (microseconds)
    pub elapsed_time: u64,
    /// Total CPU time (microseconds)
    pub cpu_time: u64,
    /// Average elapsed time per execution
    pub avg_elapsed_time: f64,
    /// Average CPU time per execution
    pub avg_cpu_time: f64,
    /// Buffer gets (logical reads)
    pub buffer_gets: u64,
    /// Disk reads (physical reads)
    pub disk_reads: u64,
    /// Rows processed
    pub rows_processed: u64,
    /// Parse calls
    pub parse_calls: u64,
    /// Optimizer cost
    pub optimizer_cost: Option<u64>,
    /// First load time
    pub first_load_time: DateTimeWrapper,
    /// Last active time
    pub last_active_time: DateTimeWrapper,
    /// Performance rating
    pub performance_rating: SqlPerformanceRating,
}

impl Default for SqlStatistic {
    fn default() -> Self {
        Self {
            sql_id: String::new(),
            sql_text: String::new(),
            executions: 0,
            elapsed_time: 0,
            cpu_time: 0,
            avg_elapsed_time: 0.0,
            avg_cpu_time: 0.0,
            buffer_gets: 0,
            disk_reads: 0,
            rows_processed: 0,
            parse_calls: 0,
            optimizer_cost: None,
            first_load_time: DateTimeWrapper::from(Utc::now()),
            last_active_time: DateTimeWrapper::from(Utc::now()),
            performance_rating: SqlPerformanceRating::Good,
        }
    }
}

/// SQL performance summary
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SqlPerformanceSummary {
    /// Total SQL statements
    pub total_sql_statements: u64,
    /// Total executions
    pub total_executions: u64,
    /// Total elapsed time
    pub total_elapsed_time: u64,
    /// Total CPU time
    pub total_cpu_time: u64,
    /// Average SQL execution time
    pub avg_sql_execution_time: f64,
    /// Hard parse ratio
    pub hard_parse_ratio: f64,
    /// Cursor sharing efficiency
    pub cursor_sharing_efficiency: f64,
    /// SQL cache hit ratio
    pub sql_cache_hit_ratio: f64,
}
