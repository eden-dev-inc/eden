use super::*;
use function_name::named;

impl OracleTransactionInfo {
    #[allow(dead_code)]
    pub(crate) const LONG_TRANSACTION_THRESHOLD: u64 = 1800;
    #[allow(dead_code)]
    pub(crate) const VERY_LONG_TRANSACTION_THRESHOLD: u64 = 7200;
    pub(crate) const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    pub(crate) const MAX_DETAILED_RESULTS: usize = 100;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: OracleAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut transaction_info = OracleTransactionInfo::default();
        let requests = self.request();

        if let Some(row) = run_single_row(&requests, "transaction_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.active_transactions = row.get_u64("active_transactions")?;
            transaction_info.long_running_transactions = row.get_u64("long_running_transactions")?;
            transaction_info.very_long_transactions = row.get_u64("very_long_transactions")?;
            transaction_info.avg_transaction_duration = row.get_f64("avg_transaction_duration")?;
            transaction_info.max_transaction_duration = row.get_f64("max_transaction_duration")?;
        }

        if let Some(row) = run_single_row(&requests, "commit_rollback_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.user_commits = row.get_u64("user_commits")?;
            transaction_info.user_rollbacks = row.get_u64("user_rollbacks")?;
        }

        if let Some(row) = run_single_row(&requests, "session_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.active_sessions = row.get_u64("active_sessions")?;
            transaction_info.sessions_waiting_locks = row.get_u64("sessions_waiting_locks")?;
            transaction_info.blocking_sessions = row.get_u64("blocking_sessions")?;
        }

        if let Some(row) = run_single_row(&requests, "lock_stats", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.deadlocks_detected = row.get_u64("deadlocks_detected")?;
            transaction_info.lock_timeouts = row.get_u64("lock_timeouts")?;
        }

        if let Some(row) = run_single_row(&requests, "undo_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.undo_segments = row.get_u64("undo_segments")?;
            transaction_info.active_undo_segments = row.get_u64("active_undo_segments")?;
            transaction_info.undo_blocks_used = row.get_u64("undo_blocks_used")?;
        }

        if let Some(row) = run_single_row(&requests, "undo_retention", context.clone(), Self::QUERY_TIMEOUT).await? {
            transaction_info.max_undo_retention = row.get_u64("max_undo_retention")?;
            transaction_info.current_undo_retention = row.get_u64("current_undo_retention")?;
        }

        transaction_info.rollback_ratio = transaction_info.rollback_percentage();
        transaction_info.transaction_health_score = Self::calculate_health_score(&transaction_info);
        transaction_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&transaction_info, context).await?;

        Ok(transaction_info)
    }

    pub(crate) fn calculate_health_score(transaction_info: &OracleTransactionInfo) -> f64 {
        let mut score = 100.0;

        if transaction_info.very_long_transactions > 0 {
            score -= 30.0;
        }

        if transaction_info.active_transactions > 0 {
            let long_ratio = transaction_info.long_running_transactions as f64 / transaction_info.active_transactions as f64;
            score -= long_ratio * 20.0;
        }

        if transaction_info.active_sessions > 0 {
            let blocking_ratio = transaction_info.blocking_sessions as f64 / transaction_info.active_sessions as f64;
            score -= blocking_ratio * 25.0;
        }

        if transaction_info.deadlocks_detected > 0 {
            score -= 15.0;
        }

        if transaction_info.rollback_percentage() > 20.0 {
            score -= 10.0;
        } else if transaction_info.rollback_percentage() > 10.0 {
            score -= 5.0;
        }

        score.clamp(0.0, 100.0)
    }
}
