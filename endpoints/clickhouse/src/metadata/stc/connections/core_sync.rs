use super::ClickhouseConnectionInfo;
use crate::metadata::stc::utils::{MetadataQueryBatch, RowExt};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::MetadataCollection;
use error::ResultEP;

pub(super) async fn sync_metadata(metadata: &ClickhouseConnectionInfo, context: ClickhouseAsync) -> ResultEP<ClickhouseConnectionInfo> {
    let mut connection_info = ClickhouseConnectionInfo::default();
    let requests = metadata.request();

    let metadata_queries = MetadataQueryBatch::new(context.clone(), &requests, ClickhouseConnectionInfo::QUERY_TIMEOUT);

    let (connection_summary_row, stats_rows, user_rows, connection_history_row) = tokio::try_join!(
        metadata_queries.row(ClickhouseConnectionInfo::QUERY_CONNECTION_SUMMARY),
        metadata_queries.rows(ClickhouseConnectionInfo::QUERY_CONNECTION_STATS),
        metadata_queries.rows(ClickhouseConnectionInfo::QUERY_USER_CONNECTIONS),
        metadata_queries.row(ClickhouseConnectionInfo::QUERY_CONNECTION_HISTORY),
    )?;

    if let Some(row) = connection_summary_row {
        connection_info.total_connections = row.u64_or_zero("total_connections")?;
        connection_info.active_users_count = row.u64_or_zero("unique_users")?;
        connection_info.active_databases_count = row.u64_or_zero("unique_databases")?;
        connection_info.total_connection_memory = row.u64_or_zero("total_memory")?;
        connection_info.avg_connection_duration = row.f64_or_zero("avg_duration")?;
        connection_info.longest_connection_duration = row.f64_or_zero("max_duration")?;
        connection_info.max_connections = row.u64_or_zero("max_connections")?;

        connection_info.avg_memory_per_connection = if connection_info.total_connections > 0 {
            connection_info.total_connection_memory / connection_info.total_connections
        } else {
            0
        };

        connection_info.connection_utilization_pct = if connection_info.max_connections > 0 {
            (connection_info.total_connections as f64 / connection_info.max_connections as f64) * 100.0
        } else {
            0.0
        };
    }

    connection_info.protocol_stats = super::parsers::parse_protocol_stats(&stats_rows)?;
    ClickhouseConnectionInfo::update_protocol_counts(&mut connection_info);
    connection_info.user_connections = super::parsers::parse_user_connections(&user_rows)?;

    if let Some(row) = connection_history_row {
        connection_info.connections_last_minute = row.u64_or_zero("connections_last_minute")?;
        connection_info.connection_failures_last_minute = row.u64_or_zero("failures_last_minute")?;

        connection_info.connection_success_rate_pct = if connection_info.connections_last_minute > 0 {
            let successful = connection_info.connections_last_minute - connection_info.connection_failures_last_minute;
            (successful as f64 / connection_info.connections_last_minute as f64) * 100.0
        } else {
            100.0
        };
    }

    connection_info.detailed_metrics = super::detailed_sync::collect_detailed_metrics_if_needed(&connection_info, context).await?;

    Ok(connection_info)
}
