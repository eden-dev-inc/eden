use super::*;

impl OracleTableInfo {
    pub(crate) fn parse_table_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTableDetails>> {
        map_rows(rows, |row| {
            Ok(OracleTableDetails {
                owner: row.get_string("owner")?,
                table_name: row.get_string("table_name")?,
                num_rows: row.get_u64("num_rows")?,
                table_size_bytes: row.get_u64("table_size_bytes")?,
                avg_row_len: row.get_u64("avg_row_len")?,
                blocks: row.get_u64("blocks")?,
                empty_blocks: row.get_u64("empty_blocks")?,
                last_analyzed: row.get_opt_string("last_analyzed")?,
                compression: row.get_string("compression")?,
                partitioned: row.get_string("partitioned")?,
                degree: row.get_string("degree")?,
                tablespace_name: row.get_string("tablespace_name")?,
                pct_free: row.get_u64("pct_free")?,
                pct_used: row.get_u64("pct_used")?,
                sample_size: row.get_u64("sample_size")?,
                table_size_mb: row.get_f64("table_size_mb")?,
                rows_per_block: row.get_f64("rows_per_block")?,
                space_utilization_pct: row.get_f64("space_utilization_pct")?,
                issue_severity: row.get_string("issue_severity")?,
            })
        })
    }

    pub(crate) fn parse_lob_details(rows: Vec<Row>) -> ResultEP<Vec<OracleLobDetails>> {
        map_rows(rows, |row| {
            Ok(OracleLobDetails {
                owner: row.get_string("owner")?,
                table_name: row.get_string("table_name")?,
                column_name: row.get_string("column_name")?,
                segment_name: row.get_string("segment_name")?,
                lob_size_bytes: row.get_u64("lob_size_bytes")?,
                in_row: row.get_string("in_row")?,
                chunk: row.get_u64("chunk")?,
                compression: row.get_string("compression")?,
                deduplication: row.get_string("deduplication")?,
                tablespace_name: row.get_string("tablespace_name")?,
                lob_size_mb: row.get_f64("lob_size_mb")?,
            })
        })
    }

    pub(crate) fn parse_constraint_details(rows: Vec<Row>) -> ResultEP<Vec<OracleConstraintDetails>> {
        map_rows(rows, |row| {
            Ok(OracleConstraintDetails {
                owner: row.get_string("owner")?,
                constraint_name: row.get_string("constraint_name")?,
                constraint_type: row.get_string("constraint_type")?,
                table_name: row.get_string("table_name")?,
                status: row.get_string("status")?,
                validated: row.get_string("validated")?,
                deferrable: row.get_string("deferrable")?,
                deferred: row.get_string("deferred")?,
                rely: row.get_string("rely")?,
                bad: row.get_string("bad")?,
                delete_rule: row.get_opt_string("delete_rule")?,
                r_table_name: row.get_opt_string("r_table_name")?,
            })
        })
    }

    pub(crate) fn parse_growth_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTableGrowth>> {
        map_rows(rows, |row| {
            Ok(OracleTableGrowth {
                table_owner: row.get_string("table_owner")?,
                table_name: row.get_string("table_name")?,
                inserts: row.get_u64("inserts")?,
                updates: row.get_u64("updates")?,
                deletes: row.get_u64("deletes")?,
                total_dml: row.get_u64("total_dml")?,
                table_size_bytes: row.get_u64("table_size_bytes")?,
                growth_rate_daily: row.get_f64("growth_rate_daily")?,
                projected_size_30d: row.get_u64("projected_size_30d")?,
                growth_category: row.get_string("growth_category")?,
            })
        })
    }

    pub(crate) fn parse_partition_details(rows: Vec<Row>) -> ResultEP<Vec<OraclePartitionDetails>> {
        map_rows(rows, |row| {
            Ok(OraclePartitionDetails {
                table_owner: row.get_string("table_owner")?,
                table_name: row.get_string("table_name")?,
                partition_name: row.get_string("partition_name")?,
                partition_position: row.get_u64("partition_position")?,
                partition_size_bytes: row.get_u64("partition_size_bytes")?,
                num_rows: row.get_u64("num_rows")?,
                compression: row.get_string("compression")?,
                tablespace_name: row.get_string("tablespace_name")?,
                high_value: row.get_string("high_value")?,
                last_analyzed: row.get_opt_string("last_analyzed")?,
                partition_size_mb: row.get_f64("partition_size_mb")?,
            })
        })
    }

    pub(crate) fn parse_index_details(rows: Vec<Row>) -> ResultEP<Vec<OracleIndexDetails>> {
        map_rows(rows, |row| {
            Ok(OracleIndexDetails {
                owner: row.get_string("owner")?,
                index_name: row.get_string("index_name")?,
                table_name: row.get_string("table_name")?,
                index_type: row.get_string("index_type")?,
                uniqueness: row.get_string("uniqueness")?,
                status: row.get_string("status")?,
                visibility: row.get_string("visibility")?,
                degree: row.get_string("degree")?,
                compression: row.get_string("compression")?,
                distinct_keys: row.get_u64("distinct_keys")?,
                leaf_blocks: row.get_u64("leaf_blocks")?,
                clustering_factor: row.get_u64("clustering_factor")?,
                index_size_bytes: row.get_u64("index_size_bytes")?,
                index_size_mb: row.get_f64("index_size_mb")?,
                selectivity: row.get_f64("selectivity")?,
                last_analyzed: row.get_opt_string("last_analyzed")?,
            })
        })
    }

    pub(crate) fn parse_statistics_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTableStatistics>> {
        map_rows(rows, |row| {
            Ok(OracleTableStatistics {
                owner: row.get_string("owner")?,
                table_name: row.get_string("table_name")?,
                num_rows: row.get_u64("num_rows")?,
                blocks: row.get_u64("blocks")?,
                avg_row_len: row.get_u64("avg_row_len")?,
                sample_size: row.get_u64("sample_size")?,
                last_analyzed: row.get_opt_string("last_analyzed")?,
                staleness_days: row.get_i64("staleness_days")?,
                quality_score: row.get_f64("quality_score")?,
                stats_status: row.get_string("stats_status")?,
            })
        })
    }
}
