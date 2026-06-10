use super::*;

impl OracleSegmentInfo {
    pub(crate) fn parse_segment_details(rows: Vec<Row>) -> ResultEP<Vec<OracleSegmentDetails>> {
        map_rows(rows, |row| {
            Ok(OracleSegmentDetails {
                owner: row.get_string("owner")?,
                segment_name: row.get_string("segment_name")?,
                segment_type: row.get_string("segment_type")?,
                tablespace_name: row.get_string("tablespace_name")?,
                bytes: row.get_u64("bytes")?,
                extents: row.get_u64("extents")?,
                initial_extent: row.get_u64("initial_extent")?,
                next_extent: row.get_u64("next_extent")?,
                max_extents: row.get_opt_u64("max_extents")?,
                size_mb: row.get_f64("size_mb")?,
                fragmentation_level: row.get_string("fragmentation_level")?,
            })
        })
    }

    pub(crate) fn parse_fragmented_segments(rows: Vec<Row>) -> ResultEP<Vec<OracleFragmentedSegment>> {
        map_rows(rows, |row| {
            Ok(OracleFragmentedSegment {
                owner: row.get_string("owner")?,
                segment_name: row.get_string("segment_name")?,
                segment_type: row.get_string("segment_type")?,
                tablespace_name: row.get_string("tablespace_name")?,
                bytes: row.get_u64("bytes")?,
                extents: row.get_u64("extents")?,
                max_extents: row.get_opt_u64("max_extents")?,
                initial_extent: row.get_u64("initial_extent")?,
                next_extent: row.get_u64("next_extent")?,
                avg_extent_size: row.get_u64("avg_extent_size")?,
                wasted_space_mb: row.get_f64("wasted_space_mb")?,
            })
        })
    }

    pub(crate) fn parse_growing_segments(rows: Vec<Row>) -> ResultEP<Vec<OracleGrowingSegment>> {
        map_rows(rows, |row| {
            Ok(OracleGrowingSegment {
                owner: row.get_string("owner")?,
                segment_name: row.get_string("segment_name")?,
                segment_type: row.get_string("segment_type")?,
                tablespace_name: row.get_string("tablespace_name")?,
                current_size: row.get_u64("current_size")?,
                current_extents: row.get_u64("current_extents")?,
                new_extents_24h: row.get_u64("new_extents_24h")?,
                growth_bytes_24h: row.get_u64("growth_bytes_24h")?,
                growth_mb_24h: row.get_f64("growth_mb_24h")?,
            })
        })
    }

    pub(crate) fn parse_tablespace_issues(rows: Vec<Row>) -> ResultEP<Vec<OracleTablespaceIssue>> {
        map_rows(rows, |row| {
            Ok(OracleTablespaceIssue {
                tablespace_name: row.get_string("tablespace_name")?,
                total_size_mb: row.get_f64("total_size_mb")?,
                used_size_mb: row.get_f64("used_size_mb")?,
                free_size_mb: row.get_f64("free_size_mb")?,
                usage_pct: row.get_f64("usage_pct")?,
                largest_free_mb: row.get_f64("largest_free_mb")?,
                status: row.get_string("status")?,
            })
        })
    }

    pub(crate) fn parse_chained_segments(rows: Vec<Row>) -> ResultEP<Vec<OracleChainedSegment>> {
        map_rows(rows, |row| {
            Ok(OracleChainedSegment {
                owner: row.get_string("owner")?,
                table_name: row.get_string("table_name")?,
                tablespace_name: row.get_string("tablespace_name")?,
                num_rows: row.get_u64("num_rows")?,
                chain_cnt: row.get_u64("chain_cnt")?,
                avg_row_len: row.get_u64("avg_row_len")?,
                blocks: row.get_u64("blocks")?,
                avg_space: row.get_u64("avg_space")?,
                chain_pct: row.get_f64("chain_pct")?,
            })
        })
    }
}
