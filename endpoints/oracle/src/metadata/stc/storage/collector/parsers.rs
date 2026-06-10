use super::*;

impl OracleStorageInfo {
    pub(crate) fn parse_tablespace_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTablespaceDetails>> {
        map_rows(rows, |row| {
            Ok(OracleTablespaceDetails {
                tablespace_name: row.get_string("tablespace_name")?,
                status: row.get_string("status")?,
                contents: row.get_string("contents")?,
                extent_management: row.get_string("extent_management")?,
                allocation_type: row.get_string("allocation_type")?,
                total_size: row.get_u64("total_size")?,
                used_size: row.get_u64("used_size")?,
                free_size: row.get_u64("free_size")?,
                usage_pct: row.get_f64("usage_pct")?,
                largest_free_extent: row.get_u64("largest_free_extent")?,
                file_count: row.get_u64("file_count")?,
                autoextend_count: row.get_u64("autoextend_count")?,
                alert_level: row.get_string("alert_level")?,
            })
        })
    }

    pub(crate) fn parse_datafile_details(rows: Vec<Row>) -> ResultEP<Vec<OracleDataFileDetails>> {
        map_rows(rows, |row| {
            Ok(OracleDataFileDetails {
                file_name: row.get_string("file_name")?,
                file_id: row.get_i32("file_id")?,
                tablespace_name: row.get_string("tablespace_name")?,
                bytes: row.get_u64("bytes")?,
                maxbytes: row.get_u64("maxbytes")?,
                increment_by: row.get_u64("increment_by")?,
                autoextensible: row.get_string("autoextensible")?,
                status: row.get_string("status")?,
                size_mb: row.get_f64("size_mb")?,
                pct_of_maxsize: row.get_f64("pct_of_maxsize")?,
                size_status: row.get_string("size_status")?,
            })
        })
    }

    pub(crate) fn parse_growth_analysis(rows: Vec<Row>) -> ResultEP<Vec<OracleStorageGrowth>> {
        map_rows(rows, |row| {
            Ok(OracleStorageGrowth {
                tablespace_name: row.get_string("tablespace_name")?,
                potential_growth_bytes: row.get_u64("potential_growth_bytes")?,
                autoextend_files: row.get_u64("autoextend_files")?,
                avg_increment_size: row.get_u64("avg_increment_size")?,
                max_increment_size: row.get_u64("max_increment_size")?,
                potential_growth_mb: row.get_f64("potential_growth_mb")?,
            })
        })
    }

    pub(crate) fn parse_fragmentation_analysis(rows: Vec<Row>) -> ResultEP<Vec<OracleFragmentationDetails>> {
        map_rows(rows, |row| {
            Ok(OracleFragmentationDetails {
                tablespace_name: row.get_string("tablespace_name")?,
                extent_count: row.get_u64("extent_count")?,
                avg_extent_size: row.get_u64("avg_extent_size")?,
                min_extent_size: row.get_u64("min_extent_size")?,
                max_extent_size: row.get_u64("max_extent_size")?,
                small_extents: row.get_u64("small_extents")?,
                small_extent_bytes: row.get_u64("small_extent_bytes")?,
                avg_extent_kb: row.get_f64("avg_extent_kb")?,
            })
        })
    }

    pub(crate) fn parse_special_tablespaces(rows: Vec<Row>) -> ResultEP<Vec<OracleSpecialTablespace>> {
        map_rows(rows, |row| {
            Ok(OracleSpecialTablespace {
                tablespace_name: row.get_string("tablespace_name")?,
                contents: row.get_string("contents")?,
                status: row.get_string("status")?,
                tablespace_type: row.get_string("tablespace_type")?,
                total_size: row.get_u64("total_size")?,
                used_size: row.get_u64("used_size")?,
                usage_pct: row.get_f64("usage_pct")?,
            })
        })
    }

    pub(crate) fn parse_file_limit_issues(rows: Vec<Row>) -> ResultEP<Vec<OracleFileLimitIssue>> {
        map_rows(rows, |row| {
            Ok(OracleFileLimitIssue {
                file_name: row.get_string("file_name")?,
                tablespace_name: row.get_string("tablespace_name")?,
                bytes: row.get_u64("bytes")?,
                maxbytes: row.get_u64("maxbytes")?,
                increment_by: row.get_u64("increment_by")?,
                current_size_mb: row.get_f64("current_size_mb")?,
                max_size_mb: row.get_f64("max_size_mb")?,
                pct_of_max: row.get_f64("pct_of_max")?,
                remaining_mb: row.get_f64("remaining_mb")?,
                risk_level: row.get_string("risk_level")?,
            })
        })
    }
}
