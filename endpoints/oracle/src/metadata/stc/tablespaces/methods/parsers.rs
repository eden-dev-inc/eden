use super::*;

impl OracleTablespaceInfo {
    pub(crate) fn parse_tablespace_details(rows: Vec<Row>) -> ResultEP<Vec<OracleTablespaceDetails>> {
        map_rows(rows, |row| {
            Ok(OracleTablespaceDetails {
                tablespace_name: row.get_string("tablespace_name")?,
                contents: row.get_string("contents")?,
                status: row.get_string("status")?,
                logging: row.get_string("logging")?,
                force_logging: row.get_string("force_logging")?,
                extent_management: row.get_string("extent_management")?,
                allocation_type: row.get_string("allocation_type")?,
                bigfile: row.get_string("bigfile")?,
                total_bytes: row.get_u64("total_bytes")?,
                used_bytes: row.get_u64("used_bytes")?,
                free_bytes: row.get_u64("free_bytes")?,
                max_bytes: row.get_u64("max_bytes")?,
                usage_percent: row.get_f64("usage_percent")?,
                datafile_count: row.get_u64("datafile_count")?,
                autoextend_count: row.get_u64("autoextend_count")?,
                total_gb: row.get_f64("total_gb")?,
                used_gb: row.get_f64("used_gb")?,
                free_gb: row.get_f64("free_gb")?,
                issue_severity: row.get_string("issue_severity")?,
            })
        })
    }

    pub(crate) fn parse_datafile_details(rows: Vec<Row>) -> ResultEP<Vec<OracleDatafileDetails>> {
        map_rows(rows, |row| {
            Ok(OracleDatafileDetails {
                file_id: row.get_u64("file_id")?,
                file_name: row.get_string("file_name")?,
                tablespace_name: row.get_string("tablespace_name")?,
                bytes: row.get_u64("bytes")?,
                max_bytes: row.get_u64("max_bytes")?,
                autoextensible: row.get_string("autoextensible")?,
                increment_by: row.get_u64("increment_by")?,
                status: row.get_string("status")?,
                online_status: row.get_string("online_status")?,
                size_gb: row.get_f64("size_gb")?,
                max_gb: row.get_f64("max_gb")?,
                usage_percent: row.get_f64("usage_percent")?,
            })
        })
    }
}
