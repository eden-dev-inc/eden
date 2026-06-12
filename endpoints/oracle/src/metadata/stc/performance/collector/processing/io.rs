use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn process_io_statistics(
        file_io_rows: &[Row],
        tablespace_io_rows: &[Row],
        wait_events: &[WaitEventStat],
    ) -> ResultEP<IoStatistics> {
        let mut io_stats = IoStatistics::default();

        for row in file_io_rows {
            let file_id = row.get_u32("file_id")?;
            let file_name = row.get_string("file_name")?;
            let tablespace_name = row.get_string("tablespace_name")?;
            let physical_reads = row.get_u64("physical_reads")?;
            let physical_writes = row.get_u64("physical_writes")?;
            let physical_block_reads = row.get_u64("physical_block_reads")?;
            let physical_block_writes = row.get_u64("physical_block_writes")?;
            let read_time = row.get_u64("read_time")?;
            let write_time = row.get_u64("write_time")?;
            let avg_read_time_ms = row.get_f64("avg_read_time_ms")?;
            let avg_write_time_ms = row.get_f64("avg_write_time_ms")?;
            let file_type_str = row.get_string("file_type")?;

            let file_type = match file_type_str.as_str() {
                "TEMP" => FileType::Temp,
                "UNDO" => FileType::Undo,
                "REDO" => FileType::Redo,
                "DATA" => FileType::Data,
                _ => FileType::Other,
            };

            let file_io_stat = FileIoStat {
                file_id,
                file_name,
                tablespace_name,
                physical_reads,
                physical_writes,
                physical_block_reads,
                physical_block_writes,
                read_time,
                write_time,
                avg_read_time_ms,
                avg_write_time_ms,
                file_type,
            };

            io_stats.file_io_stats.push(file_io_stat);
        }

        for row in tablespace_io_rows {
            let tablespace_name = row.get_string("tablespace_name")?;
            let total_reads = row.get_u64("total_reads")?;
            let total_writes = row.get_u64("total_writes")?;
            let avg_read_time = row.get_f64("avg_read_time")?;
            let avg_write_time = row.get_f64("avg_write_time")?;

            let tablespace_io_stat = TablespaceIoStat {
                tablespace_name,
                total_reads,
                total_writes,
                avg_read_time,
                avg_write_time,
                read_iops: 0.0,
                write_iops: 0.0,
                total_iops: 0.0,
            };

            io_stats.tablespace_io_stats.push(tablespace_io_stat);
        }

        io_stats.io_wait_events = wait_events
            .iter()
            .filter(|we| matches!(we.category, WaitEventCategory::SystemIo | WaitEventCategory::UserIo))
            .cloned()
            .collect();

        io_stats.io_summary.total_physical_reads = io_stats.file_io_stats.iter().map(|f| f.physical_reads).sum();
        io_stats.io_summary.total_physical_writes = io_stats.file_io_stats.iter().map(|f| f.physical_writes).sum();
        io_stats.io_summary.total_io_requests = io_stats.io_summary.total_physical_reads + io_stats.io_summary.total_physical_writes;

        let total_read_time: f64 = io_stats.file_io_stats.iter().map(|f| f.avg_read_time_ms * f.physical_reads as f64).sum();
        let total_write_time: f64 = io_stats.file_io_stats.iter().map(|f| f.avg_write_time_ms * f.physical_writes as f64).sum();

        if io_stats.io_summary.total_io_requests > 0 {
            io_stats.io_summary.avg_io_time_ms = (total_read_time + total_write_time) / io_stats.io_summary.total_io_requests as f64;
        }

        Ok(io_stats)
    }
}
