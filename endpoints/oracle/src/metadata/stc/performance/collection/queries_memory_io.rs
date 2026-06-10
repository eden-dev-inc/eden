use super::*;

pub(super) fn memory_io_queries() -> Vec<(String, QueryInput)> {
    vec![
        (
            "memory_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    'SGA' as memory_type,
                    sg.name,
                    sg.value as bytes_value
                FROM v$sga sg
                UNION ALL
                SELECT
                    'PGA' as memory_type,
                    ps.name,
                    ps.value as bytes_value
                FROM v$pgastat ps
                WHERE ps.name IN (
                    'total PGA allocated',
                    'total PGA used by SQL work areas',
                    'maximum PGA allocated',
                    'cache hit percentage'
                )
                UNION ALL
                SELECT
                    'POOL' as memory_type,
                    sp.pool || '_' || sp.name as name,
                    sp.bytes as bytes_value
                FROM v$sgastat sp
                WHERE sp.name IN ('free memory', 'buffer_cache')"
                    .to_string(),
            ),
        ),
        (
            "buffer_pool_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    bp.name as pool_name,
                    bp.block_size,
                    bp.physical_reads,
                    bp.physical_writes,
                    (bp.db_block_gets + bp.consistent_gets) as logical_reads,
                    CASE
                        WHEN (bp.db_block_gets + bp.consistent_gets) > 0 THEN
                            ROUND((1 - (bp.physical_reads / (bp.db_block_gets + bp.consistent_gets))) * 100, 2)
                        ELSE 0
                    END as hit_ratio,
                    bp.free_buffer_wait as free_buffer_waits,
                    bp.buffer_busy_wait as buffer_busy_waits
                FROM v$buffer_pool_statistics bp"
                    .to_string(),
            ),
        ),
        (
            "library_cache_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    lc.namespace,
                    lc.gets,
                    lc.gethits,
                    CASE
                        WHEN lc.gets > 0 THEN ROUND((lc.gethits / lc.gets) * 100, 2)
                        ELSE 0
                    END as get_hit_ratio,
                    lc.pins,
                    lc.pinhits,
                    CASE
                        WHEN lc.pins > 0 THEN ROUND((lc.pinhits / lc.pins) * 100, 2)
                        ELSE 0
                    END as pin_hit_ratio,
                    lc.reloads,
                    lc.invalidations
                FROM v$librarycache lc"
                    .to_string(),
            ),
        ),
        (
            "file_io_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    f.file_id,
                    f.file_name,
                    f.tablespace_name,
                    fs.phyrds as physical_reads,
                    fs.phywrts as physical_writes,
                    fs.phyblkrd as physical_block_reads,
                    fs.phyblkwrt as physical_block_writes,
                    fs.readtim as read_time,
                    fs.writetim as write_time,
                    CASE
                        WHEN fs.phyrds > 0 THEN (fs.readtim / fs.phyrds) * 10
                        ELSE 0
                    END as avg_read_time_ms,
                    CASE
                        WHEN fs.phywrts > 0 THEN (fs.writetim / fs.phywrts) * 10
                        ELSE 0
                    END as avg_write_time_ms,
                    CASE
                        WHEN f.file_name LIKE '%temp%' THEN 'TEMP'
                        WHEN f.file_name LIKE '%undo%' THEN 'UNDO'
                        WHEN f.file_name LIKE '%redo%' THEN 'REDO'
                        ELSE 'DATA'
                    END as file_type
                FROM dba_data_files f
                JOIN v$filestat fs ON f.file_id = fs.file#
                ORDER BY fs.phyrds + fs.phywrts DESC"
                    .to_string(),
            ),
        ),
        (
            "memory_advisors".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    'SGA_TARGET' as advisor_type,
                    sa.sga_size as size_mb,
                    sa.sga_size_factor as size_factor,
                    sa.estd_db_time_factor,
                    sa.estd_physical_reads
                FROM v$sga_target_advice sa
                WHERE sa.sga_size_factor BETWEEN 0.5 AND 2.0
                UNION ALL
                SELECT
                    'PGA_TARGET' as advisor_type,
                    pa.pga_target_for_estimate as size_mb,
                    pa.pga_target_factor as size_factor,
                    pa.estd_time as estd_db_time_factor,
                    pa.estd_pga_cache_hit_percentage as estd_physical_reads
                FROM v$pga_target_advice pa
                WHERE pa.pga_target_factor BETWEEN 0.5 AND 2.0
                ORDER BY advisor_type, size_factor"
                    .to_string(),
            ),
        ),
        (
            "tablespace_io".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ts.tablespace_name,
                    SUM(fs.phyrds) as total_reads,
                    SUM(fs.phywrts) as total_writes,
                    AVG(CASE WHEN fs.phyrds > 0 THEN fs.readtim / fs.phyrds * 10 ELSE 0 END) as avg_read_time,
                    AVG(CASE WHEN fs.phywrts > 0 THEN fs.writetim / fs.phywrts * 10 ELSE 0 END) as avg_write_time
                FROM dba_tablespaces ts
                JOIN dba_data_files df ON ts.tablespace_name = df.tablespace_name
                JOIN v$filestat fs ON df.file_id = fs.file#
                GROUP BY ts.tablespace_name
                ORDER BY (SUM(fs.phyrds) + SUM(fs.phywrts)) DESC"
                    .to_string(),
            ),
        ),
        (
            "workarea_stats".to_string(),
            crate::metadata::stc::utils::query(
                "SELECT
                    ws.low_optimal_size || '-' || ws.high_optimal_size as operation_type,
                    ws.optimal_executions,
                    ws.onepass_executions,
                    ws.multipasses_executions,
                    ws.total_executions,
                    CASE
                        WHEN ws.total_executions > 0 THEN
                            ROUND((ws.optimal_executions / ws.total_executions) * 100, 2)
                        ELSE 0
                    END as optimal_pct,
                    CASE
                        WHEN ws.total_executions > 0 THEN
                            ROUND((ws.onepass_executions / ws.total_executions) * 100, 2)
                        ELSE 0
                    END as onepass_pct,
                    CASE
                        WHEN ws.total_executions > 0 THEN
                            ROUND((ws.multipasses_executions / ws.total_executions) * 100, 2)
                        ELSE 0
                    END as multipass_pct
                FROM v$sql_workarea_histogram ws
                WHERE ws.total_executions > 0
                ORDER BY ws.total_executions DESC"
                    .to_string(),
            ),
        ),
    ]
}
