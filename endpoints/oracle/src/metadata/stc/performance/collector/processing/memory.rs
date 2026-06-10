use super::*;

impl OraclePerformanceStatsCollection {
    pub(crate) fn process_memory_utilization(
        memory_rows: &[Row],
        buffer_pool_rows: &[Row],
        library_cache_rows: &[Row],
    ) -> ResultEP<MemoryUtilization> {
        let mut memory_util = MemoryUtilization::default();

        for row in memory_rows {
            let memory_type = row.get_string("memory_type")?;
            let name = row.get_string("name")?;
            let value = row.get_u64("bytes_value")?;

            match memory_type.as_str() {
                "SGA" => {
                    match name.as_str() {
                        "Fixed Size" => memory_util.sga_stats.fixed_size = value,
                        "Variable Size" => memory_util.sga_stats.variable_size = value,
                        "Database Buffers" => memory_util.sga_stats.buffer_cache_size = value,
                        "Redo Buffers" => memory_util.sga_stats.redo_buffer_size = value,
                        _ => {}
                    }
                    memory_util.sga_stats.total_size += value;
                }
                "PGA" => match name.as_str() {
                    "total PGA allocated" => memory_util.pga_stats.total_allocated = value,
                    "total PGA used by SQL work areas" => memory_util.pga_stats.total_used = value,
                    "maximum PGA allocated" => memory_util.pga_stats.max_allocated = value,
                    "cache hit percentage" => memory_util.pga_stats.cache_hit_ratio = value as f64,

                    _ => {}
                },
                _ => {}
            }
        }

        for row in buffer_pool_rows {
            let pool_name = row.get_string("pool_name")?;
            let block_size = row.get_u32("block_size")?;
            let physical_reads = row.get_u64("physical_reads")?;
            let physical_writes = row.get_u64("physical_writes")?;
            let logical_reads = row.get_u64("logical_reads")?;
            let hit_ratio = row.get_f64("hit_ratio")?;
            let free_buffer_waits = row.get_u64("free_buffer_waits")?;
            let buffer_busy_waits = row.get_u64("buffer_busy_waits")?;

            let buffer_pool = BufferPoolStat {
                pool_name,
                block_size,
                physical_reads,
                physical_writes,
                logical_reads,
                hit_ratio,
                free_buffer_waits,
                buffer_busy_waits,
            };

            memory_util.buffer_pools.push(buffer_pool);
        }

        if let Some(row) = library_cache_rows.first() {
            let gets = row.get_u64("gets")?;
            let get_hits = row.get_u64("gethits")?;
            let get_hit_ratio = row.get_f64("get_hit_ratio")?;
            let pins = row.get_u64("pins")?;
            let pin_hits = row.get_u64("pinhits")?;
            let pin_hit_ratio = row.get_f64("pin_hit_ratio")?;
            let reloads = row.get_u64("reloads")?;
            let invalidations = row.get_u64("invalidations")?;

            memory_util.shared_pool.library_cache = LibraryCacheStats {
                gets,
                get_hits,
                get_hit_ratio,
                pins,
                pin_hits,
                pin_hit_ratio,
                reloads,
                invalidations,
            };
        };

        if memory_util.sga_stats.total_size > 0 {
            memory_util.sga_stats.utilization_pct = (memory_util.sga_stats.total_size - memory_util.shared_pool.free_memory) as f64
                / memory_util.sga_stats.total_size as f64
                * 100.0;
        }

        if memory_util.pga_stats.total_allocated > 0 {
            memory_util.pga_stats.utilization_pct =
                memory_util.pga_stats.total_used as f64 / memory_util.pga_stats.total_allocated as f64 * 100.0;
        }

        Ok(memory_util)
    }
}
