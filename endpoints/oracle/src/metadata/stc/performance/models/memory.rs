use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MemoryUtilization {
    /// SGA statistics
    pub sga_stats: SgaStatistics,
    /// PGA statistics
    pub pga_stats: PgaStatistics,
    /// Buffer pool statistics
    pub buffer_pools: Vec<BufferPoolStat>,
    /// Shared pool statistics
    pub shared_pool: SharedPoolStatistics,
    /// Memory advisors
    pub advisors: MemoryAdvisors,
}

/// SGA (System Global Area) statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SgaStatistics {
    /// Total SGA size
    pub total_size: u64,
    /// Fixed SGA size
    pub fixed_size: u64,
    /// Variable SGA size
    pub variable_size: u64,
    /// Database buffer cache size
    pub buffer_cache_size: u64,
    /// Shared pool size
    pub shared_pool_size: u64,
    /// Large pool size
    pub large_pool_size: u64,
    /// Java pool size
    pub java_pool_size: u64,
    /// Streams pool size
    pub streams_pool_size: u64,
    /// Redo buffer size
    pub redo_buffer_size: u64,
    /// SGA utilization percentage
    pub utilization_pct: f64,
}

/// PGA (Program Global Area) statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PgaStatistics {
    /// Total PGA allocated
    pub total_allocated: u64,
    /// Total PGA used
    pub total_used: u64,
    /// PGA target
    pub pga_target: u64,
    /// Maximum PGA allocated
    pub max_allocated: u64,
    /// PGA cache hit ratio
    pub cache_hit_ratio: f64,
    /// Over allocation count
    pub over_allocation_count: u64,
    /// PGA utilization percentage
    pub utilization_pct: f64,
    /// Work area memory usage
    pub workarea_memory: WorkareaMemoryStats,
}

/// Work area memory statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct WorkareaMemoryStats {
    /// Optimal executions
    pub optimal_executions: u64,
    /// One-pass executions
    pub onepass_executions: u64,
    /// Multi-pass executions
    pub multipass_executions: u64,
    /// Total executions
    pub total_executions: u64,
    /// Optimal percentage
    pub optimal_pct: f64,
    /// One-pass percentage
    pub onepass_pct: f64,
    /// Multi-pass percentage
    pub multipass_pct: f64,
}

/// Buffer pool statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct BufferPoolStat {
    /// Pool name
    pub pool_name: String,
    /// Block size
    pub block_size: u32,
    /// Physical reads
    pub physical_reads: u64,
    /// Physical writes
    pub physical_writes: u64,
    /// Logical reads
    pub logical_reads: u64,
    /// Buffer pool hit ratio
    pub hit_ratio: f64,
    /// Free buffer waits
    pub free_buffer_waits: u64,
    /// Buffer busy waits
    pub buffer_busy_waits: u64,
}

/// Shared pool statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct SharedPoolStatistics {
    /// Total size
    pub total_size: u64,
    /// Free memory
    pub free_memory: u64,
    /// Used memory
    pub used_memory: u64,
    /// Free percentage
    pub free_pct: f64,
    /// Library cache statistics
    pub library_cache: LibraryCacheStats,
    /// Dictionary cache statistics
    pub dictionary_cache: DictionaryCacheStats,
}

/// Library cache statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LibraryCacheStats {
    /// Total gets
    pub gets: u64,
    /// Get hits
    pub get_hits: u64,
    /// Get hit ratio
    pub get_hit_ratio: f64,
    /// Pins
    pub pins: u64,
    /// Pin hits
    pub pin_hits: u64,
    /// Pin hit ratio
    pub pin_hit_ratio: f64,
    /// Reloads
    pub reloads: u64,
    /// Invalidations
    pub invalidations: u64,
}

/// Dictionary cache statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DictionaryCacheStats {
    /// Total gets
    pub gets: u64,
    /// Get hits
    pub get_hits: u64,
    /// Get hit ratio
    pub get_hit_ratio: f64,
    /// Get misses
    pub get_misses: u64,
    /// Scan gets
    pub scan_gets: u64,
    /// Scan misses
    pub scan_misses: u64,
    /// Modifications
    pub modifications: u64,
    /// Flushes
    pub flushes: u64,
}

/// Memory advisors
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MemoryAdvisors {
    /// SGA target advisor
    pub sga_target_advisor: Vec<AdvisorRecommendation>,
    /// PGA target advisor
    pub pga_target_advisor: Vec<AdvisorRecommendation>,
    /// Buffer cache advisor
    pub buffer_cache_advisor: Vec<AdvisorRecommendation>,
    /// Shared pool advisor
    pub shared_pool_advisor: Vec<AdvisorRecommendation>,
}

/// Memory advisor recommendation
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct AdvisorRecommendation {
    /// Memory size (MB)
    pub size_mb: u64,
    /// Size factor
    pub size_factor: f64,
    /// Estimated physical reads
    pub estd_physical_reads: u64,
    /// Estimated time (seconds)
    pub estd_time: f64,
    /// Physical reads improvement factor
    pub estd_pct_of_db_time_for_reads: f64,
}
