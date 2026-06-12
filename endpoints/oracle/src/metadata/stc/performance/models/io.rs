use super::*;
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct IoStatistics {
    /// File I/O statistics
    pub file_io_stats: Vec<FileIoStat>,
    /// Tablespace I/O statistics
    pub tablespace_io_stats: Vec<TablespaceIoStat>,
    /// I/O summary
    pub io_summary: IoSummary,
    /// Wait events related to I/O
    pub io_wait_events: Vec<WaitEventStat>,
}

/// File I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct FileIoStat {
    /// File ID
    pub file_id: u32,
    /// File name
    pub file_name: String,
    /// Tablespace name
    pub tablespace_name: String,
    /// Physical reads
    pub physical_reads: u64,
    /// Physical writes
    pub physical_writes: u64,
    /// Physical block reads
    pub physical_block_reads: u64,
    /// Physical block writes
    pub physical_block_writes: u64,
    /// Read time (centiseconds)
    pub read_time: u64,
    /// Write time (centiseconds)
    pub write_time: u64,
    /// Average read time (ms)
    pub avg_read_time_ms: f64,
    /// Average write time (ms)
    pub avg_write_time_ms: f64,
    /// File type
    pub file_type: FileType,
}

/// Tablespace I/O statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct TablespaceIoStat {
    /// Tablespace name
    pub tablespace_name: String,
    /// Total reads
    pub total_reads: u64,
    /// Total writes
    pub total_writes: u64,
    /// Average read time
    pub avg_read_time: f64,
    /// Average write time
    pub avg_write_time: f64,
    /// Read IOPS
    pub read_iops: f64,
    /// Write IOPS
    pub write_iops: f64,
    /// Total IOPS
    pub total_iops: f64,
}

/// I/O summary statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct IoSummary {
    /// Total physical reads
    pub total_physical_reads: u64,
    /// Total physical writes
    pub total_physical_writes: u64,
    /// Total I/O requests
    pub total_io_requests: u64,
    /// Average I/O time
    pub avg_io_time_ms: f64,
    /// Read latency percentiles
    pub read_latency_p95: f64,
    pub read_latency_p99: f64,
    /// Write latency percentiles
    pub write_latency_p95: f64,
    pub write_latency_p99: f64,
    /// I/O throughput (MB/s)
    pub io_throughput_mbs: f64,
}
