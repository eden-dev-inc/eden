use crate::api::{InfoInput, RedisJsonValue};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{MetadataCollection, SyncFrequency};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct RedisServerInfo {
    pub redis_version: String,
    pub redis_git_sha1: String,
    pub redis_git_dirty: bool,
    pub redis_build_id: String,
    pub redis_mode: RedisMode,
    pub os: String,
    pub arch_bits: u32,
    pub multiplexing_api: String,
    pub atomicvar_api: String,
    pub gcc_version: String,
    pub process_id: u32,
    pub process_supervised: ProcessSupervised,
    pub run_id: String,
    pub tcp_port: u16,
    pub server_time_usec: u64,
    pub uptime_in_seconds: u64,
    pub uptime_in_days: u32,
    pub hz: u32,
    pub configured_hz: u32,
    pub lru_clock: u64,
    pub executable: Option<String>,
    pub config_file: Option<String>,
    pub io_threads_active: bool,
    pub shutdown_in_milliseconds: Option<u64>,
}

impl MetadataCollection for RedisServerInfo {
    type Request = InfoInput;

    fn request(&self) -> Self::Request {
        Self::Request::new(Some(vec![RedisJsonValue::String("server".to_string())]))
    }
    fn description(&self) -> &'static str {
        "Return the server information for the Redis database"
    }
    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }
    fn category(&self) -> &'static str {
        "server"
    }
    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum RedisMode {
    Standalone,
    Sentinel,
    Cluster,
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ProcessSupervised {
    Upstart,
    Systemd,
    Unknown,
    No,
}

// Default implementations for nested structs
impl Default for RedisServerInfo {
    fn default() -> Self {
        Self {
            redis_version: String::new(),
            redis_git_sha1: String::new(),
            redis_git_dirty: false,
            redis_build_id: String::new(),
            redis_mode: RedisMode::Standalone,
            os: String::new(),
            arch_bits: 64,
            multiplexing_api: String::new(),
            atomicvar_api: String::new(),
            gcc_version: String::new(),
            process_id: 0,
            process_supervised: ProcessSupervised::No,
            run_id: String::new(),
            tcp_port: 6379,
            server_time_usec: 0,
            uptime_in_seconds: 0,
            uptime_in_days: 0,
            hz: 10,
            configured_hz: 10,
            lru_clock: 0,
            executable: None,
            config_file: None,
            io_threads_active: false,
            shutdown_in_milliseconds: None,
        }
    }
}
