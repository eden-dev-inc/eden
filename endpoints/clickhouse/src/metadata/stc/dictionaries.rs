use crate::api::lib::QueryInput;
use borsh::{BorshDeserialize, BorshSerialize};
use clickhouse_core::ClickhouseAsync;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

mod core_sync;
mod detailed_sync;
mod parsers;

/// Clickhouse dictionary information and performance statistics.
///
/// Covers dictionary status, load times and cache hit rates.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDictionaryInfo {
    /// Total number of dictionaries
    pub total_dictionaries: u64,
    /// Number of dictionaries that are currently loaded
    pub loaded_dictionaries: u64,
    /// Number of dictionaries that failed to load
    pub failed_dictionaries: u64,
    /// Number of dictionaries currently loading
    pub loading_dictionaries: u64,
    /// Total memory used by all dictionaries in bytes
    pub total_memory_usage: u64,
    /// Total number of elements across all loaded dictionaries
    pub total_elements: u64,
    /// Average load time across all dictionaries in seconds
    pub avg_load_time: f64,
    /// Number of dictionaries with high hit rates (>90%)
    pub high_performance_dictionaries: u64,
    /// Number of dictionaries with low hit rates (<50%)
    pub low_performance_dictionaries: u64,
    /// Total cache hits across all dictionaries
    pub total_cache_hits: u64,
    /// Total cache misses across all dictionaries
    pub total_cache_misses: u64,
    /// Number of dictionaries that need reloading
    pub dictionaries_needing_reload: u64,
    /// Number of external dictionaries (vs built-in)
    pub external_dictionaries: u64,
    /// Detailed metrics collected when problems are detected
    pub detailed_metrics: Option<ClickhouseDictionaryDetailedMetrics>,
}

/// Detailed dictionary metrics collected when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDictionaryDetailedMetrics {
    /// Failed dictionaries with error details
    pub failed_dictionary_details: Vec<ClickhouseFailedDictionary>,
    /// Slow-loading dictionaries
    pub slow_loading_dictionaries: Vec<ClickhouseSlowDictionary>,
    /// Memory-intensive dictionaries
    pub memory_intensive_dictionaries: Vec<ClickhouseMemoryDictionary>,
    /// Poor performing dictionaries (low hit rates)
    pub poor_performance_dictionaries: Vec<ClickhousePoorPerformanceDictionary>,
    /// Recently updated dictionaries
    pub recently_updated_dictionaries: Vec<ClickhouseDictionaryUpdate>,
    /// Dictionary source breakdown
    pub source_breakdown: Vec<ClickhouseDictionarySourceInfo>,
}

impl MetadataCollection for ClickhouseDictionaryInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        query_map([
            (
                Self::QUERY_DICTIONARY_OVERVIEW,
                query(
                    "SELECT
                    count() as total_dictionaries,
                    countIf(status = 'LOADED') as loaded_dictionaries,
                    countIf(status = 'FAILED') as failed_dictionaries,
                    countIf(status = 'LOADING') as loading_dictionaries,
                    sum(bytes_allocated) as total_memory_usage,
                    sum(element_count) as total_elements,
                    avgIf(loading_duration, loading_duration > 0) as avg_load_time,
                    countIf(origin = 'ClickHouse') as external_dictionaries
                FROM system.dictionaries"
                        .to_string(),
                ),
            ),
            (
                Self::QUERY_DICTIONARY_PERFORMANCE,
                query(
                    "SELECT
                    sum(toUInt64(hit_rate * query_count)) as total_cache_hits,
                    sum(toUInt64((1 - hit_rate) * query_count)) as total_cache_misses,
                    countIf(hit_rate > 0.9) as high_performance_dictionaries,
                    countIf(hit_rate < 0.5) as low_performance_dictionaries,
                    countIf(last_exception != '') as dictionaries_needing_reload
                FROM system.dictionaries
                WHERE status = 'LOADED'"
                        .to_string(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential Clickhouse dictionary status and performance metrics"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "dictionary"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }
}

use crate::metadata::stc::utils::{query, query_map};
use function_name::named;
use std::time::Duration;

impl ClickhouseDictionaryInfo {
    const QUERY_DICTIONARY_OVERVIEW: &'static str = "dictionary_overview";
    const QUERY_DICTIONARY_PERFORMANCE: &'static str = "dictionary_performance";
    const SLOW_LOAD_THRESHOLD: f64 = 60.0; // 60 seconds
    const HIGH_MEMORY_THRESHOLD: u64 = 1_073_741_824; // 1GB
    const LOW_HIT_RATE_THRESHOLD: f64 = 0.5; // 50%
    const QUERY_TIMEOUT: Duration = Duration::from_secs(8);
    const MAX_DETAILED_RESULTS: usize = 50;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: ClickhouseAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());
        core_sync::sync_metadata(self, context).await
    }
}

/// Failed dictionary information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseFailedDictionary {
    /// Dictionary name
    pub name: String,
    /// Database name
    pub database: String,
    /// Data source configuration
    pub source: String,
    /// Last exception message
    pub last_exception: String,
    /// When the last exception occurred
    pub last_exception_time: Option<DateTimeWrapper>,
    /// When loading started
    pub loading_start_time: Option<DateTimeWrapper>,
    /// How long the loading attempt took
    pub loading_duration: f64,
    /// Dictionary origin (ClickHouse etc.)
    pub origin: String,
    /// Dictionary type (flat, hashed etc.)
    pub dictionary_type: String,
    /// Key definition
    pub key_definition: String,
    /// Minimum lifetime in seconds
    pub lifetime_min: u64,
    /// Maximum lifetime in seconds
    pub lifetime_max: u64,
}

/// Slow loading dictionary information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseSlowDictionary {
    /// Dictionary name
    pub name: String,
    /// Database name
    pub database: String,
    /// Data source configuration
    pub source: String,
    /// Loading duration in seconds
    pub loading_duration: f64,
    /// When loading started
    pub loading_start_time: Option<DateTimeWrapper>,
    /// Number of elements in the dictionary
    pub element_count: u64,
    /// Memory allocated in bytes
    pub bytes_allocated: u64,
    /// Current status
    pub status: String,
    /// Dictionary origin
    pub origin: String,
    /// Dictionary type
    pub dictionary_type: String,
}

/// Memory-intensive dictionary information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseMemoryDictionary {
    /// Dictionary name
    pub name: String,
    /// Database name
    pub database: String,
    /// Data source configuration
    pub source: String,
    /// Memory allocated in bytes
    pub bytes_allocated: u64,
    /// Number of elements
    pub element_count: u64,
    /// Loading duration
    pub loading_duration: f64,
    /// Last successful update time
    pub last_successful_update_time: Option<DateTimeWrapper>,
    /// Current status
    pub status: String,
    /// Dictionary type
    pub dictionary_type: String,
    /// Dictionary origin
    pub origin: String,
}

/// Poor performance dictionary information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhousePoorPerformanceDictionary {
    /// Dictionary name
    pub name: String,
    /// Database name
    pub database: String,
    /// Data source configuration
    pub source: String,
    /// Cache hits
    pub hits: u64,
    /// Cache misses
    pub misses: u64,
    /// Hit rate (0.0 to 1.0)
    pub hit_rate: f64,
    /// Number of elements
    pub element_count: u64,
    /// Memory allocated
    pub bytes_allocated: u64,
    /// Last successful update
    pub last_successful_update_time: Option<DateTimeWrapper>,
    /// Dictionary type
    pub dictionary_type: String,
}

/// Dictionary update information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDictionaryUpdate {
    /// Dictionary name
    pub name: String,
    /// Database name
    pub database: String,
    /// Data source configuration
    pub source: String,
    /// When the update completed
    pub last_successful_update_time: Option<DateTimeWrapper>,
    /// Number of elements after update
    pub element_count: u64,
    /// Memory allocated after update
    pub bytes_allocated: u64,
    /// Loading duration for the update
    pub loading_duration: f64,
    /// Current status
    pub status: String,
    /// Dictionary type
    pub dictionary_type: String,
}

/// Dictionary source breakdown information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct ClickhouseDictionarySourceInfo {
    /// Data source type/configuration
    pub source: String,
    /// Total number of dictionaries using this source
    pub dictionary_count: u64,
    /// Number of successfully loaded dictionaries
    pub loaded_count: u64,
    /// Number of failed dictionaries
    pub failed_count: u64,
    /// Total memory used by dictionaries from this source
    pub total_memory: u64,
    /// Total elements across all dictionaries from this source
    pub total_elements: u64,
    /// Average loading time for this source type
    pub avg_load_time: f64,
}

impl ClickhouseDictionaryInfo {
    /// Checks if there are failed dictionaries
    pub fn has_failed_dictionaries(&self) -> bool {
        self.failed_dictionaries > 0
    }

    /// Checks if there are dictionaries currently loading
    pub fn has_loading_dictionaries(&self) -> bool {
        self.loading_dictionaries > 0
    }

    /// Checks if average load time is above threshold
    pub fn has_slow_loading(&self, threshold_seconds: f64) -> bool {
        self.avg_load_time > threshold_seconds
    }

    /// Checks if total memory usage is high
    pub fn has_high_memory_usage(&self, threshold_bytes: u64) -> bool {
        self.total_memory_usage > threshold_bytes
    }

    /// Checks if there are dictionaries with poor performance
    pub fn has_poor_performance_dictionaries(&self) -> bool {
        self.low_performance_dictionaries > 0
    }

    /// Checks if there are dictionaries needing reload
    pub fn has_dictionaries_needing_reload(&self) -> bool {
        self.dictionaries_needing_reload > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets overall cache hit rate
    pub fn get_overall_hit_rate(&self) -> f64 {
        let total_requests = self.total_cache_hits + self.total_cache_misses;
        if total_requests == 0 {
            return 0.0;
        }
        self.total_cache_hits as f64 / total_requests as f64
    }

    /// Gets memory usage in MB
    pub fn get_memory_usage_mb(&self) -> f64 {
        self.total_memory_usage as f64 / 1_048_576.0 // Convert bytes to MB
    }

    /// Gets memory usage in GB
    pub fn get_memory_usage_gb(&self) -> f64 {
        self.total_memory_usage as f64 / 1_073_741_824.0 // Convert bytes to GB
    }

    /// Gets average elements per dictionary
    pub fn get_avg_elements_per_dictionary(&self) -> f64 {
        if self.loaded_dictionaries == 0 {
            return 0.0;
        }
        self.total_elements as f64 / self.loaded_dictionaries as f64
    }

    /// Gets average memory per dictionary in MB
    pub fn get_avg_memory_per_dictionary_mb(&self) -> f64 {
        if self.loaded_dictionaries == 0 {
            return 0.0;
        }
        (self.total_memory_usage as f64 / self.loaded_dictionaries as f64) / 1_048_576.0
    }

    /// Gets dictionary load success rate
    pub fn get_load_success_rate(&self) -> f64 {
        if self.total_dictionaries == 0 {
            return 0.0;
        }
        self.loaded_dictionaries as f64 / self.total_dictionaries as f64
    }

    /// Gets percentage of external vs built-in dictionaries
    pub fn get_external_dictionary_percentage(&self) -> f64 {
        if self.total_dictionaries == 0 {
            return 0.0;
        }
        (self.external_dictionaries as f64 / self.total_dictionaries as f64) * 100.0
    }

    /// Gets performance distribution summary
    pub fn get_performance_summary(&self) -> DictionaryPerformanceSummary {
        DictionaryPerformanceSummary {
            high_performance_count: self.high_performance_dictionaries,
            low_performance_count: self.low_performance_dictionaries,
            medium_performance_count: self
                .loaded_dictionaries
                .saturating_sub(self.high_performance_dictionaries)
                .saturating_sub(self.low_performance_dictionaries),
            overall_hit_rate: self.get_overall_hit_rate(),
        }
    }

    /// Checks overall dictionary health
    pub fn get_health_status(&self) -> DictionaryHealthStatus {
        let failed_rate = if self.total_dictionaries > 0 {
            self.failed_dictionaries as f64 / self.total_dictionaries as f64
        } else {
            0.0
        };

        let poor_performance_rate = if self.loaded_dictionaries > 0 {
            self.low_performance_dictionaries as f64 / self.loaded_dictionaries as f64
        } else {
            0.0
        };

        let overall_hit_rate = self.get_overall_hit_rate();

        if failed_rate > 0.1 || poor_performance_rate > 0.2 || overall_hit_rate < 0.7 {
            DictionaryHealthStatus::Poor
        } else if failed_rate > 0.05 || poor_performance_rate > 0.1 || overall_hit_rate < 0.8 {
            DictionaryHealthStatus::Warning
        } else {
            DictionaryHealthStatus::Good
        }
    }
}

/// Dictionary performance summary
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct DictionaryPerformanceSummary {
    /// Number of high-performance dictionaries (>90% hit rate)
    pub high_performance_count: u64,
    /// Number of low-performance dictionaries (<50% hit rate)
    pub low_performance_count: u64,
    /// Number of medium-performance dictionaries (50-90% hit rate)
    pub medium_performance_count: u64,
    /// Overall hit rate across all dictionaries
    pub overall_hit_rate: f64,
}

/// Dictionary health status
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum DictionaryHealthStatus {
    /// All dictionaries are performing well
    Good,
    /// Some issues detected but not critical
    Warning,
    /// Significant issues requiring attention
    Poor,
}

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::test_utils::database_test_utils::connect_to_clickhouse;
//     use crate::test_utils::database_manager_test_utils::create_database_manager;
//
//     #[tokio::test]
//     async fn test_clickhouse_dictionary_info() {
//         let (_redis, _clickhouse, _db_manager) = create_database_manager().await;
//
//         let (_clickhouse, endpoint_cache_uuid, clickhouse_ep, telemetry_wrapper) =
//             connect_to_clickhouse().await;
//
//         let dictionary_info = ClickhouseDictionaryInfo::default();
//
//         let result = dictionary_info
//             .sync_metadata(
//                 clickhouse_ep
//                     .0
//                     .read_conn_async(&endpoint_cache_uuid, telemetry_wrapper)
//                     .await?;
//                     .expect("failed to get connection")
//                     .to_owned(),
//                 telemetry_wrapper,
//             )
//             .await;
//
//         assert!(result.is_ok());
//         let info = result.unwrap_or_default();
//
//         // Verify core metrics are collected
//         assert!(info.get_overall_hit_rate() >= 0.0);
//         assert!(info.get_overall_hit_rate() <= 1.0);
//         assert!(info.get_load_success_rate() >= 0.0);
//         assert!(info.get_load_success_rate() <= 1.0);
//     }
//
//     #[test]
//     fn test_clickhouse_dictionary_calculations() {
//         let mut dict_info = ClickhouseDictionaryInfo::default();
//         dict_info.total_dictionaries = 10;
//         dict_info.loaded_dictionaries = 8;
//         dict_info.failed_dictionaries = 2;
//         dict_info.total_cache_hits = 9000;
//         dict_info.total_cache_misses = 1000;
//         dict_info.total_memory_usage = 2_147_483_648; // 2GB
//         dict_info.total_elements = 1_000_000;
//         dict_info.high_performance_dictionaries = 6;
//         dict_info.low_performance_dictionaries = 1;
//
//         assert_eq!(dict_info.get_overall_hit_rate(), 0.9);
//         assert_eq!(dict_info.get_load_success_rate(), 0.8);
//         assert_eq!(dict_info.get_memory_usage_gb(), 2.0);
//         assert_eq!(dict_info.get_avg_elements_per_dictionary(), 125_000.0);
//         assert!(dict_info.has_failed_dictionaries());
//
//         let performance_summary = dict_info.get_performance_summary();
//         assert_eq!(performance_summary.high_performance_count, 6);
//         assert_eq!(performance_summary.low_performance_count, 1);
//         assert_eq!(performance_summary.medium_performance_count, 1);
//
//         let health_status = dict_info.get_health_status();
//         assert!(matches!(health_status, DictionaryHealthStatus::Warning));
//     }
//
//     #[test]
//     fn test_dictionary_health_status() {
//         // Test good health
//         let mut good_dict = ClickhouseDictionaryInfo::default();
//         good_dict.total_dictionaries = 10;
//         good_dict.loaded_dictionaries = 10;
//         good_dict.failed_dictionaries = 0;
//         good_dict.total_cache_hits = 9500;
//         good_dict.total_cache_misses = 500;
//         good_dict.low_performance_dictionaries = 0;
//
//         assert!(matches!(good_dict.get_health_status(), DictionaryHealthStatus::Good));
//
//         // Test poor health
//         let mut poor_dict = ClickhouseDictionaryInfo::default();
//         poor_dict.total_dictionaries = 10;
//         poor_dict.loaded_dictionaries = 7;
//         poor_dict.failed_dictionaries = 3; // 30% failure rate
//         poor_dict.total_cache_hits = 6000;
//         poor_dict.total_cache_misses = 4000;
//         poor_dict.low_performance_dictionaries = 3;
//
//         assert!(matches!(poor_dict.get_health_status(), DictionaryHealthStatus::Poor));
//     }
// }
