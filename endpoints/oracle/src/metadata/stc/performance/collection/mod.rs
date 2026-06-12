use super::*;

mod queries_core;
mod queries_memory_io;
mod queries_sessions;

use queries_core::core_queries;
use queries_memory_io::memory_io_queries;
use queries_sessions::session_queries;

impl MetadataCollection for OraclePerformanceStatsCollection {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        let entries = core_queries().into_iter().chain(memory_io_queries()).chain(session_queries()).collect::<Vec<_>>();

        HashMap::from_iter(entries)
    }

    crate::impl_metadata_collection_boilerplate!("Oracle database performance statistics and analysis", "performance", SyncFrequency::High);
}
