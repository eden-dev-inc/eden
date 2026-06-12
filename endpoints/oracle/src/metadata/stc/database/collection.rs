use super::*;

mod requests;

impl MetadataCollection for OracleDatabaseStats {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        requests::build_requests()
    }

    crate::impl_metadata_collection_boilerplate!(
        "Oracle database statistics and performance metrics",
        "database_stats",
        SyncFrequency::Medium
    );
}
