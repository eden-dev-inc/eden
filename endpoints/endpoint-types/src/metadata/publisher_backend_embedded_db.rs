use bytes::Bytes;
use database::db::internal_cache::CompositeCacheKey;
use database::lib::ShardCache;
use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use error::ResultEP;
use std::sync::Arc;

const METADATA_NAMESPACE: &[u8] = b"eden-metadata-output";

pub async fn publish(
    db_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    key: &str,
    field: &str,
    batch_json: &str,
) -> ResultEP<()> {
    db_manager
        .internal_cache()
        .typed_composite::<Bytes>(METADATA_NAMESPACE)?
        .insert(CompositeCacheKey::new(key, field), Bytes::copy_from_slice(batch_json.as_bytes()));
    Ok(())
}

pub async fn read(
    db_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    key: &str,
    field: &str,
) -> ResultEP<Option<String>> {
    db_manager
        .internal_cache()
        .typed_composite::<Bytes>(METADATA_NAMESPACE)?
        .get(&CompositeCacheKey::new(key, field))
        .map_err(error::EpError::cache)?
        .map(|bytes| String::from_utf8(bytes.to_vec()).map_err(error::EpError::parse))
        .transpose()
}
