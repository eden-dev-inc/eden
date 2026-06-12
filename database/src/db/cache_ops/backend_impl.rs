use super::super::lib::{DatabaseManager, EdenClickhouseConnection, EdenRedisConnection, ShardCache};
use super::CacheOps;
use eden_core::error::ResultEP;

impl<R, P, C> CacheOps for DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: Sync + Send + 'static,
    C: EdenClickhouseConnection + Sync,
{
    async fn kv_set_ex(&self, key: String, value: String, ttl_secs: u64) -> ResultEP<()> {
        self.internal_cache().kv_set_ex(key, value, ttl_secs).await
    }

    async fn kv_get(&self, key: &str) -> ResultEP<Option<String>> {
        self.internal_cache().kv_get(key).await
    }

    async fn kv_del(&self, key: &str) -> ResultEP<()> {
        self.internal_cache().kv_del(key).await
    }

    async fn kv_get_del(&self, key: &str) -> ResultEP<Option<String>> {
        self.internal_cache().kv_get_del(key).await
    }

    async fn kv_expire(&self, key: &str, ttl_secs: u64) -> ResultEP<()> {
        self.internal_cache().kv_expire(key, ttl_secs).await
    }

    async fn clear_all(&self) -> ResultEP<()> {
        self.internal_cache().clear_all().await
    }
}
