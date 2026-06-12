use super::ElsRedisPool;
use eden_core::format::cache_uuid::EndpointCacheUuid;

pub async fn lookup_els_credentials(
    pool: &ElsRedisPool,
    endpoint: &EndpointCacheUuid,
    pg_user: &str,
    _org_key_provider: Option<&dyn database::encryption::OrgKeyProvider>,
) -> Option<database::els::ResolvedPolicy> {
    let key = format!("els::{endpoint}");
    let json = pool.els_policy_get_raw(&key, pg_user).await.ok()??;
    serde_json::from_str(&json).ok()
}
