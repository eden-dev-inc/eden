use super::ElsRedisPool;
use base64::Engine;
use eden_core::format::cache_uuid::EndpointCacheUuid;

pub async fn lookup_els_credentials(
    pool: &ElsRedisPool,
    endpoint: &EndpointCacheUuid,
    pg_user: &str,
    org_key_provider: Option<&dyn database::encryption::OrgKeyProvider>,
) -> Option<database::els::ResolvedPolicy> {
    let key = format!("els::{endpoint}");
    let raw = pool.els_policy_get_raw(&key, pg_user).await.ok()?;
    let raw = raw?;

    if !raw.starts_with("ENC:") {
        return serde_json::from_str(&raw).ok();
    }

    let provider = org_key_provider?;
    let b64_dek = pool.els_policy_get_raw(&key, "__dek").await.ok()??;
    let key_ref = pool.els_policy_get_raw(&key, "__key_ref").await.ok()??;

    let wrapped = base64::engine::general_purpose::STANDARD.decode(&b64_dek).ok()?;
    let dek_bytes = provider.unwrap(&key_ref, &wrapped).await.ok()?;
    if dek_bytes.len() != database::encryption::KEY_SIZE {
        return None;
    }
    let mut dek = [0u8; database::encryption::KEY_SIZE];
    dek.copy_from_slice(&dek_bytes);

    let json = database::encryption::decrypt_cache_value(&dek, &raw).ok()?;
    serde_json::from_str(&json).ok()
}
