use bytes::Bytes;
use endpoint_types::request::EpWireRequest;
use ep_core::ReqType;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use redis_core::RedisAsync;

use ep_redis::api::RedisApi;
use ep_redis::protocol::extract_resp_command_str;

use crate::policy;

#[derive(Debug, Clone)]
pub struct ElasticacheBytes(Bytes);

impl ElasticacheBytes {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<Vec<u8>> for ElasticacheBytes {
    fn from(v: Vec<u8>) -> Self {
        ElasticacheBytes(Bytes::from(v))
    }
}

impl From<Bytes> for ElasticacheBytes {
    fn from(v: Bytes) -> Self {
        ElasticacheBytes(v)
    }
}

impl EpWireRequest<RedisAsync> for ElasticacheBytes {
    fn kind(&self) -> EpKind {
        EpKind::Elasticache
    }

    fn request_type(&self) -> ResultEP<ReqType> {
        policy::ensure_raw_bytes_allowed(self.0.as_ref())?;

        if let Some(cmd) = extract_resp_command_str(&self.0)
            && let Ok(api) = RedisApi::try_from(cmd)
        {
            return Ok(api.request_type());
        }

        // Default to Write for safety (writes are more restrictive than reads)
        Ok(ReqType::Write)
    }

    async fn send_raw_bytes(&self, context: &RedisAsync) -> ResultEP<(Bytes, u64)> {
        use ep_core::pool::PoisonGuard;

        let client = context.get().await.map_err(EpError::request)?;
        let mut guard = PoisonGuard::new(client);
        let (response, network_latency_us) = guard.send_command_raw(&self.0).await?;
        guard.disarm();
        Ok((response.to_bytes(), network_latency_us))
    }
}
