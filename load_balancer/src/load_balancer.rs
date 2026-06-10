use std::{
    sync::{Arc, Mutex},
    time::Instant,
};

use async_trait::async_trait;
use opentelemetry::KeyValue;
use pingora_core::upstreams::peer::HttpPeer;
use pingora_error::Result as PingoraResult;
use pingora_load_balancing::{LoadBalancer, selection::RoundRobin};
use pingora_proxy::{ProxyHttp, Session};

use crate::{LB_METRICS, LbMetrics, config::Config};

pub struct LB {
    upstreams: Upstreams,
    config: Config,
}

pub struct LbContext {
    request_received: Instant,
}

pub struct Upstreams {
    pub upstreams: Arc<LoadBalancer<RoundRobin>>,
    pub sni: String,
}

impl LB {
    pub fn new(upstreams: Upstreams, config: &Config) -> Self {
        Self { upstreams, config: config.to_owned() }
    }
}

#[async_trait]
impl ProxyHttp for LB {
    type CTX = LbContext;

    fn new_ctx(&self) -> Self::CTX {
        LbContext { request_received: Instant::now() }
    }

    async fn upstream_peer(&self, _session: &mut Session, _ctx: &mut LbContext) -> PingoraResult<Box<HttpPeer>> {
        let upstream = self
            .upstreams
            .upstreams
            .select(b"", 256) // hash doesn't matter
            .ok_or_else(|| pingora_error::Error::new_str("No upstream available"))?;

        log::info!("upstream peer is: {:?}", upstream);

        let peer = Box::new(HttpPeer::new(upstream, false, self.upstreams.sni.to_owned()));
        Ok(peer)
    }

    async fn upstream_request_filter(
        &self,
        _session: &mut Session,
        upstream_request: &mut pingora_http::RequestHeader,
        _ctx: &mut Self::CTX,
    ) -> PingoraResult<()> {
        upstream_request.insert_header("Host", self.upstreams.sni.as_str())?;
        Ok(())
    }

    async fn early_request_filter(&self, _session: &mut Session, ctx: &mut Self::CTX) -> pingora_error::Result<()>
    where
        Self::CTX: Send + Sync,
    {
        ctx.request_received = Instant::now();
        Ok(())
    }

    async fn logging(&self, session: &mut Session, e: Option<&pingora_error::Error>, ctx: &mut Self::CTX)
    where
        Self::CTX: Send + Sync,
    {
        let mut labels = vec![KeyValue::new("request_summary", session.request_summary())];
        if let Some(err) = e {
            labels.push(KeyValue::new("error", err.to_string()));
        }
        let lbm = LB_METRICS.get_or_init(|| Mutex::new(LbMetrics::new(&self.config)));
        if let Ok(m) = lbm.lock() {
            m.add_request(&labels);
            m.add_latency(ctx.request_received.elapsed().as_micros() as u64, &labels);
        }
    }
}
