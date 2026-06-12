//! Redis Cluster virtualization helpers for the proxy processor.

use super::routing::RoutingState;
use super::*;
use std::future::Future;
use std::pin::Pin;

mod execution;
mod processor;
mod response;
mod topology;
#[doc(hidden)]
pub mod validation;

pub use execution::ClusterExecution;
#[cfg(test)]
use execution::ClusterSessionTransition;
pub(super) use processor::ClusterProcessor;
use response::ClusterResponseRewriter;
#[cfg(test)]
use response::{ClusterRedirectRewrite, ClusterSlotsNodeFrames};
pub use topology::ClusterSupport;
use topology::ClusterTopologyLoader;
#[cfg(test)]
use topology::{ClusterNodesParseWarning, ClusterNodesParseWarningKind, ClusterNodesParser, ClusterTopologyBuilder};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterProxyNodeRole {
    Master,
    Replica,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterProxyNode {
    node_id: String,
    host: String,
    port: u16,
    bus_port: Option<u16>,
    role: ClusterProxyNodeRole,
    master_id: Option<String>,
    flags: Vec<String>,
    slot_ranges: Vec<(u16, u16)>,
    connected: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VirtualClusterNode {
    listener_id: String,
    bind_port: u16,
    advertise_port: u16,
    stable_node_id: String,
    role: ClusterProxyNodeRole,
    effective_slot_ranges: Vec<(u16, u16)>,
    backend: ClusterProxyNode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VirtualClusterTopology {
    endpoint_uuid: EndpointCacheUuid,
    redis_config: RedisConfig,
    advertise_host: String,
    nodes: Vec<VirtualClusterNode>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterClientKey {
    endpoint_uuid: EndpointCacheUuid,
    listener_id: String,
}

struct ClusterClientConnection {
    backend_host: String,
    backend_port: u16,
    readonly_mode: bool,
    client: RedisClient,
}

struct ClusterDispatchResult {
    response: Bytes,
    topology: VirtualClusterTopology,
    listener_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ClusterPinnedRoute {
    endpoint_uuid: EndpointCacheUuid,
    listener_id: String,
}

/// Per-client Redis Cluster virtualization state.
pub struct ClusterConnectionState {
    /// Last interlay cache version used to build the routing and topology views.
    state_version: u64,
    /// Current endpoint routing policy, including migration routing when enabled.
    routing: RoutingState,
    /// Listener that accepted this client connection.
    current_listener_id: String,
    /// Virtual topology for the primary endpoint.
    old_topology: VirtualClusterTopology,
    /// Virtual topology for the migration target endpoint, when traffic can reach it.
    new_topology: Option<VirtualClusterTopology>,
    /// Backend clients keyed by endpoint and listener so session state stays isolated.
    clients: HashMap<ClusterClientKey, ClusterClientConnection>,
    /// Whether the next dispatched command must be prefixed with ASKING.
    asking_next: bool,
    /// Whether read commands may stay on replica listeners when slot ownership permits it.
    readonly_mode: bool,
    /// Whether this client has active WATCH state on a pinned backend connection.
    watching: bool,
    /// Whether this client is inside MULTI/EXEC transaction state.
    in_multi: bool,
    /// Endpoint/listener route that must be reused while WATCH or MULTI state is active.
    pinned_route: Option<ClusterPinnedRoute>,
    /// Deferred topology refresh requested while transaction state prevented rebuilding.
    pending_refresh: bool,
}

#[derive(Debug, Clone)]
pub struct ClusterDirectTarget {
    pub endpoint_uuid: EndpointCacheUuid,
    pub listener_id: String,
    pub backend_host: String,
    pub backend_port: u16,
    pub connection: RedisConnection,
}

#[derive(Debug, Clone)]
pub struct ClusterDirectDispatch {
    pub target: ClusterDirectTarget,
    pub command_bytes: Bytes,
    pub expected_responses: usize,
    pub internal_prefixes: Vec<Bytes>,
    pub pinned: bool,
}

pub struct ClusterDirectCommandResult {
    pub response: Bytes,
    pub needs_topology_refresh: bool,
    pub close_after_response: bool,
    rewrite_topology: Option<VirtualClusterTopology>,
}

pub trait ClusterDirectTransport {
    fn dispatch<'a>(&'a mut self, request: ClusterDirectDispatch) -> Pin<Box<dyn Future<Output = Result<Bytes, EpError>> + Send + 'a>>;

    fn retain_pinned_route(&mut self, route: Option<&ClusterPinnedRoute>);
}

impl ClusterPinnedRoute {
    pub fn endpoint_uuid(&self) -> &EndpointCacheUuid {
        &self.endpoint_uuid
    }

    pub fn listener_id(&self) -> &str {
        &self.listener_id
    }
}

impl VirtualClusterNode {
    pub fn listener_id(&self) -> &str {
        &self.listener_id
    }

    pub fn backend_host(&self) -> &str {
        &self.backend.host
    }

    pub fn backend_port(&self) -> u16 {
        self.backend.port
    }
}

impl ClusterDirectCommandResult {
    fn response(
        response: Bytes,
        needs_topology_refresh: bool,
        close_after_response: bool,
        rewrite_topology: Option<VirtualClusterTopology>,
    ) -> Self {
        Self {
            response,
            needs_topology_refresh,
            close_after_response,
            rewrite_topology,
        }
    }

    pub fn into_response(self, cluster_state: &ClusterConnectionState, command: &RedisApi) -> Result<Bytes, EpError> {
        let Some(dispatch_topology) = self.rewrite_topology else {
            return Ok(self.response);
        };
        let rewrite_topology = ClusterTopologyLoader::topology_for_endpoint(cluster_state, &dispatch_topology.endpoint_uuid)
            .cloned()
            .unwrap_or(dispatch_topology);

        match command {
            RedisApi::ClusterNodes => ClusterResponseRewriter::nodes_response(&self.response, &rewrite_topology),
            RedisApi::ClusterSlots => ClusterResponseRewriter::slots_response(&self.response, &rewrite_topology),
            RedisApi::ClusterShards => ClusterResponseRewriter::shards_response(&self.response, &rewrite_topology),
            _ => Ok(ClusterResponseRewriter::redirect_response(self.response, &rewrite_topology)),
        }
    }
}

static CLUSTER_NODES_COMMAND: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"*2\r\n$7\r\nCLUSTER\r\n$5\r\nNODES\r\n"));
static ASKING_COMMAND: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"*1\r\n$6\r\nASKING\r\n"));
static READONLY_COMMAND: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"*1\r\n$8\r\nREADONLY\r\n"));
static READWRITE_COMMAND: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"*1\r\n$9\r\nREADWRITE\r\n"));
static RESP_OK: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"+OK\r\n"));
static RESP_RESET: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"+RESET\r\n"));
static CROSSSLOT_RESPONSE: Lazy<Bytes> = Lazy::new(|| Bytes::from_static(b"-CROSSSLOT Keys in request don't hash to the same slot\r\n"));
static REDIS_CLUSTER_HASH_CONFIG: Lazy<HashConfig> = Lazy::new(|| HashConfig {
    algorithm: HashAlgorithm::Crc16,
    hash_tag: Some(HashTagDelimiter { open: '{', close: '}' }),
});

#[cfg(test)]
mod tests;
