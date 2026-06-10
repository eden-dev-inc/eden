//! Redis Cluster topology discovery, parsing, and refresh.

use super::execution::ClusterExecution;
use super::*;

const REDIS_CLUSTER_MAX_SLOT: u16 = 16_383;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClusterNodesParseResult {
    pub(super) nodes: Vec<ClusterProxyNode>,
    pub(super) warnings: Vec<ClusterNodesParseWarning>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ClusterNodesParseWarning {
    pub(super) node_id: String,
    pub(super) token: String,
    pub(super) kind: ClusterNodesParseWarningKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClusterNodesParseWarningKind {
    InvalidPort,
    InvalidSlotToken,
    ReversedSlotRange,
    OutOfRangeSlot,
    ClampedSlotRange,
}

pub struct ClusterSupport;

impl ClusterSupport {
    pub fn supports_virtual_cluster_proxy(state: &InterlayState, listener_id: &str) -> bool {
        state.endpoint_kind() == EpKind::Redis
            && state.routing().is_direct()
            && state.command_policy_value().is_none()
            && state.audit_config_value().is_none()
            && state.listeners().len() > 1
            && state.advertise_host().is_some()
            && state.listeners().iter().any(|listener| listener.id() == listener_id)
    }
}

impl ClusterNodesParseWarningKind {
    fn metric_error_type(self) -> &'static str {
        match self {
            ClusterNodesParseWarningKind::InvalidPort => "cluster_nodes_invalid_port",
            ClusterNodesParseWarningKind::InvalidSlotToken => "cluster_nodes_invalid_slot_token",
            ClusterNodesParseWarningKind::ReversedSlotRange => "cluster_nodes_reversed_slot_range",
            ClusterNodesParseWarningKind::OutOfRangeSlot => "cluster_nodes_out_of_range_slot",
            ClusterNodesParseWarningKind::ClampedSlotRange => "cluster_nodes_clamped_slot_range",
        }
    }

    fn log_reason(self) -> &'static str {
        match self {
            ClusterNodesParseWarningKind::InvalidPort => "invalid node port",
            ClusterNodesParseWarningKind::InvalidSlotToken => "invalid slot token",
            ClusterNodesParseWarningKind::ReversedSlotRange => "reversed slot range",
            ClusterNodesParseWarningKind::OutOfRangeSlot => "out-of-range slot",
            ClusterNodesParseWarningKind::ClampedSlotRange => "clamped slot range",
        }
    }
}

pub(super) struct ClusterNodesParser;

impl ClusterNodesParser {
    #[cfg(test)]
    pub(super) fn parse(raw: &str) -> Vec<ClusterProxyNode> {
        Self::parse_with_warnings(raw).nodes
    }

    pub(super) fn parse_with_warnings(raw: &str) -> ClusterNodesParseResult {
        let mut nodes = Vec::new();
        let mut warnings = Vec::new();

        for line in raw.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 8 {
                continue;
            }

            let address = parts[1];
            let flags: Vec<String> = parts[2].split(',').map(ToString::to_string).collect();
            let link_state = parts[7];

            if flags.iter().any(|flag| matches!(flag.as_str(), "fail" | "fail?" | "handshake" | "noaddr")) {
                continue;
            }

            let role = if flags.iter().any(|flag| flag == "master") {
                ClusterProxyNodeRole::Master
            } else if flags.iter().any(|flag| flag == "slave" || flag == "replica") {
                ClusterProxyNodeRole::Replica
            } else {
                continue;
            };

            let (host_and_port, bus_port) = match address.split_once('@') {
                Some((host_and_port, bus)) => (host_and_port, bus.parse::<u16>().ok()),
                None => (address, None),
            };

            let Some((host, port)) = host_and_port.rsplit_once(':') else {
                continue;
            };
            let Some(port) = port.parse::<u16>().ok() else {
                warnings.push(ClusterNodesParseWarning {
                    node_id: parts[0].to_string(),
                    token: port.to_string(),
                    kind: ClusterNodesParseWarningKind::InvalidPort,
                });
                continue;
            };

            let mut slot_ranges = Vec::new();
            for slot_arg in parts.iter().skip(8) {
                if slot_arg.starts_with('[') {
                    continue;
                }
                if let Some(range) = Self::parse_slot_range(slot_arg, parts[0], &mut warnings) {
                    slot_ranges.push(range);
                }
            }

            nodes.push(ClusterProxyNode {
                node_id: parts[0].to_string(),
                host: host.to_string(),
                port,
                bus_port,
                role,
                master_id: (parts[3] != "-").then(|| parts[3].to_string()),
                flags,
                slot_ranges,
                connected: link_state == "connected",
            });
        }

        ClusterNodesParseResult { nodes, warnings }
    }

    fn parse_slot_range(slot_arg: &str, node_id: &str, warnings: &mut Vec<ClusterNodesParseWarning>) -> Option<(u16, u16)> {
        if let Some((start, end)) = slot_arg.split_once('-') {
            let start = Self::parse_slot_bound(start, slot_arg, node_id, warnings)?;
            let end = Self::parse_slot_bound(end, slot_arg, node_id, warnings)?;

            if start > end {
                warnings.push(ClusterNodesParseWarning {
                    node_id: node_id.to_string(),
                    token: slot_arg.to_string(),
                    kind: ClusterNodesParseWarningKind::ReversedSlotRange,
                });
                return None;
            }
            if start > u32::from(REDIS_CLUSTER_MAX_SLOT) {
                warnings.push(ClusterNodesParseWarning {
                    node_id: node_id.to_string(),
                    token: slot_arg.to_string(),
                    kind: ClusterNodesParseWarningKind::OutOfRangeSlot,
                });
                return None;
            }
            if end > u32::from(REDIS_CLUSTER_MAX_SLOT) {
                warnings.push(ClusterNodesParseWarning {
                    node_id: node_id.to_string(),
                    token: slot_arg.to_string(),
                    kind: ClusterNodesParseWarningKind::ClampedSlotRange,
                });
            }

            return Some((start as u16, end.min(u32::from(REDIS_CLUSTER_MAX_SLOT)) as u16));
        }

        let single = Self::parse_slot_bound(slot_arg, slot_arg, node_id, warnings)?;
        if single > u32::from(REDIS_CLUSTER_MAX_SLOT) {
            warnings.push(ClusterNodesParseWarning {
                node_id: node_id.to_string(),
                token: slot_arg.to_string(),
                kind: ClusterNodesParseWarningKind::OutOfRangeSlot,
            });
            return None;
        }

        Some((single as u16, single as u16))
    }

    fn parse_slot_bound(value: &str, token: &str, node_id: &str, warnings: &mut Vec<ClusterNodesParseWarning>) -> Option<u32> {
        let Some(slot) = value.parse::<u32>().ok() else {
            warnings.push(ClusterNodesParseWarning {
                node_id: node_id.to_string(),
                token: token.to_string(),
                kind: ClusterNodesParseWarningKind::InvalidSlotToken,
            });
            return None;
        };

        Some(slot)
    }
}

pub(super) struct ClusterNodesParseWarnings;

impl ClusterNodesParseWarnings {
    fn record(
        warnings: &[ClusterNodesParseWarning],
        org_uuid: Option<&str>,
        interlay_id: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
        ctx: Option<&LogContext>,
    ) {
        let (Some(org_uuid), Some(interlay_id)) = (org_uuid, interlay_id) else {
            return;
        };

        for warning in warnings {
            telemetry_wrapper.record(MetricEvent::ProxyError {
                org_uuid,
                interlay_uuid: interlay_id,
                error_type: warning.kind.metric_error_type(),
            });
            if let Some(ctx) = ctx {
                log_warn!(
                    ctx.clone(),
                    "Malformed Redis CLUSTER NODES entry",
                    audience = LogAudience::Internal,
                    node_id = warning.node_id.clone(),
                    token = warning.token.clone(),
                    reason = warning.kind.log_reason()
                );
            }
        }
    }
}

pub(super) struct ClusterTopologyBuilder;

impl ClusterTopologyBuilder {
    pub(super) fn ordered_nodes(nodes: &[ClusterProxyNode]) -> Vec<ClusterProxyNode> {
        let mut masters: Vec<ClusterProxyNode> = nodes.iter().filter(|node| node.role == ClusterProxyNodeRole::Master).cloned().collect();
        masters.sort_by(|left, right| {
            let left_start = left.slot_ranges.iter().map(|(start, _)| *start).min().unwrap_or(u16::MAX);
            let right_start = right.slot_ranges.iter().map(|(start, _)| *start).min().unwrap_or(u16::MAX);
            left_start.cmp(&right_start).then_with(|| left.node_id.cmp(&right.node_id))
        });

        let mut replicas_by_master: BTreeMap<String, Vec<ClusterProxyNode>> = BTreeMap::new();
        for replica in nodes.iter().filter(|node| node.role == ClusterProxyNodeRole::Replica).cloned() {
            let key = replica.master_id.clone().unwrap_or_else(|| replica.node_id.clone());
            replicas_by_master.entry(key).or_default().push(replica);
        }
        for replicas in replicas_by_master.values_mut() {
            replicas.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        }

        let mut ordered = Vec::with_capacity(nodes.len());
        for master in masters {
            let master_id = master.node_id.clone();
            ordered.push(master);
            if let Some(replicas) = replicas_by_master.remove(&master_id) {
                ordered.extend(replicas);
            }
        }

        let mut remaining_replicas: Vec<ClusterProxyNode> = replicas_by_master.into_values().flatten().collect();
        remaining_replicas.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        ordered.extend(remaining_replicas);

        ordered
    }

    pub(super) fn build(
        endpoint_uuid: &EndpointCacheUuid,
        listeners: &[ep_core::database::schema::interlay::InterlayListener],
        advertise_host: &str,
        ordered_nodes: &[ClusterProxyNode],
        template: Option<&VirtualClusterTopology>,
    ) -> Result<VirtualClusterTopology, EpError> {
        if ordered_nodes.is_empty() {
            return Err(EpError::request("cluster-aware interlay requires a Redis Cluster source topology"));
        }

        let nodes = if let Some(template) = template {
            Self::build_from_template(endpoint_uuid, listeners, ordered_nodes, template)?
        } else {
            Self::build_from_listeners(listeners, ordered_nodes)?
        };

        Ok(VirtualClusterTopology {
            endpoint_uuid: endpoint_uuid.clone(),
            redis_config: RedisConfig::default(),
            advertise_host: advertise_host.to_string(),
            nodes,
        })
    }

    fn effective_slot_ranges(node: &ClusterProxyNode, nodes: &[ClusterProxyNode]) -> Vec<(u16, u16)> {
        match node.role {
            ClusterProxyNodeRole::Master => node.slot_ranges.clone(),
            ClusterProxyNodeRole::Replica => node
                .master_id
                .as_deref()
                .and_then(|master_id| nodes.iter().find(|candidate| candidate.node_id == master_id))
                .map(|master| master.slot_ranges.clone())
                .unwrap_or_default(),
        }
    }

    fn stable_virtual_node_id(listener_id: &str) -> String {
        format!("eden-{}", listener_id)
    }

    fn backend_matches_template(template: &VirtualClusterNode, candidate: &ClusterProxyNode, ordered_nodes: &[ClusterProxyNode]) -> bool {
        candidate.role == template.role
            && (template.effective_slot_ranges.is_empty()
                || Self::effective_slot_ranges(candidate, ordered_nodes) == template.effective_slot_ranges)
    }

    fn select_backend_for_template(
        index: usize,
        template: &VirtualClusterNode,
        ordered_nodes: &[ClusterProxyNode],
        used: &[bool],
        allow_disconnected_match: bool,
    ) -> Option<(usize, ClusterProxyNode)> {
        let indexed_nodes: Vec<_> = ordered_nodes.iter().enumerate().filter(|(candidate_idx, _)| !used[*candidate_idx]).collect();
        let choose = |predicate: &dyn Fn(usize, &ClusterProxyNode) -> bool| {
            indexed_nodes
                .iter()
                .find(|(candidate_idx, candidate)| predicate(*candidate_idx, candidate))
                .map(|(candidate_idx, candidate)| (*candidate_idx, (*candidate).clone()))
        };

        choose(&|_, candidate| {
            candidate.connected
                && candidate.node_id == template.backend.node_id
                && Self::backend_matches_template(template, candidate, ordered_nodes)
        })
        .or_else(|| {
            allow_disconnected_match
                .then(|| {
                    choose(&|_, candidate| {
                        candidate.node_id == template.backend.node_id && Self::backend_matches_template(template, candidate, ordered_nodes)
                    })
                })
                .flatten()
        })
        .or_else(|| {
            choose(&|candidate_idx, candidate| {
                candidate.connected && candidate_idx == index && Self::backend_matches_template(template, candidate, ordered_nodes)
            })
        })
        .or_else(|| {
            allow_disconnected_match
                .then(|| {
                    choose(&|candidate_idx, candidate| {
                        candidate_idx == index && Self::backend_matches_template(template, candidate, ordered_nodes)
                    })
                })
                .flatten()
        })
        .or_else(|| choose(&|_, candidate| candidate.connected && Self::backend_matches_template(template, candidate, ordered_nodes)))
        .or_else(|| {
            allow_disconnected_match
                .then(|| choose(&|_, candidate| Self::backend_matches_template(template, candidate, ordered_nodes)))
                .flatten()
        })
    }

    fn build_from_template(
        endpoint_uuid: &EndpointCacheUuid,
        listeners: &[ep_core::database::schema::interlay::InterlayListener],
        ordered_nodes: &[ClusterProxyNode],
        template: &VirtualClusterTopology,
    ) -> Result<Vec<VirtualClusterNode>, EpError> {
        if template.nodes.len() != listeners.len() {
            return Err(EpError::request(format!(
                "listener count ({}) no longer matches frozen cluster topology ({})",
                listeners.len(),
                template.nodes.len()
            )));
        }

        if ordered_nodes.len() > listeners.len() {
            return Err(EpError::request(format!(
                "listener count ({}) does not match cluster node count ({})",
                listeners.len(),
                ordered_nodes.len()
            )));
        }

        let allow_disconnected_match = template.endpoint_uuid == *endpoint_uuid;
        let mut used = vec![false; ordered_nodes.len()];
        let mut next_nodes = Vec::with_capacity(listeners.len());

        for (index, (listener, template_node)) in listeners.iter().zip(template.nodes.iter()).enumerate() {
            let backend = match Self::select_backend_for_template(index, template_node, ordered_nodes, &used, allow_disconnected_match) {
                Some((candidate_idx, backend)) => {
                    used[candidate_idx] = true;
                    backend
                }
                None => {
                    return Err(EpError::request(format!(
                        "listener '{}' cannot be matched to frozen listener topology backend '{}' without changing its advertised slot layout",
                        listener.id(),
                        template_node.backend.node_id
                    )));
                }
            };

            next_nodes.push(VirtualClusterNode {
                listener_id: listener.id().to_string(),
                bind_port: listener.bind_port(),
                advertise_port: listener.advertise_port(),
                stable_node_id: template_node.stable_node_id.clone(),
                role: template_node.role,
                effective_slot_ranges: Self::effective_slot_ranges(&backend, ordered_nodes),
                backend,
            });
        }

        Ok(next_nodes)
    }

    fn build_from_listeners(
        listeners: &[ep_core::database::schema::interlay::InterlayListener],
        ordered_nodes: &[ClusterProxyNode],
    ) -> Result<Vec<VirtualClusterNode>, EpError> {
        if ordered_nodes.len() != listeners.len() {
            return Err(EpError::request(format!(
                "listener count ({}) does not match cluster node count ({})",
                listeners.len(),
                ordered_nodes.len()
            )));
        }

        Ok(listeners
            .iter()
            .cloned()
            .zip(ordered_nodes.iter().cloned())
            .map(|(listener, backend)| VirtualClusterNode {
                listener_id: listener.id().to_string(),
                bind_port: listener.bind_port(),
                advertise_port: listener.advertise_port(),
                stable_node_id: Self::stable_virtual_node_id(listener.id()),
                role: backend.role,
                effective_slot_ranges: Self::effective_slot_ranges(&backend, ordered_nodes),
                backend,
            })
            .collect())
    }
}

pub(super) struct ClusterBackendConnection;

impl ClusterBackendConnection {
    pub(super) fn node_connection(config: &RedisConfig, node: &ClusterProxyNode) -> RedisConnection {
        let target = RedisTarget {
            host: node.host.clone(),
            port: Some(node.port),
            db: config.target.db,
            tls: config.target.tls.clone(),
            insecure: config.target.insecure,
            protocol_version: config.target.protocol_version,
            connect_timeout_secs: config.target.connect_timeout_secs,
            max_retries: config.target.max_retries,
        };

        let default_credentials = RedisCredentials::default();
        let credentials = match node.role {
            ClusterProxyNodeRole::Master => config
                .write_credentials
                .as_ref()
                .or(config.read_credentials.as_ref())
                .or(config.system_credentials.as_ref())
                .or(config.admin_credentials.as_ref())
                .unwrap_or(&default_credentials),
            ClusterProxyNodeRole::Replica => config
                .read_credentials
                .as_ref()
                .or(config.write_credentials.as_ref())
                .or(config.system_credentials.as_ref())
                .or(config.admin_credentials.as_ref())
                .unwrap_or(&default_credentials),
        };

        RedisConnection::from_target_and_credentials(&target, credentials)
    }

    fn discovery_connection(config: &RedisConfig) -> RedisConnection {
        let target = config.target.clone();
        let default_credentials = RedisCredentials::default();
        let credentials = config
            .write_credentials
            .as_ref()
            .or(config.read_credentials.as_ref())
            .or(config.system_credentials.as_ref())
            .or(config.admin_credentials.as_ref())
            .unwrap_or(&default_credentials);
        RedisConnection::from_target_and_credentials(&target, credentials)
    }
}

pub(super) struct ClusterTopologyLoader;

impl ClusterTopologyLoader {
    pub(super) fn targets_for_topology(topology: &VirtualClusterTopology) -> Vec<ClusterDirectTarget> {
        topology
            .nodes
            .iter()
            .map(|node| ClusterDirectTarget {
                endpoint_uuid: topology.endpoint_uuid.clone(),
                listener_id: node.listener_id.clone(),
                backend_host: node.backend.host.clone(),
                backend_port: node.backend.port,
                connection: ClusterBackendConnection::node_connection(&topology.redis_config, &node.backend),
            })
            .collect()
    }

    pub(super) fn topology_for_endpoint<'a>(
        cluster_state: &'a ClusterConnectionState,
        endpoint_uuid: &EndpointCacheUuid,
    ) -> Option<&'a VirtualClusterTopology> {
        if cluster_state.old_topology.endpoint_uuid == *endpoint_uuid {
            Some(&cluster_state.old_topology)
        } else {
            cluster_state.new_topology.as_ref().filter(|topology| topology.endpoint_uuid == *endpoint_uuid)
        }
    }

    pub(super) async fn build_connection_state(
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_state: &InterlayState,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
        interlay_id: Option<&str>,
        ctx: Option<&LogContext>,
    ) -> Result<ClusterConnectionState, EpError> {
        let advertise_host = interlay_state
            .advertise_host()
            .ok_or_else(|| EpError::request("multi-listener Redis interlay requires advertise_host"))?;
        let routing = RoutingState::from_interlay_state(interlay_state, org)?;
        let (_redis_config, old_topology) = Self::load(
            database_manager,
            routing.resolver.primary(),
            interlay_state.listeners(),
            advertise_host,
            None,
            telemetry_wrapper,
            interlay_id,
            ctx,
        )
        .await?;
        old_topology
            .node_for_listener(listener_id)
            .ok_or_else(|| EpError::request(format!("listener '{listener_id}' is not configured for this interlay")))?;
        let new_topology: Option<VirtualClusterTopology> = None;

        Ok(ClusterConnectionState {
            state_version: interlay_state.version(),
            routing,
            current_listener_id: listener_id.to_string(),
            old_topology,
            new_topology,
            clients: HashMap::new(),
            asking_next: false,
            readonly_mode: false,
            watching: false,
            in_multi: false,
            pinned_route: None,
            pending_refresh: false,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn refresh_if_needed(
        cluster_state: &mut ClusterConnectionState,
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
        force_refresh: bool,
        ctx: Option<&LogContext>,
    ) -> Result<bool, EpError> {
        let Some(interlay_state) = interlay_endpoints.get(interlay_cache_uuid) else {
            return Err(EpError::request(format!(
                "interlay '{interlay_cache_uuid}' disappeared from cache during cluster topology refresh"
            )));
        };

        if !force_refresh && interlay_state.version() == cluster_state.state_version {
            return Ok(false);
        }

        if !ClusterSupport::supports_virtual_cluster_proxy(&interlay_state, listener_id) {
            return Err(EpError::request("interlay no longer supports Redis virtual-cluster direct routing"));
        }

        if cluster_state.watching || cluster_state.in_multi {
            cluster_state.pending_refresh = true;
            return Ok(false);
        }

        let advertise_host = interlay_state
            .advertise_host()
            .ok_or_else(|| EpError::request("multi-listener Redis interlay requires advertise_host"))?;
        let routing = RoutingState::from_interlay_state(&interlay_state, org)?;
        let interlay_id = interlay_cache_uuid.uuid().to_string();
        let (_redis_config, old_topology) = Self::load(
            database_manager,
            routing.resolver.primary(),
            interlay_state.listeners(),
            advertise_host,
            Some(&cluster_state.old_topology),
            telemetry_wrapper,
            Some(interlay_id.as_str()),
            ctx,
        )
        .await?;
        old_topology
            .node_for_listener(listener_id)
            .ok_or_else(|| EpError::request(format!("listener '{listener_id}' is not configured for this interlay")))?;
        let new_topology: Option<VirtualClusterTopology> = None;

        cluster_state.clients.retain(|key, connection| {
            let topology = if old_topology.endpoint_uuid == key.endpoint_uuid {
                Some(&old_topology)
            } else {
                new_topology.as_ref().filter(|topology| topology.endpoint_uuid == key.endpoint_uuid)
            };

            topology.and_then(|topology| topology.node_for_listener(&key.listener_id)).is_some_and(|node| {
                connection.backend_port == node.backend.port && connection.backend_host.eq_ignore_ascii_case(&node.backend.host)
            })
        });

        cluster_state.state_version = interlay_state.version();
        cluster_state.routing = routing;
        cluster_state.old_topology = old_topology;
        cluster_state.new_topology = new_topology;
        cluster_state.pending_refresh = false;

        Ok(true)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load(
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        endpoint_uuid: &EndpointCacheUuid,
        listeners: &[ep_core::database::schema::interlay::InterlayListener],
        advertise_host: &str,
        template: Option<&VirtualClusterTopology>,
        telemetry_wrapper: &mut TelemetryWrapper,
        interlay_id: Option<&str>,
        ctx: Option<&LogContext>,
    ) -> Result<(RedisConfig, VirtualClusterTopology), EpError> {
        let endpoint_schema = Self::fetch_endpoint_schema(database_manager, endpoint_uuid, telemetry_wrapper).await?;
        let redis_config = Self::extract_redis_config(&endpoint_schema)?;
        let discovery_connection = ClusterBackendConnection::discovery_connection(&redis_config);
        let org_uuid = endpoint_uuid.org().map(|org| org.eden_uuid::<eden_core::format::OrganizationUuid>().to_string());
        let mut discovery_client = RedisClient::connect_with_org_endpoint(
            &discovery_connection,
            org_uuid.as_deref().unwrap_or(eden_core::telemetry::labels::SYSTEM_ORG_UUID),
            Some(endpoint_uuid.uuid().to_string()),
        )
        .await?;
        let raw_response = discovery_client.send_command_raw_no_reconnect(CLUSTER_NODES_COMMAND.as_ref()).await?.to_bytes();
        let raw_nodes = ClusterResponseRewriter::decode_nodes_payload(&raw_response)?;
        let parsed_nodes = ClusterNodesParser::parse_with_warnings(&raw_nodes);
        ClusterNodesParseWarnings::record(&parsed_nodes.warnings, org_uuid.as_deref(), interlay_id, telemetry_wrapper, ctx);
        let ordered_nodes = ClusterTopologyBuilder::ordered_nodes(&parsed_nodes.nodes);
        let mut topology = ClusterTopologyBuilder::build(endpoint_uuid, listeners, advertise_host, &ordered_nodes, template)?;
        topology.redis_config = redis_config.clone();

        Ok((redis_config, topology))
    }

    async fn fetch_endpoint_schema(
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        endpoint_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<EndpointSchema, EpError> {
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointUuid,
            eden_core::format::cache_id::EndpointCacheId,
            EndpointId,
        >>::get_from_cache(
            database_manager.as_ref(),
            &CacheObjectType::new(Some(endpoint_uuid.clone()), None),
            telemetry_wrapper,
        )
        .await
    }

    fn extract_redis_config(endpoint_schema: &EndpointSchema) -> Result<RedisConfig, EpError> {
        endpoint_schema
            .config()
            .as_any()
            .downcast_ref::<RedisConfig>()
            .cloned()
            .ok_or_else(|| EpError::request("endpoint config is not Redis"))
    }
}

impl VirtualClusterTopology {
    pub fn endpoint_uuid(&self) -> &EndpointCacheUuid {
        &self.endpoint_uuid
    }

    pub fn nodes(&self) -> &[VirtualClusterNode] {
        &self.nodes
    }

    pub(super) fn node_for_listener(&self, listener_id: &str) -> Option<&VirtualClusterNode> {
        self.nodes.iter().find(|node| node.listener_id == listener_id)
    }

    pub(super) fn node_for_backend_address(&self, host: &str, port: u16) -> Option<&VirtualClusterNode> {
        self.nodes.iter().find(|node| node.backend.port == port && node.backend.host.eq_ignore_ascii_case(host))
    }

    pub(super) fn node_for_slot(&self, slot: u16) -> Option<&VirtualClusterNode> {
        self.nodes.iter().find(|node| node.effective_slot_ranges.iter().any(|(start, end)| (*start..=*end).contains(&slot)))
    }

    pub(super) fn master_node_for_slot(&self, slot: u16) -> Option<&VirtualClusterNode> {
        self.nodes.iter().find(|node| {
            node.role == ClusterProxyNodeRole::Master
                && node.effective_slot_ranges.iter().any(|(start, end)| slot >= *start && slot <= *end)
        })
    }
}

impl ClusterConnectionState {
    pub async fn new(
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_state: &InterlayState,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Self, EpError> {
        ClusterTopologyLoader::build_connection_state(database_manager, interlay_state, listener_id, org, telemetry_wrapper, None, None)
            .await
    }

    pub async fn new_with_context(
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_state: &InterlayState,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
        interlay_id: &str,
        ctx: &LogContext,
    ) -> Result<Self, EpError> {
        ClusterTopologyLoader::build_connection_state(
            database_manager,
            interlay_state,
            listener_id,
            org,
            telemetry_wrapper,
            Some(interlay_id),
            Some(ctx),
        )
        .await
    }

    pub fn has_active_pin(&self) -> bool {
        ClusterExecution::has_active_pin(self)
    }

    pub fn mark_pending_refresh(&mut self) {
        self.pending_refresh = true;
    }

    pub fn pending_refresh(&self) -> bool {
        self.pending_refresh
    }

    pub fn current_pinned_route(&self) -> Option<&ClusterPinnedRoute> {
        self.pinned_route.as_ref()
    }

    pub fn direct_targets(&self) -> Vec<ClusterDirectTarget> {
        let mut targets = ClusterTopologyLoader::targets_for_topology(&self.old_topology);
        if let Some(topology) = &self.new_topology {
            targets.extend(ClusterTopologyLoader::targets_for_topology(topology));
        }
        targets
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn refresh_if_needed(
        &mut self,
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
        force_refresh: bool,
    ) -> Result<bool, EpError> {
        ClusterTopologyLoader::refresh_if_needed(
            self,
            database_manager,
            interlay_cache_uuid,
            interlay_endpoints,
            listener_id,
            org,
            telemetry_wrapper,
            force_refresh,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn refresh_if_needed_with_context(
        &mut self,
        database_manager: &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        interlay_cache_uuid: &InterlayCacheUuid,
        interlay_endpoints: &Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        listener_id: &str,
        org: Option<&eden_core::format::OrganizationCacheUuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
        force_refresh: bool,
        ctx: &LogContext,
    ) -> Result<bool, EpError> {
        ClusterTopologyLoader::refresh_if_needed(
            self,
            database_manager,
            interlay_cache_uuid,
            interlay_endpoints,
            listener_id,
            org,
            telemetry_wrapper,
            force_refresh,
            Some(ctx),
        )
        .await
    }
}
