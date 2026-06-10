//! Redis Cluster command routing, session state, and dispatch.

use super::response::ClusterResponseRewriter;
use super::topology::{ClusterBackendConnection, ClusterTopologyLoader};
use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ClusterSessionTransition {
    None,
    SetWatch,
    ClearWatch,
    SetMulti,
    ClearTransaction,
}

pub struct ClusterExecution;
impl ClusterExecution {
    pub(super) fn slot_for_key(key: &[u8]) -> u16 {
        (REDIS_CLUSTER_HASH_CONFIG.hash_key(key) % 16_384) as u16
    }

    pub(super) fn command_slot(parsed: &RedisCommandArgs) -> Result<Option<u16>, Bytes> {
        let Ok(keys) = parsed.command().keys_from_args(parsed.args()) else {
            return Ok(None);
        };

        let mut slot = None;
        for key in keys {
            let key_slot = Self::slot_for_key(key.as_bytes());
            if let Some(existing_slot) = slot {
                if existing_slot != key_slot {
                    return Err(CROSSSLOT_RESPONSE.clone());
                }
            } else {
                slot = Some(key_slot);
            }
        }

        Ok(slot)
    }

    pub(super) fn has_active_pin(cluster_state: &ClusterConnectionState) -> bool {
        cluster_state.watching || cluster_state.in_multi || cluster_state.pinned_route.is_some()
    }

    pub(super) fn is_read_command(command: &RedisApi) -> bool {
        !matches!(command, RedisApi::Watch) && command.request_type().is_read()
    }

    pub(super) fn pinned_topology(cluster_state: &ClusterConnectionState) -> Option<&VirtualClusterTopology> {
        let route = cluster_state.pinned_route.as_ref()?;
        ClusterTopologyLoader::topology_for_endpoint(cluster_state, &route.endpoint_uuid)
    }

    pub(super) fn select_listener_for_topology(
        topology: &VirtualClusterTopology,
        cluster_state: &ClusterConnectionState,
        slot: Option<u16>,
        is_read: bool,
    ) -> Result<String, EpError> {
        if let Some(route) = cluster_state.pinned_route.as_ref() {
            if route.endpoint_uuid != topology.endpoint_uuid {
                return Err(EpError::request("cluster connection is pinned to a different migration endpoint"));
            }
            return Ok(route.listener_id.clone());
        }

        if cluster_state.asking_next {
            return Ok(cluster_state.current_listener_id.clone());
        }

        if cluster_state.readonly_mode
            && is_read
            && let Some(current_node) = topology.node_for_listener(&cluster_state.current_listener_id)
            && current_node.role == ClusterProxyNodeRole::Replica
            && slot.is_none_or(|slot| current_node.effective_slot_ranges.iter().any(|(start, end)| (*start..=*end).contains(&slot)))
        {
            return Ok(cluster_state.current_listener_id.clone());
        }

        if let Some(slot) = slot
            && let Some(node) = topology.master_node_for_slot(slot)
        {
            return Ok(node.listener_id.clone());
        }

        Ok(cluster_state.current_listener_id.clone())
    }

    pub(super) fn should_use_readonly_backend(
        topology: &VirtualClusterTopology,
        cluster_state: &ClusterConnectionState,
        listener_id: &str,
        is_read: bool,
    ) -> bool {
        cluster_state.readonly_mode
            && is_read
            && topology.node_for_listener(listener_id).is_some_and(|node| node.role == ClusterProxyNodeRole::Replica)
    }

    pub(super) fn response_first_line(response: &[u8]) -> Option<&str> {
        std::str::from_utf8(response).ok()?.lines().next()
    }

    pub(super) fn response_is_exec_abort(response: &[u8]) -> bool {
        Self::response_first_line(response).is_some_and(|line| line.contains("EXECABORT"))
    }

    pub(super) fn session_transition(command: &RedisApi, response: &[u8], in_multi: bool) -> ClusterSessionTransition {
        let succeeded = !RedisWire::response_contains_redis_error(response);

        match command {
            RedisApi::Watch if succeeded => ClusterSessionTransition::SetWatch,
            RedisApi::Unwatch if succeeded => ClusterSessionTransition::ClearWatch,
            RedisApi::Multi if succeeded => ClusterSessionTransition::SetMulti,
            RedisApi::Exec if succeeded || (in_multi && Self::response_is_exec_abort(response)) => {
                ClusterSessionTransition::ClearTransaction
            }
            RedisApi::Discard if succeeded => ClusterSessionTransition::ClearTransaction,
            _ => ClusterSessionTransition::None,
        }
    }

    pub(super) fn apply_session_state(
        command: &RedisApi,
        response: &[u8],
        cluster_state: &mut ClusterConnectionState,
        dispatch: &ClusterDispatchResult,
    ) {
        match Self::session_transition(command, response, cluster_state.in_multi) {
            ClusterSessionTransition::SetWatch => {
                cluster_state.pinned_route = Some(ClusterPinnedRoute {
                    endpoint_uuid: dispatch.topology.endpoint_uuid.clone(),
                    listener_id: dispatch.listener_id.clone(),
                });
                cluster_state.watching = true;
            }
            ClusterSessionTransition::ClearWatch => {
                cluster_state.watching = false;
                if !cluster_state.in_multi {
                    cluster_state.pinned_route = None;
                }
            }
            ClusterSessionTransition::SetMulti => {
                cluster_state.pinned_route = Some(ClusterPinnedRoute {
                    endpoint_uuid: dispatch.topology.endpoint_uuid.clone(),
                    listener_id: dispatch.listener_id.clone(),
                });
                cluster_state.in_multi = true;
            }
            ClusterSessionTransition::ClearTransaction => {
                cluster_state.in_multi = false;
                cluster_state.watching = false;
                cluster_state.pinned_route = None;
            }
            ClusterSessionTransition::None => {}
        }
    }

    pub(super) async fn send_internal_command(connection: &mut ClusterClientConnection, command: Bytes) -> Result<(), EpError> {
        let response = endpoints::endpoint::ep_redis::protocol::RedisBytes::from(command)
            .send_raw_bytes_on_conn_no_reconnect(&mut connection.client)
            .await?;

        if RedisWire::response_contains_redis_error(&response) {
            return Err(EpError::request(String::from_utf8_lossy(&response).trim().to_string()));
        }

        Ok(())
    }

    pub(super) async fn send_command_to_listener(
        cluster_state: &mut ClusterConnectionState,
        endpoint_uuid: &EndpointCacheUuid,
        listener_id: &str,
        command_bytes: Bytes,
        apply_asking: bool,
        use_readonly_backend: bool,
    ) -> Result<Bytes, EpError> {
        let topology = ClusterTopologyLoader::topology_for_endpoint(cluster_state, endpoint_uuid)
            .cloned()
            .ok_or_else(|| EpError::request(format!("missing cluster topology for endpoint {}", endpoint_uuid)))?;
        let node = topology
            .node_for_listener(listener_id)
            .cloned()
            .ok_or_else(|| EpError::request(format!("listener '{listener_id}' is not configured for endpoint {}", endpoint_uuid)))?;
        let key = ClusterClientKey {
            endpoint_uuid: endpoint_uuid.clone(),
            listener_id: listener_id.to_string(),
        };
        let needs_reconnect = cluster_state.clients.get(&key).is_none_or(|connection| {
            connection.backend_port != node.backend.port || !connection.backend_host.eq_ignore_ascii_case(&node.backend.host)
        });

        if needs_reconnect {
            let connection = ClusterBackendConnection::node_connection(&topology.redis_config, &node.backend);
            let org_uuid = endpoint_uuid
                .org()
                .map(|org| org.eden_uuid::<eden_core::format::OrganizationUuid>().to_string())
                .unwrap_or_else(|| eden_core::telemetry::labels::SYSTEM_ORG_UUID.to_string());
            let client = RedisClient::connect_with_org_endpoint(&connection, org_uuid, Some(endpoint_uuid.uuid().to_string())).await?;
            cluster_state.clients.insert(
                key.clone(),
                ClusterClientConnection {
                    backend_host: node.backend.host.clone(),
                    backend_port: node.backend.port,
                    readonly_mode: false,
                    client,
                },
            );
        }

        let Some(connection) = cluster_state.clients.get_mut(&key) else {
            return Err(EpError::request("cluster backend connection disappeared"));
        };

        if node.role == ClusterProxyNodeRole::Replica {
            if use_readonly_backend && !connection.readonly_mode {
                Self::send_internal_command(connection, READONLY_COMMAND.clone()).await?;
                connection.readonly_mode = true;
            } else if !use_readonly_backend && connection.readonly_mode {
                Self::send_internal_command(connection, READWRITE_COMMAND.clone()).await?;
                connection.readonly_mode = false;
            }
        } else if connection.readonly_mode {
            connection.readonly_mode = false;
        }

        if apply_asking {
            Self::send_internal_command(connection, ASKING_COMMAND.clone()).await?;
        }

        endpoints::endpoint::ep_redis::protocol::RedisBytes::from(command_bytes)
            .send_raw_bytes_on_conn_no_reconnect(&mut connection.client)
            .await
    }

    pub(super) fn command_requires_pinned_transport(cluster_state: &ClusterConnectionState, command: &RedisApi) -> bool {
        cluster_state.pinned_route.is_some() || matches!(command, RedisApi::Watch | RedisApi::Multi)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn send_command_to_listener_direct<T>(
        cluster_state: &ClusterConnectionState,
        transport: &mut T,
        endpoint_uuid: &EndpointCacheUuid,
        listener_id: &str,
        command: &RedisApi,
        command_bytes: Bytes,
        apply_asking: bool,
        use_readonly_backend: bool,
    ) -> Result<Bytes, EpError>
    where
        T: ClusterDirectTransport,
    {
        let topology = ClusterTopologyLoader::topology_for_endpoint(cluster_state, endpoint_uuid)
            .ok_or_else(|| EpError::request(format!("missing cluster topology for endpoint {}", endpoint_uuid)))?;
        let node = topology
            .node_for_listener(listener_id)
            .ok_or_else(|| EpError::request(format!("listener '{listener_id}' is not configured for endpoint {}", endpoint_uuid)))?;

        let mut internal_prefixes = Vec::with_capacity(2);
        if node.role == ClusterProxyNodeRole::Replica {
            if use_readonly_backend {
                internal_prefixes.push(READONLY_COMMAND.clone());
            } else {
                internal_prefixes.push(READWRITE_COMMAND.clone());
            }
        }
        if apply_asking {
            internal_prefixes.push(ASKING_COMMAND.clone());
        }

        let expected_responses = 1 + internal_prefixes.len();
        let target = ClusterDirectTarget {
            endpoint_uuid: endpoint_uuid.clone(),
            listener_id: listener_id.to_string(),
            backend_host: node.backend.host.clone(),
            backend_port: node.backend.port,
            connection: ClusterBackendConnection::node_connection(&topology.redis_config, &node.backend),
        };

        transport
            .dispatch(ClusterDirectDispatch {
                target,
                command_bytes,
                expected_responses,
                internal_prefixes,
                pinned: Self::command_requires_pinned_transport(cluster_state, command),
            })
            .await
    }

    pub(super) async fn execute_on_topology_direct<T>(
        cluster_state: &mut ClusterConnectionState,
        transport: &mut T,
        topology: &VirtualClusterTopology,
        parsed: &RedisCommandArgs,
        command_bytes: Bytes,
        slot: Option<u16>,
        apply_asking: bool,
    ) -> Result<ClusterDispatchResult, EpError>
    where
        T: ClusterDirectTransport,
    {
        let is_read = Self::is_read_command(parsed.command());
        let listener_id = Self::select_listener_for_topology(topology, cluster_state, slot, is_read)?;
        let use_readonly_backend = Self::should_use_readonly_backend(topology, cluster_state, &listener_id, is_read);
        let response = Self::send_command_to_listener_direct(
            cluster_state,
            transport,
            &topology.endpoint_uuid,
            &listener_id,
            parsed.command(),
            command_bytes,
            apply_asking,
            use_readonly_backend,
        )
        .await?;
        Ok(ClusterDispatchResult { response, topology: topology.clone(), listener_id })
    }

    pub(super) async fn execute_on_topology(
        cluster_state: &mut ClusterConnectionState,
        topology: &VirtualClusterTopology,
        parsed: &RedisCommandArgs,
        command_bytes: Bytes,
        slot: Option<u16>,
        apply_asking: bool,
    ) -> Result<ClusterDispatchResult, EpError> {
        let is_read = Self::is_read_command(parsed.command());
        let listener_id = Self::select_listener_for_topology(topology, cluster_state, slot, is_read)?;
        let use_readonly_backend = Self::should_use_readonly_backend(topology, cluster_state, &listener_id, is_read);
        let response = Self::send_command_to_listener(
            cluster_state,
            &topology.endpoint_uuid,
            &listener_id,
            command_bytes,
            apply_asking,
            use_readonly_backend,
        )
        .await?;
        Ok(ClusterDispatchResult { response, topology: topology.clone(), listener_id })
    }

    pub(super) fn local_session_response(command: &RedisApi, cluster_state: &mut ClusterConnectionState) -> Option<Bytes> {
        match command {
            RedisApi::Asking => {
                cluster_state.asking_next = true;
                Some(RESP_OK.clone())
            }
            RedisApi::Readonly => {
                cluster_state.readonly_mode = true;
                Some(RESP_OK.clone())
            }
            RedisApi::Readwrite => {
                cluster_state.readonly_mode = false;
                Some(RESP_OK.clone())
            }
            RedisApi::Reset => {
                Self::reset_local_session_state(cluster_state);
                Some(RESP_RESET.clone())
            }
            _ => None,
        }
    }

    pub(super) fn reset_local_session_state(cluster_state: &mut ClusterConnectionState) {
        cluster_state.clients.clear();
        cluster_state.asking_next = false;
        cluster_state.readonly_mode = false;
        cluster_state.watching = false;
        cluster_state.in_multi = false;
        cluster_state.pinned_route = None;
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_dispatched_command(
        cluster_state: &mut ClusterConnectionState,
        parsed: &RedisCommandArgs,
        command_bytes: Bytes,
        slot: Option<u16>,
        apply_asking: bool,
    ) -> Result<ClusterDispatchResult, EpError> {
        if cluster_state.pinned_route.is_some() {
            let pinned_topology = Self::pinned_topology(cluster_state)
                .cloned()
                .ok_or_else(|| EpError::request("cluster connection is pinned to a migration endpoint that is no longer available"))?;
            return Self::execute_on_topology(cluster_state, &pinned_topology, parsed, command_bytes, slot, apply_asking).await;
        }

        Self::execute_on_topology(cluster_state, &cluster_state.old_topology.clone(), parsed, command_bytes, slot, apply_asking).await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn execute_dispatched_command_direct<T>(
        cluster_state: &mut ClusterConnectionState,
        transport: &mut T,
        parsed: &RedisCommandArgs,
        command_bytes: Bytes,
        slot: Option<u16>,
        apply_asking: bool,
    ) -> Result<ClusterDispatchResult, EpError>
    where
        T: ClusterDirectTransport,
    {
        if cluster_state.pinned_route.is_some() {
            let pinned_topology = Self::pinned_topology(cluster_state)
                .cloned()
                .ok_or_else(|| EpError::request("cluster connection is pinned to a migration endpoint that is no longer available"))?;
            return Self::execute_on_topology_direct(cluster_state, transport, &pinned_topology, parsed, command_bytes, slot, apply_asking)
                .await;
        }

        Self::execute_on_topology_direct(
            cluster_state,
            transport,
            &cluster_state.old_topology.clone(),
            parsed,
            command_bytes,
            slot,
            apply_asking,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn execute_direct_command<T>(
        cluster_state: &mut ClusterConnectionState,
        transport: &mut T,
        parsed: &RedisCommandArgs,
        command_bytes: Bytes,
    ) -> Result<ClusterDirectCommandResult, EpError>
    where
        T: ClusterDirectTransport,
    {
        if let Some(local_response) = Self::local_session_response(parsed.command(), cluster_state) {
            transport.retain_pinned_route(cluster_state.current_pinned_route());
            return Ok(ClusterDirectCommandResult::response(local_response, false, false, None));
        }

        if let Some(blocked_response) = RedisWire::session_state_rejection(parsed.command()) {
            return Ok(ClusterDirectCommandResult::response(blocked_response, false, true, None));
        }

        let slot = match Self::command_slot(parsed) {
            Ok(slot) => slot,
            Err(response) => return Ok(ClusterDirectCommandResult::response(response, false, false, None)),
        };

        let apply_asking = cluster_state.asking_next;
        let dispatch = match Self::execute_dispatched_command_direct(
            cluster_state,
            transport,
            parsed,
            command_bytes.clone(),
            slot,
            apply_asking,
        )
        .await
        {
            Ok(dispatch) => dispatch,
            Err(err) => {
                transport.retain_pinned_route(cluster_state.current_pinned_route());
                return Err(err);
            }
        };
        cluster_state.asking_next = false;

        let raw_response = dispatch.response.clone();
        Self::apply_session_state(parsed.command(), &raw_response, cluster_state, &dispatch);
        transport.retain_pinned_route(cluster_state.current_pinned_route());

        let needs_topology_refresh = matches!(parsed.command(), RedisApi::ClusterNodes | RedisApi::ClusterSlots | RedisApi::ClusterShards)
            || ClusterResponseRewriter::redirect_line(&raw_response).is_some();
        let rewrite_topology = needs_topology_refresh.then_some(dispatch.topology);

        Ok(ClusterDirectCommandResult::response(raw_response, needs_topology_refresh, false, rewrite_topology))
    }
}
