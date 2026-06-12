use super::*;
use ep_core::database::schema::interlay::InterlayListener;
use ep_core::database::schema::routing::{EndpointRouting, ReplicaStrategy};

struct PanicDirectTransport;

impl ClusterDirectTransport for PanicDirectTransport {
    fn dispatch<'a>(&'a mut self, _request: ClusterDirectDispatch) -> Pin<Box<dyn Future<Output = Result<Bytes, EpError>> + Send + 'a>> {
        panic!("unsupported command should not dispatch");
    }

    fn retain_pinned_route(&mut self, _route: Option<&ClusterPinnedRoute>) {}
}

#[derive(Default)]
struct RetainingDirectTransport {
    retained_routes: Vec<Option<ClusterPinnedRoute>>,
}

impl ClusterDirectTransport for RetainingDirectTransport {
    fn dispatch<'a>(&'a mut self, _request: ClusterDirectDispatch) -> Pin<Box<dyn Future<Output = Result<Bytes, EpError>> + Send + 'a>> {
        panic!("local command should not dispatch");
    }

    fn retain_pinned_route(&mut self, route: Option<&ClusterPinnedRoute>) {
        self.retained_routes.push(route.cloned());
    }
}

fn sample_topology() -> VirtualClusterTopology {
    let endpoint_uuid = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
    VirtualClusterTopology {
        endpoint_uuid,
        redis_config: RedisConfig::default(),
        advertise_host: "proxy.example.com".to_string(),
        nodes: vec![
            VirtualClusterNode {
                listener_id: "n1".to_string(),
                bind_port: 7000,
                advertise_port: 17000,
                stable_node_id: "eden-n1".to_string(),
                role: ClusterProxyNodeRole::Master,
                effective_slot_ranges: vec![(0, 8191)],
                backend: ClusterProxyNode {
                    node_id: "node-1".to_string(),
                    host: "10.0.0.1".to_string(),
                    port: 6379,
                    bus_port: Some(16379),
                    role: ClusterProxyNodeRole::Master,
                    master_id: None,
                    flags: vec!["master".to_string()],
                    slot_ranges: vec![(0, 8191)],
                    connected: true,
                },
            },
            VirtualClusterNode {
                listener_id: "n2".to_string(),
                bind_port: 7001,
                advertise_port: 17001,
                stable_node_id: "eden-n2".to_string(),
                role: ClusterProxyNodeRole::Master,
                effective_slot_ranges: vec![(8192, 16383)],
                backend: ClusterProxyNode {
                    node_id: "node-2".to_string(),
                    host: "10.0.0.2".to_string(),
                    port: 6380,
                    bus_port: Some(16380),
                    role: ClusterProxyNodeRole::Master,
                    master_id: None,
                    flags: vec!["master".to_string()],
                    slot_ranges: vec![(8192, 16383)],
                    connected: true,
                },
            },
        ],
    }
}

fn sample_cluster_state(topology: VirtualClusterTopology) -> ClusterConnectionState {
    let endpoint_uuid = topology.endpoint_uuid.clone();
    let state = InterlayState::new(
        endpoint_uuid.clone(),
        EpKind::Redis,
        EndpointRouting::direct(endpoint_uuid.eden_uuid()),
        None,
        None,
        Default::default(),
    );
    ClusterConnectionState {
        state_version: state.version(),
        routing: RoutingState::from_interlay_state(&state, endpoint_uuid.org().as_ref()).expect("routing state"),
        current_listener_id: "n1".to_string(),
        old_topology: topology,
        new_topology: None,
        clients: HashMap::new(),
        asking_next: false,
        readonly_mode: false,
        watching: false,
        in_multi: false,
        pinned_route: None,
        pending_refresh: false,
    }
}

fn sample_cluster_interlay_state() -> InterlayState {
    let endpoint_uuid = EndpointUuid::new_uuid();
    let endpoint_cache_uuid = EndpointCacheUuid::new(None, endpoint_uuid.clone());
    let mut state = InterlayState::new(
        endpoint_cache_uuid,
        EpKind::Redis,
        EndpointRouting::direct(endpoint_uuid),
        None,
        None,
        Default::default(),
    );
    state.update_listener_config(
        vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)],
        Some("proxy.example.com".to_string()),
    );
    state
}

#[test]
fn supports_virtual_cluster_proxy_requires_direct_eligible_state() {
    let state = sample_cluster_interlay_state();
    assert!(ClusterSupport::supports_virtual_cluster_proxy(&state, "n1"));
    assert!(!ClusterSupport::supports_virtual_cluster_proxy(&state, "missing-listener"));

    let mut with_policy = state.clone();
    with_policy.update_command_policy(Some(ep_core::serde_json::json!({ "mode": "block" })));
    assert!(!ClusterSupport::supports_virtual_cluster_proxy(&with_policy, "n1"));

    let mut with_audit = state.clone();
    with_audit.update_audit_config(Some(ep_core::serde_json::json!({ "enabled": true })));
    assert!(!ClusterSupport::supports_virtual_cluster_proxy(&with_audit, "n1"));

    let mut non_direct = state.clone();
    non_direct.update_routing(EndpointRouting::ReadReplica {
        primary: non_direct.endpoint_uuid().eden_uuid(),
        replicas: vec![EndpointUuid::new_uuid()],
        strategy: ReplicaStrategy::RoundRobin,
    });
    assert!(!ClusterSupport::supports_virtual_cluster_proxy(&non_direct, "n1"));
}

#[test]
fn parse_cluster_proxy_nodes_keeps_replicas_and_slots() {
    let raw = "\
master-a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n\
replica-a 10.0.0.11:6379@16379 slave master-a 0 0 2 connected\n\
master-b 10.0.0.2:6380@16380 master - 0 0 3 connected 8192-16383\n";

    let nodes = ClusterNodesParser::parse(raw);
    assert_eq!(nodes.len(), 3);
    assert_eq!(nodes[0].slot_ranges, vec![(0, 8191)]);
    assert_eq!(nodes[1].role, ClusterProxyNodeRole::Replica);
    assert_eq!(nodes[1].master_id.as_deref(), Some("master-a"));
}

#[test]
fn parse_cluster_proxy_nodes_skips_malformed_ports() {
    let raw = "\
master-bad 10.0.0.1:not-a-port@16379 master - 0 0 1 connected 0-8191\n\
master-good 10.0.0.2:6380@16380 master - 0 0 2 connected 8192-16383\n";

    let parsed = ClusterNodesParser::parse_with_warnings(raw);

    assert_eq!(parsed.nodes.len(), 1);
    assert_eq!(parsed.nodes[0].node_id, "master-good");
    assert_eq!(
        parsed.warnings,
        vec![ClusterNodesParseWarning {
            node_id: "master-bad".to_string(),
            token: "not-a-port".to_string(),
            kind: ClusterNodesParseWarningKind::InvalidPort,
        }]
    );
}

#[test]
fn parse_cluster_proxy_nodes_reports_malformed_slot_tokens() {
    let raw = "\
master-a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-20000 9000-8000 bogus 17000\n";

    let parsed = ClusterNodesParser::parse_with_warnings(raw);

    assert_eq!(parsed.nodes.len(), 1);
    assert_eq!(parsed.nodes[0].slot_ranges, vec![(0, 16383)]);
    assert_eq!(
        parsed.warnings,
        vec![
            ClusterNodesParseWarning {
                node_id: "master-a".to_string(),
                token: "0-20000".to_string(),
                kind: ClusterNodesParseWarningKind::ClampedSlotRange,
            },
            ClusterNodesParseWarning {
                node_id: "master-a".to_string(),
                token: "9000-8000".to_string(),
                kind: ClusterNodesParseWarningKind::ReversedSlotRange,
            },
            ClusterNodesParseWarning {
                node_id: "master-a".to_string(),
                token: "bogus".to_string(),
                kind: ClusterNodesParseWarningKind::InvalidSlotToken,
            },
            ClusterNodesParseWarning {
                node_id: "master-a".to_string(),
                token: "17000".to_string(),
                kind: ClusterNodesParseWarningKind::OutOfRangeSlot,
            },
        ]
    );
}

#[test]
fn parse_cluster_proxy_nodes_ignores_failed_placeholders() {
    let raw = "\
master-a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n\
master-old 10.0.0.2:6380@16380 master,fail - 0 0 2 disconnected 8192-16383\n\
replica-old :0@0 slave,noaddr master-a 0 0 2 disconnected\n\
replica-a 10.0.0.11:6379@16379 replica master-a 0 0 2 connected\n";

    let nodes = ClusterNodesParser::parse(raw);
    assert_eq!(nodes.len(), 2);
    assert!(nodes.iter().all(|node| node.flags.iter().all(|flag| flag != "fail" && flag != "noaddr")));
}

#[test]
fn rewrite_cluster_redirect_uses_virtual_address() {
    let topology = sample_topology();
    let rewritten = ClusterResponseRewriter::redirect(b"-MOVED 12000 10.0.0.2:6380\r\n", &topology);

    assert_eq!(
        rewritten,
        ClusterRedirectRewrite::Rewritten(Bytes::from_static(b"-MOVED 12000 proxy.example.com:17001\r\n"))
    );
}

#[test]
fn rewrite_cluster_redirect_preserves_ask_kind() {
    let topology = sample_topology();
    let rewritten = ClusterResponseRewriter::redirect(b"-ASK 12000 10.0.0.2:6380\r\n", &topology);

    assert_eq!(
        rewritten,
        ClusterRedirectRewrite::Rewritten(Bytes::from_static(b"-ASK 12000 proxy.example.com:17001\r\n"))
    );
}

#[test]
fn rewrite_cluster_redirect_falls_back_to_slot_owner_when_backend_address_moved() {
    let topology = sample_topology();
    let rewritten = ClusterResponseRewriter::redirect(b"-MOVED 12000 10.0.99.99:6399\r\n", &topology);

    assert_eq!(
        rewritten,
        ClusterRedirectRewrite::Rewritten(Bytes::from_static(b"-MOVED 12000 proxy.example.com:17001\r\n"))
    );
}

#[test]
fn rewrite_cluster_redirect_masks_malformed_redirects() {
    let topology = sample_topology();

    assert_eq!(
        ClusterResponseRewriter::redirect_response(Bytes::from_static(b"-MOVED 12000 missing-port\r\n"), &topology).as_ref(),
        b"-TRYAGAIN Redis cluster topology is refreshing\r\n"
    );
}

#[test]
fn direct_cluster_result_rewrites_against_refreshed_topology() {
    let stale_topology = sample_topology();
    let mut refreshed_topology = stale_topology.clone();
    refreshed_topology.nodes[1].advertise_port = 17002;
    let state = sample_cluster_state(refreshed_topology);
    let result =
        ClusterDirectCommandResult::response(Bytes::from_static(b"-MOVED 12000 10.0.0.2:6380\r\n"), true, false, Some(stale_topology));

    let response = result.into_response(&state, &RedisApi::Get).expect("redirect rewrite");

    assert_eq!(response.as_ref(), b"-MOVED 12000 proxy.example.com:17002\r\n");
}

#[test]
fn direct_cluster_result_masks_unmappable_redirect() {
    let mut topology = sample_topology();
    for node in &mut topology.nodes {
        node.effective_slot_ranges.clear();
    }
    let state = sample_cluster_state(topology.clone());
    let result = ClusterDirectCommandResult::response(Bytes::from_static(b"-MOVED 12000 10.0.99.1:6380\r\n"), true, false, Some(topology));

    let response = result.into_response(&state, &RedisApi::Get).expect("redirect response");

    assert_eq!(response.as_ref(), b"-TRYAGAIN Redis cluster topology is refreshing\r\n");
}

#[tokio::test]
async fn direct_cluster_unsupported_session_command_requests_close() {
    let mut state = sample_cluster_state(sample_topology());
    let mut transport = PanicDirectTransport;
    let parsed = RedisCommandArgs::new(RedisApi::Auth, Vec::new());

    let result = ClusterExecution::execute_direct_command(&mut state, &mut transport, &parsed, Bytes::from_static(b"*1\r\n$4\r\nAUTH\r\n"))
        .await
        .expect("unsupported response");

    assert!(result.close_after_response);
    assert!(result.into_response(&state, parsed.command()).expect("response").starts_with(b"-ERR"));
}

#[test]
fn is_read_command_treats_watch_as_write_routed() {
    assert!(!ClusterExecution::is_read_command(&RedisApi::Watch));
    assert!(ClusterExecution::is_read_command(&RedisApi::Get));
}

#[test]
fn session_transition_requires_successful_watch_and_multi() {
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Watch, b"+OK\r\n", false),
        ClusterSessionTransition::SetWatch
    );
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Watch, b"-MOVED 12 10.0.0.2:6380\r\n", false),
        ClusterSessionTransition::None
    );
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Multi, b"+OK\r\n", false),
        ClusterSessionTransition::SetMulti
    );
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Multi, b"-ERR MULTI calls can not be nested\r\n", true),
        ClusterSessionTransition::None
    );
}

#[test]
fn session_transition_clears_exec_abort_transactions() {
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Exec, b"-EXECABORT Transaction discarded because of previous errors.\r\n", true),
        ClusterSessionTransition::ClearTransaction
    );
    assert_eq!(
        ClusterExecution::session_transition(&RedisApi::Exec, b"-ERR EXEC without MULTI\r\n", false),
        ClusterSessionTransition::None
    );
}

#[test]
fn cluster_local_reset_clears_session_state() {
    let mut state = sample_cluster_state(sample_topology());
    state.asking_next = true;
    state.readonly_mode = true;
    state.watching = true;
    state.in_multi = true;
    state.pending_refresh = true;
    state.pinned_route = Some(ClusterPinnedRoute {
        endpoint_uuid: state.old_topology.endpoint_uuid.clone(),
        listener_id: "n1".to_string(),
    });

    let response = ClusterExecution::local_session_response(&RedisApi::Reset, &mut state).expect("RESET should be handled locally");

    assert_eq!(response.as_ref(), b"+RESET\r\n");
    assert!(!state.asking_next);
    assert!(!state.readonly_mode);
    assert!(!state.watching);
    assert!(!state.in_multi);
    assert!(state.pinned_route.is_none());
    assert!(state.pending_refresh);
}

#[tokio::test]
async fn direct_cluster_reset_releases_pinned_transport() {
    let mut state = sample_cluster_state(sample_topology());
    state.watching = true;
    state.pinned_route = Some(ClusterPinnedRoute {
        endpoint_uuid: state.old_topology.endpoint_uuid.clone(),
        listener_id: "n1".to_string(),
    });
    let mut transport = RetainingDirectTransport::default();
    let parsed = RedisCommandArgs::new(RedisApi::Reset, Vec::new());

    let result =
        ClusterExecution::execute_direct_command(&mut state, &mut transport, &parsed, Bytes::from_static(b"*1\r\n$5\r\nRESET\r\n"))
            .await
            .expect("local RESET response");

    assert_eq!(result.into_response(&state, parsed.command()).expect("response").as_ref(), b"+RESET\r\n");
    assert_eq!(transport.retained_routes, vec![None]);
    assert!(state.pinned_route.is_none());
}

#[test]
fn rewrite_cluster_nodes_payload_replaces_backend_addresses() {
    let topology = sample_topology();
    let raw = "\
node-1 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-8191\n\
node-2 10.0.0.2:6380@16380 master - 0 0 2 connected 8192-16383\n";

    let rewritten = ClusterResponseRewriter::nodes_payload(raw, &topology);
    assert!(rewritten.contains("proxy.example.com:17000@16379"));
    assert!(rewritten.contains("proxy.example.com:17001@16380"));
    assert!(!rewritten.contains("10.0.0.1:6379"));
    assert!(rewritten.contains("eden-n1"));
    assert!(rewritten.contains("eden-n2"));
}

#[test]
fn rewrite_cluster_nodes_payload_replaces_moving_slot_node_ids() {
    let topology = sample_topology();
    let raw = "\
node-1 10.0.0.1:6379@16379 myself,master - 0 0 1 connected 0-4095 [4096->-node-2] [4097-<-node-2]\n";

    let rewritten = ClusterResponseRewriter::nodes_payload(raw, &topology);

    assert!(rewritten.contains("[4096->-eden-n2]"));
    assert!(rewritten.contains("[4097-<-eden-n2]"));
    assert!(!rewritten.contains("[4096->-node-2]"));
    assert!(!rewritten.contains("[4097-<-node-2]"));
}

#[test]
fn rewrite_cluster_slots_node_replaces_node_id() {
    let topology = sample_topology();
    let mut node = vec![
        Resp2Frame::BulkString(b"10.0.0.1".to_vec()),
        Resp2Frame::Integer(6379),
        Resp2Frame::BulkString(b"node-1".to_vec()),
    ];

    ClusterResponseRewriter::rewrite_slots_node_resp2(&mut node, &topology);

    assert_eq!(node[0], Resp2Frame::BulkString(b"proxy.example.com".to_vec()));
    assert_eq!(node[1], Resp2Frame::Integer(17000));
    assert_eq!(node[2], Resp2Frame::BulkString(b"eden-n1".to_vec()));
}

#[test]
fn rewrite_cluster_slots_response_leaves_malformed_node_ports_unrewritten() {
    let topology = sample_topology();
    let response = Resp2Frame::Array(vec![Resp2Frame::Array(vec![
        Resp2Frame::Integer(0),
        Resp2Frame::Integer(8191),
        Resp2Frame::Array(vec![
            Resp2Frame::BulkString(b"10.0.0.1".to_vec()),
            Resp2Frame::BulkString(b"not-a-port".to_vec()),
            Resp2Frame::BulkString(b"node-1".to_vec()),
        ]),
    ])]);
    let raw = RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(response)).expect("encode malformed CLUSTER SLOTS response");

    let rewritten = ClusterResponseRewriter::slots_response(&raw, &topology).expect("rewrite malformed-port CLUSTER SLOTS response");
    let rewritten = String::from_utf8_lossy(&rewritten);

    assert!(rewritten.contains("10.0.0.1"));
    assert!(rewritten.contains("not-a-port"));
    assert!(!rewritten.contains("proxy.example.com"));
}

#[test]
fn cluster_slots_resp2_backend_address_rejects_malformed_ports() {
    let invalid_string_port = [
        Resp2Frame::BulkString(b"10.0.0.1".to_vec()),
        Resp2Frame::BulkString(b"not-a-port".to_vec()),
    ];
    let negative_integer_port = [Resp2Frame::BulkString(b"10.0.0.1".to_vec()), Resp2Frame::Integer(-1)];

    assert_eq!(invalid_string_port.backend_address(), None);
    assert_eq!(negative_integer_port.backend_address(), None);
}

#[test]
fn cluster_slots_resp3_backend_address_rejects_malformed_ports() {
    let invalid_string_port = [
        Resp3Frame::BlobString { data: b"10.0.0.1".to_vec(), attributes: None },
        Resp3Frame::BlobString { data: b"not-a-port".to_vec(), attributes: None },
    ];
    let oversized_integer_port = [
        Resp3Frame::BlobString { data: b"10.0.0.1".to_vec(), attributes: None },
        Resp3Frame::Number { data: 70_000, attributes: None },
    ];

    assert_eq!(invalid_string_port.backend_address(), None);
    assert_eq!(oversized_integer_port.backend_address(), None);
}

#[test]
fn rewrite_cluster_shards_response_rewrites_resp2_node_maps() {
    let topology = sample_topology();
    let response = Resp2Frame::Array(vec![Resp2Frame::Array(vec![
        Resp2Frame::BulkString(b"slots".to_vec()),
        Resp2Frame::Array(vec![Resp2Frame::Array(vec![Resp2Frame::Integer(0), Resp2Frame::Integer(8191)])]),
        Resp2Frame::BulkString(b"nodes".to_vec()),
        Resp2Frame::Array(vec![Resp2Frame::Array(vec![
            Resp2Frame::BulkString(b"id".to_vec()),
            Resp2Frame::BulkString(b"node-1".to_vec()),
            Resp2Frame::BulkString(b"endpoint".to_vec()),
            Resp2Frame::BulkString(b"10.0.0.1:6379".to_vec()),
            Resp2Frame::BulkString(b"ip".to_vec()),
            Resp2Frame::BulkString(b"10.0.0.1".to_vec()),
            Resp2Frame::BulkString(b"hostname".to_vec()),
            Resp2Frame::BulkString(b"10.0.0.1".to_vec()),
            Resp2Frame::BulkString(b"port".to_vec()),
            Resp2Frame::Integer(6379),
        ])]),
    ])]);
    let raw = RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(response)).expect("encode RESP2 shards");

    let rewritten = ClusterResponseRewriter::shards_response(&raw, &topology).expect("rewrite RESP2 shards");
    let rewritten = String::from_utf8_lossy(&rewritten);

    assert!(rewritten.contains("eden-n1"));
    assert!(rewritten.contains("proxy.example.com"));
    assert!(rewritten.contains("17000"));
    assert!(!rewritten.contains("10.0.0.1"));
    assert!(!rewritten.contains("node-1"));
}

#[test]
fn build_virtual_cluster_topology_prefers_same_backend_node_id() {
    let endpoint_uuid = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
    let template = sample_topology();
    let listeners = vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)];
    let ordered_nodes = vec![
        ClusterProxyNode {
            node_id: "node-2".to_string(),
            host: "10.0.0.2".to_string(),
            port: 6380,
            bus_port: Some(16380),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(8192, 16383)],
            connected: true,
        },
        ClusterProxyNode {
            node_id: "node-1".to_string(),
            host: "10.0.0.1".to_string(),
            port: 6379,
            bus_port: Some(16379),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(0, 8191)],
            connected: true,
        },
    ];

    let topology = ClusterTopologyBuilder::build(&endpoint_uuid, &listeners, "proxy.example.com", &ordered_nodes, Some(&template))
        .expect("stable topology rebuild");

    assert_eq!(topology.nodes[0].backend.node_id, "node-1");
    assert_eq!(topology.nodes[1].backend.node_id, "node-2");
}

#[test]
fn ordered_cluster_nodes_groups_replicas_after_their_master() {
    let raw = "\
replica-b 10.0.0.12:6379@16379 slave master-b 0 0 4 connected\n\
master-b 10.0.0.2:6380@16380 master - 0 0 3 connected 8192-16383\n\
replica-a 10.0.0.11:6379@16379 slave master-a 0 0 2 connected\n\
master-a 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n";
    let nodes = ClusterNodesParser::parse(raw);

    let ordered_ids = ClusterTopologyBuilder::ordered_nodes(&nodes).into_iter().map(|node| node.node_id).collect::<Vec<_>>();

    assert_eq!(ordered_ids, vec!["master-a", "replica-a", "master-b", "replica-b"]);
}

#[test]
fn build_virtual_cluster_topology_rejects_same_endpoint_role_only_fallback() {
    let template = sample_topology();
    let endpoint_uuid = template.endpoint_uuid.clone();
    let listeners = vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)];
    let ordered_nodes = vec![template.nodes[1].backend.clone()];

    let err = ClusterTopologyBuilder::build(&endpoint_uuid, &listeners, "proxy.example.com", &ordered_nodes, Some(&template))
        .expect_err("same-endpoint refresh should not remap n1 onto node-2 by role alone");

    assert!(err.to_string().contains("listener 'n1' cannot be matched"));
}

#[test]
fn build_virtual_cluster_topology_rejects_same_endpoint_missing_binding() {
    let template = sample_topology();
    let endpoint_uuid = template.endpoint_uuid.clone();
    let listeners = vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)];
    let ordered_nodes = vec![template.nodes[0].backend.clone()];

    let err = ClusterTopologyBuilder::build(&endpoint_uuid, &listeners, "proxy.example.com", &ordered_nodes, Some(&template))
        .expect_err("same-endpoint refresh should not clone a missing frozen backend");

    assert!(err.to_string().contains("listener 'n2' cannot be matched"));
}

#[test]
fn build_virtual_cluster_topology_rejects_incompatible_target_layout() {
    let target_endpoint_uuid = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
    let template = sample_topology();
    let listeners = vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)];
    let ordered_nodes = vec![
        ClusterProxyNode {
            node_id: "target-1".to_string(),
            host: "10.1.0.1".to_string(),
            port: 6379,
            bus_port: Some(16379),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(0, 4095)],
            connected: true,
        },
        ClusterProxyNode {
            node_id: "target-2".to_string(),
            host: "10.1.0.2".to_string(),
            port: 6380,
            bus_port: Some(16380),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(4096, 16383)],
            connected: true,
        },
    ];

    let err = ClusterTopologyBuilder::build(&target_endpoint_uuid, &listeners, "proxy.example.com", &ordered_nodes, Some(&template))
        .expect_err("target topology should be incompatible");

    assert!(err.to_string().contains("frozen listener topology"));
}

#[test]
fn build_virtual_cluster_topology_rejects_disconnected_target_node() {
    let target_endpoint_uuid = EndpointCacheUuid::new(None, EndpointUuid::new_uuid());
    let template = sample_topology();
    let listeners = vec![InterlayListener::new("n1", 7000, 17000), InterlayListener::new("n2", 7001, 17001)];
    let ordered_nodes = vec![
        ClusterProxyNode {
            node_id: "node-1".to_string(),
            host: "10.1.0.1".to_string(),
            port: 6379,
            bus_port: Some(16379),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(0, 8191)],
            connected: false,
        },
        ClusterProxyNode {
            node_id: "node-2".to_string(),
            host: "10.1.0.2".to_string(),
            port: 6380,
            bus_port: Some(16380),
            role: ClusterProxyNodeRole::Master,
            master_id: None,
            flags: vec!["master".to_string()],
            slot_ranges: vec![(8192, 16383)],
            connected: true,
        },
    ];

    let err = ClusterTopologyBuilder::build(&target_endpoint_uuid, &listeners, "proxy.example.com", &ordered_nodes, Some(&template))
        .expect_err("disconnected target topology should be rejected");

    assert!(err.to_string().contains("frozen listener topology"));
}

#[test]
fn command_slot_uses_hash_tags_and_detects_crossslot() {
    let same_slot = RedisCommandArgs::new(
        RedisApi::Mget,
        vec![
            RedisJsonValue::String("user:{42}:name".to_string()),
            RedisJsonValue::String("user:{42}:email".to_string()),
        ],
    );
    assert_eq!(
        ClusterExecution::command_slot(&same_slot),
        Ok(Some(ClusterExecution::slot_for_key(b"user:{42}:name")))
    );

    let crossslot = RedisCommandArgs::new(
        RedisApi::Mget,
        vec![
            RedisJsonValue::String("user:{42}:name".to_string()),
            RedisJsonValue::String("user:{99}:email".to_string()),
        ],
    );
    assert_eq!(ClusterExecution::command_slot(&crossslot), Err(CROSSSLOT_RESPONSE.clone()));
}
