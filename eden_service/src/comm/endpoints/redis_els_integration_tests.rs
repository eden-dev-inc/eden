use super::read::endpoint_read;
use super::transaction::endpoint_transaction;
use super::write::endpoint_write;
use crate::test_utils::redis_migrate_test_utils::connect_to_multi_redis;
use crate::test_utils::telemetry_test_utils::test_telemetry;
use actix_web::body::to_bytes;
use actix_web::web;
use database::db::els::{AssignPolicyRequest, AssignmentMode, CreatePolicyRequest, ElsCommands, ElsStrategy};
use database::methods::insert::endpoint::InsertEndpoint;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, OrganizationId, PolicyUuid, UserId, UserUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::ep::EpConfig;
use endpoint_core::ep_core::settings::EdenSettings;
use endpoint_core::redis_core::{RedisConfig, RedisConnection};
use endpoint_schema::endpoint::EndpointSchema;
use endpoint_types::transaction::{EndpointTransactionInput, Transaction};
use endpoints::endpoint::ep_redis::api::lib::{GetInputBuilder, GetOutput, SetInputBuilder};
use endpoints::endpoint::ep_redis::request::RedisRequest;
use endpoints::endpoint::{EpRequest, RequestConstructor};
use ep_runtime::comp::MyEngineService;
use serde_json::json;
use serial_test::serial;
use testcontainers_modules::redis::Redis;
use testcontainers_modules::testcontainers::ContainerAsync;
use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand};

fn test_auth(org_uuid: eden_core::format::OrganizationUuid, user_uuid: UserUuid) -> ParsedJwt {
    ParsedJwt::new(UserId::from("els_user"), user_uuid, OrganizationId::from("test_organization"), org_uuid)
}

fn endpoint_cache_object(
    org_cache: &OrganizationCacheUuid,
    endpoint_schema: &EndpointSchema,
) -> CacheObjectType<EndpointCacheUuid, EndpointCacheId> {
    CacheObjectType::from((Some(org_cache.clone()), endpoint_schema.id().to_string()))
}

fn decode_redis_resp_bytes(bytes: &[u8]) -> Option<String> {
    let output = GetOutput::decode(bytes).ok()?;
    let value = output.value()?;
    serde_json::to_value(value).ok()?.as_str().map(ToOwned::to_owned)
}

fn extract_redis_string(value: &serde_json::Value) -> Option<String> {
    if let Some(s) = value.as_str() {
        return Some(s.to_string());
    }

    if let Some(s) = value.get("data").and_then(extract_redis_string) {
        return Some(s);
    }

    if let Some(s) = value.get("value").and_then(extract_redis_string) {
        return Some(s);
    }

    if let Some(array) = value.as_array() {
        if array.iter().all(|item| item.as_u64().is_some()) {
            let bytes: Vec<u8> = array.iter().filter_map(|item| item.as_u64().map(|n| n as u8)).collect();
            if let Some(decoded) = decode_redis_resp_bytes(&bytes) {
                return Some(decoded);
            }
        }

        if array.len() == 1 {
            return extract_redis_string(&array[0]);
        }
    }

    if let Some(object) = value.as_object() {
        if let Some(s) = object.values().find_map(extract_redis_string) {
            return Some(s);
        }
    }

    None
}

async fn assign_endpoint_switch_policy(
    database: &crate::EdenDb,
    endpoint_cache_uuid: &EndpointCacheUuid,
    target_schema: &EndpointSchema,
    user_uuid: &UserUuid,
) {
    let policy_uuid = database
        .els_create_policy(
            endpoint_cache_uuid,
            &CreatePolicyRequest {
                name: "redis-endpoint-switch".to_string(),
                strategy: ElsStrategy::Redis,
                config: json!({ "endpoint_uuid": target_schema.uuid().to_string() }),
            },
        )
        .await
        .expect("create endpoint-switch policy");

    database
        .els_assign_user(
            endpoint_cache_uuid,
            user_uuid,
            &AssignPolicyRequest {
                policy_uuid: PolicyUuid::from(policy_uuid),
                mode: AssignmentMode::Sync,
            },
        )
        .await
        .expect("assign endpoint-switch policy");
}

async fn assign_acl_policy(
    database: &crate::EdenDb,
    endpoint_cache_uuid: &EndpointCacheUuid,
    username: &str,
    password: &str,
    user_uuid: &UserUuid,
) {
    let policy_uuid = database
        .els_create_policy(
            endpoint_cache_uuid,
            &CreatePolicyRequest {
                name: format!("redis-acl-{username}"),
                strategy: ElsStrategy::Redis,
                config: json!({
                    "username": username,
                    "password": password,
                }),
            },
        )
        .await
        .expect("create redis ACL policy");

    database
        .els_assign_user(
            endpoint_cache_uuid,
            user_uuid,
            &AssignPolicyRequest {
                policy_uuid: PolicyUuid::from(policy_uuid),
                mode: AssignmentMode::Sync,
            },
        )
        .await
        .expect("assign redis ACL policy");
}

async fn create_acl_user(container: &ContainerAsync<Redis>, username: &str, password: &str, key_pattern: &str) {
    let password_rule = format!(">{password}");
    let key_rule = key_pattern.to_string();
    container
        .exec(
            ExecCommand::new(vec![
                "redis-cli".to_string(),
                "ACL".to_string(),
                "SETUSER".to_string(),
                username.to_string(),
                "reset".to_string(),
                "on".to_string(),
                password_rule,
                key_rule,
                "+@all".to_string(),
            ])
            .with_cmd_ready_condition(CmdWaitFor::message_on_stdout("OK")),
        )
        .await
        .expect("create redis ACL user");
}

async fn connect_acl_endpoint(
    database_manager: &crate::EdenDb,
    engine_service: &web::Data<MyEngineService>,
    organization_schema: &endpoint_core::ep_core::database::schema::organization::OrganizationSchema,
    host: String,
    port: u16,
    username: &str,
    password: &str,
) -> EndpointSchema {
    let connection = RedisConnection {
        host,
        port: Some(port),
        tls: None,
        insecure: None,
        db: None,
        username: Some(username.to_string()),
        password: Some(password.to_string()),
        protocol_version: None,
        connect_timeout_secs: None,
        max_retries: None,
    };

    let (target, creds) = connection.split().expect("split redis connection");
    let redis_config: Box<dyn EpConfig> = Box::new(RedisConfig {
        target,
        read_credentials: Some(creds.clone()),
        write_credentials: Some(creds.clone()),
        admin_credentials: Some(creds.clone()),
        system_credentials: Some(creds),
        ..Default::default()
    });

    let endpoint_schema = EndpointSchema::new(
        format!("redis-acl-{username}").into(),
        EpKind::Redis,
        redis_config,
        None,
        None,
        UserUuid::new_uuid(),
    );

    let mut telemetry_wrapper = test_telemetry();
    engine_service
        .connect(
            database_manager,
            &InsertEndpoint::new(
                organization_schema.uuid(),
                endpoint_schema.clone(),
                organization_schema.eden_node_uuids()[0].to_owned(),
            ),
            &mut telemetry_wrapper,
        )
        .await
        .expect("connect ACL-backed redis endpoint");

    endpoint_schema
}

async fn direct_set(
    engine_service: &web::Data<MyEngineService>,
    endpoint_schema: &EndpointSchema,
    org_cache: &OrganizationCacheUuid,
    key: &str,
    value: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) {
    let input = SetInputBuilder::default()
        .key(key.to_string().into())
        .value(value.to_string().into())
        .rule(None)
        .get(None)
        .options(None)
        .build()
        .expect("build redis SET");
    let mut request: Box<dyn EpRequest> = Box::new(RedisRequest::new(Box::new(input)));

    engine_service
        .write_els(&mut *request, endpoint_schema, None, org_cache.clone(), EdenSettings::default(), telemetry_wrapper)
        .await
        .expect("write key directly");
}

async fn direct_get(
    engine_service: &web::Data<MyEngineService>,
    endpoint_schema: &EndpointSchema,
    org_cache: &OrganizationCacheUuid,
    key: &str,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> String {
    let input = GetInputBuilder::default().key(key.to_string().into()).build().expect("build redis GET");
    let mut request: Box<dyn EpRequest> = Box::new(RedisRequest::new(Box::new(input)));

    let value = engine_service
        .read_els(&mut *request, endpoint_schema, None, org_cache.clone(), EdenSettings::default(), telemetry_wrapper)
        .await
        .expect("read key directly");

    extract_redis_string(&value).expect("redis GET should decode to a string")
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_endpoint_switch_read_reads_from_target_endpoint() {
    let (endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(2).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let origin_endpoint_cache_uuid = endpoints[0].1.clone();
    let origin_schema = endpoints[0].2.clone();
    let target_schema = endpoints[1].2.clone();
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());

    direct_set(
        &engine_service,
        &origin_schema,
        &org_cache,
        "els-read-key",
        "origin-read-value",
        &mut telemetry_wrapper,
    )
    .await;
    direct_set(
        &engine_service,
        &target_schema,
        &org_cache,
        "els-read-key",
        "target-read-value",
        &mut telemetry_wrapper,
    )
    .await;
    assign_endpoint_switch_policy(&database_manager, &origin_endpoint_cache_uuid, &target_schema, &user_uuid).await;

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_endpoint_switch_read");
    let response = endpoint_read(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &origin_schema),
        GetInputBuilder::default()
            .key("els-read-key".to_string().into())
            .build()
            .expect("build read input")
            .try_into()
            .expect("serialize read input"),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint read should succeed");

    let body = to_bytes(response.into_body()).await.expect("read response body");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("read response json");
    assert_eq!(value.get("kind").and_then(serde_json::Value::as_str), Some("redis"));
    assert_eq!(extract_redis_string(&value).as_deref(), Some("target-read-value"));
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_endpoint_switch_write_updates_only_target_endpoint() {
    let (endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(2).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let origin_endpoint_cache_uuid = endpoints[0].1.clone();
    let origin_schema = endpoints[0].2.clone();
    let target_schema = endpoints[1].2.clone();
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());

    direct_set(
        &engine_service,
        &origin_schema,
        &org_cache,
        "els-write-key",
        "origin-before",
        &mut telemetry_wrapper,
    )
    .await;
    direct_set(
        &engine_service,
        &target_schema,
        &org_cache,
        "els-write-key",
        "target-before",
        &mut telemetry_wrapper,
    )
    .await;
    assign_endpoint_switch_policy(&database_manager, &origin_endpoint_cache_uuid, &target_schema, &user_uuid).await;

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_endpoint_switch_write");
    let response = endpoint_write(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &origin_schema),
        SetInputBuilder::default()
            .key("els-write-key".to_string().into())
            .value("target-after".to_string().into())
            .rule(None)
            .get(None)
            .options(None)
            .build()
            .expect("build write input")
            .try_into()
            .expect("serialize write input"),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint write should succeed");

    assert!(response.status().is_success(), "write should succeed");
    assert_eq!(
        direct_get(&engine_service, &origin_schema, &org_cache, "els-write-key", &mut telemetry_wrapper,).await,
        "origin-before"
    );
    assert_eq!(
        direct_get(&engine_service, &target_schema, &org_cache, "els-write-key", &mut telemetry_wrapper,).await,
        "target-after"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_endpoint_switch_transaction_updates_only_target_endpoint() {
    let (endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(2).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let origin_endpoint_cache_uuid = endpoints[0].1.clone();
    let origin_schema = endpoints[0].2.clone();
    let target_schema = endpoints[1].2.clone();
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());

    direct_set(
        &engine_service,
        &origin_schema,
        &org_cache,
        "els-transaction-key",
        "origin-before",
        &mut telemetry_wrapper,
    )
    .await;
    direct_set(
        &engine_service,
        &target_schema,
        &org_cache,
        "els-transaction-key",
        "target-before",
        &mut telemetry_wrapper,
    )
    .await;
    assign_endpoint_switch_policy(&database_manager, &origin_endpoint_cache_uuid, &target_schema, &user_uuid).await;

    let transaction = Transaction::<RedisRequest>(vec![RedisRequest::new(Box::new(
        SetInputBuilder::default()
            .key("els-transaction-key".to_string().into())
            .value("target-after".to_string().into())
            .rule(None)
            .get(None)
            .options(None)
            .build()
            .expect("build transaction SET"),
    ))]);

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_endpoint_switch_transaction");
    let response = endpoint_transaction(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &origin_schema),
        EndpointTransactionInput::new(json!({ "kind": EpKind::Redis, "data": transaction })),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint transaction should succeed");

    assert!(response.status().is_success(), "transaction should succeed");
    assert_eq!(
        direct_get(&engine_service, &origin_schema, &org_cache, "els-transaction-key", &mut telemetry_wrapper,).await,
        "origin-before"
    );
    assert_eq!(
        direct_get(&engine_service, &target_schema, &org_cache, "els-transaction-key", &mut telemetry_wrapper,).await,
        "target-after"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_acl_read_uses_override_user_credentials() {
    let (mut endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(1).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let (redis_container, _admin_cache_uuid, admin_schema) = endpoints.pop().expect("redis endpoint");
    let host_port = redis_container.get_host_port_ipv4(6379).await.expect("redis host port");
    let base_username = "redis_els_base";
    let base_password = "redis_els_base_pw";
    let els_username = "redis_els_override";
    let els_password = "redis_els_override_pw";

    create_acl_user(&redis_container, base_username, base_password, "~base:*").await;
    create_acl_user(&redis_container, els_username, els_password, "~els:*").await;

    direct_set(&engine_service, &admin_schema, &org_cache, "els:read:key", "els-read-value", &mut telemetry_wrapper).await;

    let secured_schema = connect_acl_endpoint(
        &database_manager,
        &engine_service,
        &organization_schema,
        "127.0.0.1".to_string(),
        host_port,
        base_username,
        base_password,
    )
    .await;
    let secured_endpoint_cache_uuid = secured_schema.cache_key(org_cache.clone());
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());
    assign_acl_policy(&database_manager, &secured_endpoint_cache_uuid, els_username, els_password, &user_uuid).await;

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_acl_read");
    let response = endpoint_read(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &secured_schema),
        GetInputBuilder::default()
            .key("els:read:key".to_string().into())
            .build()
            .expect("build read input")
            .try_into()
            .expect("serialize read input"),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint read should succeed under redis ACL ELS");

    let body = to_bytes(response.into_body()).await.expect("read response body");
    let value: serde_json::Value = serde_json::from_slice(&body).expect("read response json");
    assert_eq!(value.get("kind").and_then(serde_json::Value::as_str), Some("redis"));
    assert_eq!(extract_redis_string(&value).as_deref(), Some("els-read-value"));
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_acl_write_uses_override_user_credentials() {
    let (mut endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(1).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let (redis_container, _admin_cache_uuid, admin_schema) = endpoints.pop().expect("redis endpoint");
    let host_port = redis_container.get_host_port_ipv4(6379).await.expect("redis host port");
    let base_username = "redis_els_base_write";
    let base_password = "redis_els_base_write_pw";
    let els_username = "redis_els_override_write";
    let els_password = "redis_els_override_write_pw";

    create_acl_user(&redis_container, base_username, base_password, "~base:*").await;
    create_acl_user(&redis_container, els_username, els_password, "~els:*").await;

    let secured_schema = connect_acl_endpoint(
        &database_manager,
        &engine_service,
        &organization_schema,
        "127.0.0.1".to_string(),
        host_port,
        base_username,
        base_password,
    )
    .await;
    let secured_endpoint_cache_uuid = secured_schema.cache_key(org_cache.clone());
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());
    assign_acl_policy(&database_manager, &secured_endpoint_cache_uuid, els_username, els_password, &user_uuid).await;

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_acl_write");
    let response = endpoint_write(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &secured_schema),
        SetInputBuilder::default()
            .key("els:write:key".to_string().into())
            .value("els-write-value".to_string().into())
            .rule(None)
            .get(None)
            .options(None)
            .build()
            .expect("build write input")
            .try_into()
            .expect("serialize write input"),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint write should succeed under redis ACL ELS");

    assert!(response.status().is_success(), "write should succeed");
    assert_eq!(
        direct_get(&engine_service, &admin_schema, &org_cache, "els:write:key", &mut telemetry_wrapper).await,
        "els-write-value"
    );
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
async fn redis_els_acl_transaction_uses_override_user_credentials() {
    let (mut endpoints, engine_service, database_manager, organization_schema, mut telemetry_wrapper) = connect_to_multi_redis(1).await;
    let engine_service = web::Data::from(engine_service);
    let org_cache = OrganizationCacheUuid::new(None, organization_schema.uuid());
    let (redis_container, _admin_cache_uuid, admin_schema) = endpoints.pop().expect("redis endpoint");
    let host_port = redis_container.get_host_port_ipv4(6379).await.expect("redis host port");
    let base_username = "redis_els_base_tx";
    let base_password = "redis_els_base_tx_pw";
    let els_username = "redis_els_override_tx";
    let els_password = "redis_els_override_tx_pw";

    create_acl_user(&redis_container, base_username, base_password, "~base:*").await;
    create_acl_user(&redis_container, els_username, els_password, "~els:*").await;

    let secured_schema = connect_acl_endpoint(
        &database_manager,
        &engine_service,
        &organization_schema,
        "127.0.0.1".to_string(),
        host_port,
        base_username,
        base_password,
    )
    .await;
    let secured_endpoint_cache_uuid = secured_schema.cache_key(org_cache.clone());
    let user_uuid = UserUuid::new_uuid();
    let auth = test_auth(organization_schema.uuid(), user_uuid.clone());
    assign_acl_policy(&database_manager, &secured_endpoint_cache_uuid, els_username, els_password, &user_uuid).await;

    let transaction = Transaction::<RedisRequest>(vec![RedisRequest::new(Box::new(
        SetInputBuilder::default()
            .key("els:transaction:key".to_string().into())
            .value("els-transaction-value".to_string().into())
            .rule(None)
            .get(None)
            .options(None)
            .build()
            .expect("build transaction SET"),
    ))]);

    let mut span = telemetry_wrapper.client_tracer("test.redis_els_acl_transaction");
    let response = endpoint_transaction(
        &database_manager,
        &engine_service,
        org_cache.clone(),
        &endpoint_cache_object(&org_cache, &secured_schema),
        EndpointTransactionInput::new(json!({ "kind": EpKind::Redis, "data": transaction })),
        &auth,
        EdenSettings::default(),
        &mut span,
        &mut telemetry_wrapper,
    )
    .await
    .expect("endpoint transaction should succeed under redis ACL ELS");

    assert!(response.status().is_success(), "transaction should succeed");
    assert_eq!(
        direct_get(&engine_service, &admin_schema, &org_cache, "els:transaction:key", &mut telemetry_wrapper).await,
        "els-transaction-value"
    );
}
