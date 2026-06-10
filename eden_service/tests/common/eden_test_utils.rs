#![allow(dead_code)]

use database::methods::insert::InsertMethod;
use database::methods::insert::eden_node::InsertEdenNode;
use database::methods::insert::organization::InsertOrganization;
use eden_core::auth::Password;
use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId};
use eden_core::format::cache_uuid::{EdenNodeCacheUuid, OrganizationCacheUuid};
use eden_core::format::{EdenNodeId, EdenNodeUuid, EndpointUuid, OrganizationId, UserId};
use eden_core::telemetry::TelemetryWrapper;
use eden_service::EdenDb;
use endpoint_core::ep_core::database::schema::Table;
use endpoint_core::ep_core::database::schema::eden_node::EdenNodeSchema;
use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
use endpoint_core::ep_core::database::schema::user::UserSchema;

pub async fn insert_eden_node(
    db_manager: &EdenDb,
    telemetry: &mut TelemetryWrapper,
    eden_node_id: &str,
    endpoint_uuids: Vec<EndpointUuid>,
    info: serde_json::Value,
) -> EdenNodeSchema {
    let eden_node_uuid = EdenNodeUuid::new_uuid();
    let eden_node_schema = EdenNodeSchema::new(eden_node_id.to_string(), eden_node_uuid, endpoint_uuids, info);
    let insert_eden_node = InsertEdenNode::new(eden_node_schema.clone());

    <EdenDb as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(
        db_manager,
        insert_eden_node,
        telemetry,
    )
    .await
    .expect("Failed to insert eden node");

    eden_node_schema
}

pub async fn insert_organization(
    db_manager: &EdenDb,
    telemetry: &mut TelemetryWrapper,
    organization_id: &str,
    eden_node_uuids: Vec<EdenNodeUuid>,
    description: Option<String>,
) -> OrganizationSchema {
    let organization_schema = OrganizationSchema::new(
        organization_id.to_string(),
        None,
        eden_node_uuids
            .iter()
            .map(|uuid| (format!("eden_node_{}", uuid.to_string().split_at(4).0).into(), uuid.clone()))
            .collect(),
        description,
    );
    let insert_organization = InsertOrganization::new(organization_schema.clone());

    <EdenDb as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(
        db_manager,
        insert_organization,
        telemetry,
    )
    .await
    .expect("Failed to insert organization");

    organization_schema
}

pub async fn initialize_organization(
    db_manager: &EdenDb,
    telemetry: &mut TelemetryWrapper,
) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
    let eden_node_schema = match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), telemetry).await {
        Ok(eden_node) => eden_node,
        Err(_) => insert_eden_node(db_manager, telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
    };

    let organization_id: OrganizationId = "test_organization".into();
    let organization_schema = insert_organization(db_manager, telemetry, &organization_id, vec![eden_node_schema.uuid()], None).await;

    let admin_user_schema = UserSchema::new(
        UserId::from("username"),
        Password::new("password".to_string()),
        organization_schema.uuid(),
        None,
        None,
        None,
    );

    (admin_user_schema, eden_node_schema, organization_schema)
}
