//! Integration tests for embedded-db (SQLite/Turso) database operations.
//!
//! Each test creates an isolated in-memory database via
//! [`create_local_database_manager`] so tests never interfere.
#![cfg(all(test, embedded_db))]

use crate::db::cache::CacheFunctions;
use crate::db::duckdb_analytics::DuckDbAnalyticsConfig;
use crate::db::lib::{CacheTtl, ClickhouseConn, DatabaseManager, RedisConn};
use crate::db::methods::delete::DeleteMethod;
use crate::db::methods::delete::organization::DeleteOrganization;
use crate::db::methods::delete::user::DeleteUser;
use crate::db::methods::insert::InsertMethod;
use crate::db::methods::insert::eden_node::InsertEdenNode;
use crate::db::methods::insert::endpoint::InsertEndpoint;
use crate::db::methods::insert::organization::InsertOrganization;
use crate::db::methods::insert::user::{InsertAdminUser, InsertUser};
use crate::db::methods::llm::{NewLlmCredential, NewSkill};
use crate::db::rbac::ControlPlaneRbac;
use crate::db::turso::TursoPool;
use crate::test_utils::embedded_db_test_utils::create_local_database_manager;
use crate::test_utils::telemetry_test_utils::test_telemetry;
use eden_core::auth::Password;
use eden_core::format::cache_id::{EdenNodeCacheId, EndpointCacheId, OrganizationCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, EdenNodeCacheUuid, EndpointCacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::endpoint::EpKind;
use eden_core::format::rbac::{ControlPerms, ControlPlaneRbacData};
use eden_core::format::{
    CacheObjectType, EdenId, EdenNodeId, EdenNodeUuid, EndpointId, EndpointUuid, IdKind, OrganizationId, OrganizationUuid, UserId, UserUuid,
};
use endpoint_schema::endpoint::EndpointSchema;
use ep_core::database::schema::Table;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use ep_core::database::schema::organization::OrganizationSchema;
use ep_core::database::schema::user::UserSchema;
use llm_core::connection::LlmProvider;
use postgres_core::PostgresConfig;
use std::fs;
use std::path::{Path, PathBuf};
use uuid::Uuid;

type Db = DatabaseManager<RedisConn, TursoPool, ClickhouseConn>;
const FILE_BACKED_TEST_HEX_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const FILE_BACKED_WRONG_HEX_KEY: &str = "0000000000000000000000000000000000000000000000000000000000000000";
const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Insert an eden node, returning its schema.
async fn helper_insert_eden_node(db: &Db, tel: &mut eden_core::telemetry::TelemetryWrapper, id: &str) -> EdenNodeSchema {
    let schema = EdenNodeSchema::new(id.to_string(), EdenNodeUuid::new_uuid(), vec![], serde_json::Value::default());
    let insert = InsertEdenNode::new(schema.clone());
    <Db as InsertMethod<EdenNodeSchema, EdenNodeCacheUuid, EdenNodeCacheId, InsertEdenNode>>::insert(db, insert, tel)
        .await
        .expect("insert eden node");
    schema
}

/// Insert an organization (no users), returning its schema.
async fn helper_insert_org(
    db: &Db,
    tel: &mut eden_core::telemetry::TelemetryWrapper,
    org_id: &str,
    eden_node: &EdenNodeSchema,
) -> OrganizationSchema {
    let schema = OrganizationSchema::new(org_id.to_string(), None, vec![(eden_node.id(), eden_node.uuid())], None);
    let insert = InsertOrganization::new(schema.clone());
    <Db as InsertMethod<OrganizationSchema, OrganizationCacheUuid, OrganizationCacheId, InsertOrganization>>::insert(db, insert, tel)
        .await
        .expect("insert organization");
    schema
}

/// Insert a regular user linked to the given organization.
async fn helper_insert_user(
    db: &Db,
    tel: &mut eden_core::telemetry::TelemetryWrapper,
    username: &str,
    org_uuid: OrganizationUuid,
) -> UserSchema {
    let schema = UserSchema::new(UserId::from(username), Password::new("password".to_string()), org_uuid, None, None, None);
    let insert = InsertUser::new(schema.clone());
    <Db as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertUser>>::insert(db, insert, tel)
        .await
        .expect("insert user");
    schema
}

/// Insert an admin user linked to the given organization.
async fn helper_insert_admin_user(
    db: &Db,
    tel: &mut eden_core::telemetry::TelemetryWrapper,
    username: &str,
    org_uuid: OrganizationUuid,
) -> UserSchema {
    let schema = UserSchema::new(UserId::from(username), Password::new("admin_password".to_string()), org_uuid, None, None, None);
    let insert = InsertAdminUser::new(schema.clone());
    <Db as InsertMethod<UserSchema, UserCacheUuid, UserCacheId, InsertAdminUser>>::insert(db, insert, tel)
        .await
        .expect("insert admin user");
    schema
}

fn file_backed_analytics_config(path: &str) -> DuckDbAnalyticsConfig {
    DuckDbAnalyticsConfig {
        path: PathBuf::from(format!("{path}.duckdb")),
        memory_limit: "512MB".to_string(),
        temp_directory: PathBuf::from(format!("{path}.duckdb.tmp")),
        max_temp_directory_size: "2GB".to_string(),
        checkpoint_threshold: "64MB".to_string(),
        checkpoint_interval_secs: 60,
        analytics_retention_days: 30,
        logs_retention_days: 14,
        traces_retention_days: 14,
    }
}

fn unique_file_backed_db_path(test_name: &str) -> String {
    format!("/tmp/{test_name}_{}.db", Uuid::new_v4())
}

async fn open_file_backed_local_db(path: &str, key: &str) -> Db {
    Db::new_local(path, file_backed_analytics_config(path), CacheTtl::from_secs(3600), None, Some(key.to_string()))
        .await
        .expect("new_local should succeed")
}

// ---------------------------------------------------------------------------
// Organization CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_organization() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_org_insert").await;
    let org = helper_insert_org(&db, tel, "org_insert_test", &eden_node).await;

    // Verify via cache lookup by UUID
    let fetched: OrganizationSchema = <Db as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_from_cache(
        &db, &CacheObjectType::new(Some(OrganizationCacheUuid::new(None, org.uuid())), None), tel
    )
    .await
    .expect("get organization after insert");

    assert_eq!(fetched.uuid(), org.uuid());
    assert_eq!(fetched.id(), org.id());
}

#[tokio::test]
async fn test_delete_organization() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_org_delete").await;
    let org = helper_insert_org(&db, tel, "org_delete_test", &eden_node).await;

    let delete_obj: CacheObjectType<OrganizationCacheUuid, OrganizationCacheId> =
        CacheObjectType::new(Some(OrganizationCacheUuid::new(None, org.uuid())), None);
    let deleter = <DeleteOrganization as DeleteMethod<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
        RedisConn,
        TursoPool,
        ClickhouseConn,
    >>::new(delete_obj);
    deleter.delete(&db, tel).await.expect("delete organization");

    // Verify it is gone: a select should fail
    let result = db.select_organization_uuid::<OrganizationSchema>(&org.uuid(), tel).await;
    assert!(result.is_err(), "organization should not exist after deletion");
}

// ---------------------------------------------------------------------------
// User CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_user() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_user_insert").await;
    let org = helper_insert_org(&db, tel, "org_user_insert", &eden_node).await;
    let user = helper_insert_user(&db, tel, "test_user_regular", org.uuid()).await;

    // Verify via cache lookup
    let fetched: UserSchema = <Db as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &db,
        &CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid())), None),
        tel,
    )
    .await
    .expect("get user after insert");

    assert_eq!(fetched.uuid(), user.uuid());
    assert_eq!(fetched.username(), user.username());

    // Verify the organization_users junction has an entry
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn
        .query(
            "SELECT user_uuid FROM organization_users WHERE organization_uuid = ?1 AND user_uuid = ?2",
            &[&org.uuid(), &user.uuid()],
        )
        .await
        .expect("query organization_users");
    assert_eq!(rows.len(), 1, "expected one row in organization_users");
}

#[tokio::test]
async fn test_insert_admin_user() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_admin_user").await;
    let org = helper_insert_org(&db, tel, "org_admin_user", &eden_node).await;
    let admin = helper_insert_admin_user(&db, tel, "admin_user_1", org.uuid()).await;

    // Verify the user exists
    let fetched: UserSchema = <Db as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &db,
        &CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), admin.uuid())), None),
        tel,
    )
    .await
    .expect("get admin user");
    assert_eq!(fetched.uuid(), admin.uuid());

    // Verify the user is in organization_admins
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn
        .query(
            "SELECT user_uuid FROM organization_admins WHERE organization_uuid = ?1 AND user_uuid = ?2",
            &[&org.uuid(), &admin.uuid()],
        )
        .await
        .expect("query organization_admins");
    assert_eq!(rows.len(), 1, "expected one row in organization_admins");
}

#[tokio::test]
async fn test_delete_user() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_user_del").await;
    let org = helper_insert_org(&db, tel, "org_user_del", &eden_node).await;
    let user = helper_insert_user(&db, tel, "user_to_delete", org.uuid()).await;

    let delete_obj: CacheObjectType<UserCacheUuid, UserCacheId> =
        CacheObjectType::new(Some(UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid())), None);
    let deleter =
        <DeleteUser as DeleteMethod<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId, RedisConn, TursoPool, ClickhouseConn>>::new(
            delete_obj,
        );
    deleter.delete(&db, tel).await.expect("delete user");

    // Verify user row is gone from the users table
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn.query("SELECT uuid FROM users WHERE uuid = ?1", &[&user.uuid()]).await.expect("query users after delete");
    assert!(rows.is_empty(), "user row should be deleted");
}

// ---------------------------------------------------------------------------
// Endpoint CRUD
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_endpoint() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_ep_insert").await;
    let org = helper_insert_org(&db, tel, "org_ep_insert", &eden_node).await;

    let config = Box::new(PostgresConfig::default());

    let ep_schema = EndpointSchema::new(
        EndpointId::new("ep_test".to_string()),
        EpKind::Postgres,
        config,
        None,
        Some("test endpoint".to_string()),
        UserUuid::new_uuid(),
    );

    let insert = InsertEndpoint::new(org.uuid(), ep_schema.clone(), eden_node.uuid());
    <Db as InsertMethod<EndpointSchema, EndpointCacheUuid, EndpointCacheId, InsertEndpoint>>::insert(&db, insert, tel)
        .await
        .expect("insert endpoint");

    // Verify the endpoint row exists
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn.query("SELECT id FROM endpoints WHERE id = ?1", &[&ep_schema.id()]).await.expect("query endpoints");
    assert_eq!(rows.len(), 1, "expected one endpoint row");

    // Verify organization_endpoints junction
    let rows = conn
        .query("SELECT endpoint_uuid FROM organization_endpoints WHERE organization_uuid = ?1", &[&org.uuid()])
        .await
        .expect("query organization_endpoints");
    assert_eq!(rows.len(), 1, "expected one org-endpoint link");
}

#[tokio::test]
async fn test_delete_endpoint() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_ep_del").await;
    let org = helper_insert_org(&db, tel, "org_ep_del", &eden_node).await;

    let config = Box::new(PostgresConfig::default());
    let ep_schema = EndpointSchema::new(
        EndpointId::new("ep_del_test".to_string()),
        EpKind::Postgres,
        config,
        None,
        None,
        UserUuid::new_uuid(),
    );
    let insert = InsertEndpoint::new(org.uuid(), ep_schema.clone(), eden_node.uuid());
    <Db as InsertMethod<EndpointSchema, EndpointCacheUuid, EndpointCacheId, InsertEndpoint>>::insert(&db, insert, tel)
        .await
        .expect("insert endpoint for delete test");

    // Delete
    use crate::db::methods::delete::endpoint::DeleteEndpoint;
    let delete_obj: CacheObjectType<EndpointCacheUuid, EndpointCacheId> = CacheObjectType::new(
        Some(EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), ep_schema.uuid())),
        None,
    );
    let deleter = <DeleteEndpoint as DeleteMethod<
        EndpointSchema,
        EndpointCacheUuid,
        EndpointUuid,
        EndpointCacheId,
        EndpointId,
        RedisConn,
        TursoPool,
        ClickhouseConn,
    >>::new(delete_obj);
    deleter.delete(&db, tel).await.expect("delete endpoint");

    // Verify endpoint is gone
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn
        .query("SELECT id FROM endpoints WHERE uuid = ?1", &[&ep_schema.uuid()])
        .await
        .expect("query endpoints after delete");
    assert!(rows.is_empty(), "endpoint should be deleted");
}

// ---------------------------------------------------------------------------
// RBAC operations
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_rbac_add_and_get() {
    let db = create_local_database_manager().await;
    let _tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, _tel, "node_rbac").await;
    let org = helper_insert_org(&db, _tel, "org_rbac", &eden_node).await;
    let user = helper_insert_user(&db, _tel, "rbac_user", org.uuid()).await;

    let org_uuid = OrganizationCacheUuid::new(None, org.uuid()).uuid();
    let user_uuid = UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid()).uuid();

    // Grant Admin-equivalent perms (RCPGA)
    let grant = ControlPlaneRbacData {
        org_uuid,
        entity_kind: IdKind::Organization.as_str().to_owned(),
        entity_uuid: org_uuid,
        subject_kind: IdKind::User.as_str().to_owned(),
        subject_uuid: user_uuid,
        perms: ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::PROMOTE | ControlPerms::GRANT | ControlPerms::AUDIT,
    };
    let version_ms = chrono::Utc::now().timestamp_millis();
    db.control_plane_grant(&grant, version_ms, 0i64).await.expect("add RBAC entry");

    // Verify we can get the subject's permissions
    let perms = db.control_plane_get(org_uuid, IdKind::Organization, org_uuid, IdKind::User, user_uuid).await.expect("get RBAC perms");
    assert!(perms.contains(ControlPerms::GRANT), "should have GRANT");
    assert!(!perms.contains(ControlPerms::DESTROY), "should not have DESTROY");
}

#[tokio::test]
async fn test_rbac_verify() {
    let db = create_local_database_manager().await;
    let _tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, _tel, "node_rbac_verify").await;
    let org = helper_insert_org(&db, _tel, "org_rbac_verify", &eden_node).await;
    let user = helper_insert_user(&db, _tel, "rbac_verify_user", org.uuid()).await;

    let org_uuid = OrganizationCacheUuid::new(None, org.uuid()).uuid();
    let user_uuid = UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid()).uuid();

    // Grant Write-equivalent perms (RCA)
    let grant = ControlPlaneRbacData {
        org_uuid,
        entity_kind: IdKind::Organization.as_str().to_owned(),
        entity_uuid: org_uuid,
        subject_kind: IdKind::User.as_str().to_owned(),
        subject_uuid: user_uuid,
        perms: ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::AUDIT,
    };
    let version_ms = chrono::Utc::now().timestamp_millis();
    db.control_plane_grant(&grant, version_ms, 0i64).await.expect("add RBAC entry");

    // Verify RCA passes
    let result = db
        .control_plane_verify(
            org_uuid,
            IdKind::Organization,
            org_uuid,
            IdKind::User,
            user_uuid,
            ControlPerms::READ | ControlPerms::CONFIGURE | ControlPerms::AUDIT,
        )
        .await
        .expect("verify RCA");
    assert!(result, "user should satisfy RCA");

    // Verify R passes (RCA contains R)
    let result = db
        .control_plane_verify(org_uuid, IdKind::Organization, org_uuid, IdKind::User, user_uuid, ControlPerms::READ)
        .await
        .expect("verify R");
    assert!(result, "user with RCA should satisfy R");

    // Verify G fails (RCA does not contain G)
    let result = db
        .control_plane_verify(org_uuid, IdKind::Organization, org_uuid, IdKind::User, user_uuid, ControlPerms::GRANT)
        .await
        .expect("verify G");
    assert!(!result, "user with RCA should not satisfy GRANT");
}

#[tokio::test]
async fn test_rbac_delete() {
    let db = create_local_database_manager().await;
    let _tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, _tel, "node_rbac_del").await;
    let org = helper_insert_org(&db, _tel, "org_rbac_del", &eden_node).await;
    let user = helper_insert_user(&db, _tel, "rbac_del_user", org.uuid()).await;

    let org_uuid = OrganizationCacheUuid::new(None, org.uuid()).uuid();
    let user_uuid = UserCacheUuid::new(Some(OrganizationCacheUuid::new(None, org.uuid())), user.uuid()).uuid();

    // Grant R then revoke
    let grant = ControlPlaneRbacData {
        org_uuid,
        entity_kind: IdKind::Organization.as_str().to_owned(),
        entity_uuid: org_uuid,
        subject_kind: IdKind::User.as_str().to_owned(),
        subject_uuid: user_uuid,
        perms: ControlPerms::READ,
    };
    let version_ms = chrono::Utc::now().timestamp_millis();
    db.control_plane_grant(&grant, version_ms, 0i64).await.expect("add RBAC entry");

    db.control_plane_revoke(org_uuid, IdKind::Organization, org_uuid, IdKind::User, user_uuid, version_ms + 1, 0i64)
        .await
        .expect("revoke RBAC entry");

    // Verify it's gone
    let perms = db
        .control_plane_get(org_uuid, IdKind::Organization, org_uuid, IdKind::User, user_uuid)
        .await
        .expect("get RBAC after revoke");
    assert!(perms.is_empty(), "RBAC entry should be removed");
}

// ---------------------------------------------------------------------------
// LLM: Skills
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_upsert_skill() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let skill_id = Uuid::new_v4();
    let skill = NewSkill {
        name: "test_skill",
        display_name: "Test Skill",
        description: "A test skill",
        body_markdown: "# Test\nBody content",
        tags: vec!["test".to_string()],
        estimated_tokens: 100,
        source_format: "markdown",
        is_active: true,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: None,
        organization_uuid: None,
    };

    let stored = db.upsert_skill(skill_id, skill, tel).await.expect("upsert skill");
    assert_eq!(stored.name, "test_skill");
    assert_eq!(stored.display_name, "Test Skill");
    assert!(stored.is_active);

    // Verify it can be fetched by name
    let fetched = db.get_skill_by_name("test_skill", tel).await.expect("get skill by name");
    assert!(fetched.is_some(), "skill should be found by name");
    assert_eq!(fetched.expect("skill").id, skill_id);
}

#[tokio::test]
async fn test_update_skill() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let skill_id = Uuid::new_v4();
    let skill = NewSkill {
        name: "skill_update_test",
        display_name: "Original Name",
        description: "original description",
        body_markdown: "# Original",
        tags: vec![],
        estimated_tokens: 50,
        source_format: "markdown",
        is_active: true,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: None,
        organization_uuid: None,
    };
    db.upsert_skill(skill_id, skill, tel).await.expect("upsert skill initial");

    // Update it
    let updated_skill = NewSkill {
        name: "skill_update_test",
        display_name: "Updated Name",
        description: "updated description",
        body_markdown: "# Updated",
        tags: vec!["updated".to_string()],
        estimated_tokens: 200,
        source_format: "markdown",
        is_active: false,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "premium",
        endpoint_kind: None,
        organization_uuid: None,
    };

    let result = db.update_skill(skill_id, None, updated_skill, tel).await.expect("update skill");
    assert!(result.is_some(), "update should return the skill");
    let updated = result.expect("updated skill");
    assert_eq!(updated.display_name, "Updated Name");
    assert_eq!(updated.description, "updated description");
    assert!(!updated.is_active);
}

#[tokio::test]
async fn test_list_active_skills() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    // Insert one active and one inactive
    let active_skill = NewSkill {
        name: "active_skill",
        display_name: "Active",
        description: "active",
        body_markdown: "active",
        tags: vec![],
        estimated_tokens: 10,
        source_format: "markdown",
        is_active: true,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: None,
        organization_uuid: None,
    };
    let inactive_skill = NewSkill {
        name: "inactive_skill",
        display_name: "Inactive",
        description: "inactive",
        body_markdown: "inactive",
        tags: vec![],
        estimated_tokens: 10,
        source_format: "markdown",
        is_active: false,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: None,
        organization_uuid: None,
    };

    db.upsert_skill(Uuid::new_v4(), active_skill, tel).await.expect("insert active skill");
    db.upsert_skill(Uuid::new_v4(), inactive_skill, tel).await.expect("insert inactive skill");

    let active_list = db.list_active_skills(tel).await.expect("list active skills");
    assert_eq!(active_list.len(), 1);
    assert_eq!(active_list[0].name, "active_skill");

    let all_list = db.list_all_skills(tel).await.expect("list all skills");
    assert_eq!(all_list.len(), 2);
}

#[tokio::test]
async fn test_delete_skill() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let skill_id = Uuid::new_v4();
    let skill = NewSkill {
        name: "skill_to_delete",
        display_name: "Delete Me",
        description: "deletable",
        body_markdown: "delete",
        tags: vec![],
        estimated_tokens: 10,
        source_format: "markdown",
        is_active: true,
        source_provider: "local",
        source_repo_url: None,
        source_path: None,
        source_ref: None,
        source_url: None,
        skill_tier: "standard",
        endpoint_kind: None,
        organization_uuid: None,
    };
    db.upsert_skill(skill_id, skill, tel).await.expect("insert skill");

    let deleted = db.delete_skill(skill_id, None, tel).await.expect("delete skill");
    assert!(deleted, "delete should return true");

    let fetched = db.get_skill_by_uuid(skill_id, tel).await.expect("get deleted skill");
    assert!(fetched.is_none(), "skill should be gone after deletion");
}

// ---------------------------------------------------------------------------
// LLM: Credentials
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_insert_credential() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_cred").await;
    let org = helper_insert_org(&db, tel, "org_cred", &eden_node).await;

    let cred = NewLlmCredential {
        id: Uuid::new_v4(),
        organization_uuid: &org.uuid(),
        provider: LlmProvider::OpenAI,
        label: Some("test key"),
        description: Some("a test credential"),
        base_url: None,
        api_key: "sk-test-key-12345",
    };

    let stored = db.insert_llm_credential(cred, tel).await.expect("insert credential");
    assert_eq!(stored.api_key, "sk-test-key-12345");
    assert_eq!(stored.label.as_deref(), Some("test key"));
}

#[tokio::test]
async fn test_fetch_credentials_by_ids() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_cred_fetch").await;
    let org = helper_insert_org(&db, tel, "org_cred_fetch", &eden_node).await;

    let id1 = Uuid::new_v4();
    let id2 = Uuid::new_v4();
    let id3 = Uuid::new_v4();

    for (id, label) in [(id1, "key1"), (id2, "key2"), (id3, "key3")] {
        let cred = NewLlmCredential {
            id,
            organization_uuid: &org.uuid(),
            provider: LlmProvider::Anthropic,
            label: Some(label),
            description: None,
            base_url: None,
            api_key: &format!("sk-{label}"),
        };
        db.insert_llm_credential(cred, tel).await.expect("insert credential");
    }

    // Fetch only two
    let fetched = db.fetch_llm_credentials_by_ids(&org.uuid(), &[id1, id3], tel).await.expect("fetch credentials by ids");
    assert_eq!(fetched.len(), 2, "should return exactly 2 credentials");

    let fetched_ids: Vec<Uuid> = fetched.iter().map(|c| c.id).collect();
    assert!(fetched_ids.contains(&id1));
    assert!(fetched_ids.contains(&id3));
    assert!(!fetched_ids.contains(&id2));
}

#[tokio::test]
async fn test_fetch_credentials_empty_ids() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let org_uuid = OrganizationUuid::new_uuid();
    let fetched = db.fetch_llm_credentials_by_ids(&org_uuid, &[], tel).await.expect("fetch with empty ids");
    assert!(fetched.is_empty(), "empty ID list should return empty vec");
}

// ---------------------------------------------------------------------------
// Eden node: select by ID and UUID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_eden_node_by_id_and_uuid() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let schema = helper_insert_eden_node(&db, tel, "node_select_test").await;

    let by_id: EdenNodeSchema =
        db.select_eden_node_id(&EdenNodeId::new("node_select_test".to_string()), tel).await.expect("select eden node by ID");
    assert_eq!(by_id.uuid(), schema.uuid());

    let by_uuid: EdenNodeSchema = db.select_eden_node_uuid(&schema.uuid(), tel).await.expect("select eden node by UUID");
    assert_eq!(by_uuid.id(), schema.id());
}

// ---------------------------------------------------------------------------
// Organization: select by ID and UUID
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_select_organization_by_id_and_uuid() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_org_select").await;
    let org = helper_insert_org(&db, tel, "org_select_test", &eden_node).await;

    let by_uuid: OrganizationSchema = db.select_organization_uuid(&org.uuid(), tel).await.expect("select organization by UUID");
    assert_eq!(by_uuid.id(), org.id());

    let by_id: OrganizationSchema = db.select_organization_id(&org.id(), tel).await.expect("select organization by ID");
    assert_eq!(by_id.uuid(), org.uuid());
}

// ---------------------------------------------------------------------------
// Multiple users in same organization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_users_same_org() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_multi_user").await;
    let org = helper_insert_org(&db, tel, "org_multi_user", &eden_node).await;

    let _user1 = helper_insert_user(&db, tel, "user_one", org.uuid()).await;
    let _user2 = helper_insert_user(&db, tel, "user_two", org.uuid()).await;

    // Count users in the junction table (regular users only, since
    // InsertAdminUser has a missing transaction commit — see test_insert_admin_user)
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn
        .query("SELECT user_uuid FROM organization_users WHERE organization_uuid = ?1", &[&org.uuid()])
        .await
        .expect("query organization_users");
    assert_eq!(rows.len(), 2, "expected 2 regular users in organization_users");
}

// ---------------------------------------------------------------------------
// Multiple endpoints in same organization
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_multiple_endpoints_same_org() {
    let db = create_local_database_manager().await;
    let tel = &mut test_telemetry();

    let eden_node = helper_insert_eden_node(&db, tel, "node_multi_ep").await;
    let org = helper_insert_org(&db, tel, "org_multi_ep", &eden_node).await;

    for i in 0..3 {
        let config = Box::new(PostgresConfig::default());
        let ep = EndpointSchema::new(EndpointId::new(format!("ep_multi_{i}")), EpKind::Postgres, config, None, None, UserUuid::new_uuid());
        let insert = InsertEndpoint::new(org.uuid(), ep, eden_node.uuid());
        <Db as InsertMethod<EndpointSchema, EndpointCacheUuid, EndpointCacheId, InsertEndpoint>>::insert(&db, insert, tel)
            .await
            .expect("insert endpoint");
    }

    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn
        .query("SELECT endpoint_uuid FROM organization_endpoints WHERE organization_uuid = ?1", &[&org.uuid()])
        .await
        .expect("query organization_endpoints");
    assert_eq!(rows.len(), 3, "expected 3 endpoint links");
}

/// Sanity check: verify that all expected tables are created by
/// `initialize_database_local`.
#[tokio::test]
async fn test_tables_created() {
    let db = create_local_database_manager().await;
    let conn = db.pg_connection().await.expect("pg_connection");
    let rows = conn.query("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name", &[]).await.expect("list tables");
    let tables: Vec<String> = rows.iter().map(|r| r.get::<_, String>("name")).collect();
    for expected in &[
        "organizations",
        "users",
        "eden_nodes",
        "endpoints",
        "rbac_control",
        "rbac_data",
        "llm_skills",
        "llm_credentials",
        "els_policies",
        "org_key_refs",
        "encryption_keys",
    ] {
        assert!(tables.iter().any(|t| t == expected), "table '{}' should exist", expected);
    }
}

// ---------------------------------------------------------------------------
// File-backed Turso integration
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_file_backed_new_local_bootstraps_expected_schema_objects() {
    let db_path = unique_file_backed_db_path("eden_turso_schema");
    let db = open_file_backed_local_db(&db_path, FILE_BACKED_TEST_HEX_KEY).await;
    let conn = db.pg_connection().await.expect("pg_connection");

    let table_rows = conn.query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name", &[]).await.expect("list tables");
    let tables: Vec<String> = table_rows.into_iter().map(|row| row.get("name")).collect();

    let index_rows = conn.query("SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name", &[]).await.expect("list indexes");
    let indexes: Vec<String> = index_rows.into_iter().map(|row| row.get("name")).collect();

    for expected_table in [
        "organizations",
        "users",
        "robots",
        "auths",
        "rbac_control",
        "rbac_data",
        "eden_nodes",
        "endpoints",
        "endpoint_groups",
        "organization_users",
        "organization_admins",
        "organization_endpoints",
        "llm_agents",
        "llm_agent_runs",
        "agent_metrics_hourly",
        "llm_notifications",
        "llm_system_prompts",
        "llm_credentials",
        "llm_user_tools_endpoints",
        "llm_skills",
        "els_policies",
        "els_policy_assignments",
        "els_policy_versions",
        "els_policy_pointers",
        "org_key_refs",
        "encryption_keys",
    ] {
        assert!(
            tables.iter().any(|table| table == expected_table),
            "table '{expected_table}' should exist, got {:?}",
            tables
        );
    }

    for expected_index in [
        "idx_llm_agents_org_status",
        "idx_llm_agent_runs_agent",
        "idx_llm_notifications_user",
        "llm_credentials_org_idx",
        "llm_credentials_org_label_idx",
    ] {
        assert!(
            indexes.iter().any(|index| index == expected_index),
            "index '{expected_index}' should exist, got {:?}",
            indexes
        );
    }
}

#[tokio::test]
async fn test_file_backed_new_local_creates_encrypted_turso_file_at_requested_path() {
    let db_path = unique_file_backed_db_path("eden_turso_header");
    let _db = open_file_backed_local_db(&db_path, FILE_BACKED_TEST_HEX_KEY).await;

    let db_path = Path::new(&db_path);
    assert!(db_path.exists(), "expected database file at {}", db_path.display());

    let header = fs::read(db_path).expect("read database file");
    assert!(header.len() >= SQLITE_HEADER.len(), "database file should include a header");
    assert_ne!(
        &header[..SQLITE_HEADER.len()],
        SQLITE_HEADER,
        "encrypted Turso database should not have a plaintext SQLite header"
    );
    assert_eq!(&header[..5], b"Turso", "expected Turso file header");
}

#[tokio::test]
async fn test_file_backed_new_local_reopens_with_same_key_and_preserves_data() {
    let db_path = unique_file_backed_db_path("eden_turso_reopen");
    let db = open_file_backed_local_db(&db_path, FILE_BACKED_TEST_HEX_KEY).await;
    let conn = db.pg_connection().await.expect("pg_connection");

    let org_id = "integration-org";
    let org_uuid = Uuid::new_v4();
    let description = "Persisted organization".to_string();
    let params: [&(dyn tokio_postgres::types::ToSql + Sync); 3] = [&org_id, &org_uuid, &description];
    conn.execute(
        "INSERT INTO organizations (id, uuid, description, created_at, updated_at) VALUES (?1, ?2, ?3, datetime('now'), datetime('now'))",
        &params,
    )
    .await
    .expect("insert organization");

    drop(conn);
    drop(db);

    let reopened = open_file_backed_local_db(&db_path, FILE_BACKED_TEST_HEX_KEY).await;
    let reopened_conn = reopened.pg_connection().await.expect("pg_connection");
    let row = reopened_conn
        .query_one("SELECT id, description FROM organizations WHERE uuid = ?1", &[&org_uuid])
        .await
        .expect("load organization after reopen");

    let stored_id: String = row.get("id");
    let stored_description: Option<String> = row.get("description");
    assert_eq!(stored_id, org_id);
    assert_eq!(stored_description.as_deref(), Some("Persisted organization"));
}

#[tokio::test]
async fn test_file_backed_new_local_rejects_wrong_hex_key() {
    let db_path = unique_file_backed_db_path("eden_turso_wrong_key");
    let db = open_file_backed_local_db(&db_path, FILE_BACKED_TEST_HEX_KEY).await;
    drop(db);

    let err = match Db::new_local(
        &db_path,
        file_backed_analytics_config(&db_path),
        CacheTtl::from_secs(3600),
        None,
        Some(FILE_BACKED_WRONG_HEX_KEY.to_string()),
    )
    .await
    {
        Ok(_) => panic!("opening with the wrong key should fail"),
        Err(err) => err,
    };

    let message = err.to_string().to_ascii_lowercase();
    assert!(
        message.contains("decryption failed")
            || message.contains("invalid tag")
            || message.contains("file is not a database")
            || message.contains("invalid value of database header magic bytes"),
        "unexpected wrong-key error: {message}"
    );
}

#[tokio::test]
async fn test_file_backed_new_local_rejects_invalid_hex_key() {
    let db_path = unique_file_backed_db_path("eden_turso_invalid_key");
    let err = match Db::new_local(
        &db_path,
        file_backed_analytics_config(&db_path),
        CacheTtl::from_secs(3600),
        None,
        Some("not-a-hex-key".to_string()),
    )
    .await
    {
        Ok(_) => panic!("invalid key should fail before database initialization"),
        Err(err) => err,
    };

    assert!(err.to_string().contains("64-character hexadecimal key"), "unexpected invalid-key error: {err}");
}

// ---------------------------------------------------------------------------
// Dual-backend parity wrappers (Turso side)
//
// Each test below drives a backend-generic assertion in
// [`crate::db::tests_common`]. The Postgres harness
// (`crate::db::tests_postgres`) runs the identical assertions against a real
// Postgres, so the two backends are guaranteed to agree operation-by-operation.
// Keep these in lockstep with the Postgres wrappers.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parity_organization_insert_get() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::organization_insert_get(&db).await;
}

#[tokio::test]
async fn parity_organization_select() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::organization_select(&db).await;
}

#[tokio::test]
async fn parity_organization_delete() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::organization_delete(&db).await;
}

#[tokio::test]
async fn parity_user_insert_get() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::user_insert_get(&db).await;
}

#[tokio::test]
async fn parity_admin_user_insert_get() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::admin_user_insert_get(&db).await;
}

#[tokio::test]
async fn parity_user_delete() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::user_delete(&db).await;
}

#[tokio::test]
async fn parity_eden_node_select() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::eden_node_select(&db).await;
}

#[tokio::test]
async fn parity_multiple_users_same_org() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::multiple_users_same_org(&db).await;
}

#[tokio::test]
async fn parity_analytics_dashboard_prefs_roundtrip() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::analytics_dashboard_prefs_roundtrip(&db).await;
}

#[tokio::test]
async fn parity_llm_skills_roundtrip() {
    let db = create_local_database_manager().await;
    crate::db::tests_common::llm_skills_roundtrip(&db).await;
}
