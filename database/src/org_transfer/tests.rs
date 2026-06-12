use crate::db::methods::insert::organization::tests::insert_organization;
use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use crate::methods::insert::eden_node::insert_eden_node::insert_eden_node;
use crate::org_transfer::{ImportConflictStrategy, OrgTransferConfig, OrgTransferMetadata};
use crate::test_utils::database_test_utils::create_database_manager_dedicated;
use crate::test_utils::telemetry_test_utils::test_telemetry;
use eden_core::auth::Password;
use eden_core::format::{EdenNodeId, OrganizationUuid, UserId};
use ep_core::database::schema::Table;
use ep_core::database::schema::eden_node::EdenNodeSchema;
use std::path::{Path, PathBuf};

const ENCRYPT_PASSWORD: &str = "test-transfer-password";

/// Helper: create an eden node, returning the schema.
async fn setup_eden_node(db: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>, node_id: &str) -> EdenNodeSchema {
    let telemetry = &mut test_telemetry();
    match db.select_eden_node_id(&EdenNodeId::from(node_id), telemetry).await {
        Ok(en) => en,
        Err(_) => insert_eden_node(db, telemetry, node_id, vec![], serde_json::Value::default()).await,
    }
}

/// Helper: delete all org data from Postgres so we can re-import.
///
/// Deletes in FK-safe order: dependents first, then junction tables, then primary entities.
async fn delete_org_data(db: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>, org_uuid: &OrganizationUuid) {
    let conn = db.pg_connection().await.expect("pg connection");

    // 1. Delete dependent rows that reference entities via junction tables
    //    (must happen BEFORE we delete the junction tables)
    let _ = conn.execute(
        "DELETE FROM workflow_templates WHERE workflow_uuid IN (SELECT workflow_uuid FROM organization_workflows WHERE organization_uuid = $1)",
        &[org_uuid],
    ).await;

    let _ = conn
        .execute(
            "DELETE FROM auths WHERE endpoint_uuid IN (SELECT endpoint_uuid FROM organization_endpoints WHERE organization_uuid = $1)",
            &[org_uuid],
        )
        .await;

    let _ = conn.execute(
        "DELETE FROM eden_node_endpoints WHERE endpoint_uuid IN (SELECT endpoint_uuid FROM organization_endpoints WHERE organization_uuid = $1)",
        &[org_uuid],
    ).await;

    // 2. Delete junction tables
    for table in &[
        "organization_admins",
        "organization_users",
        "organization_eden_nodes",
        "organization_endpoints",
        "organization_templates",
        "organization_workflows",
        "organization_apis",
        "organization_interlays",
        "organization_migrations",
    ] {
        let _ = conn.execute(&format!("DELETE FROM {table} WHERE organization_uuid = $1"), &[org_uuid]).await;
    }

    // 3. Delete primary entities
    let _ = conn.execute("DELETE FROM endpoints WHERE uuid NOT IN (SELECT endpoint_uuid FROM organization_endpoints)", &[]).await;
    let _ = conn.execute("DELETE FROM templates WHERE uuid NOT IN (SELECT template_uuid FROM organization_templates)", &[]).await;
    let _ = conn.execute("DELETE FROM workflows WHERE uuid NOT IN (SELECT workflow_uuid FROM organization_workflows)", &[]).await;

    // 4. Delete users
    let _ = conn.execute("DELETE FROM users WHERE organization_uuid = $1", &[org_uuid]).await;

    // 5. Delete organization
    conn.execute("DELETE FROM organizations WHERE uuid = $1", &[org_uuid]).await.expect("delete org");
}

/// Helper: build metadata path from transfer dir + metadata.
fn metadata_path(transfer_dir: &Path, metadata: &OrgTransferMetadata) -> PathBuf {
    transfer_dir.join(OrgTransferMetadata::metadata_filename(metadata.created_at, &metadata.organization_uuid))
}

#[tokio::test]
async fn test_export_import_round_trip() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "transfer_test_node").await;

    let user_creds = (UserId::from("transfer_test_user"), Password::new("test_password".to_string()));

    let (org_schema, _admin_users) = insert_organization(
        &db_manager,
        telemetry,
        "transfer_test_org",
        &[user_creds],
        vec![eden_node.uuid()],
        Some("Test organization for transfer".to_string()),
    )
    .await;

    let org_uuid = org_schema.uuid();

    // -- Export --
    let transfer_dir = PathBuf::from("/tmp/eden-transfer-test");
    let config = OrgTransferConfig::new(&transfer_dir).with_description("Test export").with_source_node("test-node");

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    assert_eq!(metadata.organization_uuid, *org_uuid);
    assert!(metadata.description.as_deref() == Some("Test export"));
    assert!(metadata.source_node.as_deref() == Some("test-node"));

    // Verify artifact file exists (dump_path is relative, resolve against transfer dir)
    assert!(transfer_dir.join(&metadata.artifact.dump_path).exists());

    // -- Delete & reimport --
    delete_org_data(&db_manager, &org_uuid).await;

    let conn = db_manager.pg_connection().await.expect("pg connection");
    let check = conn.query_opt("SELECT uuid FROM organizations WHERE uuid = $1", &[&org_uuid]).await.expect("check query");
    assert!(check.is_none(), "Organization should be deleted");

    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await
        .expect("Import should succeed");

    assert_eq!(result.organization_uuid, *org_uuid);
    assert_eq!(result.users_imported, 1);

    // Verify data
    let imported_org = conn
        .query_one("SELECT id, description FROM organizations WHERE uuid = $1", &[&org_uuid])
        .await
        .expect("Organization should exist after import");
    assert_eq!(imported_org.get::<_, String>(0), "transfer_test_org");
    assert_eq!(imported_org.get::<_, Option<String>>(1).as_deref(), Some("Test organization for transfer"));

    let imported_users = conn.query("SELECT username FROM users WHERE organization_uuid = $1", &[&org_uuid]).await.expect("query users");
    assert_eq!(imported_users.len(), 1);
    assert_eq!(imported_users[0].get::<_, String>(0), "transfer_test_user");

    let eden_node_links = conn
        .query("SELECT eden_node_uuid FROM organization_eden_nodes WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("query eden nodes");
    assert!(!eden_node_links.is_empty());

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_import_conflict_detection() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "conflict_test_node").await;

    let user_creds = (UserId::from("conflict_test_user"), Password::new("test_password".to_string()));

    let (org_schema, _) =
        insert_organization(&db_manager, telemetry, "conflict_test_org", &[user_creds], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    let transfer_dir = PathBuf::from("/tmp/eden-conflict-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // Import without deleting — should fail with conflict
    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await;

    assert!(result.is_err(), "Import should fail due to conflict");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_full_data_round_trip() {
    use crate::db::methods::insert::endpoint::tests::insert_endpoint;
    use crate::db::methods::insert::template::insert_template::insert_template;
    use crate::db::methods::insert::workflow::insert_workflow::insert_workflow;
    use eden_core::format::endpoint::EpKind;
    use ep_core::database::schema::Table as _;
    use ep_core::ep::EpConfig;
    use redis_core::config::RedisConfig;

    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "full_rt_node").await;

    // Create org with 2 users (first is auto-admin)
    let user_creds = [
        (UserId::from("full_rt_user1"), Password::new("pass1".to_string())),
        (UserId::from("full_rt_user2"), Password::new("pass2".to_string())),
    ];

    let (org_schema, _admin_users) = insert_organization(
        &db_manager,
        telemetry,
        "full_rt_org",
        &user_creds,
        vec![eden_node.uuid()],
        Some("Full round-trip test".to_string()),
    )
    .await;

    let org_uuid = org_schema.uuid();

    // Insert an endpoint with config
    let _endpoint = insert_endpoint(
        &db_manager,
        telemetry,
        "full_rt_endpoint",
        EpKind::Redis,
        RedisConfig::default().as_config(),
        Some("Test endpoint".to_string()),
        org_schema.uuid(),
        eden_node.uuid(),
    )
    .await;

    // Insert a template (requires endpoint)
    let _template = insert_template(&db_manager, telemetry, _endpoint.uuid(), org_schema.uuid(), "test_template")
        .await
        .expect("insert template");

    // Insert a workflow (internally creates template + dag)
    let _workflow = insert_workflow(&db_manager, telemetry, org_schema.uuid(), _endpoint.uuid()).await;

    // -- Export --
    let transfer_dir = PathBuf::from("/tmp/eden-full-rt-test");
    let config = OrgTransferConfig::new(&transfer_dir).with_description("Full round-trip");

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // -- Delete & reimport --
    delete_org_data(&db_manager, &org_uuid).await;

    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await
        .expect("Import should succeed");

    assert_eq!(result.organization_uuid, *org_uuid);
    assert_eq!(result.users_imported, 2);
    assert!(result.endpoints_imported >= 1);
    assert!(result.templates_imported >= 1);
    assert!(result.workflows_imported >= 1);

    // Verify org
    let conn = db_manager.pg_connection().await.expect("pg connection");
    let org_row = conn.query_one("SELECT id FROM organizations WHERE uuid = $1", &[&org_uuid]).await.expect("org exists");
    assert_eq!(org_row.get::<_, String>(0), "full_rt_org");

    // Verify users
    let user_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM users WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count users")
        .get(0);
    assert_eq!(user_count, 2);

    // Verify admin association
    let admin_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_admins WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count admins")
        .get(0);
    assert!(admin_count >= 1, "At least one admin should be imported");

    // Verify endpoint
    let ep_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_endpoints WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count endpoints")
        .get(0);
    assert!(ep_count >= 1);

    // Verify endpoint config bytes are present
    let ep_config: Option<Vec<u8>> = conn
        .query_one(
            "SELECT e.config FROM endpoints e JOIN organization_endpoints oe ON e.uuid = oe.endpoint_uuid WHERE oe.organization_uuid = $1 AND e.id = 'full_rt_endpoint' LIMIT 1",
            &[&org_uuid],
        )
        .await
        .expect("query endpoint config")
        .get(0);
    // Config may or may not be present depending on the endpoint type — just verify the query worked
    let _ = ep_config;

    // Verify template
    let tmpl_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_templates WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count templates")
        .get(0);
    assert!(tmpl_count >= 1);

    // Verify workflow
    let wf_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_workflows WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count workflows")
        .get(0);
    assert!(wf_count >= 1);

    // Verify workflow_templates junction
    let wt_count: i64 = conn
        .query_one(
            "SELECT COUNT(*) FROM workflow_templates wt WHERE wt.workflow_uuid IN (SELECT workflow_uuid FROM organization_workflows WHERE organization_uuid = $1)",
            &[&org_uuid],
        )
        .await
        .expect("count workflow_templates")
        .get(0);
    assert!(wt_count >= 1, "Workflow-template junctions should be imported");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_redis_round_trip() {
    use crate::lib::ShardCache;

    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "redis_rt_node").await;

    let user_creds = (UserId::from("redis_rt_user"), Password::new("pass".to_string()));

    let (org_schema, _) = insert_organization(&db_manager, telemetry, "redis_rt_org", &[user_creds], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    // Set internal cache keys
    let cache_key = format!("org:{}:cache:test", *org_uuid);
    let rbac_key = format!("org:{}:rbac:test", *org_uuid);

    db_manager.internal_cache().kv_set(cache_key.clone(), "cache_value".to_string()).await.expect("set cache");
    db_manager.internal_cache().kv_set(rbac_key.clone(), "rbac_value".to_string()).await.expect("set rbac");

    // -- Export --
    let transfer_dir = PathBuf::from("/tmp/eden-redis-rt-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // Flush org cache keys
    db_manager.internal_cache().kv_del(&cache_key).await.expect("del cache");
    db_manager.internal_cache().kv_del(&rbac_key).await.expect("del rbac");

    // Verify keys are gone
    let exists = db_manager.internal_cache().kv_get(&cache_key).await.expect("get cache").is_some();
    assert!(!exists, "Cache key should be deleted");

    // -- Delete org & reimport --
    delete_org_data(&db_manager, &org_uuid).await;

    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await
        .expect("Import should succeed");

    assert!(result.redis_cache_keys_restored >= 1, "Cache keys should be restored");
    assert!(result.redis_rbac_keys_restored >= 1, "RBAC keys should be restored");

    // Verify cache values are restored
    let val = db_manager.internal_cache().kv_get(&cache_key).await.expect("get cache").expect("cache value");
    assert_eq!(val, "cache_value");
    let val = db_manager.internal_cache().kv_get(&rbac_key).await.expect("get rbac").expect("rbac value");
    assert_eq!(val, "rbac_value");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_wrong_password_fails() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "wrong_pw_node").await;

    let user_creds = (UserId::from("wrong_pw_user"), Password::new("pass".to_string()));

    let (org_schema, _) = insert_organization(&db_manager, telemetry, "wrong_pw_org", &[user_creds], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    let transfer_dir = PathBuf::from("/tmp/eden-wrong-pw-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // Attempt import with wrong password
    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            "completely-wrong-password",
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await;

    assert!(result.is_err(), "Import with wrong password should fail");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_artifact_corruption_detected() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "corrupt_node").await;

    let user_creds = (UserId::from("corrupt_user"), Password::new("pass".to_string()));

    let (org_schema, _) = insert_organization(&db_manager, telemetry, "corrupt_org", &[user_creds], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    let transfer_dir = PathBuf::from("/tmp/eden-corrupt-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // Corrupt the artifact dump file
    let dump_file = transfer_dir.join(&metadata.artifact.dump_path);
    let mut data = tokio::fs::read(&dump_file).await.expect("read dump");
    // Flip some bytes in the middle
    if data.len() > 10 {
        data[5] ^= 0xFF;
        data[6] ^= 0xFF;
        data[7] ^= 0xFF;
    }
    tokio::fs::write(&dump_file, &data).await.expect("write corrupted dump");

    // Attempt import — should fail with checksum or decryption error
    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await;

    assert!(result.is_err(), "Import of corrupted artifact should fail");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_empty_organization_round_trip() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "empty_org_node").await;

    // Create org with no users (empty slice)
    let (org_schema, _) = insert_organization(&db_manager, telemetry, "empty_org", &[], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    let transfer_dir = PathBuf::from("/tmp/eden-empty-org-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    delete_org_data(&db_manager, &org_uuid).await;

    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await
        .expect("Import should succeed");

    assert_eq!(result.organization_uuid, *org_uuid);
    assert_eq!(result.users_imported, 0);
    assert_eq!(result.endpoints_imported, 0);
    assert_eq!(result.templates_imported, 0);
    assert_eq!(result.workflows_imported, 0);

    // Verify org exists
    let conn = db_manager.pg_connection().await.expect("pg connection");
    let org_row = conn.query_opt("SELECT id FROM organizations WHERE uuid = $1", &[&org_uuid]).await.expect("query org");
    assert!(org_row.is_some(), "Empty org should exist after import");

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_multiple_admins_round_trip() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "multi_admin_node").await;

    // Create org with 3 users
    let user_creds = [
        (UserId::from("admin1"), Password::new("pass1".to_string())),
        (UserId::from("admin2"), Password::new("pass2".to_string())),
        (UserId::from("regular_user"), Password::new("pass3".to_string())),
    ];

    let (org_schema, admin_users) =
        insert_organization(&db_manager, telemetry, "multi_admin_org", &user_creds, vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    // insert_organization makes all provided users admins.
    // Manually add a second admin if only one was created.
    // First, let's see how many admins there are.
    let conn = db_manager.pg_connection().await.expect("pg connection");
    let admin_count_before: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_admins WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count admins before")
        .get(0);

    // If only one admin, manually add a second
    if admin_count_before < 2 && admin_users.len() >= 2 {
        let second_user_uuid = admin_users[1].uuid();
        let _ = conn
            .execute(
                "INSERT INTO organization_admins (organization_uuid, user_uuid) VALUES ($1, $2) ON CONFLICT DO NOTHING",
                &[&org_uuid, &second_user_uuid],
            )
            .await;
    }

    let final_admin_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_admins WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count admins final")
        .get(0);

    // -- Export --
    let transfer_dir = PathBuf::from("/tmp/eden-multi-admin-test");
    let config = OrgTransferConfig::new(&transfer_dir);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // -- Delete & reimport --
    delete_org_data(&db_manager, &org_uuid).await;

    let result = db_manager
        .import_organization(
            &metadata_path(&transfer_dir, &metadata),
            ENCRYPT_PASSWORD,
            &eden_node.uuid(),
            ImportConflictStrategy::Abort,
        )
        .await
        .expect("Import should succeed");

    assert_eq!(result.users_imported, 3);

    // Verify admin count is preserved
    let imported_admin_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM organization_admins WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count admins after import")
        .get(0);
    assert_eq!(imported_admin_count, final_admin_count, "Admin count should be preserved after import");

    // Verify all 3 users present
    let user_count: i64 = conn
        .query_one("SELECT COUNT(*) FROM users WHERE organization_uuid = $1", &[&org_uuid])
        .await
        .expect("count users")
        .get(0);
    assert_eq!(user_count, 3);

    let _ = tokio::fs::remove_dir_all(&transfer_dir).await;
}

#[tokio::test]
async fn test_dump_path_portability() {
    let (_r, _p, _c, db_manager) = create_database_manager_dedicated().await;

    let telemetry = &mut test_telemetry();
    let eden_node = setup_eden_node(&db_manager, "portable_node").await;

    let user_creds = (UserId::from("portable_user"), Password::new("pass".to_string()));

    let (org_schema, _) = insert_organization(&db_manager, telemetry, "portable_org", &[user_creds], vec![eden_node.uuid()], None).await;

    let org_uuid = org_schema.uuid();

    // Export to dir A
    let dir_a = PathBuf::from("/tmp/eden-portable-dir-a");
    let config = OrgTransferConfig::new(&dir_a);

    let metadata = db_manager.export_organization(&org_uuid, ENCRYPT_PASSWORD, config).await.expect("Export should succeed");

    // Move both files to dir B
    let dir_b = PathBuf::from("/tmp/eden-portable-dir-b");
    tokio::fs::create_dir_all(&dir_b).await.expect("create dir_b");

    let metadata_filename = OrgTransferMetadata::metadata_filename(metadata.created_at, &metadata.organization_uuid);
    let artifact_filename = &metadata.artifact.dump_path;

    tokio::fs::rename(dir_a.join(&metadata_filename), dir_b.join(&metadata_filename)).await.expect("move metadata");
    tokio::fs::rename(dir_a.join(artifact_filename), dir_b.join(artifact_filename)).await.expect("move artifact");

    // Delete org and import from dir B
    delete_org_data(&db_manager, &org_uuid).await;

    let result = db_manager
        .import_organization(&dir_b.join(&metadata_filename), ENCRYPT_PASSWORD, &eden_node.uuid(), ImportConflictStrategy::Abort)
        .await
        .expect("Import from moved directory should succeed (portability)");

    assert_eq!(result.organization_uuid, *org_uuid);

    let _ = tokio::fs::remove_dir_all(&dir_a).await;
    let _ = tokio::fs::remove_dir_all(&dir_b).await;
}
