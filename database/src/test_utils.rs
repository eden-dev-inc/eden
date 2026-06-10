#[cfg(any(test, feature = "test-utils"))]
pub mod redis_test_utils {
    use std::time::Instant;
    use testcontainers_modules::testcontainers::ContainerAsync;
    use testcontainers_modules::testcontainers::core::{CmdWaitFor, ExecCommand};

    pub async fn wait_for_redis_ready<I: testcontainers_modules::testcontainers::Image>(container: &ContainerAsync<I>) {
        println!("Waiting for Redis to be ready...");
        let t0 = Instant::now();
        container
            .exec(ExecCommand::new(["redis-cli", "ping"]).with_cmd_ready_condition(CmdWaitFor::message_on_stdout("PONG")))
            .await
            .expect("Redis not ready");
        println!("Redis ready: {} ms", t0.elapsed().as_millis());
    }
}

#[cfg(any(test, feature = "test-utils"))]
pub mod clickhouse_test_utils {
    use std::time::{Duration, Instant};
    use testcontainers_modules::clickhouse::ClickHouse;
    use testcontainers_modules::testcontainers::ContainerAsync;
    use testcontainers_modules::testcontainers::ImageExt;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    #[cfg(any(test, feature = "test-utils"))]
    use reqwest;

    /// Start a ClickHouse container with retry logic for slow startup scenarios.
    /// CI can occasionally hit Docker startup contention late in the suite even
    /// when tests are serialized, so allow a wider per-attempt budget and a few
    /// more retries before failing the whole test.
    pub async fn start_clickhouse_with_retry() -> ContainerAsync<ClickHouse> {
        let mut start_attempts = 0;
        let max_start_attempts = 5;

        loop {
            start_attempts += 1;
            match tokio::time::timeout(
                Duration::from_secs(180),
                ClickHouse::default().with_startup_timeout(Duration::from_secs(150)).start(),
            )
            .await
            {
                Ok(Ok(c)) => {
                    return c;
                }
                Ok(Err(e)) => {
                    if start_attempts < max_start_attempts {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    } else {
                        panic!("Failed to start ClickHouse after {} attempts: {}", max_start_attempts, e);
                    }
                }
                Err(_) => {
                    if start_attempts < max_start_attempts {
                        tokio::time::sleep(Duration::from_secs(10)).await;
                    } else {
                        panic!("ClickHouse container failed to start within 180s timeout after {} attempts", max_start_attempts);
                    }
                }
            }
        }
    }

    /// Wait for ClickHouse to be ready by pinging the /ping endpoint.
    /// Uses exponential backoff starting at 50ms, max 120 retries (~60 seconds total).
    #[cfg(any(test, feature = "test-utils"))]
    pub async fn wait_for_clickhouse_ready(url: &str) -> Result<(), Box<dyn std::error::Error>> {
        println!("Waiting for ClickHouse to respond to /ping...");
        let t0 = Instant::now();
        let client = reqwest::Client::new();
        let max_retries = 120;
        let mut retry_delay = Duration::from_millis(50);

        for attempt in 0..max_retries {
            match client.get(url).timeout(Duration::from_secs(2)).send().await {
                Ok(resp) if resp.status().is_success() => {
                    println!("ClickHouse /ping succeeded: {} ms (attempt {})", t0.elapsed().as_millis(), attempt + 1);
                    return Ok(());
                }
                _ => {
                    if attempt < max_retries - 1 {
                        tokio::time::sleep(retry_delay).await;
                        // Exponential backoff: 50ms -> 100ms -> 200ms -> 500ms max
                        retry_delay = std::cmp::min(retry_delay.mul_f32(2.0), Duration::from_millis(500));
                    }
                }
            }
        }

        Err(format!(
            "ClickHouse failed to respond to /ping after {} ms ({} attempts)",
            t0.elapsed().as_millis(),
            max_retries
        )
        .into())
    }
}

#[cfg(any(test, feature = "test-utils"))]
#[cfg_attr(embedded_db, path = "test_utils/database_test_utils_embedded_db.rs")]
pub mod database_test_utils;

#[cfg(any(test, feature = "test-utils"))]
pub mod telemetry_test_utils {
    use eden_core::format::EdenNodeUuid;
    use eden_core::telemetry::labels::TelemetryLabels;
    use eden_core::telemetry::{TelemetryDurations, TelemetryWrapper, setup_metrics};
    use std::sync::Arc;

    pub fn test_telemetry() -> TelemetryWrapper {
        TelemetryWrapper::new(
            Arc::new(setup_metrics("http://localhost:4317", "").expect("Failed to setup metrics")),
            TelemetryLabels::new(&EdenNodeUuid::new_uuid()),
            TelemetryDurations::default(),
        )
    }
}

#[cfg(all(any(test, feature = "test-utils"), embedded_db))]
pub mod embedded_db_test_utils {
    use crate::db::duckdb_analytics::DuckDbAnalyticsConfig;
    use crate::db::encryption::{KEY_SIZE, OrgKeyProvider, decrypt_with_key, encrypt_with_key};
    use crate::db::lib::{CacheTtl, DatabaseManager, RedisConn};
    use crate::db::turso::TursoPool;
    use crate::lib::ClickhouseConn;
    use eden_core::auth::Jwt;
    use eden_core::error::ResultEP;
    use std::collections::BTreeSet;
    use std::path::{Path, PathBuf};
    use std::sync::{Arc, Mutex, OnceLock};

    const TEST_DB_ENCRYPTION_KEY: &str = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    fn registered_local_databases() -> &'static Mutex<BTreeSet<PathBuf>> {
        static REGISTERED_LOCAL_DATABASES: OnceLock<Mutex<BTreeSet<PathBuf>>> = OnceLock::new();
        REGISTERED_LOCAL_DATABASES.get_or_init(|| Mutex::new(BTreeSet::new()))
    }

    fn local_database_artifact_paths(db_path: &Path) -> [PathBuf; 4] {
        [
            db_path.to_path_buf(),
            PathBuf::from(format!("{}-wal", db_path.display())),
            PathBuf::from(format!("{}-shm", db_path.display())),
            PathBuf::from(format!("{}-journal", db_path.display())),
        ]
    }

    fn remove_local_database_artifacts(db_path: &Path) -> bool {
        let mut removed_cleanly = true;
        for artifact_path in local_database_artifact_paths(db_path) {
            if let Err(e) = std::fs::remove_file(&artifact_path)
                && e.kind() != std::io::ErrorKind::NotFound
            {
                removed_cleanly = false;
                eprintln!("Failed to remove local test database artifact {}: {}", artifact_path.display(), e);
            }
        }
        removed_cleanly
    }

    pub fn register_test_db_path(db_path: &str) {
        registered_local_databases().lock().expect("registered local databases lock").insert(PathBuf::from(db_path));
    }

    pub fn cleanup_test_db_path(db_path: &str) {
        let db_path = PathBuf::from(db_path);
        for attempt in 1..=10 {
            if remove_local_database_artifacts(&db_path) {
                registered_local_databases().lock().expect("registered local databases lock").remove(&db_path);
                return;
            }

            if attempt < 10 {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
        }

        eprintln!("Local test database cleanup will be retried at process shutdown: {}", db_path.display());
    }

    #[cfg(any(test, feature = "test-utils"))]
    #[ctor::dtor]
    fn cleanup_registered_local_databases() {
        let registered_paths = {
            let mut registered = registered_local_databases().lock().expect("registered local databases lock");
            std::mem::take(&mut *registered)
        };

        for db_path in registered_paths {
            for attempt in 1..=10 {
                if remove_local_database_artifacts(&db_path) {
                    break;
                }

                if attempt < 10 {
                    std::thread::sleep(std::time::Duration::from_millis(50));
                }
            }
        }
    }

    struct FixedTestOrgKeyProvider;

    #[async_trait::async_trait]
    impl OrgKeyProvider for FixedTestOrgKeyProvider {
        async fn wrap(&self, _key_ref: &str, plaintext: &[u8]) -> ResultEP<Vec<u8>> {
            let key = [0x11_u8; KEY_SIZE];
            encrypt_with_key(&key, plaintext)
        }

        async fn unwrap(&self, _key_ref: &str, ciphertext: &[u8]) -> ResultEP<Vec<u8>> {
            let key = [0x11_u8; KEY_SIZE];
            decrypt_with_key(&key, ciphertext)
        }

        fn provider_name(&self) -> &'static str {
            "test"
        }
    }

    /// Create a [`DatabaseManager`] backed by a temporary file-backed SQLite database.
    ///
    /// Each call produces an isolated database (unique temp file) so tests
    /// running concurrently cannot interfere with one another.
    ///
    /// Note: in-memory mode (`:memory:`) does not work for tests because
    /// Turso shared-cache in-memory databases do not share schema across
    /// connections.  File-backed databases avoid this limitation.
    pub async fn create_local_database_manager() -> DatabaseManager<RedisConn, TursoPool, ClickhouseConn> {
        let db_path = format!("/tmp/eden_test_{}.db", uuid::Uuid::new_v4());
        create_local_database_manager_at_path(&db_path, None).await
    }

    /// Create a file-backed embedded-db database at a caller-provided path.
    ///
    /// This lets multi-threaded integration tests reopen the same control-plane
    /// database file from both the server thread and the test thread.
    pub async fn create_local_database_manager_at_path(
        db_path: &str,
        jwt: Option<Jwt>,
    ) -> DatabaseManager<RedisConn, TursoPool, ClickhouseConn> {
        let analytics_config = duckdb_test_config(db_path);
        let db = DatabaseManager::<RedisConn, TursoPool, ClickhouseConn>::new_local(
            db_path,
            analytics_config,
            CacheTtl::from_secs(3600),
            jwt,
            Some(TEST_DB_ENCRYPTION_KEY.to_string()),
        )
        .await
        .expect("failed to create local db")
        .with_org_key_provider(Arc::new(FixedTestOrgKeyProvider));

        register_test_db_path(db_path);
        db
    }

    fn duckdb_test_config(db_path: &str) -> DuckDbAnalyticsConfig {
        DuckDbAnalyticsConfig {
            path: PathBuf::from(format!("{db_path}.duckdb")),
            memory_limit: "512MB".to_string(),
            temp_directory: PathBuf::from(format!("{db_path}.duckdb.tmp")),
            max_temp_directory_size: "2GB".to_string(),
            checkpoint_threshold: "64MB".to_string(),
            checkpoint_interval_secs: 60,
            analytics_retention_days: 30,
            logs_retention_days: 14,
            traces_retention_days: 14,
        }
    }
}

#[cfg(all(test, feature = "infra-tests"))]
#[allow(dead_code)]
pub(crate) mod organization_test_utils {
    use crate::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use crate::methods::insert::InsertMethod;
    use crate::methods::insert::eden_node::InsertEdenNode;
    use crate::methods::insert::organization::InsertOrganization;
    use crate::methods::insert::user::InsertUser;
    use eden_core::auth::Password;
    use eden_core::format::cache_id::{EdenNodeCacheId, OrganizationCacheId, UserCacheId};
    use eden_core::format::cache_uuid::{EdenNodeCacheUuid, OrganizationCacheUuid, UserCacheUuid};
    use eden_core::format::{EdenNodeId, EdenNodeUuid, EndpointUuid, OrganizationId, UserId};
    use eden_core::telemetry::TelemetryWrapper;
    use ep_core::database::schema::Table;
    use ep_core::database::schema::eden_node::EdenNodeSchema;
    use ep_core::database::schema::organization::OrganizationSchema;
    use ep_core::database::schema::user::UserSchema;
    use tokio::sync::OnceCell;

    static SHARED_ORGANIZATION: OnceCell<(UserSchema, EdenNodeSchema, OrganizationSchema)> = OnceCell::const_new();

    async fn insert_eden_node(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        eden_node_id: &str,
        endpoint_uuids: Vec<EndpointUuid>,
        info: serde_json::Value,
    ) -> EdenNodeSchema {
        let eden_node_uuid = EdenNodeUuid::new_uuid();
        let eden_node_schema = EdenNodeSchema::new(eden_node_id.to_string(), eden_node_uuid, endpoint_uuids, info);
        let insert_eden_node = InsertEdenNode::new(eden_node_schema.clone());

        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EdenNodeSchema,
            EdenNodeCacheUuid,
            EdenNodeCacheId,
            InsertEdenNode,
        >>::insert(db_manager, insert_eden_node, test_telemetry)
        .await
        .expect("Failed to insert eden node");

        eden_node_schema
    }

    async fn insert_organization(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        organization_id: &str,
        admin_usernames_and_passwords: &[(UserId, Password)],
        eden_node_uuids: Vec<EdenNodeUuid>,
        description: Option<String>,
    ) -> (OrganizationSchema, Vec<UserSchema>) {
        let mut eden_node_pairs = Vec::with_capacity(eden_node_uuids.len());
        for eden_node_uuid in &eden_node_uuids {
            let eden_node_schema: EdenNodeSchema =
                db_manager.select_eden_node_uuid(eden_node_uuid, test_telemetry).await.expect("Failed to fetch eden node by UUID");
            eden_node_pairs.push((eden_node_schema.id(), eden_node_uuid.clone()));
        }

        let mut organization_schema = OrganizationSchema::new(organization_id.to_string(), None, eden_node_pairs, description);
        let mut admin_users = Vec::with_capacity(admin_usernames_and_passwords.len());

        for (user_id, password) in admin_usernames_and_passwords {
            let user_schema = UserSchema::new(user_id.clone(), password.clone(), organization_schema.uuid(), None, None, None);
            organization_schema.add_user(user_schema.id(), user_schema.uuid());
            organization_schema.add_super_admin(user_schema.id(), user_schema.uuid());
            admin_users.push(user_schema);
        }

        let insert_organization = InsertOrganization::new(organization_schema.clone());
        <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            OrganizationSchema,
            OrganizationCacheUuid,
            OrganizationCacheId,
            InsertOrganization,
        >>::insert(db_manager, insert_organization, test_telemetry)
        .await
        .expect("Failed to insert organization");

        for user_schema in &admin_users {
            let insert_user = InsertUser::new(user_schema.clone());
            <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
                UserSchema,
                UserCacheUuid,
                UserCacheId,
                InsertUser,
            >>::insert(db_manager, insert_user, test_telemetry)
            .await
            .expect("Failed to insert admin user");
        }

        (organization_schema, admin_users)
    }

    pub(crate) async fn initialize_organization(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
        // create organization so we can test creating new endpoints
        let eden_node_schema = match db_manager.select_eden_node_id(&EdenNodeId::from("eden_node_test"), test_telemetry).await {
            Ok(en) => en,
            Err(_) => insert_eden_node(db_manager, test_telemetry, "eden_node_test", vec![], serde_json::Value::default()).await,
        };

        let organization_id: OrganizationId = "test_organization".into();

        let admin_user_name_and_password = (UserId::from("username"), Password::new("password".to_string()));

        let (organization_schema, admin_users) = insert_organization(
            db_manager,
            test_telemetry,
            &organization_id,
            std::slice::from_ref(&admin_user_name_and_password),
            vec![eden_node_schema.uuid()],
            None,
        )
        .await;

        let admin_user_schema = admin_users.first().cloned().expect("Failed to create admin user for organization");

        (admin_user_schema.clone(), eden_node_schema.clone(), organization_schema)
    }

    pub(crate) async fn shared_initialized_organization(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
    ) -> (UserSchema, EdenNodeSchema, OrganizationSchema) {
        SHARED_ORGANIZATION.get_or_init(|| async { initialize_organization(db_manager, test_telemetry).await }).await.clone()
    }
}
