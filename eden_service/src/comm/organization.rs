pub mod delete;
pub mod get;
pub mod patch;
pub mod post;
pub mod rate_limit;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod tests {
    use crate::EdenDb;
    use crate::comm::organization::delete::delete_organization;
    use crate::comm::organization::get::get_organization;
    use crate::comm::organization::patch::update_organization;
    use crate::comm::organization::post::insert_organization;
    use crate::test_utils::database_test_utils::create_database_manager;
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use database::cache::CacheIdFunctions;
    use database::db::cache::CacheUuidFunctions;
    use database::db::lib::{ClickhouseConn, PgConn, RedisConn};
    use database::internal_cache::RateBucketState;
    use database::lib::ShardCache;
    use database::methods::delete::DeleteMethod;
    use database::methods::delete::organization::DeleteOrganization;
    use database::methods::insert::organization::InsertOrganization;
    use database::methods::update::UpdateActor;
    use eden_core::format::cache_id::CacheId;
    use eden_core::format::cache_id::OrganizationCacheId;
    use eden_core::format::{CacheObjectType, EdenId, OrganizationCacheUuid, OrganizationId, OrganizationUuid};
    use endpoint_core::ep_core::database::schema::Table;
    use endpoint_core::ep_core::database::schema::organization::{OrganizationSchema, RateLimitSettings, UpdateOrganizationSchema};
    use serial_test::serial;

    #[tokio::test]
    #[serial]
    async fn organization_crud_test() {
        // start containers
        let db_manager = create_database_manager().await;

        let test_telemetry = &mut test_telemetry();

        let organization_id: OrganizationId = "test_org".into();

        let org_schema = OrganizationSchema::new(organization_id.id(), None, vec![], None);

        // Run same insert organization function as post
        assert!(insert_organization(&db_manager, InsertOrganization::new(org_schema.clone()), test_telemetry,).await.is_ok());

        // Get from Cache w/ UUID
        let cached_org = get_organization(
            &db_manager,
            &CacheObjectType::new(Some(OrganizationCacheUuid::from(org_schema.uuid())), None),
            test_telemetry,
        )
        .await
        .expect("Failed to get organization from Cache");

        assert_eq!(cached_org.id(), org_schema.id());
        assert_eq!(cached_org.uuid(), org_schema.uuid());
        assert_eq!(cached_org.description(), org_schema.description());

        // Get from Cache w/ ID
        let cached_org_id = get_organization(
            &db_manager,
            &CacheObjectType::new(None, Some(OrganizationCacheId::new(None, org_schema.id()))),
            test_telemetry,
        )
        .await
        .expect("Failed to get organization from Cache");

        assert_eq!(cached_org_id.id(), org_schema.id());
        assert_eq!(cached_org_id.uuid(), org_schema.uuid());
        assert_eq!(cached_org_id.description(), org_schema.description());

        // Get from Postgres w/ UUID
        let pg_org = <EdenDb as CacheUuidFunctions<OrganizationSchema, OrganizationCacheUuid>>::get_from_database(
            &db_manager,
            &OrganizationCacheUuid::from(org_schema.uuid()),
            test_telemetry,
        )
        .await
        .expect("Failed to get organization from Postgres");

        assert_eq!(pg_org.id(), org_schema.id());
        assert_eq!(pg_org.uuid(), org_schema.uuid());
        assert_eq!(pg_org.description(), org_schema.description());

        // Get from Postgres w/ ID
        let pg_org_id = <EdenDb as CacheIdFunctions<OrganizationSchema, OrganizationCacheId>>::get_from_database(
            &db_manager,
            &OrganizationCacheId::new(None, org_schema.id()),
            test_telemetry,
        )
        .await
        .expect("Failed to get organization from Postgres");

        assert_eq!(pg_org_id.id(), org_schema.id());
        assert_eq!(pg_org_id.uuid(), org_schema.uuid());
        assert_eq!(pg_org_id.description(), org_schema.description());

        let new_description = "new description for test org".to_string();

        // update organization (only update description, not ID)
        update_organization(
            &db_manager,
            &CacheObjectType::new(None, Some(OrganizationCacheId::new(None, org_schema.id()))),
            UpdateActor::System("infra-test"),
            test_telemetry,
            UpdateOrganizationSchema::new(None, Some(new_description.clone()), None),
        )
        .await
        .expect("Failed to update organization");

        let updated_schema = get_organization(
            &db_manager,
            &CacheObjectType::new(Some(OrganizationCacheUuid::from(org_schema.uuid())), None),
            test_telemetry,
        )
        .await
        .expect("Failed to get organization from Cache");

        assert_eq!(updated_schema.id(), org_schema.id());
        assert_eq!(updated_schema.description().unwrap_or_default().to_string(), new_description);

        // Delete organization
        let org_cache_uuid = OrganizationCacheUuid::from(org_schema.uuid());
        let org_cache_id = OrganizationCacheId::new(None, org_schema.id());

        let delete_result = delete_organization(
            &db_manager,
            test_telemetry,
            &<DeleteOrganization as DeleteMethod<
                OrganizationSchema,
                OrganizationCacheUuid,
                OrganizationUuid,
                OrganizationCacheId,
                OrganizationId,
                RedisConn,
                PgConn,
                ClickhouseConn,
            >>::new(CacheObjectType::new(Some(org_cache_uuid), Some(org_cache_id))),
        )
        .await;

        assert!(delete_result.is_ok(), "Delete failed: {:?}", delete_result.err());
    }

    #[tokio::test]
    #[serial]
    async fn rate_limit_integration_test() {
        use actix_web::body::MessageBody;
        use actix_web::dev::{ServiceRequest, ServiceResponse};
        use actix_web::middleware::{Next, from_fn};
        use actix_web::web::Data;
        use actix_web::{App, Error, HttpMessage, HttpResponse, test, web};
        use eden_core::auth::ParsedJwt;
        use eden_core::comm::NodeData;
        use eden_core::format::cache_uuid::CacheUuid;
        use eden_core::format::{EdenNodeId, EdenNodeUuid, UserUuid};
        use eden_core::telemetry::setup_metrics;

        use crate::middleware::org_rate_limit::org_rate_limit;

        // ── 1. Start containers and create org ──────────────────────────────
        let db_manager = create_database_manager().await;
        let test_telemetry = &mut test_telemetry();

        let organization_id: OrganizationId = "rate_limit_test_org".into();
        let org_schema = OrganizationSchema::new(organization_id.id(), None, vec![], None);

        insert_organization(&db_manager, InsertOrganization::new(org_schema.clone()), test_telemetry)
            .await
            .expect("Failed to insert organization");

        let org_cache_object = CacheObjectType::new(Some(OrganizationCacheUuid::new(None, org_schema.uuid())), None);

        // ── 2. Verify org starts with no rate limit settings ────────────────
        let cached_org = get_organization(&db_manager, &org_cache_object, test_telemetry).await.expect("Failed to get organization");
        assert_eq!(cached_org.rate_limit_settings(), None);

        // ── 3. Update org with rate limit settings (enabled) ────────────────
        let settings = RateLimitSettings {
            enabled: true,
            bandwidth_ingress_limit_bytes: Some(1_000_000),
            bandwidth_egress_limit_bytes: Some(500_000),
            token_ingress_limit: None,
            token_egress_limit: None,
        };

        update_organization(
            &db_manager,
            &org_cache_object,
            UpdateActor::System("infra-test"),
            test_telemetry,
            UpdateOrganizationSchema::new(None, None, Some(settings.clone())),
        )
        .await
        .expect("Failed to update rate limit settings");

        // ── 4. Verify settings persisted in cache ───────────────────────────
        let cached_org =
            get_organization(&db_manager, &org_cache_object, test_telemetry).await.expect("Failed to get organization after update");
        let persisted = cached_org.rate_limit_settings().expect("rate_limit_settings should be Some");
        assert!(persisted.enabled);
        assert_eq!(persisted.bandwidth_ingress_limit_bytes, Some(1_000_000));
        assert_eq!(persisted.bandwidth_egress_limit_bytes, Some(500_000));

        // ── 5. Pre-populate bandwidth buckets to simulate partial usage ────────
        let org_uuid_str = org_schema.uuid().to_string();
        let now_ts = chrono::Utc::now().timestamp();
        let bw_ingress_key = crate::rate_limiter::token_bucket_key(&org_uuid_str, "bandwidth_ingress");
        let bw_egress_key = crate::rate_limiter::token_bucket_key(&org_uuid_str, "bandwidth_egress");
        {
            // 900_000 remaining ≡ 100_000 already consumed of 1_000_000 limit.
            db_manager
                .internal_cache()
                .rate_bucket_set(&bw_ingress_key, RateBucketState { tokens: 900_000.0, last: now_ts, consumed: 100_000 })
                .await
                .expect("Failed to set ingress bucket");
            // 450_000 remaining ≡ 50_000 already consumed of 500_000 limit.
            db_manager
                .internal_cache()
                .rate_bucket_set(&bw_egress_key, RateBucketState { tokens: 450_000.0, last: now_ts, consumed: 50_000 })
                .await
                .expect("Failed to set egress bucket");
        }

        // ── 6. Verify BandwidthStatus remaining-bytes computation ───────────
        let ingress_limit = persisted.bandwidth_ingress_limit_bytes.expect("ingress limit");
        let ingress_used = 100_000u64;
        assert_eq!(ingress_limit.saturating_sub(ingress_used), 900_000);

        let egress_limit = persisted.bandwidth_egress_limit_bytes.expect("egress limit");
        let egress_used = 50_000u64;
        assert_eq!(egress_limit.saturating_sub(egress_used), 450_000);

        // ── 7. Middleware enforcement: under limit → 200 ────────────────────
        let db_data = Data::new(db_manager);
        let metrics_data = Data::new(setup_metrics("http://localhost:4317", "").expect("metrics"));
        let node_data = Data::new(NodeData::new(EdenNodeId::from("test_node"), EdenNodeUuid::new_uuid()));

        let parsed_jwt = ParsedJwt::new("test_user".into(), UserUuid::new_uuid(), org_schema.id(), org_schema.uuid());
        let jwt_data = Data::new(parsed_jwt);

        // Middleware that injects ParsedJwt from app_data into request extensions,
        // simulating what the bearer auth middleware does in production.
        async fn inject_jwt(req: ServiceRequest, next: Next<impl MessageBody>) -> Result<ServiceResponse<impl MessageBody>, Error> {
            if let Some(jwt) = req.app_data::<Data<ParsedJwt>>() {
                req.extensions_mut().insert(jwt.get_ref().clone());
            }
            next.call(req).await
        }

        let app = test::init_service(
            App::new()
                .app_data(db_data.clone())
                .app_data(metrics_data.clone())
                .app_data(node_data.clone())
                .app_data(jwt_data.clone())
                .service(
                    web::scope("/test").wrap(from_fn(org_rate_limit)).wrap(from_fn(inject_jwt)).route("", web::get().to(HttpResponse::Ok)),
                ),
        )
        .await;
        let req = test::TestRequest::get().uri("/test").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), actix_web::http::StatusCode::OK, "expected 200 when usage is under limit");

        // ── 8. Middleware enforcement: over limit → 429 ─────────────────────
        // Drain the bandwidth_ingress bucket to 0 so the next request is blocked.
        {
            db_data
                .internal_cache()
                .rate_bucket_set(&bw_ingress_key, RateBucketState { tokens: 0.0, last: now_ts, consumed: 1_000_000 })
                .await
                .expect("Failed to zero ingress bucket");
        }

        let req = test::TestRequest::get().uri("/test").to_request();
        let resp = test::try_call_service(&app, req).await;
        // The middleware returns Err(InternalError) for 429, so we expect an error.
        let err = resp.expect_err("expected error when ingress exceeds limit");
        let err_resp = err.error_response();
        assert_eq!(
            err_resp.status(),
            actix_web::http::StatusCode::TOO_MANY_REQUESTS,
            "expected 429 when ingress exceeds limit"
        );

        // Verify X-RateLimit-Limit header is present
        let limit_header = err_resp.headers().get("X-RateLimit-Limit").expect("missing X-RateLimit-Limit header");
        assert_eq!(limit_header.to_str().expect("header value"), "1000000");

        // ── 9. Middleware: disabled settings → pass through ─────────────────
        // Reset usage to over-limit but disable rate limiting
        let disabled_settings = RateLimitSettings {
            enabled: false,
            bandwidth_ingress_limit_bytes: Some(1_000_000),
            bandwidth_egress_limit_bytes: Some(500_000),
            token_ingress_limit: None,
            token_egress_limit: None,
        };
        update_organization(
            &db_data,
            &org_cache_object,
            UpdateActor::System("infra-test"),
            test_telemetry,
            UpdateOrganizationSchema::new(None, None, Some(disabled_settings)),
        )
        .await
        .expect("Failed to disable rate limiting");

        let req = test::TestRequest::get().uri("/test").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            actix_web::http::StatusCode::OK,
            "expected 200 when rate limiting is disabled, even with usage over limit"
        );
    }
}
