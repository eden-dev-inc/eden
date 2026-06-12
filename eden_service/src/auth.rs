use crate::EdenDb;
use crate::comm::auth::login::{check_subject_rbac_access, check_user_rbac_access};
use crate::comm::lib::get_org_from_header;
use actix_web::web::Data;
use actix_web::{HttpMessage, dev::ServiceRequest, error::Error, web};
use actix_web_httpauth::extractors::{basic::BasicAuth, bearer::BearerAuth};
use database::db::cache::CacheFunctions;
use eden_core::auth::{JwToken, ParsedJwt};
use eden_core::format::cache_id::{CacheId, OrganizationCacheId, UserCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::{CacheObjectType, EdenId, EdenNodeUuid, OrganizationId, OrganizationUuid, UserId, UserUuid};
use eden_core::request::ServerData;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, MetadataMapWrapper, TelemetryDurations, TelemetryWrapper, TraceContext};
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_info};
use endpoint_core::ep_core::database::schema::organization::OrganizationSchema;
use endpoint_core::ep_core::database::schema::user::UserSchema;
use function_name::named;
use std::borrow::Cow;

#[named]
pub async fn basic_auth_validator(
    req: ServiceRequest,    // service request
    credentials: BasicAuth, // auth credentials
) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let headers = req.headers().clone();
    let eden_node_uuid = if let Some(service_data) = req.app_data::<Data<ServerData>>() {
        service_data.public_key.clone()
    } else {
        EdenNodeUuid::new_uuid()
    };

    // Preserve existing telemetry data from MetricsMiddleware instead of creating new ones
    let labels = req.extensions().get::<TelemetryLabels>().cloned().unwrap_or_else(|| TelemetryLabels::new(&eden_node_uuid));

    let durations = req.extensions().get::<TelemetryDurations>().cloned().unwrap_or_default();

    let metadata = req.extensions().get::<MetadataMapWrapper>().cloned().unwrap_or_default();

    let telemetry_wrapper = &mut match req.app_data::<Data<AllMetrics>>() {
        Some(metrics) => TelemetryWrapper::new_with_telemetry(
            TraceContext::from(metadata.metadata().clone()),
            metrics.clone().into_inner(),
            labels,
            durations,
        ),
        None => {
            return Err((actix_web::error::ErrorInternalServerError("missing metrics data"), req));
        }
    };

    let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

    let _ctx = ctx_with_trace!().with_feature("auth").with_eden_node_uuid(eden_node_uuid.to_string());

    log_debug!(
        _ctx.clone(),
        "Auth validator called",
        audience = LogAudience::Internal,
        path = req.path(),
        uri = req.uri().to_string(),
        method = req.method().to_string()
    );

    span.add_event(
        "running basic auth",
        vec![
            FastSpanAttribute::new("Auth validator called for path", req.path().to_string()),
            FastSpanAttribute::new("URI", req.uri().to_string()),
            FastSpanAttribute::new("Method", req.method().to_string()),
        ],
    );

    let database = match req.app_data::<web::Data<EdenDb>>() {
        Some(database) => database,
        None => {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned("database connection failed".to_owned()) });
            return Err((actix_web::error::ErrorInternalServerError("database connection failed"), req));
        }
    };

    let user_id = UserId::new(credentials.user_id().to_string());
    let password = match credentials.password() {
        Some(password) => password.to_string(),
        None => {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned("password not provided".to_owned()) });
            return Err((actix_web::error::ErrorBadRequest("password not provided"), req));
        }
    };

    span.add_event("received login credentials", vec![FastSpanAttribute::new("username", user_id.to_string())]);

    let org_cache_object = match get_org_from_header(&headers) {
        Ok(object) => object,
        Err(e) => {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            return Err((actix_web::error::ErrorInternalServerError(e), req));
        }
    };

    span.add_event(
        "collecting org_uuid",
        vec![FastSpanAttribute::new("org cache object", format!("{:?}", org_cache_object))],
    );

    let org_uuid = match <EdenDb as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_uuid(database, &org_cache_object, telemetry_wrapper)
    .await
    {
        Ok(uuid) => {
            span.add_event("collected org_uuid", vec![FastSpanAttribute::new("uuid", uuid.to_string())]);
            uuid
        }
        Err(e) => {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            return Err((actix_web::error::ErrorBadRequest(e), req));
        }
    };

    span.add_event(
        "collecting org_id",
        vec![FastSpanAttribute::new("org cache object", format!("{:?}", org_cache_object))],
    );

    let org_id = match <EdenDb as CacheFunctions<
        OrganizationSchema,
        OrganizationCacheUuid,
        OrganizationUuid,
        OrganizationCacheId,
        OrganizationId,
    >>::get_id(database, &org_cache_object, telemetry_wrapper)
    .await
    {
        Ok(id) => {
            span.add_event("collected org_id", vec![FastSpanAttribute::new("id", id.to_string())]);
            id
        }
        Err(e) => {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            return Err((actix_web::error::ErrorBadRequest(e), req));
        }
    };

    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let user_cache_object = CacheObjectType::new(None, Some(UserCacheId::new(Some(org_key.clone()), UserId::new(user_id.to_string()))));

    span.add_event(
        "collecting user ids",
        vec![FastSpanAttribute::new("user cache object", format!("{:?}", user_cache_object))],
    );

    let (user_id, user_uuid) = match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_id_and_uuid(
        database,
        &user_cache_object,
        telemetry_wrapper,
    )
    .await
    {
        Ok((id, uuid)) => {
            span.add_event(
                "collected user ids",
                vec![
                    FastSpanAttribute::new("id", id.to_string()),
                    FastSpanAttribute::new("uuid", uuid.to_string()),
                ],
            );

            (id, uuid)
        }
        Err(e) => {
            return Err((actix_web::error::ErrorUnauthorized(e), req));
        }
    };

    let auth = ParsedJwt::new(user_id, user_uuid, org_id, org_uuid);

    // Add JWT fields as span attributes to the current span
    span.set_attribute("org.id", auth.org_id().to_string());
    span.set_attribute("org.uuid", auth.org_uuid().to_string());
    span.set_attribute("user.id", auth.user_id().to_string());
    span.set_attribute("user.uuid", auth.user_uuid().to_string());

    // Add JWT fields to metadata BEFORE calling database functions
    // so child spans created by database operations will have JWT attributes
    let metadata = telemetry_wrapper.context_mut().metadata_mut();
    if let Ok(v) = auth.org_id().as_str().parse() {
        let _ = metadata.insert("org-id", v);
    }
    if let Ok(v) = auth.org_uuid().to_string().parse() {
        let _ = metadata.insert("org-uuid", v);
    }
    if let Ok(v) = auth.user_id().as_str().parse() {
        let _ = metadata.insert("user-id", v);
    }
    if let Ok(v) = auth.user_uuid().to_string().parse() {
        let _ = metadata.insert("user-uuid", v);
    }

    span.add_event("parsed JWT from input", vec![FastSpanAttribute::new("jwt", format!("{:?}", auth))]);

    match database.verify_auth(&user_cache_object, password, telemetry_wrapper).await {
        Ok(bool) => match bool {
            true => {
                // Check if the user still has RBAC access in the organization
                // If they were deleted, they will have no RBAC entry for this organization
                let user_cache_uuid =
                    match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
                        database,
                        &user_cache_object,
                        telemetry_wrapper,
                    )
                    .await
                    {
                        Ok(uuid) => uuid,
                        Err(e) => {
                            log::error!("Failed to get user cache UUID: {}", e);
                            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                            return Err((actix_web::error::ErrorUnauthorized(e), req));
                        }
                    };

                // Check if the user still has RBAC access in the organization
                if let Err(e) = check_user_rbac_access(database, &user_cache_uuid, &org_key, telemetry_wrapper, &mut span).await {
                    return Err((e, req));
                }

                // Update telemetry labels with JWT fields before inserting into extensions
                telemetry_wrapper.mut_labels(|labels| {
                    labels.set_jwt(auth.clone());
                });

                let mut extensions = req.extensions_mut();

                extensions.insert(auth);
                extensions.insert(telemetry_wrapper.labels().clone());
                extensions.insert(telemetry_wrapper.durations().clone());
                extensions.insert(telemetry_wrapper.metadata_map().clone());

                drop(extensions);

                log_info!(_ctx.clone(), "Basic auth success", audience = LogAudience::Internal);
                span.add_event("auth success!", vec![FastSpanAttribute::new("success", "true")]);
                Ok(req)
            }
            false => {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned("incorrect email and password".to_owned()),
                });
                Err((actix_web::error::ErrorUnauthorized("incorrect email and password"), req))
            }
        },
        Err(e) => {
            log_error!(_ctx, "Basic auth failed", audience = LogAudience::Internal, error = e.to_string());
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });

            Err((actix_web::error::ErrorUnauthorized(e), req))
        }
    }
}

#[named]
pub async fn bearer_auth_validator(mut req: ServiceRequest, credentials: BearerAuth) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let eden_node_uuid = if let Some(service_data) = req.app_data::<Data<ServerData>>() {
        service_data.public_key.clone()
    } else {
        EdenNodeUuid::new_uuid()
    };

    // Preserve existing telemetry data from MetricsMiddleware instead of creating new ones
    let labels = req.extensions().get::<TelemetryLabels>().cloned().unwrap_or_else(|| TelemetryLabels::new(&eden_node_uuid));

    let durations = req.extensions().get::<TelemetryDurations>().cloned().unwrap_or_default();

    let metadata = req.extensions().get::<MetadataMapWrapper>().cloned().unwrap_or_default();

    let mut telemetry_wrapper_value = match req.app_data::<Data<AllMetrics>>() {
        Some(metrics) => TelemetryWrapper::new_with_telemetry(
            TraceContext::from(metadata.metadata().clone()),
            metrics.clone().into_inner(),
            labels,
            durations,
        ),
        None => {
            return Err((actix_web::error::ErrorInternalServerError("missing metrics data"), req));
        }
    };

    let telemetry_wrapper = &mut telemetry_wrapper_value;

    let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
    span.set_attribute("eden_node_uuid", eden_node_uuid.to_string());

    let _ctx = ctx_with_trace!().with_feature("auth").with_eden_node_uuid(eden_node_uuid.to_string());

    log_debug!(_ctx.clone(), "Bearer auth started", audience = LogAudience::Internal);
    span.add_simple_event("bearer auth started");
    match req.extract::<web::Data<EdenDb>>().await {
        Ok(db) => match db.validate_token(&JwToken::from(credentials.token())) {
            Ok(auth) => {
                // Check if user's tokens are blacklisted (all sessions revoked)
                if crate::jwt_blacklist::is_blacklisted(&**db, &auth.org_uuid().to_string(), &auth.user_uuid().to_string()).await {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned("session has been revoked".to_owned()) });
                    return Err((actix_web::error::ErrorUnauthorized("Session has been revoked. Please log in again."), req));
                }

                // Check if this specific token (jti) is blacklisted (other sessions revoked)
                if let Some(jti) = auth.jti() {
                    if crate::jwt_blacklist::is_jti_blacklisted(&**db, jti).await {
                        span.set_status(FastSpanStatus::Error { message: Cow::Owned("token has been revoked".to_owned()) });
                        return Err((actix_web::error::ErrorUnauthorized("Session has been revoked. Please log in again."), req));
                    }
                }

                if let Err(e) = check_subject_rbac_access(&db, &auth, telemetry_wrapper, &mut span).await {
                    return Err((e, req));
                }

                // Update telemetry labels with JWT fields before inserting into extensions
                telemetry_wrapper.mut_labels(|labels| {
                    labels.set_jwt(auth.clone());
                });

                // Add JWT fields as span attributes to the current span
                span.set_attribute("org.id", auth.org_id().to_string());
                span.set_attribute("org.uuid", auth.org_uuid().to_string());
                span.set_attribute("user.id", auth.user_id().to_string());
                span.set_attribute("user.uuid", auth.user_uuid().to_string());

                // Add JWT fields to metadata so they appear in span attributes
                let metadata = telemetry_wrapper.context_mut().metadata_mut();
                if let Ok(v) = auth.org_id().as_str().parse() {
                    let _ = metadata.insert("org-id", v);
                }
                if let Ok(v) = auth.org_uuid().to_string().parse() {
                    let _ = metadata.insert("org-uuid", v);
                }
                if let Ok(v) = auth.user_id().as_str().parse() {
                    let _ = metadata.insert("user-id", v);
                }
                if let Ok(v) = auth.user_uuid().to_string().parse() {
                    let _ = metadata.insert("user-uuid", v);
                }

                // Record session for activity tracking
                let mut extensions = req.extensions_mut();

                extensions.insert(auth);
                extensions.insert(telemetry_wrapper.labels().clone());
                extensions.insert(telemetry_wrapper.durations().clone());
                extensions.insert(telemetry_wrapper.metadata_map().clone());

                drop(extensions);

                log_info!(_ctx, "Bearer auth passed", audience = LogAudience::Internal);
                span.add_simple_event("bearer auth passed");
                Ok(req)
            }
            Err(e) => {
                span.set_status(FastSpanStatus::Error {
                    message: Cow::Owned(format!("invalid authentication: {}", e)),
                });
                // user-friendly API key error message
                let error_message = eden_core::error::EpError::invalid_api_key();
                Err((actix_web::error::ErrorUnauthorized(error_message), req))
            }
        },
        Err(e) => {
            span.set_status(FastSpanStatus::Error {
                message: Cow::Owned(format!("could not collect bearer token: {}", e)),
            });
            Err((actix_web::error::ErrorBadRequest(format!("could not collect bearer token: {}", e)), req))
        }
    }
}

#[named]
pub async fn org_token_validator(req: ServiceRequest, token: BearerAuth) -> Result<ServiceRequest, (Error, ServiceRequest)> {
    let server_data = match req.app_data::<Data<ServerData>>() {
        Some(server_data) => server_data,
        None => {
            return Err((actix_web::error::ErrorInternalServerError("ServerData must be included internally"), req));
        }
    };

    // Preserve existing telemetry data from MetricsMiddleware instead of creating new ones
    let labels = req.extensions().get::<TelemetryLabels>().cloned().unwrap_or_else(|| TelemetryLabels::new(&server_data.public_key));

    let durations = req.extensions().get::<TelemetryDurations>().cloned().unwrap_or_default();

    let metadata = req.extensions().get::<MetadataMapWrapper>().cloned().unwrap_or_default();

    let mut telemetry_wrapper_value = match req.app_data::<Data<AllMetrics>>() {
        Some(metrics) => TelemetryWrapper::new_with_telemetry(
            TraceContext::from(metadata.metadata().clone()),
            metrics.clone().into_inner(),
            labels,
            durations,
        ),
        None => {
            return Err((actix_web::error::ErrorInternalServerError("missing metrics data during org token validation"), req));
        }
    };

    let telemetry_wrapper = &mut telemetry_wrapper_value;

    let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());

    let _ctx = ctx_with_trace!().with_feature("auth");

    log_debug!(_ctx.clone(), "Organization token auth started", audience = LogAudience::Internal);
    span.add_simple_event("org token auth started");

    if Some(token.token()) == server_data.new_org_token.as_deref() {
        let telemetry_wrapper = telemetry_wrapper.clone();

        let mut extensions = req.extensions_mut();

        extensions.insert(telemetry_wrapper.labels().clone());
        extensions.insert(telemetry_wrapper.durations().clone());
        extensions.insert(telemetry_wrapper.metadata_map().clone());

        drop(extensions);

        log_info!(_ctx, "Organization token auth passed", audience = LogAudience::Internal);
        span.add_simple_event("org token auth passed");
        Ok(req)
    } else {
        let error_message = if server_data.new_org_token.is_none() {
            "server has not set up org token, no valid org token possible"
        } else {
            "invalid org token"
        };
        Err((actix_web::error::ErrorUnauthorized(error_message), req))
    }
}
