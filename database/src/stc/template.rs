use crate::cache::CacheFunctions;
use crate::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use eden_core::error::EpError;
use eden_core::format::cache_id::TemplateCacheId;
use eden_core::format::cache_uuid::TemplateCacheUuid;
use eden_core::format::{CacheObjectType, CacheUuid, OrganizationCacheUuid, OrganizationUuid, TemplateId, TemplateUuid};
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{ctx_with_trace, log_debug, log_trace};
use ep_core::database::schema::endpoint::{EndpointRequestInput, EndpointTransactionInput};
use ep_core::database::schema::template::TemplateSchema;
use ep_core::database::template::TemplateFields;
use ep_core::database::template::registry::TemplateRegistry;
pub use ep_core::database::template::{EndpointRequestTemplate, EndpointTransactionTemplate, TemplateKind, TemplateOutput};
use function_name::named;

impl<R, P, C> DatabaseManager<R, P, C> {
    /// Get `Template` from Registry, if missing replace from Redis
    #[named]
    pub async fn restore_template(
        &self,
        template_registry: &TemplateRegistry,
        org_uuid: &OrganizationUuid,
        template_uuid: TemplateUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<TemplateSchema, EpError>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        let cache_key = CacheObjectType::<TemplateCacheUuid, TemplateCacheId>::new(
            Some(TemplateCacheUuid::new(Some(OrganizationCacheUuid::from(org_uuid.to_owned())), template_uuid)),
            None,
        );

        // span.add_event(
        //     "generate template cache key",
        //     vec![FastSpanAttribute::new("Template Cache Key", cache_key.to_string())],
        // );

        let template_schema: TemplateSchema = <DatabaseManager<R, P, C> as CacheFunctions<
            TemplateSchema,
            TemplateCacheUuid,
            TemplateUuid,
            TemplateCacheId,
            TemplateId,
        >>::get_from_cache(self, &cache_key, telemetry_wrapper)
        .await?;

        template_registry.insert(template_schema.clone(), telemetry_wrapper).await?;

        Ok(template_schema)
    }

    /// Render the template with the input fields. The template is pulled from the handlebars
    /// registry, with the fields updated within the runtime values. The output is then serialized
    /// into a `TemplateKind` structure, which can then be sent to the engine.
    #[named]
    pub async fn render_template(
        &self,
        template_registry: &TemplateRegistry,
        template_uuid: &TemplateUuid,
        org_uuid: &OrganizationUuid,
        data: &TemplateFields,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<TemplateOutput, EpError>
    where
        R: EdenRedisConnection + Sync,
        P: EdenPostgresConnection + Sync,
        C: EdenClickhouseConnection + Sync,
    {
        let _ctx = ctx_with_trace!()
            .with_feature("database")
            .with_organization_uuid(org_uuid.to_string())
            .with_additional("template_uuid", template_uuid.to_string());

        log_debug!(_ctx.clone(), "Rendering template", audience = eden_logger_internal::LogAudience::Internal);
        let _span = telemetry_wrapper.client_tracer(format!("cache.{}", function_name!()));
        log_trace!(
            _ctx.clone(),
            "Rendering template",
            audience = eden_logger_internal::LogAudience::Internal,
            template_uuid = template_uuid.to_string()
        );

        let json_template = match template_registry.get(template_uuid, telemetry_wrapper).await? {
            Some(template) => template,
            None => self
                .restore_template(template_registry, org_uuid, template_uuid.clone(), telemetry_wrapper)
                .await?
                .template()
                .to_owned(),
        };

        let request = json_template.render(data)?;

        Ok(match json_template.kind() {
            TemplateKind::Read => {
                log_debug!(_ctx, "Template Read", audience = eden_logger_internal::LogAudience::Internal);
                TemplateOutput::Read(EndpointRequestTemplate::new(
                    json_template.endpoint_uuid().clone(),
                    EndpointRequestInput::new(request),
                ))
            }
            TemplateKind::Write => {
                log_debug!(_ctx, "Template Write", audience = eden_logger_internal::LogAudience::Internal);
                TemplateOutput::Write(EndpointRequestTemplate::new(
                    json_template.endpoint_uuid().clone(),
                    EndpointRequestInput::new(request),
                ))
            }
            TemplateKind::Transaction => {
                log_debug!(_ctx, "Template Transaction", audience = eden_logger_internal::LogAudience::Internal);
                TemplateOutput::Transaction(EndpointTransactionTemplate::new(
                    json_template.endpoint_uuid().clone(),
                    EndpointTransactionInput::new(request),
                ))
            }
            TemplateKind::TwoPhaseTransaction => {
                log_debug!(_ctx, "Template TwoPhaseTransaction", audience = eden_logger_internal::LogAudience::Internal);
                TemplateOutput::TwoPhaseTransaction(EndpointTransactionTemplate::new(
                    json_template.endpoint_uuid().clone(),
                    EndpointTransactionInput::new(request),
                ))
            }
        })
    }
}
