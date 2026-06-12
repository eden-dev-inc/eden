use crate::database::cache::TemplateCache;
use crate::database::schema::template::TemplateSchema;
use crate::database::template::handlebars::HandlebarsCache as HandlebarsRegistry;
use crate::database::template::{JsonTemplate, TemplateFields, TemplateKind};
use dashmap::DashMap;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use format::{EndpointUuid, TemplateId, TemplateUuid};
use function_name::named;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use telemetry::TelemetryWrapper;

/// Enhanced wrapper struct for Template registry with Handlebars optimization
#[derive(Debug, Clone)]
pub struct TemplateRegistry {
    /// Registry for storing JsonTemplate objects by UUID
    pub registry: Arc<DashMap<TemplateUuid, JsonTemplate>>,
    /// Optimized Handlebars registry for template compilation caching
    handlebars_cache: Arc<RwLock<HandlebarsRegistry>>,
}

impl Default for TemplateRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TemplateRegistry {
    pub fn new() -> Self {
        Self {
            registry: Arc::new(DashMap::new()),
            handlebars_cache: Arc::new(RwLock::new(HandlebarsRegistry::new())),
        }
    }

    /// Insert a new template with optimized compilation caching
    #[named]
    pub async fn insert(&self, template: TemplateSchema, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        let template_uuid = template.template_uuid().clone();

        span.set_attribute("UUID", template_uuid.to_string());
        span.add_simple_event("register template");

        // Create JsonTemplate with cached Handlebars compilation
        let json_template = self
            .create_optimized_json_template(
                template.template().endpoint_uuid().clone(),
                template.template().kind().clone(),
                template.template().handlebars().template().clone(), // Get the raw template value
                template.template().ep_kind(),
                &template_uuid,
                template.template().cache().clone(),
            )
            .await?;

        // Store in registry
        self.registry.insert(template_uuid, json_template);

        Ok(())
    }

    /// Update existing template with optimized recompilation
    #[named]
    pub async fn update(
        &self,
        template_id: TemplateId,
        template_uuid: TemplateUuid,
        template: JsonTemplate,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        {
            let mut cache = self.handlebars_cache.write().map_err(|e| EpError::template(e.to_string()))?;
            cache.register(template_id, template_uuid.clone(), template.handlebars().template().clone())?;
        }

        // Update main registry
        self.registry.insert(template_uuid, template);
        Ok(())
    }

    /// Create JsonTemplate with cached Handlebars compilation
    async fn create_optimized_json_template(
        &self,
        endpoint_uuid: EndpointUuid,
        kind: TemplateKind,
        template_value: Value,
        ep_kind: EpKind,
        template_uuid: &TemplateUuid,
        cache: Option<TemplateCache>,
    ) -> ResultEP<JsonTemplate> {
        // Get or compile the Handlebars template with caching
        let handlebars = {
            let mut cache = self.handlebars_cache.write().map_err(|e| EpError::template(e.to_string()))?;
            cache.get_uuid_or_compile(Some(template_uuid), &template_value)?
        };

        // Create JsonTemplate with the cached Handlebars
        Ok(JsonTemplate::new_with_cached_handlebars(
            endpoint_uuid,
            kind,
            (*handlebars).clone(), // Dereference the Arc
            ep_kind,
            cache,
        ))
    }

    /// High-performance rendering using cached compilation
    #[named]
    pub async fn render_optimized(
        &self,
        template_uuid: &TemplateUuid,
        values: &TemplateFields,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<Value>> {
        let mut span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        span.set_attribute("UUID", template_uuid.to_string());
        span.set_attribute("field_count", values.map().len() as i64);
        span.add_simple_event("render template optimized");

        // Get cached Handlebars directly (avoiding JsonTemplate lookup)
        let result = {
            let mut cache = self.handlebars_cache.write().map_err(|e| EpError::template(e.to_string()))?;

            // Check if we have this template in main registry first
            let exists = self.registry.contains_key(template_uuid);
            if !exists {
                return Ok(None);
            }

            // Get the template value for cache lookup
            let template_value = {
                if let Some(json_template) = self.registry.get(template_uuid) {
                    json_template.handlebars().template().clone()
                } else {
                    return Ok(None);
                }
            };

            // Get or compile with cache
            let handlebars = cache.get_uuid_or_compile(Some(template_uuid), &template_value)?;

            // Render with optimized method
            handlebars.render(values)?
        };

        span.set_attribute("result_size", result.to_string().len() as i64);
        span.add_simple_event("template rendered");

        Ok(Some(result))
    }

    /// Standard methods (unchanged for backward compatibility)
    #[named]
    pub async fn get(&self, template_uuid: &TemplateUuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<JsonTemplate>> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        Ok(self.registry.get(template_uuid).map(|template| template.value().clone()))
    }

    #[named]
    pub async fn remove(&self, uuid: &TemplateUuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        self.registry.remove(uuid);

        Ok(())
    }

    #[named]
    pub async fn contains(&self, uuid: &TemplateUuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<bool> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        Ok(self.registry.contains_key(uuid))
    }

    /// Performance monitoring
    #[named]
    pub async fn get_performance_stats(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<PerformanceStats> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        let template_count = self.registry.len();

        Ok(PerformanceStats {
            total_templates: template_count,
            // Add cache hit rate, average render time, etc. as needed
        })
    }

    /// Clear compilation cache to free memory
    #[named]
    pub async fn clear_cache(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        span.add_simple_event("clearing handlebars cache");

        self.handlebars_cache.write().map_err(|e| EpError::template(e.to_string()))?.clear_cache();
        Ok(())
    }

    /// Get cache statistics for monitoring
    #[named]
    pub async fn get_cache_stats(&self, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<CacheStats> {
        let _span = telemetry_wrapper.client_tracer(format!("template.{}", function_name!()));

        let template_count = self.registry.len();
        let cache_size = self.handlebars_cache.read().map_err(|e| EpError::template(e.to_string()))?.cache_size();

        Ok(CacheStats {
            total_templates: template_count,
            cached_compilations: cache_size,
        })
    }
}

#[derive(Debug, Clone)]
pub struct PerformanceStats {
    pub total_templates: usize,
    // Add more metrics as needed
}

#[derive(Debug, Clone)]
pub struct CacheStats {
    pub total_templates: usize,
    pub cached_compilations: usize,
}
