use crate::database::api::fields::ApiFieldName;
use crate::database::template::wrapper::TemplateFieldName;
use crate::database::template::{ApiFields, TemplateFields};
use eden_logger_internal::{ctx_with_trace, log_debug, log_trace};
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct TemplateCache {
    cache: CacheLogic,
    read_prefix: String,
    read_bindings: Vec<(TemplateFieldName, ApiFieldName)>,
    write_prefix: String,
    write_bindings: Vec<(TemplateFieldName, ApiFieldName)>,
    ttl: Option<u64>,
}

impl TemplateCache {
    #[named]
    pub fn new(
        cache: CacheLogic,
        read_prefix: String,
        read_bindings: Vec<(impl Into<TemplateFieldName> + Clone, impl Into<ApiFieldName> + Clone)>,
        write_prefix: String,
        write_bindings: Vec<(impl Into<TemplateFieldName> + Clone, impl Into<ApiFieldName> + Clone)>,
        ttl: Option<u64>, // seconds
    ) -> Self {
        let _ctx = ctx_with_trace!().with_feature("endpoint-core");

        let _read_bindings_str = read_bindings
            .iter()
            .map(|(tn, an)| format!("{}->{}", Into::<TemplateFieldName>::into(tn.to_owned()), Into::<ApiFieldName>::into(an.to_owned())))
            .collect::<Vec<String>>()
            .join(", ");

        let _write_bindings_str = write_bindings
            .iter()
            .map(|(tn, an)| format!("{}->{}", Into::<TemplateFieldName>::into(tn.to_owned()), Into::<ApiFieldName>::into(an.to_owned())))
            .collect::<Vec<String>>()
            .join(", ");

        log_trace!(
            _ctx,
            "Template cache new",
            audience = eden_logger_internal::LogAudience::Internal,
            read_bindings = _read_bindings_str,
            write_bindings = _write_bindings_str
        );

        Self {
            cache,
            read_prefix,
            read_bindings: read_bindings.into_iter().map(|(t, a)| (t.into(), a.into())).collect(),
            write_prefix,
            write_bindings: write_bindings.into_iter().map(|(t, a)| (t.into(), a.into())).collect(),
            ttl,
        }
    }

    pub fn cache_logic(&self) -> &CacheLogic {
        &self.cache
    }

    pub fn read_prefix(&self) -> String {
        self.read_prefix.clone()
    }

    pub fn read_bindings(&self) -> &Vec<(TemplateFieldName, ApiFieldName)> {
        &self.read_bindings
    }

    pub fn write_prefix(&self) -> String {
        self.write_prefix.clone()
    }

    pub fn write_bindings(&self) -> &Vec<(TemplateFieldName, ApiFieldName)> {
        &self.write_bindings
    }

    pub fn read_object_map(&self, fields: &TemplateFields) -> ApiFields {
        let mut map = ApiFields::default();

        for (template, api) in self.read_bindings() {
            if template.to_string().contains("*") {
                map.insert(api.into(), fields.clone().into())
            } else {
                let api: String = api.into();
                if let Some(value) = fields.get(template) {
                    // we enforce "key" as the name for the cache key
                    if &api == "key" {
                        map.insert(api, Value::String(self.read_prefix.clone() + &value.to_string()))
                    } else {
                        map.insert(api, value.clone())
                    }
                }
            }
        }

        // Allow callers without explicit bindings to fall back to the prefix as the cache key.
        if !map.contains_key("key") && !self.read_prefix.is_empty() {
            map.insert("key".to_string(), Value::String(self.read_prefix.clone()));
        }

        map
    }

    #[named]
    pub fn write_object_map(&self, fields: &TemplateFields) -> ApiFields {
        let mut map = ApiFields::default();

        let _ctx = ctx_with_trace!().with_feature("endpoint-core");

        for (template, api) in self.write_bindings() {
            log_debug!(
                _ctx.clone(),
                "Template cache bindings",
                audience = eden_logger_internal::LogAudience::Internal,
                template = template.to_string(),
                api = api.to_string()
            );

            log_debug!(
                _ctx.clone(),
                "Fields",
                audience = eden_logger_internal::LogAudience::Internal,
                fields = format!("{:?}", fields)
            );
            if template.to_string().contains("*") {
                map.insert(api.into(), fields.clone().into())
            } else {
                let api: String = api.into();
                if let Some(value) = fields.get(template) {
                    if &api == "key" {
                        map.insert(api, Value::String(self.write_prefix.clone() + &value.to_string()))
                    } else {
                        map.insert(api, value.clone())
                    }
                }
            }
        }

        if !map.contains_key("key") && !self.write_prefix.is_empty() {
            map.insert("key".to_string(), Value::String(self.write_prefix.clone()));
        }

        if let Some(ttl) = self.ttl {
            map.insert("ttl".to_string(), Value::Number(ttl.into()));
        }

        map
    }
}

/// Template Cache logic is itself a template... TODO: check for infinite loops
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub enum CacheLogic {
    WriteThrough(CacheWriteApi),
    WriteBehind(CacheWriteApi),
    // WriteAround(CacheWriteTemplate), <-- in practice serves no purpose
    CacheAside { read: CacheReadApi, write: CacheWriteApi },
    ReadAround(CacheWriteApi),
    Invalidate(CacheWriteApi),
}

impl Default for CacheLogic {
    fn default() -> Self {
        Self::WriteThrough(CacheWriteApi(String::default()))
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct CacheWriteApi(String);

impl CacheWriteApi {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
    pub fn id(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct CacheReadApi(String);

impl CacheReadApi {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    pub fn id(&self) -> &str {
        &self.0
    }
}

#[allow(dead_code)]
impl CacheLogic {
    fn description(&self) -> String {
        match self {
            CacheLogic::WriteThrough(_) => {
                "Writes data to both cache and backing store simultaneously. Ensures strong consistency between cache and storage but has higher write latency. Data is guaranteed to persist even if cache fails immediately after write.".to_string()
            }
            CacheLogic::WriteBehind(_) => {
                "Writes data to cache immediately and asynchronously writes to backing store later. Provides low write latency and high throughput but risks data loss if cache fails before data is persisted. Also known as write-back caching.".to_string()
            }
            // CacheLogic::WriteAround(_) => {
            //     "Writes data directly to backing store, bypassing the cache entirely. Cache is only populated on subsequent reads (cache misses). Prevents cache pollution from write-heavy workloads but may result in cache misses for recently written data.".to_string()
            // }
            CacheLogic::CacheAside { .. } => {
                "Application explicitly manages cache operations. On cache miss, application fetches from backing store, populates cache, then returns data. On cache hit, returns cached data directly. Gives application full control over caching logic and cache keys. Also known as lazy loading.".to_string()
            }
            CacheLogic::ReadAround(_) => {
                "Reads data directly from backing store, bypassing the cache for read operations. Typically used when data is read infrequently or when cache hit rates are very low. Reduces cache overhead but eliminates read performance benefits of caching.".to_string()
            }
            CacheLogic::Invalidate(_) => {
                "Removes the associated data from the cache, as to no longer be called as part of any subsequent reqyests. This is typically used when data is removed from the primary data store, or the cache identifier is modified.".to_string()
            }
        }
    }

    fn short_description(&self) -> String {
        match self {
            CacheLogic::WriteThrough(_) => "Write to cache and store simultaneously - strong consistency, higher latency".to_string(),
            CacheLogic::WriteBehind(_) => "Write to cache first, store later - low latency, risk of data loss".to_string(),
            // CacheLogic::WriteAround(_) => "Write directly to store, bypass cache - prevents cache pollution".to_string(),
            CacheLogic::CacheAside { .. } => "Application manages cache explicitly - full control, lazy loading".to_string(),
            CacheLogic::ReadAround(_) => "Read directly from store, bypass cache - for infrequent reads".to_string(),
            CacheLogic::Invalidate(_) => "Remove the data from the cache.".to_string(),
        }
    }

    fn use_cases(&self) -> Vec<&'static str> {
        match self {
            CacheLogic::WriteThrough(_) => vec![
                "Financial systems requiring data consistency",
                "Critical data that cannot be lost",
                "Systems with moderate write loads",
            ],
            CacheLogic::WriteBehind(_) => vec![
                "High-throughput write applications",
                "Gaming leaderboards and counters",
                "Analytics and logging systems",
            ],
            // CacheLogic::WriteAround(_) => vec![
            //     "Write-heavy workloads",
            //     "Data written once, read rarely",
            //     "Batch processing systems"
            // ],
            CacheLogic::CacheAside { .. } => vec![
                "Complex caching logic requirements",
                "Multiple data sources",
                "Custom cache key strategies",
            ],
            CacheLogic::ReadAround(_) => vec![
                "Archive or historical data",
                "Very low cache hit rates",
                "Cost-sensitive applications",
            ],
            CacheLogic::Invalidate(_) => vec![
                "Deleting from the backing store",
                "Updating unique identifiers",
                "Culling stale data from the cache",
            ],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database::template::TemplateFields;
    use serde_json::Value;

    #[test]
    fn test_read_object_map() {
        let cache = TemplateCache::new(
            CacheLogic::default(),
            "brewery_record:".to_string(),
            vec![("batch_id", "key")],
            "brewery_record:".to_string(),
            vec![("batch_id", "key"), ("*", "value")],
            Some(3600),
        );

        let input = TemplateFields::new(vec![
            ("batch_id".to_string(), Value::Number(1234.into())),
            ("brewery_name".to_string(), Value::String("Golden Gate Brewing".to_string())),
            ("beer_type".to_string(), Value::String("IPA".to_string())),
            ("abv".to_string(), Value::Number(6.into())),
        ]);

        let result = cache.read_object_map(&input);

        println!("{:?}", result);

        assert_eq!(result.map().len(), 1);
        assert_eq!(result.get("key".to_string()).unwrap_or_default(), &Value::String("brewery_record:1234".to_string()));
    }

    #[test]
    fn test_write_object_map() {
        let cache = TemplateCache::new(
            CacheLogic::default(),
            "brewery_record:".to_string(),
            vec![("batch_id", "key")],
            "brewery_record:".to_string(),
            vec![("batch_id", "key"), ("*", "value")],
            Some(3600),
        );

        let input = TemplateFields::new(vec![
            ("batch_id".to_string(), Value::Number(1234.into())),
            ("brewery_name".to_string(), Value::String("Golden Gate Brewing".to_string())),
            ("beer_type".to_string(), Value::String("IPA".to_string())),
            ("abv".to_string(), Value::Number(6.into())),
        ]);

        let result = cache.write_object_map(&input);

        println!("{:?}", result);

        assert_eq!(result.map().len(), 3);
        assert_eq!(result.get("key".to_string()).unwrap_or_default(), &Value::String("brewery_record:1234".to_string()));
        /* assert_eq!(
            result.get("value".to_string()).unwrap_or_default(),
            &input.into() as Value
        );*/
        assert_eq!(result.get("ttl".to_string()).unwrap_or_default(), &Value::Number(3600.into()));
    }
}
