use crate::database::api::fields::FieldType;
use crate::database::template::TemplateFields;
use error::{EpError, ResultEP};
use format::{TemplateId, TemplateUuid};
use lru::LruCache;
use rustc_hash::FxHashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use utoipa::ToSchema;

type PlaceholderWithPosition = (usize, usize, String, FieldType, bool, Option<Value>);
type ParsedPlaceholder = (String, FieldType, bool, Option<Value>);

/// A conditional block that adds additional JSON structure to a template when a trigger field is present.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConditionalBlock {
    /// Field name that triggers this conditional block (e.g., "email")
    pub trigger_field: String,
    /// Additional JSON structure to merge when trigger_field is provided
    pub template_addition: Value,
    /// Optional path to merge at (default: root merge)
    pub merge_at_path: Option<String>,
}

impl utoipa::ToSchema for ConditionalBlock {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("ConditionalBlock")
    }
}

impl utoipa::PartialSchema for ConditionalBlock {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        use utoipa::openapi::schema::*;
        utoipa::openapi::RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(Type::Object)
                .description(Some("A conditional block that adds JSON structure when a trigger field is present"))
                .property(
                    "trigger_field",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .description(Some("Field name that triggers this conditional block (e.g., 'email')"))
                        .build(),
                )
                .required("trigger_field")
                .property(
                    "template_addition",
                    ObjectBuilder::new()
                        .schema_type(Type::Object)
                        .description(Some("Additional JSON structure to merge when trigger_field is provided"))
                        .additional_properties(Some(AdditionalProperties::FreeForm(true)))
                        .build(),
                )
                .required("template_addition")
                .property(
                    "merge_at_path",
                    ObjectBuilder::new()
                        .schema_type(Type::String)
                        .description(Some("Optional JSON path to merge at (default: root merge)"))
                        .build(),
                )
                .build(),
        ))
    }
}

impl ConditionalBlock {
    pub fn new(trigger_field: impl Into<String>, template_addition: impl Into<Value>, merge_at_path: Option<String>) -> Self {
        Self {
            trigger_field: trigger_field.into(),
            template_addition: template_addition.into(),
            merge_at_path,
        }
    }
}

#[derive(Debug, Clone)]
pub struct HandlebarsCache {
    id_templates: FxHashMap<TemplateId, Arc<Handlebars>>,
    uuid_templates: FxHashMap<TemplateUuid, Arc<Handlebars>>,
    compile_cache: LruCache<u64, Arc<Handlebars>>,
}

impl Default for HandlebarsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl HandlebarsCache {
    pub fn new() -> Self {
        let cache_size = match std::num::NonZeroUsize::new(256) {
            Some(size) => size,
            None => unsafe { std::num::NonZeroUsize::new_unchecked(1) },
        };
        Self {
            id_templates: FxHashMap::default(),
            uuid_templates: FxHashMap::default(),
            compile_cache: LruCache::new(cache_size),
        }
    }

    pub fn register(&mut self, template_id: TemplateId, template_uuid: TemplateUuid, template: Value) -> ResultEP<Arc<Handlebars>> {
        let compiled = Arc::new(Handlebars::new(template)?);
        self.id_templates.insert(template_id, compiled.clone());
        self.uuid_templates.insert(template_uuid, compiled.clone());
        Ok(compiled)
    }

    pub fn register_with_conditions(
        &mut self,
        template_id: TemplateId,
        template_uuid: TemplateUuid,
        template: Value,
        conditional_blocks: Vec<ConditionalBlock>,
    ) -> ResultEP<Arc<Handlebars>> {
        let compiled = Arc::new(Handlebars::new_with_conditions(template, conditional_blocks)?);
        self.id_templates.insert(template_id, compiled.clone());
        self.uuid_templates.insert(template_uuid, compiled.clone());
        Ok(compiled)
    }

    pub fn clear_cache(&mut self) {
        self.compile_cache.clear();
    }

    pub fn cache_size(&self) -> usize {
        self.compile_cache.len()
    }

    pub fn get_id_template(&self, template_id: &TemplateId) -> Option<Arc<Handlebars>> {
        self.id_templates.get(template_id).cloned()
    }

    pub fn get_uuid_template(&self, template_uuid: &TemplateUuid) -> Option<Arc<Handlebars>> {
        self.uuid_templates.get(template_uuid).cloned()
    }

    pub fn get_id_or_compile(&mut self, template_id: Option<&TemplateId>, template: &Value) -> ResultEP<Arc<Handlebars>> {
        if let Some(id) = template_id
            && let Some(compiled) = self.get_id_template(id)
        {
            return Ok(compiled);
        }

        let hash = self.hash_template(template);
        if let Some(compiled) = self.compile_cache.get(&hash) {
            return Ok(compiled.clone());
        }

        let compiled = Arc::new(Handlebars::new(template.clone())?);
        self.compile_cache.put(hash, compiled.clone());
        Ok(compiled)
    }

    pub fn get_uuid_or_compile(&mut self, template_uuid: Option<&TemplateUuid>, template: &Value) -> ResultEP<Arc<Handlebars>> {
        if let Some(uuid) = template_uuid
            && let Some(compiled) = self.get_uuid_template(uuid)
        {
            return Ok(compiled);
        }

        let hash = self.hash_template(template);
        if let Some(compiled) = self.compile_cache.get(&hash) {
            return Ok(compiled.clone());
        }

        let compiled = Arc::new(Handlebars::new(template.clone())?);
        self.compile_cache.put(hash, compiled.clone());
        Ok(compiled)
    }

    fn hash_template(&self, template: &Value) -> u64 {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        template.to_string().hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct Handlebars {
    #[schema(value_type = Object, additional_properties)]
    template: Value,
    fields: Vec<FieldInfo>,
    field_lookup: HashMap<String, usize>,
    conditional_blocks: Vec<ConditionalBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum FieldRequirement {
    Required,
    Optional,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct FieldInfo {
    pub name: String,
    pub field_type: FieldType,
    pub requirement: FieldRequirement,
    pub occurrences: Vec<FieldOccurrence>,
    #[schema(value_type = Object, additional_properties)]
    pub default_value: Option<Value>,
}

impl FieldInfo {
    #[allow(unused)]
    fn is_optional(&self) -> bool {
        self.requirement == FieldRequirement::Optional
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct FieldOccurrence {
    pub json_path: String,                       // e.g., "user.settings.theme" or "operations[0].key"
    pub string_position: Option<StringPosition>, // Position within string if in mixed content
    pub context_type: ContextType,               // Whether it's in a key, value, or array element
    pub source: FieldSource,                     // Whether from base template or conditional block
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub struct StringPosition {
    pub start_byte: usize, // left for first brace -->{{
    pub end_byte: usize,   // right of last brace }}<--
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum ContextType {
    ObjectKey,
    ObjectValue,
    ArrayElement,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum FieldSource {
    BaseTemplate,
    ConditionalBlock(usize), // Index of the conditional block
}

impl Handlebars {
    pub fn new(template: Value) -> ResultEP<Self> {
        Self::new_with_conditions(template, Vec::new())
    }

    pub fn new_with_conditions(template: Value, conditional_blocks: Vec<ConditionalBlock>) -> ResultEP<Self> {
        let mut fields = Vec::new();
        let mut seen_fields = std::collections::HashMap::new();

        // Extract fields from base template
        Self::extract_fields_with_positions(&template, &mut fields, &mut seen_fields, "", FieldSource::BaseTemplate)?;

        // Extract fields from conditional blocks
        for (block_index, block) in conditional_blocks.iter().enumerate() {
            Self::extract_fields_with_positions(
                &block.template_addition,
                &mut fields,
                &mut seen_fields,
                block.merge_at_path.as_deref().unwrap_or(""),
                FieldSource::ConditionalBlock(block_index),
            )?;
        }

        let mut field_lookup = HashMap::with_capacity(fields.len());
        for (i, field) in fields.iter().enumerate() {
            field_lookup.insert(field.name.clone(), i);
        }

        Ok(Self { template, fields, field_lookup, conditional_blocks })
    }

    pub fn template(&self) -> &Value {
        &self.template
    }

    pub fn conditional_blocks(&self) -> &[ConditionalBlock] {
        &self.conditional_blocks
    }

    fn extract_fields_with_positions(
        value: &Value,
        fields: &mut Vec<FieldInfo>,
        seen_fields: &mut std::collections::HashMap<String, usize>,
        current_path: &str,
        source: FieldSource,
    ) -> ResultEP<()> {
        match value {
            Value::String(s) => {
                Self::extract_fields_from_string_with_positions(s, fields, seen_fields, current_path, ContextType::ObjectValue, source)?;
            }
            Value::Array(arr) => {
                for (i, item) in arr.iter().enumerate() {
                    let item_path = if current_path.is_empty() {
                        format!("[{}]", i)
                    } else {
                        format!("{}[{}]", current_path, i)
                    };
                    Self::extract_fields_with_positions(item, fields, seen_fields, &item_path, source.clone())?;
                }
            }
            Value::Object(obj) => {
                for (key, val) in obj {
                    // Extract from the key itself
                    let key_path = if current_path.is_empty() {
                        format!("@key({})", key)
                    } else {
                        format!("{}.@key({})", current_path, key)
                    };

                    Self::extract_fields_from_string_with_positions(
                        key,
                        fields,
                        seen_fields,
                        &key_path,
                        ContextType::ObjectKey,
                        source.clone(),
                    )?;

                    // Extract from the value
                    let value_path = if current_path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", current_path, key)
                    };

                    Self::extract_fields_with_positions(val, fields, seen_fields, &value_path, source.clone())?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn extract_fields_from_string_with_positions(
        s: &str,
        fields: &mut Vec<FieldInfo>,
        seen_fields: &mut std::collections::HashMap<String, usize>,
        json_path: &str,
        context_type: ContextType,
        source: FieldSource,
    ) -> ResultEP<()> {
        let placeholders = Self::find_all_placeholders_with_positions(s)?;

        for (start_pos, end_pos, field_name, field_type, is_optional, default_value) in placeholders {
            let occurrence = FieldOccurrence {
                json_path: json_path.to_string(),
                string_position: if s.trim().starts_with("{{") && s.trim().ends_with("}}") && s.matches("{{").count() == 1 {
                    None // Single placeholder, no position needed
                } else {
                    Some(StringPosition { start_byte: start_pos, end_byte: end_pos })
                },
                context_type: context_type.clone(),
                source: source.clone(),
            };

            if let Some(&field_index) = seen_fields.get(&field_name) {
                // Field already exists, add this occurrence
                fields[field_index].occurrences.push(occurrence);

                // Ensure consistency - if any occurrence is required, the field is required
                if !is_optional {
                    fields[field_index].requirement = FieldRequirement::Required;
                }
            } else {
                // New field
                let field_index = fields.len();
                seen_fields.insert(field_name.clone(), field_index);
                fields.push(FieldInfo {
                    name: field_name,
                    field_type,
                    requirement: if is_optional {
                        FieldRequirement::Optional
                    } else {
                        FieldRequirement::Required
                    },
                    occurrences: vec![occurrence],
                    default_value,
                });
            }
        }

        Ok(())
    }

    // Helper method to find all placeholders in a string with positions
    fn find_all_placeholders_with_positions(s: &str) -> ResultEP<Vec<PlaceholderWithPosition>> {
        let mut placeholders = Vec::new();
        let mut start = 0;

        while start < s.len() {
            if let Some(open_start) = s[start..].find("{{") {
                let open_pos = start + open_start;

                if let Some(close_start) = s[open_pos + 2..].find("}}") {
                    let close_pos = open_pos + 2 + close_start + 2;
                    let placeholder = &s[open_pos..close_pos];

                    if let Some((name, field_type, is_optional, default_value)) = Self::parse_placeholder(placeholder)? {
                        placeholders.push((open_pos, close_pos, name, field_type, is_optional, default_value));
                    }

                    start = close_pos;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        Ok(placeholders)
    }

    fn parse_placeholder(s: &str) -> ResultEP<Option<ParsedPlaceholder>> {
        let bytes = s.as_bytes();
        if bytes.len() < 4 || !bytes.starts_with(b"{{") || !bytes.ends_with(b"}}") {
            return Ok(None);
        }

        let inner = s[2..s.len() - 2].trim();
        if inner.is_empty() {
            return Ok(None);
        }

        let (name, field_type, is_optional, default_value) = Self::parse_name_type_and_optional(inner)?;
        Ok(Some((name, field_type, is_optional, default_value)))
    }

    fn parse_name_type_and_optional(part: &str) -> ResultEP<(String, FieldType, bool, Option<Value>)> {
        // Check if field is optional (starts with ?)
        let (name_and_type, is_optional) = if let Some(stripped) = part.strip_prefix('?') {
            (stripped, true)
        } else {
            (part, false)
        };

        match name_and_type.find(':') {
            Some(colon_pos) => {
                let name = name_and_type[..colon_pos].trim();
                let type_str = name_and_type[colon_pos + 1..].trim();
                match type_str.split_once('=') {
                    None => {
                        let field_type = FieldType::try_from(type_str.trim())?;
                        Ok((name.to_string(), field_type, is_optional, None))
                    }
                    Some((type_str, default_str)) => {
                        let field_type = FieldType::try_from(type_str.trim())?;
                        let default_value = serde_json::Value::from(default_str.trim().to_string());

                        Ok((name.to_string(), field_type, is_optional, Some(default_value)))
                    }
                }
            }
            None => Ok((name_and_type.trim().to_string(), FieldType::String, is_optional, None)),
        }
    }

    fn should_apply_conditional_block(block: &ConditionalBlock, values: &TemplateFields) -> bool {
        // Apply block if the trigger field has a non-null value
        values.get(&block.trigger_field).map(|v| !v.is_null()).unwrap_or(false)
    }

    fn deep_merge_json(base: Value, addition: &Value) -> ResultEP<Value> {
        match (base, addition) {
            (Value::Object(mut base_obj), Value::Object(addition_obj)) => {
                for (key, addition_value) in addition_obj {
                    match base_obj.get_mut(key) {
                        Some(existing_value) => {
                            // Recursively merge if both are objects or arrays
                            if (existing_value.is_object() && addition_value.is_object())
                                || (existing_value.is_array() && addition_value.is_array())
                            {
                                *existing_value = Self::deep_merge_json(existing_value.clone(), addition_value)?;
                            } else {
                                // Replace existing value with addition
                                *existing_value = addition_value.clone();
                            }
                        }
                        None => {
                            // Add new key-value pair
                            base_obj.insert(key.clone(), addition_value.clone());
                        }
                    }
                }
                Ok(Value::Object(base_obj))
            }
            (Value::Array(mut base_arr), Value::Array(addition_arr)) => {
                // For arrays, extend the base with addition items
                base_arr.extend(addition_arr.iter().cloned());
                Ok(Value::Array(base_arr))
            }
            (_, addition_val) => {
                // For non-object, non-array values, replace base with addition
                Ok(addition_val.clone())
            }
        }
    }

    fn apply_conditional_blocks(
        mut base_template: Value,
        values: &TemplateFields,
        conditional_blocks: &[ConditionalBlock],
    ) -> ResultEP<Value> {
        for block in conditional_blocks {
            if Self::should_apply_conditional_block(block, values) {
                // For now, we only support root-level merge
                // TODO: Implement merge_at_path functionality if needed
                base_template = Self::deep_merge_json(base_template, &block.template_addition)?;
            }
        }
        Ok(base_template)
    }

    fn replace_placeholders(value: &Value, values: &TemplateFields) -> ResultEP<Value> {
        match value {
            Value::String(s) => Ok(Self::replace_placeholders_in_string(s, values)?),
            Value::Array(arr) => {
                let mut new_arr = Vec::with_capacity(arr.len());
                for item in arr {
                    new_arr.push(Self::replace_placeholders(item, values)?);
                }
                Ok(Value::Array(new_arr))
            }
            Value::Object(obj) => {
                let mut new_obj = serde_json::Map::with_capacity(obj.len());
                for (key, val) in obj {
                    // Replace placeholders in both key and value
                    let new_key = if let Value::String(new_key_str) = Self::replace_placeholders_in_string(key, values)? {
                        new_key_str
                    } else {
                        key.clone() // Fallback to original key if replacement fails
                    };

                    let new_val = Self::replace_placeholders(val, values)?;
                    new_obj.insert(new_key, new_val);
                }
                Ok(Value::Object(new_obj))
            }
            _ => Ok(value.clone()),
        }
    }

    fn replace_placeholders_in_string(s: &str, values: &TemplateFields) -> ResultEP<Value> {
        // Check if the entire string is a single placeholder (no other text)
        let trimmed = s.trim();
        if trimmed.starts_with("{{")
            && trimmed.ends_with("}}")
            && trimmed.matches("{{").count() == 1
            && let Some((field_name, _, is_optional, default_value)) = Self::parse_placeholder(trimmed)?
        {
            return match values.get(&field_name) {
                Some(value) => Ok(value.clone()),
                None => {
                    if is_optional {
                        if let Some(default_value) = default_value {
                            Ok(default_value.clone())
                        } else {
                            Ok(Value::Null) // Optional fields return null when missing
                        }
                    } else {
                        Ok(Value::Null) // Keep consistent behavior for missing fields
                    }
                }
            };
        }

        // Handle multiple placeholders or mixed text
        let placeholders = Self::find_all_placeholders_with_positions(s)?;

        if placeholders.is_empty() {
            return Ok(Value::String(s.to_string()));
        }

        let mut result = s.to_string();

        // Replace placeholders in reverse order to maintain correct indices
        for (start_pos, end_pos, field_name, _, is_optional, default_value) in placeholders.into_iter().rev() {
            let replacement = match values.get(&field_name) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Number(n)) => n.to_string(),
                Some(Value::Bool(b)) => b.to_string(),
                Some(Value::Null) => "null".to_string(),
                Some(Value::Array(a)) => {
                    if a.is_empty() {
                        "null".to_string()
                    } else if a.len() == 1 {
                        match a.first() {
                            Some(Value::String(s)) => s.clone(),
                            Some(Value::Number(n)) => n.to_string(),
                            Some(Value::Bool(b)) => b.to_string(),
                            Some(Value::Null) => "null".to_string(),
                            Some(other) => {
                                // For complex types, serialize to JSON string
                                serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                            }
                            None => {
                                if is_optional {
                                    if let Some(default_value) = default_value {
                                        default_value.to_string()
                                    } else {
                                        "".to_string() // Optional fields become empty string when missing in mixed content
                                    }
                                } else {
                                    "null".to_string()
                                }
                            }
                        }
                    } else {
                        serde_json::to_string(a).unwrap_or_else(|_| "null".to_string())
                    }
                }
                Some(other) => {
                    // For complex types, serialize to JSON string
                    serde_json::to_string(other).unwrap_or_else(|_| "null".to_string())
                }
                None => {
                    if is_optional {
                        if let Some(default_value) = default_value {
                            default_value.to_string()
                        } else {
                            "".to_string() // Optional fields become empty string when missing in mixed content
                        }
                    } else {
                        "null".to_string()
                    }
                }
            };

            result.replace_range(start_pos..end_pos, &replacement);
        }

        Ok(Value::String(result))
    }

    pub fn render(&self, values: &TemplateFields) -> ResultEP<Value> {
        // Step 1: Apply conditional blocks to get the complete template
        let complete_template = Self::apply_conditional_blocks(self.template.clone(), values, &self.conditional_blocks)?;

        // Step 2: Replace placeholders in the complete template
        Self::replace_placeholders(&complete_template, values)
    }

    pub fn render_with_strings(&self, values: &HashMap<String, String>) -> ResultEP<Value> {
        let mut template_map = TemplateFields::with_capacity(values.len());
        for (k, v) in values {
            template_map.insert(k.clone(), Value::String(v.clone()));
        }
        self.render(&template_map)
    }

    pub fn render_as_string(&self, values: &TemplateFields) -> ResultEP<String> {
        let rendered = self.render(values)?;
        serde_json::to_string(&rendered).map_err(EpError::serde)
    }

    pub fn get_field(&self, name: &str) -> Option<&FieldInfo> {
        self.field_lookup.get(name).and_then(|&idx| self.fields.get(idx))
    }

    pub fn field_names(&self) -> impl Iterator<Item = &String> {
        self.fields.iter().map(|f| &f.name)
    }

    pub fn fields(&self) -> &[FieldInfo] {
        &self.fields
    }

    pub fn required_fields(&self) -> impl Iterator<Item = &FieldInfo> {
        self.fields.iter().filter(|f| f.requirement == FieldRequirement::Required)
    }

    pub fn optional_fields(&self) -> impl Iterator<Item = &FieldInfo> {
        self.fields.iter().filter(|f| f.requirement == FieldRequirement::Optional)
    }

    pub fn render_plan_size(&self) -> usize {
        self.fields.len()
    }

    pub fn validate_values(&self, values: &TemplateFields) -> ResultEP<Vec<String>> {
        let mut missing_fields = Vec::new();

        // First, determine which conditional blocks are active
        let active_blocks: Vec<bool> =
            self.conditional_blocks.iter().map(|block| Self::should_apply_conditional_block(block, values)).collect();

        for field in &self.fields {
            if field.requirement == FieldRequirement::Required {
                // Check if this field is needed based on active conditional blocks
                let is_field_needed = field.occurrences.iter().any(|occurrence| {
                    match &occurrence.source {
                        FieldSource::BaseTemplate => true, // Base template fields are always needed
                        FieldSource::ConditionalBlock(block_index) => {
                            // Conditional block fields are only needed if their block is active
                            active_blocks.get(*block_index).copied().unwrap_or(false)
                        }
                    }
                });

                if is_field_needed && !values.contains_key(&field.name) {
                    missing_fields.push(field.name.clone());
                }
            }
        }

        Ok(missing_fields)
    }

    pub fn compile_for_repeated_use(&self) -> CompiledTemplate {
        CompiledTemplate { handlebars: self.clone() }
    }

    // Enhanced methods for querying position information
    pub fn get_field_occurrences(&self, field_name: &str) -> Option<&[FieldOccurrence]> {
        self.get_field(field_name).map(|field| field.occurrences.as_slice())
    }

    pub fn fields_in_path(&self, path_prefix: &str) -> Vec<&FieldInfo> {
        self.fields.iter().filter(|field| field.occurrences.iter().any(|occ| occ.json_path.starts_with(path_prefix))).collect()
    }

    pub fn fields_in_keys(&self) -> Vec<&FieldInfo> {
        self.fields
            .iter()
            .filter(|field| field.occurrences.iter().any(|occ| occ.context_type == ContextType::ObjectKey))
            .collect()
    }

    pub fn fields_with_multiple_occurrences(&self) -> Vec<&FieldInfo> {
        self.fields.iter().filter(|field| field.occurrences.len() > 1).collect()
    }

    pub fn fields_from_conditional_blocks(&self) -> Vec<&FieldInfo> {
        self.fields
            .iter()
            .filter(|field| field.occurrences.iter().any(|occ| matches!(occ.source, FieldSource::ConditionalBlock(_))))
            .collect()
    }

    pub fn validate_values_detailed(&self, values: &TemplateFields) -> ResultEP<ValidationReport> {
        let mut missing_fields = Vec::new();
        let mut field_usage = Vec::new();

        for field in &self.fields {
            if values.contains_key(&field.name) {
                field_usage.push(FieldUsage {
                    field_name: field.name.clone(),
                    requirement: field.requirement.clone(),
                    occurrences: field.occurrences.clone(),
                    provided_value_type: values.get(&field.name).map(FieldType::from),
                });
            } else if field.requirement == FieldRequirement::Required {
                missing_fields.push(MissingField {
                    field_name: field.name.clone(),
                    field_type: field.field_type.clone(),
                    requirement: field.requirement.clone(),
                    required_at_paths: field.occurrences.iter().map(|occ| occ.json_path.clone()).collect(),
                });
            }
        }

        Ok(ValidationReport { missing_fields, field_usage })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ValidationReport {
    pub missing_fields: Vec<MissingField>,
    pub field_usage: Vec<FieldUsage>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MissingField {
    pub field_name: String,
    pub field_type: FieldType,
    pub requirement: FieldRequirement,
    pub required_at_paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FieldUsage {
    pub field_name: String,
    pub requirement: FieldRequirement,
    pub occurrences: Vec<FieldOccurrence>,
    pub provided_value_type: Option<FieldType>,
}

impl FieldUsage {
    #[allow(unused)]
    fn is_optional(&self) -> bool {
        self.requirement == FieldRequirement::Optional
    }
}

#[derive(Debug, Clone)]
pub struct CompiledTemplate {
    handlebars: Handlebars,
}

impl CompiledTemplate {
    pub fn render(&self, values: &TemplateFields) -> ResultEP<Value> {
        self.handlebars.render(values)
    }

    pub fn render_batch(&self, batch_values: &[TemplateFields]) -> ResultEP<Vec<Value>> {
        let mut results = Vec::with_capacity(batch_values.len());
        for values in batch_values {
            results.push(self.handlebars.render(values)?);
        }
        Ok(results)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_field_extraction_with_positions() {
        let template = json!({
            "name": "{{user_name: String}}",
            "age": "{{user_age: Number}}"
        });

        let handlebars = Handlebars::new(template).unwrap();
        assert_eq!(handlebars.fields().len(), 2);

        let name_field = handlebars.get_field("user_name").unwrap();
        assert_eq!(name_field.occurrences.len(), 1);
        assert_eq!(name_field.occurrences[0].json_path, "name");
        assert_eq!(name_field.occurrences[0].context_type, ContextType::ObjectValue);
        assert!(name_field.occurrences[0].string_position.is_none()); // Single placeholder

        let age_field = handlebars.get_field("user_age").unwrap();
        assert_eq!(age_field.occurrences.len(), 1);
        assert_eq!(age_field.occurrences[0].json_path, "age");
    }

    #[test]
    fn test_multiple_placeholders_in_string_with_positions() {
        let template = json!({
            "greeting": "Hello {{first_name: String}} {{last_name: String}}!"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let first_name_field = handlebars.get_field("first_name").unwrap();
        assert_eq!(first_name_field.occurrences.len(), 1);
        assert_eq!(first_name_field.occurrences[0].json_path, "greeting");

        let pos = first_name_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 6); // Position of "{{first_name: String}}"
        assert_eq!(pos.end_byte, 28);

        let last_name_field = handlebars.get_field("last_name").unwrap();
        let pos = last_name_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 29); // Position of "{{last_name: String}}"
        assert_eq!(pos.end_byte, 50);
    }

    #[test]
    fn test_field_extraction_from_object_keys_with_positions() {
        let template = json!({
            "{{dynamic_key: String}}": "static_value",
            "nested": {
                "{{nested_key: String}}": "{{nested_value: String}}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let dynamic_key_field = handlebars.get_field("dynamic_key").unwrap();
        assert_eq!(dynamic_key_field.occurrences.len(), 1);
        assert_eq!(dynamic_key_field.occurrences[0].json_path, "@key({{dynamic_key: String}})");
        assert_eq!(dynamic_key_field.occurrences[0].context_type, ContextType::ObjectKey);

        let nested_key_field = handlebars.get_field("nested_key").unwrap();
        assert_eq!(nested_key_field.occurrences[0].json_path, "nested.@key({{nested_key: String}})");
        assert_eq!(nested_key_field.occurrences[0].context_type, ContextType::ObjectKey);

        let nested_value_field = handlebars.get_field("nested_value").unwrap();
        assert_eq!(nested_value_field.occurrences[0].json_path, "nested.{{nested_key: String}}");
        assert_eq!(nested_value_field.occurrences[0].context_type, ContextType::ObjectValue);
    }

    #[test]
    fn test_array_field_positions() {
        let template = json!([
            "{{item1: String}}",
            {
                "key": "{{item2: String}}"
            }
        ]);

        let handlebars = Handlebars::new(template).unwrap();

        let item1_field = handlebars.get_field("item1").unwrap();
        assert_eq!(item1_field.occurrences[0].json_path, "[0]");
        assert_eq!(item1_field.occurrences[0].context_type, ContextType::ObjectValue);

        let item2_field = handlebars.get_field("item2").unwrap();
        assert_eq!(item2_field.occurrences[0].json_path, "[1].key");
        assert_eq!(item2_field.occurrences[0].context_type, ContextType::ObjectValue);
    }

    #[test]
    fn test_multiple_occurrences_same_field() {
        let template = json!({
            "user1": "{{user_id: String}}",
            "user2": "{{user_id: String}}",
            "nested": {
                "user3": "{{user_id: String}}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let user_id_field = handlebars.get_field("user_id").unwrap();
        assert_eq!(user_id_field.occurrences.len(), 3);

        let paths: Vec<&String> = user_id_field.occurrences.iter().map(|occ| &occ.json_path).collect();

        assert!(paths.contains(&&"user1".to_string()));
        assert!(paths.contains(&&"user2".to_string()));
        assert!(paths.contains(&&"nested.user3".to_string()));
    }

    #[test]
    fn test_fields_query_methods() {
        let template = json!({
            "{{key_field: String}}": "value",
            "normal": "{{value_field: String}}",
            "user": {
                "profile": {
                    "name": "{{user_name: String}}",
                    "{{user_key: String}}": "{{user_name: String}}"
                }
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Test fields_in_keys
        let key_fields = handlebars.fields_in_keys();
        assert_eq!(key_fields.len(), 2); // key_field and user_key

        let key_field_names: Vec<&String> = key_fields.iter().map(|f| &f.name).collect();
        assert!(key_field_names.contains(&&"key_field".to_string()));
        assert!(key_field_names.contains(&&"user_key".to_string()));

        // Test fields_in_path
        let user_fields = handlebars.fields_in_path("user.profile");
        assert_eq!(user_fields.len(), 2); // user_name and user_key

        // Test fields_with_multiple_occurrences
        let multiple_fields = handlebars.fields_with_multiple_occurrences();
        assert_eq!(multiple_fields.len(), 1); // user_name appears twice
        assert_eq!(multiple_fields[0].name, "user_name");
    }

    #[test]
    fn test_detailed_validation_report() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "age": "{{user_age: Number}}"
            },
            "meta": "{{metadata: Object}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut partial_values = TemplateFields::default();
        partial_values.insert("user_name".to_string(), Value::String("Alice".to_string()));
        // Missing user_age and metadata

        let report = handlebars.validate_values_detailed(&partial_values).unwrap();

        assert_eq!(report.missing_fields.len(), 2);
        assert_eq!(report.field_usage.len(), 1);

        let missing_age = report.missing_fields.iter().find(|f| f.field_name == "user_age").unwrap();
        assert_eq!(missing_age.required_at_paths, vec!["user.age"]);

        let used_name = &report.field_usage[0];
        assert_eq!(used_name.field_name, "user_name");
        assert_eq!(used_name.provided_value_type, Some(FieldType::String));
    }

    #[test]
    fn test_complex_url_structure_with_positions() {
        let template = json!({
            "endpoint": "{{base_url: String}}/{{version: String}}/{{resource: String}}",
            "auth": "Bearer {{token: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Test that all fields are extracted with correct positions
        let base_url_field = handlebars.get_field("base_url").unwrap();
        let pos = base_url_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 0);
        assert_eq!(pos.end_byte, 20); // "{{base_url: String}}"

        let version_field = handlebars.get_field("version").unwrap();
        let pos = version_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 21); // After "/"
        assert_eq!(pos.end_byte, 40); // "{{version: String}}"

        let resource_field = handlebars.get_field("resource").unwrap();
        let pos = resource_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 41); // After second "/"
        assert_eq!(pos.end_byte, 61); // "{{resource: String}}"

        // Test rendering
        let mut values = TemplateFields::default();
        values.insert("base_url".to_string(), Value::String("https://api.example.com".to_string()));
        values.insert("version".to_string(), Value::String("v1".to_string()));
        values.insert("resource".to_string(), Value::String("users".to_string()));
        values.insert("token".to_string(), Value::String("abc123".to_string()));

        let result = handlebars.render(&values).unwrap();
        assert_eq!(result["endpoint"], "https://api.example.com/v1/users");
        assert_eq!(result["auth"], "Bearer abc123");
    }

    #[test]
    fn test_single_placeholder_returns_value_type() {
        let template = json!({
            "just_string": "{{name: String}}",
            "just_number": "{{age: Number}}",
            "just_bool": "{{active: Boolean}}",
            "just_object": "{{data: Object}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut values = TemplateFields::default();
        values.insert("name".to_string(), Value::String("Alice".to_string()));
        values.insert("age".to_string(), Value::Number(serde_json::Number::from(30)));
        values.insert("active".to_string(), Value::Bool(true));
        values.insert("data".to_string(), json!({"key": "value"}));

        let result = handlebars.render(&values).unwrap();

        // Single placeholders should return the actual value, not a string
        assert_eq!(result["just_string"], "Alice");
        assert_eq!(result["just_number"], 30);
        assert_eq!(result["just_bool"], true);
        assert_eq!(result["just_object"]["key"], "value");

        // Verify that single placeholders have no string position
        let name_field = handlebars.get_field("name").unwrap();
        assert!(name_field.occurrences[0].string_position.is_none());
    }

    #[test]
    fn test_consecutive_placeholders_with_positions() {
        let template = json!({
            "fullname": "{{first: String}}{{last: String}}",
            "spaced": "{{first: String}} {{last: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let first_field = handlebars.get_field("first").unwrap();
        assert_eq!(first_field.occurrences.len(), 2); // Appears in both fullname and spaced

        // Check positions in "fullname" (consecutive)
        let fullname_occurrence = first_field.occurrences.iter().find(|occ| occ.json_path == "fullname").unwrap();
        let pos = fullname_occurrence.string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 0);
        assert_eq!(pos.end_byte, 17); // "{{first: String}}"

        let last_field = handlebars.get_field("last").unwrap();
        let fullname_last_occurrence = last_field.occurrences.iter().find(|occ| occ.json_path == "fullname").unwrap();
        let pos = fullname_last_occurrence.string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 17); // Immediately after first
        assert_eq!(pos.end_byte, 33); // "{{last: String}}"

        // Test rendering
        let mut values = TemplateFields::default();
        values.insert("first".to_string(), Value::String("John".to_string()));
        values.insert("last".to_string(), Value::String("Doe".to_string()));

        let result = handlebars.render(&values).unwrap();
        assert_eq!(result["fullname"], "JohnDoe");
        assert_eq!(result["spaced"], "John Doe");
    }

    #[test]
    fn test_nested_complex_structure_with_comprehensive_positions() {
        let template = json!({
            "{{service_name: String}}": {
                "{{action_type: String}}": {
                    "endpoint": "{{base_url: String}}/{{version: String}}/{{resource: String}}",
                    "headers": {
                        "{{auth_header: String}}": "Bearer {{token: String}}"
                    },
                    "body": {
                        "{{entity_id_field: String}}": "{{entity_id: String}}",
                        "metadata": {
                            "timestamp": "{{timestamp: String}}",
                            "user": "{{user_id: String}}"
                        }
                    }
                }
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Verify all fields are extracted with proper paths
        assert_eq!(handlebars.fields().len(), 11);

        // Check service_name (object key at root)
        let service_field = handlebars.get_field("service_name").unwrap();
        assert_eq!(service_field.occurrences[0].json_path, "@key({{service_name: String}})");
        assert_eq!(service_field.occurrences[0].context_type, ContextType::ObjectKey);

        // Check action_type (nested object key)
        let action_field = handlebars.get_field("action_type").unwrap();
        assert_eq!(action_field.occurrences[0].json_path, "{{service_name: String}}.@key({{action_type: String}})");
        assert_eq!(action_field.occurrences[0].context_type, ContextType::ObjectKey);

        // Check deeply nested field
        let timestamp_field = handlebars.get_field("timestamp").unwrap();
        assert_eq!(
            timestamp_field.occurrences[0].json_path,
            "{{service_name: String}}.{{action_type: String}}.body.metadata.timestamp"
        );
        assert_eq!(timestamp_field.occurrences[0].context_type, ContextType::ObjectValue);

        // Check field in multi-placeholder string
        let base_url_field = handlebars.get_field("base_url").unwrap();
        assert_eq!(base_url_field.occurrences[0].json_path, "{{service_name: String}}.{{action_type: String}}.endpoint");
        assert!(base_url_field.occurrences[0].string_position.is_some());

        // Test path-based queries
        let body_fields = handlebars.fields_in_path("{{service_name: String}}.{{action_type: String}}.body");
        assert_eq!(body_fields.len(), 4); // entity_id_field, entity_id, timestamp, user_id

        let key_fields = handlebars.fields_in_keys();
        assert_eq!(key_fields.len(), 4); // service_name, action_type, auth_header, entity_id_field
    }

    #[test]
    fn test_malformed_placeholders_ignored() {
        let template = json!({
            "valid": "{{name: String}}",
            "incomplete_open": "{{name",
            "incomplete_close": "name}}",
            "empty": "{{}}",
            "whitespace": "{{ }}",
            "mixed_valid_invalid": "Valid: {{name: String}} Invalid: {{incomplete"
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Should only extract the valid placeholder
        assert_eq!(handlebars.fields().len(), 1);

        let name_field = handlebars.get_field("name").unwrap();
        assert_eq!(name_field.occurrences.len(), 2); // In "valid" and "mixed_valid_invalid"

        let paths: Vec<&String> = name_field.occurrences.iter().map(|occ| &occ.json_path).collect();
        assert!(paths.contains(&&"valid".to_string()));
        assert!(paths.contains(&&"mixed_valid_invalid".to_string()));

        // Test rendering
        let mut values = TemplateFields::default();
        values.insert("name".to_string(), Value::String("Alice".to_string()));

        let result = handlebars.render(&values).unwrap();
        assert_eq!(result["valid"], "Alice");
        assert_eq!(result["mixed_valid_invalid"], "Valid: Alice Invalid: {{incomplete");
        assert_eq!(result["incomplete_open"], "{{name");
        assert_eq!(result["incomplete_close"], "name}}");
    }

    #[test]
    fn test_array_with_nested_structures() {
        let template = json!([
            {
                "type": "{{type1: String}}",
                "{{key1: String}}": "{{value1: String}}"
            },
            "{{item2: String}}",
            {
                "nested": {
                    "deep": "{{deep_value: String}}"
                }
            }
        ]);

        let handlebars = Handlebars::new(template).unwrap();

        // Check array element paths
        let type1_field = handlebars.get_field("type1").unwrap();
        assert_eq!(type1_field.occurrences[0].json_path, "[0].type");

        let key1_field = handlebars.get_field("key1").unwrap();
        assert_eq!(key1_field.occurrences[0].json_path, "[0].@key({{key1: String}})");
        assert_eq!(key1_field.occurrences[0].context_type, ContextType::ObjectKey);

        let value1_field = handlebars.get_field("value1").unwrap();
        assert_eq!(value1_field.occurrences[0].json_path, "[0].{{key1: String}}");

        let item2_field = handlebars.get_field("item2").unwrap();
        assert_eq!(item2_field.occurrences[0].json_path, "[1]");

        let deep_value_field = handlebars.get_field("deep_value").unwrap();
        assert_eq!(deep_value_field.occurrences[0].json_path, "[2].nested.deep");
    }

    #[test]
    fn test_performance_with_large_template() {
        // Create a template with many placeholders
        let mut template_obj = serde_json::Map::new();
        for i in 0..100 {
            template_obj.insert(
                format!("field_{}", i),
                Value::String("{{".to_string() + format!("value_{}: String", i).as_str() + "}}"),
            );
        }
        let template = Value::Object(template_obj);

        let start = std::time::Instant::now();
        let handlebars = Handlebars::new(template).unwrap();
        let compile_time = start.elapsed();

        assert_eq!(handlebars.fields().len(), 100);

        // Ensure compilation is reasonably fast (less than 10ms for 100 fields)
        assert!(compile_time.as_millis() < 10);

        // Test that all fields have correct paths
        for i in 0..100 {
            let field = handlebars.get_field(&format!("value_{}", i)).unwrap();
            assert_eq!(field.occurrences[0].json_path, format!("field_{}", i));
        }
    }

    #[test]
    fn test_render_with_position_tracking() {
        let template = json!({
            "config": {
                "{{env: String}}": {
                    "database_url": "{{protocol: String}}://{{host: String}}:{{port: Number}}/{{db_name: String}}",
                    "{{feature_flag: String}}": "{{enabled: Boolean}}"
                }
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut values = TemplateFields::default();
        values.insert("env".to_string(), Value::String("production".to_string()));
        values.insert("protocol".to_string(), Value::String("postgresql".to_string()));
        values.insert("host".to_string(), Value::String("db.example.com".to_string()));
        values.insert("port".to_string(), Value::Number(serde_json::Number::from(5432)));
        values.insert("db_name".to_string(), Value::String("myapp".to_string()));
        values.insert("feature_flag".to_string(), Value::String("new_ui".to_string()));
        values.insert("enabled".to_string(), Value::Bool(true));

        let result = handlebars.render(&values).unwrap();

        // Verify the structure was rendered correctly
        assert!(result["config"].as_object().unwrap().contains_key("production"));
        let prod_config = &result["config"]["production"];
        assert_eq!(prod_config["database_url"], "postgresql://db.example.com:5432/myapp");
        assert!(prod_config.as_object().unwrap().contains_key("new_ui"));
        assert_eq!(prod_config["new_ui"], true);

        // Verify position information is preserved during compilation
        let protocol_field = handlebars.get_field("protocol").unwrap();
        assert_eq!(protocol_field.occurrences[0].json_path, "config.{{env: String}}.database_url");
        assert!(protocol_field.occurrences[0].string_position.is_some());
    }

    // Legacy compatibility tests to ensure existing functionality still works
    #[test]
    fn test_legacy_basic_rendering() {
        let template = json!({
            "user": {
                "name": "{{ user_name: String }}",
                "age": "{{ age: Number }}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut values = TemplateFields::default();
        values.insert("user_name".to_string(), Value::String("Jane".to_string()));
        values.insert("age".to_string(), Value::Number(serde_json::Number::from(25)));

        let result = handlebars.render(&values).unwrap();

        assert_eq!(result["user"]["name"], "Jane");
        assert_eq!(result["user"]["age"], 25);
    }

    #[test]
    fn test_legacy_validation() {
        let template = json!({
            "name": "{{ user_name: String }}",
            "age": "{{ user_age: Number }}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut incomplete_values = TemplateFields::default();
        incomplete_values.insert("user_name".to_string(), Value::String("John".to_string()));

        let missing = handlebars.validate_values(&incomplete_values).unwrap();
        assert_eq!(missing, vec!["user_age"]);
    }

    #[test]
    fn test_legacy_batch_rendering() {
        let template = json!({"message": "{{ text: String }}"});
        let handlebars = Handlebars::new(template).unwrap();
        let compiled = handlebars.compile_for_repeated_use();

        let batch_data: Vec<TemplateFields> = (0..3)
            .map(|i| {
                let mut map = TemplateFields::default();
                map.insert("text".to_string(), Value::String(format!("message_{}", i)));
                map
            })
            .collect();

        let results = compiled.render_batch(&batch_data).unwrap();
        assert_eq!(results.len(), 3);

        for (i, result) in results.iter().enumerate() {
            assert_eq!(result["message"], format!("message_{}", i));
        }
    }

    #[test]
    fn test_optional_field_parsing() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}",
                "phone": "{{?phone: String}}"
            },
            "profile": "{{?bio: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let name_field = handlebars.get_field("user_name").unwrap();
        assert!(!name_field.is_optional());

        let email_field = handlebars.get_field("email").unwrap();
        assert!(email_field.is_optional());

        let phone_field = handlebars.get_field("phone").unwrap();
        assert!(phone_field.is_optional());

        let bio_field = handlebars.get_field("bio").unwrap();
        assert!(bio_field.is_optional());
    }

    #[allow(dead_code)]
    pub struct Conditions {
        key: String,       // eg. "email"
        conditions: Value, //eg.  json!({"{{email: String}}@gmail.com"}),
    }

    #[test]
    fn test_optional_field_validation() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Test with only required field - should pass
        let mut required_only = TemplateFields::default();
        required_only.insert("user_name".to_string(), Value::String("John".to_string()));

        let missing = handlebars.validate_values(&required_only).unwrap();
        assert!(missing.is_empty());

        // Test with missing required field - should fail
        let mut missing_required = TemplateFields::default();
        missing_required.insert("email".to_string(), Value::String("john@example.com".to_string()));

        let missing = handlebars.validate_values(&missing_required).unwrap();
        assert_eq!(missing, vec!["user_name"]);
    }

    #[test]
    fn test_optional_with_default() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String = john@example.com}}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut fields = TemplateFields::default();
        fields.insert("user_name".to_string(), Value::String("John".to_string()));

        let template = handlebars.render(&fields).unwrap();

        assert_eq!(template["user"]["name"], "John");
        assert_eq!(template["user"]["email"], "john@example.com");

        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        let mut fields = TemplateFields::default();
        fields.insert("user_name".to_string(), Value::String("John".to_string()));

        let template = handlebars.render(&fields).unwrap();

        assert_eq!(template["user"]["name"], "John");
        assert_eq!(template["user"]["email"], Value::Null);
    }

    #[test]
    fn test_optional_field_rendering() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            },
            "profile": "{{?bio: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Test with all fields provided
        let mut all_values = TemplateFields::default();
        all_values.insert("user_name".to_string(), Value::String("John".to_string()));
        all_values.insert("email".to_string(), Value::String("john@example.com".to_string()));
        all_values.insert("bio".to_string(), Value::String("Software developer".to_string()));

        let result = handlebars.render(&all_values).unwrap();
        assert_eq!(result["user"]["name"], "John");
        assert_eq!(result["user"]["email"], "john@example.com");
        assert_eq!(result["profile"], "Software developer");

        // Test with only required fields
        let mut required_only = TemplateFields::default();
        required_only.insert("user_name".to_string(), Value::String("Jane".to_string()));

        let result = handlebars.render(&required_only).unwrap();
        assert_eq!(result["user"]["name"], "Jane");
        assert_eq!(result["user"]["email"], Value::Null);
        assert_eq!(result["profile"], Value::Null);
    }

    #[test]
    fn test_optional_fields_in_mixed_text() {
        let template = json!({
            "greeting": "Hello {{user_name: String}}{{?title: String}}",
            "contact": "Email: {{?email: String}} Phone: {{?phone: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Test with all fields
        let mut all_values = TemplateFields::default();
        all_values.insert("user_name".to_string(), Value::String("John".to_string()));
        all_values.insert("title".to_string(), Value::String(", PhD".to_string()));
        all_values.insert("email".to_string(), Value::String("john@example.com".to_string()));
        all_values.insert("phone".to_string(), Value::String("123-456-7890".to_string()));

        let result = handlebars.render(&all_values).unwrap();
        assert_eq!(result["greeting"], "Hello John, PhD");
        assert_eq!(result["contact"], "Email: john@example.com Phone: 123-456-7890");

        // Test with missing optional fields
        let mut required_only = TemplateFields::default();
        required_only.insert("user_name".to_string(), Value::String("Jane".to_string()));

        let result = handlebars.render(&required_only).unwrap();
        assert_eq!(result["greeting"], "Hello Jane");
        assert_eq!(result["contact"], "Email:  Phone: ");
    }

    #[test]
    fn test_convenience_methods() {
        let template = json!({
            "required_field": "{{req: String}}",
            "optional_field": "{{?opt: String}}"
        });

        let handlebars = Handlebars::new(template).unwrap();

        let required: Vec<&str> = handlebars.required_fields().map(|f| f.name.as_str()).collect();
        assert_eq!(required, vec!["req"]);

        let optional: Vec<&str> = handlebars.optional_fields().map(|f| f.name.as_str()).collect();
        assert_eq!(optional, vec!["opt"]);
    }

    #[test]
    fn test_complex_nested_json_with_optional_fields() {
        let template = json!({
            "api": {
                "version": "{{api_version: String}}",
                "{{?environment: String}}": {
                    "database": {
                        "host": "{{db_host: String}}",
                        "port": "{{db_port: Number}}",
                        "ssl": "{{?ssl_enabled: Boolean}}",
                        "credentials": {
                            "username": "{{db_user: String}}",
                            "password": "{{?db_password: String}}",
                            "auth_method": "{{?auth_method: String}}"
                        },
                        "connection_pool": {
                            "min_connections": "{{?min_conn: Number}}",
                            "max_connections": "{{max_conn: Number}}",
                            "timeout": "{{?timeout_ms: Number}}"
                        }
                    },
                    "services": [
                        {
                            "name": "{{service_name: String}}",
                            "endpoint": "{{base_url: String}}/{{api_version: String}}/{{service_name: String}}",
                            "health_check": "{{base_url: String}}/health/{{?health_endpoint: String}}",
                            "config": {
                                "retries": "{{?max_retries: Number}}",
                                "timeout": "{{?service_timeout: Number}}",
                                "{{?feature_flag: String}}": "{{?feature_enabled: Boolean}}"
                            }
                        },
                        "{{?secondary_service: String}}"
                    ],
                    "monitoring": {
                        "enabled": "{{monitoring_enabled: Boolean}}",
                        "{{?metrics_provider: String}}": {
                            "api_key": "{{?metrics_api_key: String}}",
                            "endpoint": "{{?metrics_endpoint: String}}",
                            "tags": {
                                "environment": "{{?environment: String}}",
                                "version": "{{api_version: String}}",
                                "{{?custom_tag_key: String}}": "{{?custom_tag_value: String}}"
                            }
                        }
                    }
                }
            },
            "deployment": {
                "strategy": "{{deploy_strategy: String}}",
                "replicas": "{{replica_count: Number}}",
                "resources": {
                    "cpu": "{{cpu_limit: String}}",
                    "memory": "{{memory_limit: String}}",
                    "storage": "{{?storage_size: String}}"
                },
                "secrets": [
                    "{{?secret_name_1: String}}",
                    "{{?secret_name_2: String}}"
                ],
                "environment_vars": {
                    "LOG_LEVEL": "{{log_level: String}}",
                    "{{?custom_env_key: String}}": "{{?custom_env_value: String}}",
                    "DEBUG_MODE": "{{?debug_enabled: Boolean}}"
                }
            }
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Verify field extraction
        assert_eq!(handlebars.fields().len(), 36); // Total unique fields

        // Check required fields
        let required_fields: Vec<&str> = handlebars.required_fields().map(|f| f.name.as_str()).collect();
        let expected_required = vec![
            "api_version",
            "db_host",
            "db_port",
            "db_user",
            "max_conn",
            "service_name",
            "base_url",
            "monitoring_enabled",
            "deploy_strategy",
            "replica_count",
            "cpu_limit",
            "memory_limit",
            "log_level",
        ];
        for req in &expected_required {
            assert!(required_fields.contains(req), "Missing required field: {}", req);
        }

        // Check optional fields
        let optional_fields: Vec<&str> = handlebars.optional_fields().map(|f| f.name.as_str()).collect();
        let expected_optional = vec![
            "environment",
            "ssl_enabled",
            "db_password",
            "auth_method",
            "min_conn",
            "timeout_ms",
            "health_endpoint",
            "max_retries",
            "service_timeout",
            "feature_flag",
            "feature_enabled",
            "secondary_service",
            "metrics_provider",
            "metrics_api_key",
            "metrics_endpoint",
            "custom_tag_key",
            "custom_tag_value",
            "storage_size",
            "secret_name_1",
            "secret_name_2",
            "custom_env_key",
            "custom_env_value",
            "debug_enabled",
        ];
        for opt in &expected_optional {
            assert!(optional_fields.contains(opt), "Missing optional field: {}", opt);
        }

        // Test rendering with minimal required values
        let mut minimal_values = TemplateFields::default();
        minimal_values.insert("api_version".to_string(), Value::String("v2".to_string()));
        minimal_values.insert("db_host".to_string(), Value::String("localhost".to_string()));
        minimal_values.insert("db_port".to_string(), Value::Number(serde_json::Number::from(5432)));
        minimal_values.insert("db_user".to_string(), Value::String("admin".to_string()));
        minimal_values.insert("max_conn".to_string(), Value::Number(serde_json::Number::from(100)));
        minimal_values.insert("service_name".to_string(), Value::String("user-service".to_string()));
        minimal_values.insert("base_url".to_string(), Value::String("https://api.example.com".to_string()));
        minimal_values.insert("monitoring_enabled".to_string(), Value::Bool(true));
        minimal_values.insert("deploy_strategy".to_string(), Value::String("rolling".to_string()));
        minimal_values.insert("replica_count".to_string(), Value::Number(serde_json::Number::from(3)));
        minimal_values.insert("cpu_limit".to_string(), Value::String("1000m".to_string()));
        minimal_values.insert("memory_limit".to_string(), Value::String("2Gi".to_string()));
        minimal_values.insert("log_level".to_string(), Value::String("INFO".to_string()));

        // Should pass validation with only required fields
        let missing = handlebars.validate_values(&minimal_values).unwrap();
        assert!(missing.is_empty(), "Should not have missing required fields: {:?}", missing);

        let result = handlebars.render(&minimal_values).unwrap();

        // Verify required fields are rendered correctly
        assert_eq!(result["api"]["version"], "v2");
        assert_eq!(result["deployment"]["strategy"], "rolling");
        assert_eq!(result["deployment"]["replicas"], 3);

        // Verify optional fields are null when not provided
        assert_eq!(result["api"][""]["database"]["ssl"], Value::Null);
        assert_eq!(result["api"][""]["database"]["credentials"]["password"], Value::Null);
        assert_eq!(result["deployment"]["resources"]["storage"], Value::Null);

        // Test with some optional values provided
        let mut partial_values = minimal_values.clone();
        partial_values.insert("environment".to_string(), Value::String("production".to_string()));
        partial_values.insert("ssl_enabled".to_string(), Value::Bool(true));
        partial_values.insert("db_password".to_string(), Value::String("secret123".to_string()));
        partial_values.insert("max_retries".to_string(), Value::Number(serde_json::Number::from(3)));
        partial_values.insert("metrics_provider".to_string(), Value::String("datadog".to_string()));
        partial_values.insert("metrics_api_key".to_string(), Value::String("dd_key_123".to_string()));

        let result = handlebars.render(&partial_values).unwrap();

        // Verify optional fields are rendered when provided
        assert!(result["api"].as_object().unwrap().contains_key("production"));
        assert_eq!(result["api"]["production"]["database"]["ssl"], true);
        assert_eq!(result["api"]["production"]["database"]["credentials"]["password"], "secret123");
        assert_eq!(result["api"]["production"]["services"][0]["config"]["retries"], 3);
        assert!(result["api"]["production"]["monitoring"].as_object().unwrap().contains_key("datadog"));
        assert_eq!(result["api"]["production"]["monitoring"]["datadog"]["api_key"], "dd_key_123");

        // Test complex URL construction with mixed optional/required fields
        let service_endpoint = &result["api"]["production"]["services"][0]["endpoint"];
        assert_eq!(service_endpoint, "https://api.example.com/v2/user-service");

        let health_check = &result["api"]["production"]["services"][0]["health_check"];
        assert_eq!(health_check, "https://api.example.com/health/"); // Optional health_endpoint is empty

        // Test field occurrence tracking for fields that appear multiple times
        let api_version_field = handlebars.get_field("api_version").unwrap();
        assert_eq!(api_version_field.occurrences.len(), 3); // Appears in version, endpoint, and tags

        let environment_field = handlebars.get_field("environment").unwrap();
        assert!(environment_field.is_optional());
        assert_eq!(environment_field.occurrences.len(), 2); // In key and in tags

        let base_url_field = handlebars.get_field("base_url").unwrap();
        assert!(!base_url_field.is_optional());
        assert_eq!(base_url_field.occurrences.len(), 2); // In endpoint and health_check

        // Test validation report with complex structure
        let report = handlebars.validate_values_detailed(&partial_values).unwrap();
        assert!(report.missing_fields.is_empty());
        assert_eq!(report.field_usage.len(), 19); // Number of provided fields

        // Verify specific field usage info
        let ssl_usage = report.field_usage.iter().find(|u| u.field_name == "ssl_enabled").unwrap();
        assert!(ssl_usage.is_optional());
        assert_eq!(ssl_usage.provided_value_type, Some(FieldType::Boolean));

        let db_host_usage = report.field_usage.iter().find(|u| u.field_name == "db_host").unwrap();
        assert!(!db_host_usage.is_optional());
        assert_eq!(db_host_usage.provided_value_type, Some(FieldType::String));
    }
    #[test]
    fn test_conditional_block_basic() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            }
        });

        let conditional_block = ConditionalBlock {
            trigger_field: "email".to_string(),
            template_addition: json!({
                "user": {
                    "email_domain": "{{email: String}}@gmail.com",
                    "email_settings": {
                        "verified": "{{?email_verified: Boolean}}"
                    }
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![conditional_block]).unwrap();

        // Test with email provided
        let mut values_with_email = TemplateFields::default();
        values_with_email.insert("user_name".to_string(), Value::String("John".to_string()));
        values_with_email.insert("email".to_string(), Value::String("john".to_string()));
        values_with_email.insert("email_verified".to_string(), Value::Bool(true));

        let result = handlebars.render(&values_with_email).unwrap();
        assert_eq!(result["user"]["name"], "John");
        assert_eq!(result["user"]["email"], "john");
        assert_eq!(result["user"]["email_domain"], "john@gmail.com");
        assert_eq!(result["user"]["email_settings"]["verified"], true);

        // Test without email
        let mut values_without_email = TemplateFields::default();
        values_without_email.insert("user_name".to_string(), Value::String("Jane".to_string()));

        let result = handlebars.render(&values_without_email).unwrap();
        assert_eq!(result["user"]["name"], "Jane");
        assert_eq!(result["user"]["email"], Value::Null);
        // Conditional block fields should not be present
        assert!(!result["user"].as_object().unwrap().contains_key("email_domain"));
        assert!(!result["user"].as_object().unwrap().contains_key("email_settings"));
    }

    #[test]
    fn test_multiple_conditional_blocks() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}",
                "phone": "{{?phone: String}}"
            }
        });

        let email_block = ConditionalBlock {
            trigger_field: "email".to_string(),
            template_addition: json!({
                "user": {
                    "email_notifications": "{{?email_notifications: Boolean}}",
                    "email_marketing": {
                        "enabled": "{{?marketing_emails: Boolean}}"
                    }
                }
            }),
            merge_at_path: None,
        };

        let phone_block = ConditionalBlock {
            trigger_field: "phone".to_string(),
            template_addition: json!({
                "user": {
                    "sms_notifications": "{{?sms_notifications: Boolean}}",
                    "phone_verified": "{{?phone_verified: Boolean}}"
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![email_block, phone_block]).unwrap();

        // Test with both email and phone
        let mut values_both = TemplateFields::default();
        values_both.insert("user_name".to_string(), Value::String("Alice".to_string()));
        values_both.insert("email".to_string(), Value::String("alice@example.com".to_string()));
        values_both.insert("phone".to_string(), Value::String("123-456-7890".to_string()));
        values_both.insert("email_notifications".to_string(), Value::Bool(true));
        values_both.insert("sms_notifications".to_string(), Value::Bool(false));

        let result = handlebars.render(&values_both).unwrap();
        assert_eq!(result["user"]["name"], "Alice");
        assert_eq!(result["user"]["email"], "alice@example.com");
        assert_eq!(result["user"]["phone"], "123-456-7890");
        assert_eq!(result["user"]["email_notifications"], true);
        assert_eq!(result["user"]["sms_notifications"], false);
        assert_eq!(result["user"]["email_marketing"]["enabled"], Value::Null);

        // Test with only email
        let mut values_email_only = TemplateFields::default();
        values_email_only.insert("user_name".to_string(), Value::String("Bob".to_string()));
        values_email_only.insert("email".to_string(), Value::String("bob@example.com".to_string()));
        values_email_only.insert("marketing_emails".to_string(), Value::Bool(true));

        let result = handlebars.render(&values_email_only).unwrap();
        assert_eq!(result["user"]["name"], "Bob");
        assert_eq!(result["user"]["email"], "bob@example.com");
        assert_eq!(result["user"]["phone"], Value::Null);
        assert_eq!(result["user"]["email_marketing"]["enabled"], true);
        // Phone-related fields should not be present
        assert!(!result["user"].as_object().unwrap().contains_key("sms_notifications"));
        assert!(!result["user"].as_object().unwrap().contains_key("phone_verified"));
    }

    #[test]
    fn test_conditional_block_with_null_trigger_field() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            }
        });

        let conditional_block = ConditionalBlock {
            trigger_field: "email".to_string(),
            template_addition: json!({
                "user": {
                    "email_domain": "gmail.com"
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![conditional_block]).unwrap();

        // Test with email explicitly set to null
        let mut values_null_email = TemplateFields::default();
        values_null_email.insert("user_name".to_string(), Value::String("Charlie".to_string()));
        values_null_email.insert("email".to_string(), Value::Null);

        let result = handlebars.render(&values_null_email).unwrap();
        assert_eq!(result["user"]["name"], "Charlie");
        assert_eq!(result["user"]["email"], Value::Null);
        // Conditional block should not be applied since email is null
        assert!(!result["user"].as_object().unwrap().contains_key("email_domain"));
    }

    #[test]
    fn test_conditional_field_extraction() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}",
                "email": "{{?email: String}}"
            }
        });

        let conditional_block = ConditionalBlock {
            trigger_field: "email".to_string(),
            template_addition: json!({
                "user": {
                    "email_domain": "{{email: String}}@{{domain: String}}",
                    "settings": {
                        "notifications": "{{?notifications: Boolean}}",
                        "theme": "{{theme: String}}"
                    }
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![conditional_block]).unwrap();

        // Verify all fields are extracted
        assert_eq!(handlebars.fields().len(), 5); // user_name, email, domain, notifications, theme

        // Check base template fields
        let base_fields: Vec<&FieldInfo> = handlebars
            .fields()
            .iter()
            .filter(|f| f.occurrences.iter().any(|occ| matches!(occ.source, FieldSource::BaseTemplate)))
            .collect();
        assert_eq!(base_fields.len(), 2); // user_name, email

        // Check conditional block fields
        let conditional_fields: Vec<&FieldInfo> = handlebars.fields_from_conditional_blocks();
        assert_eq!(conditional_fields.len(), 4); // email (reused), domain, notifications, theme

        // Verify field requirements
        let domain_field = handlebars.get_field("domain").unwrap();
        assert_eq!(domain_field.requirement, FieldRequirement::Required);

        let notifications_field = handlebars.get_field("notifications").unwrap();
        assert_eq!(notifications_field.requirement, FieldRequirement::Optional);

        // Verify field sources
        let email_field = handlebars.get_field("email").unwrap();
        assert_eq!(email_field.occurrences.len(), 2); // One from base, one from conditional
        assert!(email_field.occurrences.iter().any(|occ| matches!(occ.source, FieldSource::BaseTemplate)));
        assert!(email_field.occurrences.iter().any(|occ| matches!(occ.source, FieldSource::ConditionalBlock(0))));
    }

    #[test]
    fn test_conditional_block_deep_merge() {
        let template = json!({
            "config": {
                "database": {
                    "host": "{{db_host: String}}",
                    "port": "{{db_port: Number}}"
                },
                "logging": {
                    "level": "{{log_level: String}}"
                }
            }
        });

        let ssl_block = ConditionalBlock {
            trigger_field: "ssl_enabled".to_string(),
            template_addition: json!({
                "config": {
                    "database": {
                        "ssl": true,
                        "ssl_cert": "{{ssl_cert: String}}",
                        "ssl_key": "{{ssl_key: String}}"
                    },
                    "security": {
                        "tls_version": "{{tls_version: String}}"
                    }
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![ssl_block]).unwrap();

        // Test with SSL enabled
        let mut values_with_ssl = TemplateFields::default();
        values_with_ssl.insert("db_host".to_string(), Value::String("localhost".to_string()));
        values_with_ssl.insert("db_port".to_string(), Value::Number(serde_json::Number::from(5432)));
        values_with_ssl.insert("log_level".to_string(), Value::String("INFO".to_string()));
        values_with_ssl.insert("ssl_enabled".to_string(), Value::Bool(true));
        values_with_ssl.insert("ssl_cert".to_string(), Value::String("/path/to/cert".to_string()));
        values_with_ssl.insert("ssl_key".to_string(), Value::String("/path/to/key".to_string()));
        values_with_ssl.insert("tls_version".to_string(), Value::String("1.3".to_string()));

        let result = handlebars.render(&values_with_ssl).unwrap();
        assert_eq!(result["config"]["database"]["host"], "localhost");
        assert_eq!(result["config"]["database"]["port"], 5432);
        assert_eq!(result["config"]["database"]["ssl"], true);
        assert_eq!(result["config"]["database"]["ssl_cert"], "/path/to/cert");
        assert_eq!(result["config"]["logging"]["level"], "INFO");
        assert_eq!(result["config"]["security"]["tls_version"], "1.3");

        // Test without SSL
        let mut values_without_ssl = TemplateFields::default();
        values_without_ssl.insert("db_host".to_string(), Value::String("localhost".to_string()));
        values_without_ssl.insert("db_port".to_string(), Value::Number(serde_json::Number::from(5432)));
        values_without_ssl.insert("log_level".to_string(), Value::String("INFO".to_string()));

        let result = handlebars.render(&values_without_ssl).unwrap();
        assert_eq!(result["config"]["database"]["host"], "localhost");
        assert_eq!(result["config"]["database"]["port"], 5432);
        assert_eq!(result["config"]["logging"]["level"], "INFO");
        // SSL and security sections should not be present
        assert!(!result["config"]["database"].as_object().unwrap().contains_key("ssl"));
        assert!(!result["config"].as_object().unwrap().contains_key("security"));
    }

    #[test]
    fn test_conditional_block_array_extension() {
        let template = json!({
            "services": [
                {
                    "name": "core",
                    "port": "{{core_port: Number}}"
                }
            ]
        });

        let monitoring_block = ConditionalBlock {
            trigger_field: "monitoring_enabled".to_string(),
            template_addition: json!({
                "services": [
                    {
                        "name": "prometheus",
                        "port": "{{prometheus_port: Number}}"
                    },
                    {
                        "name": "grafana",
                        "port": "{{grafana_port: Number}}"
                    }
                ]
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![monitoring_block]).unwrap();

        // Test with monitoring enabled
        let mut values_with_monitoring = TemplateFields::default();
        values_with_monitoring.insert("core_port".to_string(), Value::Number(serde_json::Number::from(8080)));
        values_with_monitoring.insert("monitoring_enabled".to_string(), Value::Bool(true));
        values_with_monitoring.insert("prometheus_port".to_string(), Value::Number(serde_json::Number::from(9090)));
        values_with_monitoring.insert("grafana_port".to_string(), Value::Number(serde_json::Number::from(3000)));

        let result = handlebars.render(&values_with_monitoring).unwrap();
        let services = result["services"].as_array().unwrap();
        assert_eq!(services.len(), 3);
        assert_eq!(services[0]["name"], "core");
        assert_eq!(services[0]["port"], 8080);
        assert_eq!(services[1]["name"], "prometheus");
        assert_eq!(services[1]["port"], 9090);
        assert_eq!(services[2]["name"], "grafana");
        assert_eq!(services[2]["port"], 3000);

        // Test without monitoring
        let mut values_without_monitoring = TemplateFields::default();
        values_without_monitoring.insert("core_port".to_string(), Value::Number(serde_json::Number::from(8080)));

        let result = handlebars.render(&values_without_monitoring).unwrap();
        let services = result["services"].as_array().unwrap();
        assert_eq!(services.len(), 1);
        assert_eq!(services[0]["name"], "core");
        assert_eq!(services[0]["port"], 8080);
    }

    #[test]
    fn test_conditional_block_validation() {
        let template = json!({
            "user": {
                "name": "{{user_name: String}}"
            }
        });

        let profile_block = ConditionalBlock {
            trigger_field: "profile_enabled".to_string(),
            template_addition: json!({
                "user": {
                    "bio": "{{bio: String}}",
                    "avatar": "{{?avatar_url: String}}"
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![profile_block]).unwrap();

        // Test validation with profile disabled (should only require user_name)
        let mut values_profile_disabled = TemplateFields::default();
        values_profile_disabled.insert("user_name".to_string(), Value::String("John".to_string()));

        let missing = handlebars.validate_values(&values_profile_disabled).unwrap();
        assert!(missing.is_empty());

        // Test validation with profile enabled but missing bio
        let mut values_profile_enabled_incomplete = TemplateFields::default();
        values_profile_enabled_incomplete.insert("user_name".to_string(), Value::String("John".to_string()));
        values_profile_enabled_incomplete.insert("profile_enabled".to_string(), Value::Bool(true));
        // Missing bio, but that's only required if the conditional block is applied during rendering

        let missing = handlebars.validate_values(&values_profile_enabled_incomplete).unwrap();
        // Note: Current validation doesn't check conditional requirements
        // This is expected behavior as validation is based on field definitions, not runtime conditions
        assert_eq!(missing, vec!["bio"]);
    }

    #[test]
    fn test_conditional_block_field_positions() {
        let template = json!({
            "message": "Hello {{name: String}}"
        });

        let signature_block = ConditionalBlock {
            trigger_field: "include_signature".to_string(),
            template_addition: json!({
                "signature": "Best regards, {{author: String}} from {{company: String}}"
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![signature_block]).unwrap();

        // Verify field positions are tracked correctly
        let name_field = handlebars.get_field("name").unwrap();
        assert_eq!(name_field.occurrences.len(), 1);
        assert!(matches!(name_field.occurrences[0].source, FieldSource::BaseTemplate));

        let author_field = handlebars.get_field("author").unwrap();
        assert_eq!(author_field.occurrences.len(), 1);
        assert!(matches!(author_field.occurrences[0].source, FieldSource::ConditionalBlock(0)));

        let company_field = handlebars.get_field("company").unwrap();
        assert_eq!(company_field.occurrences.len(), 1);
        assert!(matches!(company_field.occurrences[0].source, FieldSource::ConditionalBlock(0)));

        // Test that string positions are correctly calculated
        let pos = author_field.occurrences[0].string_position.as_ref().unwrap();
        assert_eq!(pos.start_byte, 14); // Position in "Best regards, {{author: String}} from..."
        assert_eq!(pos.end_byte, 32);
    }

    #[test]
    fn test_handlebars_cache_with_conditions() {
        let mut cache = HandlebarsCache::new();

        let template = json!({
            "user": {
                "name": "{{user_name: String}}"
            }
        });

        let conditional_block = ConditionalBlock {
            trigger_field: "premium".to_string(),
            template_addition: json!({
                "user": {
                    "tier": "premium",
                    "features": ["advanced_analytics", "priority_support"]
                }
            }),
            merge_at_path: None,
        };

        let template_id = TemplateId::from(1.to_string());
        let template_uuid = TemplateUuid::new_uuid();

        let compiled =
            cache.register_with_conditions(template_id.clone(), template_uuid.clone(), template, vec![conditional_block]).unwrap();

        // Test retrieval
        let retrieved_by_id = cache.get_id_template(&template_id).unwrap();
        let retrieved_by_uuid = cache.get_uuid_template(&template_uuid).unwrap();

        assert_eq!(Arc::as_ptr(&compiled), Arc::as_ptr(&retrieved_by_id));
        assert_eq!(Arc::as_ptr(&compiled), Arc::as_ptr(&retrieved_by_uuid));

        // Test that conditional blocks are preserved
        assert_eq!(compiled.conditional_blocks().len(), 1);
        assert_eq!(compiled.conditional_blocks()[0].trigger_field, "premium");
    }

    #[test]
    fn test_required_wins_over_optional() {
        let template = json!({
            "required_usage": "{{user_id: String}}",      // Required
            "optional_usage": "{{?user_id: String}}",     // Optional
            "mixed_string": "ID: {{user_id: String}} ({{?user_id: String}})"
        });

        let handlebars = Handlebars::new(template).unwrap();

        // Field should be required because at least one occurrence is required
        let user_id_field = handlebars.get_field("user_id").unwrap();
        assert_eq!(user_id_field.requirement, FieldRequirement::Required);
        assert_eq!(user_id_field.occurrences.len(), 4); // Three total occurrences

        // Validation should require the field
        let empty_values = TemplateFields::default();
        let missing = handlebars.validate_values(&empty_values).unwrap();
        assert_eq!(missing, vec!["user_id"]);

        // Should work when provided
        let mut values = TemplateFields::default();
        values.insert("user_id".to_string(), Value::String("123".to_string()));
        let missing = handlebars.validate_values(&values).unwrap();
        assert!(missing.is_empty());

        // Test rendering
        let result = handlebars.render(&values).unwrap();
        assert_eq!(result["required_usage"], "123");
        assert_eq!(result["optional_usage"], "123");
        assert_eq!(result["mixed_string"], "ID: 123 (123)");
    }

    #[test]
    fn test_complex_conditional_scenario() {
        let template = json!({
            "api": {
                "version": "{{api_version: String}}",
                "endpoints": {
                    "users": "/api/{{api_version: String}}/users"
                }
            }
        });

        let auth_block = ConditionalBlock {
            trigger_field: "auth_enabled".to_string(),
            template_addition: json!({
                "api": {
                    "auth": {
                        "type": "{{auth_type: String}}",
                        "endpoint": "/api/{{api_version: String}}/auth"
                    }
                }
            }),
            merge_at_path: None,
        };

        let rate_limit_block = ConditionalBlock {
            trigger_field: "rate_limiting".to_string(),
            template_addition: json!({
                "api": {
                    "rate_limit": {
                        "requests_per_minute": "{{rpm: Number}}",
                        "burst_size": "{{?burst_size: Number}}"
                    }
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(template, vec![auth_block, rate_limit_block]).unwrap();

        // Test all combinations
        let test_cases = vec![
            (false, false, vec!["api_version"]),
            (true, false, vec!["api_version", "auth_type"]),
            (false, true, vec!["api_version", "rpm"]),
            (true, true, vec!["api_version", "auth_type", "rpm"]),
        ];

        for (auth_enabled, rate_limiting, _expected_keys) in test_cases {
            let mut values = TemplateFields::default();
            values.insert("api_version".to_string(), Value::String("v1".to_string()));

            if auth_enabled {
                values.insert("auth_enabled".to_string(), Value::Bool(true));
                values.insert("auth_type".to_string(), Value::String("JWT".to_string()));
            }

            if rate_limiting {
                values.insert("rate_limiting".to_string(), Value::Bool(true));
                values.insert("rpm".to_string(), Value::Number(serde_json::Number::from(1000)));
                values.insert("burst_size".to_string(), Value::Number(serde_json::Number::from(100)));
            }

            let result = handlebars.render(&values).unwrap();

            // Verify basic structure
            assert_eq!(result["api"]["version"], "v1");
            assert_eq!(result["api"]["endpoints"]["users"], "/api/v1/users");

            // Verify conditional sections
            if auth_enabled {
                assert!(result["api"].as_object().unwrap().contains_key("auth"));
                assert_eq!(result["api"]["auth"]["type"], "JWT");
                assert_eq!(result["api"]["auth"]["endpoint"], "/api/v1/auth");
            } else {
                assert!(!result["api"].as_object().unwrap().contains_key("auth"));
            }

            if rate_limiting {
                assert!(result["api"].as_object().unwrap().contains_key("rate_limit"));
                assert_eq!(result["api"]["rate_limit"]["requests_per_minute"], 1000);
                assert_eq!(result["api"]["rate_limit"]["burst_size"], 100);
            } else {
                assert!(!result["api"].as_object().unwrap().contains_key("rate_limit"));
            }
        }
    }

    #[test]
    fn test_database_configuration_mongodb_vs_postgresql() {
        // Base configuration template that works for any database
        let base_template = json!({
            "service": {
                "name": "{{service_name: String}}",
                "version": "{{version: String}}",
                "environment": "{{env: String}}"
            },
            "database": {
                "type": "{{db_type: String}}",
                "host": "{{db_host: String}}",
                "port": "{{db_port: Number}}",
                "name": "{{db_name: String}}",
                "timeout_ms": "{{?timeout: Number}}"
            },
            "logging": {
                "level": "{{log_level: String}}",
                "database_queries": "{{?log_queries: Boolean}}"
            }
        });

        // MongoDB-specific configuration
        let mongodb_block = ConditionalBlock {
            trigger_field: "use_mongodb".to_string(),
            template_addition: json!({
                "database": {
                    "connection_string": "mongodb://{{db_host: String}}:{{db_port: Number}}/{{db_name: String}}",
                    "replica_set": "{{?replica_set: String}}",
                    "auth_source": "{{?auth_db: String}}",
                    "options": {
                        "max_pool_size": "{{?max_connections: Number}}",
                        "server_selection_timeout_ms": "{{?selection_timeout: Number}}",
                        "write_concern": "{{?write_concern: String}}"
                    }
                },
                "indexes": [
                    {
                        "collection": "{{main_collection: String}}",
                        "fields": ["{{index_field: String}}"],
                        "unique": "{{?unique_index: Boolean}}"
                    }
                ],
                "aggregation": {
                    "pipeline_timeout_ms": "{{?pipeline_timeout: Number}}"
                }
            }),
            merge_at_path: None,
        };

        // PostgreSQL-specific configuration
        let postgresql_block = ConditionalBlock {
            trigger_field: "use_postgresql".to_string(),
            template_addition: json!({
                "database": {
                    "connection_string": "postgresql://{{?db_user: String}}:{{?db_password: String}}@{{db_host: String}}:{{db_port: Number}}/{{db_name: String}}",
                    "schema": "{{?db_schema: String}}",
                    "ssl_mode": "{{?ssl_mode: String}}",
                    "pool": {
                        "min_connections": "{{?min_connections: Number}}",
                        "max_connections": "{{?max_connections: Number}}",
                        "connection_timeout_ms": "{{?connection_timeout: Number}}"
                    }
                },
                "migrations": {
                    "auto_migrate": "{{?auto_migrate: Boolean}}",
                    "migration_table": "{{?migration_table: String}}"
                },
                "query_optimization": {
                    "enable_prepared_statements": "{{?prepared_statements: Boolean}}",
                    "statement_cache_size": "{{?cache_size: Number}}"
                }
            }),
            merge_at_path: None,
        };

        let handlebars = Handlebars::new_with_conditions(base_template, vec![mongodb_block, postgresql_block]).unwrap();

        // Test MongoDB configuration
        let mut mongodb_values = TemplateFields::default();
        mongodb_values.insert("service_name".to_string(), Value::String("user-service".to_string()));
        mongodb_values.insert("version".to_string(), Value::String("1.2.3".to_string()));
        mongodb_values.insert("env".to_string(), Value::String("production".to_string()));
        mongodb_values.insert("db_type".to_string(), Value::String("mongodb".to_string()));
        mongodb_values.insert("db_host".to_string(), Value::String("mongo-cluster.example.com".to_string()));
        mongodb_values.insert("db_port".to_string(), Value::Number(serde_json::Number::from(27017)));
        mongodb_values.insert("db_name".to_string(), Value::String("userdb".to_string()));
        mongodb_values.insert("log_level".to_string(), Value::String("INFO".to_string()));
        mongodb_values.insert("use_mongodb".to_string(), Value::Bool(true));
        mongodb_values.insert("replica_set".to_string(), Value::String("rs0".to_string()));
        mongodb_values.insert("max_connections".to_string(), Value::Number(serde_json::Number::from(100)));
        mongodb_values.insert("main_collection".to_string(), Value::String("users".to_string()));
        mongodb_values.insert("index_field".to_string(), Value::String("email".to_string()));
        mongodb_values.insert("unique_index".to_string(), Value::Bool(true));

        let mongodb_result = handlebars.render(&mongodb_values).unwrap();

        // Verify MongoDB-specific configuration
        assert_eq!(mongodb_result["service"]["name"], "user-service");
        assert_eq!(mongodb_result["database"]["type"], "mongodb");
        assert_eq!(mongodb_result["database"]["connection_string"], "mongodb://mongo-cluster.example.com:27017/userdb");
        assert_eq!(mongodb_result["database"]["replica_set"], "rs0");
        assert_eq!(mongodb_result["database"]["options"]["max_pool_size"], 100);
        assert_eq!(mongodb_result["indexes"][0]["collection"], "users");
        assert_eq!(mongodb_result["indexes"][0]["fields"][0], "email");
        assert_eq!(mongodb_result["indexes"][0]["unique"], true);
        // PostgreSQL-specific fields should not be present
        assert!(!mongodb_result["database"].as_object().unwrap().contains_key("schema"));
        assert!(!mongodb_result.as_object().unwrap().contains_key("migrations"));

        // Test PostgreSQL configuration
        let mut postgresql_values = TemplateFields::default();
        postgresql_values.insert("service_name".to_string(), Value::String("analytics-service".to_string()));
        postgresql_values.insert("version".to_string(), Value::String("2.1.0".to_string()));
        postgresql_values.insert("env".to_string(), Value::String("staging".to_string()));
        postgresql_values.insert("db_type".to_string(), Value::String("postgresql".to_string()));
        postgresql_values.insert("db_host".to_string(), Value::String("postgres.internal".to_string()));
        postgresql_values.insert("db_port".to_string(), Value::Number(serde_json::Number::from(5432)));
        postgresql_values.insert("db_name".to_string(), Value::String("analytics".to_string()));
        postgresql_values.insert("log_level".to_string(), Value::String("DEBUG".to_string()));
        postgresql_values.insert("use_postgresql".to_string(), Value::Bool(true));
        postgresql_values.insert("db_user".to_string(), Value::String("app_user".to_string()));
        postgresql_values.insert("db_password".to_string(), Value::String("secure_password".to_string()));
        postgresql_values.insert("db_schema".to_string(), Value::String("public".to_string()));
        postgresql_values.insert("ssl_mode".to_string(), Value::String("require".to_string()));
        postgresql_values.insert("max_connections".to_string(), Value::Number(serde_json::Number::from(50)));
        postgresql_values.insert("auto_migrate".to_string(), Value::Bool(true));
        postgresql_values.insert("prepared_statements".to_string(), Value::Bool(true));

        let postgresql_result = handlebars.render(&postgresql_values).unwrap();

        // Verify PostgreSQL-specific configuration
        assert_eq!(postgresql_result["service"]["name"], "analytics-service");
        assert_eq!(postgresql_result["database"]["type"], "postgresql");
        assert_eq!(
            postgresql_result["database"]["connection_string"],
            "postgresql://app_user:secure_password@postgres.internal:5432/analytics"
        );
        assert_eq!(postgresql_result["database"]["schema"], "public");
        assert_eq!(postgresql_result["database"]["ssl_mode"], "require");
        assert_eq!(postgresql_result["database"]["pool"]["max_connections"], 50);
        assert_eq!(postgresql_result["migrations"]["auto_migrate"], true);
        assert_eq!(postgresql_result["query_optimization"]["enable_prepared_statements"], true);
        // MongoDB-specific fields should not be present
        assert!(!postgresql_result["database"].as_object().unwrap().contains_key("replica_set"));
        assert!(!postgresql_result.as_object().unwrap().contains_key("indexes"));
        assert!(!postgresql_result.as_object().unwrap().contains_key("aggregation"));

        // Test minimal configuration (neither MongoDB nor PostgreSQL specific features)
        let mut minimal_values = TemplateFields::default();
        minimal_values.insert("service_name".to_string(), Value::String("simple-service".to_string()));
        minimal_values.insert("version".to_string(), Value::String("1.0.0".to_string()));
        minimal_values.insert("env".to_string(), Value::String("development".to_string()));
        minimal_values.insert("db_type".to_string(), Value::String("sqlite".to_string()));
        minimal_values.insert("db_host".to_string(), Value::String("localhost".to_string()));
        minimal_values.insert("db_port".to_string(), Value::Number(serde_json::Number::from(5432)));
        minimal_values.insert("db_name".to_string(), Value::String("simple.db".to_string()));
        minimal_values.insert("log_level".to_string(), Value::String("WARN".to_string()));

        let minimal_result = handlebars.render(&minimal_values).unwrap();

        // Verify only base configuration is present
        assert_eq!(minimal_result["service"]["name"], "simple-service");
        assert_eq!(minimal_result["database"]["type"], "sqlite");
        assert_eq!(minimal_result["database"]["timeout_ms"], Value::Null);
        assert_eq!(minimal_result["logging"]["database_queries"], Value::Null);
        // No database-specific sections should be present
        assert!(!minimal_result["database"].as_object().unwrap().contains_key("connection_string"));
        assert!(!minimal_result.as_object().unwrap().contains_key("indexes"));
        assert!(!minimal_result.as_object().unwrap().contains_key("migrations"));

        // Test validation - should require database-specific fields when triggered
        let missing_mongodb = handlebars.validate_values(&mongodb_values).unwrap();
        assert!(missing_mongodb.is_empty());

        let missing_postgresql = handlebars.validate_values(&postgresql_values).unwrap();
        assert!(missing_postgresql.is_empty());

        let missing_minimal = handlebars.validate_values(&minimal_values).unwrap();
        assert!(missing_minimal.is_empty());

        // Test field extraction - verify both database-specific fields are captured
        let all_fields: Vec<&str> = handlebars.fields().iter().map(|f| f.name.as_str()).collect();
        assert!(all_fields.contains(&"replica_set")); // MongoDB-specific
        assert!(all_fields.contains(&"ssl_mode")); // PostgreSQL-specific
        assert!(all_fields.contains(&"main_collection")); // MongoDB-specific
        assert!(all_fields.contains(&"auto_migrate")); // PostgreSQL-specific

        // Verify conditional fields are properly sourced
        let conditional_fields = handlebars.fields_from_conditional_blocks();
        assert!(conditional_fields.len() > 10); // Should have many conditional fields

        // Verify field reuse (max_connections appears in both database types)
        let max_conn_field = handlebars.get_field("max_connections").unwrap();
        assert_eq!(max_conn_field.occurrences.len(), 2); // Used in both MongoDB and PostgreSQL blocks
    }
}
