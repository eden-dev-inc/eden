use crate::database::api::fields::ApiFieldName;
use crate::database::template::wrapper::TemplateFieldName;
use crate::database::template::{ApiFields, TemplateFields};
use eden_logger_internal::{ctx_with_trace, log_trace};
use error::{EpError, ResultEP};
use format::TemplateUuid;
use function_name::named;
use postgres_types::{FromSql, ToSql, Type};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Binding {
    template: TemplateUuid,
    /// Template field name, api field name
    fields: Vec<(TemplateFieldName, ApiFieldName)>,
}

impl Binding {
    pub fn new(template: TemplateUuid, fields: Vec<(TemplateFieldName, ApiFieldName)>) -> Self {
        Self { template, fields }
    }
    pub fn template(&self) -> &TemplateUuid {
        &self.template
    }
    pub fn fields(&self) -> &[(TemplateFieldName, ApiFieldName)] {
        &self.fields
    }
    /// Returns an object that matches the exact structure of the requested data.
    pub fn map_template_fields(&self, api_fields: ApiFields) -> ResultEP<TemplateFields> {
        let mut output = TemplateFields::default();

        for (template_field, api_field) in &self.fields {
            let api_field = api_field.to_string();

            let mut nested_object = api_fields.clone().into();
            // Using dot notation iterate into the nested object
            for binding_part in api_field.split('.') {
                nested_object = nested_mapping(&nested_object, binding_part)?;
            }

            output.insert(template_field.to_string(), nested_object.to_owned());
        }

        Ok(output)
    }
    pub fn map_value(&self, object: Value) -> ResultEP<TemplateFields> {
        let mut output = TemplateFields::default();

        for (template_field, api_field) in &self.fields {
            let api_field = api_field.to_string();

            let mut nested_object = object.to_owned();
            // Using dot notation iterate into the nested object
            for binding_part in api_field.split('.') {
                nested_object = nested_mapping(&nested_object, binding_part)?;
            }

            output.insert(template_field.to_string(), nested_object.to_owned());
        }

        Ok(output)
    }
}

#[named]
fn nested_mapping(object: &Value, binding_part: &str) -> ResultEP<Value> {
    let _ctx = ctx_with_trace!().with_feature("ep_core");
    log_trace!(_ctx, "nested_mapping: {object}", audience = eden_logger_internal::LogAudience::Internal);
    let _ctx = ctx_with_trace!().with_feature("ep_core");
    log_trace!(
        _ctx,
        "{}",
        audience = eden_logger_internal::LogAudience::Internal,
        details = std::backtrace::Backtrace::force_capture()
    );

    match object {
        Value::Object(map) => match map.get(binding_part) {
            Some(value) => Ok(value.to_owned()),
            None => Err(EpError::api(format!("no key `{}` exists in the object: {object}", binding_part)))?,
        },
        Value::Array(vec) => {
            if vec.is_empty() {
                Ok(Value::Null)
            } else if vec.len() == 1 {
                Ok(nested_mapping(&vec[0], binding_part)?)
            } else {
                let mut nested_vec = Vec::new();
                for value in vec {
                    nested_vec.push(nested_mapping(value, binding_part)?);
                }

                Ok(Value::Array(nested_vec))
            }
        }
        Value::String(string) => Err(EpError::api(format!(
            "expected object or array, found a string `{string}` for key `{binding_part}`"
        ))),
        Value::Number(number) => Err(EpError::api(format!(
            "expected object or array, found a number `{number}` for key `{binding_part}`"
        ))),
        Value::Bool(boolean) => Err(EpError::api(format!("expected object or array, found a bool `{boolean}` for key `{binding_part}`"))),
        Value::Null => Err(EpError::api(format!("expected object or array, found a null for key `{binding_part}`"))),
    }
}

fn total_permutations(permutations: &Vec<TemplateFields>) -> usize {
    let mut total = 1;
    for permutation in permutations {
        total *= permutation.map().len();
    }
    total
}

pub fn find_permutations(object: &TemplateFields) -> Vec<TemplateFields> {
    let mut permutation_vec = Vec::new();
    for (key, value) in object.map() {
        let field_vec = TemplateFields::default();
        permutation_vec.push(permutation_iter(field_vec, key.to_string(), value));
    }
    permutation_vec
}

fn permutation_iter(mut permutation: TemplateFields, key: String, object: &Value) -> TemplateFields {
    match object {
        Value::Array(array) => {
            let mut index_permutation = permutation;
            for value in array {
                index_permutation.extend(&permutation_iter(TemplateFields::default(), key.clone(), value));
            }
            index_permutation
        }
        _ => {
            permutation.insert(key.clone(), object.clone());
            permutation
        }
    }
}

pub fn iterate_object(object: &TemplateFields) -> ResultEP<Vec<TemplateFields>> {
    let permutations = find_permutations(object);

    // Early return for edge cases
    if permutations.is_empty() {
        return Ok(vec![TemplateFields::default()]);
    }

    if permutations.iter().any(|p| p.map().is_empty()) {
        return Ok(vec![]);
    }

    let total_permutations = total_permutations(&permutations);
    let mut result_objects = Vec::with_capacity(total_permutations);

    for i in 0..total_permutations {
        let mut map = TemplateFields::default();
        let mut temp_index = i;

        // Process permutations in reverse order to get correct index calculation
        for permutation in &permutations {
            let idx = temp_index % permutation.map().len();
            temp_index /= permutation.map().len();

            if let Some((key, value)) = permutation.map().get(idx) {
                map.insert(key.clone(), value.clone());
            }
        }

        result_objects.push(map);
    }

    Ok(result_objects)
}

impl ToSql for Binding {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            _ => Err(format!("cannot convert Bindings to SQL type {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }

    postgres_types::to_sql_checked!();
}

impl<'a> FromSql<'a> for Binding {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => {
                let json_str = std::str::from_utf8(raw)?;
                Ok(serde_json::from_str(json_str)?)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_str = std::str::from_utf8(raw)?;
                Ok(serde_json::from_str(json_str)?)
            }
            _ => Err(format!("cannot convert SQL type {} to Binding", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BindingBuilder {
    template_id: String,
    /// Template field name, api field name
    fields: Vec<(TemplateFieldName, ApiFieldName)>,
}

impl BindingBuilder {
    pub fn new(template_id: impl Into<String>, fields: Vec<(impl Into<TemplateFieldName>, impl Into<ApiFieldName>)>) -> Self {
        Self {
            template_id: template_id.into(),
            fields: fields.into_iter().map(|(t, a)| (t.into(), a.into())).collect(),
        }
    }
    pub fn template(&self) -> &str {
        &self.template_id
    }
    pub fn fields(&self) -> &Vec<(TemplateFieldName, ApiFieldName)> {
        &self.fields
    }
}

#[cfg(test)]
mod test {
    use crate::database::api::bindings::{Binding, iterate_object};
    use crate::database::api::fields::ApiFieldName;
    use crate::database::template::wrapper::TemplateFieldName;
    use crate::database::template::{ApiFields, TemplateFields};
    use format::TemplateUuid;
    use serde_json::json;

    #[test]
    fn test_complete_order_workflow() {
        // Input data matching your existing test case
        let sample_input: ApiFields = json!({
            "order": {
                "order_id": "ORD-2024-001",
                "customer_name": "Brewery Supply Co",
                "customer_type": "wholesale",
                "order_date": "2024-01-15T10:30:00Z",
                "total_amount": 2500.00,
                "order_status": "pending",
                "delivery_address": "123 Brewery Lane",
                "special_instructions": "Handle with care"
            },
            "items": [
                {
                    "batch_id": "BATCH-IPA-001",
                    "quantity": 50,
                    "unit_price": 25.00,
                    "line_total": 1250.00,
                    "packaging_type": "keg",
                    "ingredients" : [
                        {
                            "sample": 12,
                        },
                        {
                            "sample": 7,
                        },
                    ]
                },
                {
                    "batch_id": "BATCH-LAGER-002",
                    "quantity": 100,
                    "unit_price": 12.50,
                    "line_total": 1250.00,
                    "packaging_type": "case",
                    "ingredients" : [
                        {
                            "sample": 16,
                        },
                        {
                            "sample": 9,
                        },
                    ]
                }
            ]
        })
        .try_into()
        .unwrap_or_default();

        let test_order_binding = Binding::new(
            TemplateUuid::new_uuid(),
            vec![
                (TemplateFieldName::new("order_id"), ApiFieldName::new("order.order_id")),
                (TemplateFieldName::new("customer_name"), ApiFieldName::new("order.customer_name")),
                (TemplateFieldName::new("order_date"), ApiFieldName::new("order.order_date")),
            ],
        );

        let out = test_order_binding.map_template_fields(sample_input.clone());

        let expected_out = TemplateFields::try_from(json!( {
            "order_id": "ORD-2024-001",
            "customer_name": "Brewery Supply Co",
            "order_date": "2024-01-15T10:30:00Z",
        }))
        .unwrap_or_default();

        assert!(out.is_ok());
        assert_eq!(out.as_ref().unwrap().len(), expected_out.len());
        // order of elements in TemplateFields vector is not deterministic, so verify key by key
        for (out_key, out_val) in out.as_ref().unwrap().map() {
            assert_eq!(out_val, expected_out.get(out_key).unwrap());
        }

        let test_item_binding = Binding::new(
            TemplateUuid::new_uuid(),
            vec![
                (TemplateFieldName::new("order_id"), ApiFieldName::new("order.order_id")),
                (TemplateFieldName::new("batch_id"), ApiFieldName::new("items.batch_id")),
                (TemplateFieldName::new("quantity"), ApiFieldName::new("items.quantity")),
            ],
        );

        let out = test_item_binding.map_template_fields(sample_input.clone());

        // Second object: Item binding result
        let expected_out = TemplateFields::try_from(json!({
            "order_id": "ORD-2024-001",
            "batch_id": ["BATCH-IPA-001", "BATCH-LAGER-002"],
            "quantity": [50, 100],
        }))
        .unwrap_or_default();

        assert!(out.is_ok());
        assert_eq!(out.as_ref().unwrap().len(), expected_out.len());
        // order of elements in TemplateFields vector is not deterministic, so verify key by key
        for (out_key, out_val) in out.as_ref().unwrap().map() {
            assert_eq!(out_val, expected_out.get(out_key).unwrap());
        }

        let test_item2_binding = Binding::new(
            TemplateUuid::new_uuid(),
            vec![
                (TemplateFieldName::new("order_id"), ApiFieldName::new("order.order_id")),
                (TemplateFieldName::new("batch_id"), ApiFieldName::new("items.batch_id")),
                (TemplateFieldName::new("sample"), ApiFieldName::new("items.ingredients.sample")),
            ],
        );

        let out = test_item2_binding.map_template_fields(sample_input.clone());

        let expected_out = TemplateFields::try_from(json!({
            "order_id": "ORD-2024-001",
            "batch_id": ["BATCH-IPA-001", "BATCH-LAGER-002"],
            "sample": [[12, 7], [16, 9]],
        }))
        .unwrap_or_default();

        assert!(out.is_ok());
        assert_eq!(out.as_ref().unwrap().len(), expected_out.len());
        // order of elements in TemplateFields vector is not deterministic, so verify key by key
        for (out_key, out_val) in out.as_ref().unwrap().map() {
            assert_eq!(out_val, expected_out.get(out_key).unwrap());
        }

        let out = iterate_object(&out.unwrap_or_default());

        let expected_out = [
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-IPA-001",
                "sample": 12,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-LAGER-002",
                "sample": 12,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-IPA-001",
                "sample": 7,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-LAGER-002",
                "sample": 7,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-IPA-001",
                "sample": 16,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-LAGER-002",
                "sample": 16,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-IPA-001",
                "sample": 9,
            }))
            .unwrap_or_default(),
            TemplateFields::try_from(json!({
                "order_id": "ORD-2024-001",
                "batch_id": "BATCH-LAGER-002",
                "sample": 9,
            }))
            .unwrap_or_default(),
        ];

        assert!(out.is_ok());
        assert_eq!(out.as_ref().unwrap().len(), expected_out.len());
        // order of elements in TemplateFields vector is not deterministic, so verify key by key
        for (i, template_fields) in out.as_ref().unwrap().iter().enumerate() {
            for (out_key, out_val) in template_fields.iter() {
                assert_eq!(out_val, expected_out[i].get(out_key).unwrap());
            }
        }
    }
}
