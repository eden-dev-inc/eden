use borsh::{BorshDeserialize, BorshSerialize};
use error::{EpError, WorkflowError};
use json::flatten::flatten_json;
use json::map::map_json;
use json::parse::parse_json;
use json::reduce::reduce_json;
use json::unflatten::unflatten_json;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, PartialEq)]
pub enum JsonOps {
    Flatten(String),                              // Prefix for flattened keys
    Map(String, String, HashMap<String, String>), // Schema, value field, mappings
    Parse(String, Vec<String>),                   // Field to parse, paths to extract
    Reduce(String, Vec<String>),                  // Field to reduce, paths to keep
    Unflatten(String),                            // Field to unflatten
}

impl JsonOps {
    pub fn process(&self, input: &Value) -> Result<Value, EpError> {
        match self {
            JsonOps::Flatten(prefix) => {
                let mut result = Map::new();
                flatten_json(input, prefix, &mut result);
                Ok(Value::Object(result))
            }

            JsonOps::Map(schema_str, value_field, mappings) => {
                let schema: Value = serde_json::from_str(schema_str).map_err(EpError::parse)?;

                let value = if value_field.is_empty() {
                    input.clone()
                } else {
                    input.get(value_field).ok_or_else(|| EpError::parse(format!("Field '{}' not found in input", value_field)))?.clone()
                };

                let mapped = map_json(&value, &schema, mappings).map_err(EpError::parse)?;
                serde_json::from_str(&mapped).map_err(EpError::parse)
            }

            JsonOps::Parse(field, paths) => {
                let value = if field.is_empty() {
                    input.clone()
                } else {
                    input.get(field).ok_or_else(|| format!("Field '{}' not found in input", field))?.clone()
                };

                let parsed = parse_json(&value, paths.clone()).map_err(EpError::parse)?;

                serde_json::from_str(&parsed).map_err(EpError::parse)
            }

            JsonOps::Reduce(field, paths) => {
                let value = if field.is_empty() {
                    input.clone()
                } else {
                    input.get(field).ok_or_else(|| format!("Field '{}' not found in input", field))?.clone()
                };

                let reduced = reduce_json(&value, paths.clone()).map_err(|e| format!("Reduce error: {}", e))?;

                serde_json::from_str(&reduced).map_err(EpError::parse)
            }

            JsonOps::Unflatten(field) => {
                let flat_map: HashMap<String, Value> = if field.is_empty() {
                    serde_json::from_value(input.clone()).map_err(|e| format!("Invalid flat map: {}", e))?
                } else {
                    let field_value = input.get(field).ok_or_else(|| format!("Field '{}' not found in input", field))?;

                    serde_json::from_value(field_value.clone()).map_err(|e| format!("Invalid flat map: {}", e))?
                };

                Ok(unflatten_json(&flat_map))
            }
        }
    }

    pub fn process_many(&self, inputs: &[Value]) -> Result<Value, EpError> {
        if inputs.is_empty() {
            return Err(EpError::Workflow(WorkflowError::NoInputsProvided));
        }

        // For now, process just the first input
        // This could be enhanced to handle multiple inputs differently based on operation
        self.process(&inputs[0])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn flatten_operation() {
        let input = json!({
            "person": {
                "name": {
                    "first": "John",
                    "last": "Doe"
                },
                "age": 30,
                "hobbies": ["reading", "cycling"]
            }
        });

        let op = JsonOps::Flatten("".to_string());
        let result = op.process(&input).unwrap();

        println!("Flattened: {}", serde_json::to_string_pretty(&result).unwrap());
        assert!(result.as_object().unwrap().contains_key("person.name.first"));
        assert_eq!(result.get("person.name.first").unwrap(), "John");
    }

    #[test]
    fn map_operation() {
        let input = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25}
            ]
        });

        let mut mappings = HashMap::new();
        mappings.insert("output/names".to_string(), "users/../name".to_string());
        mappings.insert("output/ages".to_string(), "users/../age".to_string());

        let schema = json!({
            "output": {
                "names": "string",
                "ages": "number"
            }
        });

        let op = JsonOps::Map(serde_json::to_string(&schema).unwrap(), "".to_string(), mappings);

        let result = op.process(&input).unwrap();

        println!("Mapped: {}", serde_json::to_string_pretty(&result).unwrap());
        assert!(result.get("output").unwrap().get("names").is_some());
        assert!(result.get("output").unwrap().get("ages").is_some());
    }

    #[test]
    fn reduce_operation() {
        let input = json!({
            "users": [
                {"name": "Alice", "age": 30, "city": "New York"},
                {"name": "Bob", "age": 25, "city": "Boston"}
            ]
        });

        let paths = vec!["users/../name".to_string(), "users/../age".to_string()];
        let op = JsonOps::Reduce("".to_string(), paths);

        let result = op.process(&input).unwrap();

        // println!(
        //     "Reduced: {}",
        //     serde_json::to_string_pretty(&result).unwrap()
        // );

        assert!(result.get("users").unwrap().get("0").unwrap().get("city").is_none());
        assert!(result.get("users").unwrap().get("0").unwrap().get("name").is_some());
        assert!(result.get("users").unwrap().get("0").unwrap().get("age").is_some());
    }

    #[test]
    fn parse_operation() {
        let input = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35}
            ]
        });

        let paths = vec!["users/1..".to_string()];
        let op = JsonOps::Parse("".to_string(), paths);

        let result = op.process(&input).unwrap();

        println!("Parsed: {}", serde_json::to_string_pretty(&result).unwrap());
        assert!(result.as_object().unwrap().contains_key("users/1"));
        assert_eq!(result.get("users/1").unwrap().get("name").unwrap(), "Bob");
    }

    #[test]
    fn unflatten_operation() {
        let input = json!({
            "person.name.first": "John",
            "person.name.last": "Doe",
            "person.age": 30,
            "person.hobbies[0]": "reading",
            "person.hobbies[1]": "cycling"
        });

        let op = JsonOps::Unflatten("".to_string());
        let result = op.process(&input).unwrap();

        println!("Unflattened: {}", serde_json::to_string_pretty(&result).unwrap());

        let input = json!({
            "person": {
                "name": {
                    "first": "John",
                    "last": "Doe"
                },
                "age": 30,
                "hobbies": ["reading", "cycling"]
            }
        });

        assert_eq!(result, input)
    }

    #[test]
    fn error_handling() {
        let input = json!({
            "data": { "value": 42 }
        });

        // Test missing field
        let op = JsonOps::Reduce("missing_field".to_string(), vec![]);
        let result = op.process(&input);
        assert!(result.is_err());
        // assert!(result.unwrap_err().contains("not found in input"));

        // Test invalid schema
        let op = JsonOps::Map("{invalid_json}".to_string(), "".to_string(), HashMap::new());
        let result = op.process(&input);
        assert!(result.is_err());
        // assert!(result.unwrap_err().contains("Schema parse error"));
    }

    #[test]
    fn process_many() {
        let input1 = json!({"value": 1});
        let input2 = json!({"value": 2});

        let op = JsonOps::Flatten("".to_string());
        let result = op.process_many(&[input1, input2]).unwrap();

        assert_eq!(result.get("value").unwrap(), 1);

        // Test empty input
        let result = op.process_many(&[]);
        assert!(result.is_err());
        // assert_eq!(result.unwrap_err(), "No inputs provided");
    }
}
