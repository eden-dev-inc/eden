use std::{
    collections::HashMap,
    io::{Error, ErrorKind},
};

use crate::extract::extract_value;
use serde_json::{Map, Value};

pub fn map_json(input: &Value, schema: &Value, path_mappings: &HashMap<String, String>) -> std::io::Result<String> {
    let mut result = Map::new();

    fn traverse_schema(
        schema: &Value,
        input: &Value,
        path_mappings: &HashMap<String, String>,
        current_path: &str,
        result: &mut Map<String, Value>,
    ) -> std::io::Result<()> {
        match schema {
            Value::Object(obj) => {
                for (key, value) in obj {
                    let new_path = if current_path.is_empty() {
                        key.clone()
                    } else {
                        format!("{current_path}/{key}")
                    };
                    traverse_schema(value, input, path_mappings, &new_path, result)?;
                }
            }
            Value::String(_) => {
                if let Some(input_path) = path_mappings.get(current_path) {
                    let values = extract_value(input, input_path);
                    if !values.is_empty() {
                        let mut current = result;
                        let parts: Vec<&str> = current_path.split('/').collect();
                        for (i, &part) in parts.iter().enumerate() {
                            if i == parts.len() - 1 {
                                // Last part of the path, insert the value
                                if values.len() == 1 {
                                    current.insert(part.to_string(), values[0].clone());
                                } else {
                                    let array_value = Value::Array(values.iter().map(|&v| v.clone()).collect());
                                    current.insert(part.to_string(), array_value);
                                }
                            } else {
                                // Not the last part, ensure the nested structure exists
                                current = current
                                    .entry(part)
                                    .or_insert_with(|| Value::Object(Map::new()))
                                    .as_object_mut()
                                    .ok_or_else(|| Error::other(format!("Failed to create nested structure at {part}")))?;
                            }
                        }
                    }
                }
            }
            _ => {
                return Err(Error::new(ErrorKind::InvalidData, format!("Unsupported schema type at path: {current_path}")));
            }
        }
        Ok(())
    }

    traverse_schema(schema, input, path_mappings, "", &mut result)?;
    Ok(serde_json::to_string(&Value::Object(result))?)
}

#[cfg(test)]
pub mod test {
    use std::collections::HashMap;

    use serde_json::{Value, json};

    use crate::map::map_json;

    #[test]
    fn mapping_test() {
        let input = json!({
            "users": [
                {"name": "Alice", "age": 30},
                {"name": "Bob", "age": 25},
                {"name": "Charlie", "age": 35},
                {"name": "David", "age": 28},
                {"name": "Eve", "age": 22}
            ],
            "metadata": {
                "created_at": "2024-08-27T12:00:00Z"
            }
        });

        let schema = json!({
            "people": {
                "names": "string",
                "ages": "number"
            },
            "info": {
                "timestamp": "string"
            }
        });

        let mut path_mappings = HashMap::new();
        path_mappings.insert("people/names".to_string(), "users/../name".to_string());
        path_mappings.insert("people/ages".to_string(), "users/../age".to_string());
        path_mappings.insert("info/timestamp".to_string(), "metadata/created_at".to_string());

        let result = map_json(&input, &schema, &path_mappings).unwrap_or_default();
        println!("{:#?}", serde_json::from_str::<Value>(&result).unwrap_or_default());
    }
}

// #[cfg(test)]
// pub mod tests {
//     use std::{collections::HashMap, rc::Rc};

//     use serde_json::{json, Map, Value};

//     use crate::communicator::json::{
//         apply_math_operations, filter_json, flatten_json, map_json, parse_json, reduce_json,
//         rename_keys, unflatten_json, FilterAction, MathOperation,
//     };

//     #[test]
//     fn non_recursive_test() {
//         let data = json!({
//             "user": {
//                 "name": "John Doe",
//                 "age": 30,
//                 "addresses": [
//                     {
//                         "type": "home",
//                         "street": "123 Main St",
//                         "city": "Anytown"
//                     },
//                     {
//                         "type": "work",
//                         "street": "456 Office Blvd",
//                         "city": "Workville"
//                     }
//                 ],
//                 "preferences": {
//                     "theme": "dark",
//                     "notifications": true
//                 }
//             },
//             "metadata": {
//                 "created_at": "2024-08-27T12:00:00Z"
//             }
//         });

//         let paths = vec![
//             "user/name".to_string(),
//             "user/addresses/0/city".to_string(),
//             "user/preferences/theme".to_string(),
//             "metadata/created_at".to_string(),
//         ];

//         let reduced = reduce_json(&data, paths).unwrap_or_default();
//         println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());

//         let paths = vec!["user/addresses/1/type".to_string()];

//         let reduced = parse_json(&data, paths).unwrap_or_default();
//         println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());
//     }

//     #[test]
//     fn recusive_test() {
//         let data = json!({
//             "users": [
//                 {"name": "Alice", "age": 30},
//                 {"name": "Bob", "age": 25},
//                 {"name": "Charlie", "age": 35},
//                 {"name": "David", "age": 28},
//                 {"name": "Eve", "age": 22}
//             ],
//             "metadata": {
//                 "created_at": "2024-08-27T12:00:00Z"
//             }
//         });

//         let paths = vec![
//             "users/../name".to_string(),
//             "users/2../age".to_string(),
//             "metadata/created_at".to_string(),
//         ];

//         let reduced = reduce_json(&data, paths).unwrap_or_default();
//         println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());

//         let paths = vec!["users/1..".to_string()];

//         let reduced = parse_json(&data, paths).unwrap_or_default();
//         println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());
//     }

//     #[test]
//     fn rename_test() {
//         let input = json!({
//             "name": "John Doe",
//             "age": 30,
//             "contact_info": {
//                 "email": "john@example.com",
//                 "phone": "123-456-7890"
//             },
//             "hobbies": [
//                 {
//                     "name": "reading",
//                     "years_practiced": 10
//                 },
//                 {
//                     "name": "cycling",
//                     "years_practiced": 5
//                 }
//             ]
//         });

//         let mut rename_map = HashMap::new();
//         rename_map.insert("name".to_string(), "full_name".to_string());
//         rename_map.insert("age".to_string(), "years_old".to_string());
//         rename_map.insert("contact_info".to_string(), "contact_details".to_string());
//         rename_map.insert("email".to_string(), "email_address".to_string());
//         rename_map.insert(
//             "years_practiced".to_string(),
//             "experience_years".to_string(),
//         );

//         let result = rename_keys(&input, &rename_map);

//         println!("Original JSON:");
//         println!("{}", serde_json::to_string_pretty(&input).unwrap_or_default());

//         println!("\nJSON with renamed keys:");
//         println!("{}", serde_json::to_string_pretty(&result).unwrap_or_default());
//     }
// }
