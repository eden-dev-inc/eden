use serde_json::{Map, Value};
use std::collections::HashMap;

pub fn unflatten_json(flat: &HashMap<String, Value>) -> Value {
    let mut result = Map::new();

    // Process regular fields first
    let mut array_fields = Vec::new();

    for (key, value) in flat {
        if key.contains('[') {
            array_fields.push((key.clone(), value.clone()));
        } else {
            let parts: Vec<&str> = key.split('.').collect();
            insert_object_field(&mut result, &parts, value);
        }
    }

    // Process array fields after all regular fields
    for (key, value) in array_fields {
        insert_array_field(&mut result, &key, &value);
    }

    Value::Object(result)
}

// Insert a regular dotted-path field
fn insert_object_field(current: &mut Map<String, Value>, parts: &[&str], value: &Value) {
    if parts.is_empty() {
        return;
    }

    if parts.len() == 1 {
        current.insert(parts[0].to_string(), value.clone());
        return;
    }

    let entry = current.entry(parts[0]).or_insert_with(|| Value::Object(Map::new()));

    if let Value::Object(map) = entry {
        insert_object_field(map, &parts[1..], value);
    }
}

// Handle array paths like "person.hobbies[0]"
fn insert_array_field(result: &mut Map<String, Value>, key: &str, value: &Value) {
    // Find the last dot before the array notation
    if let Some(last_dot_pos) = key.rfind('.') {
        let (path, array_part) = key.split_at(last_dot_pos);
        let array_part = &array_part[1..]; // Skip the dot

        // Extract field name and index
        if let Some(bracket_pos) = array_part.find('[') {
            let field_name = &array_part[..bracket_pos];

            // Extract the index
            if let Some(end_bracket) = array_part.find(']') {
                let index_str = &array_part[bracket_pos + 1..end_bracket];
                if let Ok(index) = index_str.parse::<usize>() {
                    // Navigate to the parent object
                    let parts: Vec<&str> = path.split('.').collect();
                    let mut current = result;

                    for part in parts {
                        let entry = current.entry(part).or_insert_with(|| Value::Object(Map::new()));

                        if let Value::Object(map) = entry {
                            current = map;
                        } else {
                            // Cannot navigate further
                            return;
                        }
                    }

                    // Now current is at the parent object of the array
                    let array = current.entry(field_name).or_insert_with(|| Value::Array(Vec::new()));

                    // Ensure it's an array
                    if let Value::Array(vec) = array {
                        // Expand if needed
                        while vec.len() <= index {
                            vec.push(Value::Null);
                        }
                        vec[index] = value.clone();
                    } else {
                        // Convert to array
                        let mut vec = Vec::new();
                        while vec.len() <= index {
                            vec.push(Value::Null);
                        }
                        vec[index] = value.clone();
                        *array = Value::Array(vec);
                    }
                }
            }
        }
    } else {
        // No parent path, direct array access like "hobbies[0]"
        if let Some(bracket_pos) = key.find('[') {
            let field_name = &key[..bracket_pos];

            if let Some(end_bracket) = key.find(']') {
                let index_str = &key[bracket_pos + 1..end_bracket];
                if let Ok(index) = index_str.parse::<usize>() {
                    let array = result.entry(field_name).or_insert_with(|| Value::Array(Vec::new()));

                    if let Value::Array(vec) = array {
                        while vec.len() <= index {
                            vec.push(Value::Null);
                        }
                        vec[index] = value.clone();
                    } else {
                        let mut vec = Vec::new();
                        while vec.len() <= index {
                            vec.push(Value::Null);
                        }
                        vec[index] = value.clone();
                        *array = Value::Array(vec);
                    }
                }
            }
        }
    }
}
