use serde_json::{Map, Value};

use crate::extract::extract_value;

use super::parse::parse_range;

pub fn reduce_json(json: &Value, paths: Vec<String>) -> Result<String, serde_json::Error> {
    let mut result = Map::new();

    for path in &paths {
        let values = extract_value(json, path);
        if values.is_empty() {
            continue;
        }

        let parts: Vec<&str> = path.split('/').collect();
        let range_start = parse_range(parts[parts.len() - 2]).and_then(|(start, _)| start).unwrap_or(0);

        'outer: for (i, value) in values.iter().enumerate() {
            let mut current = &mut result;
            for (j, &part) in parts.iter().enumerate() {
                if j == parts.len() - 1 {
                    // Last part of the path, insert the value
                    current.insert(part.to_string(), value.to_owned().clone());
                } else {
                    // Not the last part, ensure the nested structure exists
                    let next_part = if j == parts.len() - 2 && values.len() > 1 {
                        // If it's the second-to-last part and we have multiple values,
                        // use the correct index as the key
                        (range_start + i).to_string()
                    } else {
                        part.to_string()
                    };

                    // Get or create nested object
                    let entry = current.entry(&next_part).or_insert_with(|| Value::Object(Map::new()));

                    // We just inserted an Object above, so as_object_mut should succeed
                    // If it doesn't, skip this entire value (shouldn't happen in practice)
                    if let Some(map) = entry.as_object_mut() {
                        current = map;
                    } else {
                        continue 'outer;
                    }
                }
            }
        }
    }

    serde_json::to_string(&Value::Object(result))
}

#[cfg(test)]
pub mod test {
    use serde_json::{Value, json};

    use crate::reduce::reduce_json;

    #[test]
    fn non_recursive_test() {
        let data = json!({
            "user": {
                "name": "John Doe",
                "age": 30,
                "addresses": [
                    {
                        "type": "home",
                        "street": "123 Main St",
                        "city": "Anytown"
                    },
                    {
                        "type": "work",
                        "street": "456 Office Blvd",
                        "city": "Workville"
                    }
                ],
                "preferences": {
                    "theme": "dark",
                    "notifications": true
                }
            },
            "metadata": {
                "created_at": "2024-08-27T12:00:00Z"
            }
        });

        let paths = vec![
            "user/name".to_string(),
            "user/addresses/0/city".to_string(),
            "user/preferences/theme".to_string(),
            "metadata/created_at".to_string(),
        ];

        let reduced = reduce_json(&data, paths).unwrap_or_default();
        println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());
    }

    #[test]
    fn recusive_test() {
        let data = json!({
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

        let paths = vec![
            "users/../name".to_string(),
            "users/2../age".to_string(),
            "metadata/created_at".to_string(),
        ];

        let reduced = reduce_json(&data, paths).unwrap_or_default();
        println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());
    }
}
