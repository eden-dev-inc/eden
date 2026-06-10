use serde_json::{Map, Value};

pub fn flatten_json(value: &Value, prefix: &str, result: &mut Map<String, Value>) {
    match value {
        Value::Object(map) => {
            for (k, v) in map {
                let new_key = if prefix.is_empty() {
                    k.to_string()
                } else {
                    format!("{prefix}.{k}")
                };
                flatten_json(v, &new_key, result);
            }
        }
        Value::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                let new_key = format!("{prefix}[{i}]");
                flatten_json(v, &new_key, result);
            }
        }
        _ => {
            result.insert(prefix.to_string(), value.clone());
        }
    }
}

#[cfg(test)]
pub mod test {
    use serde_json::{Map, Value, json};

    use crate::flatten::flatten_json;

    #[test]
    fn flatten_test() {
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

        let mut flattened = Map::new();
        flatten_json(&input, "", &mut flattened);

        println!("{}", serde_json::to_string_pretty(&Value::Object(flattened)).unwrap_or_default());
    }
}
