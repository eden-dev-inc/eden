use serde_json::{Map, Value};

pub enum FilterAction {
    Include,
    Exclude,
}

type FilterFn = dyn Fn(&str, &Value, bool) -> FilterAction;

/// filter the JSON to include or exclude data
pub fn filter_json(value: &Value, filter: &FilterFn, is_nested: bool) -> Value {
    match value {
        Value::Object(map) => {
            let mut new_map = Map::new();
            for (k, v) in map {
                match filter(k, v, is_nested) {
                    FilterAction::Include => {
                        new_map.insert(k.clone(), filter_json(v, filter, true));
                    }
                    FilterAction::Exclude => {}
                }
            }
            Value::Object(new_map)
        }
        Value::Array(arr) => {
            let new_arr: Vec<Value> = arr
                .iter()
                .filter_map(|v| match filter("", v, is_nested) {
                    FilterAction::Include => Some(filter_json(v, filter, true)),
                    FilterAction::Exclude => None,
                })
                .collect();
            Value::Array(new_arr)
        }
        _ => value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::rc::Rc;

    #[test]
    fn filter_test() {
        let input = json!({
            "name": "John Doe",
            "age": 30,
            "email": "john@example.com",
            "address": {
                "street": "123 Main St",
                "city": "Anytown",
                "country": "USA"
            },
            "phones": [
                {"type": "home", "number": "555-1234"},
                {"type": "work", "number": "555-5678"}
            ]
        });

        // Example 1: Exclude specific keys
        let exclude_keys = Rc::new(vec!["email", "phones"]);
        let filtered = filter_json(
            &input,
            &move |key, _, is_nested| {
                if !is_nested && exclude_keys.contains(&key) {
                    FilterAction::Exclude
                } else {
                    FilterAction::Include
                }
            },
            false,
        );

        println!("Filtered (excluding email and phones):");
        println!("{}", serde_json::to_string_pretty(&filtered).unwrap_or_default());

        // Example 2: Only include specific keys
        let include_keys = Rc::new(vec!["name", "age", "address"]);
        let filtered = filter_json(
            &input,
            &move |key, _, is_nested| {
                if is_nested || include_keys.contains(&key) {
                    FilterAction::Include
                } else {
                    FilterAction::Exclude
                }
            },
            false,
        );

        println!("\nFiltered (including only name, age, and address):");
        println!("{}", serde_json::to_string_pretty(&filtered).unwrap_or_default());

        // Example 3: Filter based on value (exclude if age < 40)
        let filtered = filter_json(
            &input,
            &|key, value, _| {
                if key == "age"
                    && let Some(age) = value.as_u64()
                    && age < 40
                {
                    return FilterAction::Exclude;
                }
                FilterAction::Include
            },
            false,
        );

        println!("\nFiltered (excluding age < 40):");
        println!("{}", serde_json::to_string_pretty(&filtered).unwrap_or_default());
    }
}
