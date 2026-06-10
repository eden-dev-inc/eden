use crate::extract::extract_value;
use regex::Regex;
use serde_json::{Map, Value};

pub fn parse_json(json: &Value, paths: Vec<String>) -> Result<String, serde_json::Error> {
    let mut result = Map::new();

    let range_regex = match Regex::new(r"(\d*)\.\.(\d*)") {
        Ok(regex) => regex,
        Err(_) => return serde_json::to_string(&Value::Object(result)),
    };

    for path in &paths {
        let values = extract_value(json, path);
        if values.is_empty() {
            continue;
        } else if values.len() == 1 {
            result.insert(path.to_string(), values[0].clone());
        } else {
            let start = range_regex.captures(path).and_then(|caps| caps.get(1)).and_then(|m| m.as_str().parse::<usize>().ok()).unwrap_or(0);

            let new_path = range_regex.replace(path, "{}");

            for (i, value) in values.iter().enumerate() {
                let index = new_path.find("{}").unwrap_or_default();
                let final_path = format!("{}{}{}", &new_path[..index], start + i, &new_path[index + 2..]);
                result.insert(final_path, value.to_owned().clone());
            }
        }
    }

    serde_json::to_string(&Value::Object(result))
}

pub fn parse_range(s: &str) -> Option<(Option<usize>, Option<usize>)> {
    if s == ".." {
        return Some((None, None));
    }
    let parts: Vec<&str> = s.split("..").collect();
    match parts.len() {
        1 => s.parse().ok().map(|n| (Some(n), Some(n + 1))),
        2 => {
            let start = if parts[0].is_empty() { None } else { parts[0].parse().ok() };
            let end = if parts[1].is_empty() { None } else { parts[1].parse().ok() };
            Some((start, end))
        }
        _ => None,
    }
}

#[cfg(test)]
pub mod test {
    use serde_json::{Value, json};

    use crate::parse::parse_json;

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

        let paths = vec!["user/addresses/1/type".to_string()];

        let reduced = parse_json(&data, paths).unwrap_or_default();
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

        let paths = vec!["users/3..".to_string()];

        let reduced = parse_json(&data, paths).unwrap_or_default();
        println!("{:#?}", serde_json::from_str::<Value>(&reduced).unwrap_or_default());
    }
}
