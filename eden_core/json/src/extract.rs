use crate::parse::parse_range;
use serde_json::Value;

pub fn extract_value<'a>(json: &'a Value, path: &str) -> Vec<&'a Value> {
    let mut current = vec![json];
    for key in path.split('/') {
        let mut next = Vec::new();
        for value in current {
            match value {
                Value::Object(map) => {
                    if let Some(v) = map.get(key) {
                        next.push(v);
                    }
                }
                Value::Array(vec) => {
                    if key == ".." {
                        next.extend(vec.iter());
                    } else if let Some((start, end)) = parse_range(key) {
                        let start = start.unwrap_or(0);
                        let end = end.unwrap_or(vec.len());
                        next.extend(vec.iter().skip(start).take(end.saturating_sub(start)));
                    } else if let Ok(index) = key.parse::<usize>()
                        && let Some(v) = vec.get(index)
                    {
                        next.push(v);
                    }
                }
                _ => {}
            }
        }
        current = next;
    }
    current
}
