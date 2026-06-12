use std::collections::HashMap;

use super::types::AwsFilter;

/// RFC 3986 unreserved-character percent-encoding (for form-encoded AWS query bodies).
pub fn percent_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len());
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            b => encoded.push_str(&format!("%{b:02X}")),
        }
    }
    encoded
}

/// Build a form-encoded body for AWS query-protocol services.
///
/// Produces: `Action={action}&Version={version}&{key=value&...}`
pub fn build_query_body(action: &str, version: &str, params: &HashMap<String, String>) -> String {
    let mut parts = vec![
        format!("Action={}", percent_encode(action)),
        format!("Version={}", percent_encode(version)),
    ];
    for (k, v) in params {
        parts.push(format!("{}={}", percent_encode(k), percent_encode(v)));
    }
    parts.join("&")
}

/// Convert a slice of `AwsFilter` values into query-protocol key-value pairs.
///
/// Produces: `Filter.1.Name=name&Filter.1.Value.1=v1&Filter.1.Value.2=v2&Filter.2.Name=...`
pub fn filters_to_params(filters: &[AwsFilter]) -> HashMap<String, String> {
    let mut params = HashMap::new();
    for (fi, f) in filters.iter().enumerate() {
        params.insert(format!("Filter.{}.Name", fi + 1), f.name.clone());
        for (vi, v) in f.values.iter().enumerate() {
            params.insert(format!("Filter.{}.Value.{}", fi + 1, vi + 1), v.clone());
        }
    }
    params
}

/// Convert a list of scalar values to indexed query-protocol params.
///
/// E.g. `indexed_list_params("InstanceId", &["i-1", "i-2"])` →
/// `{"InstanceId.1": "i-1", "InstanceId.2": "i-2"}`
pub fn indexed_list_params(key: &str, values: &[String]) -> HashMap<String, String> {
    values.iter().enumerate().map(|(i, v)| (format!("{}.{}", key, i + 1), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_encode_alphanumeric_passthrough() {
        assert_eq!(percent_encode("DescribeInstances"), "DescribeInstances");
        assert_eq!(percent_encode("2016-11-15"), "2016-11-15");
    }

    #[test]
    fn percent_encode_special_chars() {
        assert_eq!(percent_encode("a b"), "a%20b");
        assert_eq!(percent_encode("a&b"), "a%26b");
        assert_eq!(percent_encode("a=b"), "a%3Db");
    }

    #[test]
    fn build_query_body_basic() {
        let body = build_query_body("DescribeInstances", "2016-11-15", &HashMap::new());
        assert_eq!(body, "Action=DescribeInstances&Version=2016-11-15");
    }

    #[test]
    fn filters_to_params_basic() {
        let filters = vec![AwsFilter::new(
            "instance-state-name",
            vec!["running".to_string(), "stopped".to_string()],
        )];
        let params = filters_to_params(&filters);
        assert_eq!(params["Filter.1.Name"], "instance-state-name");
        assert_eq!(params["Filter.1.Value.1"], "running");
        assert_eq!(params["Filter.1.Value.2"], "stopped");
    }

    #[test]
    fn indexed_list_params_basic() {
        let ids = vec!["i-111".to_string(), "i-222".to_string()];
        let params = indexed_list_params("InstanceId", &ids);
        assert_eq!(params["InstanceId.1"], "i-111");
        assert_eq!(params["InstanceId.2"], "i-222");
    }
}
