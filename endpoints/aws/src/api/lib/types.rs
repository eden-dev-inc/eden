/// A filter for AWS query-protocol API operations (EC2, RDS, etc.).
///
/// Corresponds to the `Filter.N.Name` / `Filter.N.Value.M` query-string parameters used by
/// EC2, Auto Scaling, RDS and other query-protocol services.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema, schemars::JsonSchema)]
pub struct AwsFilter {
    /// Filter name (e.g. "instance-state-name", "tag:Name")
    pub name: String,
    /// Filter values (e.g. ["running", "stopped"])
    pub values: Vec<String>,
}

impl AwsFilter {
    pub fn new(name: impl Into<String>, values: Vec<String>) -> Self {
        Self { name: name.into(), values }
    }
}

/// A key-value tag for AWS resources.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize, utoipa::ToSchema, schemars::JsonSchema)]
pub struct AwsTag {
    pub key: String,
    pub value: String,
}

impl AwsTag {
    pub fn new(key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { key: key.into(), value: value.into() }
    }
}
