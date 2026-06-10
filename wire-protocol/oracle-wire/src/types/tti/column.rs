//! Column metadata for result sets.
//!
//! This module defines structures for describing columns in query results,
//! including data type, size, precision, and naming information.

use super::data_types::{OracleDataType, TypeDescriptor};

/// Metadata for a single column in a result set.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnInfo {
    /// Column name.
    pub name: String,
    /// Column alias (if different from name).
    pub alias: Option<String>,
    /// Schema name (owner).
    pub schema: Option<String>,
    /// Table name.
    pub table: Option<String>,
    /// Type descriptor.
    pub type_desc: TypeDescriptor,
    /// Whether the column is nullable.
    pub nullable: bool,
    /// Column position (1-based).
    pub position: u16,
}

impl ColumnInfo {
    /// Create a new column info with required fields.
    pub fn new(name: impl Into<String>, data_type: OracleDataType) -> Self {
        Self {
            name: name.into(),
            alias: None,
            schema: None,
            table: None,
            type_desc: TypeDescriptor::new(data_type),
            nullable: true,
            position: 0,
        }
    }

    /// Set the alias.
    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    /// Set the schema and table.
    pub fn with_source(mut self, schema: impl Into<String>, table: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self.table = Some(table.into());
        self
    }

    /// Set the type descriptor.
    pub fn with_type_desc(mut self, desc: TypeDescriptor) -> Self {
        self.type_desc = desc;
        self
    }

    /// Set nullability.
    pub fn with_nullable(mut self, nullable: bool) -> Self {
        self.nullable = nullable;
        self
    }

    /// Set the position.
    pub fn with_position(mut self, position: u16) -> Self {
        self.position = position;
        self
    }

    /// Get the display name (alias if set, otherwise name).
    pub fn display_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }

    /// Get the data type.
    pub fn data_type(&self) -> OracleDataType {
        self.type_desc.data_type
    }

    /// Get the max size in bytes.
    pub fn max_size(&self) -> u32 {
        self.type_desc.max_size
    }

    /// Get precision (for numeric types).
    pub fn precision(&self) -> u8 {
        self.type_desc.precision
    }

    /// Get scale (for numeric types).
    pub fn scale(&self) -> i8 {
        self.type_desc.scale
    }
}

/// Metadata for all columns in a result set.
#[derive(Clone, Debug, Default)]
pub struct ResultSetMetadata {
    /// Column information, in order.
    columns: Vec<ColumnInfo>,
}

impl ResultSetMetadata {
    /// Create empty metadata.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create with preallocated capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { columns: Vec::with_capacity(capacity) }
    }

    /// Add a column.
    pub fn add_column(&mut self, mut info: ColumnInfo) {
        info.position = (self.columns.len() + 1) as u16;
        self.columns.push(info);
    }

    /// Get the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get column info by index (0-based).
    pub fn column(&self, index: usize) -> Option<&ColumnInfo> {
        self.columns.get(index)
    }

    /// Get column info by name (case-insensitive).
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnInfo> {
        let name_upper = name.to_uppercase();
        self.columns
            .iter()
            .find(|c| c.name.to_uppercase() == name_upper || c.alias.as_ref().map(|a| a.to_uppercase()) == Some(name_upper.clone()))
    }

    /// Get the index of a column by name (case-insensitive).
    pub fn column_index(&self, name: &str) -> Option<usize> {
        let name_upper = name.to_uppercase();
        self.columns
            .iter()
            .position(|c| c.name.to_uppercase() == name_upper || c.alias.as_ref().map(|a| a.to_uppercase()) == Some(name_upper.clone()))
    }

    /// Iterate over columns.
    pub fn iter(&self) -> impl Iterator<Item = &ColumnInfo> {
        self.columns.iter()
    }

    /// Get all column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.display_name()).collect()
    }
}

impl<'a> IntoIterator for &'a ResultSetMetadata {
    type Item = &'a ColumnInfo;
    type IntoIter = std::slice::Iter<'a, ColumnInfo>;

    fn into_iter(self) -> Self::IntoIter {
        self.columns.iter()
    }
}

/// Builder for result set metadata.
pub struct MetadataBuilder {
    metadata: ResultSetMetadata,
}

impl MetadataBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self { metadata: ResultSetMetadata::new() }
    }

    /// Add a VARCHAR2 column.
    pub fn varchar2(mut self, name: &str, max_len: u32) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Varchar2).with_type_desc(TypeDescriptor::varchar2(max_len));
        self.metadata.add_column(info);
        self
    }

    /// Add a NUMBER column.
    pub fn number(mut self, name: &str, precision: u8, scale: i8) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Number).with_type_desc(TypeDescriptor::number(precision, scale));
        self.metadata.add_column(info);
        self
    }

    /// Add a DATE column.
    pub fn date(mut self, name: &str) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Date).with_type_desc(TypeDescriptor::date());
        self.metadata.add_column(info);
        self
    }

    /// Add a TIMESTAMP column.
    pub fn timestamp(mut self, name: &str) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Timestamp).with_type_desc(TypeDescriptor::timestamp());
        self.metadata.add_column(info);
        self
    }

    /// Add a CLOB column.
    pub fn clob(mut self, name: &str) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Clob).with_type_desc(TypeDescriptor::clob());
        self.metadata.add_column(info);
        self
    }

    /// Add a BLOB column.
    pub fn blob(mut self, name: &str) -> Self {
        let info = ColumnInfo::new(name, OracleDataType::Blob).with_type_desc(TypeDescriptor::blob());
        self.metadata.add_column(info);
        self
    }

    /// Add a custom column.
    pub fn column(mut self, info: ColumnInfo) -> Self {
        self.metadata.add_column(info);
        self
    }

    /// Build the metadata.
    pub fn build(self) -> ResultSetMetadata {
        self.metadata
    }
}

impl Default for MetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_info() {
        let col = ColumnInfo::new("EMPLOYEE_ID", OracleDataType::Number)
            .with_alias("ID")
            .with_source("HR", "EMPLOYEES")
            .with_type_desc(TypeDescriptor::number(10, 0))
            .with_nullable(false)
            .with_position(1);

        assert_eq!(col.name, "EMPLOYEE_ID");
        assert_eq!(col.display_name(), "ID");
        assert_eq!(col.schema, Some("HR".to_string()));
        assert_eq!(col.table, Some("EMPLOYEES".to_string()));
        assert!(!col.nullable);
        assert_eq!(col.precision(), 10);
    }

    #[test]
    fn test_metadata_builder() {
        let metadata = MetadataBuilder::new().number("ID", 10, 0).varchar2("NAME", 100).date("HIRE_DATE").build();

        assert_eq!(metadata.column_count(), 3);
        assert_eq!(metadata.column(0).unwrap().name, "ID");
        assert_eq!(metadata.column(1).unwrap().name, "NAME");
        assert_eq!(metadata.column(2).unwrap().name, "HIRE_DATE");
    }

    #[test]
    fn test_column_lookup() {
        let metadata = MetadataBuilder::new().varchar2("FIRST_NAME", 50).varchar2("LAST_NAME", 50).build();

        // Case-insensitive lookup
        assert!(metadata.column_by_name("first_name").is_some());
        assert!(metadata.column_by_name("FIRST_NAME").is_some());
        assert!(metadata.column_by_name("First_Name").is_some());

        // Index lookup
        assert_eq!(metadata.column_index("last_name"), Some(1));
    }
}
