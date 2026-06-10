use serde_json::{Map, Value};

pub const DEFAULT_TOOL_RESULT_MAX_ROWS: usize = 50;
pub const DEFAULT_TOOL_RESULT_MAX_CELLS: usize = 2_000;
pub const DEFAULT_TOOL_RESULT_MAX_BYTES: usize = 64 * 1024;

const TRUNCATED_SUFFIX: &str = "\n\n[truncated]";
const MIN_STRING_BYTES: usize = 16;
const MAX_STRING_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub enum ToolResultProjection {
    Table(CompactTable),
    Json { value: Value, truncated: bool },
    Text { text: String, truncated: bool },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CompactTable {
    pub columns: Vec<String>,
    pub rows: Vec<Vec<Value>>,
    pub row_count: usize,
    pub truncated: bool,
}

#[derive(Debug, Clone, Copy)]
struct ProjectionLimits {
    max_rows: usize,
    max_cells: usize,
    max_bytes: usize,
}

impl ProjectionLimits {
    fn new(max_rows: usize, max_cells: usize, max_bytes: usize) -> Self {
        Self {
            max_rows: if max_rows == 0 { DEFAULT_TOOL_RESULT_MAX_ROWS } else { max_rows },
            max_cells: if max_cells == 0 { DEFAULT_TOOL_RESULT_MAX_CELLS } else { max_cells },
            max_bytes: if max_bytes == 0 { DEFAULT_TOOL_RESULT_MAX_BYTES } else { max_bytes },
        }
    }
}

impl ToolResultProjection {
    pub fn to_compact_json(&self) -> String {
        let payload = match self {
            Self::Table(table) => Value::Object(
                [
                    ("type".to_string(), Value::String("table".to_string())),
                    ("columns".to_string(), Value::Array(table.columns.iter().cloned().map(Value::String).collect())),
                    ("rows".to_string(), Value::Array(table.rows.iter().cloned().map(Value::Array).collect())),
                    ("row_count".to_string(), Value::Number(serde_json::Number::from(table.row_count))),
                    ("truncated".to_string(), Value::Bool(table.truncated)),
                ]
                .into_iter()
                .collect(),
            ),
            Self::Json { value, truncated } => Value::Object(
                [
                    ("type".to_string(), Value::String("json".to_string())),
                    ("value".to_string(), value.clone()),
                    ("truncated".to_string(), Value::Bool(*truncated)),
                ]
                .into_iter()
                .collect(),
            ),
            Self::Text { text, truncated } => Value::Object(
                [
                    ("type".to_string(), Value::String("text".to_string())),
                    ("text".to_string(), Value::String(text.clone())),
                    ("truncated".to_string(), Value::Bool(*truncated)),
                ]
                .into_iter()
                .collect(),
            ),
        };

        match serde_json::to_string(&payload) {
            Ok(json) => json,
            Err(_) => "{\"type\":\"text\",\"text\":\"tool result projection serialization failed\",\"truncated\":true}".to_string(),
        }
    }
}

pub fn project_tool_result(raw: &str, max_rows: usize, max_cells: usize, max_bytes: usize) -> ToolResultProjection {
    let limits = ProjectionLimits::new(max_rows, max_cells, max_bytes);

    match serde_json::from_str::<Value>(raw) {
        Ok(value) => {
            if let Some(table) = project_compact_table(&value, limits) {
                return ToolResultProjection::Table(table);
            }

            let (value, truncated) = truncate_json_projection(value, limits);
            ToolResultProjection::Json { value, truncated }
        }
        Err(_) => {
            let (text, truncated) = truncate_text(raw, limits.max_bytes);
            ToolResultProjection::Text { text, truncated }
        }
    }
}

fn project_compact_table(value: &Value, limits: ProjectionLimits) -> Option<CompactTable> {
    let Value::Array(items) = value else {
        return None;
    };
    let Some(Value::Object(first_row)) = items.first() else {
        return None;
    };

    let columns: Vec<String> = first_row.keys().cloned().collect();
    if !first_row.values().all(is_scalar) {
        return None;
    }

    let is_consistent = items.iter().all(|item| match item {
        Value::Object(row) => row.len() == columns.len() && columns.iter().all(|column| row.get(column).is_some_and(is_scalar)),
        _ => false,
    });

    if !is_consistent {
        return None;
    }

    let row_count = items.len();
    let row_limit_by_cells = if columns.is_empty() {
        limits.max_rows
    } else {
        limits.max_cells / columns.len()
    };
    let row_limit = row_count.min(limits.max_rows).min(row_limit_by_cells);

    let rows = items
        .iter()
        .take(row_limit)
        .filter_map(|item| match item {
            Value::Object(row) => Some(columns.iter().map(|column| row.get(column).cloned().unwrap_or(Value::Null)).collect::<Vec<_>>()),
            _ => None,
        })
        .collect::<Vec<_>>();

    let mut table = CompactTable { columns, rows, row_count, truncated: row_limit < row_count };

    fit_table_to_budget(&mut table, limits.max_bytes);
    Some(table)
}

fn truncate_json_projection(value: Value, limits: ProjectionLimits) -> (Value, bool) {
    let cell_count = count_json_cells(&value);
    let original_serialized_len = serialized_len(&value);

    if cell_count <= limits.max_cells && original_serialized_len <= limits.max_bytes {
        return (value, false);
    }

    let mut truncated = false;
    let mut cell_budget = limits.max_cells;
    let mut projected = truncate_json_value(value, &mut cell_budget, limits.max_rows, &mut truncated);

    if serialized_len(&projected) > limits.max_bytes {
        truncated = true;
        truncate_json_strings_to_budget(&mut projected, limits.max_bytes);
    }

    while serialized_len(&projected) > limits.max_bytes {
        truncated = true;
        if !drop_last_json_item(&mut projected) {
            let serialized = safe_minified_json(&projected);
            let (preview, _) = truncate_text(&serialized, limits.max_bytes.saturating_sub(2));
            projected = Value::String(preview);
            break;
        }
    }

    (projected, truncated)
}

fn truncate_json_value(value: Value, cell_budget: &mut usize, max_rows: usize, truncated: &mut bool) -> Value {
    match value {
        Value::Array(items) => {
            let original_len = items.len();
            let mut out = Vec::new();

            for item in items.into_iter().take(max_rows) {
                if *cell_budget == 0 {
                    *truncated = true;
                    break;
                }
                out.push(truncate_json_value(item, cell_budget, max_rows, truncated));
            }

            if out.len() < original_len {
                *truncated = true;
            }

            Value::Array(out)
        }
        Value::Object(map) => {
            let original_len = map.len();
            let mut out = Map::new();

            for (key, value) in map {
                if *cell_budget == 0 {
                    *truncated = true;
                    break;
                }
                out.insert(key, truncate_json_value(value, cell_budget, max_rows, truncated));
            }

            if out.len() < original_len {
                *truncated = true;
            }

            Value::Object(out)
        }
        scalar => {
            if *cell_budget == 0 {
                *truncated = true;
                return Value::Null;
            }
            *cell_budget = cell_budget.saturating_sub(1);
            scalar
        }
    }
}

fn fit_table_to_budget(table: &mut CompactTable, max_bytes: usize) {
    if table.rows.is_empty() && table.columns.is_empty() {
        return;
    }

    if table_serialized_len(table) <= max_bytes {
        return;
    }

    table.truncated = true;
    truncate_table_strings_to_budget(table, max_bytes);

    while table_serialized_len(table) > max_bytes && !table.rows.is_empty() {
        table.rows.pop();
    }

    while table_serialized_len(table) > max_bytes && !table.columns.is_empty() {
        table.columns.pop();
        for row in &mut table.rows {
            if row.len() > table.columns.len() {
                row.pop();
            }
        }
    }
}

fn truncate_table_strings_to_budget(table: &mut CompactTable, max_bytes: usize) {
    let string_count = table.rows.iter().flat_map(|row| row.iter()).filter(|value| matches!(value, Value::String(_))).count();

    if string_count == 0 {
        return;
    }

    let quota = (max_bytes / string_count).clamp(MIN_STRING_BYTES, MAX_STRING_BYTES);
    for row in &mut table.rows {
        for value in row {
            if let Value::String(text) = value {
                *text = truncate_utf8(text, quota);
            }
        }
    }
}

fn truncate_json_strings_to_budget(value: &mut Value, max_bytes: usize) {
    let string_count = count_json_strings(value);
    if string_count == 0 {
        return;
    }

    let quota = (max_bytes / string_count).clamp(MIN_STRING_BYTES, MAX_STRING_BYTES);
    truncate_json_strings(value, quota);
}

fn truncate_json_strings(value: &mut Value, max_string_bytes: usize) {
    match value {
        Value::String(text) => {
            *text = truncate_utf8(text, max_string_bytes);
        }
        Value::Array(items) => {
            for item in items {
                truncate_json_strings(item, max_string_bytes);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                truncate_json_strings(value, max_string_bytes);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
}

fn drop_last_json_item(value: &mut Value) -> bool {
    match value {
        Value::Array(items) => {
            if items.pop().is_some() {
                return true;
            }
            false
        }
        Value::Object(map) => {
            if let Some(last_key) = map.keys().next_back().cloned() {
                map.remove(&last_key);
                return true;
            }
            false
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => false,
    }
}

fn truncate_text(raw: &str, max_bytes: usize) -> (String, bool) {
    if raw.len() <= max_bytes {
        return (raw.to_string(), false);
    }

    let suffix_bytes = TRUNCATED_SUFFIX.len();
    if max_bytes <= suffix_bytes {
        return (truncate_utf8(raw, max_bytes), true);
    }

    let body_budget = max_bytes.saturating_sub(suffix_bytes);
    let mut preview = truncate_utf8(raw, body_budget);
    preview.push_str(TRUNCATED_SUFFIX);
    (preview, true)
}

fn truncate_utf8(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end = end.saturating_sub(1);
    }
    value[..end].to_string()
}

fn count_json_cells(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => 1,
        Value::Array(items) => items.iter().map(count_json_cells).sum(),
        Value::Object(map) => map.values().map(count_json_cells).sum(),
    }
}

fn count_json_strings(value: &Value) -> usize {
    match value {
        Value::String(_) => 1,
        Value::Array(items) => items.iter().map(count_json_strings).sum(),
        Value::Object(map) => map.values().map(count_json_strings).sum(),
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

fn is_scalar(value: &Value) -> bool {
    matches!(value, Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_))
}

fn serialized_len(value: &Value) -> usize {
    safe_minified_json(value).len()
}

fn table_serialized_len(table: &CompactTable) -> usize {
    ToolResultProjection::Table(table.clone()).to_compact_json().len()
}

fn safe_minified_json(value: &Value) -> String {
    match serde_json::to_string(value) {
        Ok(json) => json,
        Err(_) => "null".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::{CompactTable, DEFAULT_TOOL_RESULT_MAX_BYTES, ToolResultProjection, project_tool_result};
    use serde_json::{Value, json};

    #[test]
    fn projects_consistent_scalar_rows_into_table() {
        let raw = r#"[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]"#;

        let projection = project_tool_result(raw, 50, 2_000, DEFAULT_TOOL_RESULT_MAX_BYTES);

        assert_eq!(
            projection,
            ToolResultProjection::Table(CompactTable {
                columns: vec!["id".to_string(), "name".to_string()],
                rows: vec![vec![json!(1), json!("alpha")], vec![json!(2), json!("beta")],],
                row_count: 2,
                truncated: false,
            })
        );
        assert_eq!(
            serde_json::from_str::<Value>(&projection.to_compact_json()).expect("parse compact json"),
            json!({
                "type": "table",
                "columns": ["id", "name"],
                "rows": [[1, "alpha"], [2, "beta"]],
                "row_count": 2,
                "truncated": false,
            })
        );
    }

    #[test]
    fn falls_back_to_json_when_rows_are_not_scalar() {
        let raw = r#"[{"id":1,"meta":{"active":true}},{"id":2,"meta":{"active":false}}]"#;

        let projection = project_tool_result(raw, 50, 2_000, DEFAULT_TOOL_RESULT_MAX_BYTES);

        match projection {
            ToolResultProjection::Json { value, truncated } => {
                assert_eq!(value, json!([{ "id": 1, "meta": { "active": true } }, { "id": 2, "meta": { "active": false } }]));
                assert!(!truncated);
            }
            other => panic!("expected json projection, got {other:?}"),
        }
    }

    #[test]
    fn falls_back_to_text_for_invalid_json() {
        let projection = project_tool_result("not json", 50, 2_000, DEFAULT_TOOL_RESULT_MAX_BYTES);

        assert_eq!(projection, ToolResultProjection::Text { text: "not json".to_string(), truncated: false });
    }

    #[test]
    fn truncates_table_rows_by_row_and_cell_limits() {
        let raw = serde_json::to_string(&(0..100).map(|id| json!({ "id": id, "name": format!("row-{id}") })).collect::<Vec<_>>())
            .expect("serialize test rows");

        let projection = project_tool_result(&raw, 50, 60, DEFAULT_TOOL_RESULT_MAX_BYTES);

        match projection {
            ToolResultProjection::Table(table) => {
                assert_eq!(table.row_count, 100);
                assert_eq!(table.rows.len(), 30);
                assert!(table.truncated);
            }
            other => panic!("expected table projection, got {other:?}"),
        }
    }

    #[test]
    fn truncates_large_json_to_fit_budget() {
        let raw = serde_json::to_string(&json!({
            "items": (0..100).map(|idx| {
                json!({
                    "id": idx,
                    "body": "x".repeat(2_048),
                })
            }).collect::<Vec<_>>()
        }))
        .expect("serialize large json");

        let projection = project_tool_result(&raw, 50, 2_000, 2_048);

        match projection {
            ToolResultProjection::Json { value, truncated } => {
                assert!(truncated);
                assert!(serde_json::to_string(&value).expect("serialize projection").len() <= 2_048);
            }
            other => panic!("expected json projection, got {other:?}"),
        }
    }

    #[test]
    fn truncates_large_text_to_fit_budget() {
        let raw = "a".repeat(10_000);

        let projection = project_tool_result(&raw, 50, 2_000, 256);

        match projection {
            ToolResultProjection::Text { text, truncated } => {
                assert!(truncated);
                assert!(text.len() <= 256);
                assert!(text.ends_with("[truncated]"));
            }
            other => panic!("expected text projection, got {other:?}"),
        }
    }
}
