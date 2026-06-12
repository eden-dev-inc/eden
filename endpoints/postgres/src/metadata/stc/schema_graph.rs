use crate::api::lib::query::QueryInput;
use crate::api::wrapper::input::SqlParam;
use crate::metadata::stc::utils::{RowExt, run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use log::warn;
use postgres_core::{PgSimpleRow, PostgresAsync};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use telemetry::TelemetryWrapper;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSchemaGraph {
    pub schemas: Vec<PostgresSchemaInfo>,
    pub tables: Vec<PostgresTableSchema>,
    pub sampled_at_unix_secs: u64,
    pub total_tables: u32,
    pub truncated: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresSchemaInfo {
    pub name: String,
    pub owner: String,
    pub description: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresTableSchema {
    pub schema_name: String,
    pub table_name: String,
    pub description: String,
    pub row_count_estimate: Option<i64>,
    pub columns: Vec<PostgresColumnSchema>,
    pub primary_key: Vec<String>,
    pub foreign_keys: Vec<PostgresForeignKeySchema>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresColumnSchema {
    pub name: String,
    pub ordinal_position: i32,
    pub source_type: String,
    pub nullable: bool,
    pub description: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresForeignKeySchema {
    pub constraint_name: String,
    pub source_columns: Vec<String>,
    pub target_schema: String,
    pub target_table: String,
    pub target_columns: Vec<String>,
    pub on_delete: String,
    pub on_update: String,
}

impl MetadataCollection for PostgresSchemaGraph {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "schemas".to_string(),
                QueryInput::new(
                    "SELECT
                        n.nspname,
                        COALESCE(r.rolname, '') AS owner,
                        COALESCE(d.description, '') AS description
                    FROM pg_namespace n
                    LEFT JOIN pg_roles r ON r.oid = n.nspowner
                    LEFT JOIN pg_description d ON d.objoid = n.oid AND d.objsubid = 0
                    WHERE n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        AND n.nspname NOT LIKE 'pg\\_%'
                    ORDER BY n.nspname"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "table_count".to_string(),
                QueryInput::new(
                    "SELECT COUNT(*) AS total_tables
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE c.relkind IN ('r', 'p')
                        AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                        AND n.nspname NOT LIKE 'pg\\_%'"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "tables".to_string(),
                QueryInput::new(
                    format!(
                        "SELECT
                            n.nspname,
                            c.relname,
                            c.reltuples::bigint AS row_count_estimate,
                            COALESCE(obj_description(c.oid, 'pg_class'), '') AS description
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace
                        WHERE c.relkind IN ('r', 'p')
                            AND n.nspname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                            AND n.nspname NOT LIKE 'pg\\_%'
                        ORDER BY n.nspname, c.relname
                        LIMIT {}",
                        Self::MAX_TABLES
                    ),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return the PostgreSQL relational schema graph for user schemas and tables"
    }

    fn category(&self) -> &'static str {
        "schema_graph"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Low
    }
}

impl PostgresSchemaGraph {
    const MAX_TABLES: usize = 10_000;
    const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

    pub async fn sync_metadata(
        &self,
        ctx: PostgresAsync,
        telemetry: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry.client_tracer("postgres.schema_graph.sync_metadata".to_string());
        let requests = self.request();

        let schema_rows =
            run_query_with_timeout(&requests["schemas"], ctx.clone(), Self::QUERY_TIMEOUT, "postgres.schema_graph.schemas").await?;
        let schemas = Self::parse_schemas(schema_rows)?;

        let total_tables = match run_single_row(&requests, "table_count", ctx.clone(), Self::QUERY_TIMEOUT).await? {
            Some(row) => Self::u64_to_u32(row.get_u64("total_tables")?, "total_tables"),
            None => 0,
        };
        let truncated = usize::try_from(total_tables).unwrap_or(usize::MAX) > Self::MAX_TABLES;
        if truncated {
            warn!(
                "postgres schema graph truncated at {} tables; endpoint reported {} tables",
                Self::MAX_TABLES,
                total_tables
            );
        }

        let table_rows =
            run_query_with_timeout(&requests["tables"], ctx.clone(), Self::QUERY_TIMEOUT, "postgres.schema_graph.tables").await?;
        let mut tables = Vec::with_capacity(table_rows.len());

        for row in table_rows.into_iter().take(Self::MAX_TABLES) {
            let schema_name = row.get_string("nspname")?;
            let table_name = row.get_string("relname")?;
            let row_count_estimate = Self::parse_optional_i64(&row, "row_count_estimate")?;
            let description = row.get_string("description")?;

            let columns = Self::load_columns(&ctx, &schema_name, &table_name).await?;
            let primary_key = Self::load_primary_key(&ctx, &schema_name, &table_name).await?;
            let foreign_keys = Self::load_foreign_keys(&ctx, &schema_name, &table_name).await?;

            tables.push(PostgresTableSchema {
                schema_name,
                table_name,
                description,
                row_count_estimate,
                columns,
                primary_key,
                foreign_keys,
            });
        }

        Ok(Self {
            schemas,
            tables,
            sampled_at_unix_secs: Utc::now().timestamp().try_into().unwrap_or_default(),
            total_tables,
            truncated,
        })
    }

    fn parse_schemas(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresSchemaInfo>> {
        rows.into_iter()
            .map(|row| {
                Ok(PostgresSchemaInfo {
                    name: row.get_string("nspname")?,
                    owner: row.get_string("owner")?,
                    description: row.get_string("description")?,
                })
            })
            .collect()
    }

    async fn load_columns(context: &PostgresAsync, schema_name: &str, table_name: &str) -> ResultEP<Vec<PostgresColumnSchema>> {
        let query = QueryInput::new(
            "SELECT
                cols.column_name,
                cols.ordinal_position,
                COALESCE(NULLIF(cols.udt_name, ''), cols.data_type) AS source_type,
                cols.is_nullable,
                COALESCE(pd.description, '') AS description
            FROM information_schema.columns cols
            LEFT JOIN pg_namespace n ON n.nspname = cols.table_schema
            LEFT JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = cols.table_name
            LEFT JOIN pg_attribute a ON a.attrelid = c.oid AND a.attname = cols.column_name
            LEFT JOIN pg_description pd ON pd.objoid = c.oid AND pd.objsubid = a.attnum
            WHERE cols.table_schema = $1 AND cols.table_name = $2
            ORDER BY cols.ordinal_position"
                .to_string(),
            vec![SqlParam::Text(schema_name.to_string()), SqlParam::Text(table_name.to_string())],
        );
        let rows = run_query_with_timeout(&query, context.clone(), Self::QUERY_TIMEOUT, "postgres.schema_graph.table_columns").await?;

        rows.into_iter()
            .map(|row| {
                let nullable = matches!(row.get_string("is_nullable")?.as_str(), "YES" | "yes" | "t" | "true");
                Ok(PostgresColumnSchema {
                    name: row.get_string("column_name")?,
                    ordinal_position: row.get_i32("ordinal_position")?,
                    source_type: row.get_string("source_type")?,
                    nullable,
                    description: row.get_string("description")?,
                })
            })
            .collect()
    }

    async fn load_primary_key(context: &PostgresAsync, schema_name: &str, table_name: &str) -> ResultEP<Vec<String>> {
        let query = QueryInput::new(
            "SELECT a.attname
            FROM pg_index i
            JOIN pg_class c ON c.oid = i.indrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN LATERAL unnest(i.indkey) WITH ORDINALITY AS key_cols(attnum, ordinality) ON TRUE
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = key_cols.attnum
            WHERE i.indisprimary
                AND n.nspname = $1
                AND c.relname = $2
            ORDER BY key_cols.ordinality"
                .to_string(),
            vec![SqlParam::Text(schema_name.to_string()), SqlParam::Text(table_name.to_string())],
        );
        let rows = run_query_with_timeout(&query, context.clone(), Self::QUERY_TIMEOUT, "postgres.schema_graph.primary_key").await?;

        rows.into_iter().map(|row| row.get_string("attname")).collect()
    }

    async fn load_foreign_keys(context: &PostgresAsync, schema_name: &str, table_name: &str) -> ResultEP<Vec<PostgresForeignKeySchema>> {
        let query = QueryInput::new(
            "SELECT
                c.conname,
                target_ns.nspname AS target_schema,
                target.relname AS target_table,
                COALESCE(
                    to_jsonb(array_agg(src_attr.attname ORDER BY key_map.ordinality)
                        FILTER (WHERE src_attr.attname IS NOT NULL)),
                    '[]'::jsonb
                ) AS source_columns,
                COALESCE(
                    to_jsonb(array_agg(target_attr.attname ORDER BY key_map.ordinality)
                        FILTER (WHERE target_attr.attname IS NOT NULL)),
                    '[]'::jsonb
                ) AS target_columns,
                c.confupdtype::text AS confupdtype,
                c.confdeltype::text AS confdeltype
            FROM pg_constraint c
            JOIN pg_class source ON source.oid = c.conrelid
            JOIN pg_namespace source_ns ON source_ns.oid = source.relnamespace
            JOIN pg_class target ON target.oid = c.confrelid
            JOIN pg_namespace target_ns ON target_ns.oid = target.relnamespace
            JOIN LATERAL unnest(c.conkey, c.confkey) WITH ORDINALITY AS key_map(source_attnum, target_attnum, ordinality) ON TRUE
            LEFT JOIN pg_attribute src_attr ON src_attr.attrelid = c.conrelid AND src_attr.attnum = key_map.source_attnum
            LEFT JOIN pg_attribute target_attr ON target_attr.attrelid = c.confrelid AND target_attr.attnum = key_map.target_attnum
            WHERE c.contype = 'f'
                AND source_ns.nspname = $1
                AND source.relname = $2
            GROUP BY c.conname, target_ns.nspname, target.relname, c.confupdtype, c.confdeltype
            ORDER BY c.conname"
                .to_string(),
            vec![SqlParam::Text(schema_name.to_string()), SqlParam::Text(table_name.to_string())],
        );
        let rows = run_query_with_timeout(&query, context.clone(), Self::QUERY_TIMEOUT, "postgres.schema_graph.foreign_keys").await?;

        rows.into_iter()
            .map(|row| {
                Ok(PostgresForeignKeySchema {
                    constraint_name: row.get_string("conname")?,
                    source_columns: Self::parse_string_array(row.get_json("source_columns")?, "source_columns")?,
                    target_schema: row.get_string("target_schema")?,
                    target_table: row.get_string("target_table")?,
                    target_columns: Self::parse_string_array(row.get_json("target_columns")?, "target_columns")?,
                    on_delete: Self::referential_action(&row.get_string("confdeltype")?),
                    on_update: Self::referential_action(&row.get_string("confupdtype")?),
                })
            })
            .collect()
    }

    fn parse_optional_i64(row: &PgSimpleRow, column: &str) -> ResultEP<Option<i64>> {
        row.get_opt_string(column)?
            .map(|value| {
                value.parse::<i64>().map_err(|_| EpError::metadata(format!("Failed to parse column {column} value '{value}' as i64")))
            })
            .transpose()
    }

    fn parse_string_array(value: serde_json::Value, field_name: &str) -> ResultEP<Vec<String>> {
        let array = value.as_array().ok_or_else(|| EpError::metadata(format!("Expected JSON array for field {field_name}")))?;

        array
            .iter()
            .map(|item| {
                item.as_str()
                    .map(ToOwned::to_owned)
                    .ok_or_else(|| EpError::metadata(format!("Expected string elements in field {field_name}")))
            })
            .collect()
    }

    fn referential_action(code: &str) -> String {
        match code {
            "a" => "NO ACTION",
            "r" => "RESTRICT",
            "c" => "CASCADE",
            "n" => "SET NULL",
            "d" => "SET DEFAULT",
            _ => "",
        }
        .to_string()
    }

    fn u64_to_u32(value: u64, field_name: &str) -> u32 {
        match u32::try_from(value) {
            Ok(value) => value,
            Err(_) => {
                warn!("value for {field_name} exceeds u32 range, clamping to u32::MAX");
                u32::MAX
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_graph_round_trips_in_json_and_borsh() {
        let graph = PostgresSchemaGraph {
            schemas: vec![PostgresSchemaInfo {
                name: "public".to_string(),
                owner: "postgres".to_string(),
                description: "default schema".to_string(),
            }],
            tables: vec![PostgresTableSchema {
                schema_name: "public".to_string(),
                table_name: "users".to_string(),
                description: "application users".to_string(),
                row_count_estimate: Some(42),
                columns: vec![PostgresColumnSchema {
                    name: "id".to_string(),
                    ordinal_position: 1,
                    source_type: "int8".to_string(),
                    nullable: false,
                    description: "primary key".to_string(),
                }],
                primary_key: vec!["id".to_string()],
                foreign_keys: vec![PostgresForeignKeySchema {
                    constraint_name: "users_account_id_fkey".to_string(),
                    source_columns: vec!["account_id".to_string()],
                    target_schema: "public".to_string(),
                    target_table: "accounts".to_string(),
                    target_columns: vec!["id".to_string()],
                    on_delete: "CASCADE".to_string(),
                    on_update: "NO ACTION".to_string(),
                }],
            }],
            sampled_at_unix_secs: 1,
            total_tables: 1,
            truncated: false,
        };

        let json = serde_json::to_vec(&graph).expect("schema graph should serialize to json");
        let decoded_json: PostgresSchemaGraph = serde_json::from_slice(&json).expect("schema graph should deserialize from json");
        assert_eq!(decoded_json, graph);

        let borsh = borsh::to_vec(&graph).expect("schema graph should serialize to borsh");
        let decoded_borsh = borsh::from_slice::<PostgresSchemaGraph>(&borsh).expect("schema graph should deserialize from borsh");
        assert_eq!(decoded_borsh, graph);
    }
}
