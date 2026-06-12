#[cfg(feature = "integration")]
pub mod context;

#[cfg(feature = "integration")]
mod advanced_sql;
#[cfg(feature = "integration")]
mod aggregate_ext;
#[cfg(feature = "integration")]
mod aggregates;
#[cfg(feature = "integration")]
mod api_behavior;
#[cfg(feature = "integration")]
mod array_operations;
#[cfg(feature = "integration")]
mod array_types;
#[cfg(feature = "integration")]
mod batch_execute_tests;
#[cfg(feature = "integration")]
mod common_patterns;
#[cfg(feature = "integration")]
mod conditional_expressions;
#[cfg(feature = "integration")]
mod copy_operations;
#[cfg(feature = "integration")]
mod data_types;
#[cfg(feature = "integration")]
mod data_types_extended;
#[cfg(feature = "integration")]
mod date_time_ext;
#[cfg(feature = "integration")]
mod ddl;
#[cfg(feature = "integration")]
mod dml;
#[cfg(feature = "integration")]
mod edge_cases;
#[cfg(feature = "integration")]
mod error_handling;
#[cfg(feature = "integration")]
mod error_scenarios;
#[cfg(feature = "integration")]
mod generated_columns;
#[cfg(feature = "integration")]
mod grouping_sets;
#[cfg(feature = "integration")]
mod joins;
#[cfg(feature = "integration")]
mod json_operations;
#[cfg(feature = "integration")]
mod lateral_joins;
#[cfg(feature = "integration")]
mod plpgsql;
#[cfg(feature = "integration")]
mod query_typed_tests;
#[cfg(feature = "integration")]
mod regex;
#[cfg(feature = "integration")]
mod select;
#[cfg(feature = "integration")]
mod simple_query_tests;
#[cfg(feature = "integration")]
mod sql_functions;
#[cfg(feature = "integration")]
mod string_functions_ext;
#[cfg(feature = "integration")]
mod subqueries;
#[cfg(feature = "integration")]
mod table_inheritance;
#[cfg(feature = "integration")]
mod temp_tables;
#[cfg(feature = "integration")]
mod transactions;
#[cfg(feature = "integration")]
mod type_coercion;
#[cfg(feature = "integration")]
mod views_sequences;
#[cfg(feature = "integration")]
mod window_functions;
