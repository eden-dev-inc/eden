pub mod agents;
pub mod create;
pub mod delete;
pub mod insert;
pub mod jwt_blacklist;
#[cfg(feature = "llm")]
pub mod llm;
pub mod select;
pub mod update;
pub mod user_notifications;

/// Returns the `&str` for the SQL in the relevant folder.
///
/// In the example _create_ refers to the `../create/` folder, and _user_
/// refers to the `users.sql` file in the folder.
///
/// ```ignore
/// use database::sql_file;
/// let SQL: &str = sql_file!("create", "users");
/// ```
///
/// When `embedded-db` is enabled, CREATE statements are loaded from
/// `sql/turso/create/` (SQLite-compatible DDL). All other operations
/// use the standard SQL files — the `TursoConnection` handles dialect
/// rewriting (`$1` → `?1`, etc.) at runtime.
#[cfg(not(embedded_db))]
#[macro_export]
macro_rules! sql_file {
    ("create", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/create/", $name, ".sql"))
    };
    ("insert", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/insert/", $name, ".sql"))
    };
    ("delete", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/delete/", $name, ".sql"))
    };
    ("update", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/update/", $name, ".sql"))
    };
    ("select", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/select/", $name, ".sql"))
    };
}

#[cfg(embedded_db)]
#[macro_export]
macro_rules! sql_file {
    ("create", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/turso/create/", $name, ".sql"))
    };
    ("insert", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/insert/", $name, ".sql"))
    };
    ("delete", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/delete/", $name, ".sql"))
    };
    ("update", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/update/", $name, ".sql"))
    };
    ("select", $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/select/", $name, ".sql"))
    };
}

/// let SQL: &str = sql_file!("create", "endpoint", "file");
#[cfg(not(embedded_db))]
#[macro_export]
macro_rules! sql_files {
    ("create", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/create/", $dir, "/", $name, ".sql"))
    };
    ("insert", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/insert/", $dir, "/", $name, ".sql"))
    };
    ("delete", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/delete/", $dir, "/", $name, ".sql"))
    };
    ("update", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/update/", $dir, "/", $name, ".sql"))
    };
    ("select", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/select/", $dir, "/", $name, ".sql"))
    };
}

#[cfg(embedded_db)]
#[macro_export]
macro_rules! sql_files {
    ("create", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/turso/create/", $dir, "/", $name, ".sql"))
    };
    ("insert", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/insert/", $dir, "/", $name, ".sql"))
    };
    ("delete", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/delete/", $dir, "/", $name, ".sql"))
    };
    ("update", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/update/", $dir, "/", $name, ".sql"))
    };
    ("select", $dir:expr, $name:expr) => {
        ::std::include_str!(::std::concat!(::std::env!("CARGO_MANIFEST_DIR"), "/sql/select/", $dir, "/", $name, ".sql"))
    };
}
