//! Statement and cursor lifecycle management.
//!
//! Oracle uses cursors to manage SQL statements. A typical lifecycle is:
//! 1. Parse: Compile SQL and get cursor ID
//! 2. Describe: Get column metadata (for queries)
//! 3. Execute: Run the statement
//! 4. Fetch: Retrieve result rows (for queries)
//! 5. Close: Release the cursor

use super::bind::BindSet;
use super::column::ResultSetMetadata;
use super::function_codes::FunctionCode;

/// Statement type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatementType {
    /// SELECT query.
    Select,
    /// INSERT statement.
    Insert,
    /// UPDATE statement.
    Update,
    /// DELETE statement.
    Delete,
    /// MERGE statement.
    Merge,
    /// CREATE (DDL).
    Create,
    /// ALTER (DDL).
    Alter,
    /// DROP (DDL).
    Drop,
    /// PL/SQL block.
    PlSql,
    /// CALL procedure.
    Call,
    /// EXPLAIN PLAN.
    Explain,
    /// Unknown/other.
    Unknown,
}

impl StatementType {
    /// Check if this statement returns rows.
    pub const fn returns_rows(self) -> bool {
        matches!(self, Self::Select)
    }

    /// Check if this is a DML statement.
    pub const fn is_dml(self) -> bool {
        matches!(self, Self::Insert | Self::Update | Self::Delete | Self::Merge)
    }

    /// Check if this is a DDL statement.
    pub const fn is_ddl(self) -> bool {
        matches!(self, Self::Create | Self::Alter | Self::Drop)
    }

    /// Check if this is PL/SQL.
    pub const fn is_plsql(self) -> bool {
        matches!(self, Self::PlSql | Self::Call)
    }

    /// Parse from Oracle's statement type code.
    pub const fn from_code(code: u8) -> Self {
        match code {
            0x01 => Self::Select,
            0x02 => Self::Insert,
            0x03 => Self::Update,
            0x04 => Self::Delete,
            0x05 => Self::Create,
            0x06 => Self::Drop,
            0x07 => Self::Alter,
            0x08 | 0x0E => Self::PlSql,
            0x09 => Self::Merge,
            _ => Self::Unknown,
        }
    }
}

/// Cursor state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CursorState {
    /// Cursor is not initialized.
    #[default]
    Uninitialized,
    /// Statement has been parsed.
    Parsed,
    /// Statement has been described.
    Described,
    /// Statement has been executed.
    Executed,
    /// Fetch in progress (for queries).
    Fetching,
    /// All rows fetched.
    Exhausted,
    /// Cursor is closed.
    Closed,
}

/// A cursor handle.
#[derive(Clone, Debug)]
pub struct Cursor {
    /// Cursor ID assigned by server.
    id: u32,
    /// Current state.
    state: CursorState,
    /// Statement type (after parse).
    statement_type: Option<StatementType>,
    /// Column metadata (after describe).
    metadata: Option<ResultSetMetadata>,
    /// Rows affected (for DML).
    rows_affected: Option<u64>,
    /// Whether there are more rows to fetch.
    has_more_rows: bool,
}

impl Cursor {
    /// Create a new cursor with the given ID.
    pub fn new(id: u32) -> Self {
        Self {
            id,
            state: CursorState::Parsed,
            statement_type: None,
            metadata: None,
            rows_affected: None,
            has_more_rows: false,
        }
    }

    /// Get the cursor ID.
    pub fn id(&self) -> u32 {
        self.id
    }

    /// Get the current state.
    pub fn state(&self) -> CursorState {
        self.state
    }

    /// Get the statement type.
    pub fn statement_type(&self) -> Option<StatementType> {
        self.statement_type
    }

    /// Get column metadata.
    pub fn metadata(&self) -> Option<&ResultSetMetadata> {
        self.metadata.as_ref()
    }

    /// Get rows affected.
    pub fn rows_affected(&self) -> Option<u64> {
        self.rows_affected
    }

    /// Check if there are more rows.
    pub fn has_more_rows(&self) -> bool {
        self.has_more_rows
    }

    /// Check if cursor is usable.
    pub fn is_usable(&self) -> bool {
        !matches!(self.state, CursorState::Uninitialized | CursorState::Closed)
    }

    /// Set the statement type.
    pub fn set_statement_type(&mut self, stmt_type: StatementType) {
        self.statement_type = Some(stmt_type);
    }

    /// Set metadata after describe.
    pub fn set_metadata(&mut self, metadata: ResultSetMetadata) {
        self.metadata = Some(metadata);
        self.state = CursorState::Described;
    }

    /// Mark as executed.
    pub fn mark_executed(&mut self, rows_affected: Option<u64>) {
        self.state = CursorState::Executed;
        self.rows_affected = rows_affected;
        if self.statement_type.is_some_and(|t| t.returns_rows()) {
            self.has_more_rows = true;
        }
    }

    /// Mark as fetching.
    pub fn mark_fetching(&mut self, has_more: bool) {
        self.state = CursorState::Fetching;
        self.has_more_rows = has_more;
    }

    /// Mark as exhausted (no more rows).
    pub fn mark_exhausted(&mut self) {
        self.state = CursorState::Exhausted;
        self.has_more_rows = false;
    }

    /// Mark as closed.
    pub fn mark_closed(&mut self) {
        self.state = CursorState::Closed;
    }
}

/// Parse statement request.
#[derive(Clone, Debug)]
pub struct ParseRequest {
    /// SQL statement.
    pub sql: String,
    /// Whether to only parse (not prepare).
    pub parse_only: bool,
    /// Cursor ID to reuse (0 for new).
    pub cursor_id: u32,
    /// Statement tag for caching.
    pub tag: Option<String>,
}

impl ParseRequest {
    /// Create a new parse request.
    pub fn new(sql: impl Into<String>) -> Self {
        Self { sql: sql.into(), parse_only: false, cursor_id: 0, tag: None }
    }

    /// Reuse an existing cursor.
    pub fn with_cursor(mut self, cursor_id: u32) -> Self {
        self.cursor_id = cursor_id;
        self
    }

    /// Set parse-only mode.
    pub fn parse_only(mut self) -> Self {
        self.parse_only = true;
        self
    }

    /// Set a statement tag.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let sql_bytes = self.sql.as_bytes();
        let tag_bytes = self.tag.as_ref().map(|t| t.as_bytes());
        let tag_len = tag_bytes.map(|b| b.len()).unwrap_or(0);

        let mut buf = Vec::with_capacity(sql_bytes.len() + tag_len + 20);

        // Function code
        buf.push(FunctionCode::Parse.as_u8());

        // Cursor ID (4 bytes)
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Flags
        let flags: u8 = if self.parse_only { 0x01 } else { 0x00 };
        buf.push(flags);

        // SQL length (4 bytes) and data
        buf.extend_from_slice(&(sql_bytes.len() as u32).to_be_bytes());
        buf.extend_from_slice(sql_bytes);

        // Tag (if present)
        if let Some(tag) = tag_bytes {
            buf.push(tag.len() as u8);
            buf.extend_from_slice(tag);
        } else {
            buf.push(0);
        }

        buf
    }
}

/// Parse response.
#[derive(Clone, Debug)]
pub struct ParseResponse {
    /// Assigned cursor ID.
    pub cursor_id: u32,
    /// Statement type.
    pub statement_type: StatementType,
    /// Number of bind variables.
    pub bind_count: u16,
    /// Number of define (output) columns.
    pub define_count: u16,
}

impl ParseResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 10 {
            return Err(StatementError::TooShort { expected: 10, actual: data.len() });
        }

        let cursor_id = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let statement_type = StatementType::from_code(data[4]);
        let bind_count = u16::from_be_bytes([data[5], data[6]]);
        let define_count = u16::from_be_bytes([data[7], data[8]]);

        Ok(Self { cursor_id, statement_type, bind_count, define_count })
    }
}

/// Execute statement request.
#[derive(Clone, Debug)]
pub struct ExecuteRequest {
    /// Cursor ID.
    pub cursor_id: u32,
    /// Bind variables.
    pub binds: Option<BindSet>,
    /// Number of rows to execute (for array operations).
    pub iterations: u32,
    /// Execute options.
    pub options: ExecuteOptions,
}

impl ExecuteRequest {
    /// Create a new execute request.
    pub fn new(cursor_id: u32) -> Self {
        Self {
            cursor_id,
            binds: None,
            iterations: 1,
            options: ExecuteOptions::default(),
        }
    }

    /// Set bind variables.
    pub fn with_binds(mut self, binds: BindSet) -> Self {
        self.binds = Some(binds);
        self
    }

    /// Set iterations for array execute.
    pub fn with_iterations(mut self, iterations: u32) -> Self {
        self.iterations = iterations;
        self
    }

    /// Set options.
    pub fn with_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);

        // Function code
        buf.push(FunctionCode::Execute.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Options flags
        buf.extend_from_slice(&self.options.as_flags().to_be_bytes());

        // Iterations
        buf.extend_from_slice(&self.iterations.to_be_bytes());

        // Binds (if present)
        if let Some(ref binds) = self.binds {
            buf.extend(binds.encode_descriptors());
            buf.extend(binds.encode_values(super::charset::CharsetId::AL32UTF8));
        } else {
            // No binds
            buf.extend_from_slice(&0u16.to_be_bytes());
        }

        buf
    }
}

/// Execute options.
#[derive(Clone, Copy, Debug, Default)]
pub struct ExecuteOptions {
    /// Auto-commit after execution.
    pub auto_commit: bool,
    /// Describe columns before execute.
    pub describe_before: bool,
    /// Don't execute, just describe.
    pub describe_only: bool,
    /// Batch errors (continue on error).
    pub batch_errors: bool,
    /// Prefetch rows on execute (for queries).
    pub prefetch_rows: u32,
}

impl ExecuteOptions {
    /// Create default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable auto-commit.
    pub fn auto_commit(mut self) -> Self {
        self.auto_commit = true;
        self
    }

    /// Enable describe before execute.
    pub fn describe_before(mut self) -> Self {
        self.describe_before = true;
        self
    }

    /// Enable describe only (no execute).
    pub fn describe_only(mut self) -> Self {
        self.describe_only = true;
        self
    }

    /// Enable batch errors.
    pub fn batch_errors(mut self) -> Self {
        self.batch_errors = true;
        self
    }

    /// Set prefetch rows.
    pub fn with_prefetch(mut self, rows: u32) -> Self {
        self.prefetch_rows = rows;
        self
    }

    /// Convert to wire protocol flags.
    pub fn as_flags(self) -> u32 {
        let mut flags = 0u32;
        if self.auto_commit {
            flags |= 0x0001;
        }
        if self.describe_before {
            flags |= 0x0002;
        }
        if self.describe_only {
            flags |= 0x0004;
        }
        if self.batch_errors {
            flags |= 0x0008;
        }
        // Prefetch rows in upper 16 bits
        flags |= (self.prefetch_rows & 0xFFFF) << 16;
        flags
    }
}

/// Execute response.
#[derive(Clone, Debug)]
pub struct ExecuteResponse {
    /// Rows affected (for DML).
    pub rows_affected: u64,
    /// Whether there are more rows (for query with prefetch).
    pub has_more_rows: bool,
    /// Prefetched row count.
    pub prefetched_rows: u32,
    /// Last ROWID (for single-row DML).
    pub last_rowid: Option<String>,
}

impl ExecuteResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 12 {
            return Err(StatementError::TooShort { expected: 12, actual: data.len() });
        }

        let rows_affected = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let has_more_rows = data[8] != 0;
        let prefetched_rows = u32::from_be_bytes([data[9], data[10], data[11], 0]);

        Ok(Self {
            rows_affected,
            has_more_rows,
            prefetched_rows,
            last_rowid: None,
        })
    }
}

/// Fetch response containing rows.
#[derive(Clone, Debug)]
pub struct FetchResponse {
    /// Number of rows returned.
    pub row_count: u32,
    /// Whether there are more rows to fetch.
    pub has_more: bool,
    /// Row data (raw bytes, needs to be decoded using column metadata).
    pub row_data: Vec<u8>,
    /// Offset into row_data for each row start.
    pub row_offsets: Vec<usize>,
}

impl FetchResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            row_count: 0,
            has_more: false,
            row_data: Vec::new(),
            row_offsets: Vec::new(),
        }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 5 {
            return Err(StatementError::TooShort { expected: 5, actual: data.len() });
        }

        let row_count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let has_more = data[4] != 0;

        // The rest is row data
        let row_data = if data.len() > 5 { data[5..].to_vec() } else { Vec::new() };

        Ok(Self {
            row_count,
            has_more,
            row_data,
            row_offsets: Vec::new(), // Offsets computed during decoding
        })
    }

    /// Check if this response has data.
    pub fn has_data(&self) -> bool {
        self.row_count > 0
    }

    /// Check if all rows have been fetched.
    pub fn is_complete(&self) -> bool {
        !self.has_more
    }
}

/// Describe response with column metadata.
#[derive(Clone, Debug)]
pub struct DescribeResponse {
    /// Number of columns.
    pub column_count: u16,
    /// Column descriptors.
    pub columns: Vec<ColumnDescriptor>,
    /// Number of bind variables (for prepared statements).
    pub bind_count: u16,
    /// Bind variable descriptors.
    pub binds: Vec<BindDescriptor>,
}

impl DescribeResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            column_count: 0,
            columns: Vec::new(),
            bind_count: 0,
            binds: Vec::new(),
        }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 4 {
            return Err(StatementError::TooShort { expected: 4, actual: data.len() });
        }

        let column_count = u16::from_be_bytes([data[0], data[1]]);
        let bind_count = u16::from_be_bytes([data[2], data[3]]);

        // Parse column descriptors
        let mut offset = 4;
        let mut columns = Vec::with_capacity(column_count as usize);

        for _ in 0..column_count {
            if offset + 10 > data.len() {
                // Need at least 10 bytes for fixed fields (minimum before name)
                break;
            }

            let col = ColumnDescriptor::parse(&data[offset..])?;
            offset += col.wire_size();
            columns.push(col);
        }

        // Parse bind descriptors
        let mut binds = Vec::with_capacity(bind_count as usize);
        for _ in 0..bind_count {
            if offset + 7 > data.len() {
                // Need at least 7 bytes for fixed fields (minimum before name)
                break;
            }

            let bind = BindDescriptor::parse(&data[offset..])?;
            offset += bind.wire_size();
            binds.push(bind);
        }

        Ok(Self { column_count, columns, bind_count, binds })
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

/// Column descriptor from describe response.
#[derive(Clone, Debug)]
pub struct ColumnDescriptor {
    /// Column name.
    pub name: String,
    /// Oracle data type.
    pub data_type: super::data_types::OracleDataType,
    /// Maximum size in bytes.
    pub max_size: u32,
    /// Precision (for NUMBER).
    pub precision: u8,
    /// Scale (for NUMBER).
    pub scale: i8,
    /// Whether NULL is allowed.
    pub nullable: bool,
    /// Character set form (1=CHAR, 2=NCHAR).
    pub charset_form: u8,
}

impl ColumnDescriptor {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 12 {
            return Err(StatementError::TooShort { expected: 12, actual: data.len() });
        }

        let data_type = super::data_types::OracleDataType::from_u8(data[0]);
        let max_size = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let precision = data[5];
        let scale = data[6] as i8;
        let nullable = data[7] != 0;
        let charset_form = data[8];
        let name_len = data[9] as usize;

        let name = if data.len() >= 10 + name_len {
            String::from_utf8_lossy(&data[10..10 + name_len]).to_string()
        } else {
            String::new()
        };

        Ok(Self {
            name,
            data_type,
            max_size,
            precision,
            scale,
            nullable,
            charset_form,
        })
    }

    /// Get wire size of this descriptor.
    pub fn wire_size(&self) -> usize {
        10 + self.name.len()
    }
}

/// Bind descriptor from describe response.
#[derive(Clone, Debug)]
pub struct BindDescriptor {
    /// Bind name (e.g., ":1" or ":name").
    pub name: String,
    /// Oracle data type.
    pub data_type: super::data_types::OracleDataType,
    /// Maximum size in bytes.
    pub max_size: u32,
    /// Direction (IN, OUT, INOUT).
    pub direction: super::bind::BindDirection,
}

impl BindDescriptor {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 8 {
            return Err(StatementError::TooShort { expected: 8, actual: data.len() });
        }

        let data_type = super::data_types::OracleDataType::from_u8(data[0]);
        let max_size = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let direction = super::bind::BindDirection::from_code(data[5]).unwrap_or_default();
        let name_len = data[6] as usize;

        let name = if data.len() >= 7 + name_len {
            String::from_utf8_lossy(&data[7..7 + name_len]).to_string()
        } else {
            String::new()
        };

        Ok(Self { name, data_type, max_size, direction })
    }

    /// Get wire size of this descriptor.
    pub fn wire_size(&self) -> usize {
        7 + self.name.len()
    }
}

/// Fetch rows request.
#[derive(Clone, Debug)]
pub struct FetchRequest {
    /// Cursor ID.
    pub cursor_id: u32,
    /// Number of rows to fetch.
    pub fetch_size: u32,
}

impl FetchRequest {
    /// Create a new fetch request.
    pub fn new(cursor_id: u32, fetch_size: u32) -> Self {
        Self { cursor_id, fetch_size }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);

        // Function code
        buf.push(FunctionCode::Fetch.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Fetch size
        buf.extend_from_slice(&self.fetch_size.to_be_bytes());

        buf
    }
}

/// Close cursor request.
#[derive(Clone, Debug)]
pub struct CloseRequest {
    /// Cursor ID to close.
    pub cursor_id: u32,
}

impl CloseRequest {
    /// Create a new close request.
    pub fn new(cursor_id: u32) -> Self {
        Self { cursor_id }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);

        // Function code
        buf.push(FunctionCode::CloseCursor.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        buf
    }
}

/// Describe request (get column metadata).
#[derive(Clone, Debug)]
pub struct DescribeRequest {
    /// Cursor ID.
    pub cursor_id: u32,
}

impl DescribeRequest {
    /// Create a new describe request.
    pub fn new(cursor_id: u32) -> Self {
        Self { cursor_id }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);

        // Function code
        buf.push(FunctionCode::Describe.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        buf
    }
}

/// Commit request.
#[derive(Clone, Debug)]
pub struct CommitRequest;

impl CommitRequest {
    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        vec![FunctionCode::Commit.as_u8()]
    }
}

/// Rollback request.
#[derive(Clone, Debug)]
pub struct RollbackRequest;

impl RollbackRequest {
    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        vec![FunctionCode::Rollback.as_u8()]
    }
}

/// Transaction begin request (function code 0x03).
///
/// Explicitly starts a new transaction. In Oracle, transactions are implicit
/// (started with the first DML), but this can be used to explicitly mark
/// the start of a transaction boundary.
#[derive(Clone, Debug)]
pub struct TransactionBeginRequest {
    /// Transaction isolation level.
    pub isolation_level: TransactionIsolation,
    /// Transaction name (for debugging/monitoring).
    pub name: Option<String>,
    /// Read-only transaction.
    pub read_only: bool,
}

impl TransactionBeginRequest {
    /// Create a new transaction begin request with default settings.
    pub fn new() -> Self {
        Self {
            isolation_level: TransactionIsolation::ReadCommitted,
            name: None,
            read_only: false,
        }
    }

    /// Set the isolation level.
    pub fn with_isolation(mut self, level: TransactionIsolation) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set a transaction name.
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Mark as read-only transaction.
    pub fn read_only(mut self) -> Self {
        self.read_only = true;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(32);

        // Function code
        buf.push(FunctionCode::TransactionBegin.as_u8());

        // Flags
        let mut flags: u8 = 0;
        if self.read_only {
            flags |= 0x01;
        }
        match self.isolation_level {
            TransactionIsolation::ReadCommitted => flags |= 0x00,
            TransactionIsolation::Serializable => flags |= 0x02,
            TransactionIsolation::ReadOnly => flags |= 0x04,
        }
        buf.push(flags);

        // Transaction name (optional, length-prefixed)
        if let Some(ref name) = self.name {
            let name_bytes = name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
        } else {
            buf.push(0);
        }

        buf
    }
}

impl Default for TransactionBeginRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Transaction isolation level.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TransactionIsolation {
    /// READ COMMITTED - default Oracle isolation level.
    #[default]
    ReadCommitted,
    /// SERIALIZABLE - strict isolation.
    Serializable,
    /// READ ONLY - no modifications allowed.
    ReadOnly,
}

impl TransactionIsolation {
    /// Get the isolation level code.
    pub fn code(&self) -> u8 {
        match self {
            Self::ReadCommitted => 0x00,
            Self::Serializable => 0x01,
            Self::ReadOnly => 0x02,
        }
    }

    /// Parse from code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x00 => Some(Self::ReadCommitted),
            0x01 => Some(Self::Serializable),
            0x02 => Some(Self::ReadOnly),
            _ => None,
        }
    }
}

/// Execute and fetch combined request (function code 0x12).
///
/// This is an optimization that combines execute and fetch into a single round-trip,
/// reducing network latency for queries. The server executes the statement and
/// returns the first batch of rows in the same response.
#[derive(Clone, Debug)]
pub struct ExecuteAndFetchRequest {
    /// Cursor ID.
    pub cursor_id: u32,
    /// Bind variables.
    pub binds: Option<BindSet>,
    /// Number of rows to fetch in the response.
    pub fetch_size: u32,
    /// Execute options.
    pub options: ExecuteOptions,
}

impl ExecuteAndFetchRequest {
    /// Create a new execute-and-fetch request.
    pub fn new(cursor_id: u32, fetch_size: u32) -> Self {
        Self {
            cursor_id,
            binds: None,
            fetch_size,
            options: ExecuteOptions::default(),
        }
    }

    /// Set bind variables.
    pub fn with_binds(mut self, binds: BindSet) -> Self {
        self.binds = Some(binds);
        self
    }

    /// Set options.
    pub fn with_options(mut self, options: ExecuteOptions) -> Self {
        self.options = options;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);

        // Function code
        buf.push(FunctionCode::ExecuteAndFetch.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Options flags
        buf.extend_from_slice(&self.options.as_flags().to_be_bytes());

        // Fetch size
        buf.extend_from_slice(&self.fetch_size.to_be_bytes());

        // Binds (if present)
        if let Some(ref binds) = self.binds {
            buf.extend(binds.encode_descriptors());
            buf.extend(binds.encode_values(super::charset::CharsetId::AL32UTF8));
        } else {
            buf.extend_from_slice(&0u16.to_be_bytes());
        }

        buf
    }
}

/// Execute and fetch combined response.
#[derive(Clone, Debug)]
pub struct ExecuteAndFetchResponse {
    /// Rows affected (for DML portion if any).
    pub rows_affected: u64,
    /// Number of rows returned.
    pub row_count: u32,
    /// Whether there are more rows to fetch.
    pub has_more_rows: bool,
    /// Row data (raw bytes, needs to be decoded using column metadata).
    pub row_data: Vec<u8>,
    /// Offset into row_data for each row start.
    pub row_offsets: Vec<usize>,
}

impl ExecuteAndFetchResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            rows_affected: 0,
            row_count: 0,
            has_more_rows: false,
            row_data: Vec::new(),
            row_offsets: Vec::new(),
        }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 13 {
            return Err(StatementError::TooShort { expected: 13, actual: data.len() });
        }

        let rows_affected = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let row_count = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let has_more_rows = data[12] != 0;

        let row_data = if data.len() > 13 { data[13..].to_vec() } else { Vec::new() };

        Ok(Self {
            rows_affected,
            row_count,
            has_more_rows,
            row_data,
            row_offsets: Vec::new(),
        })
    }

    /// Check if this response has data.
    pub fn has_data(&self) -> bool {
        self.row_count > 0
    }

    /// Check if all rows have been fetched.
    pub fn is_complete(&self) -> bool {
        !self.has_more_rows
    }
}

/// Batch execute request (function code 0x70).
///
/// Executes the same statement multiple times with different bind values.
/// This is more efficient than individual execute calls for bulk DML operations.
#[derive(Clone, Debug)]
pub struct BatchExecuteRequest {
    /// Cursor ID.
    pub cursor_id: u32,
    /// Number of iterations (rows to process).
    pub batch_size: u32,
    /// Bind variables for all iterations.
    /// The BindSet should contain arrays of values, one per iteration.
    pub binds: BindSet,
    /// Execute options.
    pub options: BatchExecuteOptions,
}

impl BatchExecuteRequest {
    /// Create a new batch execute request.
    pub fn new(cursor_id: u32, batch_size: u32, binds: BindSet) -> Self {
        Self {
            cursor_id,
            batch_size,
            binds,
            options: BatchExecuteOptions::default(),
        }
    }

    /// Set options.
    pub fn with_options(mut self, options: BatchExecuteOptions) -> Self {
        self.options = options;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(128);

        // Function code
        buf.push(FunctionCode::BatchExecute.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Batch size
        buf.extend_from_slice(&self.batch_size.to_be_bytes());

        // Options flags
        buf.extend_from_slice(&self.options.as_flags().to_be_bytes());

        // Bind descriptors
        buf.extend(self.binds.encode_descriptors());

        // All bind values for all iterations
        buf.extend(self.binds.encode_values(super::charset::CharsetId::AL32UTF8));

        buf
    }
}

/// Batch execute options.
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchExecuteOptions {
    /// Auto-commit after all iterations complete.
    pub auto_commit: bool,
    /// Continue on error (collect errors for each failed row).
    pub continue_on_error: bool,
    /// Return rows affected per iteration.
    pub return_row_counts: bool,
    /// Return generated keys (for INSERT with RETURNING).
    pub return_generated_keys: bool,
}

impl BatchExecuteOptions {
    /// Create default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable auto-commit.
    pub fn auto_commit(mut self) -> Self {
        self.auto_commit = true;
        self
    }

    /// Enable continue on error.
    pub fn continue_on_error(mut self) -> Self {
        self.continue_on_error = true;
        self
    }

    /// Enable row count tracking per iteration.
    pub fn return_row_counts(mut self) -> Self {
        self.return_row_counts = true;
        self
    }

    /// Enable generated keys return.
    pub fn return_generated_keys(mut self) -> Self {
        self.return_generated_keys = true;
        self
    }

    /// Convert to wire protocol flags.
    pub fn as_flags(self) -> u32 {
        let mut flags = 0u32;
        if self.auto_commit {
            flags |= 0x0001;
        }
        if self.continue_on_error {
            flags |= 0x0002;
        }
        if self.return_row_counts {
            flags |= 0x0004;
        }
        if self.return_generated_keys {
            flags |= 0x0008;
        }
        flags
    }
}

/// Batch execute response.
#[derive(Clone, Debug)]
pub struct BatchExecuteResponse {
    /// Total rows affected across all iterations.
    pub total_rows_affected: u64,
    /// Number of successful iterations.
    pub success_count: u32,
    /// Number of failed iterations (if continue_on_error was enabled).
    pub error_count: u32,
    /// Rows affected per iteration (if return_row_counts was enabled).
    pub row_counts: Vec<u64>,
    /// Errors per failed iteration (if continue_on_error was enabled).
    /// Each entry is (iteration_index, error_code, error_message).
    pub errors: Vec<BatchError>,
}

/// Error from a batch iteration.
#[derive(Clone, Debug)]
pub struct BatchError {
    /// Zero-based index of the failed iteration.
    pub index: u32,
    /// Oracle error code.
    pub code: i32,
    /// Error message.
    pub message: String,
}

impl BatchExecuteResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            total_rows_affected: 0,
            success_count: 0,
            error_count: 0,
            row_counts: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 16 {
            return Err(StatementError::TooShort { expected: 16, actual: data.len() });
        }

        let total_rows_affected = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);
        let success_count = u32::from_be_bytes([data[8], data[9], data[10], data[11]]);
        let error_count = u32::from_be_bytes([data[12], data[13], data[14], data[15]]);

        // Parse row counts and errors from remaining data
        let mut offset = 16;
        let mut row_counts = Vec::new();
        let mut errors = Vec::new();

        // Row counts (if present)
        if offset + 4 <= data.len() {
            let count = u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            offset += 4;

            for _ in 0..count {
                if offset + 8 <= data.len() {
                    let row_count = u64::from_be_bytes([
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                        data[offset + 4],
                        data[offset + 5],
                        data[offset + 6],
                        data[offset + 7],
                    ]);
                    row_counts.push(row_count);
                    offset += 8;
                }
            }
        }

        // Errors (if present)
        if offset + 4 <= data.len() {
            let err_list_count = u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            offset += 4;

            for _ in 0..err_list_count {
                if offset + 8 <= data.len() {
                    let idx = u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
                    let code = i32::from_be_bytes([data[offset + 4], data[offset + 5], data[offset + 6], data[offset + 7]]);
                    offset += 8;

                    // Message length and content
                    let msg_len = if offset < data.len() { data[offset] as usize } else { 0 };
                    offset += 1;

                    let message = if offset + msg_len <= data.len() {
                        String::from_utf8_lossy(&data[offset..offset + msg_len]).to_string()
                    } else {
                        String::new()
                    };
                    offset += msg_len;

                    errors.push(BatchError { index: idx, code, message });
                }
            }
        }

        Ok(Self {
            total_rows_affected,
            success_count,
            error_count,
            row_counts,
            errors,
        })
    }

    /// Check if all iterations succeeded.
    pub fn is_success(&self) -> bool {
        self.error_count == 0
    }

    /// Check if there were partial failures.
    pub fn has_errors(&self) -> bool {
        self.error_count > 0
    }
}

/// Open cursor request (function code 0x06).
///
/// Pre-allocates a cursor on the server. This is typically used when you want
/// to reuse a cursor across multiple statements or when implementing cursor
/// pooling.
#[derive(Clone, Debug)]
pub struct OpenCursorRequest {
    /// Number of cursors to open.
    pub count: u16,
}

impl OpenCursorRequest {
    /// Create a request to open one cursor.
    pub fn new() -> Self {
        Self { count: 1 }
    }

    /// Create a request to open multiple cursors.
    pub fn with_count(count: u16) -> Self {
        Self { count }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4);

        // Function code
        buf.push(FunctionCode::OpenCursor.as_u8());

        // Count (2 bytes)
        buf.extend_from_slice(&self.count.to_be_bytes());

        buf
    }
}

impl Default for OpenCursorRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Open cursor response.
#[derive(Clone, Debug)]
pub struct OpenCursorResponse {
    /// Cursor IDs allocated by the server.
    pub cursor_ids: Vec<u32>,
}

impl OpenCursorResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 2 {
            return Err(StatementError::TooShort { expected: 2, actual: data.len() });
        }

        let count = u16::from_be_bytes([data[0], data[1]]) as usize;

        if data.len() < 2 + count * 4 {
            return Err(StatementError::TooShort { expected: 2 + count * 4, actual: data.len() });
        }

        let mut cursor_ids = Vec::with_capacity(count);
        for i in 0..count {
            let offset = 2 + i * 4;
            let id = u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            cursor_ids.push(id);
        }

        Ok(Self { cursor_ids })
    }

    /// Get the first cursor ID (convenience for single-cursor requests).
    pub fn cursor_id(&self) -> Option<u32> {
        self.cursor_ids.first().copied()
    }
}

/// Statement error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum StatementError {
    #[error("data too short: expected {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("cursor {0} not found")]
    CursorNotFound(u32),
    #[error("cursor {0} is closed")]
    CursorClosed(u32),
    #[error("invalid cursor state for operation: {0:?}")]
    InvalidState(CursorState),
    #[error("parse error: {0}")]
    ParseError(String),
    #[error("SQL error {code}: {message}")]
    SqlError { code: i32, message: String },
}

// ============================================================================
// Scrollable Cursor Support (Oracle 8i+)
// ============================================================================
//
// Scrollable cursors allow bidirectional navigation through result sets.
// Unlike forward-only cursors (the default), scrollable cursors can:
// - Move backward through rows
// - Jump to absolute positions
// - Move relative to current position
// - Jump to first/last row
//
// These types are ADDITIVE and do not replace the existing Cursor/FetchRequest.
// Use the standard Cursor for forward-only access (more efficient).

/// Scroll direction for scrollable cursor fetch.
///
/// Specifies how to navigate within a scrollable result set.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ScrollDirection {
    /// Fetch rows forward from current position (default).
    #[default]
    Forward,
    /// Fetch rows backward from current position.
    Backward,
    /// Jump to an absolute row position (1-based).
    Absolute,
    /// Move relative to current position (can be negative).
    Relative,
    /// Jump to the first row.
    First,
    /// Jump to the last row.
    Last,
}

impl ScrollDirection {
    /// Get the wire protocol code.
    pub fn code(&self) -> u8 {
        match self {
            Self::Forward => 0x00,
            Self::Backward => 0x01,
            Self::Absolute => 0x02,
            Self::Relative => 0x03,
            Self::First => 0x04,
            Self::Last => 0x05,
        }
    }

    /// Parse from wire code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x00 => Some(Self::Forward),
            0x01 => Some(Self::Backward),
            0x02 => Some(Self::Absolute),
            0x03 => Some(Self::Relative),
            0x04 => Some(Self::First),
            0x05 => Some(Self::Last),
            _ => None,
        }
    }
}

/// Scrollable cursor mode.
///
/// Determines sensitivity to changes made by other transactions.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ScrollMode {
    /// Scroll-insensitive: result set is a snapshot.
    /// Changes by other transactions are not visible.
    #[default]
    Insensitive,
    /// Scroll-sensitive: changes by other transactions may be visible.
    /// More expensive but provides live view of data.
    Sensitive,
}

impl ScrollMode {
    /// Get the wire protocol code.
    pub fn code(&self) -> u8 {
        match self {
            Self::Insensitive => 0x00,
            Self::Sensitive => 0x01,
        }
    }
}

/// A scrollable cursor handle.
///
/// Extends the basic Cursor with bidirectional navigation support.
/// Use this for result sets that need backward navigation or random access.
///
/// For forward-only access, use the standard `Cursor` type instead.
#[derive(Clone, Debug)]
pub struct ScrollableCursor {
    /// The underlying cursor.
    inner: Cursor,
    /// Scroll mode (sensitive/insensitive).
    scroll_mode: ScrollMode,
    /// Current absolute position (1-based, None if not positioned).
    current_position: Option<u64>,
    /// Total number of rows (if known).
    total_rows: Option<u64>,
}

impl ScrollableCursor {
    /// Create a new scrollable cursor.
    pub fn new(id: u32, scroll_mode: ScrollMode) -> Self {
        Self {
            inner: Cursor::new(id),
            scroll_mode,
            current_position: None,
            total_rows: None,
        }
    }

    /// Get the cursor ID.
    pub fn id(&self) -> u32 {
        self.inner.id()
    }

    /// Get the scroll mode.
    pub fn scroll_mode(&self) -> ScrollMode {
        self.scroll_mode
    }

    /// Check if this is a scrollable cursor.
    pub fn is_scrollable(&self) -> bool {
        true
    }

    /// Get current absolute position (1-based).
    pub fn position(&self) -> Option<u64> {
        self.current_position
    }

    /// Get total row count (if known).
    pub fn total_rows(&self) -> Option<u64> {
        self.total_rows
    }

    /// Check if positioned before first row.
    pub fn is_before_first(&self) -> bool {
        self.current_position == Some(0)
    }

    /// Check if positioned after last row.
    pub fn is_after_last(&self) -> bool {
        match (self.current_position, self.total_rows) {
            (Some(pos), Some(total)) => pos > total,
            _ => false,
        }
    }

    /// Update position after a scroll fetch.
    pub fn set_position(&mut self, position: u64, total: u64) {
        self.current_position = Some(position);
        self.total_rows = Some(total);
    }

    /// Get the underlying cursor state.
    pub fn state(&self) -> CursorState {
        self.inner.state()
    }

    /// Get column metadata.
    pub fn metadata(&self) -> Option<&ResultSetMetadata> {
        self.inner.metadata()
    }

    /// Mark as closed.
    pub fn mark_closed(&mut self) {
        self.inner.mark_closed();
    }
}

/// Scrollable fetch request.
///
/// Use this instead of `FetchRequest` when working with scrollable cursors.
/// Supports bidirectional navigation and absolute/relative positioning.
#[derive(Clone, Debug)]
pub struct ScrollFetchRequest {
    /// Cursor ID.
    pub cursor_id: u32,
    /// Scroll direction.
    pub direction: ScrollDirection,
    /// Position offset (interpretation depends on direction).
    /// - Forward/Backward: number of rows to fetch
    /// - Absolute: target row number (1-based)
    /// - Relative: offset from current position (can be negative)
    /// - First/Last: ignored
    pub offset: i64,
    /// Number of rows to fetch.
    pub fetch_size: u32,
}

impl ScrollFetchRequest {
    /// Create a new scroll fetch request (forward by default).
    pub fn new(cursor_id: u32) -> Self {
        Self {
            cursor_id,
            direction: ScrollDirection::Forward,
            offset: 0,
            fetch_size: 100,
        }
    }

    /// Fetch forward from current position.
    pub fn forward(mut self, rows: u32) -> Self {
        self.direction = ScrollDirection::Forward;
        self.fetch_size = rows;
        self
    }

    /// Fetch backward from current position.
    pub fn backward(mut self, rows: u32) -> Self {
        self.direction = ScrollDirection::Backward;
        self.fetch_size = rows;
        self
    }

    /// Jump to absolute row position (1-based).
    pub fn absolute(mut self, row: u64, fetch_size: u32) -> Self {
        self.direction = ScrollDirection::Absolute;
        self.offset = row as i64;
        self.fetch_size = fetch_size;
        self
    }

    /// Move relative to current position.
    pub fn relative(mut self, offset: i64, fetch_size: u32) -> Self {
        self.direction = ScrollDirection::Relative;
        self.offset = offset;
        self.fetch_size = fetch_size;
        self
    }

    /// Jump to first row.
    pub fn first(mut self, fetch_size: u32) -> Self {
        self.direction = ScrollDirection::First;
        self.fetch_size = fetch_size;
        self
    }

    /// Jump to last row.
    pub fn last(mut self, fetch_size: u32) -> Self {
        self.direction = ScrollDirection::Last;
        self.fetch_size = fetch_size;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20);

        // Function code for scrollable fetch
        buf.push(FunctionCode::ScrollFetch.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Direction
        buf.push(self.direction.code());

        // Offset (8 bytes, signed)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Fetch size
        buf.extend_from_slice(&self.fetch_size.to_be_bytes());

        buf
    }
}

/// Scrollable fetch response.
///
/// Extended fetch response with position information.
#[derive(Clone, Debug)]
pub struct ScrollFetchResponse {
    /// Number of rows returned.
    pub row_count: u32,
    /// Current absolute position after fetch (1-based).
    pub current_position: u64,
    /// Total number of rows in result set.
    pub total_rows: u64,
    /// Whether there are more rows after current position.
    pub has_more_after: bool,
    /// Whether there are rows before current position.
    pub has_more_before: bool,
    /// Row data.
    pub row_data: Vec<u8>,
    /// Row offsets.
    pub row_offsets: Vec<usize>,
}

impl ScrollFetchResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            row_count: 0,
            current_position: 0,
            total_rows: 0,
            has_more_after: false,
            has_more_before: false,
            row_data: Vec::new(),
            row_offsets: Vec::new(),
        }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 22 {
            return Err(StatementError::TooShort { expected: 22, actual: data.len() });
        }

        let row_count = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);
        let current_position = u64::from_be_bytes([data[4], data[5], data[6], data[7], data[8], data[9], data[10], data[11]]);
        let total_rows = u64::from_be_bytes([data[12], data[13], data[14], data[15], data[16], data[17], data[18], data[19]]);
        let has_more_after = data[20] != 0;
        let has_more_before = data[21] != 0;

        let row_data = if data.len() > 22 { data[22..].to_vec() } else { Vec::new() };

        Ok(Self {
            row_count,
            current_position,
            total_rows,
            has_more_after,
            has_more_before,
            row_data,
            row_offsets: Vec::new(),
        })
    }

    /// Check if at beginning of result set.
    pub fn at_beginning(&self) -> bool {
        !self.has_more_before
    }

    /// Check if at end of result set.
    pub fn at_end(&self) -> bool {
        !self.has_more_after
    }
}

// ============================================================================
// Implicit Results Support (Oracle 12c+)
// ============================================================================
//
// Implicit results allow a PL/SQL block to return multiple result sets
// without explicitly declaring OUT REF CURSOR parameters.
//
// Example PL/SQL:
//   BEGIN
//     DBMS_SQL.RETURN_RESULT(DBMS_SQL.OPEN_CURSOR);
//     DBMS_SQL.RETURN_RESULT(DBMS_SQL.OPEN_CURSOR);
//   END;
//
// The server sends back multiple cursor IDs with their metadata.

/// Implicit result cursor info.
///
/// Describes one of multiple result sets returned by a PL/SQL block.
#[derive(Clone, Debug)]
pub struct ImplicitResultCursor {
    /// Cursor ID for fetching from this result set.
    pub cursor_id: u32,
    /// Column metadata for this result set.
    pub columns: Vec<ColumnDescriptor>,
}

/// Implicit results response (Oracle 12c+).
///
/// Contains multiple result set cursors returned from a PL/SQL block
/// using DBMS_SQL.RETURN_RESULT.
#[derive(Clone, Debug)]
pub struct ImplicitResultsResponse {
    /// List of implicit result cursors.
    pub cursors: Vec<ImplicitResultCursor>,
}

impl ImplicitResultsResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self { cursors: Vec::new() }
    }

    /// Check if there are any implicit results.
    pub fn has_results(&self) -> bool {
        !self.cursors.is_empty()
    }

    /// Get the number of implicit result sets.
    pub fn count(&self) -> usize {
        self.cursors.len()
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 2 {
            return Err(StatementError::TooShort { expected: 2, actual: data.len() });
        }

        let cursor_count = u16::from_be_bytes([data[0], data[1]]) as usize;
        let mut offset = 2;
        let mut cursors = Vec::with_capacity(cursor_count);

        for _ in 0..cursor_count {
            if offset + 6 > data.len() {
                break;
            }

            let cursor_id = u32::from_be_bytes([data[offset], data[offset + 1], data[offset + 2], data[offset + 3]]);
            offset += 4;

            let column_count = u16::from_be_bytes([data[offset], data[offset + 1]]) as usize;
            offset += 2;

            let mut columns = Vec::with_capacity(column_count);
            for _ in 0..column_count {
                if offset + 12 > data.len() {
                    break;
                }

                let col = ColumnDescriptor::parse(&data[offset..])?;
                offset += col.wire_size();
                columns.push(col);
            }

            cursors.push(ImplicitResultCursor { cursor_id, columns });
        }

        Ok(Self { cursors })
    }
}

// ============================================================================
// Statement Cache Support
// ============================================================================
//
// Statement caching allows reusing parsed statements to avoid repeated
// parsing overhead. The cache maps SQL text (or hash) to cursor IDs.

/// Statement cache entry.
///
/// Represents a cached prepared statement that can be reused.
#[derive(Clone, Debug)]
pub struct StatementCacheEntry {
    /// Cached cursor ID.
    pub cursor_id: u32,
    /// Hash of the SQL text.
    pub sql_hash: u64,
    /// Statement type.
    pub statement_type: StatementType,
    /// Number of bind variables.
    pub bind_count: u16,
    /// Number of output columns (for SELECT).
    pub column_count: u16,
    /// When this entry was created.
    pub parsed_at: std::time::Instant,
    /// When this entry was last used.
    pub last_used: std::time::Instant,
    /// Number of times this entry has been used.
    pub use_count: u64,
}

impl StatementCacheEntry {
    /// Create a new cache entry.
    pub fn new(cursor_id: u32, sql_hash: u64, statement_type: StatementType) -> Self {
        let now = std::time::Instant::now();
        Self {
            cursor_id,
            sql_hash,
            statement_type,
            bind_count: 0,
            column_count: 0,
            parsed_at: now,
            last_used: now,
            use_count: 1,
        }
    }

    /// Record a use of this cached statement.
    pub fn record_use(&mut self) {
        self.last_used = std::time::Instant::now();
        self.use_count += 1;
    }

    /// Check if this entry is stale (hasn't been used recently).
    pub fn is_stale(&self, max_idle: std::time::Duration) -> bool {
        self.last_used.elapsed() > max_idle
    }
}

/// Statement cache tag request.
///
/// Used to associate a tag with a parsed statement for later retrieval.
#[derive(Clone, Debug)]
pub struct SetStatementTagRequest {
    /// Cursor ID of the statement.
    pub cursor_id: u32,
    /// Tag to associate with the statement.
    pub tag: String,
}

impl SetStatementTagRequest {
    /// Create a new tag request.
    pub fn new(cursor_id: u32, tag: impl Into<String>) -> Self {
        Self { cursor_id, tag: tag.into() }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let tag_bytes = self.tag.as_bytes();
        let mut buf = Vec::with_capacity(8 + tag_bytes.len());

        // Function code
        buf.push(FunctionCode::SetStatementTag.as_u8());

        // Cursor ID
        buf.extend_from_slice(&self.cursor_id.to_be_bytes());

        // Tag length and data
        buf.extend_from_slice(&(tag_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(tag_bytes);

        buf
    }
}

/// Get cached statement by tag request.
#[derive(Clone, Debug)]
pub struct GetStatementByTagRequest {
    /// Tag to look up.
    pub tag: String,
}

impl GetStatementByTagRequest {
    /// Create a new request.
    pub fn new(tag: impl Into<String>) -> Self {
        Self { tag: tag.into() }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let tag_bytes = self.tag.as_bytes();
        let mut buf = Vec::with_capacity(4 + tag_bytes.len());

        // Function code
        buf.push(FunctionCode::GetStatementByTag.as_u8());

        // Tag length and data
        buf.extend_from_slice(&(tag_bytes.len() as u16).to_be_bytes());
        buf.extend_from_slice(tag_bytes);

        buf
    }
}

/// Get cached statement response.
#[derive(Clone, Debug)]
pub struct GetStatementByTagResponse {
    /// Whether the statement was found.
    pub found: bool,
    /// Cursor ID if found.
    pub cursor_id: Option<u32>,
    /// Statement type if found.
    pub statement_type: Option<StatementType>,
}

impl GetStatementByTagResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.is_empty() {
            return Err(StatementError::TooShort { expected: 1, actual: 0 });
        }

        let found = data[0] != 0;

        if !found {
            return Ok(Self { found: false, cursor_id: None, statement_type: None });
        }

        if data.len() < 6 {
            return Err(StatementError::TooShort { expected: 6, actual: data.len() });
        }

        let cursor_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let statement_type = StatementType::from_code(data[5]);

        Ok(Self {
            found: true,
            cursor_id: Some(cursor_id),
            statement_type: Some(statement_type),
        })
    }
}

// ============================================================================
// RETURNING Clause Support
// ============================================================================
//
// Oracle's RETURNING clause allows DML statements (INSERT, UPDATE, DELETE)
// to return values from the affected rows in a single round-trip.
//
// Example SQL:
//   INSERT INTO users (name) VALUES ('Alice') RETURNING id, created_at INTO :id, :ts
//   UPDATE users SET status = 'active' WHERE id = :1 RETURNING name INTO :name
//   DELETE FROM orders WHERE id = :1 RETURNING amount INTO :amt
//
// The RETURNING values are sent back as part of the execute response.

/// Descriptor for a RETURNING clause column.
///
/// Describes what data will be returned from a DML statement.
#[derive(Clone, Debug)]
pub struct ReturningColumnDescriptor {
    /// Column name or expression.
    pub name: String,
    /// Data type of the returned value.
    pub data_type: super::data_types::OracleDataType,
    /// Maximum size in bytes.
    pub max_size: u32,
    /// Precision (for NUMBER).
    pub precision: u8,
    /// Scale (for NUMBER).
    pub scale: i8,
}

impl ReturningColumnDescriptor {
    /// Create a new returning column descriptor.
    pub fn new(name: impl Into<String>, data_type: super::data_types::OracleDataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            max_size: 0,
            precision: 0,
            scale: 0,
        }
    }

    /// Set maximum size.
    pub fn with_max_size(mut self, max_size: u32) -> Self {
        self.max_size = max_size;
        self
    }

    /// Set precision and scale (for NUMBER).
    pub fn with_precision_scale(mut self, precision: u8, scale: i8) -> Self {
        self.precision = precision;
        self.scale = scale;
        self
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 10 {
            return Err(StatementError::TooShort { expected: 10, actual: data.len() });
        }

        let data_type = super::data_types::OracleDataType::from_u8(data[0]);
        let max_size = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
        let precision = data[5];
        let scale = data[6] as i8;
        let name_len = data[7] as usize;

        let name = if data.len() >= 8 + name_len {
            String::from_utf8_lossy(&data[8..8 + name_len]).to_string()
        } else {
            String::new()
        };

        Ok(Self { name, data_type, max_size, precision, scale })
    }

    /// Get wire size of this descriptor.
    pub fn wire_size(&self) -> usize {
        8 + self.name.len()
    }
}

/// A single returned value from a RETURNING clause.
#[derive(Clone, Debug)]
pub enum ReturningValue {
    /// NULL value.
    Null,
    /// String value.
    String(String),
    /// Binary data.
    Binary(Vec<u8>),
    /// Integer value.
    Int(i64),
    /// Floating-point value.
    Float(f64),
    /// Oracle NUMBER as raw bytes.
    Number(Vec<u8>),
    /// Date as raw bytes (7 bytes).
    Date(Vec<u8>),
    /// Timestamp as raw bytes.
    Timestamp(Vec<u8>),
    /// ROWID as string.
    Rowid(String),
    /// LOB locator.
    LobLocator(Vec<u8>),
}

impl ReturningValue {
    /// Check if this is NULL.
    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    /// Get as string (if applicable).
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(s) | Self::Rowid(s) => Some(s),
            _ => None,
        }
    }

    /// Get as bytes (if applicable).
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Binary(b) | Self::Number(b) | Self::Date(b) | Self::Timestamp(b) | Self::LobLocator(b) => Some(b),
            _ => None,
        }
    }

    /// Get as i64 (if applicable).
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::Int(n) => Some(*n),
            _ => None,
        }
    }

    /// Get as f64 (if applicable).
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Float(f) => Some(*f),
            Self::Int(n) => Some(*n as f64),
            _ => None,
        }
    }
}

/// A single row of RETURNING clause results.
#[derive(Clone, Debug)]
pub struct ReturningRow {
    /// Values for each RETURNING column.
    pub values: Vec<ReturningValue>,
}

impl ReturningRow {
    /// Create a new returning row.
    pub fn new(values: Vec<ReturningValue>) -> Self {
        Self { values }
    }

    /// Get a value by index.
    pub fn get(&self, index: usize) -> Option<&ReturningValue> {
        self.values.get(index)
    }

    /// Get number of columns.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Result of a RETURNING clause.
///
/// Contains all rows returned from a DML statement with RETURNING clause.
#[derive(Clone, Debug)]
pub struct ReturningClauseResult {
    /// Column descriptors for the returned data.
    pub columns: Vec<ReturningColumnDescriptor>,
    /// Returned rows (one per affected row).
    pub rows: Vec<ReturningRow>,
}

impl ReturningClauseResult {
    /// Create an empty result.
    pub fn empty() -> Self {
        Self { columns: Vec::new(), rows: Vec::new() }
    }

    /// Check if there are any returned values.
    pub fn has_data(&self) -> bool {
        !self.rows.is_empty()
    }

    /// Get number of returned rows.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a specific row.
    pub fn get_row(&self, index: usize) -> Option<&ReturningRow> {
        self.rows.get(index)
    }

    /// Get a specific value by row and column index.
    pub fn get_value(&self, row: usize, col: usize) -> Option<&ReturningValue> {
        self.rows.get(row)?.values.get(col)
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Iterate over rows.
    pub fn iter_rows(&self) -> impl Iterator<Item = &ReturningRow> {
        self.rows.iter()
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, StatementError> {
        if data.len() < 4 {
            return Err(StatementError::TooShort { expected: 4, actual: data.len() });
        }

        let column_count = u16::from_be_bytes([data[0], data[1]]) as usize;
        let row_count = u16::from_be_bytes([data[2], data[3]]) as usize;

        let mut offset = 4;
        let mut columns = Vec::with_capacity(column_count);

        // Parse column descriptors
        for _ in 0..column_count {
            if offset + 10 > data.len() {
                break;
            }
            let col = ReturningColumnDescriptor::parse(&data[offset..])?;
            offset += col.wire_size();
            columns.push(col);
        }

        // Parse row data
        let mut rows = Vec::with_capacity(row_count);
        for _ in 0..row_count {
            let mut values = Vec::with_capacity(column_count);
            for col in &columns {
                if offset >= data.len() {
                    break;
                }

                // Read null indicator
                let is_null = data[offset] == 0xFF;
                offset += 1;

                if is_null {
                    values.push(ReturningValue::Null);
                    continue;
                }

                // Read value length
                if offset >= data.len() {
                    break;
                }
                let len = data[offset] as usize;
                offset += 1;

                if offset + len > data.len() {
                    break;
                }

                let value_data = &data[offset..offset + len];
                offset += len;

                // Parse based on column type
                let value = match col.data_type {
                    super::data_types::OracleDataType::Varchar2
                    | super::data_types::OracleDataType::Char
                    | super::data_types::OracleDataType::Nchar => ReturningValue::String(String::from_utf8_lossy(value_data).to_string()),
                    super::data_types::OracleDataType::Number => ReturningValue::Number(value_data.to_vec()),
                    super::data_types::OracleDataType::Date => ReturningValue::Date(value_data.to_vec()),
                    super::data_types::OracleDataType::Timestamp | super::data_types::OracleDataType::TimestampTz => {
                        ReturningValue::Timestamp(value_data.to_vec())
                    }
                    super::data_types::OracleDataType::Rowid | super::data_types::OracleDataType::Urowid => {
                        ReturningValue::Rowid(String::from_utf8_lossy(value_data).to_string())
                    }
                    super::data_types::OracleDataType::Clob | super::data_types::OracleDataType::Blob => {
                        ReturningValue::LobLocator(value_data.to_vec())
                    }
                    super::data_types::OracleDataType::Raw | super::data_types::OracleDataType::LongRaw => {
                        ReturningValue::Binary(value_data.to_vec())
                    }
                    super::data_types::OracleDataType::BinaryDouble => {
                        if value_data.len() >= 8 {
                            let f = f64::from_be_bytes([
                                value_data[0],
                                value_data[1],
                                value_data[2],
                                value_data[3],
                                value_data[4],
                                value_data[5],
                                value_data[6],
                                value_data[7],
                            ]);
                            ReturningValue::Float(f)
                        } else {
                            ReturningValue::Binary(value_data.to_vec())
                        }
                    }
                    super::data_types::OracleDataType::BinaryFloat => {
                        if value_data.len() >= 4 {
                            let f = f32::from_be_bytes([value_data[0], value_data[1], value_data[2], value_data[3]]);
                            ReturningValue::Float(f as f64)
                        } else {
                            ReturningValue::Binary(value_data.to_vec())
                        }
                    }
                    _ => ReturningValue::Binary(value_data.to_vec()),
                };

                values.push(value);
            }
            rows.push(ReturningRow::new(values));
        }

        Ok(Self { columns, rows })
    }
}

/// Execute options with RETURNING clause support.
///
/// Extends ExecuteOptions to handle RETURNING INTO binds.
#[derive(Clone, Debug, Default)]
pub struct ExecuteWithReturningOptions {
    /// Base execute options.
    pub base: ExecuteOptions,
    /// Number of RETURNING columns.
    pub returning_column_count: u16,
    /// Maximum rows to return (for BULK operations).
    pub max_returning_rows: u32,
}

impl ExecuteWithReturningOptions {
    /// Create new options for RETURNING clause.
    pub fn new(returning_column_count: u16) -> Self {
        Self {
            base: ExecuteOptions::default(),
            returning_column_count,
            max_returning_rows: 1,
        }
    }

    /// Set base options.
    pub fn with_base(mut self, options: ExecuteOptions) -> Self {
        self.base = options;
        self
    }

    /// Set maximum returning rows (for bulk operations).
    pub fn with_max_rows(mut self, max_rows: u32) -> Self {
        self.max_returning_rows = max_rows;
        self
    }

    /// Convert to wire flags.
    pub fn as_flags(&self) -> u64 {
        let base_flags = self.base.as_flags() as u64;
        // Add RETURNING indicator in upper bits
        let returning_flag = if self.returning_column_count > 0 { 0x0100_0000_0000 } else { 0 };
        base_flags | returning_flag
    }
}

/// Execute response with RETURNING clause data.
#[derive(Clone, Debug)]
pub struct ExecuteWithReturningResponse {
    /// Base execute response.
    pub base: ExecuteResponse,
    /// RETURNING clause result (if any).
    pub returning: Option<ReturningClauseResult>,
}

impl ExecuteWithReturningResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self {
            base: ExecuteResponse {
                rows_affected: 0,
                has_more_rows: false,
                prefetched_rows: 0,
                last_rowid: None,
            },
            returning: None,
        }
    }

    /// Check if there are returning values.
    pub fn has_returning(&self) -> bool {
        self.returning.as_ref().is_some_and(|r| r.has_data())
    }

    /// Get rows affected.
    pub fn rows_affected(&self) -> u64 {
        self.base.rows_affected
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8], has_returning: bool) -> Result<Self, StatementError> {
        let base = ExecuteResponse::parse(data)?;

        let returning = if has_returning && data.len() > 12 {
            Some(ReturningClauseResult::parse(&data[12..])?)
        } else {
            None
        };

        Ok(Self { base, returning })
    }
}

/// Helper to build RETURNING clause bind descriptors.
#[derive(Clone, Debug)]
pub struct ReturningBindBuilder {
    /// Column descriptors for RETURNING values.
    columns: Vec<ReturningColumnDescriptor>,
}

impl ReturningBindBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self { columns: Vec::new() }
    }

    /// Add a VARCHAR2 returning column.
    pub fn varchar2(mut self, name: impl Into<String>, max_size: u32) -> Self {
        self.columns
            .push(ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Varchar2).with_max_size(max_size));
        self
    }

    /// Add a NUMBER returning column.
    pub fn number(mut self, name: impl Into<String>, precision: u8, scale: i8) -> Self {
        self.columns.push(
            ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Number)
                .with_max_size(22)
                .with_precision_scale(precision, scale),
        );
        self
    }

    /// Add a DATE returning column.
    pub fn date(mut self, name: impl Into<String>) -> Self {
        self.columns.push(ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Date).with_max_size(7));
        self
    }

    /// Add a TIMESTAMP returning column.
    pub fn timestamp(mut self, name: impl Into<String>) -> Self {
        self.columns.push(ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Timestamp).with_max_size(11));
        self
    }

    /// Add a ROWID returning column.
    pub fn rowid(mut self, name: impl Into<String>) -> Self {
        self.columns.push(ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Rowid).with_max_size(18));
        self
    }

    /// Add a RAW returning column.
    pub fn raw(mut self, name: impl Into<String>, max_size: u32) -> Self {
        self.columns.push(ReturningColumnDescriptor::new(name, super::data_types::OracleDataType::Raw).with_max_size(max_size));
        self
    }

    /// Get the number of returning columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Build the column descriptors.
    pub fn build(self) -> Vec<ReturningColumnDescriptor> {
        self.columns
    }

    /// Encode for wire transmission.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Column count (2 bytes)
        buf.extend_from_slice(&(self.columns.len() as u16).to_be_bytes());

        // Each column descriptor
        for col in &self.columns {
            buf.push(col.data_type.code());
            buf.extend_from_slice(&col.max_size.to_be_bytes());
            buf.push(col.precision);
            buf.push(col.scale as u8);
            let name_bytes = col.name.as_bytes();
            buf.push(name_bytes.len() as u8);
            buf.extend_from_slice(name_bytes);
        }

        buf
    }
}

impl Default for ReturningBindBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_statement_type() {
        assert!(StatementType::Select.returns_rows());
        assert!(!StatementType::Insert.returns_rows());
        assert!(StatementType::Insert.is_dml());
        assert!(StatementType::Create.is_ddl());
        assert!(StatementType::PlSql.is_plsql());
    }

    #[test]
    fn test_cursor_lifecycle() {
        let mut cursor = Cursor::new(1);
        assert_eq!(cursor.state(), CursorState::Parsed);
        assert!(cursor.is_usable());

        cursor.set_statement_type(StatementType::Select);
        cursor.mark_executed(None);
        assert_eq!(cursor.state(), CursorState::Executed);
        assert!(cursor.has_more_rows());

        cursor.mark_fetching(true);
        assert_eq!(cursor.state(), CursorState::Fetching);

        cursor.mark_exhausted();
        assert_eq!(cursor.state(), CursorState::Exhausted);
        assert!(!cursor.has_more_rows());

        cursor.mark_closed();
        assert_eq!(cursor.state(), CursorState::Closed);
        assert!(!cursor.is_usable());
    }

    #[test]
    fn test_parse_request() {
        let request = ParseRequest::new("SELECT * FROM dual").with_tag("test_stmt");

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::Parse.as_u8());
    }

    #[test]
    fn test_execute_options() {
        let options = ExecuteOptions::new().auto_commit().describe_before().with_prefetch(100);

        let flags = options.as_flags();
        assert_eq!(flags & 0x0001, 0x0001); // auto_commit
        assert_eq!(flags & 0x0002, 0x0002); // describe_before
        assert_eq!(flags >> 16, 100); // prefetch
    }

    #[test]
    fn test_execute_request() {
        let mut binds = BindSet::new();
        binds.add_i64(Some(":id"), 42);

        let request = ExecuteRequest::new(1).with_binds(binds).with_options(ExecuteOptions::new().auto_commit());

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::Execute.as_u8());
    }

    #[test]
    fn test_fetch_request() {
        let request = FetchRequest::new(1, 100);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::Fetch.as_u8());
    }

    #[test]
    fn test_close_request() {
        let request = CloseRequest::new(1);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::CloseCursor.as_u8());
    }

    #[test]
    fn test_commit_rollback() {
        let commit = CommitRequest.encode();
        assert_eq!(commit[0], FunctionCode::Commit.as_u8());

        let rollback = RollbackRequest.encode();
        assert_eq!(rollback[0], FunctionCode::Rollback.as_u8());
    }

    #[test]
    fn test_fetch_response() {
        // row_count=2, has_more=true, plus some row data
        let data = [
            0, 0, 0, 2, // row_count = 2
            1, // has_more = true
            1, 2, 3, 4, // row data
        ];

        let response = FetchResponse::parse(&data).unwrap();
        assert_eq!(response.row_count, 2);
        assert!(response.has_more);
        assert!(response.has_data());
        assert!(!response.is_complete());
        assert_eq!(response.row_data, vec![1, 2, 3, 4]);
    }

    #[test]
    fn test_fetch_response_empty() {
        let data = [0, 0, 0, 0, 0]; // row_count=0, has_more=false

        let response = FetchResponse::parse(&data).unwrap();
        assert_eq!(response.row_count, 0);
        assert!(!response.has_more);
        assert!(!response.has_data());
        assert!(response.is_complete());
    }

    #[test]
    fn test_describe_response() {
        // Simple response with 1 column, 0 binds
        let mut data = vec![
            0, 1, // column_count = 1
            0, 0, // bind_count = 0
        ];

        // Column descriptor: type=2 (NUMBER), size=22, prec=10, scale=2, nullable=1, charset=1, name="ID"
        data.extend_from_slice(&[
            2, // data_type = NUMBER
            0, 0, 0, 22, // max_size = 22
            10, // precision = 10
            2,  // scale = 2
            1,  // nullable = true
            1,  // charset_form = 1
            2,  // name_len = 2
            b'I', b'D', // name = "ID"
        ]);

        let response = DescribeResponse::parse(&data).unwrap();
        assert_eq!(response.column_count, 1);
        assert_eq!(response.columns.len(), 1);
        assert_eq!(response.columns[0].name, "ID");
        assert_eq!(response.columns[0].precision, 10);
        assert_eq!(response.columns[0].scale, 2);
    }

    #[test]
    fn test_column_descriptor() {
        let data = [
            1, // data_type = VARCHAR2
            0, 0, 0, 100, // max_size = 100
            0,   // precision = 0
            0,   // scale = 0
            1,   // nullable = true
            1,   // charset_form = 1
            4,   // name_len = 4
            b'N', b'A', b'M', b'E', // name = "NAME"
        ];

        let col = ColumnDescriptor::parse(&data).unwrap();
        assert_eq!(col.name, "NAME");
        assert_eq!(col.max_size, 100);
        assert!(col.nullable);
        assert_eq!(col.wire_size(), 14); // 10 + 4
    }

    #[test]
    fn test_execute_and_fetch_request() {
        let request = ExecuteAndFetchRequest::new(1, 100).with_options(ExecuteOptions::new().auto_commit());

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::ExecuteAndFetch.as_u8());
    }

    #[test]
    fn test_execute_and_fetch_response() {
        let data = [
            0, 0, 0, 0, 0, 0, 0, 5, // rows_affected = 5
            0, 0, 0, 3, // row_count = 3
            1, // has_more_rows = true
            65, 66, 67, // row data
        ];

        let response = ExecuteAndFetchResponse::parse(&data).unwrap();
        assert_eq!(response.rows_affected, 5);
        assert_eq!(response.row_count, 3);
        assert!(response.has_more_rows);
        assert!(response.has_data());
        assert!(!response.is_complete());
        assert_eq!(response.row_data, vec![65, 66, 67]);
    }

    #[test]
    fn test_execute_and_fetch_response_complete() {
        let data = [
            0, 0, 0, 0, 0, 0, 0, 0, // rows_affected = 0
            0, 0, 0, 2, // row_count = 2
            0, // has_more_rows = false
        ];

        let response = ExecuteAndFetchResponse::parse(&data).unwrap();
        assert_eq!(response.row_count, 2);
        assert!(!response.has_more_rows);
        assert!(response.is_complete());
    }

    #[test]
    fn test_batch_execute_options() {
        let options = BatchExecuteOptions::new().auto_commit().continue_on_error().return_row_counts();

        let flags = options.as_flags();
        assert_eq!(flags & 0x0001, 0x0001); // auto_commit
        assert_eq!(flags & 0x0002, 0x0002); // continue_on_error
        assert_eq!(flags & 0x0004, 0x0004); // return_row_counts
        assert_eq!(flags & 0x0008, 0x0000); // return_generated_keys not set
    }

    #[test]
    fn test_batch_execute_request() {
        let mut binds = BindSet::new();
        binds.add_i64(Some(":id"), 1);

        let request = BatchExecuteRequest::new(1, 10, binds).with_options(BatchExecuteOptions::new().continue_on_error());

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::BatchExecute.as_u8());
    }

    #[test]
    fn test_batch_execute_response() {
        let data = [
            0, 0, 0, 0, 0, 0, 0, 100, // total_rows_affected = 100
            0, 0, 0, 10, // success_count = 10
            0, 0, 0, 0, // error_count = 0
        ];

        let response = BatchExecuteResponse::parse(&data).unwrap();
        assert_eq!(response.total_rows_affected, 100);
        assert_eq!(response.success_count, 10);
        assert_eq!(response.error_count, 0);
        assert!(response.is_success());
        assert!(!response.has_errors());
    }

    #[test]
    fn test_batch_execute_response_with_errors() {
        let data = [
            0, 0, 0, 0, 0, 0, 0, 50, // total_rows_affected = 50
            0, 0, 0, 8, // success_count = 8
            0, 0, 0, 2, // error_count = 2
        ];

        let response = BatchExecuteResponse::parse(&data).unwrap();
        assert_eq!(response.total_rows_affected, 50);
        assert_eq!(response.success_count, 8);
        assert_eq!(response.error_count, 2);
        assert!(!response.is_success());
        assert!(response.has_errors());
    }

    #[test]
    fn test_open_cursor_request() {
        let request = OpenCursorRequest::new();
        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::OpenCursor.as_u8());
        assert_eq!(encoded[1], 0);
        assert_eq!(encoded[2], 1); // count = 1
    }

    #[test]
    fn test_open_cursor_request_multiple() {
        let request = OpenCursorRequest::with_count(5);
        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::OpenCursor.as_u8());
        assert_eq!(u16::from_be_bytes([encoded[1], encoded[2]]), 5);
    }

    #[test]
    fn test_open_cursor_response() {
        let data = [
            0, 2, // count = 2
            0, 0, 0, 10, // cursor_id = 10
            0, 0, 0, 11, // cursor_id = 11
        ];

        let response = OpenCursorResponse::parse(&data).unwrap();
        assert_eq!(response.cursor_ids.len(), 2);
        assert_eq!(response.cursor_ids[0], 10);
        assert_eq!(response.cursor_ids[1], 11);
        assert_eq!(response.cursor_id(), Some(10));
    }

    #[test]
    fn test_open_cursor_response_single() {
        let data = [
            0, 1, // count = 1
            0, 0, 0, 42, // cursor_id = 42
        ];

        let response = OpenCursorResponse::parse(&data).unwrap();
        assert_eq!(response.cursor_ids.len(), 1);
        assert_eq!(response.cursor_id(), Some(42));
    }

    #[test]
    fn test_scroll_direction() {
        assert_eq!(ScrollDirection::Forward.code(), 0x00);
        assert_eq!(ScrollDirection::Backward.code(), 0x01);
        assert_eq!(ScrollDirection::Absolute.code(), 0x02);
        assert_eq!(ScrollDirection::Relative.code(), 0x03);
        assert_eq!(ScrollDirection::First.code(), 0x04);
        assert_eq!(ScrollDirection::Last.code(), 0x05);
    }

    #[test]
    fn test_scrollable_fetch_request() {
        let request = ScrollFetchRequest::new(1).forward(50);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::ScrollFetch.as_u8());
    }

    #[test]
    fn test_scrollable_cursor() {
        let mut cursor = ScrollableCursor::new(1, ScrollMode::Insensitive);
        assert_eq!(cursor.scroll_mode(), ScrollMode::Insensitive);
        assert!(cursor.is_scrollable());
        assert_eq!(cursor.position(), None);

        cursor.set_position(10, 100);
        assert_eq!(cursor.position(), Some(10));
        assert_eq!(cursor.total_rows(), Some(100));
    }

    #[test]
    fn test_implicit_results_response() {
        // Build a mock response with 2 implicit cursors
        let mut data = vec![
            0, 2, // implicit_cursor_count = 2
        ];
        // Cursor 1
        data.extend_from_slice(&[
            0, 0, 0, 100, // cursor_id = 100
            0, 1, // column_count = 1
        ]);
        // Column descriptor for cursor 1
        data.extend_from_slice(&[1, 0, 0, 0, 50, 0, 0, 1, 1, 2, b'I', b'D']);
        // Cursor 2
        data.extend_from_slice(&[
            0, 0, 0, 101, // cursor_id = 101
            0, 1, // column_count = 1
        ]);
        // Column descriptor for cursor 2
        data.extend_from_slice(&[1, 0, 0, 0, 100, 0, 0, 1, 1, 4, b'N', b'A', b'M', b'E']);

        let response = ImplicitResultsResponse::parse(&data).unwrap();
        assert_eq!(response.cursors.len(), 2);
        assert_eq!(response.cursors[0].cursor_id, 100);
        assert_eq!(response.cursors[1].cursor_id, 101);
    }

    #[test]
    fn test_statement_cache_entry() {
        let entry = StatementCacheEntry {
            cursor_id: 42,
            sql_hash: 0x12345678,
            statement_type: StatementType::Select,
            bind_count: 2,
            column_count: 5,
            parsed_at: std::time::Instant::now(),
            last_used: std::time::Instant::now(),
            use_count: 1,
        };

        assert!(!entry.is_stale(std::time::Duration::from_secs(60)));
    }

    #[test]
    fn test_returning_column_descriptor() {
        let col = ReturningColumnDescriptor::new("id", super::super::data_types::OracleDataType::Number)
            .with_max_size(22)
            .with_precision_scale(10, 0);

        assert_eq!(col.name, "id");
        assert_eq!(col.max_size, 22);
        assert_eq!(col.precision, 10);
        assert_eq!(col.scale, 0);
    }

    #[test]
    fn test_returning_value_types() {
        let null_val = ReturningValue::Null;
        assert!(null_val.is_null());

        let str_val = ReturningValue::String("test".to_string());
        assert!(!str_val.is_null());
        assert_eq!(str_val.as_str(), Some("test"));

        let int_val = ReturningValue::Int(42);
        assert_eq!(int_val.as_i64(), Some(42));
        assert_eq!(int_val.as_f64(), Some(42.0));

        let float_val = ReturningValue::Float(3.25);
        assert_eq!(float_val.as_f64(), Some(3.25));
    }

    #[test]
    fn test_returning_row() {
        let row = ReturningRow::new(vec![
            ReturningValue::Int(1),
            ReturningValue::String("Alice".to_string()),
            ReturningValue::Null,
        ]);

        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        assert_eq!(row.get(0).and_then(|v| v.as_i64()), Some(1));
        assert_eq!(row.get(1).and_then(|v| v.as_str()), Some("Alice"));
        assert!(row.get(2).is_some_and(|v| v.is_null()));
    }

    #[test]
    fn test_returning_clause_result_empty() {
        let result = ReturningClauseResult::empty();
        assert!(!result.has_data());
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.column_count(), 0);
    }

    #[test]
    fn test_returning_clause_result() {
        let result = ReturningClauseResult {
            columns: vec![
                ReturningColumnDescriptor::new("id", super::super::data_types::OracleDataType::Number),
                ReturningColumnDescriptor::new("name", super::super::data_types::OracleDataType::Varchar2),
            ],
            rows: vec![
                ReturningRow::new(vec![ReturningValue::Int(1), ReturningValue::String("Alice".to_string())]),
                ReturningRow::new(vec![ReturningValue::Int(2), ReturningValue::String("Bob".to_string())]),
            ],
        };

        assert!(result.has_data());
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 2);
        assert_eq!(result.column_names(), vec!["id", "name"]);

        assert_eq!(result.get_value(0, 0).and_then(|v| v.as_i64()), Some(1));
        assert_eq!(result.get_value(1, 1).and_then(|v| v.as_str()), Some("Bob"));
    }

    #[test]
    fn test_execute_with_returning_options() {
        let options = ExecuteWithReturningOptions::new(2).with_base(ExecuteOptions::new().auto_commit()).with_max_rows(10);

        assert_eq!(options.returning_column_count, 2);
        assert_eq!(options.max_returning_rows, 10);
        assert!(options.base.auto_commit);
    }

    #[test]
    fn test_returning_bind_builder() {
        let builder = ReturningBindBuilder::new().number("id", 10, 0).varchar2("name", 100).date("created_at").rowid("row_id");

        assert_eq!(builder.column_count(), 4);

        let columns = builder.build();
        assert_eq!(columns[0].name, "id");
        assert_eq!(columns[1].name, "name");
        assert_eq!(columns[2].name, "created_at");
        assert_eq!(columns[3].name, "row_id");
    }

    #[test]
    fn test_returning_bind_builder_encode() {
        let builder = ReturningBindBuilder::new().number("id", 10, 0);

        let encoded = builder.encode();
        assert!(!encoded.is_empty());
        // First 2 bytes: column count
        assert_eq!(u16::from_be_bytes([encoded[0], encoded[1]]), 1);
    }

    #[test]
    fn test_execute_with_returning_response_empty() {
        let response = ExecuteWithReturningResponse::empty();
        assert!(!response.has_returning());
        assert_eq!(response.rows_affected(), 0);
    }
}
