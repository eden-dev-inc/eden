//! PostgreSQL command vocabulary for lightweight protocol classification.

use super::bucket::StatementBucket;

/// Protocol-level command categories.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandCategory {
    Read,
    Write,
    Admin,
    PubSub,
    Transaction,
    Other,
}

/// Coarse command risk, kept intentionally small for protocol users.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CommandRiskLevel {
    Safe,
    WarnLargeData,
    Dangerous,
    Blocking,
}

/// Fine-enough PostgreSQL command ID for routing and mux safety.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PgCommand {
    Select,
    Insert,
    Update,
    Delete,
    Merge,
    Copy,
    Table,
    Values,
    Create,
    Alter,
    Drop,
    Truncate,
    Comment,
    SecurityLabel,
    Begin,
    Commit,
    Rollback,
    Savepoint,
    ReleaseSavepoint,
    PrepareTransaction,
    CommitPrepared,
    RollbackPrepared,
    Set,
    Reset,
    Show,
    Discard,
    Grant,
    Revoke,
    Explain,
    ExplainAnalyze,
    Analyze,
    Vacuum,
    Reindex,
    Cluster,
    RefreshMaterializedView,
    Prepare,
    Execute,
    Deallocate,
    Declare,
    Fetch,
    Move,
    Close,
    Listen,
    Unlisten,
    Notify,
    Do,
    Call,
    Lock,
    Load,
    ImportForeignSchema,
    ReassignOwned,
    Start,
    With,
    Checkpoint,
    Other,
}

impl PgCommand {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
            Self::Merge => "MERGE",
            Self::Copy => "COPY",
            Self::Table => "TABLE",
            Self::Values => "VALUES",
            Self::Create => "CREATE",
            Self::Alter => "ALTER",
            Self::Drop => "DROP",
            Self::Truncate => "TRUNCATE",
            Self::Comment => "COMMENT",
            Self::SecurityLabel => "SECURITY_LABEL",
            Self::Begin => "BEGIN",
            Self::Commit => "COMMIT",
            Self::Rollback => "ROLLBACK",
            Self::Savepoint => "SAVEPOINT",
            Self::ReleaseSavepoint => "RELEASE_SAVEPOINT",
            Self::PrepareTransaction => "PREPARE_TRANSACTION",
            Self::CommitPrepared => "COMMIT_PREPARED",
            Self::RollbackPrepared => "ROLLBACK_PREPARED",
            Self::Set => "SET",
            Self::Reset => "RESET",
            Self::Show => "SHOW",
            Self::Discard => "DISCARD",
            Self::Grant => "GRANT",
            Self::Revoke => "REVOKE",
            Self::Explain => "EXPLAIN",
            Self::ExplainAnalyze => "EXPLAIN_ANALYZE",
            Self::Analyze => "ANALYZE",
            Self::Vacuum => "VACUUM",
            Self::Reindex => "REINDEX",
            Self::Cluster => "CLUSTER",
            Self::RefreshMaterializedView => "REFRESH_MATERIALIZED_VIEW",
            Self::Prepare => "PREPARE",
            Self::Execute => "EXECUTE",
            Self::Deallocate => "DEALLOCATE",
            Self::Declare => "DECLARE",
            Self::Fetch => "FETCH",
            Self::Move => "MOVE",
            Self::Close => "CLOSE",
            Self::Listen => "LISTEN",
            Self::Unlisten => "UNLISTEN",
            Self::Notify => "NOTIFY",
            Self::Do => "DO",
            Self::Call => "CALL",
            Self::Lock => "LOCK",
            Self::Load => "LOAD",
            Self::ImportForeignSchema => "IMPORT_FOREIGN_SCHEMA",
            Self::ReassignOwned => "REASSIGN_OWNED",
            Self::Start => "START",
            Self::With => "WITH",
            Self::Checkpoint => "CHECKPOINT",
            Self::Other => "OTHER",
        }
    }

    pub const fn category(self) -> CommandCategory {
        match self {
            Self::Select | Self::Table | Self::Values | Self::Show | Self::Explain | Self::Fetch | Self::Move => CommandCategory::Read,
            Self::Insert | Self::Update | Self::Delete | Self::Merge | Self::Copy | Self::ExplainAnalyze => CommandCategory::Write,
            Self::Create
            | Self::Alter
            | Self::Drop
            | Self::Truncate
            | Self::Comment
            | Self::SecurityLabel
            | Self::Analyze
            | Self::Vacuum
            | Self::Reindex
            | Self::Cluster
            | Self::RefreshMaterializedView
            | Self::Prepare
            | Self::Deallocate
            | Self::Declare
            | Self::Close
            | Self::Do
            | Self::Call
            | Self::Lock
            | Self::Load
            | Self::ImportForeignSchema
            | Self::ReassignOwned
            | Self::Checkpoint => CommandCategory::Admin,
            Self::Begin
            | Self::Commit
            | Self::Rollback
            | Self::Savepoint
            | Self::ReleaseSavepoint
            | Self::PrepareTransaction
            | Self::CommitPrepared
            | Self::RollbackPrepared
            | Self::Start => CommandCategory::Transaction,
            Self::Listen | Self::Unlisten | Self::Notify => CommandCategory::PubSub,
            Self::Set | Self::Reset | Self::Discard | Self::Execute | Self::With | Self::Other => CommandCategory::Other,
            Self::Grant | Self::Revoke => CommandCategory::Admin,
        }
    }

    pub const fn risk(self) -> CommandRiskLevel {
        match self {
            Self::Drop | Self::Truncate | Self::Discard | Self::ReassignOwned => CommandRiskLevel::Dangerous,
            Self::Vacuum | Self::Reindex | Self::Cluster | Self::Lock => CommandRiskLevel::Blocking,
            Self::Create | Self::Alter | Self::Analyze | Self::RefreshMaterializedView => CommandRiskLevel::WarnLargeData,
            _ => CommandRiskLevel::Safe,
        }
    }

    pub const fn bucket(self) -> StatementBucket {
        match self.category() {
            CommandCategory::Read => StatementBucket::Read,
            CommandCategory::Write => StatementBucket::Write,
            CommandCategory::Transaction => StatementBucket::TCL,
            CommandCategory::Admin => match self {
                Self::Grant | Self::Revoke => StatementBucket::DCL,
                _ => StatementBucket::DDL,
            },
            CommandCategory::PubSub | CommandCategory::Other => StatementBucket::Other,
        }
    }

    pub const fn base_is_write(self) -> bool {
        match self {
            Self::Select | Self::Table | Self::Values | Self::Show | Self::Explain | Self::Fetch | Self::Move => false,
            Self::Other => true,
            _ => !matches!(self.category(), CommandCategory::Read),
        }
    }

    pub const fn is_session_state(self) -> bool {
        matches!(
            self,
            Self::Set
                | Self::Reset
                | Self::Discard
                | Self::Prepare
                | Self::Execute
                | Self::Deallocate
                | Self::Declare
                | Self::Close
                | Self::Listen
                | Self::Unlisten
                | Self::Notify
                | Self::Load
        )
    }

    pub const fn is_transaction_control(self) -> bool {
        matches!(
            self,
            Self::Begin
                | Self::Commit
                | Self::Rollback
                | Self::Savepoint
                | Self::ReleaseSavepoint
                | Self::PrepareTransaction
                | Self::CommitPrepared
                | Self::RollbackPrepared
                | Self::Start
        )
    }
}

/// Parse the leading PostgreSQL keyword(s) into a command.
pub fn pg_command_from_sql(sql: &str) -> PgCommand {
    let upper = super::classify::skip_sql_comments(sql.trim()).to_ascii_uppercase();
    let mut words = upper.split_whitespace();
    let Some(first) = words.next() else {
        return PgCommand::Other;
    };

    match first {
        "SELECT" => PgCommand::Select,
        "INSERT" => PgCommand::Insert,
        "UPDATE" => PgCommand::Update,
        "DELETE" => PgCommand::Delete,
        "MERGE" => PgCommand::Merge,
        "COPY" => PgCommand::Copy,
        "TABLE" => PgCommand::Table,
        "VALUES" => PgCommand::Values,
        "CREATE" => PgCommand::Create,
        "ALTER" => PgCommand::Alter,
        "DROP" => PgCommand::Drop,
        "TRUNCATE" => PgCommand::Truncate,
        "COMMENT" => PgCommand::Comment,
        "SECURITY" => PgCommand::SecurityLabel,
        "BEGIN" => PgCommand::Begin,
        "COMMIT" => match words.next() {
            Some("PREPARED") => PgCommand::CommitPrepared,
            _ => PgCommand::Commit,
        },
        "END" => PgCommand::Commit,
        "ROLLBACK" | "ABORT" => match words.next() {
            Some("PREPARED") => PgCommand::RollbackPrepared,
            _ => PgCommand::Rollback,
        },
        "SAVEPOINT" => PgCommand::Savepoint,
        "RELEASE" => PgCommand::ReleaseSavepoint,
        "PREPARE" => match words.next() {
            Some("TRANSACTION") => PgCommand::PrepareTransaction,
            _ => PgCommand::Prepare,
        },
        "SET" => PgCommand::Set,
        "RESET" => PgCommand::Reset,
        "SHOW" => PgCommand::Show,
        "DISCARD" => PgCommand::Discard,
        "GRANT" => PgCommand::Grant,
        "REVOKE" => PgCommand::Revoke,
        "EXPLAIN" => PgCommand::Explain,
        "ANALYZE" | "ANALYSE" => PgCommand::Analyze,
        "VACUUM" => PgCommand::Vacuum,
        "REINDEX" => PgCommand::Reindex,
        "CLUSTER" => PgCommand::Cluster,
        "REFRESH" => PgCommand::RefreshMaterializedView,
        "EXECUTE" => PgCommand::Execute,
        "DEALLOCATE" => PgCommand::Deallocate,
        "DECLARE" => PgCommand::Declare,
        "FETCH" => PgCommand::Fetch,
        "MOVE" => PgCommand::Move,
        "CLOSE" => PgCommand::Close,
        "LISTEN" => PgCommand::Listen,
        "UNLISTEN" => PgCommand::Unlisten,
        "NOTIFY" => PgCommand::Notify,
        "DO" => PgCommand::Do,
        "CALL" => PgCommand::Call,
        "LOCK" => PgCommand::Lock,
        "LOAD" => PgCommand::Load,
        "IMPORT" => PgCommand::ImportForeignSchema,
        "REASSIGN" => PgCommand::ReassignOwned,
        "START" => PgCommand::Start,
        "WITH" => PgCommand::With,
        "CHECKPOINT" => PgCommand::Checkpoint,
        _ => PgCommand::Other,
    }
}
