//! MySQL wire protocol error types.

use std::num::ParseIntError;
use std::str::Utf8Error;

/// Error when an unexpected MySQL packet type is encountered.
#[derive(Copy, Clone, Eq, PartialEq, Debug, thiserror::Error)]
#[error("encountered incorrect packet type {encountered:#04X}; expected {expected:#04X}")]
pub struct IncorrectPacketType {
    /// The packet type byte that was actually found.
    pub encountered: u8,
    /// The packet type byte that was expected.
    pub expected: u8,
}

impl IncorrectPacketType {
    /// Returns a human-readable name for the encountered packet type.
    pub fn encountered_name(&self) -> &'static str {
        packet_type_name(self.encountered)
    }

    /// Returns a human-readable name for the expected packet type.
    pub fn expected_name(&self) -> &'static str {
        packet_type_name(self.expected)
    }
}

/// Error when parsing a MySQL length or numeric value.
#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum InvalidLength {
    #[error("length is not an integer")]
    NonNumeric,

    #[error("length is too large")]
    TooLarge,

    #[error("reserved value encountered (0xFF)")]
    Reserved,

    #[error("length is invalid UTF-8: {0}")]
    InvalidUtf8(#[from] Utf8Error),

    #[error("length is invalid: {0}")]
    ParseIntError(#[from] ParseIntError),
}

/// General MySQL wire protocol error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum MysqlWireError {
    #[error("packet too short: expected at least {expected} bytes, got {actual}")]
    PacketTooShort { expected: usize, actual: usize },

    #[error("invalid packet header")]
    InvalidPacketHeader,

    #[error("packet too large: {size} exceeds maximum {max}")]
    PacketTooLarge { size: usize, max: usize },

    #[error("unexpected packet sequence: expected {expected}, got {actual}")]
    SequenceMismatch { expected: u8, actual: u8 },

    #[error("unsupported protocol version: {0}")]
    UnsupportedVersion(u8),

    #[error("missing capability: {0}")]
    MissingCapability(&'static str),

    #[error("invalid string encoding")]
    InvalidStringEncoding,

    #[error("server error {code}: {message}")]
    ServerError { code: u16, state: String, message: String },

    #[error(transparent)]
    IncorrectPacketType(#[from] IncorrectPacketType),

    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
}

impl MysqlWireError {
    pub fn packet_too_short(expected: usize, actual: usize) -> Self {
        Self::PacketTooShort { expected, actual }
    }

    pub fn packet_too_large(size: usize, max: usize) -> Self {
        Self::PacketTooLarge { size, max }
    }

    pub fn sequence_mismatch(expected: u8, actual: u8) -> Self {
        Self::SequenceMismatch { expected, actual }
    }

    pub fn server_error(code: u16, state: impl Into<String>, message: impl Into<String>) -> Self {
        Self::ServerError { code, state: state.into(), message: message.into() }
    }
}

/// MySQL packet type header bytes.
pub mod packet_types {
    /// OK packet header (also 0xFE for EOF in older protocols with DEPRECATE_EOF).
    pub const OK: u8 = 0x00;
    /// ERR packet header.
    pub const ERR: u8 = 0xFF;
    /// EOF packet header (deprecated in 4.1+, but still used without DEPRECATE_EOF).
    pub const EOF: u8 = 0xFE;
    /// Local infile request.
    pub const LOCAL_INFILE: u8 = 0xFB;
}

/// MySQL command types (COM_*).
pub mod commands {
    /// Internal server command.
    pub const COM_SLEEP: u8 = 0x00;
    /// Close the connection.
    pub const COM_QUIT: u8 = 0x01;
    /// Change the default schema.
    pub const COM_INIT_DB: u8 = 0x02;
    /// Execute a text query.
    pub const COM_QUERY: u8 = 0x03;
    /// Get column definitions of a table.
    pub const COM_FIELD_LIST: u8 = 0x04;
    /// Create a schema.
    pub const COM_CREATE_DB: u8 = 0x05;
    /// Drop a schema.
    pub const COM_DROP_DB: u8 = 0x06;
    /// Flush tables, logs, etc.
    pub const COM_REFRESH: u8 = 0x07;
    /// Shutdown the server (deprecated).
    pub const COM_SHUTDOWN: u8 = 0x08;
    /// Get server statistics.
    pub const COM_STATISTICS: u8 = 0x09;
    /// Get process list.
    pub const COM_PROCESS_INFO: u8 = 0x0A;
    /// Internal server command.
    pub const COM_CONNECT: u8 = 0x0B;
    /// Kill a connection.
    pub const COM_PROCESS_KILL: u8 = 0x0C;
    /// Dump debug info.
    pub const COM_DEBUG: u8 = 0x0D;
    /// Ping the server.
    pub const COM_PING: u8 = 0x0E;
    /// Internal server command.
    pub const COM_TIME: u8 = 0x0F;
    /// Internal server command.
    pub const COM_DELAYED_INSERT: u8 = 0x10;
    /// Change user during connection.
    pub const COM_CHANGE_USER: u8 = 0x11;
    /// Start binlog dump.
    pub const COM_BINLOG_DUMP: u8 = 0x12;
    /// Dump a table.
    pub const COM_TABLE_DUMP: u8 = 0x13;
    /// Internal server command.
    pub const COM_CONNECT_OUT: u8 = 0x14;
    /// Register a slave.
    pub const COM_REGISTER_SLAVE: u8 = 0x15;
    /// Prepare a statement.
    pub const COM_STMT_PREPARE: u8 = 0x16;
    /// Execute a prepared statement.
    pub const COM_STMT_EXECUTE: u8 = 0x17;
    /// Send long data for a prepared statement.
    pub const COM_STMT_SEND_LONG_DATA: u8 = 0x18;
    /// Close a prepared statement.
    pub const COM_STMT_CLOSE: u8 = 0x19;
    /// Reset a prepared statement.
    pub const COM_STMT_RESET: u8 = 0x1A;
    /// Set options.
    pub const COM_SET_OPTION: u8 = 0x1B;
    /// Fetch rows from a prepared statement.
    pub const COM_STMT_FETCH: u8 = 0x1C;
    /// Internal server command.
    pub const COM_DAEMON: u8 = 0x1D;
    /// Start binlog dump with GTID.
    pub const COM_BINLOG_DUMP_GTID: u8 = 0x1E;
    /// Reset connection state.
    pub const COM_RESET_CONNECTION: u8 = 0x1F;
    /// Clone connection (MySQL 8.0.17+).
    pub const COM_CLONE: u8 = 0x20;
}

/// MySQL column types (field types).
pub mod column_types {
    pub const MYSQL_TYPE_DECIMAL: u8 = 0x00;
    pub const MYSQL_TYPE_TINY: u8 = 0x01;
    pub const MYSQL_TYPE_SHORT: u8 = 0x02;
    pub const MYSQL_TYPE_LONG: u8 = 0x03;
    pub const MYSQL_TYPE_FLOAT: u8 = 0x04;
    pub const MYSQL_TYPE_DOUBLE: u8 = 0x05;
    pub const MYSQL_TYPE_NULL: u8 = 0x06;
    pub const MYSQL_TYPE_TIMESTAMP: u8 = 0x07;
    pub const MYSQL_TYPE_LONGLONG: u8 = 0x08;
    pub const MYSQL_TYPE_INT24: u8 = 0x09;
    pub const MYSQL_TYPE_DATE: u8 = 0x0A;
    pub const MYSQL_TYPE_TIME: u8 = 0x0B;
    pub const MYSQL_TYPE_DATETIME: u8 = 0x0C;
    pub const MYSQL_TYPE_YEAR: u8 = 0x0D;
    pub const MYSQL_TYPE_NEWDATE: u8 = 0x0E;
    pub const MYSQL_TYPE_VARCHAR: u8 = 0x0F;
    pub const MYSQL_TYPE_BIT: u8 = 0x10;
    pub const MYSQL_TYPE_TIMESTAMP2: u8 = 0x11;
    pub const MYSQL_TYPE_DATETIME2: u8 = 0x12;
    pub const MYSQL_TYPE_TIME2: u8 = 0x13;
    pub const MYSQL_TYPE_TYPED_ARRAY: u8 = 0x14;
    pub const MYSQL_TYPE_JSON: u8 = 0xF5;
    pub const MYSQL_TYPE_NEWDECIMAL: u8 = 0xF6;
    pub const MYSQL_TYPE_ENUM: u8 = 0xF7;
    pub const MYSQL_TYPE_SET: u8 = 0xF8;
    pub const MYSQL_TYPE_TINY_BLOB: u8 = 0xF9;
    pub const MYSQL_TYPE_MEDIUM_BLOB: u8 = 0xFA;
    pub const MYSQL_TYPE_LONG_BLOB: u8 = 0xFB;
    pub const MYSQL_TYPE_BLOB: u8 = 0xFC;
    pub const MYSQL_TYPE_VAR_STRING: u8 = 0xFD;
    pub const MYSQL_TYPE_STRING: u8 = 0xFE;
    pub const MYSQL_TYPE_GEOMETRY: u8 = 0xFF;
}

/// Common MySQL error codes.
///
/// These are the numeric error codes returned in ERR packets.
/// See: https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
pub mod error_codes {
    // Connection errors (1000-1099)
    /// Unknown error
    pub const ER_UNKNOWN_ERROR: u16 = 1000;
    /// Can't create file
    pub const ER_CANT_CREATE_FILE: u16 = 1004;
    /// Can't create table
    pub const ER_CANT_CREATE_TABLE: u16 = 1005;
    /// Can't create database
    pub const ER_CANT_CREATE_DB: u16 = 1006;
    /// Database exists
    pub const ER_DB_CREATE_EXISTS: u16 = 1007;
    /// Can't drop database
    pub const ER_DB_DROP_EXISTS: u16 = 1008;
    /// Can't delete database directory
    pub const ER_DB_DROP_DELETE: u16 = 1009;
    /// Can't drop database (rmdir failed)
    pub const ER_DB_DROP_RMDIR: u16 = 1010;
    /// Can't delete file
    pub const ER_CANT_DELETE_FILE: u16 = 1011;
    /// Can't read record
    pub const ER_CANT_FIND_SYSTEM_REC: u16 = 1012;
    /// Can't get status
    pub const ER_CANT_GET_STAT: u16 = 1013;
    /// Can't lock file
    pub const ER_CANT_LOCK: u16 = 1015;
    /// Can't open file
    pub const ER_CANT_OPEN_FILE: u16 = 1016;
    /// Can't find file
    pub const ER_FILE_NOT_FOUND: u16 = 1017;
    /// Can't read directory
    pub const ER_CANT_READ_DIR: u16 = 1018;
    /// Record has changed
    pub const ER_CHECKREAD: u16 = 1020;
    /// Disk full
    pub const ER_DISK_FULL: u16 = 1021;
    /// Duplicate key
    pub const ER_DUP_KEY: u16 = 1022;
    /// Error on close
    pub const ER_ERROR_ON_CLOSE: u16 = 1023;
    /// Error reading file
    pub const ER_ERROR_ON_READ: u16 = 1024;
    /// Error on rename
    pub const ER_ERROR_ON_RENAME: u16 = 1025;
    /// Error writing file
    pub const ER_ERROR_ON_WRITE: u16 = 1026;
    /// File used
    pub const ER_FILE_USED: u16 = 1027;
    /// Sort aborted
    pub const ER_FILSORT_ABORT: u16 = 1028;
    /// Got error from storage engine
    pub const ER_GET_ERRNO: u16 = 1030;
    /// Table storage engine doesn't have this option
    pub const ER_ILLEGAL_HA: u16 = 1031;
    /// Key not found
    pub const ER_KEY_NOT_FOUND: u16 = 1032;
    /// Incorrect information in file
    pub const ER_NOT_FORM_FILE: u16 = 1033;
    /// Incorrect key file
    pub const ER_NOT_KEYFILE: u16 = 1034;
    /// Old key file
    pub const ER_OLD_KEYFILE: u16 = 1035;
    /// Table is read only
    pub const ER_OPEN_AS_READONLY: u16 = 1036;
    /// Out of memory
    pub const ER_OUTOFMEMORY: u16 = 1037;
    /// Out of sort memory
    pub const ER_OUT_OF_SORTMEMORY: u16 = 1038;
    /// Unexpected EOF
    pub const ER_UNEXPECTED_EOF: u16 = 1039;
    /// Too many connections
    pub const ER_CON_COUNT_ERROR: u16 = 1040;
    /// Out of resources
    pub const ER_OUT_OF_RESOURCES: u16 = 1041;
    /// Can't connect to local server
    pub const ER_BAD_HOST_ERROR: u16 = 1042;
    /// Handshake error
    pub const ER_HANDSHAKE_ERROR: u16 = 1043;
    /// Unknown database
    pub const ER_DBACCESS_DENIED_ERROR: u16 = 1044;
    /// Access denied for user
    pub const ER_ACCESS_DENIED_ERROR: u16 = 1045;
    /// No database selected
    pub const ER_NO_DB_ERROR: u16 = 1046;
    /// Unknown command
    pub const ER_UNKNOWN_COM_ERROR: u16 = 1047;
    /// Column cannot be null
    pub const ER_BAD_NULL_ERROR: u16 = 1048;
    /// Unknown database
    pub const ER_BAD_DB_ERROR: u16 = 1049;
    /// Table already exists
    pub const ER_TABLE_EXISTS_ERROR: u16 = 1050;
    /// Unknown table
    pub const ER_BAD_TABLE_ERROR: u16 = 1051;
    /// Non-unique column
    pub const ER_NON_UNIQ_ERROR: u16 = 1052;
    /// Server shutdown in progress
    pub const ER_SERVER_SHUTDOWN: u16 = 1053;
    /// Unknown column
    pub const ER_BAD_FIELD_ERROR: u16 = 1054;
    /// Column in wrong table
    pub const ER_WRONG_FIELD_WITH_GROUP: u16 = 1055;
    /// Invalid group
    pub const ER_WRONG_GROUP_FIELD: u16 = 1056;
    /// Duplicate column name
    pub const ER_WRONG_SUM_SELECT: u16 = 1057;
    /// Duplicate column in GROUP BY
    pub const ER_WRONG_VALUE_COUNT: u16 = 1058;
    /// Too long ident
    pub const ER_TOO_LONG_IDENT: u16 = 1059;
    /// Duplicate column name
    pub const ER_DUP_FIELDNAME: u16 = 1060;
    /// Duplicate key name
    pub const ER_DUP_KEYNAME: u16 = 1061;
    /// Duplicate entry for key
    pub const ER_DUP_ENTRY: u16 = 1062;
    /// Incorrect column specifier
    pub const ER_WRONG_FIELD_SPEC: u16 = 1063;
    /// Parse error
    pub const ER_PARSE_ERROR: u16 = 1064;
    /// Query was empty
    pub const ER_EMPTY_QUERY: u16 = 1065;
    /// Not unique table/alias
    pub const ER_NONUNIQ_TABLE: u16 = 1066;
    /// Invalid default value
    pub const ER_INVALID_DEFAULT: u16 = 1067;
    /// Multiple primary key defined
    pub const ER_MULTIPLE_PRI_KEY: u16 = 1068;
    /// Too many keys specified
    pub const ER_TOO_MANY_KEYS: u16 = 1069;
    /// Too many key parts specified
    pub const ER_TOO_MANY_KEY_PARTS: u16 = 1070;
    /// Specified key was too long
    pub const ER_TOO_LONG_KEY: u16 = 1071;
    /// Key column doesn't exist
    pub const ER_KEY_COLUMN_DOES_NOT_EXIST: u16 = 1072;
    /// BLOB column can't be key
    pub const ER_BLOB_USED_AS_KEY: u16 = 1073;
    /// Too big fieldlength
    pub const ER_TOO_BIG_FIELDLENGTH: u16 = 1074;
    /// Incorrect table definition
    pub const ER_WRONG_AUTO_KEY: u16 = 1075;

    // More common errors
    /// Table is locked
    pub const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
    /// Deadlock found
    pub const ER_LOCK_DEADLOCK: u16 = 1213;
    /// Cannot add foreign key constraint
    pub const ER_CANNOT_ADD_FOREIGN: u16 = 1215;
    /// Cannot delete parent row
    pub const ER_ROW_IS_REFERENCED: u16 = 1217;
    /// Cannot add/update child row
    pub const ER_NO_REFERENCED_ROW: u16 = 1216;

    // Authentication errors
    /// Password hash format error
    pub const ER_PASSWORD_NO_MATCH: u16 = 1133;
    /// Password update failed
    pub const ER_PASSWORD_NOT_ALLOWED: u16 = 1131;
    /// New password format not supported
    pub const ER_NOT_SUPPORTED_AUTH_MODE: u16 = 1251;

    // Prepared statement errors
    /// Unknown prepared statement
    pub const ER_UNKNOWN_STMT_HANDLER: u16 = 1243;
    /// Prepared statement contains too many placeholders
    pub const ER_PS_MANY_PARAM: u16 = 1390;

    // Replication errors
    /// Binary log position invalid
    pub const ER_MASTER_FATAL_ERROR_READING_BINLOG: u16 = 1236;
    /// Slave already running
    pub const ER_SLAVE_RUNNING: u16 = 1198;
    /// Slave not running
    pub const ER_SLAVE_NOT_RUNNING: u16 = 1199;

    // Transaction errors
    /// This version doesn't support savepoints
    pub const ER_SP_DOES_NOT_EXIST: u16 = 1305;
    /// SAVEPOINT does not exist
    pub const ER_SAVEPOINT_DOES_NOT_EXIST: u16 = 1181;
    /// Can't execute the command in an active transaction
    pub const ER_CANT_EXECUTE_IN_READ_ONLY_TRANSACTION: u16 = 1792;

    // Client errors (2000-2999)
    /// Unknown MySQL error
    pub const CR_UNKNOWN_ERROR: u16 = 2000;
    /// Can't create UNIX socket
    pub const CR_SOCKET_CREATE_ERROR: u16 = 2001;
    /// Can't connect to local MySQL server
    pub const CR_CONNECTION_ERROR: u16 = 2002;
    /// Can't connect to MySQL server on host
    pub const CR_CONN_HOST_ERROR: u16 = 2003;
    /// Can't create TCP/IP socket
    pub const CR_IPSOCK_ERROR: u16 = 2004;
    /// Unknown MySQL server host
    pub const CR_UNKNOWN_HOST: u16 = 2005;
    /// MySQL server has gone away
    pub const CR_SERVER_GONE_ERROR: u16 = 2006;
    /// Protocol mismatch
    pub const CR_VERSION_ERROR: u16 = 2007;
    /// Out of memory
    pub const CR_OUT_OF_MEMORY: u16 = 2008;
    /// Wrong host info
    pub const CR_WRONG_HOST_INFO: u16 = 2009;
    /// Localhost via UNIX socket
    pub const CR_LOCALHOST_CONNECTION: u16 = 2010;
    /// Can't use TCP/IP
    pub const CR_TCP_CONNECTION: u16 = 2011;
    /// Server handshake error
    pub const CR_SERVER_HANDSHAKE_ERR: u16 = 2012;
    /// Lost connection during query
    pub const CR_SERVER_LOST: u16 = 2013;
    /// Commands out of sync
    pub const CR_COMMANDS_OUT_OF_SYNC: u16 = 2014;
    /// Named pipe connection error
    pub const CR_NAMEDPIPE_CONNECTION: u16 = 2015;
    /// Can't wait for named pipe
    pub const CR_NAMEDPIPEWAIT_ERROR: u16 = 2016;
    /// Can't open named pipe
    pub const CR_NAMEDPIPEOPEN_ERROR: u16 = 2017;
    /// Can't set state of named pipe
    pub const CR_NAMEDPIPESETSTATE_ERROR: u16 = 2018;
    /// Can't initialize character set
    pub const CR_CANT_READ_CHARSET: u16 = 2019;
    /// Got packet bigger than max_allowed_packet
    pub const CR_NET_PACKET_TOO_LARGE: u16 = 2020;
    /// SSL connection error
    pub const CR_SSL_CONNECTION_ERROR: u16 = 2026;
    /// Malformed packet
    pub const CR_MALFORMED_PACKET: u16 = 2027;
    /// Invalid connection handle
    pub const CR_INVALID_CONN_HANDLE: u16 = 2048;
    /// Authentication plugin error
    pub const CR_AUTH_PLUGIN_ERR: u16 = 2061;
}

/// MySQL server status flags.
pub mod status_flags {
    /// Is in a transaction.
    pub const SERVER_STATUS_IN_TRANS: u16 = 0x0001;
    /// Autocommit mode is set.
    pub const SERVER_STATUS_AUTOCOMMIT: u16 = 0x0002;
    /// More results exist.
    pub const SERVER_MORE_RESULTS_EXISTS: u16 = 0x0008;
    /// No good index was used.
    pub const SERVER_STATUS_NO_GOOD_INDEX_USED: u16 = 0x0010;
    /// No index was used.
    pub const SERVER_STATUS_NO_INDEX_USED: u16 = 0x0020;
    /// Used by the server to signal the client that the current cursor has reached the end.
    pub const SERVER_STATUS_CURSOR_EXISTS: u16 = 0x0040;
    /// Last row was sent.
    pub const SERVER_STATUS_LAST_ROW_SENT: u16 = 0x0080;
    /// Database was dropped.
    pub const SERVER_STATUS_DB_DROPPED: u16 = 0x0100;
    /// No backslash escapes.
    pub const SERVER_STATUS_NO_BACKSLASH_ESCAPES: u16 = 0x0200;
    /// Set when there is metadata changes.
    pub const SERVER_STATUS_METADATA_CHANGED: u16 = 0x0400;
    /// Query was slow.
    pub const SERVER_QUERY_WAS_SLOW: u16 = 0x0800;
    /// Statement produced warnings.
    pub const SERVER_PS_OUT_PARAMS: u16 = 0x1000;
    /// In a read-only transaction.
    pub const SERVER_STATUS_IN_TRANS_READONLY: u16 = 0x2000;
    /// Session state changed.
    pub const SERVER_SESSION_STATE_CHANGED: u16 = 0x4000;
}

/// Returns a human-readable name for a packet type.
fn packet_type_name(packet_type: u8) -> &'static str {
    match packet_type {
        packet_types::OK => "OK",
        packet_types::ERR => "ERR",
        packet_types::EOF => "EOF",
        packet_types::LOCAL_INFILE => "LOCAL_INFILE",
        _ => "Unknown",
    }
}

/// Returns a human-readable name for a command.
pub fn command_name(cmd: u8) -> &'static str {
    match cmd {
        commands::COM_SLEEP => "COM_SLEEP",
        commands::COM_QUIT => "COM_QUIT",
        commands::COM_INIT_DB => "COM_INIT_DB",
        commands::COM_QUERY => "COM_QUERY",
        commands::COM_FIELD_LIST => "COM_FIELD_LIST",
        commands::COM_CREATE_DB => "COM_CREATE_DB",
        commands::COM_DROP_DB => "COM_DROP_DB",
        commands::COM_REFRESH => "COM_REFRESH",
        commands::COM_SHUTDOWN => "COM_SHUTDOWN",
        commands::COM_STATISTICS => "COM_STATISTICS",
        commands::COM_PROCESS_INFO => "COM_PROCESS_INFO",
        commands::COM_CONNECT => "COM_CONNECT",
        commands::COM_PROCESS_KILL => "COM_PROCESS_KILL",
        commands::COM_DEBUG => "COM_DEBUG",
        commands::COM_PING => "COM_PING",
        commands::COM_CHANGE_USER => "COM_CHANGE_USER",
        commands::COM_BINLOG_DUMP => "COM_BINLOG_DUMP",
        commands::COM_TABLE_DUMP => "COM_TABLE_DUMP",
        commands::COM_REGISTER_SLAVE => "COM_REGISTER_SLAVE",
        commands::COM_STMT_PREPARE => "COM_STMT_PREPARE",
        commands::COM_STMT_EXECUTE => "COM_STMT_EXECUTE",
        commands::COM_STMT_SEND_LONG_DATA => "COM_STMT_SEND_LONG_DATA",
        commands::COM_STMT_CLOSE => "COM_STMT_CLOSE",
        commands::COM_STMT_RESET => "COM_STMT_RESET",
        commands::COM_SET_OPTION => "COM_SET_OPTION",
        commands::COM_STMT_FETCH => "COM_STMT_FETCH",
        commands::COM_BINLOG_DUMP_GTID => "COM_BINLOG_DUMP_GTID",
        commands::COM_RESET_CONNECTION => "COM_RESET_CONNECTION",
        commands::COM_CLONE => "COM_CLONE",
        _ => "Unknown",
    }
}

/// Returns a human-readable name for a column type.
pub fn column_type_name(col_type: u8) -> &'static str {
    match col_type {
        column_types::MYSQL_TYPE_DECIMAL => "DECIMAL",
        column_types::MYSQL_TYPE_TINY => "TINY",
        column_types::MYSQL_TYPE_SHORT => "SHORT",
        column_types::MYSQL_TYPE_LONG => "LONG",
        column_types::MYSQL_TYPE_FLOAT => "FLOAT",
        column_types::MYSQL_TYPE_DOUBLE => "DOUBLE",
        column_types::MYSQL_TYPE_NULL => "NULL",
        column_types::MYSQL_TYPE_TIMESTAMP => "TIMESTAMP",
        column_types::MYSQL_TYPE_LONGLONG => "LONGLONG",
        column_types::MYSQL_TYPE_INT24 => "INT24",
        column_types::MYSQL_TYPE_DATE => "DATE",
        column_types::MYSQL_TYPE_TIME => "TIME",
        column_types::MYSQL_TYPE_DATETIME => "DATETIME",
        column_types::MYSQL_TYPE_YEAR => "YEAR",
        column_types::MYSQL_TYPE_VARCHAR => "VARCHAR",
        column_types::MYSQL_TYPE_BIT => "BIT",
        column_types::MYSQL_TYPE_JSON => "JSON",
        column_types::MYSQL_TYPE_NEWDECIMAL => "NEWDECIMAL",
        column_types::MYSQL_TYPE_ENUM => "ENUM",
        column_types::MYSQL_TYPE_SET => "SET",
        column_types::MYSQL_TYPE_TINY_BLOB => "TINY_BLOB",
        column_types::MYSQL_TYPE_MEDIUM_BLOB => "MEDIUM_BLOB",
        column_types::MYSQL_TYPE_LONG_BLOB => "LONG_BLOB",
        column_types::MYSQL_TYPE_BLOB => "BLOB",
        column_types::MYSQL_TYPE_VAR_STRING => "VAR_STRING",
        column_types::MYSQL_TYPE_STRING => "STRING",
        column_types::MYSQL_TYPE_GEOMETRY => "GEOMETRY",
        _ => "Unknown",
    }
}

bitflags::bitflags! {
    /// Server status flags returned in OK and EOF packets.
    ///
    /// These flags indicate the current state of the server/session.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct ServerStatusFlags: u16 {
        /// A transaction is active.
        const IN_TRANS = 0x0001;
        /// Autocommit mode is enabled.
        const AUTOCOMMIT = 0x0002;
        /// More results are available (multi-statement).
        const MORE_RESULTS_EXISTS = 0x0008;
        /// No good index was used for the query.
        const NO_GOOD_INDEX_USED = 0x0010;
        /// No index was used for the query.
        const NO_INDEX_USED = 0x0020;
        /// A cursor exists for this result set.
        const CURSOR_EXISTS = 0x0040;
        /// This is the last row of the cursor.
        const LAST_ROW_SENT = 0x0080;
        /// The database was dropped.
        const DB_DROPPED = 0x0100;
        /// Backslash escapes are disabled.
        const NO_BACKSLASH_ESCAPES = 0x0200;
        /// Metadata has changed.
        const METADATA_CHANGED = 0x0400;
        /// The query was slow.
        const QUERY_WAS_SLOW = 0x0800;
        /// Output parameters are available (stored procedure).
        const PS_OUT_PARAMS = 0x1000;
        /// The transaction is read-only.
        const IN_TRANS_READONLY = 0x2000;
        /// Session state has changed.
        const SESSION_STATE_CHANGED = 0x4000;
    }
}

impl ServerStatusFlags {
    /// Check if a transaction is active.
    #[inline]
    pub fn in_transaction(self) -> bool {
        self.contains(Self::IN_TRANS)
    }

    /// Check if autocommit is enabled.
    #[inline]
    pub fn autocommit(self) -> bool {
        self.contains(Self::AUTOCOMMIT)
    }

    /// Check if more results exist (for multi-statement queries).
    #[inline]
    pub fn more_results(self) -> bool {
        self.contains(Self::MORE_RESULTS_EXISTS)
    }

    /// Check if session state changed (need to track session variables).
    #[inline]
    pub fn session_state_changed(self) -> bool {
        self.contains(Self::SESSION_STATE_CHANGED)
    }
}

impl Default for ServerStatusFlags {
    fn default() -> Self {
        Self::AUTOCOMMIT
    }
}

bitflags::bitflags! {
    /// Column flags from column definition packets.
    ///
    /// These flags describe properties of a column in result set metadata.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct ColumnFlags: u16 {
        /// Column cannot be NULL.
        const NOT_NULL = 0x0001;
        /// Column is part of a primary key.
        const PRI_KEY = 0x0002;
        /// Column is part of a unique key.
        const UNIQUE_KEY = 0x0004;
        /// Column is part of a non-unique key.
        const MULTIPLE_KEY = 0x0008;
        /// Column is a BLOB or TEXT.
        const BLOB = 0x0010;
        /// Column is unsigned.
        const UNSIGNED = 0x0020;
        /// Column is ZEROFILL.
        const ZEROFILL = 0x0040;
        /// Column is BINARY.
        const BINARY = 0x0080;
        /// Column is an ENUM.
        const ENUM = 0x0100;
        /// Column is AUTO_INCREMENT.
        const AUTO_INCREMENT = 0x0200;
        /// Column is a TIMESTAMP.
        const TIMESTAMP = 0x0400;
        /// Column is a SET.
        const SET = 0x0800;
        /// Column has no default value.
        const NO_DEFAULT_VALUE = 0x1000;
        /// Column is set to NOW on update.
        const ON_UPDATE_NOW = 0x2000;
        /// Column is a number (internal).
        const NUM = 0x8000;
    }
}

impl ColumnFlags {
    /// Check if the column can contain NULL values.
    #[inline]
    pub fn is_nullable(self) -> bool {
        !self.contains(Self::NOT_NULL)
    }

    /// Check if the column is part of the primary key.
    #[inline]
    pub fn is_primary_key(self) -> bool {
        self.contains(Self::PRI_KEY)
    }

    /// Check if the column is unsigned.
    #[inline]
    pub fn is_unsigned(self) -> bool {
        self.contains(Self::UNSIGNED)
    }

    /// Check if the column is auto-increment.
    #[inline]
    pub fn is_auto_increment(self) -> bool {
        self.contains(Self::AUTO_INCREMENT)
    }

    /// Check if the column is a BLOB type.
    #[inline]
    pub fn is_blob(self) -> bool {
        self.contains(Self::BLOB)
    }

    /// Check if the column is binary.
    #[inline]
    pub fn is_binary(self) -> bool {
        self.contains(Self::BINARY)
    }
}

impl Default for ColumnFlags {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_server_status_flags() {
        let flags = ServerStatusFlags::IN_TRANS | ServerStatusFlags::AUTOCOMMIT;
        assert!(flags.in_transaction());
        assert!(flags.autocommit());
        assert!(!flags.more_results());
    }

    #[test]
    fn test_column_flags() {
        let flags = ColumnFlags::NOT_NULL | ColumnFlags::PRI_KEY | ColumnFlags::AUTO_INCREMENT;
        assert!(!flags.is_nullable());
        assert!(flags.is_primary_key());
        assert!(flags.is_auto_increment());
        assert!(!flags.is_unsigned());
    }

    #[test]
    fn test_server_status_from_bits() {
        let flags = ServerStatusFlags::from_bits_truncate(0x0003);
        assert!(flags.contains(ServerStatusFlags::IN_TRANS));
        assert!(flags.contains(ServerStatusFlags::AUTOCOMMIT));
    }

    #[test]
    fn test_column_flags_from_bits() {
        let flags = ColumnFlags::from_bits_truncate(0x0023);
        assert!(flags.contains(ColumnFlags::NOT_NULL));
        assert!(flags.contains(ColumnFlags::PRI_KEY));
        assert!(flags.contains(ColumnFlags::UNSIGNED));
    }
}
