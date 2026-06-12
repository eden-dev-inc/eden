//! MySQL command packets.
//!
//! Commands are sent by the client to the server to request actions.

use crate::error::commands;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// Parsed MySQL command.
#[derive(Clone, Debug)]
pub enum Command {
    /// COM_QUIT - Close the connection.
    Quit,
    /// COM_INIT_DB - Change default database.
    InitDb { database: String },
    /// COM_QUERY - Execute a text query.
    Query { query: String },
    /// COM_FIELD_LIST - Get column definitions for a table.
    FieldList { table: String, wildcard: String },
    /// COM_PING - Ping the server.
    Ping,
    /// COM_STATISTICS - Get server statistics.
    Statistics,
    /// COM_DEBUG - Dump debug info.
    Debug,
    /// COM_PROCESS_INFO - Get process list.
    ProcessInfo,
    /// COM_PROCESS_KILL - Kill a connection.
    ProcessKill { connection_id: u32 },
    /// COM_CHANGE_USER - Change user during connection.
    ChangeUser {
        username: String,
        auth_response: Vec<u8>,
        database: String,
        character_set: u16,
        auth_plugin: String,
    },
    /// COM_STMT_PREPARE - Prepare a statement.
    StmtPrepare { query: String },
    /// COM_STMT_EXECUTE - Execute a prepared statement.
    StmtExecute {
        statement_id: u32,
        flags: u8,
        iteration_count: u32,
        params: Vec<u8>,
    },
    /// COM_STMT_SEND_LONG_DATA - Send long data for a parameter.
    StmtSendLongData { statement_id: u32, param_id: u16, data: Vec<u8> },
    /// COM_STMT_CLOSE - Close a prepared statement.
    StmtClose { statement_id: u32 },
    /// COM_STMT_RESET - Reset a prepared statement.
    StmtReset { statement_id: u32 },
    /// COM_STMT_FETCH - Fetch rows from a prepared statement cursor.
    StmtFetch { statement_id: u32, num_rows: u32 },
    /// COM_SET_OPTION - Set options.
    SetOption { option: u16 },
    /// COM_RESET_CONNECTION - Reset session state.
    ResetConnection,
    /// COM_BINLOG_DUMP - Start binlog replication.
    BinlogDump {
        binlog_pos: u32,
        flags: u16,
        server_id: u32,
        binlog_filename: String,
    },
    /// COM_BINLOG_DUMP_GTID - Start binlog replication with GTID.
    BinlogDumpGtid {
        flags: u16,
        server_id: u32,
        binlog_filename: String,
        binlog_pos: u64,
        gtid_data: Vec<u8>,
    },
    /// COM_REGISTER_SLAVE - Register as a replication slave.
    RegisterSlave {
        server_id: u32,
        hostname: String,
        user: String,
        password: String,
        port: u16,
        replication_rank: u32,
        master_id: u32,
    },
    /// Unknown command.
    Unknown { command_type: u8, data: Vec<u8> },
}

impl Command {
    /// Get the command type byte.
    pub fn command_type(&self) -> u8 {
        match self {
            Command::Quit => commands::COM_QUIT,
            Command::InitDb { .. } => commands::COM_INIT_DB,
            Command::Query { .. } => commands::COM_QUERY,
            Command::FieldList { .. } => commands::COM_FIELD_LIST,
            Command::Ping => commands::COM_PING,
            Command::Statistics => commands::COM_STATISTICS,
            Command::Debug => commands::COM_DEBUG,
            Command::ProcessInfo => commands::COM_PROCESS_INFO,
            Command::ProcessKill { .. } => commands::COM_PROCESS_KILL,
            Command::ChangeUser { .. } => commands::COM_CHANGE_USER,
            Command::StmtPrepare { .. } => commands::COM_STMT_PREPARE,
            Command::StmtExecute { .. } => commands::COM_STMT_EXECUTE,
            Command::StmtSendLongData { .. } => commands::COM_STMT_SEND_LONG_DATA,
            Command::StmtClose { .. } => commands::COM_STMT_CLOSE,
            Command::StmtReset { .. } => commands::COM_STMT_RESET,
            Command::StmtFetch { .. } => commands::COM_STMT_FETCH,
            Command::SetOption { .. } => commands::COM_SET_OPTION,
            Command::ResetConnection => commands::COM_RESET_CONNECTION,
            Command::BinlogDump { .. } => commands::COM_BINLOG_DUMP,
            Command::BinlogDumpGtid { .. } => commands::COM_BINLOG_DUMP_GTID,
            Command::RegisterSlave { .. } => commands::COM_REGISTER_SLAVE,
            Command::Unknown { command_type, .. } => *command_type,
        }
    }

    /// Get the command name.
    pub fn name(&self) -> &'static str {
        crate::error::command_name(self.command_type())
    }

    /// Check if this is a query command.
    pub fn is_query(&self) -> bool {
        matches!(self, Command::Query { .. })
    }

    /// Get the query text if this is a query command.
    pub fn query_text(&self) -> Option<&str> {
        match self {
            Command::Query { query } => Some(query),
            Command::StmtPrepare { query } => Some(query),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CommandError {
    #[error("invalid command type: {0:#04X}")]
    InvalidCommandType(u8),
    #[error("command payload too short")]
    PayloadTooShort,
    #[error("invalid string encoding in command")]
    InvalidStringEncoding,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for Command {
    type ParseError = CommandError;
    type Value<'s>
        = Command
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let cmd_type = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        match cmd_type {
            commands::COM_QUIT => Ok(Command::Quit),

            commands::COM_INIT_DB => {
                let mut bytes = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    bytes.push(byte);
                }
                let database = String::from_utf8_lossy(&bytes).into_owned();
                Ok(Command::InitDb { database })
            }

            commands::COM_QUERY => {
                let mut bytes = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    bytes.push(byte);
                }
                let query = String::from_utf8_lossy(&bytes).into_owned();
                Ok(Command::Query { query })
            }

            commands::COM_PING => Ok(Command::Ping),

            commands::COM_STATISTICS => Ok(Command::Statistics),

            commands::COM_DEBUG => Ok(Command::Debug),

            commands::COM_PROCESS_INFO => Ok(Command::ProcessInfo),

            commands::COM_PROCESS_KILL => {
                let connection_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::ProcessKill { connection_id })
            }

            commands::COM_STMT_PREPARE => {
                let mut bytes = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    bytes.push(byte);
                }
                let query = String::from_utf8_lossy(&bytes).into_owned();
                Ok(Command::StmtPrepare { query })
            }

            commands::COM_STMT_EXECUTE => {
                let statement_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let flags = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                let iteration_count = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let mut params = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    params.push(byte);
                }
                Ok(Command::StmtExecute { statement_id, flags, iteration_count, params })
            }

            commands::COM_STMT_CLOSE => {
                let statement_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::StmtClose { statement_id })
            }

            commands::COM_STMT_RESET => {
                let statement_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::StmtReset { statement_id })
            }

            commands::COM_STMT_FETCH => {
                let statement_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let num_rows = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::StmtFetch { statement_id, num_rows })
            }

            commands::COM_SET_OPTION => {
                let option = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::SetOption { option })
            }

            commands::COM_RESET_CONNECTION => Ok(Command::ResetConnection),

            commands::COM_BINLOG_DUMP => {
                let binlog_pos = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                let server_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let mut filename_bytes = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    filename_bytes.push(byte);
                }
                let binlog_filename = String::from_utf8_lossy(&filename_bytes).into_owned();
                Ok(Command::BinlogDump { binlog_pos, flags, server_id, binlog_filename })
            }

            commands::COM_BINLOG_DUMP_GTID => {
                let flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                let server_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let filename_len = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let filename_bytes = stream.read_bytes_sync(filename_len as usize).map_err(MysqlParseError::Stream)?;
                let binlog_filename = String::from_utf8_lossy(&filename_bytes).into_owned();
                let binlog_pos = stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?;
                // Read GTID data if present
                let gtid_data_len = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let gtid_data = stream.read_bytes_sync(gtid_data_len as usize).map_err(MysqlParseError::Stream)?;
                Ok(Command::BinlogDumpGtid { flags, server_id, binlog_filename, binlog_pos, gtid_data })
            }

            commands::COM_REGISTER_SLAVE => {
                let server_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let hostname_len = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                let hostname_bytes = stream.read_bytes_sync(hostname_len as usize).map_err(MysqlParseError::Stream)?;
                let hostname = String::from_utf8_lossy(&hostname_bytes).into_owned();
                let user_len = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                let user_bytes = stream.read_bytes_sync(user_len as usize).map_err(MysqlParseError::Stream)?;
                let user = String::from_utf8_lossy(&user_bytes).into_owned();
                let password_len = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                let password_bytes = stream.read_bytes_sync(password_len as usize).map_err(MysqlParseError::Stream)?;
                let password = String::from_utf8_lossy(&password_bytes).into_owned();
                let port = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                let replication_rank = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                let master_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;
                Ok(Command::RegisterSlave {
                    server_id,
                    hostname,
                    user,
                    password,
                    port,
                    replication_rank,
                    master_id,
                })
            }

            _ => {
                // Unknown command - read remaining data
                let mut data = Vec::new();
                while let Ok(byte) = stream.read_u8_sync() {
                    data.push(byte);
                }
                Ok(Command::Unknown { command_type: cmd_type, data })
            }
        }
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for Command {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parse_quit() {
        let data = [commands::COM_QUIT];
        let stream = SliceStream::new(&data);

        let cmd = Command::parse_sync(&stream).unwrap();
        assert!(matches!(cmd, Command::Quit));
    }

    #[test]
    fn test_parse_ping() {
        let data = [commands::COM_PING];
        let stream = SliceStream::new(&data);

        let cmd = Command::parse_sync(&stream).unwrap();
        assert!(matches!(cmd, Command::Ping));
    }

    #[test]
    fn test_parse_query() {
        let mut data = vec![commands::COM_QUERY];
        data.extend_from_slice(b"SELECT 1");

        let stream = SliceStream::new(&data);
        let cmd = Command::parse_sync(&stream).unwrap();

        match cmd {
            Command::Query { query } => assert_eq!(query, "SELECT 1"),
            _ => panic!("Expected Query command"),
        }
    }

    #[test]
    fn test_parse_init_db() {
        let mut data = vec![commands::COM_INIT_DB];
        data.extend_from_slice(b"mydb");

        let stream = SliceStream::new(&data);
        let cmd = Command::parse_sync(&stream).unwrap();

        match cmd {
            Command::InitDb { database } => assert_eq!(database, "mydb"),
            _ => panic!("Expected InitDb command"),
        }
    }

    #[test]
    fn test_parse_stmt_close() {
        let mut data = vec![commands::COM_STMT_CLOSE];
        data.extend_from_slice(&42u32.to_le_bytes());

        let stream = SliceStream::new(&data);
        let cmd = Command::parse_sync(&stream).unwrap();

        match cmd {
            Command::StmtClose { statement_id } => assert_eq!(statement_id, 42),
            _ => panic!("Expected StmtClose command"),
        }
    }

    #[test]
    fn test_command_name() {
        let cmd = Command::Query { query: "SELECT 1".to_string() };
        assert_eq!(cmd.name(), "COM_QUERY");

        let cmd = Command::Ping;
        assert_eq!(cmd.name(), "COM_PING");
    }

    #[test]
    fn test_query_text() {
        let cmd = Command::Query { query: "SELECT 1".to_string() };
        assert_eq!(cmd.query_text(), Some("SELECT 1"));

        let cmd = Command::Ping;
        assert_eq!(cmd.query_text(), None);
    }
}
