use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Redis operation errors (0x18XX error codes).
///
/// Covers all error types specific to Redis operations including connection failures,
/// authentication errors, type mismatches, cluster routing, and server state errors.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum RedisError {
    // Connection & Network (0x01-0x0F)
    /// Connection timeout when attempting to connect to Redis server
    ConnectionTimeout, // 0x01
    /// Connection refused by Redis server
    ConnectionRefused, // 0x02
    /// Connection lost or broken pipe error
    ConnectionLost, // 0x03
    /// Connection pool exhausted (no available connections)
    PoolExhausted, // 0x04

    // Authentication & Authorization (0x10-0x1F)
    /// Authentication required (NOAUTH error from Redis)
    AuthRequired, // 0x10
    /// Invalid password provided (WRONGPASS error)
    InvalidPassword, // 0x11
    /// Permission denied for operation
    PermissionDenied, // 0x12

    // Data Type Errors (0x20-0x2F)
    /// Wrong data type for operation (WRONGTYPE error)
    WrongType, // 0x20
    /// Failed to convert between types
    TypeConversionFailed, // 0x21
    /// Invalid argument provided to command
    InvalidArgument, // 0x22

    // Command Errors (0x30-0x3F)
    /// Redis command not found or not supported
    CommandNotFound, // 0x30
    /// Invalid command syntax
    InvalidSyntax, // 0x31
    /// Value out of valid range
    OutOfRange, // 0x32

    // Cluster Errors (0x40-0x4F)
    /// Cluster MOVED redirect - key moved to different slot
    ClusterMoved, // 0x40
    /// Cluster ASK redirect - temporary redirect for key
    ClusterAsk, // 0x41
    /// Cluster is down or unavailable
    ClusterDown, // 0x42
    /// Cross-slot operation not allowed in cluster mode
    ClusterCrossSlot, // 0x43

    // Transaction Errors (0x50-0x5F)
    /// Transaction aborted (EXECABORT error)
    TransactionAborted, // 0x50
    /// WATCH key modified, transaction failed
    WatchFailed, // 0x51

    // Scripting Errors (0x60-0x6F)
    /// Lua script not found (NOSCRIPT error)
    ScriptNotFound, // 0x60
    /// Error executing Lua script
    ScriptError, // 0x61

    // Server State (0x70-0x7F)
    /// Server is busy loading dataset or processing command
    ServerBusy, // 0x70
    /// Server is in read-only mode (replica)
    ServerReadOnly, // 0x71
    /// Server out of memory (OOM error)
    ServerOutOfMemory, // 0x72
    /// Master server is down or unreachable
    MasterDown, // 0x73

    // Protocol Errors (0x80-0x8F)
    /// Redis protocol error or malformed response
    ProtocolError, // 0x80
    /// Invalid response from Redis server
    InvalidResponse, // 0x81

    // Retry-able Errors (0x90-0x9F)
    /// Operation should be retried (TryAgain error)
    TryAgain, // 0x90
    /// Operation timed out
    Timeout, // 0x91

    // Generic (0xFX)
    /// I/O error with details
    IoError(String), // 0xFE
    /// Custom error message for backward compatibility
    Custom(String), // 0xFF
}

impl RedisError {
    /// Returns the specific error code (0x01-0xFF) for this Redis error.
    pub fn error_code(&self) -> u8 {
        match self {
            // Connection & Network
            RedisError::ConnectionTimeout => 0x01,
            RedisError::ConnectionRefused => 0x02,
            RedisError::ConnectionLost => 0x03,
            RedisError::PoolExhausted => 0x04,

            // Authentication & Authorization
            RedisError::AuthRequired => 0x10,
            RedisError::InvalidPassword => 0x11,
            RedisError::PermissionDenied => 0x12,

            // Data Type Errors
            RedisError::WrongType => 0x20,
            RedisError::TypeConversionFailed => 0x21,
            RedisError::InvalidArgument => 0x22,

            // Command Errors
            RedisError::CommandNotFound => 0x30,
            RedisError::InvalidSyntax => 0x31,
            RedisError::OutOfRange => 0x32,

            // Cluster Errors
            RedisError::ClusterMoved => 0x40,
            RedisError::ClusterAsk => 0x41,
            RedisError::ClusterDown => 0x42,
            RedisError::ClusterCrossSlot => 0x43,

            // Transaction Errors
            RedisError::TransactionAborted => 0x50,
            RedisError::WatchFailed => 0x51,

            // Scripting Errors
            RedisError::ScriptNotFound => 0x60,
            RedisError::ScriptError => 0x61,

            // Server State
            RedisError::ServerBusy => 0x70,
            RedisError::ServerReadOnly => 0x71,
            RedisError::ServerOutOfMemory => 0x72,
            RedisError::MasterDown => 0x73,

            // Protocol Errors
            RedisError::ProtocolError => 0x80,
            RedisError::InvalidResponse => 0x81,

            // Retry-able Errors
            RedisError::TryAgain => 0x90,
            RedisError::Timeout => 0x91,

            // Generic
            RedisError::IoError(_) => 0xFE,
            RedisError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for RedisError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            // Connection & Network
            RedisError::ConnectionTimeout => {
                write!(f, "Connection timeout. The Redis server did not respond in time")
            }
            RedisError::ConnectionRefused => {
                write!(f, "Connection refused. Unable to connect to Redis server")
            }
            RedisError::ConnectionLost => {
                write!(f, "Connection lost. The connection to Redis was terminated")
            }
            RedisError::PoolExhausted => {
                write!(f, "Connection pool exhausted. All Redis connections are in use")
            }

            // Authentication & Authorization
            RedisError::AuthRequired => {
                write!(f, "Authentication required. Please provide valid Redis credentials")
            }
            RedisError::InvalidPassword => {
                write!(f, "Invalid password. The provided Redis password is incorrect")
            }
            RedisError::PermissionDenied => {
                write!(f, "Permission denied. You do not have permission to perform this operation")
            }

            // Data Type Errors
            RedisError::WrongType => {
                write!(f, "Wrong type. Operation against a key holding the wrong kind of value")
            }
            RedisError::TypeConversionFailed => {
                write!(f, "Type conversion failed. Unable to convert Redis value to expected type")
            }
            RedisError::InvalidArgument => {
                write!(f, "Invalid argument. The provided argument is not valid for this command")
            }

            // Command Errors
            RedisError::CommandNotFound => {
                write!(f, "Command not found. The Redis command is not recognized or supported")
            }
            RedisError::InvalidSyntax => {
                write!(f, "Invalid syntax. The command syntax is incorrect")
            }
            RedisError::OutOfRange => {
                write!(f, "Out of range. The value is outside the valid range")
            }

            // Cluster Errors
            RedisError::ClusterMoved => {
                write!(f, "Cluster redirect (MOVED). The key has been moved to a different cluster node")
            }
            RedisError::ClusterAsk => {
                write!(f, "Cluster redirect (ASK). Temporary redirect to a different cluster node")
            }
            RedisError::ClusterDown => {
                write!(f, "Cluster down. The Redis cluster is unavailable")
            }
            RedisError::ClusterCrossSlot => {
                write!(f, "Cross-slot error. Keys in request don't hash to the same slot")
            }

            // Transaction Errors
            RedisError::TransactionAborted => {
                write!(f, "Transaction aborted. The transaction was aborted due to an error")
            }
            RedisError::WatchFailed => {
                write!(f, "Watch failed. A watched key was modified, transaction cannot proceed")
            }

            // Scripting Errors
            RedisError::ScriptNotFound => {
                write!(f, "Script not found. The Lua script does not exist on the server")
            }
            RedisError::ScriptError => {
                write!(f, "Script error. An error occurred while executing the Lua script")
            }

            // Server State
            RedisError::ServerBusy => {
                write!(f, "Server busy. Redis is busy loading the dataset or processing a command")
            }
            RedisError::ServerReadOnly => {
                write!(f, "Server read-only. Write operations are not allowed on this Redis instance")
            }
            RedisError::ServerOutOfMemory => {
                write!(f, "Out of memory. Redis server has run out of memory")
            }
            RedisError::MasterDown => {
                write!(f, "Master down. The Redis master server is unreachable")
            }

            // Protocol Errors
            RedisError::ProtocolError => {
                write!(f, "Protocol error. Invalid Redis protocol or malformed response")
            }
            RedisError::InvalidResponse => {
                write!(f, "Invalid response. Received an unexpected response from Redis")
            }

            // Retry-able Errors
            RedisError::TryAgain => {
                write!(f, "Try again. The operation should be retried as it may succeed")
            }
            RedisError::Timeout => {
                write!(f, "Timeout. The operation timed out before completing")
            }

            // Generic
            RedisError::IoError(msg) => {
                write!(f, "I/O error: {}", msg)
            }
            RedisError::Custom(msg) => {
                write!(f, "{}", msg)
            }
        }
    }
}

impl std::error::Error for RedisError {}
