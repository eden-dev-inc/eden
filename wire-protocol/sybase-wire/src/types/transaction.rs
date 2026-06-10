//! Transaction management helpers.
//!
//! Provides utilities for transaction control in Sybase TDS.

use crate::types::packet::PacketType;
use crate::write::PacketBuilder;

/// Transaction isolation levels.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum IsolationLevel {
    /// Read uncommitted (dirty reads allowed).
    ReadUncommitted = 0,
    /// Read committed (default).
    ReadCommitted = 1,
    /// Repeatable read.
    RepeatableRead = 2,
    /// Serializable (most restrictive).
    Serializable = 3,
}

impl IsolationLevel {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(IsolationLevel::ReadUncommitted),
            1 => Some(IsolationLevel::ReadCommitted),
            2 => Some(IsolationLevel::RepeatableRead),
            3 => Some(IsolationLevel::Serializable),
            _ => None,
        }
    }

    /// Get the SQL command to set this isolation level.
    pub fn set_command(&self) -> &'static str {
        match self {
            IsolationLevel::ReadUncommitted => "SET TRANSACTION ISOLATION LEVEL 0",
            IsolationLevel::ReadCommitted => "SET TRANSACTION ISOLATION LEVEL 1",
            IsolationLevel::RepeatableRead => "SET TRANSACTION ISOLATION LEVEL 2",
            IsolationLevel::Serializable => "SET TRANSACTION ISOLATION LEVEL 3",
        }
    }
}

/// Transaction state tracking.
#[derive(Clone, Debug, Default)]
pub struct TransactionState {
    /// Whether a transaction is currently active.
    pub in_transaction: bool,
    /// Current nesting level (for savepoints).
    pub nesting_level: u32,
    /// Last transaction ID (if known).
    pub transaction_id: Option<u64>,
}

impl TransactionState {
    /// Create a new transaction state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record that a transaction has begun.
    pub fn begin(&mut self) {
        self.in_transaction = true;
        self.nesting_level += 1;
    }

    /// Record that a transaction has committed.
    pub fn commit(&mut self) {
        if self.nesting_level > 0 {
            self.nesting_level -= 1;
        }
        if self.nesting_level == 0 {
            self.in_transaction = false;
            self.transaction_id = None;
        }
    }

    /// Record that a transaction has rolled back.
    pub fn rollback(&mut self) {
        self.in_transaction = false;
        self.nesting_level = 0;
        self.transaction_id = None;
    }

    /// Record a savepoint.
    pub fn savepoint(&mut self) {
        self.nesting_level += 1;
    }
}

/// Builder for transaction control packets.
pub struct TransactionBuilder;

impl TransactionBuilder {
    /// Build a BEGIN TRANSACTION packet.
    pub fn begin() -> Vec<u8> {
        Self::query("BEGIN TRANSACTION")
    }

    /// Build a BEGIN TRANSACTION packet with a name.
    pub fn begin_named(name: &str) -> Vec<u8> {
        Self::query(&format!("BEGIN TRANSACTION {}", name))
    }

    /// Build a COMMIT TRANSACTION packet.
    pub fn commit() -> Vec<u8> {
        Self::query("COMMIT TRANSACTION")
    }

    /// Build a COMMIT TRANSACTION packet with a name.
    pub fn commit_named(name: &str) -> Vec<u8> {
        Self::query(&format!("COMMIT TRANSACTION {}", name))
    }

    /// Build a ROLLBACK TRANSACTION packet.
    pub fn rollback() -> Vec<u8> {
        Self::query("ROLLBACK TRANSACTION")
    }

    /// Build a ROLLBACK TRANSACTION packet with a name.
    pub fn rollback_named(name: &str) -> Vec<u8> {
        Self::query(&format!("ROLLBACK TRANSACTION {}", name))
    }

    /// Build a SAVE TRANSACTION packet (create savepoint).
    pub fn savepoint(name: &str) -> Vec<u8> {
        Self::query(&format!("SAVE TRANSACTION {}", name))
    }

    /// Build a ROLLBACK to savepoint packet.
    pub fn rollback_to_savepoint(name: &str) -> Vec<u8> {
        Self::query(&format!("ROLLBACK TRANSACTION {}", name))
    }

    /// Build a SET TRANSACTION ISOLATION LEVEL packet.
    pub fn set_isolation_level(level: IsolationLevel) -> Vec<u8> {
        Self::query(level.set_command())
    }

    /// Build a SET CHAINED ON packet (auto-transaction mode).
    pub fn set_chained_on() -> Vec<u8> {
        Self::query("SET CHAINED ON")
    }

    /// Build a SET CHAINED OFF packet (unchained mode).
    pub fn set_chained_off() -> Vec<u8> {
        Self::query("SET CHAINED OFF")
    }

    /// Helper to build a query packet.
    fn query(sql: &str) -> Vec<u8> {
        PacketBuilder::new(PacketType::Query5).write_bytes(sql.as_bytes()).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_isolation_level_values() {
        assert_eq!(IsolationLevel::ReadUncommitted as u8, 0);
        assert_eq!(IsolationLevel::ReadCommitted as u8, 1);
        assert_eq!(IsolationLevel::RepeatableRead as u8, 2);
        assert_eq!(IsolationLevel::Serializable as u8, 3);
    }

    #[test]
    fn test_isolation_level_from_u8() {
        assert_eq!(IsolationLevel::from_u8(0), Some(IsolationLevel::ReadUncommitted));
        assert_eq!(IsolationLevel::from_u8(1), Some(IsolationLevel::ReadCommitted));
        assert_eq!(IsolationLevel::from_u8(4), None);
    }

    #[test]
    fn test_transaction_state() {
        let mut state = TransactionState::new();
        assert!(!state.in_transaction);
        assert_eq!(state.nesting_level, 0);

        state.begin();
        assert!(state.in_transaction);
        assert_eq!(state.nesting_level, 1);

        state.savepoint();
        assert_eq!(state.nesting_level, 2);

        state.commit();
        assert!(state.in_transaction);
        assert_eq!(state.nesting_level, 1);

        state.commit();
        assert!(!state.in_transaction);
        assert_eq!(state.nesting_level, 0);
    }

    #[test]
    fn test_transaction_rollback() {
        let mut state = TransactionState::new();
        state.begin();
        state.savepoint();
        state.rollback();

        assert!(!state.in_transaction);
        assert_eq!(state.nesting_level, 0);
    }

    #[test]
    fn test_transaction_builder_begin() {
        let packet = TransactionBuilder::begin();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_transaction_builder_commit() {
        let packet = TransactionBuilder::commit();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_transaction_builder_rollback() {
        let packet = TransactionBuilder::rollback();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_transaction_builder_savepoint() {
        let packet = TransactionBuilder::savepoint("sp1");
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_transaction_builder_isolation() {
        let packet = TransactionBuilder::set_isolation_level(IsolationLevel::Serializable);
        assert!(!packet.is_empty());
    }
}
