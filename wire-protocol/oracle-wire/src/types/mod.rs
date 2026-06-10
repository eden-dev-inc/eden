//! Oracle TNS packet types.
//!
//! This module organizes TNS packet types by version compatibility:
//!
//! # Common Types (All Versions)
//! - [`packet`]: Base packet header and type definitions
//! - [`connect`]: Connection request packets
//! - [`accept`]: Connection accept packets
//! - [`data`]: Data transfer packets
//! - [`refuse`]: Connection refusal packets
//! - [`redirect`]: Redirect packets (load balancing, RAC)
//!
//! # TNS v8+ Types
//! - Basic TNS protocol support
//!
//! # TNS v11+ Types
//! - DRCP (Database Resident Connection Pooling) support
//! - Session multiplexing
//! - [`marker`]: Marker packets for break/reset signaling
//!
//! # TNS v12+ Types
//! - [`data_descriptor`]: Data descriptor packets
//! - Multitenant architecture support
//!
//! # TTI (Two-Task Interface)
//! - [`tti`]: Database operation protocol on top of TNS Data packets
//!
//! # Dynamic Type
//! - [`dynamic`]: Version-agnostic parser that can handle any TNS version

// Common types (all TNS versions)
pub mod abort;
pub mod accept;
pub mod ack;
pub mod connect;
pub mod control;
pub mod data;
pub mod packet;
pub mod redirect;
pub mod refuse;
pub mod resend;

// TNS v11+ types
pub mod marker;

// TNS v12+ types
pub mod data_descriptor;

// TTI (Two-Task Interface) - database operations
pub mod tti;

// Dynamic type that can parse any TNS version
pub mod dynamic;
