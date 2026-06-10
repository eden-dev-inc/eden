#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Eden Core Authentication
//!
//! Authentication and authorization primitives for Eve.
//!
//! ## Overview
//!
//! This crate provides the authentication infrastructure for Eve, including:
//! - JWT token generation and validation
//! - Password hashing and verification
//! - Bearer token authentication
//! - Organization-scoped authentication
//!
//! ## Architecture
//!
//! Eden's authentication system uses a layered approach:
//!
//! 1. **Password Authentication**: User credentials are hashed using SHA256 with
//!    random salts for secure storage.
//! 2. **JWT Tokens**: Authenticated users receive HMAC-SHA256 signed JWT tokens
//!    containing their identity (user ID, user UUID) and organization context
//!    (organization ID, organization UUID).
//! 3. **Bearer Token Validation**: API requests validate bearer tokens to extract
//!    user identity and permissions.
//!
//! All authentication operations are organization-scoped, ensuring complete isolation
//! between different organizations using the system.
//!
//! ## Core Concepts
//!
//! ### JWT Token Flow
//!
//! 1. User authenticates with username/password
//! 2. System validates credentials against stored hash
//! 3. System generates JWT containing user ID and organization ID
//! 4. Client includes JWT in `Authorization: Bearer <token>` header
//! 5. System validates JWT and extracts identity for request processing
//!
//! ### Token Expiration
//!
//! JWT tokens have configurable expiration times (default: 1 hour). Expired tokens
//! are rejected, requiring re-authentication. This balances security with user
//! experience.
//!
//! ## Integration with eden_service
//!
//! The authentication types defined here are used by `eden_service` HTTP middleware:
//! - `basic_auth_validator`: Validates HTTP Basic Auth credentials
//! - `bearer_auth_validator`: Validates JWT bearer tokens
//! - `org_token_validator`: Validates organization-specific tokens for relay operations

pub mod api_key;
pub mod auth;
pub mod bearer;
pub mod jwt;
pub mod password;

pub use api_key::*;
pub use auth::*;
pub use bearer::*;
pub use jwt::*;
pub use password::*;
