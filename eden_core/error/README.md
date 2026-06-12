# Eden Error Handling

## Overview

This guide covers the internal implementation of Eden's error handling system for developers working on the Eden codebase.

## Architecture

The error system uses **nested enums** with a hierarchical u16 error code structure:

```
Error Code (u16) = (main_code: u8 << 8) | (sub_code: u8)
```

### File Structure

```
eden_core/error/src/
├── lib.rs              # Main exports
├── ep.rs               # EpError main enum
├── db.rs               # DBError
├── common.rs           # Shared types
├── verification.rs     # Verification errors
└── types/
    ├── mod.rs          # Type exports
    ├── api.rs          # ApiError
    ├── auth.rs         # AuthError
    ├── cache.rs        # CacheError
    ├── connection.rs   # ConnectError
    ├── data.rs         # DataError
    ├── database.rs     # DatabaseError
    ├── fs.rs           # FsError
    ├── init.rs         # InitError
    ├── lock.rs         # LockError
    ├── tools.rs        # ToolsError
    ├── metadata.rs     # MetadataError
    ├── migration.rs    # MigrationError
    ├── parse.rs        # ParseError
    ├── rbac.rs         # RbacError
    ├── request.rs      # RequestError
    ├── serde.rs        # SerdeError
    ├── template.rs     # TemplateError
    ├── timeout.rs      # TimeoutError
    ├── transaction.rs  # TransactionError
    └── workflow.rs     # WorkflowError
```

---

## Usage Examples

### Creating Errors

```rust
use eden_error::{EpError, ResultEP};

// Using structured error constructors (recommended)
let error = EpError::database_user_not_found();  // E0A06
let error = EpError::invalid_credentials();      // E0801
let error = EpError::api_rate_limit_exceeded();  // E0102

// Using generic constructors with custom messages
let error = EpError::database("Connection pool exhausted");  // E0AFF
let error = EpError::auth("MFA token invalid");              // E08FF

// Direct variant construction
let error = EpError::Database(DatabaseError::UserNotFound);
let error = EpError::Auth(AuthError::TokenExpired);
```

### Retrieving Error Codes

```rust
let error = EpError::database_user_not_found();

// Get numeric error code
let code: u16 = error.error_code();  // 0x0A06 (2566 in decimal)

// Get hex string representation
let hex: String = error.error_hex();  // "E0A06"

// Display full error message
println!("{}", error);
// Output: [E0A06] Database error: User not found. Please verify the user ID is correct
```

### Helper Functions for Common Patterns

```rust
// Database query error detection
let result = EpError::database_query_error(
    "query returned an unexpected number of rows",
    EntityType::User
);
// Returns: EpError::Database(DatabaseError::UserNotFound)

// RBAC operation error
let result = EpError::rbac_operation_error(
    RbacErrorType::RuleNotFound,
    "delete_user"
);
// Returns: EpError::Rbac(RbacError::RuleNotFound)
```

### HTTP Status Code Conversion (Actix-Web)

```rust
use actix_web::{Error, HttpResponse};
use eden_error::EpError;

fn handler() -> Result<HttpResponse, Error> {
    let user = find_user("123")
        .map_err(|e: EpError| -> Error { e.into() })?;

    Ok(HttpResponse::Ok().json(user))
}
```

The error will automatically convert to the appropriate HTTP status code:
- `E0A06` (UserNotFound) → 404 Not Found
- `E0801` (InvalidCredentials) → 401 Unauthorized
- `E0905` (Unauthorized) → 403 Forbidden
- `E0A0B` (DuplicateUser) → 409 Conflict

---

## Best Practices

### ✅ Use Structured Errors When Possible

```rust
// Good - Provides consistent error code
EpError::database_user_not_found()  // E0A06

// Avoid - Uses generic Custom variant
EpError::database("User not found")  // E0AFF
```

Structured errors provide:
- Consistent error codes
- Type safety
- Better error matching
- Automatic logging

### ✅ Double-Wrapping Prevention

The system automatically prevents double-wrapping:

```rust
let original = EpError::database_user_not_found();  // E0A06
let wrapped = EpError::database(original);          // Still E0A06 (not E0AFF)
```

### ✅ Use Specific Error Types

```rust
// Too generic
EpError::api_internal_error()

// Better
EpError::database_connection_timeout()

// Best - Direct variant construction
EpError::Database(DatabaseError::ConnectionTimeout)
```

### ✅ Entity-Specific Database Errors

Use `database_query_error` for automatic entity-specific error detection:

```rust
// Automatically detects "not found" errors
let error = EpError::database_query_error(
    "query returned an unexpected number of rows",
    EntityType::Organization
);
// Returns: DatabaseError::OrganizationNotFound (E0A07)

// Automatically detects duplicate errors
let error = EpError::database_query_error(
    "duplicate key value violates unique constraint",
    EntityType::User
);
// Returns: DatabaseError::DuplicateUser (E0A0B)

// Detects connection errors
let error = EpError::database_query_error(
    "connection timeout",
    EntityType::User
);
// Returns: DatabaseError::ConnectionTimeout (E0A01)

// Detects auth errors
let error = EpError::database_query_error(
    "authentication failed",
    EntityType::User
);
// Returns: DatabaseError::AuthenticationFailed (E0A02)

// Detects schema errors
let error = EpError::database_query_error(
    "relation does not exist",
    EntityType::User
);
// Returns: DatabaseError::SchemaError (E0A03)
```

---

## Adding New Error Types

### Step 1: Define the Nested Enum

Create a new file in `eden_core/error/src/types/` (e.g., `mynew.rs`):

```rust
use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum MyNewError {
    SomeError,      // 0x01
    AnotherError,   // 0x02
    Custom(String), // 0xFF
}

impl MyNewError {
    pub fn error_code(&self) -> u8 {
        match self {
            MyNewError::SomeError => 0x01,
            MyNewError::AnotherError => 0x02,
            MyNewError::Custom(_) => 0xFF,
        }
    }
}

impl fmt::Display for MyNewError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let message = match self {
            MyNewError::SomeError => "Some error occurred",
            MyNewError::AnotherError => "Another error occurred",
            MyNewError::Custom(msg) => return write!(f, "{}", msg),
        };
        write!(f, "{}", message)
    }
}
```

### Step 2: Add to Main EpError Enum

In `eden_core/error/src/ep.rs`:

```rust
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub enum EpError {
    // ... existing variants
    MyNew(MyNewError),  // 0x15 (next available code)
}

impl EpError {
    pub fn error_code(&self) -> u16 {
        let main_code: u8 = match self {
            // ... existing matches
            EpError::MyNew(_) => 0x15,
        };

        let sub_code: u8 = match self {
            // ... existing matches
            EpError::MyNew(err) => err.error_code(),
        };

        (main_code as u16) << 8 | sub_code as u16
    }
}

impl fmt::Display for EpError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let (category, message) = match self {
            // ... existing matches
            EpError::MyNew(err) => ("MyNew", err.to_string()),
        };

        write!(f, "[{}] {} error: {}", self.error_hex(), category, message)
    }
}
```

### Step 3: Add Constructor Methods

In `eden_core/error/src/ep.rs`:

```rust
impl EpError {
    // Structured constructor
    pub fn mynew_some_error() -> Self {
        let error = EpError::MyNew(MyNewError::SomeError);
        log::error!("{}", error);
        error
    }

    // Generic constructor
    pub fn mynew<E: std::fmt::Display + 'static>(message: E) -> Self {
        if let Some(ep_error) = (&message as &dyn Any).downcast_ref::<EpError>() {
            return ep_error.clone();
        }

        let error = EpError::MyNew(MyNewError::Custom(message.to_string()));
        log::error!("{}", error);
        error
    }
}
```

### Step 4: Add HTTP Status Mapping (if needed)

In `eden_core/error/src/ep.rs`:

```rust
impl From<EpError> for actix_web::error::Error {
    fn from(error: EpError) -> Self {
        use actix_web::error::*;

        match &error {
            // ... existing matches
            EpError::MyNew(_) => ErrorInternalServerError(error),
        }
    }
}
```

---

## Performance Considerations

### Error Creation Cost
- Structured errors: Very cheap (enum construction)
- Custom errors: Slightly more expensive (String allocation)
