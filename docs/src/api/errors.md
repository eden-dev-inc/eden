# Eden API Error Codes

## Overview

When you interact with Eden's API, errors are returned with standardized error codes to help you quickly identify and resolve issues.

**Error Format:**

```
[{error_code}] {category} error: {message}
```

**Example:**

```
[E0A06] Database error: User not found. Please verify the user ID is correct
```

### Custom Error Codes

In addition to the standardized error codes, you may occasionally receive errors ending in `FF` (e.g., `E01FF`, `E0AFF`). These are **custom error codes** that provide context-specific error messages for situations that don't fit into the standard error categories.

**Example Custom Error:**

```
[E0AFF] Database error: {error details here}
```

---

## Error Categories

| Category    | Code Range | Description                          |
| ----------- | ---------- | ------------------------------------ |
| API         | `E01__`    | General API errors                   |
| Init        | `E02__`    | Initialization errors                |
| Transaction | `E03__`    | Transaction errors                   |
| Request     | `E04__`    | Request validation errors            |
| Connect     | `E05__`    | Connection errors                    |
| Serde       | `E06__`    | Serialization/deserialization errors |
| Cache       | `E07__`    | Cache-related errors                 |
| Auth        | `E08__`    | Authentication errors                |
| Rbac        | `E09__`    | Role-based access control errors     |
| Database    | `E0A__`    | Database errors                      |
| Metadata    | `E0B__`    | Metadata errors                      |
| Parse       | `E0C__`    | Parsing errors                       |
| Lock        | `E0D__`    | Lock/concurrency errors              |
| Migration   | `E0E__`    | Migration errors                     |
| Fs          | `E0F__`    | File system errors                   |
| Data        | `E10__`    | Data validation errors               |
| Timeout     | `E11__`    | Timeout errors                       |
| Mcp         | `E12__`    | MCP protocol errors                  |
| Template    | `E13__`    | Template errors                      |
| Workflow    | `E14__`    | Workflow errors                      |

---

## Complete Error Code Reference

### E01\_\_ - API Errors

| Code      | Error              | HTTP Status | Description                                             |
| --------- | ------------------ | ----------- | ------------------------------------------------------- |
| **E0101** | InvalidRequest     | 400         | Invalid request format or parameters                    |
| **E0102** | RateLimitExceeded  | 429         | Rate limit exceeded. Please slow down your requests     |
| **E0103** | ServiceUnavailable | 503         | Service temporarily unavailable. Please try again later |
| **E0104** | InvalidInput       | 400         | Invalid input provided. Please check your data          |
| **E0105** | InternalError      | 500         | Internal API error occurred                             |

---

### E02\_\_ - Initialization Errors

| Code      | Error                | HTTP Status | Description                                                      |
| --------- | -------------------- | ----------- | ---------------------------------------------------------------- |
| **E0201** | ConfigurationMissing | 500         | Required configuration is missing. Please check your settings    |
| **E0202** | DatabaseInitFailed   | 500         | Database initialization failed. Please check database connection |
| **E0203** | ServiceStartupFailed | 500         | Service failed to start properly. Please check logs              |
| **E0204** | DependencyMissing    | 500         | Required dependency is missing or unavailable                    |
| **E0205** | PermissionDenied     | 500         | Permission denied during initialization                          |

---

### E03\_\_ - Transaction Errors

| Code      | Error                         | HTTP Status | Description                                         |
| --------- | ----------------------------- | ----------- | --------------------------------------------------- |
| **E0301** | BeginFailed                   | 500         | Failed to begin database transaction                |
| **E0302** | CommitFailed                  | 500         | Failed to commit database transaction               |
| **E0303** | RollbackFailed                | 500         | Failed to rollback database transaction             |
| **E0304** | DeadlockDetected              | 500         | Database deadlock detected. Transaction was aborted |
| **E0305** | TimeoutExceeded               | 500         | Transaction timeout exceeded                        |
| **E0306** | ChannelFailure                | 500         | Channel failure                                     |
| **E0307** | Rollback                      | 500         | Rollback                                            |
| **E0308** | FailedToDowncast              | 500         | Failed to downcast transaction                      |
| **E0309** | NotImplemented                | 500         | Not implemented                                     |
| **E030A** | TransactionsNotImplemented    | 500         | Transactions are not implemented                    |
| **E030B** | PrepareCannotRunInTransaction | 500         | Prepare cannot run in a transaction                 |
| **E030C** | FailedToCollectApprovals      | 500         | Failed to collect valid approvals                   |
| **E030D** | NothingReceived               | 500         | Nothing received                                    |

---

### E04\_\_ - Request Errors

| Code      | Error                     | HTTP Status | Description                                                    |
| --------- | ------------------------- | ----------- | -------------------------------------------------------------- |
| **E0401** | InvalidFormat             | 400         | Request format is invalid. Please check your request structure |
| **E0402** | MissingParameters         | 400         | Required parameters are missing from the request               |
| **E0403** | InvalidParameters         | 400         | One or more parameters are invalid                             |
| **E0404** | PayloadTooLarge           | 400         | Request payload is too large. Please reduce the size           |
| **E0405** | UnsupportedMethod         | 400         | HTTP method is not supported for this endpoint                 |
| **E0406** | FailedToUnwrapResponse    | 400         | Failed to unwrap response                                      |
| **E0407** | FailedToEncodeRequest     | 400         | Failed to encode request                                       |
| **E0408** | ErrorOutputNotImplemented | 400         | Error output not yet implemented                               |

---

### E05\_\_ - Connection Errors

| Code      | Error                   | HTTP Status | Description                                            |
| --------- | ----------------------- | ----------- | ------------------------------------------------------ |
| **E0501** | ConnectionRefused       | 500         | Connection was refused by the target host              |
| **E0502** | NetworkUnreachable      | 500         | Network is unreachable. Please check your connectivity |
| **E0503** | TimeoutReached          | 500         | Connection timeout reached                             |
| **E0504** | SslHandshakeFailed      | 500         | SSL/TLS handshake failed. Please check certificates    |
| **E0505** | ProtocolMismatch        | 500         | Protocol version mismatch detected                     |
| **E0506** | ConnectionNotFound      | 500         | Could not find connection                              |
| **E0507** | FailedToDowncastConfig  | 500         | Failed to downcast config                              |
| **E0508** | FailedToDowncastRouter  | 500         | Failed to downcast router                              |
| **E0509** | FailedToDowncastRequest | 500         | Failed to downcast request                             |
| **E050A** | CouldNotGetConnection   | 500         | Could not get connection                               |
| **E050B** | CouldNotGetEndpoint     | 500         | Could not get endpoint                                 |
| **E050C** | SyncConnectionNotExist  | 500         | Sync connection does not exist                         |
| **E050D** | IncorrectPoolFormat     | 500         | Incorrect pool format: sync                            |
| **E050E** | InvalidHeaderName       | 500         | Invalid header name                                    |
| **E050F** | InvalidHeaderValue      | 500         | Invalid header value                                   |

---

### E06\_\_ - Serialization Errors

| Code      | Error                 | HTTP Status | Description                                      |
| --------- | --------------------- | ----------- | ------------------------------------------------ |
| **E0601** | SerializationFailed   | 400         | Failed to serialize data to required format      |
| **E0602** | DeserializationFailed | 400         | Failed to deserialize data from source format    |
| **E0603** | InvalidFormat         | 400         | Data format is invalid or unsupported            |
| **E0604** | MissingField          | 400         | Required field is missing from data structure    |
| **E0605** | TypeMismatch          | 400         | Data type mismatch encountered during processing |
| **E0606** | InvalidRequest        | 400         | Invalid request                                  |
| **E0607** | FailedToParseRow      | 400         | Failed to parse row                              |
| **E0608** | ExpectedJsonObject    | 400         | Expected a JSON object                           |
| **E0609** | BorshNotImplemented   | 400         | Borsh serialize not implemented for Client       |
| **E060A** | ModelNotString        | 400         | Passed model is not a string                     |
| **E060B** | MaxTokensNotNumber    | 400         | Passed max tokens is not a number                |
| **E060C** | UrlNotString          | 400         | Passed URL is not a string                       |
| **E060D** | ApiKeyNotString       | 400         | Passed API key is not a string                   |

---

### E07\_\_ - Cache Errors

| Code      | Error            | HTTP Status | Description                              |
| --------- | ---------------- | ----------- | ---------------------------------------- |
| **E0701** | KeyNotFound      | 404         | Requested key was not found in cache     |
| **E0702** | ConnectionLost   | 500         | Lost connection to cache server          |
| **E0703** | MemoryExhausted  | 500         | Cache memory limit exceeded              |
| **E0704** | ExpirationFailed | 500         | Failed to set or update cache expiration |
| **E0705** | InvalidKey       | 500         | Cache key format is invalid              |
| **E0706** | NoKeyProvided    | 500         | No key provided                          |

---

### E08\_\_ - Authentication Errors

| Code      | Error                   | HTTP Status | Description                                                                |
| --------- | ----------------------- | ----------- | -------------------------------------------------------------------------- |
| **E0801** | InvalidCredentials      | 401         | Invalid username or password. Please verify your credentials               |
| **E0802** | InvalidApiKey           | 401         | Invalid API key. Please verify your API key is correct and has not expired |
| **E0803** | TokenExpired            | 401         | Authentication token has expired. Please log in again                      |
| **E0804** | TokenMalformed          | 401         | Authentication token is invalid                                            |
| **E0805** | InsufficientPermissions | 401         | You do not have sufficient permissions for this operation                  |
| **E0806** | SessionExpired          | 401         | Your session has expired. Please log in again                              |

---

### E09\_\_ - RBAC Errors

| Code      | Error              | HTTP Status | Description                                                                      |
| --------- | ------------------ | ----------- | -------------------------------------------------------------------------------- |
| **E0901** | RuleNotFound       | 403         | Access control rule not found. Please verify the entity and subject exist        |
| **E0902** | InvalidAccessLevel | 403         | Invalid access level specified. Valid levels are: Read, Write, Admin, SuperAdmin |
| **E0903** | ConnectionFailure  | 403         | Connection timeout to RBAC cache. Please check network connectivity              |
| **E0904** | PermissionDenied   | 403         | Access denied due to insufficient permissions                                    |
| **E0905** | EntityNotFound     | 403         | Referenced entity not found in access control system                             |
| **E0906** | Unauthorized       | 403         | Unauthorized                                                                     |

---

### E0A\_\_ - Database Errors

| Code      | Error                 | HTTP Status | Description                                                                  |
| --------- | --------------------- | ----------- | ---------------------------------------------------------------------------- |
| **E0A01** | ConnectionTimeout     | 500         | Database connection timeout. Please check network connectivity and try again |
| **E0A02** | AuthenticationFailed  | 500         | Database authentication failed. Please check your database credentials       |
| **E0A03** | SchemaError           | 500         | Database schema error. Please ensure the database is properly initialized    |
| **E0A04** | QueryFailed           | 500         | Database query execution failed                                              |
| **E0A05** | TransactionFailed     | 500         | Database transaction failed                                                  |
| **E0A06** | UserNotFound          | 404         | User not found. Please verify the user ID is correct                         |
| **E0A07** | OrganizationNotFound  | 404         | Organization not found. Please verify the organization ID is correct         |
| **E0A08** | EndpointNotFound      | 404         | Endpoint not found. Please verify the endpoint ID is correct                 |
| **E0A09** | TemplateNotFound      | 404         | Template not found. Please verify the template ID is correct                 |
| **E0A0A** | WorkflowNotFound      | 404         | Workflow not found. Please verify the workflow ID is correct                 |
| **E0A0B** | DuplicateUser         | 409         | User already exists. Please choose a different username                      |
| **E0A0C** | DuplicateOrganization | 409         | Organization already exists. Please choose a different name                  |
| **E0A0D** | DuplicateEndpoint     | 409         | Endpoint already exists. Please choose a different identifier                |
| **E0A0E** | DuplicateTemplate     | 409         | Template already exists. Please choose a different identifier                |
| **E0A0F** | DuplicateWorkflow     | 409         | Workflow already exists. Please choose a different identifier                |
| **E0A10** | ConstraintViolation   | 409         | Database constraint violation. Operation violates data integrity rules       |
| **E0A11** | IndexCorruption       | 500         | Database index corruption detected. Please contact system administrator      |
| **E0A12** | FeatureNotEnabled     | 500         | Database feature not enabled. Contact administrator                          |

---

### E0B\_\_ - Metadata Errors

| Code      | Error                  | HTTP Status | Description                               |
| --------- | ---------------------- | ----------- | ----------------------------------------- |
| **E0B01** | InvalidFormat          | 500         | Metadata format is invalid or unsupported |
| **E0B02** | MissingRequired        | 500         | Required metadata fields are missing      |
| **E0B03** | CorruptedData          | 500         | Metadata is corrupted or unreadable       |
| **E0B04** | VersionMismatch        | 500         | Metadata version mismatch detected        |
| **E0B05** | AccessDenied           | 500         | Access denied to metadata resource        |
| **E0B06** | FailedToDowncastRouter | 500         | Failed to downcast router                 |
| **E0B07** | QueryTimeout           | 408         | Query timeout exceeded                    |

---

### E0C\_\_ - Parse Errors

| Code      | Error                    | HTTP Status | Description                               |
| --------- | ------------------------ | ----------- | ----------------------------------------- |
| **E0C01** | InvalidSyntax            | 400         | Invalid syntax encountered during parsing |
| **E0C02** | UnexpectedToken          | 400         | Unexpected token found in input           |
| **E0C03** | InvalidEncoding          | 400         | Invalid character encoding detected       |
| **E0C04** | IncompleteData           | 400         | Input data is incomplete or truncated     |
| **E0C05** | FormatNotSupported       | 400         | Data format is not supported              |
| **E0C06** | FailedToParseMetadata    | 400         | Failed to parse metadata                  |
| **E0C07** | FailedToDowncastMetadata | 400         | Failed to downcast metadata               |
| **E0C08** | FailedToDowncastInput    | 400         | Failed to downcast input                  |
| **E0C09** | InvalidDatabaseUsername  | 400         | Invalid database username                 |
| **E0C0A** | FailedToAddRbacKey       | 400         | Failed to add RbacKey to Rbac Entity      |

---

### E0D\_\_ - Lock Errors

| Code      | Error             | HTTP Status | Description                                       |
| --------- | ----------------- | ----------- | ------------------------------------------------- |
| **E0D01** | AlreadyLocked     | 500         | Resource is already locked by another process     |
| **E0D02** | TimeoutReached    | 500         | Lock acquisition timeout reached                  |
| **E0D03** | DeadlockDetected  | 500         | Deadlock detected in lock acquisition             |
| **E0D04** | InvalidLockState  | 500         | Lock is in an invalid state                       |
| **E0D05** | OwnershipMismatch | 500         | Lock ownership mismatch. Cannot perform operation |

---

### E0E\_\_ - Migration Errors

| Code      | Error                 | HTTP Status | Description                                    |
| --------- | --------------------- | ----------- | ---------------------------------------------- |
| **E0E01** | SchemaVersionMismatch | 500         | Database schema version mismatch detected      |
| **E0E02** | MigrationFailed       | 500         | Database migration failed to execute           |
| **E0E03** | RollbackFailed        | 500         | Failed to rollback database migration          |
| **E0E04** | InvalidMigration      | 422         | Migration script is invalid or corrupted       |
| **E0E05** | DependencyMissing     | 500         | Migration dependency is missing or unavailable |
| **E0E06** | NoMigrationConfigured | 500         | No migration configured                        |

---

### E0F\_\_ - File System Errors

| Code      | Error                  | HTTP Status | Description                                                |
| --------- | ---------------------- | ----------- | ---------------------------------------------------------- |
| **E0F01** | FileNotFound           | 404         | File or directory not found                                |
| **E0F02** | PermissionDenied       | 500         | Permission denied. Please check file/directory permissions |
| **E0F03** | DiskFull               | 500         | Disk is full. Please free up space and try again           |
| **E0F04** | InvalidPath            | 500         | File path is invalid or contains illegal characters        |
| **E0F05** | IoError                | 500         | Input/output error occurred during file operation          |
| **E0F06** | OrganizationIdEmpty    | 500         | Organization ID cannot be empty                            |
| **E0F07** | InfoMustBeJsonObject   | 500         | Info must be a valid JSON object                           |
| **E0F08** | NodeMustHaveEndpoint   | 500         | Eden Node must have at least one endpoint                  |
| **E0F09** | NodeIdEmpty            | 500         | Eden Node ID cannot be empty                               |
| **E0F0A** | DuplicateEndpointUuids | 500         | Duplicate endpoint UUIDs are not allowed                   |

---

### E10\_\_ - Data Errors

| Code      | Error            | HTTP Status | Description                                     |
| --------- | ---------------- | ----------- | ----------------------------------------------- |
| **E1001** | CorruptedData    | 400         | Data is corrupted or unreadable                 |
| **E1002** | InvalidFormat    | 400         | Data format is invalid or unsupported           |
| **E1003** | MissingRequired  | 400         | Required data fields are missing                |
| **E1004** | ValidationFailed | 400         | Data validation failed. Please check your input |
| **E1005** | ConversionFailed | 400         | Failed to convert data to required format       |

---

### E11\_\_ - Timeout Errors

| Code      | Error             | HTTP Status | Description                                |
| --------- | ----------------- | ----------- | ------------------------------------------ |
| **E1101** | RequestTimeout    | 408         | Request timeout exceeded. Please try again |
| **E1102** | ConnectionTimeout | 408         | Connection timeout reached                 |
| **E1103** | ReadTimeout       | 408         | Read operation timed out                   |
| **E1104** | WriteTimeout      | 408         | Write operation timed out                  |
| **E1105** | ProcessTimeout    | 408         | Process execution timed out                |

---

### E12\_\_ - MCP Errors

| Code      | Error                     | HTTP Status | Description                        |
| --------- | ------------------------- | ----------- | ---------------------------------- |
| **E1201** | ConnectionFailed          | 500         | Failed to establish MCP connection |
| **E1202** | ProtocolError             | 500         | MCP protocol error encountered     |
| **E1203** | InvalidMessage            | 500         | Invalid MCP message format         |
| **E1204** | TimeoutError              | 500         | MCP operation timed out            |
| **E1205** | AuthenticationError       | 500         | MCP authentication failed          |
| **E1206** | UnknownTool               | 500         | Unknown tool                       |
| **E1207** | ErrorReadingResult        | 500         | Error reading result               |
| **E1208** | ErrorPreparingRequestBody | 500         | Error preparing request body       |
| **E1209** | ErrorInRequestToRelay     | 500         | Error in request to relay          |
| **E120A** | InvalidToolArguments      | 500         | Invalid tool arguments             |

---

### E13\_\_ - Template Errors

| Code      | Error             | HTTP Status | Description                                                  |
| --------- | ----------------- | ----------- | ------------------------------------------------------------ |
| **E1301** | TemplateNotFound  | 400         | Template not found. Please verify the template ID is correct |
| **E1302** | CompilationFailed | 400         | Template compilation failed. Please check template syntax    |
| **E1303** | RenderingFailed   | 400         | Template rendering failed. Please check template variables   |
| **E1304** | InvalidSyntax     | 400         | Invalid template syntax detected                             |
| **E1305** | VariableMissing   | 400         | Required template variable is missing                        |

---

### E14\_\_ - Workflow Errors

| Code      | Error             | HTTP Status | Description                                                  |
| --------- | ----------------- | ----------- | ------------------------------------------------------------ |
| **E1401** | WorkflowNotFound  | 404         | Workflow not found. Please verify the workflow ID is correct |
| **E1402** | ExecutionFailed   | 500         | Workflow execution failed                                    |
| **E1403** | InvalidDefinition | 422         | Workflow definition is invalid or corrupted                  |
| **E1404** | StepFailed        | 500         | Workflow step failed to execute                              |
| **E1405** | TimeoutExceeded   | 500         | Workflow execution timeout exceeded                          |
| **E1406** | NoInputsProvided  | 500         | No inputs provided                                           |
| **E1407** | CycleDetected     | 422         | Cycle detected in DAG                                        |
| **E1408** | ChannelSendError  | 500         | Channel send error                                           |

---

## Quick Reference

| Category              | Count | Most Common         |
| --------------------- | ----- | ------------------- |
| API (E01\_\_)         | 5     | E0101, E0102, E0103 |
| Init (E02\_\_)        | 5     | E0201, E0202        |
| Transaction (E03\_\_) | 13    | E0302, E0304        |
| Request (E04\_\_)     | 8     | E0401, E0402, E0403 |
| Connect (E05\_\_)     | 15    | E0501, E0502, E0503 |
| Serde (E06\_\_)       | 13    | E0601, E0602, E0603 |
| Cache (E07\_\_)       | 6     | E0701, E0702        |
| Auth (E08\_\_)        | 6     | E0801, E0802, E0803 |
| Rbac (E09\_\_)        | 6     | E0904, E0906        |
| Database (E0A\_\_)    | 18    | E0A06, E0A07, E0A0B |
| Metadata (E0B\_\_)    | 7     | E0B01, E0B07        |
| Parse (E0C\_\_)       | 10    | E0C01, E0C02        |
| Lock (E0D\_\_)        | 5     | E0D01, E0D02        |
| Migration (E0E\_\_)   | 6     | E0E02, E0E04        |
| Fs (E0F\_\_)          | 10    | E0F01, E0F02        |
| Data (E10\_\_)        | 5     | E1001, E1004        |
| Timeout (E11\_\_)     | 5     | E1101, E1102        |
| Mcp (E12\_\_)         | 10    | E1201, E1206        |
| Template (E13\_\_)    | 5     | E1301, E1302, E1305 |
| Workflow (E14\_\_)    | 8     | E1401, E1402, E1403 |
