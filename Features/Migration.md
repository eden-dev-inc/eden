# Migrations Implementation Guide

Migrations in Eden provide a robust system for managing database schema changes and API updates with rollback capabilities. This guide provides detailed implementation instructions for creating, managing, and executing migrations safely in your production environment.

## What Are Migrations?

Migrations are coordinated schema and data transformation operations that:
- Group multiple APIs under a single migration unit
- Provide atomic commit/rollback capabilities across multiple APIs
- Use distributed locking to prevent concurrent migration conflicts
- Track migration state and progress
- Enable safe deployment of database schema changes

## Core Migration Concepts

### Migration States
- **Pending**: Migration created but not started
- **In Progress**: Migration is currently executing
- **Completed**: Migration has been successfully committed
- **Failed**: Migration encountered errors and may need rollback

### Migration Components
1. **Migration Schema**: The main migration configuration
2. **API Collection**: Set of APIs that participate in the migration
3. **Locking Mechanism**: Prevents concurrent migration execution
4. **State Management**: Tracks progress and enables rollback

## Creating a Migration

### Step 1: Define Migration Schema

```json
{
  "id": "customer_schema_v2",
  "description": "Migrate customer table to new schema with additional fields"
}
```

### Step 2: HTTP Request to Create Migration

```http
POST /api/v1/migrations
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "id": "customer_schema_v2",
  "description": "Migrate customer table to new schema with additional fields"
}
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

**What Happens:**
1. Migration schema is created with empty API list
2. Distributed lock is initialized for the migration
3. Migration is ready to have APIs added

## Adding APIs to Migrations

### Step 1: Create API Migration Configuration

```json
{
  "id": "existing_api_id",
  "bindings": [
    {
      "template": "backup_customer_data",
      "fields": {
        "source_table": "customers",
        "backup_table": "customers_backup_v1",
        "timestamp": "migration.started_at"
      }
    },
    {
      "template": "transform_customer_schema",
      "fields": {
        "source_table": "customers", 
        "target_table": "customers_v2",
        "batch_size": "migration.batch_size"
      }
    },
    {
      "template": "verify_data_integrity",
      "fields": {
        "source_count": "migration.source_record_count",
        "target_count": "migration.target_record_count"
      }
    }
  ]
}
```

### Step 2: Add API to Migration

```http
POST /api/v1/migrations/customer_schema_v2/add_api
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "id": "customer_data_migration_api",
  "bindings": [
    {
      "template": "backup_customer_data",
      "fields": {
        "source_table": "customers",
        "backup_table": "customers_backup_v1"
      }
    },
    {
      "template": "transform_customer_schema", 
      "fields": {
        "source_table": "customers",
        "target_table": "customers_v2"
      }
    }
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "added Api to migration"
}
```

**What Happens:**
1. System validates the API exists and doesn't already have a migration
2. Migration bindings are resolved (template IDs → UUIDs)
3. API schema is updated with migration configuration
4. Migration schema is updated to include the API UUID

### Important Constraints:
- APIs can only belong to one migration at a time
- All referenced templates must exist in your organization
- Migration bindings are separate from regular API bindings

## Retrieving Migration Information

### Basic Migration Details

```http
GET /api/v1/migrations/customer_schema_v2
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "customer_schema_v2",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Migrate customer table to new schema with additional fields",
    "state": "Pending",
    "api_uuids": [
      "550e8400-e29b-41d4-a716-446655440001",
      "550e8400-e29b-41d4-a716-446655440002"
    ],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

### Verbose Migration Details (with API schemas)

```http
GET /api/v1/migrations/customer_schema_v2
Authorization: Bearer your_jwt_token
X-Eden-Verbose: true
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "customer_schema_v2",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Migrate customer table to new schema with additional fields",
    "state": "Pending",
    "apis": [
      {
        "id": "customer_data_migration_api",
        "uuid": "550e8400-e29b-41d4-a716-446655440001",
        "fields": [...],
        "bindings": [...],
        "migration": {
          "uuid": "550e8400-e29b-41d4-a716-446655440000",
          "state": false,
          "bindings": [
            {
              "template": "550e8400-e29b-41d4-a716-446655440003",
              "fields": {
                "source_table": "customers",
                "backup_table": "customers_backup_v1"
              }
            }
          ]
        },
        "created_at": "2024-01-15T10:30:00Z",
        "updated_at": "2024-01-15T10:30:00Z"
      }
    ],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Migration Execution

### Step 1: Sync Migration State

Before executing, sync the migration state to prepare all APIs:

```http
POST /api/v1/migrations/customer_schema_v2/sync
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "message": "migration sync started"
}
```

**What Sync Does:**
1. Acquires write lock on the migration
2. Updates migration state on all associated APIs in parallel
3. Prepares APIs for execution by updating their internal state
4. Validates all APIs are ready for migration

### Step 2: Execute Migration

```http
POST /api/v1/migrations/customer_schema_v2/migrate
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success", 
  "message": "migration commit completed"
}
```

**What Migration Does:**
1. Acquires write lock to prevent concurrent migrations
2. Executes `commit_migration()` on all APIs in parallel using JoinSet
3. Each API commits its migration configuration permanently
4. Updates migration state to completed
5. Releases migration lock

### Migration Execution Flow

1. **Lock Acquisition**: Write lock prevents other migrations
2. **Parallel Execution**: All APIs execute simultaneously for speed
3. **Atomic Commit**: Either all APIs succeed or all fail
4. **State Update**: Migration marked as completed
5. **Lock Release**: Other migrations can proceed

## Advanced Migration Patterns

### Multi-Stage Migration

```json
{
  "id": "multi_stage_customer_migration",
  "description": "Complex customer migration with backup and validation"
}
```

**Stage 1: Backup**
```http
POST /api/v1/migrations/multi_stage_customer_migration/add_api

{
  "id": "backup_stage_api",
  "bindings": [
    {
      "template": "create_backup_table",
      "fields": {
        "source": "customers",
        "backup": "customers_backup_20240115"
      }
    },
    {
      "template": "copy_data_to_backup",
      "fields": {
        "source": "customers",
        "destination": "customers_backup_20240115"
      }
    }
  ]
}
```

**Stage 2: Schema Changes**
```http
POST /api/v1/migrations/multi_stage_customer_migration/add_api

{
  "id": "schema_change_api",
  "bindings": [
    {
      "template": "add_new_columns",
      "fields": {
        "table": "customers",
        "columns": "email_verified, phone_verified, created_by"
      }
    },
    {
      "template": "create_indexes",
      "fields": {
        "table": "customers",
        "indexes": "idx_email_verified, idx_phone_verified"
      }
    }
  ]
}
```

**Stage 3: Data Migration**
```http
POST /api/v1/migrations/multi_stage_customer_migration/add_api

{
  "id": "data_migration_api", 
  "bindings": [
    {
      "template": "migrate_customer_data",
      "fields": {
        "batch_size": "1000",
        "source_table": "customers",
        "transformation_rules": "migration.rules"
      }
    },
    {
      "template": "validate_migrated_data",
      "fields": {
        "table": "customers",
        "validation_queries": "migration.validation_sql"
      }
    }
  ]
}
```

### Rollback Migration

Create a separate migration for rollback:

```json
{
  "id": "rollback_customer_schema_v2",
  "description": "Rollback customer schema migration if needed"
}
```

```http
POST /api/v1/migrations/rollback_customer_schema_v2/add_api

{
  "id": "rollback_api",
  "bindings": [
    {
      "template": "restore_from_backup",
      "fields": {
        "backup_table": "customers_backup_v1",
        "target_table": "customers"
      }
    },
    {
      "template": "drop_new_columns", 
      "fields": {
        "table": "customers",
        "columns": "new_field1, new_field2"
      }
    }
  ]
}
```

## Migration Locking Implementation

### How Locking Works

1. **Lock Creation**: When migration is created, a RwLock is added to DashMap
2. **Read Lock**: Regular API execution acquires read lock
3. **Write Lock**: Migration operations acquire write lock
4. **Lock Cleanup**: Locks are removed when migration is deleted

### Lock Behavior

```rust
// Read lock (allows concurrent API execution)
let _migration_guard = if let Some(ref lock_arc) = migration_lock_arc {
    Some(lock_arc.read().await)
} else {
    None
};

// Write lock (exclusive migration execution) 
let _migration_guard = if let Some(ref lock_arc) = migration_lock_arc {
    Some(lock_arc.write().await)
} else {
    None
};
```

### Concurrent Execution Rules

- **API Execution**: Can run concurrently (read locks)
- **Migration Sync**: Exclusive access (write lock)
- **Migration Execution**: Exclusive access (write lock)
- **Multiple Migrations**: Cannot run simultaneously on same APIs

## Error Handling & Recovery

### Common Error Scenarios

1. **API Already Has Migration**
```json
{
  "error": "Api Schema already has an active migration",
  "details": "Each API can only participate in one migration at a time"
}
```

2. **Template Not Found**
```json
{
  "error": "Template not found",
  "template_id": "nonexistent_template",
  "suggestion": "Verify template exists in your organization"
}
```

3. **Migration Lock Timeout**
```json
{
  "error": "Migration lock acquisition timeout", 
  "migration_id": "customer_schema_v2",
  "suggestion": "Another migration may be in progress"
}
```

4. **Parallel Execution Failure**
```json
{
  "error": "Migration execution failed",
  "failed_apis": ["api_uuid_1", "api_uuid_2"],
  "details": "One or more APIs failed during parallel execution"
}
```

### Recovery Strategies

1. **Check Migration State**: Use GET endpoint to verify current status
2. **Retry Sync**: Run sync operation again if it failed
3. **Investigate Failures**: Check individual API execution logs
4. **Manual Intervention**: May need to fix data issues manually
5. **Rollback Migration**: Create and execute rollback migration

## Deleting Migrations

### Remove Migration

```http
DELETE /api/v1/migrations/customer_schema_v2
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "message": "success"
}
```

**What Happens:**
1. RBAC verification (Admin access required)
2. Migration UUID is retrieved for lock cleanup
3. Migration schema is deleted from database
4. Migration lock is removed from memory

**Important Notes:**
- Associated APIs retain their migration configuration
- Only removes the migration coordination mechanism
- APIs can still be executed individually
- Consider running rollback before deletion

## Best Practices for Migration Implementation

### 1. Planning & Design
- **Create Backups**: Always backup data before schema changes
- **Test Migrations**: Run in staging environment first
- **Batch Processing**: Use reasonable batch sizes for large datasets
- **Validation**: Include data validation steps in migration templates
- **Rollback Plan**: Design rollback migration before executing forward migration

### 2. Migration Execution
- **Off-Peak Hours**: Run migrations during low-traffic periods
- **Monitor Progress**: Watch system metrics during execution
- **Staged Approach**: Break complex migrations into smaller stages
- **Verification**: Validate results after each stage
- **Communication**: Notify stakeholders of migration schedules

### 3. Template Design for Migrations
- **Idempotent Operations**: Templates should be safe to run multiple times
- **Error Handling**: Include comprehensive error checking
- **Progress Logging**: Log migration progress for monitoring
- **Checkpoints**: Create checkpoints for long-running operations
- **Cleanup**: Include cleanup operations for temporary resources

### 4. Error Handling
- **Timeout Handling**: Set appropriate timeouts for operations
- **Retry Logic**: Implement retries for transient failures
- **Graceful Degradation**: Handle partial failures appropriately
- **Detailed Logging**: Log detailed error information for debugging
- **Alerting**: Set up alerts for migration failures

### 5. Testing Strategy
- **Unit Tests**: Test individual migration templates
- **Integration Tests**: Test complete migration flows
- **Load Tests**: Verify performance with production data volumes
- **Rollback Tests**: Test rollback procedures regularly
- **Disaster Recovery**: Test recovery from various failure scenarios

### 6. Security Considerations
- **Access Control**: Restrict migration access to authorized users
- **Data Validation**: Validate all input data during migration
- **Audit Trails**: Maintain detailed logs of migration activities
- **Backup Security**: Secure backup data appropriately
- **Environment Isolation**: Use separate environments for testing

## Monitoring Migration Progress

### Key Metrics to Monitor
- **Lock Wait Times**: How long operations wait for locks
- **Parallel Execution Time**: Duration of parallel API execution
- **Template Execution Time**: Individual template performance
- **Error Rates**: Frequency of migration failures
- **Data Consistency**: Validation of migrated data

### Troubleshooting Common Issues

#### Migration Won't Start
1. Check if another migration is running
2. Verify all APIs are accessible
3. Confirm templates exist and are valid
4. Check RBAC permissions

#### Migration Hangs
1. Check for deadlocks in database
2. Verify network connectivity
3. Look for resource contention
4. Check template execution logs

#### Partial Migration Success
1. Identify which APIs failed
2. Check individual template logs
3. Verify data consistency
4. Decide on retry vs rollback

#### Performance Issues
1. Monitor database performance
2. Check batch sizes in templates
3. Verify resource allocation
4. Consider migration timing

This implementation guide provides the comprehensive information needed to successfully implement and manage migrations in your Eden environment, ensuring safe and reliable database schema changes.