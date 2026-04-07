# Migrations

Eden-MDBS provides a powerful migration system for safely moving traffic and data between endpoints. This enables zero-downtime database migrations, A/B testing, and gradual rollouts.

## Overview

Migrations in Eden-MDBS orchestrate the transition from one endpoint to another. The system supports:

- **Traffic routing**: Control how read/write requests are distributed during migration
- **Data movement**: Automatically copy historical data from source to target
- **Multiple strategies**: Choose the rollout pattern that fits your needs
- **Failure handling**: Define how errors are managed during migration

## Migration Concepts

### APIs and Interlays

Migrations can be applied to two types of resources:

- **APIs**: Logical endpoints that map to templates and database operations
- **Interlays**: Routing layers that direct traffic to underlying endpoints

### Migration Lifecycle

1. **Pending**: Migration created but not configured
2. **Ready**: APIs/Interlays added, ready to execute
3. **Running**: Migration in progress
4. **Paused**: Migration temporarily stopped
5. **Completed**: Migration finished successfully
6. **Failed**: Migration encountered errors
7. **RollingBack**: Reverting to original state
8. **RolledBack**: Successfully reverted

## Migration Strategies

### Big Bang

Immediate cutover to the new endpoint. All traffic switches at once.

```json
{
  "strategy": {
    "type": "big_bang"
  }
}
```

**Best for**: Simple migrations, acceptable brief downtime, low-risk changes.

### Canary

Route a percentage of traffic to test the new system gradually.

```json
{
  "strategy": {
    "type": "canary",
    "read_percentage": 0.05,
    "write_mode": {
      "mode": "dual_write",
      "policy": "old_authoritative"
    }
  }
}
```

**Parameters**:
- `read_percentage`: Fraction of reads to route to new system (0.0 to 1.0)
- `write_mode`: How writes are handled
  - `dual_write`: Write to both systems (safest)
  - `cutover`: Gradually shift writes to new system

**Best for**: Risk mitigation, performance validation, gradual rollouts.

### Blue-Green

Maintain two identical environments with instant traffic switching.

```json
{
  "strategy": {
    "type": "blue_green",
    "active_is_new": false
  }
}
```

**Parameters**:
- `active_is_new`: `false` = blue (old), `true` = green (new)

**Best for**: Zero-downtime migrations, instant rollback capability.

### Rolling Update

Replace instances one by one, minimizing blast radius.

```json
{
  "strategy": {
    "type": "rolling_update",
    "migrated_instances": [],
    "total_instances": 5
  }
}
```

**Parameters**:
- `migrated_instances`: List of migrated instance IDs
- `total_instances`: Total number of instances to migrate

**Best for**: Sharded databases, distributed systems.

### Shadow Traffic

Duplicate requests to new system without affecting responses. Test safely in production.

```json
{
  "strategy": {
    "type": "shadow_traffic"
  }
}
```

**Best for**: Risk-free testing, validation, finding edge cases.

### Strangler Fig

Migrate feature by feature, gradually replacing the old system.

```json
{
  "strategy": {
    "type": "strangler_fig",
    "features": {
      "user_auth": true,
      "payments": false,
      "reporting": false
    }
  }
}
```

**Parameters**:
- `features`: Map of feature names to migration status

**Best for**: Large monolithic systems, long-term refactoring.

### Feature Flag

Control migration with feature flags per user segment.

```json
{
  "strategy": {
    "type": "feature_flag",
    "flags": {
      "new_database": true,
      "beta_features": false
    },
    "rollout_percentage": 0.25
  }
}
```

**Parameters**:
- `flags`: Map of flag names to enabled status
- `rollout_percentage`: Percentage of users who see enabled flags

**Best for**: Fine-grained control, rapid rollback.

### Geographic

Migrate one region at a time.

```json
{
  "strategy": {
    "type": "geographic",
    "regions": {
      "us-east-1": true,
      "eu-west-1": false,
      "ap-southeast-1": false
    }
  }
}
```

**Parameters**:
- `regions`: Map of region identifiers to migration status

**Best for**: Global systems, regional compliance, timezone-based rollouts.

### Time Window

Schedule migration during a maintenance window.

```json
{
  "strategy": {
    "type": "time_window",
    "window_start": "2024-01-15T02:00:00Z",
    "window_end": "2024-01-15T04:00:00Z",
    "executed": false
  }
}
```

**Parameters**:
- `window_start`: ISO 8601 start time
- `window_end`: ISO 8601 end time
- `executed`: Whether migration has completed

**Best for**: Systems with clear low-traffic periods.

## Data Movement

Control how historical data is transferred from source to target.

### None

No automatic data transfer. Use when:
- Starting fresh with empty database
- Data already migrated manually
- Testing environments

```json
{
  "data": "none"
}
```

### Snapshot

Point-in-time snapshot copied to new database.

```json
{
  "data": {
    "type": "snapshot",
    "replace": "none"
  }
}
```

**Characteristics**:
- Consistent view at specific moment
- Lower impact on production
- Faster for large datasets

### Scan

Continuously scan and replicate data in real-time.

```json
{
  "data": {
    "type": "scan",
    "replace": "replace"
  }
}
```

**Conflict Rules**:
- `none`: Skip conflicting records
- `replace`: Overwrite target with source data
- `merge`: Use database-specific merge logic

## Failure Handling

Define how errors are managed during migration.

### Rollback All

Revert everything if any component fails (default).

```json
{
  "failure_handling": "rollback_all"
}
```

### Retry Then Rollback

Retry failed operations before rolling back.

```json
{
  "failure_handling": {
    "type": "retry_then_all",
    "retry_count": 3
  }
}
```

### Allow Partial

Allow some migrations to fail while others succeed.

```json
{
  "failure_handling": "allow_partial"
}
```

## Creating a Migration

### Step 1: Create Migration Definition

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "migrate_to_new_redis",
    "description": "Migrate user cache to new Redis cluster",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {
        "mode": "dual_write",
        "policy": "old_authoritative"
      }
    },
    "data": {
      "type": "scan",
      "replace": "replace"
    },
    "failure_handling": "rollback_all"
  }'
```

### Step 2: Add Resources to Migration

Add an API to the migration:

```bash
curl http://{host}:8000/api/v1/migrations/migrate_to_new_redis/api/user_cache_api \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_cache_api",
    "bindings": [
      {
        "template": "get_user_template",
        "fields": {}
      }
    ]
  }'
```

Add an interlay to the migration:

```bash
curl http://{host}:8000/api/v1/migrations/migrate_to_new_redis/interlay/user_cache_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "new_redis_cluster"
  }'
```

### Step 3: Test Migration

Verify the migration configuration:

```bash
curl http://{host}:8000/api/v1/migrations/migrate_to_new_redis/test \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

### Step 4: Execute Migration

Start the migration:

```bash
curl http://{host}:8000/api/v1/migrations/migrate_to_new_redis/migrate \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

## Monitoring Migrations

### Get Migration Status

```bash
curl http://{host}:8000/api/v1/migrations/migrate_to_new_redis \
  -H "Authorization: Bearer $TOKEN"
```

**Response**:

```json
{
  "status": "success",
  "data": {
    "id": "migrate_to_new_redis",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "description": "Migrate user cache to new Redis cluster",
    "status": "running",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05
    },
    "apis": [...],
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:35:00Z"
  }
}
```

### List All Migrations

```bash
curl http://{host}:8000/api/v1/migrations \
  -H "Authorization: Bearer $TOKEN"
```

## Canary Rollout Example

A typical canary migration proceeds through these stages:

### Stage 1: Initial Setup (5% reads)

```bash
# Create migration with 5% read traffic
curl http://{host}:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "canary_migration",
    "strategy": {
      "type": "canary",
      "read_percentage": 0.05,
      "write_mode": {"mode": "dual_write", "policy": "old_authoritative"}
    }
  }'
```

### Stage 2: Increase Traffic (25% reads)

Update the migration to increase traffic:

```bash
curl http://{host}:8000/api/v1/migrations/canary_migration \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 0.25,
      "write_mode": {"mode": "dual_write", "policy": "old_authoritative"}
    }
  }'
```

### Stage 3: Majority Traffic (75% reads)

```bash
curl http://{host}:8000/api/v1/migrations/canary_migration \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 0.75,
      "write_mode": {"mode": "dual_write", "policy": "old_authoritative"}
    }
  }'
```

### Stage 4: Full Cutover (100%)

```bash
curl http://{host}:8000/api/v1/migrations/canary_migration \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "strategy": {
      "type": "canary",
      "read_percentage": 1.0,
      "write_mode": {"mode": "cutover", "write_percentage": 1.0}
    }
  }'
```

## Access Control

| Operation         | Required Access |
| ----------------- | --------------- |
| Create migration  | Admin           |
| Get migration     | Admin           |
| List migrations   | Read            |
| Add API/Interlay  | Admin           |
| Test migration    | Admin           |
| Execute migration | Admin           |

## Best Practices

### Planning

- **Test in staging first**: Validate migration strategy before production
- **Start small**: Begin with low traffic percentage
- **Monitor metrics**: Watch latency, error rates, and throughput
- **Have rollback plan**: Know how to revert if issues arise

### During Migration

- **Gradual rollout**: Increase traffic incrementally
- **Validate at each step**: Check data consistency and performance
- **Keep both systems running**: Don't decommission old system until confident

### Data Movement

- **Use dual-write during migration**: Ensures no data loss
- **Verify data consistency**: Compare source and target after completion
- **Plan for schema differences**: Handle data transformations if needed

## Related

- [Migrations API](../api/migrations.md) - Complete API reference
- [Endpoints](./endpoints.md) - Managing database connections
- [Transactions](./transactions.md) - Atomic operations
