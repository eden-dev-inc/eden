# Endpoint Management API

This reference covers the APIs for managing database endpoints in Eden-MDBS.

## Overview

Endpoints are managed database connections. Each endpoint connects to a specific database and can be used for executing queries.

## Create Endpoint

Create a new endpoint connection.

### Request

```http
POST /api/v1/endpoints
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field        | Type   | Required | Description                              |
| ------------ | ------ | -------- | ---------------------------------------- |
| `endpoint`   | string | Yes      | Unique endpoint identifier               |
| `kind`       | string | Yes      | Database type (postgres, mysql, mongo, redis, cassandra, clickhouse, pinecone, http) |
| `config`     | object | Yes      | Database-specific configuration          |
| `description`| string | No       | Endpoint description                     |

### PostgreSQL Example

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "my_postgres",
    "kind": "postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:password@host:5432/database"
      }
    },
    "description": "Production PostgreSQL database"
  }'
```

### MySQL Example

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "my_mysql",
    "kind": "mysql",
    "config": {
      "write_conn": {
        "url": "mysql://user:password@host:3306/database"
      }
    }
  }'
```

### MongoDB Example

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "my_mongo",
    "kind": "mongo",
    "config": {
      "db_name": "mydb",
      "write_conn": {
        "url": "mongodb://user:password@host:27017"
      }
    }
  }'
```

### Redis Example

Redis supports both URL format and host/port configuration:

```bash
# Using host/port (recommended)
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "my_redis",
    "kind": "redis",
    "config": {
      "write_conn": {
        "host": "redis-host",
        "port": 6379,
        "tls": false
      }
    },
    "description": "Redis cache endpoint"
  }'
```

```bash
# Using URL format
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "my_redis",
    "kind": "redis",
    "config": {
      "write_conn": {
        "url": "redis://host:6379"
      }
    }
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "my_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

## List Endpoints

Get all endpoints in your organization.

### Request

```http
GET /api/v1/endpoints
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "endpoints": [
      {
        "id": "my_postgres",
        "uuid": "550e8400-e29b-41d4-a716-446655440000",
        "kind": "Postgres",
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "id": "my_redis",
        "uuid": "550e8400-e29b-41d4-a716-446655440001",
        "kind": "Redis",
        "created_at": "2024-01-15T11:00:00Z"
      }
    ]
  }
}
```

## Get Endpoint

Get details of a specific endpoint.

### Request

```http
GET /api/v1/endpoints/{id}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "my_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

## Update Endpoint

Update an endpoint's configuration.

### Request

```http
PATCH /api/v1/endpoints/{endpoint}
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "config": {
      "write_conn": {
        "url": "postgresql://user:newpassword@host:5432/database"
      }
    }
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "my_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "updated_at": "2024-01-16T10:30:00Z"
  }
}
```

## Delete Endpoint

Remove an endpoint.

### Request

```http
DELETE /api/v1/endpoints/{id}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": {
    "message": "Endpoint deleted successfully"
  }
}
```

## Test Connection

Verify connectivity to an endpoint.

### Request

```http
POST /api/v1/endpoints/{id}/test
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/endpoints/my_postgres/test \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

### Response

```json
{
  "status": "success",
  "data": {
    "connected": true,
    "latency_ms": 15
  }
}
```

## Configuration Options

### Connection URL Format

Each database type uses a specific URL format:

| Database   | URL Format                                      |
| ---------- | ----------------------------------------------- |
| PostgreSQL | `postgresql://user:pass@host:5432/database`     |
| MySQL      | `mysql://user:pass@host:3306/database`          |
| MongoDB    | `mongodb://user:pass@host:27017/database`       |
| Redis      | `redis://host:6379` or `redis://:pass@host:6379`|
| Cassandra  | `cassandra://user:pass@host:9042/keyspace`      |
| ClickHouse | `clickhouse://user:pass@host:8123/database`     |

### Read Replicas

Configure read replicas for load distribution:

```bash
curl http://{host}:8000/api/v1/connect/postgres \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "my_postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:pass@primary:5432/db"
      },
      "read_conn": {
        "url": "postgresql://user:pass@replica:5432/db"
      }
    }
  }'
```

## Access Control

| Operation        | Required Access |
| ---------------- | --------------- |
| Connect endpoint | Admin           |
| List endpoints   | Read            |
| Get endpoint     | Read            |
| Update endpoint  | Admin           |
| Delete endpoint  | Admin           |
| Test connection  | Read            |

## Error Responses

### Invalid Configuration

```json
{
  "error": "Invalid configuration",
  "message": "Connection URL is required"
}
```

### Connection Failed

```json
{
  "error": "Connection failed",
  "message": "Unable to connect to database: connection refused"
}
```

### Endpoint Not Found

```json
{
  "error": "Not found",
  "message": "Endpoint 'my_postgres' does not exist"
}
```

### Duplicate ID

```json
{
  "error": "Conflict",
  "message": "Endpoint with id 'my_postgres' already exists"
}
```

## Best Practices

### Connection Security

- Use SSL/TLS connections when available
- Store credentials securely
- Use dedicated database users with minimal permissions

### Naming Conventions

- Use descriptive, lowercase IDs
- Include environment or purpose in name (e.g., `prod_users_db`, `staging_cache`)

### Connection Management

- Test connections after creation
- Monitor endpoint health
- Update credentials before they expire

## Related

- [Query Execution](./queries.md) - Execute queries on endpoints
- [Transactions](./transactions.md) - Atomic operations
- [Endpoint Types](../guide/endpoints.md) - Database-specific guides
