# Endpoints

Endpoints in Eden-MDBS are managed connections to external databases, services, and APIs. They provide a unified interface for interacting with various data sources while handling connection pooling, authentication, monitoring, and access control.

## What Are Endpoints?

Endpoints are connection abstractions that:

- **Establish and manage connections** to external databases and services
- **Provide unified read/write/transaction interfaces** across different database types
- **Handle connection pooling** and health monitoring automatically
- **Integrate with RBAC** for fine-grained access control
- **Support metadata collection** and schema introspection

## Supported Endpoint Types

### Databases

| Type           | Description                       |
| -------------- | --------------------------------- |
| **Postgres**   | PostgreSQL relational database    |
| **MySQL**      | MySQL/MariaDB relational database |
| **Mongo**      | MongoDB document database         |
| **Redis**      | Key-value store and caching       |
| **Cassandra**  | Distributed NoSQL database        |
| **ClickHouse** | Columnar analytical database      |
| **Mssql**      | Microsoft SQL Server              |
| **Oracle**     | Oracle database                   |

### Vector Databases

| Type         | Description              |
| ------------ | ------------------------ |
| **Pinecone** | Vector similarity search |

### External Services

| Type     | Description                       |
| -------- | --------------------------------- |
| **Http** | RESTful API endpoints             |
| **Llm**  | Large Language Model integrations |

## Creating Endpoints

### PostgreSQL

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "main_postgres",
    "kind": "Postgres",
    "config": {
      "write_conn": {
        "url": "postgresql://user:password@host:5432/database"
      }
    }
  }'
```

### Redis

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "cache",
    "kind": "Redis",
    "config": {
      "write_conn": {
        "url": "redis://host:6379"
      }
    }
  }'
```

### MongoDB

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "documents",
    "kind": "Mongo",
    "config": {
      "write_conn": {
        "url": "mongodb://user:password@host:27017/database"
      }
    }
  }'
```

### HTTP Endpoint

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "external_api",
    "kind": "Http",
    "config": {
      "base_url": "https://api.example.com",
      "headers": {
        "Authorization": "Bearer api_key",
        "Content-Type": "application/json"
      }
    }
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "main_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

## Database Operations

### Reading Data

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT * FROM users WHERE status = $1 LIMIT $2",
    "params": ["active", 10]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows": [
      {
        "id": 1,
        "name": "John Doe",
        "email": "john@example.com",
        "status": "active"
      }
    ],
    "row_count": 1
  }
}
```

### Writing Data

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO users (name, email, status) VALUES ($1, $2, $3) RETURNING id",
    "params": ["Alice", "alice@example.com", "active"]
  }'
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "rows": [{ "id": 3 }],
    "rows_affected": 1
  }
}
```

### Transaction Operations

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres/transaction \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "operations": [
      {
        "query": "UPDATE accounts SET balance = balance - $1 WHERE id = $2",
        "params": [100.00, 1]
      },
      {
        "query": "UPDATE accounts SET balance = balance + $1 WHERE id = $2",
        "params": [100.00, 2]
      },
      {
        "query": "INSERT INTO transactions (from_account, to_account, amount) VALUES ($1, $2, $3)",
        "params": [1, 2, 100.00]
      }
    ]
  }'
```

## Database-Specific Operations

### MongoDB

```bash
curl http://{host}:8000/api/v1/endpoints/documents/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "products",
    "operation": "find",
    "filter": {
      "category": "electronics",
      "price": {"$lt": 1000}
    },
    "options": {
      "limit": 20,
      "sort": {"price": 1}
    }
  }'
```

### Redis

```bash
# SET operation
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "SET",
    "args": ["user:123", "{\"name\": \"John\"}"]
  }'

# GET operation
curl http://{host}:8000/api/v1/endpoints/cache/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "command": "GET",
    "args": ["user:123"]
  }'
```

## Retrieving Endpoint Information

### Get Endpoint Details

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres \
  -H "Authorization: Bearer $TOKEN"
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "main_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "myapp"
    },
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

> **Note**: Sensitive credentials like passwords are not returned in GET responses.

### List All Endpoints

```bash
curl http://{host}:8000/api/v1/endpoints \
  -H "Authorization: Bearer $TOKEN"
```

## Updating Endpoints

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PATCH \
  -d '{
    "config": {
      "pool_size": 20,
      "connection_timeout": 45
    }
  }'
```

## Deleting Endpoints

```bash
curl http://{host}:8000/api/v1/endpoints/main_postgres \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

**Response:**

```json
{
  "status": "success",
  "data": {
    "id": "main_postgres",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "modified_objects": {
      "objects": {
        "deleted_from_cache": ["endpoint_cache_uuid"],
        "deleted_from_postgres": ["endpoint_postgres_uuid"]
      }
    }
  }
}
```

## Access Control

Endpoints integrate with Eden's RBAC system:

| Access Level | Permissions                                        |
| ------------ | -------------------------------------------------- |
| **Admin**    | Create, update, delete endpoints; full data access |
| **Write**    | Read and write data operations                     |
| **Read**     | Read-only data access                              |

### Grant Access

```bash
curl http://{host}:8000/api/v1/iam/rbac/endpoints/subjects \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "subjects": [
      ["user@company.com", "Read"],
      ["admin@company.com", "Admin"]
    ]
  }'
```

## Error Handling

### Connection Timeout

```json
{
  "error": "Connection timeout",
  "details": "Failed to connect to database within 30 seconds",
  "suggestion": "Check network connectivity and database availability"
}
```

### Invalid Query

```json
{
  "error": "SQL syntax error",
  "details": "Syntax error at or near 'SELCT'"
}
```

### Insufficient Permissions

```json
{
  "error": "Access denied",
  "details": "User does not have Write access to endpoint",
  "access_level": "Read",
  "required_level": "Write"
}
```

### Pool Exhaustion

```json
{
  "error": "Connection pool exhausted",
  "details": "All connections in use",
  "suggestion": "Increase pool_size or optimize query performance"
}
```

## Best Practices

### Connection Configuration

- **Pool Sizing**: Start with 10-20 connections, monitor and adjust
- **Timeouts**: Set appropriate connection and query timeouts
- **SSL/TLS**: Always use encrypted connections in production

### Query Optimization

- **Parameterized Queries**: Always use parameterized queries to prevent SQL injection
- **Connection Reuse**: Leverage connection pooling for better performance
- **Transaction Scope**: Keep transactions as short as possible

### Security

- **Least Privilege**: Grant minimum necessary access levels
- **Credential Rotation**: Rotate database credentials regularly
- **Audit Logging**: Monitor all database operations

### Monitoring

- **Health Checks**: Implement regular connection health checks
- **Performance Metrics**: Track query performance and connection usage
- **Error Alerting**: Set up alerts for connection failures

## Related

- [Organizations](./organizations.md) - Organization management
- [RBAC](./rbac.md) - Access control for endpoints
- [Workflows](./workflows.md) - Automating database operations
- [Transactions](./transactions.md) - Cross-database transactions
