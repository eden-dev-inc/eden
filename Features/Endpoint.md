# Endpoints Implementation Guide

Endpoints in Eden are managed connections to external databases, services, and APIs. They provide a unified interface for interacting with various data sources while handling connection pooling, authentication, monitoring, and access control. This guide provides comprehensive implementation instructions for creating, managing, and using endpoints in your applications.

## What Are Endpoints?

Endpoints are connection abstractions that:
- Establish and manage connections to external databases and services
- Provide unified read/write/transaction interfaces across different database types
- Handle connection pooling and health monitoring automatically
- Integrate with RBAC for fine-grained access control
- Support metadata collection and schema introspection
- Enable MCP (Model Context Protocol) integration for AI applications

## Supported Endpoint Types

Eden supports the following endpoint types through feature flags:

### Databases
- **PostgreSQL**: Full-featured relational database support
- **Redis**: Key-value store and caching
- **MongoDB**: Document database operations
- **MySQL**: MySQL/MariaDB relational databases
- **Microsoft SQL Server**: Enterprise SQL Server support
- **Cassandra**: Distributed NoSQL database
- **Oracle**: Enterprise Oracle database support
- **ClickHouse**: Columnar analytical database

### Vector Databases
- **Pinecone**: Vector similarity search and embeddings

### External Services
- **HTTP**: RESTful API and webhook endpoints
- **LLM**: Large Language Model integrations

## Creating Endpoints

### Step 1: Define Endpoint Configuration

Each endpoint type requires specific configuration. Here are examples for common types:

#### PostgreSQL Endpoint
```json
{
  "id": "main_postgres_db",
  "kind": "Postgres",
  "description": "Main application database",
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "myapp",
    "username": "myuser",
    "password": "mypassword",
    "ssl_mode": "prefer",
    "connection_timeout": 30,
    "pool_size": 10
  }
}
```

#### Redis Endpoint
```json
{
  "id": "cache_redis",
  "kind": "Redis", 
  "description": "Application cache layer",
  "config": {
    "host": "localhost",
    "port": 6379,
    "password": "redispassword",
    "database": 0,
    "connection_timeout": 5,
    "pool_size": 20
  }
}
```

#### MongoDB Endpoint
```json
{
  "id": "documents_mongo",
  "kind": "Mongo",
  "description": "Document storage database",
  "config": {
    "uri": "mongodb://user:password@localhost:27017/documents",
    "database": "documents",
    "pool_size": 15,
    "connection_timeout": 10
  }
}
```

#### HTTP Endpoint
```json
{
  "id": "external_api",
  "kind": "Http",
  "description": "External service API",
  "config": {
    "base_url": "https://api.example.com",
    "headers": {
      "Authorization": "Bearer token123",
      "Content-Type": "application/json"
    },
    "timeout": 30,
    "retry_attempts": 3
  }
}
```

#### LLM Endpoint
```json
{
  "id": "openai_gpt4",
  "kind": "Llm", 
  "description": "OpenAI GPT-4 integration",
  "config": {
    "provider": "openai",
    "api_key": "sk-...",
    "model": "gpt-4",
    "max_tokens": 2048,
    "temperature": 0.7
  }
}
```

### Step 2: HTTP Request to Create Endpoint

```http
POST /api/v1/endpoints
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "id": "main_postgres_db",
  "kind": "Postgres",
  "description": "Main application database",
  "config": {
    "host": "localhost",
    "port": 5432,
    "database": "myapp",
    "username": "myuser",
    "password": "mypassword",
    "ssl_mode": "prefer",
    "connection_timeout": 30,
    "pool_size": 10
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "main_postgres_db",
    "uuid": "550e8400-e29b-41d4-a716-446655440000"
  }
}
```

**What Happens During Creation:**
1. **RBAC Verification**: Admin access level required
2. **Schema Validation**: Configuration is validated against endpoint type requirements
3. **Connection Test**: System attempts to connect to verify configuration
4. **Engine Registration**: Endpoint is registered with the connection engine
5. **Database Storage**: Configuration is stored securely in the database
6. **Pool Initialization**: Connection pool is created and initialized

## Retrieving Endpoint Information

### Get Endpoint Details

```http
GET /api/v1/endpoints/main_postgres_db
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "main_postgres_db",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "description": "Main application database",
    "config": {
      "host": "localhost",
      "port": 5432,
      "database": "myapp",
      "username": "myuser",
      "ssl_mode": "prefer", 
      "connection_timeout": 30,
      "pool_size": 10
    },
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

**Security Note**: Sensitive credentials like passwords are not returned in GET responses.

## Database Operations

### Reading Data

```http
POST /api/v1/endpoints/main_postgres_db/read
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
    "query": "SELECT * FROM users WHERE status = $1 LIMIT $2",
    "params": ["active", 10]
  }
}
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
        "status": "active",
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "id": 2,
        "name": "Jane Smith",
        "email": "jane@example.com",
        "status": "active", 
        "created_at": "2024-01-14T15:20:00Z"
      }
    ],
    "row_count": 2,
    "execution_time_ms": 45
  }
}
```

### Writing Data

```http
POST /api/v1/endpoints/main_postgres_db/write
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
    "query": "INSERT INTO users (name, email, status) VALUES ($1, $2, $3) RETURNING id",
    "params": ["Alice Johnson", "alice@example.com", "active"]
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "rows": [
      {
        "id": 3
      }
    ],
    "rows_affected": 1,
    "execution_time_ms": 23
  }
}
```

### Transaction Operations

```http
POST /api/v1/endpoints/main_postgres_db/transaction
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
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
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "transaction_id": "txn_550e8400e29b41d4a716446655440000",
    "operations_completed": 3,
    "total_execution_time_ms": 156,
    "committed": true
  }
}
```

## Database-Specific Operations

### MongoDB Document Operations

```http
POST /api/v1/endpoints/documents_mongo/read
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
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
  }
}
```

### Redis Cache Operations

```http
POST /api/v1/endpoints/cache_redis/write
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
    "operation": "set",
    "key": "user_session_123",
    "value": {
      "user_id": 456,
      "login_time": "2024-01-15T10:30:00Z",
      "permissions": ["read", "write"]
    },
    "ttl": 3600
  }
}
```

### HTTP API Calls

```http
POST /api/v1/endpoints/external_api/read
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
    "method": "GET",
    "path": "/users/123",
    "headers": {
      "Accept": "application/json"
    }
  }
}
```

### LLM Completions

```http
POST /api/v1/endpoints/openai_gpt4/read
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "request": {
    "messages": [
      {
        "role": "system", 
        "content": "You are a helpful assistant."
      },
      {
        "role": "user",
        "content": "Explain the benefits of database connection pooling."
      }
    ],
    "max_tokens": 500,
    "temperature": 0.7
  }
}
```

## Metadata Collection

Eden automatically collects metadata about your endpoints to provide schema information and performance insights.

### Manual Metadata Refresh

```http
POST /api/v1/endpoints/main_postgres_db/metadata
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "tables": [
      {
        "name": "users",
        "schema": "public",
        "columns": [
          {
            "name": "id",
            "type": "integer",
            "nullable": false,
            "primary_key": true
          },
          {
            "name": "name",
            "type": "varchar(255)",
            "nullable": false
          },
          {
            "name": "email", 
            "type": "varchar(255)",
            "nullable": false,
            "unique": true
          }
        ],
        "indexes": [
          {
            "name": "idx_users_email",
            "columns": ["email"],
            "unique": true
          }
        ]
      }
    ],
    "last_sync": "2024-01-15T10:30:00Z",
    "sync_duration_ms": 234
  }
}
```

### Automatic Metadata Sync

Metadata is automatically collected at different intervals:
- **High Priority** (1 minute): Active connections and frequently used tables
- **Medium Priority** (30 minutes): General schema information
- **Low Priority** (24 hours): Comprehensive metadata and statistics

Configure sync intervals with environment variables:
```bash
METADATA_HIGH_PRIORITY_INTERVAL_SECS=60
METADATA_MEDIUM_PRIORITY_INTERVAL_SECS=1800
METADATA_LOW_PRIORITY_INTERVAL_SECS=86400
```

## MCP (Model Context Protocol) Integration

Eden provides MCP servers for endpoints to enable AI model integration.

### List Available MCP Servers

```http
GET /api/v1/endpoints/main_postgres_db/mcp
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "servers": [
    {
      "name": "postgres_schema_server",
      "description": "Provides database schema information for AI models"
    },
    {
      "name": "postgres_query_server", 
      "description": "Enables AI models to query the database safely"
    }
  ]
}
```

### Connect to MCP Server

```http
GET /api/v1/endpoints/main_postgres_db/mcp/postgres_schema_server
Authorization: Bearer your_jwt_token
```

This establishes a Server-Sent Events (SSE) connection for real-time AI model interaction.

## Updating Endpoints

### Modify Endpoint Configuration

```http
PATCH /api/v1/endpoints/main_postgres_db
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "description": "Updated main application database",
  "config": {
    "pool_size": 20,
    "connection_timeout": 45
  }
}
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "status": "updated"
  }
}
```

**Update Process:**
1. Configuration is validated
2. Connection pool is gracefully updated
3. Existing connections are maintained during transition
4. New connections use updated configuration

## Deleting Endpoints

### Remove Endpoint

```http
DELETE /api/v1/endpoints/main_postgres_db
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "main_postgres_db",
    "uuid": "550e8400-e29b-41d4-a716-446655440000",
    "kind": "Postgres",
    "description": "Main application database",
    "config": {...},
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z",
    "modified_objects": {
      "objects": {
        "deleted_from_cache": ["endpoint_cache_uuid"],
        "deleted_from_postgres": ["endpoint_postgres_uuid"]
      },
      "rbac": {
        "removed_subjects": {
          "users": ["user_uuid_1", "user_uuid_2"],
          "roles": ["admin_role_uuid"]
        }
      }
    }
  }
}
```

**Deletion Process:**
1. **Connection Drainage**: All active connections are gracefully closed
2. **Engine Disconnection**: Endpoint is removed from connection engine
3. **RBAC Cleanup**: All access permissions are removed
4. **Database Cleanup**: Configuration is removed from storage
5. **Cache Invalidation**: All cached data is cleared

## Access Control & Security

### RBAC Integration

Endpoints integrate with Eden's RBAC system:
- **Admin**: Create, update, delete endpoints; full data access
- **Write**: Read and write data operations
- **Read**: Read-only data access

### Permission Examples

Grant read access to a user:
```http
POST /api/v1/iam/rbac/endpoints/subjects
Authorization: Bearer your_jwt_token

{
  "subject": "user_email@company.com",
  "endpoint_uuid": "550e8400-e29b-41d4-a716-446655440000",
  "access_level": "Read"
}
```

### Connection Security

- **Credential Encryption**: All passwords and sensitive data are encrypted at rest
- **TLS/SSL Support**: Secure connections for all database types
- **Connection Validation**: Credentials are validated during endpoint creation
- **Audit Logging**: All operations are logged for security monitoring

## Error Handling & Troubleshooting

### Common Error Scenarios

1. **Connection Timeout**
```json
{
  "error": "Connection timeout",
  "details": "Failed to connect to database within 30 seconds",
  "endpoint_id": "main_postgres_db",
  "suggestion": "Check network connectivity and database availability"
}
```

2. **Invalid Query**
```json
{
  "error": "SQL syntax error",
  "details": "Syntax error at or near 'SELCT'",
  "query": "SELCT * FROM users",
  "suggestion": "Check SQL syntax and try again"
}
```

3. **Insufficient Permissions**
```json
{
  "error": "Access denied",
  "details": "User does not have Write access to endpoint",
  "access_level": "Read",
  "required_level": "Write"
}
```

4. **Pool Exhaustion**
```json
{
  "error": "Connection pool exhausted",
  "details": "All 10 connections in use",
  "active_connections": 10,
  "suggestion": "Increase pool_size or optimize query performance"
}
```

### Performance Monitoring

Monitor endpoint performance through:
- **Connection Pool Metrics**: Active/idle connection counts
- **Query Performance**: Execution times and slow query detection
- **Error Rates**: Connection failures and query errors
- **Metadata Sync Status**: Schema synchronization health

### Troubleshooting Steps

1. **Connection Issues**:
   - Verify network connectivity
   - Check firewall settings
   - Validate credentials
   - Review SSL/TLS configuration

2. **Performance Problems**:
   - Monitor connection pool usage
   - Analyze slow queries
   - Check database server resources
   - Consider increasing pool size

3. **Permission Errors**:
   - Verify RBAC configuration
   - Check user access levels
   - Review endpoint permissions

## Best Practices for Implementation

### 1. Connection Configuration
- **Pool Sizing**: Start with 10-20 connections, monitor and adjust
- **Timeouts**: Set appropriate connection and query timeouts
- **SSL/TLS**: Always use encrypted connections in production
- **Credential Management**: Use environment variables or secure vaults

### 2. Query Optimization
- **Parameterized Queries**: Always use parameterized queries to prevent SQL injection
- **Connection Reuse**: Leverage connection pooling for better performance
- **Transaction Scope**: Keep transactions as short as possible
- **Batch Operations**: Use batch operations for multiple related queries

### 3. Security Practices
- **Least Privilege**: Grant minimum necessary access levels
- **Regular Rotation**: Rotate database credentials regularly
- **Audit Logging**: Monitor all database operations
- **Network Security**: Use VPNs or private networks for database connections

### 4. Monitoring & Maintenance
- **Health Checks**: Implement regular connection health checks
- **Metadata Sync**: Monitor metadata collection for schema changes
- **Performance Metrics**: Track query performance and connection usage
- **Error Alerting**: Set up alerts for connection failures and errors

### 5. Development Workflow
- **Environment Separation**: Use different endpoints for dev/staging/production
- **Testing**: Test endpoint configurations thoroughly before deployment
- **Documentation**: Document endpoint purposes and connection details
- **Backup Endpoints**: Consider backup endpoints for high availability

This implementation guide provides the comprehensive information needed to successfully create, manage, and use endpoints in your Eden environment, ensuring reliable and secure database connectivity across your applications.