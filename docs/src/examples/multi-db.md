# Multi-Database Examples

This page demonstrates how to work with multiple database types through Eden-MDBS.

## Setup: Multiple Endpoints

First, create endpoints for different database types:

```bash
# PostgreSQL for relational data
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "users_db",
    "kind": "Postgres",
    "config": {"write_conn": {"url": "postgresql://user:pass@pg-host:5432/users"}}
  }'

# MongoDB for documents
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "documents_db",
    "kind": "Mongo",
    "config": {"write_conn": {"url": "mongodb://user:pass@mongo-host:27017/docs"}}
  }'

# Redis for caching
curl http://{host}:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "cache",
    "kind": "Redis",
    "config": {"write_conn": {"url": "redis://redis-host:6379"}}
  }'
```

## Pattern: Cache-Aside

Read from cache first, fall back to database:

```bash
# Step 1: Check cache
CACHE_RESULT=$(curl -s http://{host}:8000/api/v1/endpoints/cache/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"command": "GET", "args": ["user:123"]}')

# Step 2: If cache miss, query database
if [ "$CACHE_RESULT" = "null" ]; then
  DB_RESULT=$(curl -s http://{host}:8000/api/v1/endpoints/users_db/read \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d '{"query": "SELECT * FROM users WHERE id = $1", "params": [123]}')

  # Step 3: Store in cache for next time
  curl http://{host}:8000/api/v1/endpoints/cache/write \
    -H "Content-Type: application/json" \
    -H "Authorization: Bearer $TOKEN" \
    -d "{\"command\": \"SET\", \"args\": [\"user:123\", \"$DB_RESULT\", \"EX\", \"3600\"]}"
fi
```

## Pattern: Read from PostgreSQL, Write to MongoDB

Export relational data to document store:

```bash
# Read users from PostgreSQL
curl http://{host}:8000/api/v1/endpoints/users_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "SELECT id, name, email, preferences FROM users WHERE updated_at > $1",
    "params": ["2024-01-01"]
  }'

# Write user profile to MongoDB
curl http://{host}:8000/api/v1/endpoints/documents_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "user_profiles",
    "operation": "insertOne",
    "document": {
      "user_id": 123,
      "name": "John Doe",
      "email": "john@example.com",
      "preferences": {"theme": "dark"},
      "synced_at": "2024-01-15T10:30:00Z"
    }
  }'
```

## Pattern: Aggregation Across Databases

Query different databases and combine results:

```bash
# Get user info from PostgreSQL
USER=$(curl -s http://{host}:8000/api/v1/endpoints/users_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"query": "SELECT * FROM users WHERE id = $1", "params": [123]}')

# Get user's documents from MongoDB
DOCS=$(curl -s http://{host}:8000/api/v1/endpoints/documents_db/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "documents",
    "operation": "find",
    "filter": {"user_id": 123},
    "options": {"limit": 10}
  }')

# Get user's session from Redis
SESSION=$(curl -s http://{host}:8000/api/v1/endpoints/cache/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"command": "GET", "args": ["session:123"]}')
```

## Pattern: Write-Through Cache

Update database and cache simultaneously:

```bash
# Update user in PostgreSQL
curl http://{host}:8000/api/v1/endpoints/users_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "UPDATE users SET name = $1, updated_at = NOW() WHERE id = $2 RETURNING *",
    "params": ["Jane Doe", 123]
  }'

# Invalidate cache
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"command": "DEL", "args": ["user:123"]}'
```

## Pattern: Event Logging

Store events in different databases for different purposes:

```bash
# Log to PostgreSQL for durable storage
curl http://{host}:8000/api/v1/endpoints/users_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "query": "INSERT INTO event_log (event_type, user_id, data, created_at) VALUES ($1, $2, $3, NOW())",
    "params": ["login", 123, "{\"ip\": \"192.168.1.1\"}"]
  }'

# Also log to MongoDB for flexible querying
curl http://{host}:8000/api/v1/endpoints/documents_db/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "collection": "events",
    "operation": "insertOne",
    "document": {
      "event_type": "login",
      "user_id": 123,
      "data": {"ip": "192.168.1.1", "user_agent": "Mozilla/5.0"},
      "timestamp": {"$date": "2024-01-15T10:30:00Z"}
    }
  }'

# Increment counter in Redis for real-time metrics
curl http://{host}:8000/api/v1/endpoints/cache/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"command": "INCR", "args": ["metrics:logins:2024-01-15"]}'
```

## Using Templates for Multi-DB Operations

Create templates for common multi-database patterns:

```bash
# Template for PostgreSQL user lookup
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_pg",
    "template": {
      "endpoint_uuid": "POSTGRES_UUID",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM users WHERE id = {{user_id}}",
        "params": ["{{user_id}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'

# Template for MongoDB document lookup
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_docs",
    "template": {
      "endpoint_uuid": "MONGO_UUID",
      "kind": "Read",
      "template": {
        "collection": "documents",
        "operation": "find",
        "filter": {"user_id": "{{user_id}}"}
      },
      "endpoint_kind": "Mongo"
    }
  }'

# Template for Redis cache
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_user_cache",
    "template": {
      "endpoint_uuid": "REDIS_UUID",
      "kind": "Read",
      "template": {
        "command": "GET",
        "args": ["user:{{user_id}}"]
      },
      "endpoint_kind": "Redis"
    }
  }'
```

## Best Practices

### Consistency
- Be aware that operations across databases are not transactional
- Design for eventual consistency when needed
- Use compensating transactions for failure scenarios

### Performance
- Use Redis for frequently accessed data
- Batch operations when possible
- Consider data locality

### Error Handling
- Handle partial failures gracefully
- Implement retry logic for transient failures
- Log failures for debugging

## Related

- [Basic Examples](./basic.md) - Single database examples
- [Transactions](./transactions.md) - Atomic operations
- [Workflows](./workflows.md) - Multi-step automation
