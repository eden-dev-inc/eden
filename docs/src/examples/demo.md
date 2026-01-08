# Redis Migration Demo

This is a complete, plug-and-play demo that walks you through a Redis migration workflow. All commands use `localhost` and can be copied and executed directly without modification.

## Prerequisites

Before running this demo, ensure you have:

1. Eden-MDBS running on `localhost:8000`
2. Two Redis instances:
   - Source Redis on `localhost:6378`
   - Target Redis on `localhost:6377`
3. The organization creation token set as environment variable:

```bash
export EDEN_NEW_ORG_TOKEN="your_org_creation_token"
```

## Step 1: Create Organization

Create a new organization with a super admin user:

```bash
curl http://localhost:8000/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{
    "id": "TestOrg",
    "description": "test organization",
    "super_admins": [
      {
        "username": "admin",
        "password": "password",
        "description": null
      }
    ]
  }'
```

**Expected Response:**

```json
{
  "id": "TestOrg",
  "uuid": "550e8400-e29b-41d4-a716-446655440000"
}
```

## Step 2: Login

Login with basic authentication and save the token:

```bash
curl http://localhost:8000/api/v1/auth/login \
  -u admin:password \
  -H "X-Org-Id: TestOrg" \
  -X POST
```

**Expected Response:**

```json
{
  "status": "success",
  "data": {
    "token": "eyJhbGciOiJIUzI1NiIs..."
  }
}
```

Save the token for subsequent requests:

```bash
export TOKEN="eyJhbGciOiJIUzI1NiIs..."
```

## Step 3: Create Source Redis Endpoint

Create the first Redis endpoint (source database on port 6378):

```bash
curl http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_test1",
    "kind": "redis",
    "config": {
      "read_conn": null,
      "write_conn": {
        "host": "localhost",
        "port": 6378,
        "password": null,
        "tls": false
      }
    }
  }'
```

**Expected Response:**

```json
{
  "status": "success",
  "data": {
    "id": "redis_test1",
    "uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
  }
}
```

Save the UUID for later use:

```bash
export REDIS1_UUID="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
```

## Step 4: Create Target Redis Endpoint

Create the second Redis endpoint (target database on port 6377):

```bash
curl http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_test2",
    "kind": "redis",
    "config": {
      "read_conn": null,
      "write_conn": {
        "host": "localhost",
        "port": 6377,
        "password": null,
        "tls": false
      }
    }
  }'
```

**Expected Response:**

```json
{
  "status": "success",
  "data": {
    "id": "redis_test2",
    "uuid": "ffffffff-1111-2222-3333-444444444444"
  }
}
```

Save the UUID:

```bash
export REDIS2_UUID="ffffffff-1111-2222-3333-444444444444"
```

## Step 5: Create an Interlay

Create an interlay that initially routes traffic to the source endpoint. The interlay exposes port 6366 for client connections:

```bash
curl http://localhost:8000/api/v1/interlays \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "redis_interlay",
    "endpoint": "'$REDIS1_UUID'",
    "port": 6366,
    "settings": {},
    "tls": false
  }'
```

**Expected Response:**

```json
{
  "status": "success",
  "data": {
    "id": "redis_interlay",
    "uuid": "11111111-2222-3333-4444-555555555555"
  }
}
```

## Step 6: Write Data to Source Redis

Write some test data to the source Redis through the endpoint:

```bash
curl http://localhost:8000/api/v1/endpoints/$REDIS1_UUID/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "request": {
      "type": "Set",
      "key": "test_key",
      "value": "hello_world"
    }
  }'
```

**Expected Response:**

```json
{
  "kind": "redis",
  "data": [{ "Resp3": [43, 79, 75, 13, 10] }]
}
```

The response contains RESP3 protocol bytes representing "OK".

Verify the data was written:

```bash
curl http://localhost:8000/api/v1/endpoints/$REDIS1_UUID/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "request": {
      "type": "Get",
      "key": "test_key"
    }
  }'
```

**Expected Response:**

```json
{
  "kind": "redis",
  "data": [
    {
      "Resp3": [
        36, 49, 49, 13, 10, 104, 101, 108, 108, 111, 95, 119, 111, 114, 108,
        100, 13, 10
      ]
    }
  ]
}
```

The response contains RESP3 protocol bytes representing "hello_world".

## Step 7: Create a Migration

Create a new migration that will move data from source to target:

```bash
curl http://localhost:8000/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "test_migration",
    "description": "Test migration from redis_test1 to redis_test2",
    "strategy": {
      "type": "big_bang",
      "durability": true
    }
  }'
```

**Expected Response:**

```json
{
  "status": "success",
  "data": {
    "id": "test_migration",
    "uuid": "66666666-7777-8888-9999-aaaaaaaaaaaa"
  }
}
```

## Step 8: Add Interlay to Migration

Link the interlay to the migration with traffic routing rules:

```bash
curl http://localhost:8000/api/v1/migrations/test_migration/interlay/redis_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "migration_relay",
    "endpoint": "redis_test2",
    "description": "Redis migration interlay",
    "migration_strategy": {
      "type": "big_bang",
      "durability": true
    },
    "migration_data": {
      "Scan": {
        "replace": "None"
      }
    },
    "migration_rules": {
      "traffic": {
        "read": "Replicated",
        "write": "New"
      },
      "error": "DoNothing",
      "rollback": "Ignore",
      "completion": {
        "milestone": "Immediate",
        "require_manual_approval": false
      }
    }
  }'
```

**Expected Response:**

```json
{
  "status": "success",
  "data": "added Interlay to migration"
}
```

## Step 9: Start the Migration

Execute the migration to transfer data and switch traffic:

```bash
curl http://localhost:8000/api/v1/migrations/test_migration/migrate \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST
```

**Expected Response:**

```json
{
  "status": "success",
  "data": "Migration started"
}
```

## Step 10: Verify Migration

Check that data has been migrated to the target Redis:

```bash
curl http://localhost:8000/api/v1/endpoints/$REDIS2_UUID/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "request": {
      "type": "Get",
      "key": "test_key"
    }
  }'
```

**Expected Response:**

```json
{
  "kind": "redis",
  "data": [
    {
      "Resp3": [
        36, 49, 49, 13, 10, 104, 101, 108, 108, 111, 95, 119, 111, 114, 108,
        100, 13, 10
      ]
    }
  ]
}
```

The response contains RESP3 protocol bytes representing "hello_world".

## Complete Demo Script

Here's the entire demo as a single script you can run:

```bash
#!/bin/bash
set -e

# Configuration
HOST="localhost:8000"
EDEN_NEW_ORG_TOKEN="${EDEN_NEW_ORG_TOKEN:-your_org_creation_token}"

echo "=== Step 1: Create Organization ==="
curl -s http://$HOST/api/v1/new \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $EDEN_NEW_ORG_TOKEN" \
  -d '{
    "id": "TestOrg",
    "description": "test organization",
    "super_admins": [{"username": "admin", "password": "password", "description": null}]
  }' | jq .

echo -e "\n=== Step 2: Login ==="
TOKEN=$(curl -s http://$HOST/api/v1/auth/login \
  -u admin:password \
  -H "X-Org-Id: TestOrg" \
  -X POST | jq -r '.data.token')
echo "Token: ${TOKEN:0:50}..."

echo -e "\n=== Step 3: Create Source Redis Endpoint ==="
REDIS1_RESPONSE=$(curl -s http://$HOST/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_test1",
    "kind": "redis",
    "config": {"read_conn": null, "write_conn": {"host": "localhost", "port": 6378, "password": null, "tls": false}}
  }')
echo $REDIS1_RESPONSE | jq .
REDIS1_UUID=$(echo $REDIS1_RESPONSE | jq -r '.data.uuid')

echo -e "\n=== Step 4: Create Target Redis Endpoint ==="
REDIS2_RESPONSE=$(curl -s http://$HOST/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_test2",
    "kind": "redis",
    "config": {"read_conn": null, "write_conn": {"host": "localhost", "port": 6377, "password": null, "tls": false}}
  }')
echo $REDIS2_RESPONSE | jq .
REDIS2_UUID=$(echo $REDIS2_RESPONSE | jq -r '.data.uuid')

echo -e "\n=== Step 5: Create Interlay ==="
curl -s http://$HOST/api/v1/interlays \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d "{
    \"id\": \"redis_interlay\",
    \"endpoint\": \"$REDIS1_UUID\",
    \"port\": 6366,
    \"settings\": {},
    \"tls\": false
  }" | jq .

echo -e "\n=== Step 6: Write Test Data ==="
curl -s http://$HOST/api/v1/endpoints/$REDIS1_UUID/write \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"request": {"type": "Set", "key": "test_key", "value": "hello_world"}}' | jq .

echo -e "\n=== Step 7: Create Migration ==="
curl -s http://$HOST/api/v1/migrations \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "test_migration",
    "description": "Test migration from redis_test1 to redis_test2",
    "strategy": {"type": "big_bang", "durability": true}
  }' | jq .

echo -e "\n=== Step 8: Add Interlay to Migration ==="
curl -s http://$HOST/api/v1/migrations/test_migration/interlay/redis_interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "migration_relay",
    "endpoint": "redis_test2",
    "description": "Redis migration interlay",
    "migration_strategy": {"type": "big_bang", "durability": true},
    "migration_data": {"Scan": {"replace": "None"}},
    "migration_rules": {
      "traffic": {"read": "Replicated", "write": "New"},
      "error": "DoNothing",
      "rollback": "Ignore",
      "completion": {"milestone": "Immediate", "require_manual_approval": false}
    }
  }' | jq .

echo -e "\n=== Step 9: Start Migration ==="
curl -s http://$HOST/api/v1/migrations/test_migration/migrate \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST | jq .

echo -e "\n=== Step 10: Verify Data on Target ==="
sleep 2  # Wait for migration to complete
curl -s http://$HOST/api/v1/endpoints/$REDIS2_UUID/read \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"request": {"type": "Get", "key": "test_key"}}' | jq .

echo -e "\n=== Demo Complete ==="
```

Save this as `redis_migration_demo.sh`, make it executable with `chmod +x redis_migration_demo.sh`, and run it.

## Traffic Routing Rules

During migration, you can configure how traffic is routed:

| Rule         | Read Behavior            | Write Behavior          |
| ------------ | ------------------------ | ----------------------- |
| `Old`        | Read from source         | Write to source only    |
| `New`        | Read from target         | Write to target only    |
| `Replicated` | Read from both (compare) | N/A                     |
| `Both`       | N/A                      | Write to both endpoints |

## Cleanup

To clean up after the demo:

```bash
# Delete the migration
curl http://localhost:8000/api/v1/migrations/test_migration \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE

# Delete the interlay
curl http://localhost:8000/api/v1/interlays/redis_interlay \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE

# Delete the endpoints
curl http://localhost:8000/api/v1/endpoints/redis_test1 \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE

curl http://localhost:8000/api/v1/endpoints/redis_test2 \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE

# Delete the organization (requires super admin)
curl http://localhost:8000/api/v1/organizations \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

## Related

- [Migrations Guide](../guide/migrations.md) - Understanding migrations
- [Interlays API](../api/interlays.md) - Interlay configuration
- [Endpoints API](../api/endpoints.md) - Endpoint management
- [Basic Examples](./basic.md) - More usage examples
