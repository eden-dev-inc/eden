# Step 6: Shadow Test

**Phase**: Preparation
**Previous**: [Step 5: Connect Target](./step-05-connect-target.md)
**Next**: [Step 7: Redirect to Eden](./step-07-redirect-to-eden.md)

---

## Shadow Validation

Eden duplicates all writes to the target while continuing to serve reads from the source. This validates that the target can handle production load with **zero customer impact**.

**Best for**: Risk-free testing, validation, finding edge cases.

## How It Works

```
┌─────┐      ┌──────┐      ┌────────────┐
│ App │ ───▶ │ Eden │ ───▶ │ Source DB  │  ← Response returned
└─────┘      └──────┘      └────────────┘
                 │
                 │  shadow (async)
                 ▼
             ┌────────────┐
             │ Target DB  │  ← Response compared, not returned
             └────────────┘
```

- Reads: Only from source (returned to client)
- Writes: Replicated to both (source is authoritative)

Clients only receive responses from the source. The target receives the same writes but its responses are not used.

## Setting Up Shadow Testing

### 1. Create Interlay

```bash
curl -X POST http://localhost:8000/api/v1/interlays \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "redis-interlay",
    "endpoint": "'$SOURCE_UUID'",
    "port": 6366,
    "description": "Redis proxy for shadow testing",
    "tls": null,
    "settings": {}
  }'
```

### 2. Create Migration with Shadow Strategy

```bash
curl -X POST http://localhost:8000/api/v1/migrations \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "id": "shadow-test",
    "description": "Shadow testing before production migration",
    "strategy": {"type": "shadow_traffic"},
    "data": "None",
    "failure_handling": null,
    "tests": []
  }'
```

### 3. Add Interlay to Migration

```bash
curl -X POST http://localhost:8000/api/v1/migrations/shadow-test/interlay/redis-interlay \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "shadow-test",
    "endpoint": "'$TARGET_UUID'",
    "description": "Shadow test interlay config",
    "migration_strategy": {"type": "shadow_traffic"},
    "migration_data": "None",
    "migration_rules": {
      "traffic": {
        "read": "Old",
        "write": {"Replicated": {"policy": "OldAuthoritative"}}
      },
      "error": "DoNothing",
      "rollback": "Ignore",
      "completion": {"milestone": "Immediate", "require_manual_approval": false}
    }
  }'
```

### 4. Start Shadow Testing

```bash
curl -X POST http://localhost:8000/api/v1/migrations/shadow-test/migrate \
  -H "Authorization: Bearer $TOKEN"
```

## Monitor Shadow Testing

Check that writes are being replicated:

```bash
curl http://localhost:8000/api/v1/migrations/shadow-test \
  -H "Authorization: Bearer $TOKEN"
```

## What Changes Next

In [Step 7: Redirect to Eden](./step-07-redirect-to-eden.md), you'll update DNS/connection strings to route traffic through Eden's proxy.

---

**Navigation**: [← Step 5](./step-05-connect-target.md) | [Overview](./overview.md) | **Step 6** | [Step 7 →](./step-07-redirect-to-eden.md)
