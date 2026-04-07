# Step 5: Connect Target

**Phase**: Preparation
**Previous**: [Step 4: Recommend Target](./step-04-recommend-target.md)
**Next**: [Step 6: Shadow Test](./step-06-shadow-test.md)

---

## Establish Link

Eden establishes connections to **both** databases. The target is provisioned and ready.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Customer VPC                             │
│                                                                  │
│                      ┌───────────┐                               │
│              ┌──────▶│   Eden    │◀──────┐                       │
│              │       └───────────┘       │                       │
│              │                           │                       │
│              ▼                           ▼                       │
│   ┌───────────────────┐       ┌───────────────────┐              │
│   │  Source Database  │       │  Target Database  │              │
│   │  (current)        │       │  (migration dest) │              │
│   └───────────────────┘       └───────────────────┘              │
│                                                                  │
│   ┌─────────────┐  ┌─────────────┐                               │
│   │   App 1     │  │   App N     │  (Still connect               │
│   │             │──┼─────────────┼───directly to source)         │
│   └─────────────┘  └─────────────┘                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Connect Source Endpoint

**Important:** If you plan to use DNS redirection (Step 7), use a legacy DNS name or IP that won't change when you update the main DNS record.

```bash
curl -X POST http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_source",
    "kind": "redis",
    "config": {
      "read_conn": null,
      "write_conn": {
        "host": "redis-old",
        "port": 6379,
        "tls": false,
        "password": ""
      }
    },
    "description": "Source Redis"
  }'
```

Use `redis-old` (legacy DNS) instead of `redis` so the DNS change in Step 7 won't affect Eden's connection to the source.

**Response:**

```json
{
  "id": "redis_source",
  "uuid": "e5ad68f8-806a-4b34-b418-de6c30f70ad5"
}
```

## Connect Target Endpoint

```bash
curl -X POST http://localhost:8000/api/v1/endpoints \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "endpoint": "redis_target",
    "kind": "redis",
    "config": {
      "read_conn": null,
      "write_conn": {
        "host": "myredis.redis.cache.azure.com",
        "port": 6380,
        "tls": true,
        "password": "your-access-key"
      }
    },
    "description": "Target Redis - Azure"
  }'
```

## Connection Status

At this point:

| Component       | Status                 |
| --------------- | ---------------------- |
| Source endpoint | Connected              |
| Target endpoint | Connected              |
| Applications    | Still direct to source |
| Traffic routing | Not configured         |

## What Changes Next

In [Step 6: Shadow Test](./step-06-shadow-test.md), you can optionally validate the target with shadow traffic before routing production traffic.

---

**Navigation**: [← Step 4](./step-04-recommend-target.md) | [Overview](./overview.md) | **Step 5** | [Step 6 →](./step-06-shadow-test.md)
