# Step 9: Complete Migration

**Phase**: Migration
**Previous**: [Step 8: Canary Migration](./step-08-canary-migration.md)
**Next**: [Step 10: Decommission Source](./step-10-decommission-source.md)

---

## Full Cutover

All traffic flows to the target database. The source is **idle but still available as a fallback**.

## Architecture

```
┌─────┐      ┌──────┐      ┌────────────┐
│ App │ ───▶ │ Eden │      │ Source DB  │  ← Idle (fallback)
└─────┘      └──────┘      └────────────┘
                 │
                 │  100% traffic
                 ▼
             ┌────────────┐
             │ Target DB  │  ← All traffic
             └────────────┘
```

## Prerequisites

Before completing the migration:

1. Read percentage should be at 100% (from [Step 8](./step-08-canary-migration.md))
2. Monitor for errors and latency anomalies
3. Confirm data consistency between source and target

## Complete the Migration

Once you're confident the migration is successful, mark it as complete:

```bash
curl -X POST http://localhost:8000/api/v1/migrations/$MIGRATION_ID/complete \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "reason": "Migration validated, all checks passed"
  }'
```

**Parameters:**

- `reason`: Optional description of why the migration is being completed
- `force`: Set to `true` to complete even if automatic completion criteria aren't met (default: `false`)

This action:

- Sets migration status to `Completed`
- Swaps the interlay endpoint pointer from source to target
- Clears migration state from the interlay

## Verify Completion

```bash
curl http://localhost:8000/api/v1/migrations/$MIGRATION_ID \
  -H "Authorization: Bearer $TOKEN"
```

The response should show `"status": "Completed"`.

## Rollback Before Completion

**Before** calling `/complete`, you can still rollback by reducing the read percentage:

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 0.75}'
```

**After** calling `/complete`, the migration is finalized and the interlay now points directly to the target. To revert, you would need to create a new migration from target back to source.

## Source Database Status

At this point:

| Aspect      | Status                |
| ----------- | --------------------- |
| Traffic     | None (idle)           |
| Data        | Stale (no new writes) |
| Connections | Eden only             |
| Purpose     | Fallback/rollback     |

## What Changes Next

In [Step 10: Decommission Source](./step-10-decommission-source.md), you'll safely remove the source database after confirming stable operation.

---

**Navigation**: [← Step 8](./step-08-canary-migration.md) | [Overview](./overview.md) | **Step 9** | [Step 10 →](./step-10-decommission-source.md)
