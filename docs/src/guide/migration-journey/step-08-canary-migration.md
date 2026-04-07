# Step 8: Canary Migration

**Phase**: Migration
**Previous**: [Step 7: Redirect to Eden](./step-07-redirect-to-eden.md)
**Next**: [Step 9: Complete Migration](./step-09-complete-migration.md)

---

## Gradual Traffic Shift

Eden shifts read traffic gradually to the target. **Maximum 25% change per update** for safety. **Instant rollback at every step.**

## Update Traffic Split

Use the `/traffic` endpoint to adjust the read percentage:

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "read_percentage": 0.25
  }'
```

**Parameters:**

- `read_percentage`: Fraction of reads to route to new system (0.0 to 1.0)
- `reason`: Optional description of why this change was made

**Constraint:** Cannot change by more than 25% per update (safety limit).

## Gradual Rollout

### Stage 1: 5% → 25% Reads

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 0.25}'
```

### Stage 2: 25% → 50% Reads

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 0.50}'
```

### Stage 3: 50% → 75% Reads

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 0.75}'
```

### Stage 4: 75% → 100% Reads

```bash
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 1.0}'
```

## Monitor Status

```bash
curl http://localhost:8000/api/v1/migrations/$MIGRATION_ID \
  -H "Authorization: Bearer $TOKEN"
```

## Instant Rollback

At **any stage**, rollback by reducing read percentage (max 25% per call):

```bash
# From 50% back to 25%
curl -X PATCH http://localhost:8000/api/v1/migrations/$MIGRATION_ID/traffic \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"read_percentage": 0.25, "reason": "Elevated error rate observed"}'
```

To fully rollback, step down in 25% increments or adjust the migration strategy.

## What Changes Next

In [Step 9: Complete Migration](./step-09-complete-migration.md), you'll perform the full cutover to 100% target traffic.

---

**Navigation**: [← Step 7](./step-07-redirect-to-eden.md) | [Overview](./overview.md) | **Step 8** | [Step 9 →](./step-09-complete-migration.md)
