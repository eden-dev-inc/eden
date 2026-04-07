# Step 11: Remove Eden

**Phase**: Completion (Optional)
**Previous**: [Step 10: Decommission Source](./step-10-decommission-source.md)
**Next**: None (Migration Complete)

---

## Optional Exit

Optionally remove Eden from the stack. Your applications connect directly to the target database. **No lock-in, no residual dependencies.**

## Architecture After Eden Removal

```
┌─────┐                  ┌────────────┐
│ App │ ────────────────▶│ Target DB  │
└─────┘                  └────────────┘
```

**This is the same architecture as Step 1**, but now connected to your new database.

## Should You Remove Eden?

### Consider Keeping Eden If:

- Future migrations planned
- Multi-database architecture
- Traffic analysis needed
- Centralized access control required

### Consider Removing Eden If:

- Minimal latency critical
- Simplify architecture
- No future migrations planned

## Eden Removal Steps

### 1. Update DNS/Connection Strings

Point applications directly to the target database:

```bash
# Update DNS record
# redis.internal → target-redis.redis.azure.net
```

Or update application configuration:

```yaml
# Before (through Eden)
REDIS_HOST: eden-proxy.internal

# After (direct connection)
REDIS_HOST: target-redis.redis.azure.net
```

### 2. Remove Network Forwarding

If you used nftables for port forwarding:

```bash
sudo nft delete table ip edenswitch
```

### 3. Stop Eden Service

Stop and remove Eden using your deployment method.

### 4. Clean Up Interlay

```bash
curl -X DELETE http://localhost:8000/api/v1/interlays/redis_interlay \
  -H "Authorization: Bearer $TOKEN"
```

## Migration Journey Complete

```
✓ Step 1:  Current State (assessed)
✓ Step 2:  Deploy Eden
✓ Step 3:  Analyze traffic
✓ Step 4:  Recommend target
✓ Step 5:  Connect target
✓ Step 6:  Shadow test
✓ Step 7:  Redirect to Eden
✓ Step 8:  Canary migration
✓ Step 9:  Complete migration
✓ Step 10: Decommission source
✓ Step 11: Remove Eden (optional)

Total downtime: 0
```

## Re-Engaging Eden

If you need Eden's capabilities in the future:

1. Deploy Eden service
2. Connect target database as endpoint
3. Create interlay
4. Update connection strings
5. Ready for next migration

## Next Migration

When you're ready for your next migration:

1. Re-deploy Eden (or keep it running)
2. Return to [Step 1](./step-01-current-state.md)
3. Follow the same proven process

---

**Navigation**: [← Step 10](./step-10-decommission-source.md) | [Overview](./overview.md) | **Step 11 (Complete)**
