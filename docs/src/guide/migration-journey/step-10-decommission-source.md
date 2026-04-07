# Step 10: Decommission Source

**Phase**: Completion
**Previous**: [Step 9: Complete Migration](./step-09-complete-migration.md)
**Next**: [Step 11: Remove Eden](./step-11-remove-eden.md)

---

## Clean Up

Source database decommissioned. Eden stays as the **data orchestration layer**, ready for the next migration.

## Architecture

```
┌─────┐      ┌──────┐
│ App │ ───▶ │ Eden │
└─────┘      └──────┘
                 │
                 ▼
             ┌────────────┐
             │ Target DB  │  ← Primary database
             └────────────┘

             ┌────────────┐
             │ Source DB  │  ← Decommissioned
             └────────────┘
```

## Pre-Decommission Checklist

Before removing the source database:

- [ ] Migration completed and stable for observation period
- [ ] No errors or anomalies observed
- [ ] Stakeholder approval received
- [ ] Final backup created (if required by your organization)

## Decommission Steps

### 1. Remove Source Endpoint from Eden

```bash
curl -X DELETE http://localhost:8000/api/v1/endpoints/$SOURCE_ENDPOINT_ID \
  -H "Authorization: Bearer $TOKEN"
```

### 2. Remove Source Database

Shut down the source database using your cloud provider's tools or infrastructure management.

## Rollback No Longer Available

**Important:** After source decommission, rollback to the original database is no longer possible.

If issues arise:

1. Use Eden to migrate to a new instance
2. Restore from backup to new database
3. Follow the same migration process

## Benefits of Keeping Eden

With the source decommissioned, Eden continues to provide:

- **Future migrations**: Ready instantly for next migration
- **Traffic analysis**: Continuous insights into access patterns
- **Unified observability**: Single pane of glass for all databases

## What Changes Next

In [Step 11: Remove Eden](./step-11-remove-eden.md), you can optionally remove Eden for a direct connection to the target database.

---

**Navigation**: [← Step 9](./step-09-complete-migration.md) | [Overview](./overview.md) | **Step 10** | [Step 11 →](./step-11-remove-eden.md)
