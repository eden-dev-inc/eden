# Step 4: Recommend Target

**Phase**: Preparation
**Previous**: [Step 3: Analyze](./step-03-analyze.md)
**Next**: [Step 5: Connect Target](./step-05-connect-target.md)

---

## Eden's Workload Analysis

After running analysis in Step 3, Eden provides target recommendations based on your workload characteristics.

### Workload Profile Classification

Eden classifies your workload into one of three profiles based on the **ops/sec to memory ratio**:

| Profile      | Ratio | Characteristics                         | Recommended SKU              |
| ------------ | ----- | --------------------------------------- | ---------------------------- |
| **Memory**   | < 1   | High memory usage, low throughput       | Memory-optimized (M-series)  |
| **Balanced** | 1-50  | Moderate ops/sec relative to memory     | General Purpose (P-series)   |
| **Compute**  | > 50  | High throughput, light memory footprint | Compute-optimized (C-series) |

### Getting Recommendations

The analysis results from Step 3 include sizing recommendations:

```bash
curl http://localhost:8000/api/v1/analysis/$SOURCE_UUID/info \
  -H "Authorization: Bearer $TOKEN"
```

Response includes:

```json
{
  "aggregated_results": {
    "workload_profile": "balanced",
    "avg_ops_per_sec": 15000.0,
    "used_memory_mb": 512.0,
    "workload_ratio": 29.3,
    "sizing_recommendation": "General Purpose (P-series)"
  }
}
```

## Additional Considerations

### Protocol Compatibility

Eden supports protocol-compatible migrations (Redis → Redis, PostgreSQL → PostgreSQL). The target must support the same protocol and commands your applications use.

### Sizing Headroom

Eden's recommendation is based on current usage. When provisioning, consider:

- Memory: Current usage + 20-30% headroom for growth
- Connections: Match or exceed current pool sizes
- Network throughput: Based on ops/sec analysis

### Provider Options

- AWS ElastiCache
- Azure Cache for Redis / Azure Managed Redis
- GCP Memorystore
- Self-hosted

## Provision the Target

Provision your target database based on Eden's recommendation before proceeding to the next step.

## What Changes Next

In [Step 5: Connect Target](./step-05-connect-target.md), you'll configure Eden endpoints for both source and target databases.

---

**Navigation**: [← Step 3](./step-03-analyze.md) | [Overview](./overview.md) | **Step 4** | [Step 5 →](./step-05-connect-target.md)
