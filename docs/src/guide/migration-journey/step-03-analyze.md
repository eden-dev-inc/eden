# Step 3: Analyze

**Phase**: Setup
**Previous**: [Step 2: Deploy Eden](./step-02-deploy-eden.md)
**Next**: [Step 4: Recommend Target](./step-04-recommend-target.md)

---

## Traffic Analysis

Eden collects database metrics to inform migration planning and target sizing.

> **Note:** Analysis runs on a migration, which requires endpoints and an interlay to be configured first. The conceptual "analyze" phase happens early in planning, but the API calls shown here can be run after creating the migration in [Step 7](./step-07-redirect-to-eden.md). Alternatively, use your existing monitoring tools to gather baseline metrics before proceeding.

## Prerequisites

Before running analysis via the Eden API, you need:

1. Source endpoint created (see [Step 5](./step-05-connect-target.md))
2. Interlay created pointing to source endpoint
3. Migration created and interlay added to it

## Start Analysis

```bash
curl -X POST http://localhost:8000/api/v1/migrations/$MIGRATION_ID/analysis/start \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "configuration": {
      "duration": {"unit": "Minutes", "value": 30},
      "sample_count": 10
    }
  }'
```

**Configuration options:**

- `duration`: How long to collect data. Format: `{"unit": "Seconds|Minutes|Hours|Days", "value": N}`
- `sample_count`: Number of samples to collect (default: 10)

## Check Analysis Status

```bash
curl http://localhost:8000/api/v1/migrations/$MIGRATION_ID/analysis/info \
  -H "Authorization: Bearer $TOKEN"
```

Returns analysis records with status, samples collected, and aggregated results.

## Stop Analysis Early

```bash
curl -X POST http://localhost:8000/api/v1/migrations/$MIGRATION_ID/analysis/stop \
  -H "Authorization: Bearer $TOKEN"
```

Partial results are saved and can be viewed via `/info`.

## What Gets Collected

| Metric        | Description                            |
| ------------- | -------------------------------------- |
| `ops_per_sec` | Operations per second (traffic volume) |
| `used_memory` | Memory usage in bytes                  |
| `total_cpu`   | CPU consumption (sys + user)           |

## Aggregated Results

After analysis completes, Eden produces:

- **Workload profile**: Memory-heavy, Balanced, or Compute-heavy
- **Average ops/sec**: Across all samples
- **Average memory**: Usage in bytes
- **Average CPU**: Total CPU usage
- **Sizing recommendation**: Based on workload profile

## Analysis States

| Status       | Description                      |
| ------------ | -------------------------------- |
| `NotStarted` | Initial state                    |
| `Running`    | Actively collecting data         |
| `Completed`  | Finished with aggregated results |
| `Failed`     | Encountered an error             |

## What Changes Next

In [Step 4: Recommend Target](./step-04-recommend-target.md), you'll use the analysis results to determine the optimal target database configuration.

---

**Navigation**: [← Step 2](./step-02-deploy-eden.md) | [Overview](./overview.md) | **Step 3** | [Step 4 →](./step-04-recommend-target.md)
