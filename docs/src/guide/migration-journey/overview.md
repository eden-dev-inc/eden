# Migration Journey Overview

Eden enables zero-downtime database migrations through an 11-step process. This guide walks through each phase, from initial deployment to complete migration and optional removal.

> **Note**: The diagrams in this guide show an Azure example (Azure Cache for Redis to Azure Managed Redis), but the process is identical across all cloud providers and database systems.

## The 11 Steps

| Step | Name                                                    | Phase       | Description                            |
| ---- | ------------------------------------------------------- | ----------- | -------------------------------------- |
| 1    | [Current State](./step-01-current-state.md)             | Setup       | Direct connection to existing database |
| 2    | [Deploy Eden](./step-02-deploy-eden.md)                 | Setup       | Passive listener deployment            |
| 3    | [Analyze](./step-03-analyze.md)                         | Setup       | Traffic and metadata analysis          |
| 4    | [Recommend Target](./step-04-recommend-target.md)       | Preparation | Optimal deployment recommendation      |
| 5    | [Connect Target](./step-05-connect-target.md)           | Preparation | Establish link to new database         |
| 6    | [Shadow Test](./step-06-shadow-test.md)                 | Preparation | Validate with shadow traffic           |
| 7    | [Redirect to Eden](./step-07-redirect-to-eden.md)       | Migration   | Proxy activated                        |
| 8    | [Canary Migration](./step-08-canary-migration.md)       | Migration   | Gradual traffic shift                  |
| 9    | [Complete Migration](./step-09-complete-migration.md)   | Migration   | Full cutover                           |
| 10   | [Decommission Source](./step-10-decommission-source.md) | Completion  | Clean up source                        |
| 11   | [Remove Eden](./step-11-remove-eden.md)                 | Completion  | Optional exit                          |

## Migration Phases

```
Phase 1: Setup        Steps 1-3   Deploy Eden, analyze traffic
Phase 2: Preparation  Steps 4-6   Connect target, shadow test
Phase 3: Migration    Steps 7-9   Proxy traffic, canary rollout
Phase 4: Completion   Steps 10-11 Decommission source, optionally exit
```

## Why Eden?

### Modernization

Upgrade from legacy or EOL databases to modern managed services without rewriting a single line of application code.

### Cost Arbitrage

Move to a cheaper provider or tier. Same performance, lower bill. Eden handles the migration so you capture savings immediately.

### Scale

Outgrowing your current setup? Migrate from a single instance to a clustered, sharded, or higher-throughput deployment with zero downtime.

## Key Principles

- **Zero Downtime**: Applications continue operating throughout the migration
- **Instant Rollback**: Revert to source at any step if issues arise
- **No Code Changes**: Connection string is the only change required
- **Gradual Transition**: Shift traffic incrementally to validate at each step
- **No Lock-in**: Eden can be completely removed after migration

## Next Steps

Start with [Step 1: Current State](./step-01-current-state.md) to understand your starting point, or jump to any step relevant to your migration phase.

For technical API details on migration strategies, see [Migrations](../migrations.md).
