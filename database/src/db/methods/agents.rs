use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::sql_file;
use chrono::{DateTime, Utc};
use eden_core::error::{EpError, ResultEP};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
#[cfg(embedded_db)]
use ep_core::database::schema::Row;
use function_name::named;
use serde_json::Value;
use std::borrow::Cow;
#[cfg(not(embedded_db))]
use tokio_postgres::Row;
use uuid::Uuid;

/// A stored persistent agent loaded from the database.
#[derive(Debug, Clone)]
pub struct StoredAgent {
    pub id: Uuid,
    pub version: i32,
    pub name: String,
    pub description: Option<String>,
    pub prompt: String,
    pub cron_expression: String,
    pub status: String,
    pub scope: Value,
    pub overlap_policy: String,
    pub endpoint_uuid: Uuid,
    pub organization_uuid: Uuid,
    pub created_by: Uuid,
    pub robot_uuid: Option<Uuid>,
    pub skill_ids: Vec<String>,
    pub tool_endpoint_uuids: Vec<String>,
    pub orchestrate: bool,
    pub max_consecutive_failures: i32,
    pub consecutive_failures: i32,
    pub last_run_at: Option<DateTime<Utc>>,
    pub next_run_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// A stored agent run loaded from the database.
#[derive(Debug, Clone)]
pub struct StoredAgentRun {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub run_status: String,
    pub workflow_id: Option<Uuid>,
    pub conversation_id: Option<Uuid>,
    pub response_text: Option<String>,
    pub error: Option<String>,
    pub duration_ms: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
}

/// A stored version snapshot for an agent configuration.
#[derive(Debug, Clone)]
pub struct StoredAgentVersion {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub version: i32,
    pub prompt: String,
    pub cron_expression: String,
    pub scope: Value,
    pub skill_ids: Vec<String>,
    pub tool_endpoint_uuids: Vec<String>,
    pub orchestrate: bool,
    pub created_at: DateTime<Utc>,
    pub created_by: Uuid,
}

/// A stored notification.
#[derive(Debug, Clone)]
pub struct StoredNotification {
    pub id: Uuid,
    pub user_uuid: Uuid,
    pub organization_uuid: Uuid,
    pub agent_id: Option<Uuid>,
    pub run_id: Option<Uuid>,
    pub title: String,
    pub body: String,
    pub read: bool,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredAgentCard {
    pub id: Uuid,
    pub agent_id: Uuid,
    pub name: String,
    pub description: String,
    pub capabilities: Value,
    pub input_schema: Option<Value>,
    pub output_schema: Option<Value>,
    pub is_active: bool,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredAgentTask {
    pub id: Uuid,
    pub from_agent: Uuid,
    pub to_agent: Uuid,
    pub objective: String,
    pub context: Value,
    pub constraints: Value,
    pub status: String,
    pub result: Option<Value>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StoredAgentMetricsHourly {
    pub agent_id: Uuid,
    pub metric_hour: DateTime<Utc>,
    pub success_count: i32,
    pub failure_count: i32,
    pub avg_duration_ms: Option<i64>,
    pub p95_duration_ms: Option<i64>,
    pub total_tokens: i32,
    pub total_cost_usd: f64,
}

#[derive(Debug, Clone)]
pub struct StoredOrgMetricsSummary {
    pub total_runs: i64,
    pub success_rate: f64,
    pub avg_duration_ms: Option<f64>,
    pub total_tokens: i64,
    pub total_cost_usd: f64,
}

fn row_to_agent(row: &Row) -> StoredAgent {
    StoredAgent {
        id: row.get("id"),
        version: row.get("version"),
        name: row.get("name"),
        description: row.get("description"),
        prompt: row.get("prompt"),
        cron_expression: row.get("cron_expression"),
        status: row.get("status"),
        scope: row.get("scope"),
        overlap_policy: row.get("overlap_policy"),
        endpoint_uuid: row.get("endpoint_uuid"),
        organization_uuid: row.get("organization_uuid"),
        created_by: row.get("created_by"),
        robot_uuid: row.get("robot_uuid"),
        skill_ids: row.get::<_, Option<Vec<String>>>("skill_ids").unwrap_or_default(),
        tool_endpoint_uuids: row.get::<_, Option<Vec<String>>>("tool_endpoint_uuids").unwrap_or_default(),
        orchestrate: row.get("orchestrate"),
        max_consecutive_failures: row.get("max_consecutive_failures"),
        consecutive_failures: row.get("consecutive_failures"),
        last_run_at: row.get("last_run_at"),
        next_run_at: row.get("next_run_at"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_agent_run(row: &Row) -> StoredAgentRun {
    StoredAgentRun {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        run_status: row.get("run_status"),
        workflow_id: row.get("workflow_id"),
        conversation_id: row.get("conversation_id"),
        response_text: row.get("response_text"),
        error: row.get("error"),
        duration_ms: row.get("duration_ms"),
        created_at: row.get("created_at"),
        completed_at: row.get("completed_at"),
    }
}

fn row_to_agent_version(row: &Row) -> StoredAgentVersion {
    StoredAgentVersion {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        version: row.get("version"),
        prompt: row.get("prompt"),
        cron_expression: row.get("cron_expression"),
        scope: row.get("scope"),
        skill_ids: row.get::<_, Option<Vec<String>>>("skill_ids").unwrap_or_default(),
        tool_endpoint_uuids: row.get::<_, Option<Vec<String>>>("tool_endpoint_uuids").unwrap_or_default(),
        orchestrate: row.get("orchestrate"),
        created_at: row.get("created_at"),
        created_by: row.get("created_by"),
    }
}

fn row_to_notification(row: &Row) -> StoredNotification {
    StoredNotification {
        id: row.get("id"),
        user_uuid: row.get("user_uuid"),
        organization_uuid: row.get("organization_uuid"),
        agent_id: row.get("agent_id"),
        run_id: row.get("run_id"),
        title: row.get("title"),
        body: row.get("body"),
        read: row.get("read"),
        created_at: row.get("created_at"),
    }
}

fn row_to_agent_card(row: &Row) -> StoredAgentCard {
    StoredAgentCard {
        id: row.get("id"),
        agent_id: row.get("agent_id"),
        name: row.get("name"),
        description: row.get("description"),
        capabilities: row.get("capabilities"),
        input_schema: row.get("input_schema"),
        output_schema: row.get("output_schema"),
        is_active: row.get("is_active"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_agent_task(row: &Row) -> StoredAgentTask {
    StoredAgentTask {
        id: row.get("id"),
        from_agent: row.get("from_agent"),
        to_agent: row.get("to_agent"),
        objective: row.get("objective"),
        context: row.get("context"),
        constraints: row.get("constraints"),
        status: row.get("status"),
        result: row.get("result"),
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    }
}

fn row_to_agent_metrics_hourly(row: &Row) -> StoredAgentMetricsHourly {
    StoredAgentMetricsHourly {
        agent_id: row.get("agent_id"),
        metric_hour: row.get("metric_hour"),
        success_count: row.get("success_count"),
        failure_count: row.get("failure_count"),
        avg_duration_ms: row.get("avg_duration_ms"),
        p95_duration_ms: row.get("p95_duration_ms"),
        total_tokens: row.get("total_tokens"),
        total_cost_usd: row.get("total_cost_usd"),
    }
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    // =========================================================================
    // Agent CRUD
    // =========================================================================

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_agent(
        &self,
        id: Uuid,
        version: i32,
        name: &str,
        description: Option<&str>,
        prompt: &str,
        cron_expression: &str,
        status: &str,
        scope: &Value,
        overlap_policy: &str,
        endpoint_uuid: Uuid,
        organization_uuid: Uuid,
        created_by: Uuid,
        robot_uuid: Option<Uuid>,
        skill_ids: &[String],
        tool_endpoint_uuids: &[String],
        orchestrate: bool,
        max_consecutive_failures: i32,
        next_run_at: Option<DateTime<Utc>>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let skill_refs: Vec<&str> = skill_ids.iter().map(|s| s.as_str()).collect();
        let tool_refs: Vec<&str> = tool_endpoint_uuids.iter().map(|s| s.as_str()).collect();

        conn.execute(
            sql_file!("insert", "llm/agent"),
            &[
                &id,
                &version,
                &name,
                &description,
                &prompt,
                &cron_expression,
                &status,
                scope,
                &overlap_policy,
                &endpoint_uuid,
                &organization_uuid,
                &created_by,
                &robot_uuid,
                &skill_refs,
                &tool_refs,
                &orchestrate,
                &max_consecutive_failures,
                &next_run_at,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn load_agent(&self, agent_id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<StoredAgent> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_one(sql_file!("select", "llm/agent_by_id"), &[&agent_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row_to_agent(&row))
    }

    #[named]
    pub async fn list_agents_by_org(
        &self,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgent>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/agents_by_org"), &[&organization_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_agent).collect())
    }

    #[named]
    pub async fn load_agents_due(&self, batch_size: i64, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<StoredAgent>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/agents_due"), &[&batch_size]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_agent).collect())
    }

    /// Atomically load due agents **and** insert a `running` row for each in a
    /// single transaction so that concurrent scheduler instances cannot claim the
    /// same agent.  Returns `(StoredAgent, run_id)` pairs for each claimed agent.
    #[named]
    pub async fn claim_agents_due(&self, batch_size: i64, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Vec<(StoredAgent, Uuid)>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        #[cfg(embedded_db)]
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;
        #[cfg(not(embedded_db))]
        let mut conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let transaction = conn.transaction().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        let rows = transaction.query(sql_file!("select", "llm/agents_due"), &[&batch_size]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        let agents: Vec<StoredAgent> = rows.iter().map(row_to_agent).collect();
        let mut claimed: Vec<(StoredAgent, Uuid)> = Vec::with_capacity(agents.len());

        for agent in agents {
            let run_id = Uuid::new_v4();
            let status = "running";
            let workflow_id: Option<Uuid> = None;
            let conversation_id: Option<Uuid> = None;

            // Insert the run row inside the transaction. The unique partial
            // index on (agent_id) WHERE run_status = 'running' provides an
            // additional safety net against double-dispatch.
            let inserted = transaction
                .execute(sql_file!("insert", "llm/agent_run"), &[&run_id, &agent.id, &status, &workflow_id, &conversation_id])
                .await
                .map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::database(e)
                })?;

            if inserted > 0 {
                claimed.push((agent, run_id));
            }
        }

        transaction.commit().await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(claimed)
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_agent_version(
        &self,
        id: Uuid,
        agent_id: Uuid,
        version: i32,
        prompt: &str,
        cron_expression: &str,
        scope: &Value,
        skill_ids: &[String],
        tool_endpoint_uuids: &[String],
        orchestrate: bool,
        created_by: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let skill_refs: Vec<&str> = skill_ids.iter().map(|s| s.as_str()).collect();
        let tool_refs: Vec<&str> = tool_endpoint_uuids.iter().map(|s| s.as_str()).collect();

        conn.execute(
            sql_file!("insert", "llm/agent_version"),
            &[
                &id,
                &agent_id,
                &version,
                &prompt,
                &cron_expression,
                scope,
                &skill_refs,
                &tool_refs,
                &orchestrate,
                &created_by,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn update_agent_status(&self, agent_id: Uuid, status: &str, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "llm/agent_status"), &[&agent_id, &status]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn update_agent_definition(
        &self,
        agent_id: Uuid,
        name: &str,
        description: Option<&str>,
        prompt: &str,
        cron_expression: &str,
        scope: &Value,
        overlap_policy: &str,
        endpoint_uuid: Uuid,
        robot_uuid: Option<Uuid>,
        skill_ids: &[String],
        tool_endpoint_uuids: &[String],
        orchestrate: bool,
        max_consecutive_failures: i32,
        updated_by: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredAgent>> {
        let existing = self.load_agent(agent_id, telemetry_wrapper).await?;
        let config_changed = existing.name != name
            || existing.description.as_deref() != description
            || existing.prompt != prompt
            || existing.cron_expression != cron_expression
            || existing.scope != *scope
            || existing.overlap_policy != overlap_policy
            || existing.endpoint_uuid != endpoint_uuid
            || existing.robot_uuid != robot_uuid
            || existing.skill_ids != skill_ids
            || existing.tool_endpoint_uuids != tool_endpoint_uuids
            || existing.orchestrate != orchestrate
            || existing.max_consecutive_failures != max_consecutive_failures;

        if !config_changed {
            return Ok(Some(existing));
        }

        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let next_version = existing.version + 1;
        let skill_refs: Vec<&str> = skill_ids.iter().map(|s| s.as_str()).collect();
        let tool_refs: Vec<&str> = tool_endpoint_uuids.iter().map(|s| s.as_str()).collect();

        let row = conn
            .query_opt(
                sql_file!("update", "llm/agent_definition"),
                &[
                    &agent_id,
                    &next_version,
                    &name,
                    &description,
                    &prompt,
                    &cron_expression,
                    scope,
                    &overlap_policy,
                    &endpoint_uuid,
                    &robot_uuid,
                    &skill_refs,
                    &tool_refs,
                    &orchestrate,
                    &max_consecutive_failures,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        let updated = row.as_ref().map(row_to_agent);

        if updated.is_some() {
            self.insert_agent_version(
                Uuid::new_v4(),
                agent_id,
                next_version,
                prompt,
                cron_expression,
                scope,
                skill_ids,
                tool_endpoint_uuids,
                orchestrate,
                updated_by,
                telemetry_wrapper,
            )
            .await?;
        }

        Ok(updated)
    }

    #[named]
    pub async fn update_agent_after_run(
        &self,
        agent_id: Uuid,
        next_run_at: Option<DateTime<Utc>>,
        consecutive_failures: i32,
        status: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("update", "llm/agent_after_run"),
            &[&agent_id, &next_run_at, &consecutive_failures, &status],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    // =========================================================================
    // Agent Run CRUD
    // =========================================================================

    #[named]
    pub async fn insert_agent_run(
        &self,
        id: Uuid,
        agent_id: Uuid,
        run_status: &str,
        workflow_id: Option<Uuid>,
        conversation_id: Option<Uuid>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("insert", "llm/agent_run"), &[&id, &agent_id, &run_status, &workflow_id, &conversation_id])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(())
    }

    #[named]
    pub async fn complete_agent_run(
        &self,
        run_id: Uuid,
        status: &str,
        response_text: Option<&str>,
        error: Option<&str>,
        duration_ms: Option<i64>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("update", "llm/agent_run_complete"),
            &[&run_id, &status, &response_text, &error, &duration_ms],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn load_agent_runs(
        &self,
        agent_id: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentRun>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/agent_runs"), &[&agent_id, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_agent_run).collect())
    }

    #[named]
    pub async fn list_execution_runs_by_agent(
        &self,
        agent_id: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentRun>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "execution_runs_by_agent"), &[&agent_id, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_agent_run).collect())
    }

    #[named]
    pub async fn load_running_agent_run(
        &self,
        agent_id: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredAgentRun>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/agent_running_run"), &[&agent_id]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_agent_run))
    }

    #[named]
    pub async fn list_agent_versions(
        &self,
        agent_id: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentVersion>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/agent_versions"), &[&agent_id, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_agent_version).collect())
    }

    #[named]
    pub async fn get_agent_version(
        &self,
        agent_id: Uuid,
        version: i32,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredAgentVersion>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn.query_opt(sql_file!("select", "llm/agent_version"), &[&agent_id, &version]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(row.as_ref().map(row_to_agent_version))
    }

    // =========================================================================
    // Agent Cards / A2A Tasks
    // =========================================================================

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_agent_card(
        &self,
        id: Uuid,
        agent_id: Uuid,
        name: &str,
        description: &str,
        capabilities: &Value,
        input_schema: Option<&Value>,
        output_schema: Option<&Value>,
        is_active: bool,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredAgentCard> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                "INSERT INTO agent_cards (
                    id, agent_id, name, description, capabilities, input_schema, output_schema, is_active, created_at, updated_at
                 ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                 ON CONFLICT (agent_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    description = EXCLUDED.description,
                    capabilities = EXCLUDED.capabilities,
                    input_schema = EXCLUDED.input_schema,
                    output_schema = EXCLUDED.output_schema,
                    is_active = EXCLUDED.is_active,
                    updated_at = CURRENT_TIMESTAMP
                 RETURNING id, agent_id, name, description, capabilities, input_schema, output_schema, is_active, created_at, updated_at",
                &[
                    &id,
                    &agent_id,
                    &name,
                    &description,
                    capabilities,
                    &input_schema,
                    &output_schema,
                    &is_active,
                ],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row_to_agent_card(&row))
    }

    #[named]
    pub async fn load_agent_card(&self, agent_id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<StoredAgentCard>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "SELECT id, agent_id, name, description, capabilities, input_schema, output_schema, is_active, created_at, updated_at
                 FROM agent_cards
                 WHERE agent_id = $1
                 LIMIT 1",
                &[&agent_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.as_ref().map(row_to_agent_card))
    }

    #[named]
    pub async fn list_agent_cards(
        &self,
        organization_uuid: Uuid,
        active_only: bool,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentCard>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                "SELECT ac.id, ac.agent_id, ac.name, ac.description, ac.capabilities, ac.input_schema, ac.output_schema,
                        ac.is_active, ac.created_at, ac.updated_at
                 FROM agent_cards ac
                 JOIN llm_agents a ON a.id = ac.agent_id
                 WHERE a.organization_uuid = $1
                   AND ($2 = FALSE OR ac.is_active = TRUE)
                 ORDER BY ac.name ASC",
                &[&organization_uuid, &active_only],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(rows.iter().map(row_to_agent_card).collect())
    }

    #[allow(clippy::too_many_arguments)]
    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_agent_task(
        &self,
        id: Uuid,
        from_agent: Uuid,
        to_agent: Uuid,
        objective: &str,
        context: &Value,
        constraints: &Value,
        status: &str,
        result: Option<&Value>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            "INSERT INTO agent_tasks (
                id, from_agent, to_agent, objective, context, constraints, status, result, created_at, updated_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)",
            &[&id, &from_agent, &to_agent, &objective, context, constraints, &status, &result],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn get_agent_task(&self, task_id: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Option<StoredAgentTask>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "SELECT id, from_agent, to_agent, objective, context, constraints, status, result, created_at, updated_at
                 FROM agent_tasks
                 WHERE id = $1
                 LIMIT 1",
                &[&task_id],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.as_ref().map(row_to_agent_task))
    }

    #[named]
    pub async fn update_agent_task_status(
        &self,
        task_id: Uuid,
        status: &str,
        result: Option<&Value>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<StoredAgentTask>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_opt(
                "UPDATE agent_tasks
                 SET status = $2,
                     result = $3,
                     updated_at = CURRENT_TIMESTAMP
                 WHERE id = $1
                 RETURNING id, from_agent, to_agent, objective, context, constraints, status, result, created_at, updated_at",
                &[&task_id, &status, &result],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(row.as_ref().map(row_to_agent_task))
    }

    // =========================================================================
    // Agent Metrics
    // =========================================================================

    #[named]
    pub async fn refresh_agent_metrics_hourly(
        &self,
        agent_id: Uuid,
        metric_hour: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let hour_end = metric_hour + chrono::Duration::hours(1);
        conn.execute(
            "DELETE FROM agent_metrics_hourly
             WHERE agent_id = $1
               AND metric_hour = $2",
            &[&agent_id, &metric_hour],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        conn.execute(
            "INSERT INTO agent_metrics_hourly (
                agent_id, metric_hour, success_count, failure_count, avg_duration_ms, p95_duration_ms, total_tokens, total_cost_usd
             )
             SELECT
                $1,
                $2,
                COUNT(*) FILTER (WHERE er.state = 'completed')::INTEGER,
                COUNT(*) FILTER (WHERE er.state = 'failed')::INTEGER,
                AVG(er.duration_ms)::BIGINT,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY er.duration_ms)::BIGINT,
                COALESCE(SUM(COALESCE(usage.total_tokens, 0)), 0)::INTEGER,
                COALESCE(SUM(COALESCE(usage.total_cost_usd, 0)), 0)::NUMERIC(12, 6)
             FROM execution_runs er
             LEFT JOIN (
                SELECT run_id,
                       SUM(COALESCE(tokens_used, 0)) AS total_tokens,
                       SUM(COALESCE(cost_usd, 0)) AS total_cost_usd
                FROM run_events
                GROUP BY run_id
             ) usage ON usage.run_id = er.id
             WHERE er.agent_id = $1
               AND er.created_at >= $2
               AND er.created_at < $3
             GROUP BY 1, 2",
            &[&agent_id, &metric_hour, &hour_end],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn get_agent_metrics(
        &self,
        agent_id: Uuid,
        since: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredAgentMetricsHourly>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn
            .query(
                "SELECT agent_id, metric_hour, success_count, failure_count, avg_duration_ms, p95_duration_ms,
                        COALESCE(total_tokens, 0) AS total_tokens,
                        COALESCE(total_cost_usd, 0)::DOUBLE PRECISION AS total_cost_usd
                 FROM agent_metrics_hourly
                 WHERE agent_id = $1
                   AND metric_hour >= $2
                 ORDER BY metric_hour ASC",
                &[&agent_id, &since],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(rows.iter().map(row_to_agent_metrics_hourly).collect())
    }

    #[named]
    pub async fn get_org_metrics_summary(
        &self,
        organization_uuid: Uuid,
        since: DateTime<Utc>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<StoredOrgMetricsSummary> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let row = conn
            .query_one(
                "SELECT
                    COUNT(*)::BIGINT AS total_runs,
                    COALESCE(AVG(CASE WHEN er.state = 'completed' THEN 1.0 ELSE 0.0 END), 0.0)::DOUBLE PRECISION AS success_rate,
                    AVG(er.duration_ms)::DOUBLE PRECISION AS avg_duration_ms,
                    COALESCE(SUM(COALESCE(usage.total_tokens, 0)), 0)::BIGINT AS total_tokens,
                    COALESCE(SUM(COALESCE(usage.total_cost_usd, 0)), 0)::DOUBLE PRECISION AS total_cost_usd
                 FROM execution_runs er
                 LEFT JOIN (
                    SELECT run_id,
                           SUM(COALESCE(tokens_used, 0)) AS total_tokens,
                           SUM(COALESCE(cost_usd, 0)) AS total_cost_usd
                    FROM run_events
                    GROUP BY run_id
                 ) usage ON usage.run_id = er.id
                 WHERE er.organization_uuid = $1
                   AND er.created_at >= $2",
                &[&organization_uuid, &since],
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(StoredOrgMetricsSummary {
            total_runs: row.get("total_runs"),
            success_rate: row.get("success_rate"),
            avg_duration_ms: row.get("avg_duration_ms"),
            total_tokens: row.get("total_tokens"),
            total_cost_usd: row.get("total_cost_usd"),
        })
    }

    // =========================================================================
    // Notifications
    // =========================================================================

    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_notification(
        &self,
        id: Uuid,
        user_uuid: Uuid,
        organization_uuid: Uuid,
        agent_id: Option<Uuid>,
        run_id: Option<Uuid>,
        title: &str,
        body: &str,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "llm/notification"),
            &[&id, &user_uuid, &organization_uuid, &agent_id, &run_id, &title, &body],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    #[named]
    pub async fn load_notifications(
        &self,
        user_uuid: Uuid,
        organization_uuid: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredNotification>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "llm/notifications_for_user"), &[&user_uuid, &organization_uuid, &limit]).await.map_err(
            |e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            },
        )?;

        Ok(rows.iter().map(row_to_notification).collect())
    }

    #[named]
    pub async fn mark_notification_read(
        &self,
        notification_id: Uuid,
        user_uuid: Uuid,
        organization_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "llm/notification_read"), &[&notification_id, &user_uuid, &organization_uuid])
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;

        Ok(())
    }
}
