pub mod apm;
pub mod apm_retention;
pub mod cases;
pub mod ci_cd;
pub mod containers;
pub mod custom;
pub mod dashboards;
pub mod downtimes;
pub mod events;
pub mod incident_services;
pub mod incident_teams;
pub mod incidents;
pub mod infrastructure;
pub mod key_management;
pub mod logs;
pub mod logs_archives;
pub mod logs_metrics;
pub mod metrics;
pub mod monitor_policies;
pub mod monitors;
pub mod notebooks;
pub mod on_call;
pub mod organizations;
pub mod powerpack;
pub mod roles;
pub mod rum;
pub mod security;
pub mod service_catalog;
pub mod service_definitions;
pub mod slos;
pub mod spans_metrics;
pub mod synthetics;
pub mod tags;
pub mod teams;
pub mod usage;
pub mod users;
pub mod workflows;

#[allow(unused_imports)]
use apm::*;
#[allow(unused_imports)]
use apm_retention::*;
#[allow(unused_imports)]
use cases::*;
#[allow(unused_imports)]
use ci_cd::*;
#[allow(unused_imports)]
use containers::*;
#[allow(unused_imports)]
use custom::*;
#[allow(unused_imports)]
use dashboards::*;
#[allow(unused_imports)]
use downtimes::*;
#[allow(unused_imports)]
use events::*;
#[allow(unused_imports)]
use incident_services::*;
#[allow(unused_imports)]
use incident_teams::*;
#[allow(unused_imports)]
use incidents::*;
#[allow(unused_imports)]
use infrastructure::*;
#[allow(unused_imports)]
use key_management::*;
#[allow(unused_imports)]
use logs::*;
#[allow(unused_imports)]
use logs_archives::*;
#[allow(unused_imports)]
use logs_metrics::*;
#[allow(unused_imports)]
use metrics::*;
#[allow(unused_imports)]
use monitor_policies::*;
#[allow(unused_imports)]
use monitors::*;
#[allow(unused_imports)]
use notebooks::*;
#[allow(unused_imports)]
use on_call::*;
#[allow(unused_imports)]
use organizations::*;
#[allow(unused_imports)]
use powerpack::*;
#[allow(unused_imports)]
use roles::*;
#[allow(unused_imports)]
use rum::*;
#[allow(unused_imports)]
use security::*;
#[allow(unused_imports)]
use service_catalog::*;
#[allow(unused_imports)]
use service_definitions::*;
#[allow(unused_imports)]
use slos::*;
#[allow(unused_imports)]
use spans_metrics::*;
#[allow(unused_imports)]
use synthetics::*;
#[allow(unused_imports)]
use tags::*;
#[allow(unused_imports)]
use teams::*;
#[allow(unused_imports)]
use usage::*;
#[allow(unused_imports)]
use users::*;
#[allow(unused_imports)]
use workflows::*;

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum DatadogApi {
    GetMetrics,
    ListMetrics,
    SubmitMetrics,
    ListEvents,
    CreateEvent,
    SearchLogs,
    ListMonitors,
    GetMonitor,
    ListDashboards,
    GetDashboard,
    GetHosts,
    ListIncidents,
    GetIncident,
    SearchSpans,
    Custom,
    ListSlos,
    GetSlo,
    GetSloHistory,
    ListDowntimes,
    ListSyntheticTests,
    SearchRumEvents,
    SearchAuditLogs,
    SearchSecuritySignals,
    SearchErrorTrackingIssues,
    ListServiceCatalog,
    ListUsers,
    ListNotebooks,
    CreateMonitor,
    UpdateMonitor,
    MuteMonitor,
    CreateDowntime,
    CreateSlo,
    ListSecurityMonitoringRules,
    CreateSecurityMonitoringRule,
    GetSecurityMonitoringRule,
    UpdateSecurityMonitoringRule,
    DeleteSecurityMonitoringRule,
    ListSecurityMonitoringFindings,
    ListCases,
    CreateCase,
    GetCase,
    UpdateCaseStatus,
    ArchiveCase,
    ListMonitorConfigPolicies,
    CreateMonitorConfigPolicy,
    GetMonitorConfigPolicy,
    UpdateMonitorConfigPolicy,
    DeleteMonitorConfigPolicy,
    GetDowntimeV2,
    UpdateDowntimeV2,
    CancelDowntimeV2,
    ListApiKeys,
    CreateApiKey,
    GetApiKey,
    UpdateApiKey,
    DeleteApiKey,
    ListAppKeys,
    CreateAppKey,
    GetAppKey,
    DeleteAppKey,
    ListHostTags,
    GetHostTags,
    CreateHostTags,
    UpdateHostTags,
    DeleteHostTags,
    ListOrgs,
    GetOrg,
    CreateChildOrg,
    QueryMetrics,
    ListActiveMetrics,
    GetMetricMetadata,
    UpdateMetricMetadata,
    GetHostTotals,
    MuteHost,
    UnmuteHost,
    CreateApiTest,
    GetApiTest,
    UpdateApiTest,
    CreateBrowserTest,
    GetBrowserTest,
    UpdateBrowserTest,
    GetSyntheticTest,
    DeleteSyntheticTests,
    TriggerSyntheticTests,
    TriggerCiTests,
    ListSyntheticGlobalVariables,
    CreateSyntheticGlobalVariable,
    DeleteSyntheticGlobalVariable,
    // logs_archives
    ListLogsArchives,
    CreateLogsArchive,
    GetLogsArchive,
    UpdateLogsArchive,
    DeleteLogsArchive,
    // logs_metrics
    ListLogsMetrics,
    CreateLogsMetric,
    GetLogsMetric,
    UpdateLogsMetric,
    DeleteLogsMetric,
    // apm_retention
    ListApmRetentionFilters,
    CreateApmRetentionFilter,
    GetApmRetentionFilter,
    UpdateApmRetentionFilter,
    DeleteApmRetentionFilter,
    // metrics tag configurations
    ListTagConfigurations,
    CreateTagConfiguration,
    UpdateTagConfiguration,
    DeleteTagConfiguration,
    // ci_cd
    ListCiTestEvents,
    SearchCiTestEvents,
    ListCiPipelineEvents,
    SearchCiPipelineEvents,
    // containers
    ListContainers,
    ListContainerImages,
    ListProcesses,
    // incidents (additional)
    CreateIncident,
    UpdateIncident,
    DeleteIncident,
    SearchIncidents,
    // incident_services
    ListIncidentServices,
    CreateIncidentService,
    GetIncidentService,
    UpdateIncidentService,
    DeleteIncidentService,
    // incident_teams
    ListIncidentTeams,
    CreateIncidentTeam,
    GetIncidentTeam,
    UpdateIncidentTeam,
    DeleteIncidentTeam,
    // teams
    ListTeams,
    CreateTeam,
    GetTeam,
    UpdateTeam,
    DeleteTeam,
    // roles
    ListRoles,
    CreateRole,
    GetRole,
    UpdateRole,
    DeleteRole,
    // users (additional)
    CreateUser,
    GetUser,
    UpdateUser,
    DisableUser,
    // workflows
    ListWorkflows,
    CreateWorkflow,
    GetWorkflow,
    UpdateWorkflow,
    DeleteWorkflow,
    RunWorkflow,
    // rum applications
    ListRumApplications,
    CreateRumApplication,
    UpdateRumApplication,
    DeleteRumApplication,
    // spans_metrics
    ListSpansMetrics,
    CreateSpansMetric,
    GetSpansMetric,
    UpdateSpansMetric,
    DeleteSpansMetric,
    // service_definitions
    ListServiceDefinitions,
    CreateOrUpdateServiceDefinition,
    GetServiceDefinition,
    DeleteServiceDefinition,
    // powerpack
    ListPowerpacks,
    CreatePowerpack,
    GetPowerpack,
    UpdatePowerpack,
    DeletePowerpack,
    // on_call
    CreateOnCallSchedule,
    GetOnCallSchedule,
    UpdateOnCallSchedule,
    DeleteOnCallSchedule,
    // usage
    GetUsageSummary,
    GetHourlyUsage,
    GetMonthlyUsageAttribution,
    // dashboards (additional)
    CreateDashboard,
    UpdateDashboard,
    DeleteDashboard,
    // monitors (additional)
    DeleteMonitor,
    ValidateMonitor,
    SearchMonitors,
    SearchMonitorGroups,
    ListMonitorDowntimes,
    // downtimes v1 (additional)
    GetDowntime,
    UpdateDowntime,
    CancelDowntime,
    CancelDowntimesByScope,
    // slos (additional)
    UpdateSlo,
    DeleteSlo,
    SearchSlo,
    // notebooks (additional)
    CreateNotebook,
    GetNotebook,
    UpdateNotebook,
    DeleteNotebook,
    // events (additional)
    GetEvent,
}

impl DatadogApi {
    pub fn name() -> String {
        "DatadogApi".to_string()
    }

    pub fn db_kind() -> String {
        "datadog".to_string()
    }
}

impl Display for DatadogApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::GetMetrics => f.write_str("get_metrics"),
            Self::ListMetrics => f.write_str("list_metrics"),
            Self::SubmitMetrics => f.write_str("submit_metrics"),
            Self::ListEvents => f.write_str("list_events"),
            Self::CreateEvent => f.write_str("create_event"),
            Self::SearchLogs => f.write_str("search_logs"),
            Self::ListMonitors => f.write_str("list_monitors"),
            Self::GetMonitor => f.write_str("get_monitor"),
            Self::ListDashboards => f.write_str("list_dashboards"),
            Self::GetDashboard => f.write_str("get_dashboard"),
            Self::GetHosts => f.write_str("get_hosts"),
            Self::ListIncidents => f.write_str("list_incidents"),
            Self::GetIncident => f.write_str("get_incident"),
            Self::SearchSpans => f.write_str("search_spans"),
            Self::Custom => f.write_str("custom"),
            Self::ListSlos => f.write_str("list_slos"),
            Self::GetSlo => f.write_str("get_slo"),
            Self::GetSloHistory => f.write_str("get_slo_history"),
            Self::ListDowntimes => f.write_str("list_downtimes"),
            Self::ListSyntheticTests => f.write_str("list_synthetic_tests"),
            Self::SearchRumEvents => f.write_str("search_rum_events"),
            Self::SearchAuditLogs => f.write_str("search_audit_logs"),
            Self::SearchSecuritySignals => f.write_str("search_security_signals"),
            Self::SearchErrorTrackingIssues => f.write_str("search_error_tracking_issues"),
            Self::ListServiceCatalog => f.write_str("list_service_catalog"),
            Self::ListUsers => f.write_str("list_users"),
            Self::ListNotebooks => f.write_str("list_notebooks"),
            Self::CreateMonitor => f.write_str("create_monitor"),
            Self::UpdateMonitor => f.write_str("update_monitor"),
            Self::MuteMonitor => f.write_str("mute_monitor"),
            Self::CreateDowntime => f.write_str("create_downtime"),
            Self::CreateSlo => f.write_str("create_slo"),
            Self::ListSecurityMonitoringRules => f.write_str("list_security_monitoring_rules"),
            Self::CreateSecurityMonitoringRule => f.write_str("create_security_monitoring_rule"),
            Self::GetSecurityMonitoringRule => f.write_str("get_security_monitoring_rule"),
            Self::UpdateSecurityMonitoringRule => f.write_str("update_security_monitoring_rule"),
            Self::DeleteSecurityMonitoringRule => f.write_str("delete_security_monitoring_rule"),
            Self::ListSecurityMonitoringFindings => f.write_str("list_security_monitoring_findings"),
            Self::ListCases => f.write_str("list_cases"),
            Self::CreateCase => f.write_str("create_case"),
            Self::GetCase => f.write_str("get_case"),
            Self::UpdateCaseStatus => f.write_str("update_case_status"),
            Self::ArchiveCase => f.write_str("archive_case"),
            Self::ListMonitorConfigPolicies => f.write_str("list_monitor_config_policies"),
            Self::CreateMonitorConfigPolicy => f.write_str("create_monitor_config_policy"),
            Self::GetMonitorConfigPolicy => f.write_str("get_monitor_config_policy"),
            Self::UpdateMonitorConfigPolicy => f.write_str("update_monitor_config_policy"),
            Self::DeleteMonitorConfigPolicy => f.write_str("delete_monitor_config_policy"),
            Self::GetDowntimeV2 => f.write_str("get_downtime_v2"),
            Self::UpdateDowntimeV2 => f.write_str("update_downtime_v2"),
            Self::CancelDowntimeV2 => f.write_str("cancel_downtime_v2"),
            Self::ListApiKeys => f.write_str("list_api_keys"),
            Self::CreateApiKey => f.write_str("create_api_key"),
            Self::GetApiKey => f.write_str("get_api_key"),
            Self::UpdateApiKey => f.write_str("update_api_key"),
            Self::DeleteApiKey => f.write_str("delete_api_key"),
            Self::ListAppKeys => f.write_str("list_app_keys"),
            Self::CreateAppKey => f.write_str("create_app_key"),
            Self::GetAppKey => f.write_str("get_app_key"),
            Self::DeleteAppKey => f.write_str("delete_app_key"),
            Self::ListHostTags => f.write_str("list_host_tags"),
            Self::GetHostTags => f.write_str("get_host_tags"),
            Self::CreateHostTags => f.write_str("create_host_tags"),
            Self::UpdateHostTags => f.write_str("update_host_tags"),
            Self::DeleteHostTags => f.write_str("delete_host_tags"),
            Self::ListOrgs => f.write_str("list_orgs"),
            Self::GetOrg => f.write_str("get_org"),
            Self::CreateChildOrg => f.write_str("create_child_org"),
            Self::QueryMetrics => f.write_str("query_metrics"),
            Self::ListActiveMetrics => f.write_str("list_active_metrics"),
            Self::GetMetricMetadata => f.write_str("get_metric_metadata"),
            Self::UpdateMetricMetadata => f.write_str("update_metric_metadata"),
            Self::GetHostTotals => f.write_str("get_host_totals"),
            Self::MuteHost => f.write_str("mute_host"),
            Self::UnmuteHost => f.write_str("unmute_host"),
            Self::CreateApiTest => f.write_str("create_api_test"),
            Self::GetApiTest => f.write_str("get_api_test"),
            Self::UpdateApiTest => f.write_str("update_api_test"),
            Self::CreateBrowserTest => f.write_str("create_browser_test"),
            Self::GetBrowserTest => f.write_str("get_browser_test"),
            Self::UpdateBrowserTest => f.write_str("update_browser_test"),
            Self::GetSyntheticTest => f.write_str("get_synthetic_test"),
            Self::DeleteSyntheticTests => f.write_str("delete_synthetic_tests"),
            Self::TriggerSyntheticTests => f.write_str("trigger_synthetic_tests"),
            Self::TriggerCiTests => f.write_str("trigger_ci_tests"),
            Self::ListSyntheticGlobalVariables => f.write_str("list_synthetic_global_variables"),
            Self::CreateSyntheticGlobalVariable => f.write_str("create_synthetic_global_variable"),
            Self::DeleteSyntheticGlobalVariable => f.write_str("delete_synthetic_global_variable"),
            Self::ListLogsArchives => f.write_str("list_logs_archives"),
            Self::CreateLogsArchive => f.write_str("create_logs_archive"),
            Self::GetLogsArchive => f.write_str("get_logs_archive"),
            Self::UpdateLogsArchive => f.write_str("update_logs_archive"),
            Self::DeleteLogsArchive => f.write_str("delete_logs_archive"),
            Self::ListLogsMetrics => f.write_str("list_logs_metrics"),
            Self::CreateLogsMetric => f.write_str("create_logs_metric"),
            Self::GetLogsMetric => f.write_str("get_logs_metric"),
            Self::UpdateLogsMetric => f.write_str("update_logs_metric"),
            Self::DeleteLogsMetric => f.write_str("delete_logs_metric"),
            Self::ListApmRetentionFilters => f.write_str("list_apm_retention_filters"),
            Self::CreateApmRetentionFilter => f.write_str("create_apm_retention_filter"),
            Self::GetApmRetentionFilter => f.write_str("get_apm_retention_filter"),
            Self::UpdateApmRetentionFilter => f.write_str("update_apm_retention_filter"),
            Self::DeleteApmRetentionFilter => f.write_str("delete_apm_retention_filter"),
            Self::ListTagConfigurations => f.write_str("list_tag_configurations"),
            Self::CreateTagConfiguration => f.write_str("create_tag_configuration"),
            Self::UpdateTagConfiguration => f.write_str("update_tag_configuration"),
            Self::DeleteTagConfiguration => f.write_str("delete_tag_configuration"),
            Self::ListCiTestEvents => f.write_str("list_ci_test_events"),
            Self::SearchCiTestEvents => f.write_str("search_ci_test_events"),
            Self::ListCiPipelineEvents => f.write_str("list_ci_pipeline_events"),
            Self::SearchCiPipelineEvents => f.write_str("search_ci_pipeline_events"),
            Self::ListContainers => f.write_str("list_containers"),
            Self::ListContainerImages => f.write_str("list_container_images"),
            Self::ListProcesses => f.write_str("list_processes"),
            Self::CreateIncident => f.write_str("create_incident"),
            Self::UpdateIncident => f.write_str("update_incident"),
            Self::DeleteIncident => f.write_str("delete_incident"),
            Self::SearchIncidents => f.write_str("search_incidents"),
            Self::ListIncidentServices => f.write_str("list_incident_services"),
            Self::CreateIncidentService => f.write_str("create_incident_service"),
            Self::GetIncidentService => f.write_str("get_incident_service"),
            Self::UpdateIncidentService => f.write_str("update_incident_service"),
            Self::DeleteIncidentService => f.write_str("delete_incident_service"),
            Self::ListIncidentTeams => f.write_str("list_incident_teams"),
            Self::CreateIncidentTeam => f.write_str("create_incident_team"),
            Self::GetIncidentTeam => f.write_str("get_incident_team"),
            Self::UpdateIncidentTeam => f.write_str("update_incident_team"),
            Self::DeleteIncidentTeam => f.write_str("delete_incident_team"),
            Self::ListTeams => f.write_str("list_teams"),
            Self::CreateTeam => f.write_str("create_team"),
            Self::GetTeam => f.write_str("get_team"),
            Self::UpdateTeam => f.write_str("update_team"),
            Self::DeleteTeam => f.write_str("delete_team"),
            Self::ListRoles => f.write_str("list_roles"),
            Self::CreateRole => f.write_str("create_role"),
            Self::GetRole => f.write_str("get_role"),
            Self::UpdateRole => f.write_str("update_role"),
            Self::DeleteRole => f.write_str("delete_role"),
            Self::CreateUser => f.write_str("create_user"),
            Self::GetUser => f.write_str("get_user"),
            Self::UpdateUser => f.write_str("update_user"),
            Self::DisableUser => f.write_str("disable_user"),
            Self::ListWorkflows => f.write_str("list_workflows"),
            Self::CreateWorkflow => f.write_str("create_workflow"),
            Self::GetWorkflow => f.write_str("get_workflow"),
            Self::UpdateWorkflow => f.write_str("update_workflow"),
            Self::DeleteWorkflow => f.write_str("delete_workflow"),
            Self::RunWorkflow => f.write_str("run_workflow"),
            Self::ListRumApplications => f.write_str("list_rum_applications"),
            Self::CreateRumApplication => f.write_str("create_rum_application"),
            Self::UpdateRumApplication => f.write_str("update_rum_application"),
            Self::DeleteRumApplication => f.write_str("delete_rum_application"),
            Self::ListSpansMetrics => f.write_str("list_spans_metrics"),
            Self::CreateSpansMetric => f.write_str("create_spans_metric"),
            Self::GetSpansMetric => f.write_str("get_spans_metric"),
            Self::UpdateSpansMetric => f.write_str("update_spans_metric"),
            Self::DeleteSpansMetric => f.write_str("delete_spans_metric"),
            Self::ListServiceDefinitions => f.write_str("list_service_definitions"),
            Self::CreateOrUpdateServiceDefinition => f.write_str("create_or_update_service_definition"),
            Self::GetServiceDefinition => f.write_str("get_service_definition"),
            Self::DeleteServiceDefinition => f.write_str("delete_service_definition"),
            Self::ListPowerpacks => f.write_str("list_powerpacks"),
            Self::CreatePowerpack => f.write_str("create_powerpack"),
            Self::GetPowerpack => f.write_str("get_powerpack"),
            Self::UpdatePowerpack => f.write_str("update_powerpack"),
            Self::DeletePowerpack => f.write_str("delete_powerpack"),
            Self::CreateOnCallSchedule => f.write_str("create_on_call_schedule"),
            Self::GetOnCallSchedule => f.write_str("get_on_call_schedule"),
            Self::UpdateOnCallSchedule => f.write_str("update_on_call_schedule"),
            Self::DeleteOnCallSchedule => f.write_str("delete_on_call_schedule"),
            Self::GetUsageSummary => f.write_str("get_usage_summary"),
            Self::GetHourlyUsage => f.write_str("get_hourly_usage"),
            Self::GetMonthlyUsageAttribution => f.write_str("get_monthly_usage_attribution"),
            Self::CreateDashboard => f.write_str("create_dashboard"),
            Self::UpdateDashboard => f.write_str("update_dashboard"),
            Self::DeleteDashboard => f.write_str("delete_dashboard"),
            Self::DeleteMonitor => f.write_str("delete_monitor"),
            Self::ValidateMonitor => f.write_str("validate_monitor"),
            Self::SearchMonitors => f.write_str("search_monitors"),
            Self::SearchMonitorGroups => f.write_str("search_monitor_groups"),
            Self::ListMonitorDowntimes => f.write_str("list_monitor_downtimes"),
            Self::GetDowntime => f.write_str("get_downtime"),
            Self::UpdateDowntime => f.write_str("update_downtime"),
            Self::CancelDowntime => f.write_str("cancel_downtime"),
            Self::CancelDowntimesByScope => f.write_str("cancel_downtimes_by_scope"),
            Self::UpdateSlo => f.write_str("update_slo"),
            Self::DeleteSlo => f.write_str("delete_slo"),
            Self::SearchSlo => f.write_str("search_slo"),
            Self::CreateNotebook => f.write_str("create_notebook"),
            Self::GetNotebook => f.write_str("get_notebook"),
            Self::UpdateNotebook => f.write_str("update_notebook"),
            Self::DeleteNotebook => f.write_str("delete_notebook"),
            Self::GetEvent => f.write_str("get_event"),
        }
    }
}
