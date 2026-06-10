pub mod custom;

pub mod advisor;
pub mod apim;
pub mod app_configuration;
pub mod app_insights;
pub mod app_service;
pub mod arc;
pub mod attestation;
pub mod authorization;
pub mod automation;
pub mod batch;
pub mod bot_service;
pub mod cdn;
pub mod chaos;
pub mod cognitive;
pub mod communication;
pub mod compute;
pub mod confidential_ledger;
pub mod container;
pub mod container_apps;
pub mod container_instance;
pub mod container_registry;
pub mod cosmosdb;
pub mod cost_management;
pub mod data_factory;
pub mod databricks;
pub mod ddos_protection;
pub mod dev_center;
pub mod digital_twins;
pub mod dns;
pub mod eventgrid;
pub mod eventhub;
pub mod fluid_relay;
pub mod frontdoor;
pub mod functions;
pub mod hdinsight;
pub mod healthcare;
pub mod iot_hub;
pub mod keyvault;
pub mod kusto;
pub mod load_testing;
pub mod logic_apps;
pub mod machine_learning;
pub mod maintenance;
pub mod managed_applications;
pub mod managed_cassandra;
pub mod managed_grafana;
pub mod managed_identity;
pub mod maps;
pub mod migrate;
pub mod monitor;
pub mod mysql;
pub mod netapp;
pub mod network;
pub mod notification_hubs;
pub mod orbital;
pub mod policy;
pub mod postgresql;
pub mod purview;
pub mod quantum;
pub mod recovery_services;
pub mod redis_cache;
pub mod relay;
pub mod resource;
pub mod search;
pub mod security;
pub mod service_fabric;
pub mod servicebus;
pub mod signalr;
pub mod spring_apps;
pub mod sql;
pub mod stack_hci;
pub mod static_web_apps;
pub mod storage;
pub mod stream_analytics;
pub mod synapse;
pub mod vmware;
pub mod web_pubsub;

#[allow(unused_imports)]
use advisor::*;
#[allow(unused_imports)]
use apim::*;
#[allow(unused_imports)]
use app_configuration::*;
#[allow(unused_imports)]
use app_insights::*;
#[allow(unused_imports)]
use app_service::*;
#[allow(unused_imports)]
use arc::*;
#[allow(unused_imports)]
use attestation::*;
#[allow(unused_imports)]
use authorization::*;
#[allow(unused_imports)]
use automation::*;
#[allow(unused_imports)]
use batch::*;
#[allow(unused_imports)]
use bot_service::*;
#[allow(unused_imports)]
use cdn::*;
#[allow(unused_imports)]
use chaos::*;
#[allow(unused_imports)]
use cognitive::*;
#[allow(unused_imports)]
use communication::*;
#[allow(unused_imports)]
use compute::*;
#[allow(unused_imports)]
use confidential_ledger::*;
#[allow(unused_imports)]
use container::*;
#[allow(unused_imports)]
use container_apps::*;
#[allow(unused_imports)]
use container_instance::*;
#[allow(unused_imports)]
use container_registry::*;
#[allow(unused_imports)]
use cosmosdb::*;
#[allow(unused_imports)]
use cost_management::*;
#[allow(unused_imports)]
use custom::*;
#[allow(unused_imports)]
use data_factory::*;
#[allow(unused_imports)]
use databricks::*;
#[allow(unused_imports)]
use ddos_protection::*;
#[allow(unused_imports)]
use dev_center::*;
#[allow(unused_imports)]
use digital_twins::*;
#[allow(unused_imports)]
use dns::*;
#[allow(unused_imports)]
use eventgrid::*;
#[allow(unused_imports)]
use eventhub::*;
#[allow(unused_imports)]
use fluid_relay::*;
#[allow(unused_imports)]
use frontdoor::*;
#[allow(unused_imports)]
use functions::*;
#[allow(unused_imports)]
use hdinsight::*;
#[allow(unused_imports)]
use healthcare::*;
#[allow(unused_imports)]
use iot_hub::*;
#[allow(unused_imports)]
use keyvault::*;
#[allow(unused_imports)]
use kusto::*;
#[allow(unused_imports)]
use load_testing::*;
#[allow(unused_imports)]
use logic_apps::*;
#[allow(unused_imports)]
use machine_learning::*;
#[allow(unused_imports)]
use maintenance::*;
#[allow(unused_imports)]
use managed_applications::*;
#[allow(unused_imports)]
use managed_cassandra::*;
#[allow(unused_imports)]
use managed_grafana::*;
#[allow(unused_imports)]
use managed_identity::*;
#[allow(unused_imports)]
use maps::*;
#[allow(unused_imports)]
use migrate::*;
#[allow(unused_imports)]
use monitor::*;
#[allow(unused_imports)]
use mysql::*;
#[allow(unused_imports)]
use netapp::*;
#[allow(unused_imports)]
use network::*;
#[allow(unused_imports)]
use notification_hubs::*;
#[allow(unused_imports)]
use orbital::*;
#[allow(unused_imports)]
use policy::*;
#[allow(unused_imports)]
use postgresql::*;
#[allow(unused_imports)]
use purview::*;
#[allow(unused_imports)]
use quantum::*;
#[allow(unused_imports)]
use recovery_services::*;
#[allow(unused_imports)]
use redis_cache::*;
#[allow(unused_imports)]
use relay::*;
#[allow(unused_imports)]
use resource::*;
#[allow(unused_imports)]
use search::*;
#[allow(unused_imports)]
use security::*;
#[allow(unused_imports)]
use service_fabric::*;
#[allow(unused_imports)]
use servicebus::*;
#[allow(unused_imports)]
use signalr::*;
#[allow(unused_imports)]
use spring_apps::*;
#[allow(unused_imports)]
use sql::*;
#[allow(unused_imports)]
use stack_hci::*;
#[allow(unused_imports)]
use static_web_apps::*;
#[allow(unused_imports)]
use storage::*;
#[allow(unused_imports)]
use stream_analytics::*;
#[allow(unused_imports)]
use synapse::*;
#[allow(unused_imports)]
use vmware::*;
#[allow(unused_imports)]
use web_pubsub::*;

use std::fmt::Display;

use serde::{Deserialize, Serialize};
use strum_macros::EnumIter;
use utoipa::ToSchema;

#[derive(Serialize, Deserialize, Hash, Debug, Clone, Copy, PartialEq, Eq, Default, EnumIter, ToSchema)]
pub enum AzureApi {
    #[default]
    Custom,
    // Compute (Microsoft.Compute)
    ComputeListVirtualMachines,
    ComputeGetVirtualMachine,
    ComputeCreateOrUpdateVirtualMachine,
    ComputeDeleteVirtualMachine,
    ComputeStartVirtualMachine,
    ComputeStopVirtualMachine,
    ComputeRestartVirtualMachine,
    ComputeDeallocateVirtualMachine,
    ComputeListVmss,
    ComputeGetVmss,
    ComputeListDisks,
    ComputeGetDisk,
    ComputeCreateOrUpdateDisk,
    ComputeDeleteDisk,
    ComputeListImages,
    ComputeGetImage,
    ComputeListAvailabilitySets,
    ComputeGetAvailabilitySet,
    ComputeCreateOrUpdateVmss,
    ComputeDeleteVmss,
    ComputeListVmSizes,
    ComputeListSnapshots,
    ComputeGetSnapshot,
    ComputeCreateOrUpdateSnapshot,
    ComputeDeleteSnapshot,
    ComputeListProximityPlacementGroups,
    ComputeGetProximityPlacementGroup,
    ComputeListDedicatedHosts,
    ComputeListGalleries,
    ComputeGetGallery,
    ComputeListGalleryImages,
    // Network (Microsoft.Network)
    NetworkListVirtualNetworks,
    NetworkGetVirtualNetwork,
    NetworkCreateOrUpdateVirtualNetwork,
    NetworkListSubnets,
    NetworkGetSubnet,
    NetworkListNetworkSecurityGroups,
    NetworkGetNetworkSecurityGroup,
    NetworkCreateOrUpdateNetworkSecurityGroup,
    NetworkListPublicIpAddresses,
    NetworkGetPublicIpAddress,
    NetworkCreateOrUpdatePublicIpAddress,
    NetworkDeletePublicIpAddress,
    NetworkListLoadBalancers,
    NetworkGetLoadBalancer,
    NetworkListNetworkInterfaces,
    NetworkGetNetworkInterface,
    NetworkListApplicationGateways,
    NetworkGetApplicationGateway,
    NetworkListRouteTables,
    NetworkGetRouteTable,
    NetworkCreateOrUpdateRouteTable,
    NetworkListVpnGateways,
    NetworkGetVpnGateway,
    NetworkListPrivateEndpoints,
    NetworkGetPrivateEndpoint,
    NetworkListNatGateways,
    NetworkGetNatGateway,
    NetworkCreateOrUpdateNatGateway,
    NetworkDeleteNatGateway,
    NetworkListFirewalls,
    NetworkGetFirewall,
    NetworkCreateOrUpdateLoadBalancer,
    NetworkDeleteLoadBalancer,
    NetworkDeleteVirtualNetwork,
    NetworkCreateOrUpdateSubnet,
    NetworkDeleteSubnet,
    NetworkListSecurityRules,
    NetworkCreateOrUpdateSecurityRule,
    NetworkDeleteSecurityRule,
    NetworkDeletePrivateEndpoint,
    NetworkCreateOrUpdateVpnGateway,
    NetworkDeleteVpnGateway,
    NetworkListExpressRouteCircuits,
    NetworkGetExpressRouteCircuit,
    NetworkListPrivateDnsZones,
    NetworkGetPrivateDnsZone,
    NetworkCreateOrUpdatePrivateDnsZone,
    NetworkListWafPolicies,
    NetworkGetWafPolicy,
    NetworkListBastionHosts,
    NetworkGetBastionHost,
    NetworkListTrafficManagerProfiles,
    NetworkGetTrafficManagerProfile,
    NetworkCreateOrUpdateApplicationGateway,
    NetworkDeleteApplicationGateway,
    NetworkCreateOrUpdateFirewall,
    NetworkDeleteFirewall,
    NetworkCreateOrUpdateBastionHost,
    NetworkDeleteBastionHost,
    NetworkCreateOrUpdateTrafficManagerProfile,
    NetworkDeleteTrafficManagerProfile,
    NetworkCreateOrUpdateExpressRouteCircuit,
    NetworkDeleteExpressRouteCircuit,
    NetworkCreateOrUpdateWafPolicy,
    NetworkDeleteWafPolicy,
    NetworkCreateOrUpdatePrivateEndpoint,
    NetworkListNetworkWatchers,
    NetworkGetNetworkWatcher,
    NetworkCreateOrUpdateNetworkWatcher,
    NetworkDeleteNetworkWatcher,
    NetworkGetNetworkWatcherTopology,
    NetworkListFlowLogs,
    NetworkListFirewallPolicies,
    NetworkGetFirewallPolicy,
    NetworkCreateOrUpdateFirewallPolicy,
    NetworkDeleteFirewallPolicy,
    // DDoS Protection (Microsoft.Network)
    DdosProtectionListPlans,
    DdosProtectionGetPlan,
    DdosProtectionCreateOrUpdatePlan,
    DdosProtectionDeletePlan,
    // Resource (Microsoft.Resources)
    ResourceListSubscriptions,
    ResourceGetSubscription,
    ResourceListResourceGroups,
    ResourceGetResourceGroup,
    ResourceCreateOrUpdateResourceGroup,
    ResourceDeleteResourceGroup,
    ResourceListResources,
    // Storage (Microsoft.Storage)
    StorageListAccounts,
    StorageGetAccount,
    StorageCreateAccount,
    StorageDeleteAccount,
    StorageListAccountKeys,
    StorageListContainers,
    StorageListBlobs,
    StorageCreateContainer,
    StorageDeleteContainer,
    StorageGetContainer,
    StorageListFileShares,
    StorageGetFileShare,
    StorageCreateFileShare,
    StorageListQueueServices,
    StorageRegenerateAccountKey,
    StorageUpdateAccount,
    StorageGetBlobServiceProperties,
    StorageSetBlobServiceProperties,
    // Key Vault (Microsoft.KeyVault)
    KeyVaultListVaults,
    KeyVaultGetVault,
    KeyVaultCreateOrUpdateVault,
    KeyVaultGetSecret,
    KeyVaultSetSecret,
    KeyVaultListSecrets,
    KeyVaultGetKey,
    KeyVaultListKeys,
    KeyVaultDeleteSecret,
    KeyVaultRecoverDeletedSecret,
    KeyVaultListCertificates,
    KeyVaultGetCertificate,
    KeyVaultCreateKey,
    KeyVaultDeleteKey,
    KeyVaultDeleteVault,
    KeyVaultPurgeDeletedSecret,
    KeyVaultBackupSecret,
    KeyVaultCreateOrImportCertificate,
    KeyVaultDeleteCertificate,
    KeyVaultBackupKey,
    // Authorization (Microsoft.Authorization)
    AuthorizationListRoleAssignments,
    AuthorizationCreateRoleAssignment,
    AuthorizationDeleteRoleAssignment,
    AuthorizationListRoleDefinitions,
    // SQL (Microsoft.Sql)
    SqlListServers,
    SqlGetServer,
    SqlListDatabases,
    SqlGetDatabase,
    SqlCreateOrUpdateDatabase,
    SqlDeleteDatabase,
    SqlListFirewallRules,
    SqlCreateOrUpdateServer,
    SqlDeleteServer,
    SqlCreateOrUpdateFirewallRule,
    SqlDeleteFirewallRule,
    SqlListElasticPools,
    SqlGetElasticPool,
    SqlCreateOrUpdateElasticPool,
    SqlDeleteElasticPool,
    // Cosmos DB (Microsoft.DocumentDB)
    CosmosDbListDatabaseAccounts,
    CosmosDbGetDatabaseAccount,
    CosmosDbListSqlDatabases,
    CosmosDbCreateOrUpdateSqlDatabase,
    CosmosDbDeleteSqlDatabase,
    CosmosDbListSqlContainers,
    CosmosDbGetSqlContainer,
    CosmosDbCreateOrUpdateSqlContainer,
    CosmosDbDeleteSqlContainer,
    CosmosDbListMongoDatabases,
    CosmosDbCreateOrUpdateMongoDatabase,
    CosmosDbUpdateSqlDatabaseThroughput,
    CosmosDbDeleteMongoDatabase,
    CosmosDbListTableResources,
    CosmosDbListCassandraKeyspaces,
    CosmosDbListGremlinDatabases,
    CosmosDbFailoverDatabaseAccount,
    CosmosDbListConnectionStrings,
    CosmosDbListKeys,
    // App Service (Microsoft.Web)
    AppServiceListWebApps,
    AppServiceGetWebApp,
    AppServiceCreateOrUpdateWebApp,
    AppServiceDeleteWebApp,
    AppServiceRestartWebApp,
    AppServiceStopWebApp,
    AppServiceStartWebApp,
    AppServiceListConfigurations,
    AppServiceListDeploymentSlots,
    AppServiceGetDeploymentSlot,
    AppServiceSwapSlot,
    AppServiceListCustomDomains,
    AppServiceGetSourceControl,
    AppServiceListPlans,
    AppServiceGetPlan,
    AppServiceCreateOrUpdatePlan,
    AppServiceDeletePlan,
    AppServiceDeleteDeploymentSlot,
    AppServiceListPublishProfiles,
    AppServiceListDeployments,
    AppServiceListHybridConnections,
    // Container (Microsoft.ContainerService)
    ContainerListManagedClusters,
    ContainerGetManagedCluster,
    ContainerCreateOrUpdateManagedCluster,
    ContainerDeleteManagedCluster,
    ContainerListAgentPools,
    ContainerGetAgentPool,
    ContainerListClusterAdminCredentials,
    ContainerListClusterUserCredentials,
    ContainerCreateOrUpdateAgentPool,
    ContainerDeleteAgentPool,
    ContainerStartManagedCluster,
    ContainerStopManagedCluster,
    ContainerListAvailableUpgrades,
    ContainerRunCommand,
    ContainerRotateClusterCertificates,
    // Functions (Microsoft.Web kind=functionapp)
    FunctionsListFunctionApps,
    FunctionsGetFunctionApp,
    FunctionsListFunctions,
    FunctionsCreateOrUpdateFunctionApp,
    FunctionsDeleteFunctionApp,
    FunctionsStartFunctionApp,
    FunctionsStopFunctionApp,
    FunctionsRestartFunctionApp,
    // Service Bus (Microsoft.ServiceBus)
    ServiceBusListNamespaces,
    ServiceBusGetNamespace,
    ServiceBusListQueues,
    ServiceBusGetQueue,
    ServiceBusListTopics,
    ServiceBusCreateOrUpdateQueue,
    ServiceBusCreateOrUpdateTopic,
    ServiceBusDeleteQueue,
    ServiceBusDeleteTopic,
    ServiceBusListSubscriptions,
    ServiceBusGetSubscription,
    ServiceBusCreateOrUpdateNamespace,
    ServiceBusDeleteNamespace,
    // Event Hub (Microsoft.EventHub)
    EventHubListNamespaces,
    EventHubGetNamespace,
    EventHubListEventHubs,
    EventHubGetEventHub,
    EventHubCreateOrUpdateNamespace,
    EventHubCreateOrUpdateEventHub,
    EventHubDeleteEventHub,
    EventHubDeleteNamespace,
    EventHubListConsumerGroups,
    EventHubGetConsumerGroup,
    // Monitor (Microsoft.Insights)
    MonitorListMetricDefinitions,
    MonitorListMetrics,
    MonitorListActivityLogs,
    MonitorListAlertRules,
    MonitorGetAlertRule,
    MonitorListDiagnosticSettings,
    MonitorGetDiagnosticSetting,
    MonitorCreateOrUpdateAlertRule,
    MonitorDeleteAlertRule,
    MonitorListActionGroups,
    MonitorGetActionGroup,
    MonitorCreateOrUpdateActionGroup,
    MonitorListLogAnalyticsWorkspaces,
    MonitorGetLogAnalyticsWorkspace,
    MonitorCreateOrUpdateDiagnosticSetting,
    MonitorListAutoscaleSettings,
    MonitorGetAutoscaleSetting,
    MonitorCreateOrUpdateAutoscaleSetting,
    MonitorQueryLogAnalytics,
    MonitorCreateOrUpdateLogAnalyticsWorkspace,
    MonitorDeleteLogAnalyticsWorkspace,
    // DNS (Microsoft.Network/dnszones)
    DnsListZones,
    DnsGetZone,
    DnsListRecordSets,
    DnsGetRecordSet,
    DnsCreateOrUpdateRecordSet,
    DnsDeleteRecordSet,
    // CDN (Microsoft.Cdn)
    CdnListProfiles,
    CdnGetProfile,
    CdnListEndpoints,
    CdnGetEndpoint,
    CdnPurgeEndpoint,
    // Front Door (Microsoft.Network/frontDoors)
    FrontDoorListFrontDoors,
    FrontDoorGetFrontDoor,
    FrontDoorCreateOrUpdateFrontDoor,
    FrontDoorDeleteFrontDoor,
    // Cognitive Services (Microsoft.CognitiveServices)
    CognitiveListAccounts,
    CognitiveGetAccount,
    CognitiveCreateAccount,
    CognitiveDeleteAccount,
    CognitiveListKeys,
    // Redis Cache (Microsoft.Cache)
    RedisCacheListAll,
    RedisCacheGet,
    RedisCacheCreateOrUpdate,
    RedisCacheDelete,
    RedisCacheListKeys,
    RedisCacheRegenerateKey,
    // PostgreSQL (Microsoft.DBforPostgreSQL)
    PostgresqlListServers,
    PostgresqlGetServer,
    PostgresqlCreateOrUpdateServer,
    PostgresqlDeleteServer,
    PostgresqlListDatabases,
    PostgresqlGetDatabase,
    PostgresqlCreateOrUpdateDatabase,
    PostgresqlDeleteDatabase,
    PostgresqlListFirewallRules,
    PostgresqlCreateOrUpdateFirewallRule,
    PostgresqlDeleteFirewallRule,
    PostgresqlListConfigurations,
    PostgresqlGetConfiguration,
    PostgresqlRestartServer,
    PostgresqlStopServer,
    PostgresqlStartServer,
    // Data Factory (Microsoft.DataFactory)
    DataFactoryListFactories,
    DataFactoryGetFactory,
    DataFactoryListPipelines,
    DataFactoryGetPipeline,
    DataFactoryCreateOrUpdatePipeline,
    DataFactoryCreatePipelineRun,
    DataFactoryDeleteFactory,
    DataFactoryDeletePipeline,
    DataFactoryListDatasets,
    DataFactoryGetDataset,
    DataFactoryCreateOrUpdateDataset,
    DataFactoryListLinkedServices,
    DataFactoryGetLinkedService,
    DataFactoryListTriggers,
    DataFactoryGetTrigger,
    DataFactoryStartTrigger,
    DataFactoryStopTrigger,
    // Logic Apps (Microsoft.Logic)
    LogicAppsListWorkflows,
    LogicAppsGetWorkflow,
    LogicAppsCreateOrUpdateWorkflow,
    LogicAppsDeleteWorkflow,
    LogicAppsRunWorkflow,
    // Container Apps (Microsoft.App)
    ContainerAppsListContainerApps,
    ContainerAppsGetContainerApp,
    ContainerAppsCreateOrUpdateContainerApp,
    ContainerAppsDeleteContainerApp,
    ContainerAppsListEnvironments,
    ContainerAppsGetEnvironment,
    // SignalR (Microsoft.SignalRService)
    SignalRListAll,
    SignalRGet,
    SignalRCreateOrUpdate,
    SignalRDelete,
    // Notification Hubs (Microsoft.NotificationHubs)
    NotificationHubsListNamespaces,
    NotificationHubsGetNamespace,
    NotificationHubsListHubs,
    NotificationHubsGetHub,
    // Search (Microsoft.Search)
    SearchListServices,
    SearchGetService,
    SearchCreateOrUpdateService,
    SearchDeleteService,
    // API Management (Microsoft.ApiManagement)
    ApimListServices,
    ApimGetService,
    ApimListApis,
    ApimGetApi,
    ApimCreateOrUpdateService,
    ApimDeleteService,
    ApimCreateOrUpdateApi,
    ApimDeleteApi,
    ApimListProducts,
    ApimGetProduct,
    ApimListSubscriptions,
    ApimListNamedValues,
    ApimListPolicies,
    ApimCreateOrUpdateProduct,
    ApimDeleteProduct,
    ApimListBackends,
    ApimGetBackend,
    ApimCreateOrUpdateBackend,
    ApimListCertificates,
    ApimListDiagnostics,
    // Batch (Microsoft.Batch)
    BatchListAccounts,
    BatchGetAccount,
    BatchListPools,
    BatchGetPool,
    BatchCreatePool,
    BatchDeletePool,
    BatchCreateOrUpdateAccount,
    BatchDeleteAccount,
    BatchListApplications,
    BatchGetApplication,
    BatchCreateOrUpdateApplication,
    BatchListCertificates,
    BatchUpdatePool,
    // Container Registry (Microsoft.ContainerRegistry)
    ContainerRegistryListRegistries,
    ContainerRegistryGetRegistry,
    ContainerRegistryCreateOrUpdateRegistry,
    ContainerRegistryDeleteRegistry,
    ContainerRegistryListRepositories,
    ContainerRegistryListWebhooks,
    ContainerRegistryGetWebhook,
    ContainerRegistryCreateOrUpdateWebhook,
    ContainerRegistryListReplications,
    ContainerRegistryGetReplication,
    ContainerRegistryListCredentials,
    ContainerRegistryRegenerateCredential,
    // Machine Learning (Microsoft.MachineLearningServices)
    MlListWorkspaces,
    MlGetWorkspace,
    MlCreateOrUpdateWorkspace,
    MlDeleteWorkspace,
    MlListCompute,
    MlGetCompute,
    MlListOnlineEndpoints,
    MlGetOnlineEndpoint,
    MlCreateOrUpdateOnlineEndpoint,
    MlDeleteOnlineEndpoint,
    MlListOnlineDeployments,
    MlListModels,
    // Synapse Analytics (Microsoft.Synapse)
    SynapseListWorkspaces,
    SynapseGetWorkspace,
    SynapseCreateOrUpdateWorkspace,
    SynapseDeleteWorkspace,
    SynapseListSqlPools,
    SynapseGetSqlPool,
    SynapseListSparkPools,
    SynapseGetSparkPool,
    SynapsePauseSqlPool,
    SynapseResumeSqlPool,
    // IoT Hub (Microsoft.Devices)
    IotHubListIotHubs,
    IotHubGetIotHub,
    IotHubCreateOrUpdateIotHub,
    IotHubDeleteIotHub,
    IotHubListKeys,
    IotHubGetStats,
    IotHubListConsumerGroups,
    // Communication Services (Microsoft.Communication)
    CommunicationListServices,
    CommunicationGetService,
    CommunicationCreateOrUpdateService,
    CommunicationDeleteService,
    CommunicationListKeys,
    CommunicationRegenerateKey,
    // Spring Apps (Microsoft.AppPlatform)
    SpringAppsListServices,
    SpringAppsGetService,
    SpringAppsCreateOrUpdateService,
    SpringAppsDeleteService,
    SpringAppsListApps,
    SpringAppsGetApp,
    SpringAppsListDeployments,
    SpringAppsGetDeployment,
    // Managed Identity (Microsoft.ManagedIdentity)
    ManagedIdentityListUserAssigned,
    ManagedIdentityGetUserAssigned,
    ManagedIdentityCreateOrUpdateUserAssigned,
    ManagedIdentityDeleteUserAssigned,
    // Policy (Microsoft.Authorization)
    PolicyListPolicyDefinitions,
    PolicyGetPolicyDefinition,
    PolicyListPolicyAssignments,
    PolicyGetPolicyAssignment,
    PolicyCreatePolicyAssignment,
    PolicyDeletePolicyAssignment,
    PolicyListPolicyCompliance,
    // Advisor (Microsoft.Advisor)
    AdvisorListRecommendations,
    AdvisorGetRecommendation,
    AdvisorSuppressRecommendation,
    // Cost Management (Microsoft.CostManagement)
    CostManagementQueryUsage,
    CostManagementListBudgets,
    CostManagementGetBudget,
    CostManagementCreateOrUpdateBudget,
    CostManagementListExports,
    // Defender for Cloud (Microsoft.Security)
    SecurityListAssessments,
    SecurityGetAssessment,
    SecurityListAlerts,
    SecurityGetAlert,
    SecurityDismissAlert,
    SecurityListSecureScores,
    SecurityListSecurityContacts,
    // App Configuration (Microsoft.AppConfiguration)
    AppConfigListConfigStores,
    AppConfigGetConfigStore,
    AppConfigCreateOrUpdateConfigStore,
    AppConfigDeleteConfigStore,
    AppConfigListKeys,
    AppConfigListKeyValues,
    // Service Fabric (Microsoft.ServiceFabric)
    ServiceFabricListClusters,
    ServiceFabricGetCluster,
    ServiceFabricCreateOrUpdateCluster,
    ServiceFabricDeleteCluster,
    ServiceFabricListApplications,
    ServiceFabricGetApplication,
    // Stream Analytics (Microsoft.StreamAnalytics)
    StreamAnalyticsListStreamingJobs,
    StreamAnalyticsGetStreamingJob,
    StreamAnalyticsCreateOrUpdateStreamingJob,
    StreamAnalyticsDeleteStreamingJob,
    StreamAnalyticsStartStreamingJob,
    StreamAnalyticsStopStreamingJob,
    // Purview (Microsoft.Purview)
    PurviewListAccounts,
    PurviewGetAccount,
    PurviewCreateOrUpdateAccount,
    PurviewDeleteAccount,
    PurviewListKeys,
    // Digital Twins (Microsoft.DigitalTwins)
    DigitalTwinsListInstances,
    DigitalTwinsGetInstance,
    DigitalTwinsCreateOrUpdateInstance,
    DigitalTwinsDeleteInstance,
    // Web PubSub (Microsoft.SignalRService/webPubSub)
    WebPubSubListAll,
    WebPubSubGet,
    WebPubSubCreateOrUpdate,
    WebPubSubDelete,
    WebPubSubListKeys,
    // MySQL (Microsoft.DBforMySQL)
    MysqlListServers,
    MysqlGetServer,
    MysqlCreateOrUpdateServer,
    MysqlDeleteServer,
    MysqlListDatabases,
    MysqlGetDatabase,
    MysqlCreateOrUpdateDatabase,
    MysqlDeleteDatabase,
    MysqlListFirewallRules,
    MysqlCreateOrUpdateFirewallRule,
    MysqlDeleteFirewallRule,
    MysqlListConfigurations,
    MysqlGetConfiguration,
    MysqlRestartServer,
    MysqlStopServer,
    MysqlStartServer,
    // Event Grid (Microsoft.EventGrid)
    EventGridListTopics,
    EventGridGetTopic,
    EventGridCreateOrUpdateTopic,
    EventGridDeleteTopic,
    EventGridListDomains,
    EventGridGetDomain,
    EventGridCreateOrUpdateDomain,
    EventGridDeleteDomain,
    EventGridListEventSubscriptions,
    EventGridGetEventSubscription,
    // Databricks (Microsoft.Databricks)
    DatabricksListWorkspaces,
    DatabricksGetWorkspace,
    DatabricksCreateOrUpdateWorkspace,
    DatabricksDeleteWorkspace,
    // Recovery Services (Microsoft.RecoveryServices)
    RecoveryServicesListVaults,
    RecoveryServicesGetVault,
    RecoveryServicesCreateOrUpdateVault,
    RecoveryServicesDeleteVault,
    RecoveryServicesListBackupItems,
    RecoveryServicesListBackupPolicies,
    RecoveryServicesListBackupJobs,
    // Static Web Apps (Microsoft.Web/staticSites)
    StaticWebAppsListAll,
    StaticWebAppsGet,
    StaticWebAppsCreateOrUpdate,
    StaticWebAppsDelete,
    StaticWebAppsListBuilds,
    StaticWebAppsListCustomDomains,
    // Kusto / Data Explorer (Microsoft.Kusto)
    KustoListClusters,
    KustoGetCluster,
    KustoCreateOrUpdateCluster,
    KustoDeleteCluster,
    KustoListDatabases,
    KustoGetDatabase,
    // NetApp Files (Microsoft.NetApp)
    NetAppListAccounts,
    NetAppGetAccount,
    NetAppCreateOrUpdateAccount,
    NetAppDeleteAccount,
    NetAppListPools,
    NetAppGetPool,
    NetAppListVolumes,
    // Managed Grafana (Microsoft.Dashboard)
    ManagedGrafanaListAll,
    ManagedGrafanaGet,
    ManagedGrafanaCreateOrUpdate,
    ManagedGrafanaDelete,
    // HDInsight (Microsoft.HDInsight)
    HdInsightListClusters,
    HdInsightGetCluster,
    HdInsightCreateCluster,
    HdInsightDeleteCluster,
    // Relay (Microsoft.Relay)
    RelayListNamespaces,
    RelayGetNamespace,
    RelayCreateOrUpdateNamespace,
    RelayDeleteNamespace,
    RelayListHybridConnections,
    // Maps (Microsoft.Maps)
    MapsListAccounts,
    MapsGetAccount,
    MapsCreateOrUpdateAccount,
    MapsDeleteAccount,
    MapsListKeys,
    // Bot Service (Microsoft.BotService)
    BotServiceListBots,
    BotServiceGetBot,
    BotServiceCreateOrUpdate,
    BotServiceDelete,
    // CDN gaps
    CdnCreateOrUpdateProfile,
    CdnDeleteProfile,
    CdnCreateOrUpdateEndpoint,
    CdnDeleteEndpoint,
    // Cosmos DB gaps
    CosmosDbCreateOrUpdateDatabaseAccount,
    CosmosDbDeleteDatabaseAccount,
    CosmosDbListMongoCollections,
    CosmosDbGetMongoCollection,
    CosmosDbCreateOrUpdateMongoCollection,
    // Container Registry gaps
    ContainerRegistryDeleteWebhook,
    ContainerRegistryCreateOrUpdateReplication,
    ContainerRegistryDeleteReplication,
    // Notification Hubs gaps
    NotificationHubsCreateOrUpdateNamespace,
    NotificationHubsDeleteNamespace,
    NotificationHubsCreateOrUpdateHub,
    NotificationHubsDeleteHub,
    // Storage gaps
    StorageListTableServices,
    StorageCreateTable,
    StorageDeleteTable,
    StorageListQueues,
    StorageCreateQueue,
    StorageDeleteQueue,
    StorageDeleteFileShare,
    // Chaos Studio (Microsoft.Chaos)
    ChaosListExperiments,
    ChaosGetExperiment,
    ChaosCreateOrUpdateExperiment,
    ChaosDeleteExperiment,
    ChaosStartExperiment,
    ChaosListTargets,
    // Confidential Ledger (Microsoft.ConfidentialLedger)
    ConfidentialLedgerListAll,
    ConfidentialLedgerGet,
    ConfidentialLedgerCreateOrUpdate,
    ConfidentialLedgerDelete,
    // Dev Center (Microsoft.DevCenter)
    DevCenterListAll,
    DevCenterGet,
    DevCenterCreateOrUpdate,
    DevCenterDelete,
    DevCenterListProjects,
    DevCenterGetProject,
    // Load Testing (Microsoft.LoadTestService)
    LoadTestingListAll,
    LoadTestingGet,
    LoadTestingCreateOrUpdate,
    LoadTestingDelete,
    // Azure Arc (Microsoft.HybridCompute)
    ArcListMachines,
    ArcGetMachine,
    ArcDeleteMachine,
    ArcListExtensions,
    // VMware Solution (Microsoft.AVS)
    VmwareListPrivateClouds,
    VmwareGetPrivateCloud,
    VmwareCreateOrUpdatePrivateCloud,
    VmwareDeletePrivateCloud,
    VmwareListClusters,
    // Azure Migrate (Microsoft.Migrate)
    MigrateListProjects,
    MigrateGetProject,
    MigrateListAssessments,
    // Azure Stack HCI (Microsoft.AzureStackHCI)
    StackHciListClusters,
    StackHciGetCluster,
    StackHciCreateOrUpdateCluster,
    StackHciDeleteCluster,
    // Health Data Services (Microsoft.HealthcareApis)
    HealthcareListServices,
    HealthcareGetService,
    HealthcareCreateOrUpdateService,
    HealthcareDeleteService,
    HealthcareListWorkspaces,
    // Managed Cassandra (Microsoft.DocumentDB/cassandraClusters)
    ManagedCassandraListClusters,
    ManagedCassandraGetCluster,
    ManagedCassandraCreateOrUpdateCluster,
    ManagedCassandraDeleteCluster,
    ManagedCassandraListDataCenters,
    // Fluid Relay (Microsoft.FluidRelay)
    FluidRelayListAll,
    FluidRelayGet,
    FluidRelayCreateOrUpdate,
    FluidRelayDelete,
    // Orbital (Microsoft.Orbital)
    OrbitalListSpacecrafts,
    OrbitalGetSpacecraft,
    OrbitalListContactProfiles,
    OrbitalGetContactProfile,
    // Quantum (Microsoft.Quantum)
    QuantumListWorkspaces,
    QuantumGetWorkspace,
    QuantumCreateOrUpdateWorkspace,
    QuantumDeleteWorkspace,
    // Data Factory gaps
    DataFactoryCreateOrUpdateFactory,
    DataFactoryDeleteDataset,
    DataFactoryDeleteLinkedService,
    DataFactoryCreateOrUpdateLinkedService,
    DataFactoryDeleteTrigger,
    DataFactoryCreateOrUpdateTrigger,
    // Monitor gaps
    MonitorDeleteActionGroup,
    MonitorDeleteDiagnosticSetting,
    MonitorDeleteAutoscaleSetting,
    // CosmosDB gaps
    CosmosDbDeleteMongoCollection,
    CosmosDbListCassandraTables,
    CosmosDbCreateOrUpdateCassandraKeyspace,
    CosmosDbDeleteCassandraKeyspace,
    CosmosDbCreateOrUpdateGremlinDatabase,
    CosmosDbDeleteGremlinDatabase,
    CosmosDbCreateOrUpdateTableResource,
    CosmosDbDeleteTableResource,
    // Machine Learning gaps
    MlCreateOrUpdateCompute,
    MlDeleteCompute,
    MlCreateOrUpdateOnlineDeployment,
    MlDeleteOnlineDeployment,
    // Spring Apps gaps
    SpringAppsCreateOrUpdateApp,
    SpringAppsDeleteApp,
    SpringAppsCreateOrUpdateDeployment,
    SpringAppsDeleteDeployment,
    // Service Fabric gaps
    ServiceFabricCreateOrUpdateApplication,
    ServiceFabricDeleteApplication,
    // Container Apps gaps
    ContainerAppsCreateOrUpdateEnvironment,
    ContainerAppsDeleteEnvironment,
    // Kusto gaps
    KustoCreateOrUpdateDatabase,
    KustoDeleteDatabase,
    // Relay gaps
    RelayGetHybridConnection,
    RelayCreateOrUpdateHybridConnection,
    RelayDeleteHybridConnection,
    // VMware gaps
    VmwareGetCluster,
    VmwareCreateOrUpdateCluster,
    VmwareDeleteCluster,
    // Event Grid gaps
    EventGridCreateOrUpdateEventSubscription,
    EventGridDeleteEventSubscription,
    // Web PubSub gaps
    WebPubSubRegenerateKey,
    // IoT Hub consumer group CRUD
    IotHubGetConsumerGroup,
    IotHubCreateOrUpdateConsumerGroup,
    IotHubDeleteConsumerGroup,
    // EventHub consumer group CRUD
    EventHubCreateOrUpdateConsumerGroup,
    EventHubDeleteConsumerGroup,
    // Cognitive update
    CognitiveUpdateAccount,
    // Container Apps revision management
    ContainerAppsListRevisions,
    ContainerAppsGetRevision,
    ContainerAppsActivateRevision,
    ContainerAppsDeactivateRevision,
    // AKS gaps
    ContainerGetUpgradeProfile,
    ContainerListMaintenanceConfigurations,
    // Orbital CRUD
    OrbitalCreateOrUpdateSpacecraft,
    OrbitalDeleteSpacecraft,
    OrbitalCreateOrUpdateContactProfile,
    OrbitalDeleteContactProfile,
    // Arc extension CRUD
    ArcGetExtension,
    ArcCreateOrUpdateExtension,
    ArcDeleteExtension,
    // Policy definition CRUD
    PolicyCreateOrUpdatePolicyDefinition,
    PolicyDeletePolicyDefinition,
    // Attestation (Microsoft.Attestation)
    AttestationListProviders,
    AttestationGetProvider,
    AttestationCreateProvider,
    AttestationDeleteProvider,
    // Managed Applications (Microsoft.Solutions)
    ManagedApplicationsListAll,
    ManagedApplicationsGet,
    ManagedApplicationsCreateOrUpdate,
    ManagedApplicationsDelete,
    // Maintenance Configurations (Microsoft.Maintenance)
    MaintenanceListConfigurations,
    MaintenanceGetConfiguration,
    MaintenanceCreateOrUpdateConfiguration,
    MaintenanceDeleteConfiguration,
    // Compute gaps
    ComputeListDiskEncryptionSets,
    ComputeGetDiskEncryptionSet,
    ComputeCreateOrUpdateDiskEncryptionSet,
    ComputeDeleteDiskEncryptionSet,
    ComputeListCapacityReservationGroups,
    ComputeGetCapacityReservationGroup,
    ComputeListSshPublicKeys,
    ComputeGetSshPublicKey,
    ComputeCreateOrUpdateSshPublicKey,
    ComputeDeleteSshPublicKey,
    // Container Instances (Microsoft.ContainerInstance)
    ContainerInstanceListAll,
    ContainerInstanceGet,
    ContainerInstanceCreateOrUpdate,
    ContainerInstanceDelete,
    ContainerInstanceStart,
    ContainerInstanceStop,
    ContainerInstanceRestart,
    ContainerInstanceListLogs,
    // Application Insights (Microsoft.Insights/components)
    AppInsightsListAll,
    AppInsightsGet,
    AppInsightsCreateOrUpdate,
    AppInsightsDelete,
    AppInsightsGetApiKeys,
    AppInsightsListWebTests,
    // Automation (Microsoft.Automation)
    AutomationListAccounts,
    AutomationGetAccount,
    AutomationCreateOrUpdateAccount,
    AutomationDeleteAccount,
    AutomationListRunbooks,
    AutomationGetRunbook,
    AutomationCreateOrUpdateRunbook,
    AutomationDeleteRunbook,
    AutomationListJobs,
    AutomationGetJob,
    // Network Interface CRUD
    NetworkCreateOrUpdateNetworkInterface,
    NetworkDeleteNetworkInterface,
    // Virtual WAN
    NetworkListVirtualWans,
    NetworkGetVirtualWan,
    NetworkCreateOrUpdateVirtualWan,
    NetworkDeleteVirtualWan,
    // Private Link Service
    NetworkListPrivateLinkServices,
    NetworkGetPrivateLinkService,
    NetworkCreateOrUpdatePrivateLinkService,
    NetworkDeletePrivateLinkService,
    // Private DNS Record Sets
    NetworkListPrivateDnsRecordSets,
    NetworkGetPrivateDnsRecordSet,
    NetworkCreateOrUpdatePrivateDnsRecordSet,
    NetworkDeletePrivateDnsRecordSet,
    NetworkDeletePrivateDnsZone,
    // VM Extensions
    ComputeListVmExtensions,
    ComputeGetVmExtension,
    ComputeCreateOrUpdateVmExtension,
    ComputeDeleteVmExtension,
    // VM Run Command
    ComputeRunCommand,
    // VMSS Instance Operations
    ComputeListVmssInstances,
    ComputeScaleVmss,
    ComputeStartVmss,
    ComputeStopVmss,
    ComputeRestartVmss,
    // Gallery Image Versions
    ComputeListGalleryImageVersions,
    ComputeGetGalleryImageVersion,
    // VNet Peering
    NetworkListVnetPeerings,
    NetworkGetVnetPeering,
    NetworkCreateOrUpdateVnetPeering,
    NetworkDeleteVnetPeering,
    // Load Balancer Rules & Probes
    NetworkListLbRules,
    NetworkGetLbRule,
    NetworkListLbProbes,
    NetworkGetLbProbe,
    NetworkListLbInboundNatRules,
    // SQL Failover Groups
    SqlListFailoverGroups,
    SqlGetFailoverGroup,
    SqlCreateOrUpdateFailoverGroup,
    SqlDeleteFailoverGroup,
    SqlFailoverFailoverGroup,
    // Redis Cache Firewall Rules
    RedisCacheListFirewallRules,
    RedisCacheGetFirewallRule,
    RedisCacheCreateOrUpdateFirewallRule,
    RedisCacheDeleteFirewallRule,
    // Service Bus Subscription CRUD
    ServiceBusCreateOrUpdateSubscription,
    ServiceBusDeleteSubscription,
    // Storage - Delete Blob
    StorageDeleteBlob,
    // MySQL Replicas
    MysqlListReplicas,
    // PostgreSQL Replicas
    PostgresqlListReplicas,
    // Compute VM operations
    ComputeCaptureVirtualMachine,
    ComputeGeneralizeVirtualMachine,
    ComputeRedeployVirtualMachine,
    ComputeReimageVirtualMachine,
    ComputeAssessPatches,
    // Key Vault gaps
    KeyVaultUpdateSecret,
    KeyVaultListDeletedVaults,
    KeyVaultGetDeletedVault,
    KeyVaultPurgeDeletedVault,
    // Storage gaps
    StorageGetBlob,
    StorageGetTable,
    StorageGetQueue,
    // Network App Gateway sub-resources
    NetworkListAppGatewaySslCertificates,
    NetworkListAppGatewayUrlPathMaps,
    // IoT Hub management ops
    IotHubGetQuotaMetrics,
    IotHubGetEndpointHealth,
    IotHubListEventHubConsumerGroups,
    // EventHub authorization rules
    EventHubListNamespaceAuthorizationRules,
    EventHubGetNamespaceAuthorizationRule,
    EventHubCreateOrUpdateNamespaceAuthorizationRule,
    EventHubDeleteNamespaceAuthorizationRule,
    EventHubListNamespaceKeys,
    // Container Registry tasks
    ContainerRegistryListTasks,
    ContainerRegistryGetTask,
    ContainerRegistryCreateOrUpdateTask,
    ContainerRegistryDeleteTask,
    // CosmosDB regenerate key
    CosmosDbRegenerateKey,
}

impl AzureApi {
    pub fn name() -> String {
        "AzureApi".to_string()
    }

    pub fn db_kind() -> String {
        "azure".to_string()
    }
}

impl Display for AzureApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Custom => f.write_str("custom"),
            // Compute
            Self::ComputeListVirtualMachines => f.write_str("compute_list_virtual_machines"),
            Self::ComputeGetVirtualMachine => f.write_str("compute_get_virtual_machine"),
            Self::ComputeCreateOrUpdateVirtualMachine => f.write_str("compute_create_or_update_virtual_machine"),
            Self::ComputeDeleteVirtualMachine => f.write_str("compute_delete_virtual_machine"),
            Self::ComputeStartVirtualMachine => f.write_str("compute_start_virtual_machine"),
            Self::ComputeStopVirtualMachine => f.write_str("compute_stop_virtual_machine"),
            Self::ComputeRestartVirtualMachine => f.write_str("compute_restart_virtual_machine"),
            Self::ComputeDeallocateVirtualMachine => f.write_str("compute_deallocate_virtual_machine"),
            Self::ComputeListVmss => f.write_str("compute_list_vmss"),
            Self::ComputeGetVmss => f.write_str("compute_get_vmss"),
            Self::ComputeListDisks => f.write_str("compute_list_disks"),
            Self::ComputeGetDisk => f.write_str("compute_get_disk"),
            Self::ComputeCreateOrUpdateDisk => f.write_str("compute_create_or_update_disk"),
            Self::ComputeDeleteDisk => f.write_str("compute_delete_disk"),
            Self::ComputeListImages => f.write_str("compute_list_images"),
            Self::ComputeGetImage => f.write_str("compute_get_image"),
            Self::ComputeListAvailabilitySets => f.write_str("compute_list_availability_sets"),
            Self::ComputeGetAvailabilitySet => f.write_str("compute_get_availability_set"),
            Self::ComputeCreateOrUpdateVmss => f.write_str("compute_create_or_update_vmss"),
            Self::ComputeDeleteVmss => f.write_str("compute_delete_vmss"),
            Self::ComputeListVmSizes => f.write_str("compute_list_vm_sizes"),
            Self::ComputeListSnapshots => f.write_str("compute_list_snapshots"),
            Self::ComputeGetSnapshot => f.write_str("compute_get_snapshot"),
            Self::ComputeCreateOrUpdateSnapshot => f.write_str("compute_create_or_update_snapshot"),
            Self::ComputeDeleteSnapshot => f.write_str("compute_delete_snapshot"),
            Self::ComputeListProximityPlacementGroups => f.write_str("compute_list_proximity_placement_groups"),
            Self::ComputeGetProximityPlacementGroup => f.write_str("compute_get_proximity_placement_group"),
            Self::ComputeListDedicatedHosts => f.write_str("compute_list_dedicated_hosts"),
            Self::ComputeListGalleries => f.write_str("compute_list_galleries"),
            Self::ComputeGetGallery => f.write_str("compute_get_gallery"),
            Self::ComputeListGalleryImages => f.write_str("compute_list_gallery_images"),
            // Network
            Self::NetworkListVirtualNetworks => f.write_str("network_list_virtual_networks"),
            Self::NetworkGetVirtualNetwork => f.write_str("network_get_virtual_network"),
            Self::NetworkCreateOrUpdateVirtualNetwork => f.write_str("network_create_or_update_virtual_network"),
            Self::NetworkListSubnets => f.write_str("network_list_subnets"),
            Self::NetworkGetSubnet => f.write_str("network_get_subnet"),
            Self::NetworkListNetworkSecurityGroups => f.write_str("network_list_network_security_groups"),
            Self::NetworkGetNetworkSecurityGroup => f.write_str("network_get_network_security_group"),
            Self::NetworkCreateOrUpdateNetworkSecurityGroup => f.write_str("network_create_or_update_network_security_group"),
            Self::NetworkListPublicIpAddresses => f.write_str("network_list_public_ip_addresses"),
            Self::NetworkGetPublicIpAddress => f.write_str("network_get_public_ip_address"),
            Self::NetworkCreateOrUpdatePublicIpAddress => f.write_str("network_create_or_update_public_ip_address"),
            Self::NetworkDeletePublicIpAddress => f.write_str("network_delete_public_ip_address"),
            Self::NetworkListLoadBalancers => f.write_str("network_list_load_balancers"),
            Self::NetworkGetLoadBalancer => f.write_str("network_get_load_balancer"),
            Self::NetworkListNetworkInterfaces => f.write_str("network_list_network_interfaces"),
            Self::NetworkGetNetworkInterface => f.write_str("network_get_network_interface"),
            Self::NetworkListApplicationGateways => f.write_str("network_list_application_gateways"),
            Self::NetworkGetApplicationGateway => f.write_str("network_get_application_gateway"),
            Self::NetworkListRouteTables => f.write_str("network_list_route_tables"),
            Self::NetworkGetRouteTable => f.write_str("network_get_route_table"),
            Self::NetworkCreateOrUpdateRouteTable => f.write_str("network_create_or_update_route_table"),
            Self::NetworkListVpnGateways => f.write_str("network_list_vpn_gateways"),
            Self::NetworkGetVpnGateway => f.write_str("network_get_vpn_gateway"),
            Self::NetworkListPrivateEndpoints => f.write_str("network_list_private_endpoints"),
            Self::NetworkGetPrivateEndpoint => f.write_str("network_get_private_endpoint"),
            Self::NetworkListNatGateways => f.write_str("network_list_nat_gateways"),
            Self::NetworkGetNatGateway => f.write_str("network_get_nat_gateway"),
            Self::NetworkCreateOrUpdateNatGateway => f.write_str("network_create_or_update_nat_gateway"),
            Self::NetworkDeleteNatGateway => f.write_str("network_delete_nat_gateway"),
            Self::NetworkListFirewalls => f.write_str("network_list_firewalls"),
            Self::NetworkGetFirewall => f.write_str("network_get_firewall"),
            Self::NetworkCreateOrUpdateLoadBalancer => f.write_str("network_create_or_update_load_balancer"),
            Self::NetworkDeleteLoadBalancer => f.write_str("network_delete_load_balancer"),
            Self::NetworkDeleteVirtualNetwork => f.write_str("network_delete_virtual_network"),
            Self::NetworkCreateOrUpdateSubnet => f.write_str("network_create_or_update_subnet"),
            Self::NetworkDeleteSubnet => f.write_str("network_delete_subnet"),
            Self::NetworkListSecurityRules => f.write_str("network_list_security_rules"),
            Self::NetworkCreateOrUpdateSecurityRule => f.write_str("network_create_or_update_security_rule"),
            Self::NetworkDeleteSecurityRule => f.write_str("network_delete_security_rule"),
            Self::NetworkDeletePrivateEndpoint => f.write_str("network_delete_private_endpoint"),
            Self::NetworkCreateOrUpdateVpnGateway => f.write_str("network_create_or_update_vpn_gateway"),
            Self::NetworkDeleteVpnGateway => f.write_str("network_delete_vpn_gateway"),
            Self::NetworkListExpressRouteCircuits => f.write_str("network_list_express_route_circuits"),
            Self::NetworkGetExpressRouteCircuit => f.write_str("network_get_express_route_circuit"),
            Self::NetworkListPrivateDnsZones => f.write_str("network_list_private_dns_zones"),
            Self::NetworkGetPrivateDnsZone => f.write_str("network_get_private_dns_zone"),
            Self::NetworkCreateOrUpdatePrivateDnsZone => f.write_str("network_create_or_update_private_dns_zone"),
            Self::NetworkListWafPolicies => f.write_str("network_list_waf_policies"),
            Self::NetworkGetWafPolicy => f.write_str("network_get_waf_policy"),
            Self::NetworkListBastionHosts => f.write_str("network_list_bastion_hosts"),
            Self::NetworkGetBastionHost => f.write_str("network_get_bastion_host"),
            Self::NetworkListTrafficManagerProfiles => f.write_str("network_list_traffic_manager_profiles"),
            Self::NetworkGetTrafficManagerProfile => f.write_str("network_get_traffic_manager_profile"),
            Self::NetworkCreateOrUpdateApplicationGateway => f.write_str("network_create_or_update_application_gateway"),
            Self::NetworkDeleteApplicationGateway => f.write_str("network_delete_application_gateway"),
            Self::NetworkCreateOrUpdateFirewall => f.write_str("network_create_or_update_firewall"),
            Self::NetworkDeleteFirewall => f.write_str("network_delete_firewall"),
            Self::NetworkCreateOrUpdateBastionHost => f.write_str("network_create_or_update_bastion_host"),
            Self::NetworkDeleteBastionHost => f.write_str("network_delete_bastion_host"),
            Self::NetworkCreateOrUpdateTrafficManagerProfile => f.write_str("network_create_or_update_traffic_manager_profile"),
            Self::NetworkDeleteTrafficManagerProfile => f.write_str("network_delete_traffic_manager_profile"),
            Self::NetworkCreateOrUpdateExpressRouteCircuit => f.write_str("network_create_or_update_express_route_circuit"),
            Self::NetworkDeleteExpressRouteCircuit => f.write_str("network_delete_express_route_circuit"),
            Self::NetworkCreateOrUpdateWafPolicy => f.write_str("network_create_or_update_waf_policy"),
            Self::NetworkDeleteWafPolicy => f.write_str("network_delete_waf_policy"),
            Self::NetworkCreateOrUpdatePrivateEndpoint => f.write_str("network_create_or_update_private_endpoint"),
            Self::NetworkListNetworkWatchers => f.write_str("network_list_network_watchers"),
            Self::NetworkGetNetworkWatcher => f.write_str("network_get_network_watcher"),
            Self::NetworkCreateOrUpdateNetworkWatcher => f.write_str("network_create_or_update_network_watcher"),
            Self::NetworkDeleteNetworkWatcher => f.write_str("network_delete_network_watcher"),
            Self::NetworkGetNetworkWatcherTopology => f.write_str("network_get_network_watcher_topology"),
            Self::NetworkListFlowLogs => f.write_str("network_list_flow_logs"),
            Self::NetworkListFirewallPolicies => f.write_str("network_list_firewall_policies"),
            Self::NetworkGetFirewallPolicy => f.write_str("network_get_firewall_policy"),
            Self::NetworkCreateOrUpdateFirewallPolicy => f.write_str("network_create_or_update_firewall_policy"),
            Self::NetworkDeleteFirewallPolicy => f.write_str("network_delete_firewall_policy"),
            // DDoS Protection
            Self::DdosProtectionListPlans => f.write_str("ddos_protection_list_plans"),
            Self::DdosProtectionGetPlan => f.write_str("ddos_protection_get_plan"),
            Self::DdosProtectionCreateOrUpdatePlan => f.write_str("ddos_protection_create_or_update_plan"),
            Self::DdosProtectionDeletePlan => f.write_str("ddos_protection_delete_plan"),
            // Resource
            Self::ResourceListSubscriptions => f.write_str("resource_list_subscriptions"),
            Self::ResourceGetSubscription => f.write_str("resource_get_subscription"),
            Self::ResourceListResourceGroups => f.write_str("resource_list_resource_groups"),
            Self::ResourceGetResourceGroup => f.write_str("resource_get_resource_group"),
            Self::ResourceCreateOrUpdateResourceGroup => f.write_str("resource_create_or_update_resource_group"),
            Self::ResourceDeleteResourceGroup => f.write_str("resource_delete_resource_group"),
            Self::ResourceListResources => f.write_str("resource_list_resources"),
            // Storage
            Self::StorageListAccounts => f.write_str("storage_list_accounts"),
            Self::StorageGetAccount => f.write_str("storage_get_account"),
            Self::StorageCreateAccount => f.write_str("storage_create_account"),
            Self::StorageDeleteAccount => f.write_str("storage_delete_account"),
            Self::StorageListAccountKeys => f.write_str("storage_list_account_keys"),
            Self::StorageListContainers => f.write_str("storage_list_containers"),
            Self::StorageListBlobs => f.write_str("storage_list_blobs"),
            Self::StorageCreateContainer => f.write_str("storage_create_container"),
            Self::StorageDeleteContainer => f.write_str("storage_delete_container"),
            Self::StorageGetContainer => f.write_str("storage_get_container"),
            Self::StorageListFileShares => f.write_str("storage_list_file_shares"),
            Self::StorageGetFileShare => f.write_str("storage_get_file_share"),
            Self::StorageCreateFileShare => f.write_str("storage_create_file_share"),
            Self::StorageListQueueServices => f.write_str("storage_list_queue_services"),
            Self::StorageRegenerateAccountKey => f.write_str("storage_regenerate_account_key"),
            Self::StorageUpdateAccount => f.write_str("storage_update_account"),
            Self::StorageGetBlobServiceProperties => f.write_str("storage_get_blob_service_properties"),
            Self::StorageSetBlobServiceProperties => f.write_str("storage_set_blob_service_properties"),
            // Key Vault
            Self::KeyVaultListVaults => f.write_str("keyvault_list_vaults"),
            Self::KeyVaultGetVault => f.write_str("keyvault_get_vault"),
            Self::KeyVaultCreateOrUpdateVault => f.write_str("keyvault_create_or_update_vault"),
            Self::KeyVaultGetSecret => f.write_str("keyvault_get_secret"),
            Self::KeyVaultSetSecret => f.write_str("keyvault_set_secret"),
            Self::KeyVaultListSecrets => f.write_str("keyvault_list_secrets"),
            Self::KeyVaultGetKey => f.write_str("keyvault_get_key"),
            Self::KeyVaultListKeys => f.write_str("keyvault_list_keys"),
            Self::KeyVaultDeleteSecret => f.write_str("keyvault_delete_secret"),
            Self::KeyVaultRecoverDeletedSecret => f.write_str("keyvault_recover_deleted_secret"),
            Self::KeyVaultListCertificates => f.write_str("keyvault_list_certificates"),
            Self::KeyVaultGetCertificate => f.write_str("keyvault_get_certificate"),
            Self::KeyVaultCreateKey => f.write_str("keyvault_create_key"),
            Self::KeyVaultDeleteKey => f.write_str("keyvault_delete_key"),
            Self::KeyVaultDeleteVault => f.write_str("keyvault_delete_vault"),
            Self::KeyVaultPurgeDeletedSecret => f.write_str("keyvault_purge_deleted_secret"),
            Self::KeyVaultBackupSecret => f.write_str("keyvault_backup_secret"),
            Self::KeyVaultCreateOrImportCertificate => f.write_str("keyvault_create_or_import_certificate"),
            Self::KeyVaultDeleteCertificate => f.write_str("keyvault_delete_certificate"),
            Self::KeyVaultBackupKey => f.write_str("keyvault_backup_key"),
            // Authorization
            Self::AuthorizationListRoleAssignments => f.write_str("authorization_list_role_assignments"),
            Self::AuthorizationCreateRoleAssignment => f.write_str("authorization_create_role_assignment"),
            Self::AuthorizationDeleteRoleAssignment => f.write_str("authorization_delete_role_assignment"),
            Self::AuthorizationListRoleDefinitions => f.write_str("authorization_list_role_definitions"),
            // SQL
            Self::SqlListServers => f.write_str("sql_list_servers"),
            Self::SqlGetServer => f.write_str("sql_get_server"),
            Self::SqlListDatabases => f.write_str("sql_list_databases"),
            Self::SqlGetDatabase => f.write_str("sql_get_database"),
            Self::SqlCreateOrUpdateDatabase => f.write_str("sql_create_or_update_database"),
            Self::SqlDeleteDatabase => f.write_str("sql_delete_database"),
            Self::SqlListFirewallRules => f.write_str("sql_list_firewall_rules"),
            Self::SqlCreateOrUpdateServer => f.write_str("sql_create_or_update_server"),
            Self::SqlDeleteServer => f.write_str("sql_delete_server"),
            Self::SqlCreateOrUpdateFirewallRule => f.write_str("sql_create_or_update_firewall_rule"),
            Self::SqlDeleteFirewallRule => f.write_str("sql_delete_firewall_rule"),
            Self::SqlListElasticPools => f.write_str("sql_list_elastic_pools"),
            Self::SqlGetElasticPool => f.write_str("sql_get_elastic_pool"),
            Self::SqlCreateOrUpdateElasticPool => f.write_str("sql_create_or_update_elastic_pool"),
            Self::SqlDeleteElasticPool => f.write_str("sql_delete_elastic_pool"),
            // Cosmos DB
            Self::CosmosDbListDatabaseAccounts => f.write_str("cosmosdb_list_database_accounts"),
            Self::CosmosDbGetDatabaseAccount => f.write_str("cosmosdb_get_database_account"),
            Self::CosmosDbListSqlDatabases => f.write_str("cosmosdb_list_sql_databases"),
            Self::CosmosDbCreateOrUpdateSqlDatabase => f.write_str("cosmosdb_create_or_update_sql_database"),
            Self::CosmosDbDeleteSqlDatabase => f.write_str("cosmosdb_delete_sql_database"),
            Self::CosmosDbListSqlContainers => f.write_str("cosmosdb_list_sql_containers"),
            Self::CosmosDbGetSqlContainer => f.write_str("cosmosdb_get_sql_container"),
            Self::CosmosDbCreateOrUpdateSqlContainer => f.write_str("cosmosdb_create_or_update_sql_container"),
            Self::CosmosDbDeleteSqlContainer => f.write_str("cosmosdb_delete_sql_container"),
            Self::CosmosDbListMongoDatabases => f.write_str("cosmosdb_list_mongo_databases"),
            Self::CosmosDbCreateOrUpdateMongoDatabase => f.write_str("cosmosdb_create_or_update_mongo_database"),
            Self::CosmosDbUpdateSqlDatabaseThroughput => f.write_str("cosmosdb_update_sql_database_throughput"),
            Self::CosmosDbDeleteMongoDatabase => f.write_str("cosmosdb_delete_mongo_database"),
            Self::CosmosDbListTableResources => f.write_str("cosmosdb_list_table_resources"),
            Self::CosmosDbListCassandraKeyspaces => f.write_str("cosmosdb_list_cassandra_keyspaces"),
            Self::CosmosDbListGremlinDatabases => f.write_str("cosmosdb_list_gremlin_databases"),
            Self::CosmosDbFailoverDatabaseAccount => f.write_str("cosmosdb_failover_database_account"),
            Self::CosmosDbListConnectionStrings => f.write_str("cosmosdb_list_connection_strings"),
            Self::CosmosDbListKeys => f.write_str("cosmosdb_list_keys"),
            // App Service
            Self::AppServiceListWebApps => f.write_str("app_service_list_web_apps"),
            Self::AppServiceGetWebApp => f.write_str("app_service_get_web_app"),
            Self::AppServiceCreateOrUpdateWebApp => f.write_str("app_service_create_or_update_web_app"),
            Self::AppServiceDeleteWebApp => f.write_str("app_service_delete_web_app"),
            Self::AppServiceRestartWebApp => f.write_str("app_service_restart_web_app"),
            Self::AppServiceStopWebApp => f.write_str("app_service_stop_web_app"),
            Self::AppServiceStartWebApp => f.write_str("app_service_start_web_app"),
            Self::AppServiceListConfigurations => f.write_str("app_service_list_configurations"),
            Self::AppServiceListDeploymentSlots => f.write_str("app_service_list_deployment_slots"),
            Self::AppServiceGetDeploymentSlot => f.write_str("app_service_get_deployment_slot"),
            Self::AppServiceSwapSlot => f.write_str("app_service_swap_slot"),
            Self::AppServiceListCustomDomains => f.write_str("app_service_list_custom_domains"),
            Self::AppServiceGetSourceControl => f.write_str("app_service_get_source_control"),
            Self::AppServiceListPlans => f.write_str("app_service_list_plans"),
            Self::AppServiceGetPlan => f.write_str("app_service_get_plan"),
            Self::AppServiceCreateOrUpdatePlan => f.write_str("app_service_create_or_update_plan"),
            Self::AppServiceDeletePlan => f.write_str("app_service_delete_plan"),
            Self::AppServiceDeleteDeploymentSlot => f.write_str("app_service_delete_deployment_slot"),
            Self::AppServiceListPublishProfiles => f.write_str("app_service_list_publish_profiles"),
            Self::AppServiceListDeployments => f.write_str("app_service_list_deployments"),
            Self::AppServiceListHybridConnections => f.write_str("app_service_list_hybrid_connections"),
            // Container
            Self::ContainerListManagedClusters => f.write_str("container_list_managed_clusters"),
            Self::ContainerGetManagedCluster => f.write_str("container_get_managed_cluster"),
            Self::ContainerCreateOrUpdateManagedCluster => f.write_str("container_create_or_update_managed_cluster"),
            Self::ContainerDeleteManagedCluster => f.write_str("container_delete_managed_cluster"),
            Self::ContainerListAgentPools => f.write_str("container_list_agent_pools"),
            Self::ContainerGetAgentPool => f.write_str("container_get_agent_pool"),
            Self::ContainerListClusterAdminCredentials => f.write_str("container_list_cluster_admin_credentials"),
            Self::ContainerListClusterUserCredentials => f.write_str("container_list_cluster_user_credentials"),
            Self::ContainerCreateOrUpdateAgentPool => f.write_str("container_create_or_update_agent_pool"),
            Self::ContainerDeleteAgentPool => f.write_str("container_delete_agent_pool"),
            Self::ContainerStartManagedCluster => f.write_str("container_start_managed_cluster"),
            Self::ContainerStopManagedCluster => f.write_str("container_stop_managed_cluster"),
            Self::ContainerListAvailableUpgrades => f.write_str("container_list_available_upgrades"),
            Self::ContainerRunCommand => f.write_str("container_run_command"),
            Self::ContainerRotateClusterCertificates => f.write_str("container_rotate_cluster_certificates"),
            // Functions
            Self::FunctionsListFunctionApps => f.write_str("functions_list_function_apps"),
            Self::FunctionsGetFunctionApp => f.write_str("functions_get_function_app"),
            Self::FunctionsListFunctions => f.write_str("functions_list_functions"),
            Self::FunctionsCreateOrUpdateFunctionApp => f.write_str("functions_create_or_update_function_app"),
            Self::FunctionsDeleteFunctionApp => f.write_str("functions_delete_function_app"),
            Self::FunctionsStartFunctionApp => f.write_str("functions_start_function_app"),
            Self::FunctionsStopFunctionApp => f.write_str("functions_stop_function_app"),
            Self::FunctionsRestartFunctionApp => f.write_str("functions_restart_function_app"),
            // Service Bus
            Self::ServiceBusListNamespaces => f.write_str("servicebus_list_namespaces"),
            Self::ServiceBusGetNamespace => f.write_str("servicebus_get_namespace"),
            Self::ServiceBusListQueues => f.write_str("servicebus_list_queues"),
            Self::ServiceBusGetQueue => f.write_str("servicebus_get_queue"),
            Self::ServiceBusListTopics => f.write_str("servicebus_list_topics"),
            Self::ServiceBusCreateOrUpdateQueue => f.write_str("servicebus_create_or_update_queue"),
            Self::ServiceBusCreateOrUpdateTopic => f.write_str("servicebus_create_or_update_topic"),
            Self::ServiceBusDeleteQueue => f.write_str("servicebus_delete_queue"),
            Self::ServiceBusDeleteTopic => f.write_str("servicebus_delete_topic"),
            Self::ServiceBusListSubscriptions => f.write_str("servicebus_list_subscriptions"),
            Self::ServiceBusGetSubscription => f.write_str("servicebus_get_subscription"),
            Self::ServiceBusCreateOrUpdateNamespace => f.write_str("servicebus_create_or_update_namespace"),
            Self::ServiceBusDeleteNamespace => f.write_str("servicebus_delete_namespace"),
            // Event Hub
            Self::EventHubListNamespaces => f.write_str("eventhub_list_namespaces"),
            Self::EventHubGetNamespace => f.write_str("eventhub_get_namespace"),
            Self::EventHubListEventHubs => f.write_str("eventhub_list_event_hubs"),
            Self::EventHubGetEventHub => f.write_str("eventhub_get_event_hub"),
            Self::EventHubCreateOrUpdateNamespace => f.write_str("eventhub_create_or_update_namespace"),
            Self::EventHubCreateOrUpdateEventHub => f.write_str("eventhub_create_or_update_event_hub"),
            Self::EventHubDeleteEventHub => f.write_str("eventhub_delete_event_hub"),
            Self::EventHubDeleteNamespace => f.write_str("eventhub_delete_namespace"),
            Self::EventHubListConsumerGroups => f.write_str("eventhub_list_consumer_groups"),
            Self::EventHubGetConsumerGroup => f.write_str("eventhub_get_consumer_group"),
            // Monitor
            Self::MonitorListMetricDefinitions => f.write_str("monitor_list_metric_definitions"),
            Self::MonitorListMetrics => f.write_str("monitor_list_metrics"),
            Self::MonitorListActivityLogs => f.write_str("monitor_list_activity_logs"),
            Self::MonitorListAlertRules => f.write_str("monitor_list_alert_rules"),
            Self::MonitorGetAlertRule => f.write_str("monitor_get_alert_rule"),
            Self::MonitorListDiagnosticSettings => f.write_str("monitor_list_diagnostic_settings"),
            Self::MonitorGetDiagnosticSetting => f.write_str("monitor_get_diagnostic_setting"),
            Self::MonitorCreateOrUpdateAlertRule => f.write_str("monitor_create_or_update_alert_rule"),
            Self::MonitorDeleteAlertRule => f.write_str("monitor_delete_alert_rule"),
            Self::MonitorListActionGroups => f.write_str("monitor_list_action_groups"),
            Self::MonitorGetActionGroup => f.write_str("monitor_get_action_group"),
            Self::MonitorCreateOrUpdateActionGroup => f.write_str("monitor_create_or_update_action_group"),
            Self::MonitorListLogAnalyticsWorkspaces => f.write_str("monitor_list_log_analytics_workspaces"),
            Self::MonitorGetLogAnalyticsWorkspace => f.write_str("monitor_get_log_analytics_workspace"),
            Self::MonitorCreateOrUpdateDiagnosticSetting => f.write_str("monitor_create_or_update_diagnostic_setting"),
            Self::MonitorListAutoscaleSettings => f.write_str("monitor_list_autoscale_settings"),
            Self::MonitorGetAutoscaleSetting => f.write_str("monitor_get_autoscale_setting"),
            Self::MonitorCreateOrUpdateAutoscaleSetting => f.write_str("monitor_create_or_update_autoscale_setting"),
            Self::MonitorQueryLogAnalytics => f.write_str("monitor_query_log_analytics"),
            Self::MonitorCreateOrUpdateLogAnalyticsWorkspace => f.write_str("monitor_create_or_update_log_analytics_workspace"),
            Self::MonitorDeleteLogAnalyticsWorkspace => f.write_str("monitor_delete_log_analytics_workspace"),
            // DNS
            Self::DnsListZones => f.write_str("dns_list_zones"),
            Self::DnsGetZone => f.write_str("dns_get_zone"),
            Self::DnsListRecordSets => f.write_str("dns_list_record_sets"),
            Self::DnsGetRecordSet => f.write_str("dns_get_record_set"),
            Self::DnsCreateOrUpdateRecordSet => f.write_str("dns_create_or_update_record_set"),
            Self::DnsDeleteRecordSet => f.write_str("dns_delete_record_set"),
            // CDN
            Self::CdnListProfiles => f.write_str("cdn_list_profiles"),
            Self::CdnGetProfile => f.write_str("cdn_get_profile"),
            Self::CdnListEndpoints => f.write_str("cdn_list_endpoints"),
            Self::CdnGetEndpoint => f.write_str("cdn_get_endpoint"),
            Self::CdnPurgeEndpoint => f.write_str("cdn_purge_endpoint"),
            // Front Door
            Self::FrontDoorListFrontDoors => f.write_str("frontdoor_list_front_doors"),
            Self::FrontDoorGetFrontDoor => f.write_str("frontdoor_get_front_door"),
            Self::FrontDoorCreateOrUpdateFrontDoor => f.write_str("frontdoor_create_or_update_front_door"),
            Self::FrontDoorDeleteFrontDoor => f.write_str("frontdoor_delete_front_door"),
            // Cognitive Services
            Self::CognitiveListAccounts => f.write_str("cognitive_list_accounts"),
            Self::CognitiveGetAccount => f.write_str("cognitive_get_account"),
            Self::CognitiveCreateAccount => f.write_str("cognitive_create_account"),
            Self::CognitiveDeleteAccount => f.write_str("cognitive_delete_account"),
            Self::CognitiveListKeys => f.write_str("cognitive_list_keys"),
            // Redis Cache
            Self::RedisCacheListAll => f.write_str("redis_cache_list_all"),
            Self::RedisCacheGet => f.write_str("redis_cache_get"),
            Self::RedisCacheCreateOrUpdate => f.write_str("redis_cache_create_or_update"),
            Self::RedisCacheDelete => f.write_str("redis_cache_delete"),
            Self::RedisCacheListKeys => f.write_str("redis_cache_list_keys"),
            Self::RedisCacheRegenerateKey => f.write_str("redis_cache_regenerate_key"),
            // PostgreSQL
            Self::PostgresqlListServers => f.write_str("postgresql_list_servers"),
            Self::PostgresqlGetServer => f.write_str("postgresql_get_server"),
            Self::PostgresqlCreateOrUpdateServer => f.write_str("postgresql_create_or_update_server"),
            Self::PostgresqlDeleteServer => f.write_str("postgresql_delete_server"),
            Self::PostgresqlListDatabases => f.write_str("postgresql_list_databases"),
            Self::PostgresqlGetDatabase => f.write_str("postgresql_get_database"),
            Self::PostgresqlCreateOrUpdateDatabase => f.write_str("postgresql_create_or_update_database"),
            Self::PostgresqlDeleteDatabase => f.write_str("postgresql_delete_database"),
            Self::PostgresqlListFirewallRules => f.write_str("postgresql_list_firewall_rules"),
            Self::PostgresqlCreateOrUpdateFirewallRule => f.write_str("postgresql_create_or_update_firewall_rule"),
            Self::PostgresqlDeleteFirewallRule => f.write_str("postgresql_delete_firewall_rule"),
            Self::PostgresqlListConfigurations => f.write_str("postgresql_list_configurations"),
            Self::PostgresqlGetConfiguration => f.write_str("postgresql_get_configuration"),
            Self::PostgresqlRestartServer => f.write_str("postgresql_restart_server"),
            Self::PostgresqlStopServer => f.write_str("postgresql_stop_server"),
            Self::PostgresqlStartServer => f.write_str("postgresql_start_server"),
            // Data Factory
            Self::DataFactoryListFactories => f.write_str("data_factory_list_factories"),
            Self::DataFactoryGetFactory => f.write_str("data_factory_get_factory"),
            Self::DataFactoryListPipelines => f.write_str("data_factory_list_pipelines"),
            Self::DataFactoryGetPipeline => f.write_str("data_factory_get_pipeline"),
            Self::DataFactoryCreateOrUpdatePipeline => f.write_str("data_factory_create_or_update_pipeline"),
            Self::DataFactoryCreatePipelineRun => f.write_str("data_factory_create_pipeline_run"),
            Self::DataFactoryDeleteFactory => f.write_str("data_factory_delete_factory"),
            Self::DataFactoryDeletePipeline => f.write_str("data_factory_delete_pipeline"),
            Self::DataFactoryListDatasets => f.write_str("data_factory_list_datasets"),
            Self::DataFactoryGetDataset => f.write_str("data_factory_get_dataset"),
            Self::DataFactoryCreateOrUpdateDataset => f.write_str("data_factory_create_or_update_dataset"),
            Self::DataFactoryListLinkedServices => f.write_str("data_factory_list_linked_services"),
            Self::DataFactoryGetLinkedService => f.write_str("data_factory_get_linked_service"),
            Self::DataFactoryListTriggers => f.write_str("data_factory_list_triggers"),
            Self::DataFactoryGetTrigger => f.write_str("data_factory_get_trigger"),
            Self::DataFactoryStartTrigger => f.write_str("data_factory_start_trigger"),
            Self::DataFactoryStopTrigger => f.write_str("data_factory_stop_trigger"),
            // Logic Apps
            Self::LogicAppsListWorkflows => f.write_str("logic_apps_list_workflows"),
            Self::LogicAppsGetWorkflow => f.write_str("logic_apps_get_workflow"),
            Self::LogicAppsCreateOrUpdateWorkflow => f.write_str("logic_apps_create_or_update_workflow"),
            Self::LogicAppsDeleteWorkflow => f.write_str("logic_apps_delete_workflow"),
            Self::LogicAppsRunWorkflow => f.write_str("logic_apps_run_workflow"),
            // Container Apps
            Self::ContainerAppsListContainerApps => f.write_str("container_apps_list_container_apps"),
            Self::ContainerAppsGetContainerApp => f.write_str("container_apps_get_container_app"),
            Self::ContainerAppsCreateOrUpdateContainerApp => f.write_str("container_apps_create_or_update_container_app"),
            Self::ContainerAppsDeleteContainerApp => f.write_str("container_apps_delete_container_app"),
            Self::ContainerAppsListEnvironments => f.write_str("container_apps_list_environments"),
            Self::ContainerAppsGetEnvironment => f.write_str("container_apps_get_environment"),
            // SignalR
            Self::SignalRListAll => f.write_str("signalr_list_all"),
            Self::SignalRGet => f.write_str("signalr_get"),
            Self::SignalRCreateOrUpdate => f.write_str("signalr_create_or_update"),
            Self::SignalRDelete => f.write_str("signalr_delete"),
            // Notification Hubs
            Self::NotificationHubsListNamespaces => f.write_str("notification_hubs_list_namespaces"),
            Self::NotificationHubsGetNamespace => f.write_str("notification_hubs_get_namespace"),
            Self::NotificationHubsListHubs => f.write_str("notification_hubs_list_hubs"),
            Self::NotificationHubsGetHub => f.write_str("notification_hubs_get_hub"),
            // Search
            Self::SearchListServices => f.write_str("search_list_services"),
            Self::SearchGetService => f.write_str("search_get_service"),
            Self::SearchCreateOrUpdateService => f.write_str("search_create_or_update_service"),
            Self::SearchDeleteService => f.write_str("search_delete_service"),
            // API Management
            Self::ApimListServices => f.write_str("apim_list_services"),
            Self::ApimGetService => f.write_str("apim_get_service"),
            Self::ApimListApis => f.write_str("apim_list_apis"),
            Self::ApimGetApi => f.write_str("apim_get_api"),
            Self::ApimCreateOrUpdateService => f.write_str("apim_create_or_update_service"),
            Self::ApimDeleteService => f.write_str("apim_delete_service"),
            Self::ApimCreateOrUpdateApi => f.write_str("apim_create_or_update_api"),
            Self::ApimDeleteApi => f.write_str("apim_delete_api"),
            Self::ApimListProducts => f.write_str("apim_list_products"),
            Self::ApimGetProduct => f.write_str("apim_get_product"),
            Self::ApimListSubscriptions => f.write_str("apim_list_subscriptions"),
            Self::ApimListNamedValues => f.write_str("apim_list_named_values"),
            Self::ApimListPolicies => f.write_str("apim_list_policies"),
            Self::ApimCreateOrUpdateProduct => f.write_str("apim_create_or_update_product"),
            Self::ApimDeleteProduct => f.write_str("apim_delete_product"),
            Self::ApimListBackends => f.write_str("apim_list_backends"),
            Self::ApimGetBackend => f.write_str("apim_get_backend"),
            Self::ApimCreateOrUpdateBackend => f.write_str("apim_create_or_update_backend"),
            Self::ApimListCertificates => f.write_str("apim_list_certificates"),
            Self::ApimListDiagnostics => f.write_str("apim_list_diagnostics"),
            // Batch
            Self::BatchListAccounts => f.write_str("batch_list_accounts"),
            Self::BatchGetAccount => f.write_str("batch_get_account"),
            Self::BatchListPools => f.write_str("batch_list_pools"),
            Self::BatchGetPool => f.write_str("batch_get_pool"),
            Self::BatchCreatePool => f.write_str("batch_create_pool"),
            Self::BatchDeletePool => f.write_str("batch_delete_pool"),
            Self::BatchCreateOrUpdateAccount => f.write_str("batch_create_or_update_account"),
            Self::BatchDeleteAccount => f.write_str("batch_delete_account"),
            Self::BatchListApplications => f.write_str("batch_list_applications"),
            Self::BatchGetApplication => f.write_str("batch_get_application"),
            Self::BatchCreateOrUpdateApplication => f.write_str("batch_create_or_update_application"),
            Self::BatchListCertificates => f.write_str("batch_list_certificates"),
            Self::BatchUpdatePool => f.write_str("batch_update_pool"),
            // Container Registry
            Self::ContainerRegistryListRegistries => f.write_str("container_registry_list_registries"),
            Self::ContainerRegistryGetRegistry => f.write_str("container_registry_get_registry"),
            Self::ContainerRegistryCreateOrUpdateRegistry => f.write_str("container_registry_create_or_update_registry"),
            Self::ContainerRegistryDeleteRegistry => f.write_str("container_registry_delete_registry"),
            Self::ContainerRegistryListRepositories => f.write_str("container_registry_list_repositories"),
            Self::ContainerRegistryListWebhooks => f.write_str("container_registry_list_webhooks"),
            Self::ContainerRegistryGetWebhook => f.write_str("container_registry_get_webhook"),
            Self::ContainerRegistryCreateOrUpdateWebhook => f.write_str("container_registry_create_or_update_webhook"),
            Self::ContainerRegistryListReplications => f.write_str("container_registry_list_replications"),
            Self::ContainerRegistryGetReplication => f.write_str("container_registry_get_replication"),
            Self::ContainerRegistryListCredentials => f.write_str("container_registry_list_credentials"),
            Self::ContainerRegistryRegenerateCredential => f.write_str("container_registry_regenerate_credential"),
            // Machine Learning
            Self::MlListWorkspaces => f.write_str("ml_list_workspaces"),
            Self::MlGetWorkspace => f.write_str("ml_get_workspace"),
            Self::MlCreateOrUpdateWorkspace => f.write_str("ml_create_or_update_workspace"),
            Self::MlDeleteWorkspace => f.write_str("ml_delete_workspace"),
            Self::MlListCompute => f.write_str("ml_list_compute"),
            Self::MlGetCompute => f.write_str("ml_get_compute"),
            Self::MlListOnlineEndpoints => f.write_str("ml_list_online_endpoints"),
            Self::MlGetOnlineEndpoint => f.write_str("ml_get_online_endpoint"),
            Self::MlCreateOrUpdateOnlineEndpoint => f.write_str("ml_create_or_update_online_endpoint"),
            Self::MlDeleteOnlineEndpoint => f.write_str("ml_delete_online_endpoint"),
            Self::MlListOnlineDeployments => f.write_str("ml_list_online_deployments"),
            Self::MlListModels => f.write_str("ml_list_models"),
            // Synapse Analytics
            Self::SynapseListWorkspaces => f.write_str("synapse_list_workspaces"),
            Self::SynapseGetWorkspace => f.write_str("synapse_get_workspace"),
            Self::SynapseCreateOrUpdateWorkspace => f.write_str("synapse_create_or_update_workspace"),
            Self::SynapseDeleteWorkspace => f.write_str("synapse_delete_workspace"),
            Self::SynapseListSqlPools => f.write_str("synapse_list_sql_pools"),
            Self::SynapseGetSqlPool => f.write_str("synapse_get_sql_pool"),
            Self::SynapseListSparkPools => f.write_str("synapse_list_spark_pools"),
            Self::SynapseGetSparkPool => f.write_str("synapse_get_spark_pool"),
            Self::SynapsePauseSqlPool => f.write_str("synapse_pause_sql_pool"),
            Self::SynapseResumeSqlPool => f.write_str("synapse_resume_sql_pool"),
            // IoT Hub
            Self::IotHubListIotHubs => f.write_str("iot_hub_list_iot_hubs"),
            Self::IotHubGetIotHub => f.write_str("iot_hub_get_iot_hub"),
            Self::IotHubCreateOrUpdateIotHub => f.write_str("iot_hub_create_or_update_iot_hub"),
            Self::IotHubDeleteIotHub => f.write_str("iot_hub_delete_iot_hub"),
            Self::IotHubListKeys => f.write_str("iot_hub_list_keys"),
            Self::IotHubGetStats => f.write_str("iot_hub_get_stats"),
            Self::IotHubListConsumerGroups => f.write_str("iot_hub_list_consumer_groups"),
            // Communication Services
            Self::CommunicationListServices => f.write_str("communication_list_services"),
            Self::CommunicationGetService => f.write_str("communication_get_service"),
            Self::CommunicationCreateOrUpdateService => f.write_str("communication_create_or_update_service"),
            Self::CommunicationDeleteService => f.write_str("communication_delete_service"),
            Self::CommunicationListKeys => f.write_str("communication_list_keys"),
            Self::CommunicationRegenerateKey => f.write_str("communication_regenerate_key"),
            // Spring Apps
            Self::SpringAppsListServices => f.write_str("spring_apps_list_services"),
            Self::SpringAppsGetService => f.write_str("spring_apps_get_service"),
            Self::SpringAppsCreateOrUpdateService => f.write_str("spring_apps_create_or_update_service"),
            Self::SpringAppsDeleteService => f.write_str("spring_apps_delete_service"),
            Self::SpringAppsListApps => f.write_str("spring_apps_list_apps"),
            Self::SpringAppsGetApp => f.write_str("spring_apps_get_app"),
            Self::SpringAppsListDeployments => f.write_str("spring_apps_list_deployments"),
            Self::SpringAppsGetDeployment => f.write_str("spring_apps_get_deployment"),
            // Managed Identity
            Self::ManagedIdentityListUserAssigned => f.write_str("managed_identity_list_user_assigned"),
            Self::ManagedIdentityGetUserAssigned => f.write_str("managed_identity_get_user_assigned"),
            Self::ManagedIdentityCreateOrUpdateUserAssigned => f.write_str("managed_identity_create_or_update_user_assigned"),
            Self::ManagedIdentityDeleteUserAssigned => f.write_str("managed_identity_delete_user_assigned"),
            // Policy
            Self::PolicyListPolicyDefinitions => f.write_str("policy_list_policy_definitions"),
            Self::PolicyGetPolicyDefinition => f.write_str("policy_get_policy_definition"),
            Self::PolicyListPolicyAssignments => f.write_str("policy_list_policy_assignments"),
            Self::PolicyGetPolicyAssignment => f.write_str("policy_get_policy_assignment"),
            Self::PolicyCreatePolicyAssignment => f.write_str("policy_create_policy_assignment"),
            Self::PolicyDeletePolicyAssignment => f.write_str("policy_delete_policy_assignment"),
            Self::PolicyListPolicyCompliance => f.write_str("policy_list_policy_compliance"),
            // Advisor
            Self::AdvisorListRecommendations => f.write_str("advisor_list_recommendations"),
            Self::AdvisorGetRecommendation => f.write_str("advisor_get_recommendation"),
            Self::AdvisorSuppressRecommendation => f.write_str("advisor_suppress_recommendation"),
            // Cost Management
            Self::CostManagementQueryUsage => f.write_str("cost_management_query_usage"),
            Self::CostManagementListBudgets => f.write_str("cost_management_list_budgets"),
            Self::CostManagementGetBudget => f.write_str("cost_management_get_budget"),
            Self::CostManagementCreateOrUpdateBudget => f.write_str("cost_management_create_or_update_budget"),
            Self::CostManagementListExports => f.write_str("cost_management_list_exports"),
            // Defender for Cloud
            Self::SecurityListAssessments => f.write_str("security_list_assessments"),
            Self::SecurityGetAssessment => f.write_str("security_get_assessment"),
            Self::SecurityListAlerts => f.write_str("security_list_alerts"),
            Self::SecurityGetAlert => f.write_str("security_get_alert"),
            Self::SecurityDismissAlert => f.write_str("security_dismiss_alert"),
            Self::SecurityListSecureScores => f.write_str("security_list_secure_scores"),
            Self::SecurityListSecurityContacts => f.write_str("security_list_security_contacts"),
            // App Configuration
            Self::AppConfigListConfigStores => f.write_str("app_config_list_config_stores"),
            Self::AppConfigGetConfigStore => f.write_str("app_config_get_config_store"),
            Self::AppConfigCreateOrUpdateConfigStore => f.write_str("app_config_create_or_update_config_store"),
            Self::AppConfigDeleteConfigStore => f.write_str("app_config_delete_config_store"),
            Self::AppConfigListKeys => f.write_str("app_config_list_keys"),
            Self::AppConfigListKeyValues => f.write_str("app_config_list_key_values"),
            // Service Fabric
            Self::ServiceFabricListClusters => f.write_str("service_fabric_list_clusters"),
            Self::ServiceFabricGetCluster => f.write_str("service_fabric_get_cluster"),
            Self::ServiceFabricCreateOrUpdateCluster => f.write_str("service_fabric_create_or_update_cluster"),
            Self::ServiceFabricDeleteCluster => f.write_str("service_fabric_delete_cluster"),
            Self::ServiceFabricListApplications => f.write_str("service_fabric_list_applications"),
            Self::ServiceFabricGetApplication => f.write_str("service_fabric_get_application"),
            // Stream Analytics
            Self::StreamAnalyticsListStreamingJobs => f.write_str("stream_analytics_list_streaming_jobs"),
            Self::StreamAnalyticsGetStreamingJob => f.write_str("stream_analytics_get_streaming_job"),
            Self::StreamAnalyticsCreateOrUpdateStreamingJob => f.write_str("stream_analytics_create_or_update_streaming_job"),
            Self::StreamAnalyticsDeleteStreamingJob => f.write_str("stream_analytics_delete_streaming_job"),
            Self::StreamAnalyticsStartStreamingJob => f.write_str("stream_analytics_start_streaming_job"),
            Self::StreamAnalyticsStopStreamingJob => f.write_str("stream_analytics_stop_streaming_job"),
            // Purview
            Self::PurviewListAccounts => f.write_str("purview_list_accounts"),
            Self::PurviewGetAccount => f.write_str("purview_get_account"),
            Self::PurviewCreateOrUpdateAccount => f.write_str("purview_create_or_update_account"),
            Self::PurviewDeleteAccount => f.write_str("purview_delete_account"),
            Self::PurviewListKeys => f.write_str("purview_list_keys"),
            // Digital Twins
            Self::DigitalTwinsListInstances => f.write_str("digital_twins_list_instances"),
            Self::DigitalTwinsGetInstance => f.write_str("digital_twins_get_instance"),
            Self::DigitalTwinsCreateOrUpdateInstance => f.write_str("digital_twins_create_or_update_instance"),
            Self::DigitalTwinsDeleteInstance => f.write_str("digital_twins_delete_instance"),
            // Web PubSub
            Self::WebPubSubListAll => f.write_str("web_pub_sub_list_all"),
            Self::WebPubSubGet => f.write_str("web_pub_sub_get"),
            Self::WebPubSubCreateOrUpdate => f.write_str("web_pub_sub_create_or_update"),
            Self::WebPubSubDelete => f.write_str("web_pub_sub_delete"),
            Self::WebPubSubListKeys => f.write_str("web_pub_sub_list_keys"),
            // MySQL
            Self::MysqlListServers => f.write_str("mysql_list_servers"),
            Self::MysqlGetServer => f.write_str("mysql_get_server"),
            Self::MysqlCreateOrUpdateServer => f.write_str("mysql_create_or_update_server"),
            Self::MysqlDeleteServer => f.write_str("mysql_delete_server"),
            Self::MysqlListDatabases => f.write_str("mysql_list_databases"),
            Self::MysqlGetDatabase => f.write_str("mysql_get_database"),
            Self::MysqlCreateOrUpdateDatabase => f.write_str("mysql_create_or_update_database"),
            Self::MysqlDeleteDatabase => f.write_str("mysql_delete_database"),
            Self::MysqlListFirewallRules => f.write_str("mysql_list_firewall_rules"),
            Self::MysqlCreateOrUpdateFirewallRule => f.write_str("mysql_create_or_update_firewall_rule"),
            Self::MysqlDeleteFirewallRule => f.write_str("mysql_delete_firewall_rule"),
            Self::MysqlListConfigurations => f.write_str("mysql_list_configurations"),
            Self::MysqlGetConfiguration => f.write_str("mysql_get_configuration"),
            Self::MysqlRestartServer => f.write_str("mysql_restart_server"),
            Self::MysqlStopServer => f.write_str("mysql_stop_server"),
            Self::MysqlStartServer => f.write_str("mysql_start_server"),
            // Event Grid
            Self::EventGridListTopics => f.write_str("event_grid_list_topics"),
            Self::EventGridGetTopic => f.write_str("event_grid_get_topic"),
            Self::EventGridCreateOrUpdateTopic => f.write_str("event_grid_create_or_update_topic"),
            Self::EventGridDeleteTopic => f.write_str("event_grid_delete_topic"),
            Self::EventGridListDomains => f.write_str("event_grid_list_domains"),
            Self::EventGridGetDomain => f.write_str("event_grid_get_domain"),
            Self::EventGridCreateOrUpdateDomain => f.write_str("event_grid_create_or_update_domain"),
            Self::EventGridDeleteDomain => f.write_str("event_grid_delete_domain"),
            Self::EventGridListEventSubscriptions => f.write_str("event_grid_list_event_subscriptions"),
            Self::EventGridGetEventSubscription => f.write_str("event_grid_get_event_subscription"),
            // Databricks
            Self::DatabricksListWorkspaces => f.write_str("databricks_list_workspaces"),
            Self::DatabricksGetWorkspace => f.write_str("databricks_get_workspace"),
            Self::DatabricksCreateOrUpdateWorkspace => f.write_str("databricks_create_or_update_workspace"),
            Self::DatabricksDeleteWorkspace => f.write_str("databricks_delete_workspace"),
            // Recovery Services
            Self::RecoveryServicesListVaults => f.write_str("recovery_services_list_vaults"),
            Self::RecoveryServicesGetVault => f.write_str("recovery_services_get_vault"),
            Self::RecoveryServicesCreateOrUpdateVault => f.write_str("recovery_services_create_or_update_vault"),
            Self::RecoveryServicesDeleteVault => f.write_str("recovery_services_delete_vault"),
            Self::RecoveryServicesListBackupItems => f.write_str("recovery_services_list_backup_items"),
            Self::RecoveryServicesListBackupPolicies => f.write_str("recovery_services_list_backup_policies"),
            Self::RecoveryServicesListBackupJobs => f.write_str("recovery_services_list_backup_jobs"),
            // Static Web Apps
            Self::StaticWebAppsListAll => f.write_str("static_web_apps_list_all"),
            Self::StaticWebAppsGet => f.write_str("static_web_apps_get"),
            Self::StaticWebAppsCreateOrUpdate => f.write_str("static_web_apps_create_or_update"),
            Self::StaticWebAppsDelete => f.write_str("static_web_apps_delete"),
            Self::StaticWebAppsListBuilds => f.write_str("static_web_apps_list_builds"),
            Self::StaticWebAppsListCustomDomains => f.write_str("static_web_apps_list_custom_domains"),
            // Kusto
            Self::KustoListClusters => f.write_str("kusto_list_clusters"),
            Self::KustoGetCluster => f.write_str("kusto_get_cluster"),
            Self::KustoCreateOrUpdateCluster => f.write_str("kusto_create_or_update_cluster"),
            Self::KustoDeleteCluster => f.write_str("kusto_delete_cluster"),
            Self::KustoListDatabases => f.write_str("kusto_list_databases"),
            Self::KustoGetDatabase => f.write_str("kusto_get_database"),
            // NetApp Files
            Self::NetAppListAccounts => f.write_str("net_app_list_accounts"),
            Self::NetAppGetAccount => f.write_str("net_app_get_account"),
            Self::NetAppCreateOrUpdateAccount => f.write_str("net_app_create_or_update_account"),
            Self::NetAppDeleteAccount => f.write_str("net_app_delete_account"),
            Self::NetAppListPools => f.write_str("net_app_list_pools"),
            Self::NetAppGetPool => f.write_str("net_app_get_pool"),
            Self::NetAppListVolumes => f.write_str("net_app_list_volumes"),
            // Managed Grafana
            Self::ManagedGrafanaListAll => f.write_str("managed_grafana_list_all"),
            Self::ManagedGrafanaGet => f.write_str("managed_grafana_get"),
            Self::ManagedGrafanaCreateOrUpdate => f.write_str("managed_grafana_create_or_update"),
            Self::ManagedGrafanaDelete => f.write_str("managed_grafana_delete"),
            // HDInsight
            Self::HdInsightListClusters => f.write_str("hd_insight_list_clusters"),
            Self::HdInsightGetCluster => f.write_str("hd_insight_get_cluster"),
            Self::HdInsightCreateCluster => f.write_str("hd_insight_create_cluster"),
            Self::HdInsightDeleteCluster => f.write_str("hd_insight_delete_cluster"),
            // Relay
            Self::RelayListNamespaces => f.write_str("relay_list_namespaces"),
            Self::RelayGetNamespace => f.write_str("relay_get_namespace"),
            Self::RelayCreateOrUpdateNamespace => f.write_str("relay_create_or_update_namespace"),
            Self::RelayDeleteNamespace => f.write_str("relay_delete_namespace"),
            Self::RelayListHybridConnections => f.write_str("relay_list_hybrid_connections"),
            // Maps
            Self::MapsListAccounts => f.write_str("maps_list_accounts"),
            Self::MapsGetAccount => f.write_str("maps_get_account"),
            Self::MapsCreateOrUpdateAccount => f.write_str("maps_create_or_update_account"),
            Self::MapsDeleteAccount => f.write_str("maps_delete_account"),
            Self::MapsListKeys => f.write_str("maps_list_keys"),
            // Bot Service
            Self::BotServiceListBots => f.write_str("bot_service_list_bots"),
            Self::BotServiceGetBot => f.write_str("bot_service_get_bot"),
            Self::BotServiceCreateOrUpdate => f.write_str("bot_service_create_or_update"),
            Self::BotServiceDelete => f.write_str("bot_service_delete"),
            // CDN gaps
            Self::CdnCreateOrUpdateProfile => f.write_str("cdn_create_or_update_profile"),
            Self::CdnDeleteProfile => f.write_str("cdn_delete_profile"),
            Self::CdnCreateOrUpdateEndpoint => f.write_str("cdn_create_or_update_endpoint"),
            Self::CdnDeleteEndpoint => f.write_str("cdn_delete_endpoint"),
            // Cosmos DB gaps
            Self::CosmosDbCreateOrUpdateDatabaseAccount => f.write_str("cosmosdb_create_or_update_database_account"),
            Self::CosmosDbDeleteDatabaseAccount => f.write_str("cosmosdb_delete_database_account"),
            Self::CosmosDbListMongoCollections => f.write_str("cosmosdb_list_mongo_collections"),
            Self::CosmosDbGetMongoCollection => f.write_str("cosmosdb_get_mongo_collection"),
            Self::CosmosDbCreateOrUpdateMongoCollection => f.write_str("cosmosdb_create_or_update_mongo_collection"),
            // Container Registry gaps
            Self::ContainerRegistryDeleteWebhook => f.write_str("container_registry_delete_webhook"),
            Self::ContainerRegistryCreateOrUpdateReplication => f.write_str("container_registry_create_or_update_replication"),
            Self::ContainerRegistryDeleteReplication => f.write_str("container_registry_delete_replication"),
            // Notification Hubs gaps
            Self::NotificationHubsCreateOrUpdateNamespace => f.write_str("notification_hubs_create_or_update_namespace"),
            Self::NotificationHubsDeleteNamespace => f.write_str("notification_hubs_delete_namespace"),
            Self::NotificationHubsCreateOrUpdateHub => f.write_str("notification_hubs_create_or_update_hub"),
            Self::NotificationHubsDeleteHub => f.write_str("notification_hubs_delete_hub"),
            // Storage gaps
            Self::StorageListTableServices => f.write_str("storage_list_table_services"),
            Self::StorageCreateTable => f.write_str("storage_create_table"),
            Self::StorageDeleteTable => f.write_str("storage_delete_table"),
            Self::StorageListQueues => f.write_str("storage_list_queues"),
            Self::StorageCreateQueue => f.write_str("storage_create_queue"),
            Self::StorageDeleteQueue => f.write_str("storage_delete_queue"),
            Self::StorageDeleteFileShare => f.write_str("storage_delete_file_share"),
            // Chaos Studio
            Self::ChaosListExperiments => f.write_str("chaos_list_experiments"),
            Self::ChaosGetExperiment => f.write_str("chaos_get_experiment"),
            Self::ChaosCreateOrUpdateExperiment => f.write_str("chaos_create_or_update_experiment"),
            Self::ChaosDeleteExperiment => f.write_str("chaos_delete_experiment"),
            Self::ChaosStartExperiment => f.write_str("chaos_start_experiment"),
            Self::ChaosListTargets => f.write_str("chaos_list_targets"),
            // Confidential Ledger
            Self::ConfidentialLedgerListAll => f.write_str("confidential_ledger_list_all"),
            Self::ConfidentialLedgerGet => f.write_str("confidential_ledger_get"),
            Self::ConfidentialLedgerCreateOrUpdate => f.write_str("confidential_ledger_create_or_update"),
            Self::ConfidentialLedgerDelete => f.write_str("confidential_ledger_delete"),
            // Dev Center
            Self::DevCenterListAll => f.write_str("dev_center_list_all"),
            Self::DevCenterGet => f.write_str("dev_center_get"),
            Self::DevCenterCreateOrUpdate => f.write_str("dev_center_create_or_update"),
            Self::DevCenterDelete => f.write_str("dev_center_delete"),
            Self::DevCenterListProjects => f.write_str("dev_center_list_projects"),
            Self::DevCenterGetProject => f.write_str("dev_center_get_project"),
            // Load Testing
            Self::LoadTestingListAll => f.write_str("load_testing_list_all"),
            Self::LoadTestingGet => f.write_str("load_testing_get"),
            Self::LoadTestingCreateOrUpdate => f.write_str("load_testing_create_or_update"),
            Self::LoadTestingDelete => f.write_str("load_testing_delete"),
            // Azure Arc
            Self::ArcListMachines => f.write_str("arc_list_machines"),
            Self::ArcGetMachine => f.write_str("arc_get_machine"),
            Self::ArcDeleteMachine => f.write_str("arc_delete_machine"),
            Self::ArcListExtensions => f.write_str("arc_list_extensions"),
            // VMware Solution
            Self::VmwareListPrivateClouds => f.write_str("vmware_list_private_clouds"),
            Self::VmwareGetPrivateCloud => f.write_str("vmware_get_private_cloud"),
            Self::VmwareCreateOrUpdatePrivateCloud => f.write_str("vmware_create_or_update_private_cloud"),
            Self::VmwareDeletePrivateCloud => f.write_str("vmware_delete_private_cloud"),
            Self::VmwareListClusters => f.write_str("vmware_list_clusters"),
            // Azure Migrate
            Self::MigrateListProjects => f.write_str("migrate_list_projects"),
            Self::MigrateGetProject => f.write_str("migrate_get_project"),
            Self::MigrateListAssessments => f.write_str("migrate_list_assessments"),
            // Azure Stack HCI
            Self::StackHciListClusters => f.write_str("stack_hci_list_clusters"),
            Self::StackHciGetCluster => f.write_str("stack_hci_get_cluster"),
            Self::StackHciCreateOrUpdateCluster => f.write_str("stack_hci_create_or_update_cluster"),
            Self::StackHciDeleteCluster => f.write_str("stack_hci_delete_cluster"),
            // Health Data Services
            Self::HealthcareListServices => f.write_str("healthcare_list_services"),
            Self::HealthcareGetService => f.write_str("healthcare_get_service"),
            Self::HealthcareCreateOrUpdateService => f.write_str("healthcare_create_or_update_service"),
            Self::HealthcareDeleteService => f.write_str("healthcare_delete_service"),
            Self::HealthcareListWorkspaces => f.write_str("healthcare_list_workspaces"),
            // Managed Cassandra
            Self::ManagedCassandraListClusters => f.write_str("managed_cassandra_list_clusters"),
            Self::ManagedCassandraGetCluster => f.write_str("managed_cassandra_get_cluster"),
            Self::ManagedCassandraCreateOrUpdateCluster => f.write_str("managed_cassandra_create_or_update_cluster"),
            Self::ManagedCassandraDeleteCluster => f.write_str("managed_cassandra_delete_cluster"),
            Self::ManagedCassandraListDataCenters => f.write_str("managed_cassandra_list_data_centers"),
            // Fluid Relay
            Self::FluidRelayListAll => f.write_str("fluid_relay_list_all"),
            Self::FluidRelayGet => f.write_str("fluid_relay_get"),
            Self::FluidRelayCreateOrUpdate => f.write_str("fluid_relay_create_or_update"),
            Self::FluidRelayDelete => f.write_str("fluid_relay_delete"),
            // Orbital
            Self::OrbitalListSpacecrafts => f.write_str("orbital_list_spacecrafts"),
            Self::OrbitalGetSpacecraft => f.write_str("orbital_get_spacecraft"),
            Self::OrbitalListContactProfiles => f.write_str("orbital_list_contact_profiles"),
            Self::OrbitalGetContactProfile => f.write_str("orbital_get_contact_profile"),
            // Quantum
            Self::QuantumListWorkspaces => f.write_str("quantum_list_workspaces"),
            Self::QuantumGetWorkspace => f.write_str("quantum_get_workspace"),
            Self::QuantumCreateOrUpdateWorkspace => f.write_str("quantum_create_or_update_workspace"),
            Self::QuantumDeleteWorkspace => f.write_str("quantum_delete_workspace"),
            // Data Factory gaps
            Self::DataFactoryCreateOrUpdateFactory => f.write_str("data_factory_create_or_update_factory"),
            Self::DataFactoryDeleteDataset => f.write_str("data_factory_delete_dataset"),
            Self::DataFactoryDeleteLinkedService => f.write_str("data_factory_delete_linked_service"),
            Self::DataFactoryCreateOrUpdateLinkedService => f.write_str("data_factory_create_or_update_linked_service"),
            Self::DataFactoryDeleteTrigger => f.write_str("data_factory_delete_trigger"),
            Self::DataFactoryCreateOrUpdateTrigger => f.write_str("data_factory_create_or_update_trigger"),
            // Monitor gaps
            Self::MonitorDeleteActionGroup => f.write_str("monitor_delete_action_group"),
            Self::MonitorDeleteDiagnosticSetting => f.write_str("monitor_delete_diagnostic_setting"),
            Self::MonitorDeleteAutoscaleSetting => f.write_str("monitor_delete_autoscale_setting"),
            // CosmosDB gaps
            Self::CosmosDbDeleteMongoCollection => f.write_str("cosmosdb_delete_mongo_collection"),
            Self::CosmosDbListCassandraTables => f.write_str("cosmosdb_list_cassandra_tables"),
            Self::CosmosDbCreateOrUpdateCassandraKeyspace => f.write_str("cosmosdb_create_or_update_cassandra_keyspace"),
            Self::CosmosDbDeleteCassandraKeyspace => f.write_str("cosmosdb_delete_cassandra_keyspace"),
            Self::CosmosDbCreateOrUpdateGremlinDatabase => f.write_str("cosmosdb_create_or_update_gremlin_database"),
            Self::CosmosDbDeleteGremlinDatabase => f.write_str("cosmosdb_delete_gremlin_database"),
            Self::CosmosDbCreateOrUpdateTableResource => f.write_str("cosmosdb_create_or_update_table_resource"),
            Self::CosmosDbDeleteTableResource => f.write_str("cosmosdb_delete_table_resource"),
            // Machine Learning gaps
            Self::MlCreateOrUpdateCompute => f.write_str("ml_create_or_update_compute"),
            Self::MlDeleteCompute => f.write_str("ml_delete_compute"),
            Self::MlCreateOrUpdateOnlineDeployment => f.write_str("ml_create_or_update_online_deployment"),
            Self::MlDeleteOnlineDeployment => f.write_str("ml_delete_online_deployment"),
            // Spring Apps gaps
            Self::SpringAppsCreateOrUpdateApp => f.write_str("spring_apps_create_or_update_app"),
            Self::SpringAppsDeleteApp => f.write_str("spring_apps_delete_app"),
            Self::SpringAppsCreateOrUpdateDeployment => f.write_str("spring_apps_create_or_update_deployment"),
            Self::SpringAppsDeleteDeployment => f.write_str("spring_apps_delete_deployment"),
            // Service Fabric gaps
            Self::ServiceFabricCreateOrUpdateApplication => f.write_str("service_fabric_create_or_update_application"),
            Self::ServiceFabricDeleteApplication => f.write_str("service_fabric_delete_application"),
            // Container Apps gaps
            Self::ContainerAppsCreateOrUpdateEnvironment => f.write_str("container_apps_create_or_update_environment"),
            Self::ContainerAppsDeleteEnvironment => f.write_str("container_apps_delete_environment"),
            // Kusto gaps
            Self::KustoCreateOrUpdateDatabase => f.write_str("kusto_create_or_update_database"),
            Self::KustoDeleteDatabase => f.write_str("kusto_delete_database"),
            // Relay gaps
            Self::RelayGetHybridConnection => f.write_str("relay_get_hybrid_connection"),
            Self::RelayCreateOrUpdateHybridConnection => f.write_str("relay_create_or_update_hybrid_connection"),
            Self::RelayDeleteHybridConnection => f.write_str("relay_delete_hybrid_connection"),
            // VMware gaps
            Self::VmwareGetCluster => f.write_str("vmware_get_cluster"),
            Self::VmwareCreateOrUpdateCluster => f.write_str("vmware_create_or_update_cluster"),
            Self::VmwareDeleteCluster => f.write_str("vmware_delete_cluster"),
            // Event Grid gaps
            Self::EventGridCreateOrUpdateEventSubscription => f.write_str("event_grid_create_or_update_event_subscription"),
            Self::EventGridDeleteEventSubscription => f.write_str("event_grid_delete_event_subscription"),
            // Web PubSub gaps
            Self::WebPubSubRegenerateKey => f.write_str("web_pub_sub_regenerate_key"),
            // IoT Hub consumer group
            Self::IotHubGetConsumerGroup => f.write_str("iot_hub_get_consumer_group"),
            Self::IotHubCreateOrUpdateConsumerGroup => f.write_str("iot_hub_create_or_update_consumer_group"),
            Self::IotHubDeleteConsumerGroup => f.write_str("iot_hub_delete_consumer_group"),
            // EventHub consumer group
            Self::EventHubCreateOrUpdateConsumerGroup => f.write_str("eventhub_create_or_update_consumer_group"),
            Self::EventHubDeleteConsumerGroup => f.write_str("eventhub_delete_consumer_group"),
            // Cognitive update
            Self::CognitiveUpdateAccount => f.write_str("cognitive_update_account"),
            // Container Apps revisions
            Self::ContainerAppsListRevisions => f.write_str("container_apps_list_revisions"),
            Self::ContainerAppsGetRevision => f.write_str("container_apps_get_revision"),
            Self::ContainerAppsActivateRevision => f.write_str("container_apps_activate_revision"),
            Self::ContainerAppsDeactivateRevision => f.write_str("container_apps_deactivate_revision"),
            // AKS gaps
            Self::ContainerGetUpgradeProfile => f.write_str("container_get_upgrade_profile"),
            Self::ContainerListMaintenanceConfigurations => f.write_str("container_list_maintenance_configurations"),
            // Orbital CRUD
            Self::OrbitalCreateOrUpdateSpacecraft => f.write_str("orbital_create_or_update_spacecraft"),
            Self::OrbitalDeleteSpacecraft => f.write_str("orbital_delete_spacecraft"),
            Self::OrbitalCreateOrUpdateContactProfile => f.write_str("orbital_create_or_update_contact_profile"),
            Self::OrbitalDeleteContactProfile => f.write_str("orbital_delete_contact_profile"),
            // Arc extensions
            Self::ArcGetExtension => f.write_str("arc_get_extension"),
            Self::ArcCreateOrUpdateExtension => f.write_str("arc_create_or_update_extension"),
            Self::ArcDeleteExtension => f.write_str("arc_delete_extension"),
            // Policy definitions
            Self::PolicyCreateOrUpdatePolicyDefinition => f.write_str("policy_create_or_update_policy_definition"),
            Self::PolicyDeletePolicyDefinition => f.write_str("policy_delete_policy_definition"),
            // Attestation
            Self::AttestationListProviders => f.write_str("attestation_list_providers"),
            Self::AttestationGetProvider => f.write_str("attestation_get_provider"),
            Self::AttestationCreateProvider => f.write_str("attestation_create_provider"),
            Self::AttestationDeleteProvider => f.write_str("attestation_delete_provider"),
            // Managed Applications
            Self::ManagedApplicationsListAll => f.write_str("managed_applications_list_all"),
            Self::ManagedApplicationsGet => f.write_str("managed_applications_get"),
            Self::ManagedApplicationsCreateOrUpdate => f.write_str("managed_applications_create_or_update"),
            Self::ManagedApplicationsDelete => f.write_str("managed_applications_delete"),
            // Maintenance Configurations
            Self::MaintenanceListConfigurations => f.write_str("maintenance_list_configurations"),
            Self::MaintenanceGetConfiguration => f.write_str("maintenance_get_configuration"),
            Self::MaintenanceCreateOrUpdateConfiguration => f.write_str("maintenance_create_or_update_configuration"),
            Self::MaintenanceDeleteConfiguration => f.write_str("maintenance_delete_configuration"),
            // Compute gaps
            Self::ComputeListDiskEncryptionSets => f.write_str("compute_list_disk_encryption_sets"),
            Self::ComputeGetDiskEncryptionSet => f.write_str("compute_get_disk_encryption_set"),
            Self::ComputeCreateOrUpdateDiskEncryptionSet => f.write_str("compute_create_or_update_disk_encryption_set"),
            Self::ComputeDeleteDiskEncryptionSet => f.write_str("compute_delete_disk_encryption_set"),
            Self::ComputeListCapacityReservationGroups => f.write_str("compute_list_capacity_reservation_groups"),
            Self::ComputeGetCapacityReservationGroup => f.write_str("compute_get_capacity_reservation_group"),
            Self::ComputeListSshPublicKeys => f.write_str("compute_list_ssh_public_keys"),
            Self::ComputeGetSshPublicKey => f.write_str("compute_get_ssh_public_key"),
            Self::ComputeCreateOrUpdateSshPublicKey => f.write_str("compute_create_or_update_ssh_public_key"),
            Self::ComputeDeleteSshPublicKey => f.write_str("compute_delete_ssh_public_key"),
            // Container Instances
            Self::ContainerInstanceListAll => f.write_str("container_instance_list_all"),
            Self::ContainerInstanceGet => f.write_str("container_instance_get"),
            Self::ContainerInstanceCreateOrUpdate => f.write_str("container_instance_create_or_update"),
            Self::ContainerInstanceDelete => f.write_str("container_instance_delete"),
            Self::ContainerInstanceStart => f.write_str("container_instance_start"),
            Self::ContainerInstanceStop => f.write_str("container_instance_stop"),
            Self::ContainerInstanceRestart => f.write_str("container_instance_restart"),
            Self::ContainerInstanceListLogs => f.write_str("container_instance_list_logs"),
            // Application Insights
            Self::AppInsightsListAll => f.write_str("app_insights_list_all"),
            Self::AppInsightsGet => f.write_str("app_insights_get"),
            Self::AppInsightsCreateOrUpdate => f.write_str("app_insights_create_or_update"),
            Self::AppInsightsDelete => f.write_str("app_insights_delete"),
            Self::AppInsightsGetApiKeys => f.write_str("app_insights_get_api_keys"),
            Self::AppInsightsListWebTests => f.write_str("app_insights_list_web_tests"),
            // Automation
            Self::AutomationListAccounts => f.write_str("automation_list_accounts"),
            Self::AutomationGetAccount => f.write_str("automation_get_account"),
            Self::AutomationCreateOrUpdateAccount => f.write_str("automation_create_or_update_account"),
            Self::AutomationDeleteAccount => f.write_str("automation_delete_account"),
            Self::AutomationListRunbooks => f.write_str("automation_list_runbooks"),
            Self::AutomationGetRunbook => f.write_str("automation_get_runbook"),
            Self::AutomationCreateOrUpdateRunbook => f.write_str("automation_create_or_update_runbook"),
            Self::AutomationDeleteRunbook => f.write_str("automation_delete_runbook"),
            Self::AutomationListJobs => f.write_str("automation_list_jobs"),
            Self::AutomationGetJob => f.write_str("automation_get_job"),
            // Network Interface CRUD
            Self::NetworkCreateOrUpdateNetworkInterface => f.write_str("network_create_or_update_network_interface"),
            Self::NetworkDeleteNetworkInterface => f.write_str("network_delete_network_interface"),
            // Virtual WAN
            Self::NetworkListVirtualWans => f.write_str("network_list_virtual_wans"),
            Self::NetworkGetVirtualWan => f.write_str("network_get_virtual_wan"),
            Self::NetworkCreateOrUpdateVirtualWan => f.write_str("network_create_or_update_virtual_wan"),
            Self::NetworkDeleteVirtualWan => f.write_str("network_delete_virtual_wan"),
            // Private Link Service
            Self::NetworkListPrivateLinkServices => f.write_str("network_list_private_link_services"),
            Self::NetworkGetPrivateLinkService => f.write_str("network_get_private_link_service"),
            Self::NetworkCreateOrUpdatePrivateLinkService => f.write_str("network_create_or_update_private_link_service"),
            Self::NetworkDeletePrivateLinkService => f.write_str("network_delete_private_link_service"),
            // Private DNS Record Sets
            Self::NetworkListPrivateDnsRecordSets => f.write_str("network_list_private_dns_record_sets"),
            Self::NetworkGetPrivateDnsRecordSet => f.write_str("network_get_private_dns_record_set"),
            Self::NetworkCreateOrUpdatePrivateDnsRecordSet => f.write_str("network_create_or_update_private_dns_record_set"),
            Self::NetworkDeletePrivateDnsRecordSet => f.write_str("network_delete_private_dns_record_set"),
            Self::NetworkDeletePrivateDnsZone => f.write_str("network_delete_private_dns_zone"),
            // VM Extensions
            Self::ComputeListVmExtensions => f.write_str("compute_list_vm_extensions"),
            Self::ComputeGetVmExtension => f.write_str("compute_get_vm_extension"),
            Self::ComputeCreateOrUpdateVmExtension => f.write_str("compute_create_or_update_vm_extension"),
            Self::ComputeDeleteVmExtension => f.write_str("compute_delete_vm_extension"),
            // VM Run Command
            Self::ComputeRunCommand => f.write_str("compute_run_command"),
            // VMSS Instance Operations
            Self::ComputeListVmssInstances => f.write_str("compute_list_vmss_instances"),
            Self::ComputeScaleVmss => f.write_str("compute_scale_vmss"),
            Self::ComputeStartVmss => f.write_str("compute_start_vmss"),
            Self::ComputeStopVmss => f.write_str("compute_stop_vmss"),
            Self::ComputeRestartVmss => f.write_str("compute_restart_vmss"),
            // Gallery Image Versions
            Self::ComputeListGalleryImageVersions => f.write_str("compute_list_gallery_image_versions"),
            Self::ComputeGetGalleryImageVersion => f.write_str("compute_get_gallery_image_version"),
            // VNet Peering
            Self::NetworkListVnetPeerings => f.write_str("network_list_vnet_peerings"),
            Self::NetworkGetVnetPeering => f.write_str("network_get_vnet_peering"),
            Self::NetworkCreateOrUpdateVnetPeering => f.write_str("network_create_or_update_vnet_peering"),
            Self::NetworkDeleteVnetPeering => f.write_str("network_delete_vnet_peering"),
            // Load Balancer Rules & Probes
            Self::NetworkListLbRules => f.write_str("network_list_lb_rules"),
            Self::NetworkGetLbRule => f.write_str("network_get_lb_rule"),
            Self::NetworkListLbProbes => f.write_str("network_list_lb_probes"),
            Self::NetworkGetLbProbe => f.write_str("network_get_lb_probe"),
            Self::NetworkListLbInboundNatRules => f.write_str("network_list_lb_inbound_nat_rules"),
            // SQL Failover Groups
            Self::SqlListFailoverGroups => f.write_str("sql_list_failover_groups"),
            Self::SqlGetFailoverGroup => f.write_str("sql_get_failover_group"),
            Self::SqlCreateOrUpdateFailoverGroup => f.write_str("sql_create_or_update_failover_group"),
            Self::SqlDeleteFailoverGroup => f.write_str("sql_delete_failover_group"),
            Self::SqlFailoverFailoverGroup => f.write_str("sql_failover_failover_group"),
            // Redis Cache Firewall Rules
            Self::RedisCacheListFirewallRules => f.write_str("redis_cache_list_firewall_rules"),
            Self::RedisCacheGetFirewallRule => f.write_str("redis_cache_get_firewall_rule"),
            Self::RedisCacheCreateOrUpdateFirewallRule => f.write_str("redis_cache_create_or_update_firewall_rule"),
            Self::RedisCacheDeleteFirewallRule => f.write_str("redis_cache_delete_firewall_rule"),
            // Service Bus Subscription CRUD
            Self::ServiceBusCreateOrUpdateSubscription => f.write_str("servicebus_create_or_update_subscription"),
            Self::ServiceBusDeleteSubscription => f.write_str("servicebus_delete_subscription"),
            // Storage Delete Blob
            Self::StorageDeleteBlob => f.write_str("storage_delete_blob"),
            // MySQL Replicas
            Self::MysqlListReplicas => f.write_str("mysql_list_replicas"),
            // PostgreSQL Replicas
            Self::PostgresqlListReplicas => f.write_str("postgresql_list_replicas"),
            // Compute VM operations
            Self::ComputeCaptureVirtualMachine => f.write_str("compute_capture_virtual_machine"),
            Self::ComputeGeneralizeVirtualMachine => f.write_str("compute_generalize_virtual_machine"),
            Self::ComputeRedeployVirtualMachine => f.write_str("compute_redeploy_virtual_machine"),
            Self::ComputeReimageVirtualMachine => f.write_str("compute_reimage_virtual_machine"),
            Self::ComputeAssessPatches => f.write_str("compute_assess_patches"),
            // Key Vault gaps
            Self::KeyVaultUpdateSecret => f.write_str("keyvault_update_secret"),
            Self::KeyVaultListDeletedVaults => f.write_str("keyvault_list_deleted_vaults"),
            Self::KeyVaultGetDeletedVault => f.write_str("keyvault_get_deleted_vault"),
            Self::KeyVaultPurgeDeletedVault => f.write_str("keyvault_purge_deleted_vault"),
            // Storage gaps
            Self::StorageGetBlob => f.write_str("storage_get_blob"),
            Self::StorageGetTable => f.write_str("storage_get_table"),
            Self::StorageGetQueue => f.write_str("storage_get_queue"),
            // Network App Gateway sub-resources
            Self::NetworkListAppGatewaySslCertificates => f.write_str("network_list_app_gateway_ssl_certificates"),
            Self::NetworkListAppGatewayUrlPathMaps => f.write_str("network_list_app_gateway_url_path_maps"),
            // IoT Hub management ops
            Self::IotHubGetQuotaMetrics => f.write_str("iot_hub_get_quota_metrics"),
            Self::IotHubGetEndpointHealth => f.write_str("iot_hub_get_endpoint_health"),
            Self::IotHubListEventHubConsumerGroups => f.write_str("iot_hub_list_event_hub_consumer_groups"),
            // EventHub authorization rules
            Self::EventHubListNamespaceAuthorizationRules => f.write_str("event_hub_list_namespace_authorization_rules"),
            Self::EventHubGetNamespaceAuthorizationRule => f.write_str("event_hub_get_namespace_authorization_rule"),
            Self::EventHubCreateOrUpdateNamespaceAuthorizationRule => {
                f.write_str("event_hub_create_or_update_namespace_authorization_rule")
            }
            Self::EventHubDeleteNamespaceAuthorizationRule => f.write_str("event_hub_delete_namespace_authorization_rule"),
            Self::EventHubListNamespaceKeys => f.write_str("event_hub_list_namespace_keys"),
            // Container Registry tasks
            Self::ContainerRegistryListTasks => f.write_str("container_registry_list_tasks"),
            Self::ContainerRegistryGetTask => f.write_str("container_registry_get_task"),
            Self::ContainerRegistryCreateOrUpdateTask => f.write_str("container_registry_create_or_update_task"),
            Self::ContainerRegistryDeleteTask => f.write_str("container_registry_delete_task"),
            // CosmosDB regenerate key
            Self::CosmosDbRegenerateKey => f.write_str("cosmosdb_regenerate_key"),
        }
    }
}
