pub mod apps;
pub mod clean_rooms;
pub mod cluster_policies;
pub mod compute;
pub mod delta_sharing;
pub mod files;
pub mod git_credentials;
pub mod global_init_scripts;
pub mod iam;
pub mod instance_pools;
pub mod lakeflow;
pub mod libraries;
pub mod ml;
pub mod permissions;
pub mod secrets;
pub mod serving;
pub mod settings;
pub mod sql;
pub mod sql_analytics;
pub mod tokens;
pub mod unity_catalog;
pub mod vector_search;
pub mod warehouses;
pub mod workspace;

#[allow(unused_imports)]
pub use apps::*;
#[allow(unused_imports)]
pub use clean_rooms::*;
#[allow(unused_imports)]
pub use cluster_policies::*;
#[allow(unused_imports)]
pub use compute::*;
#[allow(unused_imports)]
pub use delta_sharing::*;
#[allow(unused_imports)]
pub use files::*;
#[allow(unused_imports)]
pub use git_credentials::*;
#[allow(unused_imports)]
pub use global_init_scripts::*;
#[allow(unused_imports)]
pub use iam::*;
#[allow(unused_imports)]
pub use instance_pools::*;
#[allow(unused_imports)]
pub use lakeflow::*;
#[allow(unused_imports)]
pub use libraries::*;
#[allow(unused_imports)]
pub use ml::*;
#[allow(unused_imports)]
pub use permissions::*;
#[allow(unused_imports)]
pub use secrets::*;
#[allow(unused_imports)]
pub use serving::*;
#[allow(unused_imports)]
pub use settings::*;
#[allow(unused_imports)]
pub use sql::*;
#[allow(unused_imports)]
pub use sql_analytics::*;
#[allow(unused_imports)]
pub use tokens::*;
#[allow(unused_imports)]
pub use unity_catalog::*;
#[allow(unused_imports)]
pub use vector_search::*;
#[allow(unused_imports)]
pub use warehouses::*;
#[allow(unused_imports)]
pub use workspace::*;

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum DatabricksApi {
    // SQL Statement Execution
    Execute,
    Query,
    CancelStatement,
    GetStatementStatus,
    ListQueryHistory,

    // SQL Warehouses
    ListWarehouses,
    GetWarehouse,
    CreateWarehouse,
    DeleteWarehouse,
    EditWarehouse,
    StartWarehouse,
    StopWarehouse,

    // Unity Catalog
    ListCatalogs,
    GetCatalog,
    CreateCatalog,
    DeleteCatalog,
    UpdateCatalog,
    ListSchemas,
    GetSchema,
    CreateSchema,
    DeleteSchema,
    UpdateSchema,
    ListTables,
    GetTable,
    DeleteTable,
    ListVolumes,
    CreateVolume,
    DeleteVolume,
    ListFunctions,
    DeleteFunction,
    GetGrants,
    UpdateGrants,

    // Workspace
    ListWorkspace,
    GetWorkspaceObjectStatus,
    ImportWorkspace,
    ExportWorkspace,
    DeleteWorkspace,
    ListRepos,
    GetRepo,
    CreateRepo,
    UpdateRepo,
    DeleteRepo,

    // Compute
    ListClusters,
    GetCluster,
    CreateCluster,
    EditCluster,
    RestartCluster,
    DeleteCluster,
    StartCluster,
    TerminateCluster,
    GetClusterEvents,
    ListJobs,
    GetJob,
    CreateJob,
    DeleteJob,
    RunNow,
    ListRuns,
    GetRun,
    CancelRun,

    // Instance Pools
    ListInstancePools,
    GetInstancePool,
    CreateInstancePool,
    DeleteInstancePool,

    // Cluster Policies
    ListClusterPolicies,
    GetClusterPolicy,
    CreateClusterPolicy,
    DeleteClusterPolicy,

    // Libraries
    LibraryClusterStatus,
    LibraryAllClusterStatuses,
    InstallLibrary,
    UninstallLibrary,

    // IAM
    ListUsers,
    GetUser,
    CreateUser,
    DeleteUser,
    ListGroups,
    GetCurrentUser,
    ListServicePrincipals,

    // Permissions
    GetPermissions,
    SetPermissions,
    GetPermissionLevels,

    // Git Credentials
    ListGitCredentials,
    GetGitCredential,
    CreateGitCredential,
    DeleteGitCredential,

    // File Management (DBFS)
    DbfsList,
    DbfsGetStatus,
    DbfsRead,
    DbfsPut,
    DbfsDelete,
    DbfsMkdirs,
    DbfsMove,

    // Machine Learning
    ListExperiments,
    GetExperiment,
    CreateExperiment,
    DeleteExperiment,
    ListModels,
    GetModel,
    CreateModel,
    DeleteModel,
    ListModelVersions,
    GetModelVersion,
    CreateModelVersion,

    // Settings
    GetWorkspaceConfig,
    ListIpAccessLists,
    GetIpAccessList,
    CreateIpAccessList,
    DeleteIpAccessList,

    // Global Init Scripts
    ListGlobalInitScripts,
    GetGlobalInitScript,
    CreateGlobalInitScript,
    DeleteGlobalInitScript,

    // Apps
    ListApps,
    GetApp,
    CreateApp,
    DeleteApp,

    // Lakeflow (Delta Live Tables)
    ListPipelines,
    GetPipeline,
    CreatePipeline,
    DeletePipeline,
    StartPipeline,
    StopPipeline,

    // Delta Sharing
    ListShares,
    GetShare,
    CreateShare,
    DeleteShare,
    ListRecipients,

    // Vector Search
    ListVectorIndexes,
    CreateVectorIndex,
    DeleteVectorIndex,
    ListVectorEndpoints,
    CreateVectorEndpoint,

    // Clean Rooms
    ListCleanRooms,
    GetCleanRoom,
    CreateCleanRoom,

    // Serving Endpoints
    ListServingEndpoints,
    GetServingEndpoint,
    CreateServingEndpoint,
    DeleteServingEndpoint,
    QueryServingEndpoint,

    // Secrets
    ListSecretScopes,
    CreateSecretScope,
    DeleteSecretScope,
    ListSecrets,
    PutSecret,
    DeleteSecret,

    // Token Management
    ListTokens,
    CreateToken,
    DeleteToken,

    // SQL Analytics (Queries, Dashboards, Alerts)
    ListQueries,
    GetQuery,
    CreateQuery,
    DeleteQuery,
    ListDashboards,
    GetDashboard,
    ListAlerts,
    GetAlert,
}

impl DatabricksApi {
    pub fn name() -> String {
        "DatabricksApi".to_string()
    }

    pub fn db_kind() -> String {
        "databricks".to_string()
    }

    #[allow(dead_code)]
    pub(crate) fn as_type(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }
}

impl Display for DatabricksApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            // SQL
            Self::Execute => write!(f, "execute"),
            Self::Query => write!(f, "query"),
            Self::CancelStatement => write!(f, "cancelstatement"),
            Self::GetStatementStatus => write!(f, "getstatementstatus"),
            Self::ListQueryHistory => write!(f, "listqueryhistory"),

            // Warehouses
            Self::ListWarehouses => write!(f, "listwarehouses"),
            Self::GetWarehouse => write!(f, "getwarehouse"),
            Self::CreateWarehouse => write!(f, "createwarehouse"),
            Self::DeleteWarehouse => write!(f, "deletewarehouse"),
            Self::EditWarehouse => write!(f, "editwarehouse"),
            Self::StartWarehouse => write!(f, "startwarehouse"),
            Self::StopWarehouse => write!(f, "stopwarehouse"),

            // Unity Catalog
            Self::ListCatalogs => write!(f, "listcatalogs"),
            Self::GetCatalog => write!(f, "getcatalog"),
            Self::CreateCatalog => write!(f, "createcatalog"),
            Self::DeleteCatalog => write!(f, "deletecatalog"),
            Self::UpdateCatalog => write!(f, "updatecatalog"),
            Self::ListSchemas => write!(f, "listschemas"),
            Self::GetSchema => write!(f, "getschema"),
            Self::CreateSchema => write!(f, "createschema"),
            Self::DeleteSchema => write!(f, "deleteschema"),
            Self::UpdateSchema => write!(f, "updateschema"),
            Self::ListTables => write!(f, "listtables"),
            Self::GetTable => write!(f, "gettable"),
            Self::DeleteTable => write!(f, "deletetable"),
            Self::ListVolumes => write!(f, "listvolumes"),
            Self::CreateVolume => write!(f, "createvolume"),
            Self::DeleteVolume => write!(f, "deletevolume"),
            Self::ListFunctions => write!(f, "listfunctions"),
            Self::DeleteFunction => write!(f, "deletefunction"),
            Self::GetGrants => write!(f, "getgrants"),
            Self::UpdateGrants => write!(f, "updategrants"),

            // Workspace
            Self::ListWorkspace => write!(f, "listworkspace"),
            Self::GetWorkspaceObjectStatus => write!(f, "getworkspaceobjectstatus"),
            Self::ImportWorkspace => write!(f, "importworkspace"),
            Self::ExportWorkspace => write!(f, "exportworkspace"),
            Self::DeleteWorkspace => write!(f, "deleteworkspace"),
            Self::ListRepos => write!(f, "listrepos"),
            Self::GetRepo => write!(f, "getrepo"),
            Self::CreateRepo => write!(f, "createrepo"),
            Self::UpdateRepo => write!(f, "updaterepo"),
            Self::DeleteRepo => write!(f, "deleterepo"),

            // Compute
            Self::ListClusters => write!(f, "listclusters"),
            Self::GetCluster => write!(f, "getcluster"),
            Self::CreateCluster => write!(f, "createcluster"),
            Self::EditCluster => write!(f, "editcluster"),
            Self::RestartCluster => write!(f, "restartcluster"),
            Self::DeleteCluster => write!(f, "deletecluster"),
            Self::StartCluster => write!(f, "startcluster"),
            Self::TerminateCluster => write!(f, "terminatecluster"),
            Self::GetClusterEvents => write!(f, "getclusterevents"),
            Self::ListJobs => write!(f, "listjobs"),
            Self::GetJob => write!(f, "getjob"),
            Self::CreateJob => write!(f, "createjob"),
            Self::DeleteJob => write!(f, "deletejob"),
            Self::RunNow => write!(f, "runnow"),
            Self::ListRuns => write!(f, "listruns"),
            Self::GetRun => write!(f, "getrun"),
            Self::CancelRun => write!(f, "cancelrun"),

            // Instance Pools
            Self::ListInstancePools => write!(f, "listinstancepools"),
            Self::GetInstancePool => write!(f, "getinstancepool"),
            Self::CreateInstancePool => write!(f, "createinstancepool"),
            Self::DeleteInstancePool => write!(f, "deleteinstancepool"),

            // Cluster Policies
            Self::ListClusterPolicies => write!(f, "listclusterpolicies"),
            Self::GetClusterPolicy => write!(f, "getclusterpolicy"),
            Self::CreateClusterPolicy => write!(f, "createclusterpolicy"),
            Self::DeleteClusterPolicy => write!(f, "deleteclusterpolicy"),

            // Libraries
            Self::LibraryClusterStatus => write!(f, "libraryclusterstatus"),
            Self::LibraryAllClusterStatuses => write!(f, "libraryallclusterstatuses"),
            Self::InstallLibrary => write!(f, "installlibrary"),
            Self::UninstallLibrary => write!(f, "uninstalllibrary"),

            // IAM
            Self::ListUsers => write!(f, "listusers"),
            Self::GetUser => write!(f, "getuser"),
            Self::CreateUser => write!(f, "createuser"),
            Self::DeleteUser => write!(f, "deleteuser"),
            Self::ListGroups => write!(f, "listgroups"),
            Self::GetCurrentUser => write!(f, "getcurrentuser"),
            Self::ListServicePrincipals => write!(f, "listserviceprincipals"),

            // Permissions
            Self::GetPermissions => write!(f, "getpermissions"),
            Self::SetPermissions => write!(f, "setpermissions"),
            Self::GetPermissionLevels => write!(f, "getpermissionlevels"),

            // Git Credentials
            Self::ListGitCredentials => write!(f, "listgitcredentials"),
            Self::GetGitCredential => write!(f, "getgitcredential"),
            Self::CreateGitCredential => write!(f, "creategitcredential"),
            Self::DeleteGitCredential => write!(f, "deletegitcredential"),

            // Files
            Self::DbfsList => write!(f, "dbfslist"),
            Self::DbfsGetStatus => write!(f, "dbfsgetstatus"),
            Self::DbfsRead => write!(f, "dbfsread"),
            Self::DbfsPut => write!(f, "dbfsput"),
            Self::DbfsDelete => write!(f, "dbfsdelete"),
            Self::DbfsMkdirs => write!(f, "dbfsmkdirs"),
            Self::DbfsMove => write!(f, "dbfsmove"),

            // ML
            Self::ListExperiments => write!(f, "listexperiments"),
            Self::GetExperiment => write!(f, "getexperiment"),
            Self::CreateExperiment => write!(f, "createexperiment"),
            Self::DeleteExperiment => write!(f, "deleteexperiment"),
            Self::ListModels => write!(f, "listmodels"),
            Self::GetModel => write!(f, "getmodel"),
            Self::CreateModel => write!(f, "createmodel"),
            Self::DeleteModel => write!(f, "deletemodel"),
            Self::ListModelVersions => write!(f, "listmodelversions"),
            Self::GetModelVersion => write!(f, "getmodelversion"),
            Self::CreateModelVersion => write!(f, "createmodelversion"),

            // Settings
            Self::GetWorkspaceConfig => write!(f, "getworkspaceconfig"),
            Self::ListIpAccessLists => write!(f, "listipaccesslists"),
            Self::GetIpAccessList => write!(f, "getipaccesslist"),
            Self::CreateIpAccessList => write!(f, "createipaccesslist"),
            Self::DeleteIpAccessList => write!(f, "deleteipaccesslist"),

            // Global Init Scripts
            Self::ListGlobalInitScripts => write!(f, "listglobalinitscripts"),
            Self::GetGlobalInitScript => write!(f, "getglobalinitscript"),
            Self::CreateGlobalInitScript => write!(f, "createglobalinitscript"),
            Self::DeleteGlobalInitScript => write!(f, "deleteglobalinitscript"),

            // Apps
            Self::ListApps => write!(f, "listapps"),
            Self::GetApp => write!(f, "getapp"),
            Self::CreateApp => write!(f, "createapp"),
            Self::DeleteApp => write!(f, "deleteapp"),

            // Lakeflow
            Self::ListPipelines => write!(f, "listpipelines"),
            Self::GetPipeline => write!(f, "getpipeline"),
            Self::CreatePipeline => write!(f, "createpipeline"),
            Self::DeletePipeline => write!(f, "deletepipeline"),
            Self::StartPipeline => write!(f, "startpipeline"),
            Self::StopPipeline => write!(f, "stoppipeline"),

            // Delta Sharing
            Self::ListShares => write!(f, "listshares"),
            Self::GetShare => write!(f, "getshare"),
            Self::CreateShare => write!(f, "createshare"),
            Self::DeleteShare => write!(f, "deleteshare"),
            Self::ListRecipients => write!(f, "listrecipients"),

            // Vector Search
            Self::ListVectorIndexes => write!(f, "listvectorindexes"),
            Self::CreateVectorIndex => write!(f, "createvectorindex"),
            Self::DeleteVectorIndex => write!(f, "deletevectorindex"),
            Self::ListVectorEndpoints => write!(f, "listvectorendpoints"),
            Self::CreateVectorEndpoint => write!(f, "createvectorendpoint"),

            // Clean Rooms
            Self::ListCleanRooms => write!(f, "listcleanrooms"),
            Self::GetCleanRoom => write!(f, "getcleanroom"),
            Self::CreateCleanRoom => write!(f, "createcleanroom"),

            // Serving Endpoints
            Self::ListServingEndpoints => write!(f, "listservingendpoints"),
            Self::GetServingEndpoint => write!(f, "getservingendpoint"),
            Self::CreateServingEndpoint => write!(f, "createservingendpoint"),
            Self::DeleteServingEndpoint => write!(f, "deleteservingendpoint"),
            Self::QueryServingEndpoint => write!(f, "queryservingendpoint"),

            // Secrets
            Self::ListSecretScopes => write!(f, "listsecretscopes"),
            Self::CreateSecretScope => write!(f, "createsecretscope"),
            Self::DeleteSecretScope => write!(f, "deletesecretscope"),
            Self::ListSecrets => write!(f, "listsecrets"),
            Self::PutSecret => write!(f, "putsecret"),
            Self::DeleteSecret => write!(f, "deletesecret"),

            // Tokens
            Self::ListTokens => write!(f, "listtokens"),
            Self::CreateToken => write!(f, "createtoken"),
            Self::DeleteToken => write!(f, "deletetoken"),

            // SQL Analytics
            Self::ListQueries => write!(f, "listqueries"),
            Self::GetQuery => write!(f, "getquery"),
            Self::CreateQuery => write!(f, "createquery"),
            Self::DeleteQuery => write!(f, "deletequery"),
            Self::ListDashboards => write!(f, "listdashboards"),
            Self::GetDashboard => write!(f, "getdashboard"),
            Self::ListAlerts => write!(f, "listalerts"),
            Self::GetAlert => write!(f, "getalert"),
        }
    }
}
