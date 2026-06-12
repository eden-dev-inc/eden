pub mod cloudformation;
pub mod custom;
pub mod dynamodb;
pub mod ec2;
pub mod iam;
pub mod json_target;
pub mod lambda;
pub mod params;
pub mod query;
pub mod s3;
pub mod sqs;
pub mod sts;
pub mod types;

// New services
pub mod acm;
pub mod apigateway;
pub mod apigatewayv2;
pub mod athena;
pub mod autoscaling;
pub mod batch;
pub mod bedrock;
pub mod cloudfront;
pub mod cloudtrail;
pub mod cloudwatch;
pub mod cloudwatchlogs;
pub mod codebuild;
pub mod codedeploy;
pub mod codepipeline;
pub mod cognito;
pub mod config;
pub mod costexplorer;
pub mod ecr;
pub mod ecs;
pub mod eks;
pub mod elasticache;
pub mod elbv2;
pub mod emr;
pub mod eventbridge;
pub mod firehose;
pub mod glue;
pub mod guardduty;
pub mod kinesis;
pub mod organizations;
pub mod rds;
pub mod redshift;
pub mod route53;
pub mod sagemaker;
pub mod secretsmanager;
pub mod ses;
pub mod sesv2;
pub mod sfn;
pub mod sns;
pub mod ssm;
pub mod waf;
pub mod xray;
// Batch 4
pub mod codecommit;
pub mod comprehend;
pub mod controltower;
pub mod ds;
pub mod fms;
pub mod forecast;
pub mod globalaccelerator;
pub mod inspector;
pub mod iot;
pub mod lex;
pub mod macie;
pub mod mediaconvert;
pub mod medialive;
pub mod msk;
pub mod networkfirewall;
pub mod opensearch;
pub mod personalize;
pub mod polly;
pub mod ram;
pub mod rekognition;
pub mod securityhub;
pub mod servicequotas;
pub mod shield;
pub mod textract;
pub mod transcribe;
pub mod translate;
pub mod wafv2;
pub mod workspaces;
// Batch 5
pub mod appsync;
pub mod backup;
pub mod codeartifact;
pub mod dms;
pub mod docdb;
pub mod elasticbeanstalk;
pub mod fsx;
pub mod kendra;
pub mod kinesisanalyticsv2;
pub mod lakeformation;
pub mod lightsail;
pub mod memorydb;
pub mod mq;
pub mod neptune;
pub mod qldb;
pub mod quicksight;
pub mod servicecatalog;
pub mod storagegateway;
pub mod timestream;
pub mod transfer;
// Batch 6
pub mod acmpca;
pub mod amp;
pub mod cleanrooms;
pub mod cloudmap;
pub mod connect;
pub mod datasync;
pub mod datazone;
pub mod detective;
pub mod directconnect;
pub mod elastictranscoder;
pub mod gamelift;
pub mod iotanalytics;
pub mod iotevents;
pub mod ivs;
pub mod keyspaces;
pub mod kinesisvideostreams;
pub mod managedgrafana;
pub mod neptuneanalytics;
pub mod opsworks;
pub mod pinpoint;
pub mod route53resolver;
pub mod swf;
pub mod verifiedpermissions;
pub mod vpclattice;
// Batch 7
pub mod amplify;
pub mod appflow;
pub mod applicationdiscovery;
pub mod apprunner;
pub mod braket;
pub mod cloudhsmv2;
pub mod codeguru;
pub mod devopsguru;
pub mod drs;
pub mod efs;
pub mod frauddetector;
pub mod greengrassv2;
pub mod groundstation;
pub mod healthlake;
pub mod iotsitewise;
pub mod location;
pub mod lookoutequipment;
pub mod lookoutmetrics;
pub mod lookoutvision;
pub mod managedblockchain;
pub mod migrationhub;
pub mod networkmanager;
pub mod panorama;
pub mod proton;
pub mod redshiftserverless;
pub mod resiliencehub;
pub mod robomaker;
pub mod snowball;
pub mod workdocs;
pub mod workmail;
// Batch 8
pub mod bedrockagent;
pub mod codestarconnections;
pub mod computeoptimizer;
pub mod databrew;
pub mod emrserverless;
pub mod evidently;
pub mod internetmonitor;
pub mod iottwinmaker;
pub mod resourcegroupstagging;
pub mod rum;
pub mod s3control;
pub mod scheduler;
pub mod securitylake;
pub mod ssmincidents;
pub mod ssoadmin;
// Batch 9
pub mod budgets;
pub mod fis;
pub mod health;
pub mod licensemanager;
pub mod resourceexplorer;
pub mod resourcegroups;
pub mod savingsplans;
pub mod support;
pub mod synthetics;
// Batch 10
pub mod kms;

#[allow(unused_imports)]
use acm::*;
#[allow(unused_imports)]
use apigateway::*;
#[allow(unused_imports)]
use apigatewayv2::*;
#[allow(unused_imports)]
use athena::*;
#[allow(unused_imports)]
use autoscaling::*;
#[allow(unused_imports)]
use batch::*;
#[allow(unused_imports)]
use bedrock::*;
#[allow(unused_imports)]
use cloudformation::*;
#[allow(unused_imports)]
use cloudfront::*;
#[allow(unused_imports)]
use cloudtrail::*;
#[allow(unused_imports)]
use cloudwatch::*;
#[allow(unused_imports)]
use cloudwatchlogs::*;
#[allow(unused_imports)]
use codebuild::*;
#[allow(unused_imports)]
use codedeploy::*;
#[allow(unused_imports)]
use codepipeline::*;
#[allow(unused_imports)]
use cognito::*;
#[allow(unused_imports)]
use config::*;
#[allow(unused_imports)]
use costexplorer::*;
#[allow(unused_imports)]
use custom::*;
#[allow(unused_imports)]
use dynamodb::*;
#[allow(unused_imports)]
use ec2::*;
#[allow(unused_imports)]
use ecr::*;
#[allow(unused_imports)]
use ecs::*;
#[allow(unused_imports)]
use eks::*;
#[allow(unused_imports)]
use elasticache::*;
#[allow(unused_imports)]
use elbv2::*;
#[allow(unused_imports)]
use emr::*;
#[allow(unused_imports)]
use eventbridge::*;
#[allow(unused_imports)]
use firehose::*;
#[allow(unused_imports)]
use glue::*;
#[allow(unused_imports)]
use guardduty::*;
#[allow(unused_imports)]
use iam::*;
#[allow(unused_imports)]
use kinesis::*;
#[allow(unused_imports)]
use kms::*;
#[allow(unused_imports)]
use lambda::*;
#[allow(unused_imports)]
use organizations::*;
#[allow(unused_imports)]
use rds::*;
#[allow(unused_imports)]
use redshift::*;
#[allow(unused_imports)]
use route53::*;
#[allow(unused_imports)]
use s3::*;
#[allow(unused_imports)]
use sagemaker::*;
#[allow(unused_imports)]
use secretsmanager::*;
#[allow(unused_imports)]
use ses::*;
#[allow(unused_imports)]
use sesv2::*;
#[allow(unused_imports)]
use sfn::*;
#[allow(unused_imports)]
use sns::*;
#[allow(unused_imports)]
use sqs::*;
#[allow(unused_imports)]
use ssm::*;
#[allow(unused_imports)]
use sts::*;
#[allow(unused_imports)]
use waf::*;
#[allow(unused_imports)]
use xray::*;
// Batch 4
#[allow(unused_imports)]
use codecommit::*;
#[allow(unused_imports)]
use comprehend::*;
#[allow(unused_imports)]
use controltower::*;
#[allow(unused_imports)]
use ds::*;
#[allow(unused_imports)]
use fms::*;
#[allow(unused_imports)]
use forecast::*;
#[allow(unused_imports)]
use globalaccelerator::*;
#[allow(unused_imports)]
use inspector::*;
#[allow(unused_imports)]
use iot::*;
#[allow(unused_imports)]
use lex::*;
#[allow(unused_imports)]
use macie::*;
#[allow(unused_imports)]
use mediaconvert::*;
#[allow(unused_imports)]
use medialive::*;
#[allow(unused_imports)]
use msk::*;
#[allow(unused_imports)]
use networkfirewall::*;
#[allow(unused_imports)]
use opensearch::*;
#[allow(unused_imports)]
use personalize::*;
#[allow(unused_imports)]
use polly::*;
#[allow(unused_imports)]
use ram::*;
#[allow(unused_imports)]
use rekognition::*;
#[allow(unused_imports)]
use securityhub::*;
#[allow(unused_imports)]
use servicequotas::*;
#[allow(unused_imports)]
use shield::*;
#[allow(unused_imports)]
use textract::*;
#[allow(unused_imports)]
use transcribe::*;
#[allow(unused_imports)]
use translate::*;
#[allow(unused_imports)]
use wafv2::*;
#[allow(unused_imports)]
use workspaces::*;
// Batch 5
#[allow(unused_imports)]
use appsync::*;
#[allow(unused_imports)]
use backup::*;
#[allow(unused_imports)]
use codeartifact::*;
#[allow(unused_imports)]
use dms::*;
#[allow(unused_imports)]
use docdb::*;
#[allow(unused_imports)]
use elasticbeanstalk::*;
#[allow(unused_imports)]
use fsx::*;
#[allow(unused_imports)]
use kendra::*;
#[allow(unused_imports)]
use kinesisanalyticsv2::*;
#[allow(unused_imports)]
use lakeformation::*;
#[allow(unused_imports)]
use lightsail::*;
#[allow(unused_imports)]
use memorydb::*;
#[allow(unused_imports)]
use mq::*;
#[allow(unused_imports)]
use neptune::*;
#[allow(unused_imports)]
use qldb::*;
#[allow(unused_imports)]
use quicksight::*;
#[allow(unused_imports)]
use servicecatalog::*;
#[allow(unused_imports)]
use storagegateway::*;
#[allow(unused_imports)]
use timestream::*;
#[allow(unused_imports)]
use transfer::*;
// Batch 6
#[allow(unused_imports)]
use acmpca::*;
#[allow(unused_imports)]
use amp::*;
#[allow(unused_imports)]
use cleanrooms::*;
#[allow(unused_imports)]
use cloudmap::*;
#[allow(unused_imports)]
use connect::*;
#[allow(unused_imports)]
use datasync::*;
#[allow(unused_imports)]
use datazone::*;
#[allow(unused_imports)]
use detective::*;
#[allow(unused_imports)]
use directconnect::*;
#[allow(unused_imports)]
use elastictranscoder::*;
#[allow(unused_imports)]
use gamelift::*;
#[allow(unused_imports)]
use iotanalytics::*;
#[allow(unused_imports)]
use iotevents::*;
#[allow(unused_imports)]
use ivs::*;
#[allow(unused_imports)]
use keyspaces::*;
#[allow(unused_imports)]
use kinesisvideostreams::*;
#[allow(unused_imports)]
use managedgrafana::*;
#[allow(unused_imports)]
use neptuneanalytics::*;
#[allow(unused_imports)]
use opsworks::*;
#[allow(unused_imports)]
use pinpoint::*;
#[allow(unused_imports)]
use route53resolver::*;
#[allow(unused_imports)]
use swf::*;
#[allow(unused_imports)]
use verifiedpermissions::*;
#[allow(unused_imports)]
use vpclattice::*;
// Batch 7
#[allow(unused_imports)]
use amplify::*;
#[allow(unused_imports)]
use appflow::*;
#[allow(unused_imports)]
use applicationdiscovery::*;
#[allow(unused_imports)]
use apprunner::*;
#[allow(unused_imports)]
use braket::*;
#[allow(unused_imports)]
use cloudhsmv2::*;
#[allow(unused_imports)]
use codeguru::*;
#[allow(unused_imports)]
use devopsguru::*;
#[allow(unused_imports)]
use drs::*;
#[allow(unused_imports)]
use efs::*;
#[allow(unused_imports)]
use frauddetector::*;
#[allow(unused_imports)]
use greengrassv2::*;
#[allow(unused_imports)]
use groundstation::*;
#[allow(unused_imports)]
use healthlake::*;
#[allow(unused_imports)]
use iotsitewise::*;
#[allow(unused_imports)]
use location::*;
#[allow(unused_imports)]
use lookoutequipment::*;
#[allow(unused_imports)]
use lookoutmetrics::*;
#[allow(unused_imports)]
use lookoutvision::*;
#[allow(unused_imports)]
use managedblockchain::*;
#[allow(unused_imports)]
use migrationhub::*;
#[allow(unused_imports)]
use networkmanager::*;
#[allow(unused_imports)]
use panorama::*;
#[allow(unused_imports)]
use proton::*;
#[allow(unused_imports)]
use redshiftserverless::*;
#[allow(unused_imports)]
use resiliencehub::*;
#[allow(unused_imports)]
use robomaker::*;
#[allow(unused_imports)]
use snowball::*;
#[allow(unused_imports)]
use workdocs::*;
#[allow(unused_imports)]
use workmail::*;
// Batch 8
#[allow(unused_imports)]
use bedrockagent::*;
#[allow(unused_imports)]
use codestarconnections::*;
#[allow(unused_imports)]
use computeoptimizer::*;
#[allow(unused_imports)]
use databrew::*;
#[allow(unused_imports)]
use emrserverless::*;
#[allow(unused_imports)]
use evidently::*;
#[allow(unused_imports)]
use internetmonitor::*;
#[allow(unused_imports)]
use iottwinmaker::*;
#[allow(unused_imports)]
use resourcegroupstagging::*;
#[allow(unused_imports)]
use rum::*;
#[allow(unused_imports)]
use s3control::*;
#[allow(unused_imports)]
use scheduler::*;
#[allow(unused_imports)]
use securitylake::*;
#[allow(unused_imports)]
use ssmincidents::*;
#[allow(unused_imports)]
use ssoadmin::*;
// Batch 9
#[allow(unused_imports)]
use budgets::*;
#[allow(unused_imports)]
use fis::*;
#[allow(unused_imports)]
use health::*;
#[allow(unused_imports)]
use licensemanager::*;
#[allow(unused_imports)]
use resourceexplorer::*;
#[allow(unused_imports)]
use resourcegroups::*;
#[allow(unused_imports)]
use savingsplans::*;
#[allow(unused_imports)]
use support::*;
#[allow(unused_imports)]
use synthetics::*;

use serde::{Deserialize, Serialize};
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub enum AwsApi {
    Custom,
    Query,
    JsonTarget,
    // EC2
    Ec2DescribeInstances,
    Ec2DescribeVpcs,
    Ec2DescribeSubnets,
    Ec2DescribeSecurityGroups,
    Ec2DescribeImages,
    Ec2DescribeKeyPairs,
    Ec2DescribeVolumes,
    Ec2DescribeRegions,
    Ec2DescribeAvailabilityZones,
    Ec2StartInstances,
    Ec2StopInstances,
    Ec2TerminateInstances,
    Ec2RunInstances,
    Ec2DescribeNetworkInterfaces,
    Ec2DescribeNatGateways,
    Ec2DescribeRouteTables,
    Ec2DescribeAddresses,
    Ec2CreateVpc,
    Ec2DeleteVpc,
    Ec2CreateSubnet,
    Ec2DeleteSubnet,
    Ec2CreateSecurityGroup,
    Ec2DeleteSecurityGroup,
    Ec2AuthorizeSecurityGroupIngress,
    Ec2RevokeSecurityGroupIngress,
    Ec2AllocateAddress,
    Ec2ReleaseAddress,
    Ec2AssociateAddress,
    Ec2DisassociateAddress,
    Ec2CreateKeyPair,
    Ec2DeleteKeyPair,
    Ec2ImportKeyPair,
    Ec2CreateNatGateway,
    Ec2DeleteNatGateway,
    Ec2CreateInternetGateway,
    Ec2AttachInternetGateway,
    Ec2DetachInternetGateway,
    Ec2CreateRoute,
    Ec2DeleteRoute,
    Ec2CreateRouteTable,
    Ec2DeleteRouteTable,
    Ec2CreateSnapshot,
    Ec2DeleteSnapshot,
    Ec2DescribeSnapshots,
    Ec2CreateVolume,
    Ec2DeleteVolume,
    Ec2AttachVolume,
    Ec2DetachVolume,
    Ec2ModifyInstanceAttribute,
    Ec2DescribeInstanceStatus,
    Ec2CreateTags,
    Ec2DeleteTags,
    Ec2DescribeTags,
    Ec2RebootInstances,
    Ec2DescribeInternetGateways,
    Ec2CreateVpcPeeringConnection,
    Ec2DescribeVpcPeeringConnections,
    Ec2AcceptVpcPeeringConnection,
    Ec2DeleteVpcPeeringConnection,
    Ec2CreateLaunchTemplate,
    Ec2DescribeLaunchTemplates,
    Ec2DeleteLaunchTemplate,
    Ec2CreateFlowLogs,
    Ec2DescribeFlowLogs,
    Ec2DeleteFlowLogs,
    Ec2CreateVpcEndpoint,
    Ec2DescribeVpcEndpoints,
    Ec2DeleteVpcEndpoints,
    Ec2AuthorizeSecurityGroupEgress,
    Ec2RevokeSecurityGroupEgress,
    Ec2DescribeInstanceTypes,
    Ec2DescribeNetworkAcls,
    Ec2ModifyVpcAttribute,
    Ec2CopySnapshot,
    Ec2CopyImage,
    Ec2DeregisterImage,
    Ec2ModifyVolume,
    Ec2GetConsoleOutput,
    Ec2DescribeTransitGateways,
    Ec2CreateTransitGateway,
    Ec2DeleteTransitGateway,
    Ec2RequestSpotInstances,
    Ec2DescribeSpotInstanceRequests,
    Ec2CancelSpotInstanceRequests,
    Ec2CreateVpnGateway,
    Ec2DescribeVpnGateways,
    Ec2CreateVpnConnection,
    Ec2DescribeVpnConnections,
    Ec2CreateNetworkInterface,
    Ec2DeleteNetworkInterface,
    Ec2AttachNetworkInterface,
    Ec2DetachNetworkInterface,
    Ec2AssociateRouteTable,
    Ec2DisassociateRouteTable,
    Ec2CreateNetworkAcl,
    Ec2DeleteNetworkAcl,
    Ec2CreateNetworkAclEntry,
    Ec2DeleteNetworkAclEntry,
    Ec2CreatePlacementGroup,
    Ec2DeletePlacementGroup,
    Ec2DescribePlacementGroups,
    Ec2RegisterImage,
    Ec2ModifySubnetAttribute,
    Ec2DeleteVpnGateway,
    Ec2DeleteVpnConnection,
    Ec2AttachVpnGateway,
    Ec2DetachVpnGateway,
    Ec2CreateCustomerGateway,
    Ec2DescribeCustomerGateways,
    Ec2DeleteCustomerGateway,
    Ec2DescribeReservedInstances,
    Ec2ModifyInstanceMetadataOptions,
    Ec2DescribeTransitGatewayAttachments,
    // IAM
    IamListUsers,
    IamGetUser,
    IamListRoles,
    IamGetRole,
    IamListGroups,
    IamListPolicies,
    IamGetAccountSummary,
    IamListAccountAliases,
    IamCreateRole,
    IamDeleteRole,
    IamCreateUser,
    IamDeleteUser,
    IamAttachRolePolicy,
    IamDetachRolePolicy,
    IamCreateAccessKey,
    IamDeleteAccessKey,
    IamListAttachedRolePolicies,
    IamPutRolePolicy,
    IamDeleteRolePolicy,
    IamCreatePolicy,
    IamDeletePolicy,
    IamGetPolicy,
    IamCreateGroup,
    IamDeleteGroup,
    IamAddUserToGroup,
    IamRemoveUserFromGroup,
    IamCreateInstanceProfile,
    IamAddRoleToInstanceProfile,
    IamListAccessKeys,
    IamUpdateAccessKey,
    IamAttachUserPolicy,
    IamDetachUserPolicy,
    IamGetPolicyVersion,
    IamListPolicyVersions,
    IamChangePassword,
    IamUpdateLoginProfile,
    IamCreateLoginProfile,
    IamGetLoginProfile,
    IamDeleteLoginProfile,
    IamCreateServiceLinkedRole,
    IamDeleteServiceLinkedRole,
    IamListMfaDevices,
    IamGetRolePolicy,
    IamListRolePolicies,
    IamListUserPolicies,
    IamListGroupPolicies,
    IamListAttachedUserPolicies,
    IamCreatePolicyVersion,
    IamDeletePolicyVersion,
    IamSetDefaultPolicyVersion,
    IamListInstanceProfiles,
    IamListInstanceProfilesForRole,
    IamRemoveRoleFromInstanceProfile,
    IamDeleteInstanceProfile,
    IamGenerateCredentialReport,
    IamGetCredentialReport,
    IamTagRole,
    IamUntagRole,
    IamTagUser,
    IamUntagUser,
    IamPutUserPolicy,
    IamGetUserPolicy,
    IamDeleteUserPolicy,
    IamPutGroupPolicy,
    IamGetGroupPolicy,
    IamDeleteGroupPolicy,
    IamGetAccountPasswordPolicy,
    IamUpdateAccountPasswordPolicy,
    // STS
    StsGetCallerIdentity,
    StsAssumeRole,
    StsGetSessionToken,
    StsGetAccessKeyInfo,
    StsDecodeAuthorizationMessage,
    StsAssumeRoleWithSaml,
    StsAssumeRoleWithWebIdentity,
    // Lambda
    LambdaListFunctions,
    LambdaGetFunction,
    LambdaInvokeFunction,
    LambdaCreateFunction,
    LambdaUpdateFunctionCode,
    LambdaDeleteFunction,
    LambdaListEventSourceMappings,
    LambdaAddPermission,
    LambdaRemovePermission,
    LambdaGetPolicy,
    LambdaUpdateFunctionConfiguration,
    LambdaPublishVersion,
    LambdaListAliases,
    LambdaCreateAlias,
    LambdaListLayers,
    LambdaGetLayerVersion,
    LambdaListTags,
    LambdaTagResource,
    LambdaDeleteAlias,
    LambdaUpdateAlias,
    LambdaCreateEventSourceMapping,
    LambdaDeleteEventSourceMapping,
    LambdaUpdateEventSourceMapping,
    LambdaGetEventSourceMapping,
    LambdaGetFunctionConfiguration,
    LambdaPutFunctionConcurrency,
    LambdaDeleteFunctionConcurrency,
    LambdaDeleteLayerVersion,
    LambdaGetAccountSettings,
    LambdaListVersionsByFunction,
    LambdaCreateFunctionUrlConfig,
    LambdaGetFunctionUrlConfig,
    LambdaUpdateFunctionUrlConfig,
    LambdaDeleteFunctionUrlConfig,
    LambdaUntagResource,
    // S3
    S3ListBuckets,
    S3ListObjects,
    S3GetObject,
    S3PutObject,
    S3DeleteObject,
    S3CreateBucket,
    S3DeleteBucket,
    S3HeadObject,
    S3CopyObject,
    S3ListObjectVersions,
    S3GetBucketPolicy,
    S3PutBucketPolicy,
    S3GetBucketVersioning,
    S3PutBucketVersioning,
    S3GetBucketEncryption,
    S3PutBucketEncryption,
    S3GetBucketTagging,
    S3PutBucketTagging,
    S3GetBucketLocation,
    S3PutBucketLifecycleConfiguration,
    S3GetObjectTagging,
    S3PutObjectTagging,
    S3ListMultipartUploads,
    S3AbortMultipartUpload,
    S3CompleteMultipartUpload,
    S3CreateMultipartUpload,
    S3DeleteObjects,
    S3GetBucketAcl,
    S3PutBucketAcl,
    S3GetBucketCors,
    S3PutBucketCors,
    S3GetBucketLogging,
    S3PutBucketLogging,
    S3GetBucketNotificationConfiguration,
    S3PutBucketNotificationConfiguration,
    S3RestoreObject,
    S3UploadPart,
    S3DeleteBucketPolicy,
    S3DeleteBucketEncryption,
    S3DeleteBucketLifecycleConfiguration,
    S3DeleteBucketTagging,
    S3DeleteBucketCors,
    S3HeadBucket,
    S3ListParts,
    S3SelectObjectContent,
    S3GetObjectLockConfiguration,
    S3PutBucketReplication,
    // DynamoDB
    DynamoDbListTables,
    DynamoDbDescribeTable,
    DynamoDbGetItem,
    DynamoDbPutItem,
    DynamoDbDeleteItem,
    DynamoDbQuery,
    DynamoDbScan,
    DynamoDbCreateTable,
    DynamoDbDeleteTable,
    DynamoDbUpdateItem,
    DynamoDbBatchGetItem,
    DynamoDbBatchWriteItem,
    DynamoDbUpdateTable,
    DynamoDbDescribeTimeToLive,
    DynamoDbUpdateTimeToLive,
    DynamoDbTransactGetItems,
    DynamoDbTransactWriteItems,
    DynamoDbCreateBackup,
    DynamoDbDeleteBackup,
    DynamoDbDescribeBackup,
    DynamoDbListBackups,
    DynamoDbRestoreTableFromBackup,
    DynamoDbDescribeContinuousBackups,
    DynamoDbUpdateContinuousBackups,
    DynamoDbRestoreTableToPointInTime,
    DynamoDbListTagsOfResource,
    DynamoDbTagResource,
    DynamoDbUntagResource,
    DynamoDbCreateGlobalTable,
    DynamoDbDescribeGlobalTable,
    DynamoDbListGlobalTables,
    DynamoDbExportTableToPointInTime,
    DynamoDbDescribeEndpoints,
    DynamoDbDescribeLimits,
    // CloudFormation
    CfDescribeStacks,
    CfListStacks,
    CfCreateStack,
    CfDeleteStack,
    CfUpdateStack,
    CfDescribeStackResources,
    CfDescribeStackEvents,
    CfGetTemplate,
    CfValidateTemplate,
    CfListStackResources,
    CfListExports,
    CfCreateChangeSet,
    CfDeleteChangeSet,
    CfDescribeChangeSet,
    CfExecuteChangeSet,
    CfListChangeSets,
    CfGetTemplateSummary,
    CfDetectStackDrift,
    CfCancelUpdateStack,
    CfDescribeStackDriftDetectionStatus,
    CfContinueUpdateRollback,
    // SQS
    SqsListQueues,
    SqsCreateQueue,
    SqsSendMessage,
    SqsReceiveMessage,
    SqsDeleteQueue,
    SqsGetQueueUrl,
    SqsGetQueueAttributes,
    SqsSetQueueAttributes,
    SqsPurgeQueue,
    SqsDeleteMessage,
    SqsChangeMessageVisibility,
    SqsTagQueue,
    SqsUntagQueue,
    SqsListQueueTags,
    // SNS
    SnsListTopics,
    SnsCreateTopic,
    SnsDeleteTopic,
    SnsPublish,
    SnsSubscribe,
    SnsUnsubscribe,
    SnsListSubscriptions,
    SnsSetSubscriptionAttributes,
    SnsGetSubscriptionAttributes,
    SnsSetTopicAttributes,
    SnsGetTopicAttributes,
    SnsListSubscriptionsByTopic,
    SnsTagResource,
    SnsUntagResource,
    SnsConfirmSubscription,
    SnsListTagsForResource,
    // AutoScaling
    AutoScalingDescribeAutoScalingGroups,
    AutoScalingDescribeLaunchConfigurations,
    AutoScalingSetDesiredCapacity,
    AutoScalingUpdateAutoScalingGroup,
    AutoScalingDeleteAutoScalingGroup,
    AutoScalingCreateAutoScalingGroup,
    AutoScalingCreateLaunchConfiguration,
    AutoScalingDescribeScalingActivities,
    AutoScalingDescribePolicies,
    AutoScalingPutScalingPolicy,
    AutoScalingExecutePolicy,
    // RDS
    RdsDescribeDbInstances,
    RdsDescribeDbClusters,
    RdsCreateDbInstance,
    RdsDeleteDbInstance,
    RdsDescribeDbSnapshots,
    RdsModifyDbInstance,
    RdsRebootDbInstance,
    RdsCreateDbCluster,
    RdsDeleteDbCluster,
    RdsModifyDbCluster,
    RdsCreateDbSnapshot,
    RdsRestoreDbInstanceFromDbSnapshot,
    RdsDescribeDbSubnetGroups,
    RdsDescribeEvents,
    RdsStartDbInstance,
    RdsStopDbInstance,
    RdsCreateDbSubnetGroup,
    RdsDeleteDbSubnetGroup,
    RdsModifyDbSubnetGroup,
    RdsCreateDbClusterSnapshot,
    RdsDeleteDbSnapshot,
    RdsDescribeDbClusterSnapshots,
    RdsDescribeDbEngineVersions,
    RdsCreateDbInstanceReadReplica,
    RdsFailoverDbCluster,
    RdsCopyDbSnapshot,
    RdsAddTagsToResource,
    RdsRemoveTagsFromResource,
    RdsPromoteReadReplica,
    RdsListTagsForResource,
    RdsDescribeOrderableDbInstanceOptions,
    // ElastiCache
    ElastiCacheDescribeCacheClusters,
    ElastiCacheDescribeCacheSubnetGroups,
    ElastiCacheDescribeReplicationGroups,
    ElastiCacheCreateCacheCluster,
    ElastiCacheDeleteCacheCluster,
    ElastiCacheModifyCacheCluster,
    ElastiCacheCreateReplicationGroup,
    ElastiCacheDeleteReplicationGroup,
    ElastiCacheCreateCacheSubnetGroup,
    ElastiCacheDeleteCacheSubnetGroup,
    ElastiCacheModifyReplicationGroup,
    ElastiCacheCreateSnapshot,
    ElastiCacheDeleteSnapshot,
    ElastiCacheDescribeSnapshots,
    ElastiCacheAddTagsToResource,
    ElastiCacheRemoveTagsFromResource,
    ElastiCacheListTagsForResource,
    // Redshift
    RedshiftDescribeClusters,
    RedshiftDescribeClusterSubnetGroups,
    RedshiftCreateCluster,
    RedshiftDeleteCluster,
    RedshiftModifyCluster,
    RedshiftResizeCluster,
    RedshiftCreateClusterSubnetGroup,
    RedshiftDescribeClusterSnapshots,
    RedshiftPauseCluster,
    RedshiftResumeCluster,
    RedshiftDescribeClusterParameterGroups,
    RedshiftCreateClusterParameterGroup,
    RedshiftEnableLogging,
    RedshiftDisableLogging,
    RedshiftDescribeLoggingStatus,
    RedshiftCreateTags,
    RedshiftDeleteTags,
    // CloudWatch
    CloudWatchListMetrics,
    CloudWatchGetMetricStatistics,
    CloudWatchDescribeAlarms,
    CloudWatchPutMetricAlarm,
    CloudWatchDeleteAlarms,
    CloudWatchPutMetricData,
    CloudWatchGetMetricData,
    CloudWatchDescribeAlarmHistory,
    CloudWatchSetAlarmState,
    CloudWatchGetDashboard,
    CloudWatchListDashboards,
    CloudWatchPutDashboard,
    CloudWatchEnableAlarmActions,
    CloudWatchDisableAlarmActions,
    CloudWatchTagResource,
    CloudWatchUntagResource,
    CloudWatchListTagsForResource,
    CloudWatchDescribeAnomalyDetectors,
    // ELBv2
    ElbV2DescribeLoadBalancers,
    ElbV2DescribeListeners,
    ElbV2DescribeTargetGroups,
    ElbV2DescribeTargetHealth,
    ElbV2CreateLoadBalancer,
    ElbV2DeleteLoadBalancer,
    ElbV2CreateTargetGroup,
    ElbV2DeleteTargetGroup,
    ElbV2RegisterTargets,
    ElbV2DeregisterTargets,
    ElbV2CreateListener,
    ElbV2DeleteListener,
    ElbV2ModifyListener,
    ElbV2CreateRule,
    ElbV2DeleteRule,
    ElbV2ModifyLoadBalancerAttributes,
    ElbV2DescribeLoadBalancerAttributes,
    ElbV2ModifyTargetGroup,
    ElbV2DescribeTargetGroupAttributes,
    ElbV2AddTags,
    ElbV2RemoveTags,
    ElbV2DescribeTags,
    ElbV2SetSecurityGroups,
    ElbV2SetSubnets,
    ElbV2ModifyRule,
    // EMR
    EmrListClusters,
    EmrDescribeCluster,
    EmrRunJobFlow,
    EmrTerminateJobFlows,
    // Kinesis
    KinesisListStreams,
    KinesisDescribeStream,
    KinesisCreateStream,
    KinesisDeleteStream,
    KinesisPutRecord,
    KinesisPutRecords,
    KinesisGetShardIterator,
    KinesisGetRecords,
    // Firehose
    FirehoseListDeliveryStreams,
    FirehoseDescribeDeliveryStream,
    FirehosePutRecord,
    FirehosePutRecordBatch,
    // CloudWatch Logs
    CloudWatchLogsDescribeLogGroups,
    CloudWatchLogsDescribeLogStreams,
    CloudWatchLogsGetLogEvents,
    CloudWatchLogsFilterLogEvents,
    CloudWatchLogsCreateLogGroup,
    CloudWatchLogsDeleteLogGroup,
    CloudWatchLogsPutLogEvents,
    CloudWatchLogsPutRetentionPolicy,
    CloudWatchLogsDeleteRetentionPolicy,
    CloudWatchLogsPutSubscriptionFilter,
    CloudWatchLogsDescribeSubscriptionFilters,
    CloudWatchLogsDeleteSubscriptionFilter,
    CloudWatchLogsCreateLogStream,
    // Step Functions
    SfnListStateMachines,
    SfnDescribeStateMachine,
    SfnStartExecution,
    SfnStopExecution,
    SfnListExecutions,
    SfnDescribeExecution,
    // CodePipeline
    CodePipelineListPipelines,
    CodePipelineGetPipeline,
    CodePipelineStartPipelineExecution,
    CodePipelineGetPipelineExecution,
    // CodeDeploy
    CodeDeployListApplications,
    CodeDeployListDeployments,
    CodeDeployGetDeployment,
    CodeDeployCreateDeployment,
    // WAF
    WafListWebAcls,
    WafGetWebAcl,
    // CodeBuild
    CodeBuildListProjects,
    CodeBuildBatchGetProjects,
    CodeBuildListBuildsForProject,
    CodeBuildStartBuild,
    // Secrets Manager
    SecretsManagerGetSecretValue,
    SecretsManagerListSecrets,
    SecretsManagerCreateSecret,
    SecretsManagerDeleteSecret,
    SecretsManagerUpdateSecret,
    SecretsManagerPutSecretValue,
    SecretsManagerRotateSecret,
    SecretsManagerDescribeSecret,
    SecretsManagerRestoreSecret,
    SecretsManagerGetRandomPassword,
    SecretsManagerListSecretVersionIds,
    SecretsManagerTagResource,
    SecretsManagerUntagResource,
    // ECS
    EcsListClusters,
    EcsDescribeClusters,
    EcsListServices,
    EcsDescribeServices,
    EcsListTasks,
    EcsDescribeTasks,
    EcsRunTask,
    EcsStopTask,
    EcsCreateCluster,
    EcsDeleteCluster,
    EcsUpdateService,
    EcsCreateService,
    EcsDeleteService,
    EcsRegisterTaskDefinition,
    EcsDeregisterTaskDefinition,
    EcsDescribeTaskDefinition,
    EcsListTaskDefinitions,
    EcsExecuteCommand,
    EcsUpdateCluster,
    EcsListContainerInstances,
    EcsDescribeContainerInstances,
    EcsTagResource,
    EcsUntagResource,
    EcsListTagsForResource,
    EcsCreateCapacityProvider,
    // EKS
    EksListClusters,
    EksDescribeCluster,
    EksListNodegroups,
    EksDescribeNodegroup,
    EksCreateCluster,
    EksDeleteCluster,
    EksUpdateClusterConfig,
    EksCreateNodegroup,
    EksDeleteNodegroup,
    EksListAddons,
    EksDescribeAddon,
    EksCreateAddon,
    EksDeleteAddon,
    EksUpdateAddon,
    EksCreateFargateProfile,
    EksDeleteFargateProfile,
    EksDescribeFargateProfile,
    EksListFargateProfiles,
    EksUpdateNodegroupConfig,
    EksUpdateClusterVersion,
    EksTagResource,
    EksUntagResource,
    EksListTagsForResource,
    // API Gateway
    ApiGatewayGetRestApis,
    ApiGatewayGetResources,
    ApiGatewayGetStages,
    ApiGatewayCreateRestApi,
    ApiGatewayDeleteRestApi,
    // Batch
    BatchListJobs,
    BatchDescribeJobs,
    BatchSubmitJob,
    BatchCancelJob,
    // CloudFront
    CloudFrontListDistributions,
    CloudFrontGetDistribution,
    CloudFrontCreateDistribution,
    CloudFrontDeleteDistribution,
    CloudFrontUpdateDistribution,
    CloudFrontGetDistributionConfig,
    CloudFrontCreateInvalidation,
    CloudFrontListInvalidations,
    CloudFrontGetInvalidation,
    CloudFrontCreateOriginAccessControl,
    CloudFrontGetOriginAccessControl,
    CloudFrontListOriginAccessControls,
    CloudFrontDeleteOriginAccessControl,
    CloudFrontListCachePolicies,
    CloudFrontGetCachePolicy,
    // Route 53
    Route53ListHostedZones,
    Route53ListResourceRecordSets,
    Route53ChangeResourceRecordSets,
    Route53CreateHostedZone,
    Route53DeleteHostedZone,
    Route53GetHostedZone,
    Route53ListHealthChecks,
    Route53CreateHealthCheck,
    Route53DeleteHealthCheck,
    Route53GetHealthCheck,
    Route53GetHostedZoneCount,
    Route53TestDnsAnswer,
    Route53ListHostedZonesByName,
    // ECR
    EcrListRepositories,
    EcrDescribeRepositories,
    EcrGetAuthorizationToken,
    EcrCreateRepository,
    EcrDeleteRepository,
    EcrListImages,
    EcrDescribeImages,
    EcrBatchGetImage,
    EcrBatchDeleteImage,
    EcrPutImage,
    EcrGetLifecyclePolicy,
    EcrPutLifecyclePolicy,
    EcrGetRepositoryPolicy,
    // SSM
    SsmGetParameter,
    SsmGetParameters,
    SsmPutParameter,
    SsmDeleteParameter,
    SsmDescribeParameters,
    SsmGetParametersByPath,
    SsmSendCommand,
    SsmListCommands,
    SsmGetCommandInvocation,
    SsmListAssociations,
    SsmCreateAssociation,
    SsmDescribeInstanceInformation,
    SsmGetParameterHistory,
    SsmDeleteParameters,
    SsmStartAutomationExecution,
    SsmDescribeAutomationExecutions,
    SsmStartSession,
    // EventBridge
    EventBridgeListRules,
    EventBridgePutRule,
    EventBridgeDeleteRule,
    EventBridgePutEvents,
    EventBridgeListTargetsByRule,
    EventBridgePutTargets,
    EventBridgeRemoveTargets,
    // Cognito
    CognitoListUserPools,
    CognitoDescribeUserPool,
    CognitoListUsers,
    CognitoAdminCreateUser,
    CognitoAdminDeleteUser,
    CognitoAdminGetUser,
    CognitoInitiateAuth,
    CognitoSignUp,
    CognitoConfirmSignUp,
    CognitoForgotPassword,
    CognitoConfirmForgotPassword,
    CognitoAdminSetUserPassword,
    CognitoAdminDisableUser,
    CognitoAdminEnableUser,
    CognitoCreateUserPool,
    CognitoDeleteUserPool,
    CognitoCreateUserPoolClient,
    CognitoDescribeUserPoolClient,
    CognitoListUserPoolClients,
    CognitoDeleteUserPoolClient,
    CognitoCreateGroup,
    CognitoDeleteGroup,
    CognitoListGroups,
    CognitoAdminAddUserToGroup,
    CognitoAdminRemoveUserFromGroup,
    // SES
    SesListIdentities,
    SesSendEmail,
    SesVerifyEmailIdentity,
    SesDeleteIdentity,
    SesGetSendQuota,
    // ACM
    AcmListCertificates,
    AcmDescribeCertificate,
    AcmRequestCertificate,
    AcmDeleteCertificate,
    AcmGetCertificate,
    // CloudTrail
    CloudTrailDescribeTrails,
    CloudTrailGetTrail,
    CloudTrailGetTrailStatus,
    CloudTrailLookupEvents,
    CloudTrailStartLogging,
    CloudTrailStopLogging,
    // API Gateway V2
    ApiGatewayV2GetApis,
    ApiGatewayV2GetApi,
    ApiGatewayV2CreateApi,
    ApiGatewayV2DeleteApi,
    ApiGatewayV2GetStages,
    ApiGatewayV2GetRoutes,
    ApiGatewayV2GetIntegrations,
    ApiGatewayV2GetDeployments,
    // Athena
    AthenaStartQueryExecution,
    AthenaGetQueryExecution,
    AthenaGetQueryResults,
    AthenaStopQueryExecution,
    // Glue
    GlueGetDatabases,
    GlueGetTables,
    GlueStartJobRun,
    GlueGetJobRun,
    GlueListJobs,
    GlueGetJob,
    GlueCreateJob,
    GlueDeleteJob,
    GlueGetCrawlers,
    GlueStartCrawler,
    GlueCreateCrawler,
    GlueGetPartitions,
    GlueGetTable,
    // Organizations
    OrganizationsListAccounts,
    OrganizationsDescribeAccount,
    OrganizationsListRoots,
    OrganizationsListOrganizationalUnitsForParent,
    // GuardDuty
    GuardDutyListDetectors,
    GuardDutyListFindings,
    GuardDutyGetFindings,
    GuardDutyCreateDetector,
    GuardDutyDeleteDetector,
    // X-Ray
    XRayGetTraceSummaries,
    XRayGetTraceGraph,
    XRayPutTraceSegments,
    // SageMaker
    SageMakerCreateTrainingJob,
    SageMakerDescribeTrainingJob,
    SageMakerListTrainingJobs,
    SageMakerCreateEndpoint,
    SageMakerDescribeEndpoint,
    SageMakerDeleteEndpoint,
    SageMakerListEndpoints,
    SageMakerCreateModel,
    SageMakerDeleteModel,
    SageMakerDescribeModel,
    SageMakerListModels,
    SageMakerCreateNotebookInstance,
    SageMakerDeleteNotebookInstance,
    SageMakerDescribeNotebookInstance,
    SageMakerListNotebookInstances,
    SageMakerCreateProcessingJob,
    SageMakerDescribeProcessingJob,
    SageMakerListProcessingJobs,
    SageMakerStopTrainingJob,
    SageMakerCreateTransformJob,
    SageMakerListTransformJobs,
    // Bedrock
    BedrockListFoundationModels,
    BedrockInvokeModel,
    BedrockGetFoundationModel,
    BedrockListCustomModels,
    BedrockCreateModelCustomizationJob,
    BedrockListGuardrails,
    BedrockCreateGuardrail,
    // Config
    ConfigListDiscoveredResources,
    ConfigGetResourceConfigHistory,
    ConfigDescribeConfigRules,
    // Cost Explorer
    CostExplorerGetCostAndUsage,
    CostExplorerGetCostForecast,
    // WAFv2
    WafV2ListWebAcls,
    WafV2GetWebAcl,
    WafV2CreateWebAcl,
    WafV2DeleteWebAcl,
    // OpenSearch
    OpenSearchListDomains,
    OpenSearchDescribeDomain,
    OpenSearchCreateDomain,
    OpenSearchDeleteDomain,
    OpenSearchDescribeDomainConfig,
    OpenSearchUpdateDomainConfig,
    OpenSearchAddTags,
    OpenSearchRemoveTags,
    OpenSearchListTags,
    // MSK
    MskListClusters,
    MskDescribeCluster,
    MskCreateCluster,
    MskDeleteCluster,
    MskListKafkaVersions,
    // CodeCommit
    CodeCommitListRepositories,
    CodeCommitGetRepository,
    CodeCommitCreateRepository,
    CodeCommitDeleteRepository,
    CodeCommitListBranches,
    // Security Hub
    SecurityHubGetFindings,
    SecurityHubBatchImportFindings,
    SecurityHubEnableSecurityHub,
    SecurityHubDisableSecurityHub,
    // Inspector v2
    InspectorListFindings,
    InspectorListCoverage,
    InspectorEnable,
    InspectorDisable,
    // RAM
    RamListResources,
    RamListResourceShares,
    RamCreateResourceShare,
    RamDeleteResourceShare,
    // Comprehend
    ComprehendDetectSentiment,
    ComprehendDetectEntities,
    ComprehendDetectDominantLanguage,
    ComprehendBatchDetectSentiment,
    // Rekognition
    RekognitionDetectLabels,
    RekognitionDetectFaces,
    RekognitionIndexFaces,
    RekognitionListCollections,
    RekognitionSearchFacesByImage,
    // Transcribe
    TranscribeStartTranscriptionJob,
    TranscribeGetTranscriptionJob,
    TranscribeListTranscriptionJobs,
    TranscribeDeleteTranscriptionJob,
    // Translate
    TranslateTranslateText,
    TranslateListTextTranslationJobs,
    TranslateStartTextTranslationJob,
    // Textract
    TextractDetectDocumentText,
    TextractAnalyzeDocument,
    TextractStartDocumentAnalysis,
    TextractGetDocumentAnalysis,
    // Polly
    PollyDescribeVoices,
    PollySynthesizeSpeech,
    PollyListLexicons,
    // Service Quotas
    ServiceQuotasListServices,
    ServiceQuotasListServiceQuotas,
    ServiceQuotasGetServiceQuota,
    ServiceQuotasRequestServiceQuotaIncrease,
    // Control Tower
    ControlTowerListEnabledControls,
    ControlTowerListLandingZones,
    ControlTowerGetLandingZone,
    // Network Firewall
    NetworkFirewallListFirewalls,
    NetworkFirewallDescribeFirewall,
    NetworkFirewallCreateFirewall,
    NetworkFirewallDeleteFirewall,
    // Global Accelerator
    GlobalAcceleratorListAccelerators,
    GlobalAcceleratorDescribeAccelerator,
    GlobalAcceleratorCreateAccelerator,
    GlobalAcceleratorDeleteAccelerator,
    // IoT
    IotListThings,
    IotDescribeThing,
    IotCreateThing,
    IotDeleteThing,
    IotListThingGroups,
    // MediaLive
    MediaLiveListChannels,
    MediaLiveDescribeChannel,
    MediaLiveCreateChannel,
    MediaLiveDeleteChannel,
    MediaLiveStartChannel,
    MediaLiveStopChannel,
    // MediaConvert
    MediaConvertListJobs,
    MediaConvertGetJob,
    MediaConvertCreateJob,
    MediaConvertCancelJob,
    MediaConvertListJobTemplates,
    // WorkSpaces
    WorkSpacesDescribeWorkspaces,
    WorkSpacesDescribeWorkspaceDirectories,
    WorkSpacesCreateWorkspaces,
    WorkSpacesTerminateWorkspaces,
    // Directory Service
    DsDescribeDirectories,
    DsCreateDirectory,
    DsDeleteDirectory,
    DsListTagsForResource,
    // Lex
    LexListBots,
    LexDescribeBotVersion,
    LexCreateBot,
    LexDeleteBot,
    // Personalize
    PersonalizeListDatasets,
    PersonalizeDescribeDataset,
    PersonalizeListCampaigns,
    PersonalizeDescribeCampaign,
    // Forecast
    ForecastListDatasets,
    ForecastDescribeDataset,
    ForecastListPredictors,
    ForecastCreatePredictor,
    // Macie
    MacieListFindings,
    MacieGetFindings,
    MacieDescribeBuckets,
    MacieEnableMacie,
    MacieDisableMacie,
    // Shield
    ShieldListProtections,
    ShieldDescribeProtection,
    ShieldCreateProtection,
    ShieldDeleteProtection,
    // Firewall Manager
    FmsListPolicies,
    FmsGetPolicy,
    FmsPutPolicy,
    FmsDeletePolicy,
    // AppSync
    AppSyncListGraphqlApis,
    AppSyncGetGraphqlApi,
    AppSyncCreateGraphqlApi,
    AppSyncDeleteGraphqlApi,
    // Backup
    BackupListBackupPlans,
    BackupGetBackupPlan,
    BackupCreateBackupPlan,
    BackupDeleteBackupPlan,
    BackupListBackupVaults,
    // CodeArtifact
    CodeArtifactListDomains,
    CodeArtifactListRepositories,
    CodeArtifactGetRepositoryEndpoint,
    CodeArtifactDeleteDomain,
    // DMS
    DmsDescribeReplicationInstances,
    DmsCreateReplicationInstance,
    DmsDeleteReplicationInstance,
    DmsDescribeEndpoints,
    // DocumentDB
    DocDbDescribeDbClusters,
    DocDbCreateDbCluster,
    DocDbDeleteDbCluster,
    DocDbDescribeDbInstances,
    // Elastic Beanstalk
    ElasticBeanstalkDescribeApplications,
    ElasticBeanstalkCreateApplication,
    ElasticBeanstalkDeleteApplication,
    ElasticBeanstalkDescribeEnvironments,
    // FSx
    FSxDescribeFileSystems,
    FSxCreateFileSystem,
    FSxDeleteFileSystem,
    FSxListTagsForResource,
    // Kendra
    KendraListIndices,
    KendraDescribeIndex,
    KendraCreateIndex,
    KendraDeleteIndex,
    KendraQuery,
    // Kinesis Data Analytics v2
    KinesisAnalyticsListApplications,
    KinesisAnalyticsDescribeApplication,
    KinesisAnalyticsCreateApplication,
    KinesisAnalyticsDeleteApplication,
    // Lake Formation
    LakeFormationGetDataLakeSettings,
    LakeFormationPutDataLakeSettings,
    LakeFormationGrantPermissions,
    LakeFormationListPermissions,
    // Lightsail
    LightsailGetInstances,
    LightsailGetInstance,
    LightsailCreateInstances,
    LightsailDeleteInstance,
    LightsailGetBundles,
    // MemoryDB
    MemoryDbDescribeClusters,
    MemoryDbCreateCluster,
    MemoryDbDeleteCluster,
    MemoryDbDescribeSubnetGroups,
    // MQ
    MqListBrokers,
    MqDescribeBroker,
    MqCreateBroker,
    MqDeleteBroker,
    // Neptune
    NeptuneDescribeDbClusters,
    NeptuneCreateDbCluster,
    NeptuneDeleteDbCluster,
    NeptuneDescribeDbInstances,
    // QLDB
    QldbListLedgers,
    QldbDescribeLedger,
    QldbCreateLedger,
    QldbDeleteLedger,
    // QuickSight
    QuickSightListDashboards,
    QuickSightDescribeDashboard,
    QuickSightListDataSets,
    QuickSightCreateDashboard,
    // Service Catalog
    ServiceCatalogListPortfolios,
    ServiceCatalogSearchProducts,
    ServiceCatalogDescribeProduct,
    ServiceCatalogProvisionProduct,
    // Storage Gateway
    StorageGatewayListGateways,
    StorageGatewayDescribeGatewayInformation,
    StorageGatewayActivateGateway,
    StorageGatewayDeleteGateway,
    // Timestream
    TimestreamListDatabases,
    TimestreamCreateDatabase,
    TimestreamDeleteDatabase,
    TimestreamWriteRecords,
    TimestreamQuery,
    // Transfer Family
    TransferListServers,
    TransferDescribeServer,
    TransferCreateServer,
    TransferDeleteServer,
    // Connect
    ConnectListInstances,
    ConnectDescribeInstance,
    ConnectCreateContactFlow,
    ConnectListContactFlows,
    ConnectListQueues,
    // Pinpoint
    PinpointGetApps,
    PinpointCreateApp,
    PinpointDeleteApp,
    PinpointSendMessages,
    PinpointGetEndpoint,
    // DataSync
    DataSyncListTasks,
    DataSyncDescribeTask,
    DataSyncCreateTask,
    DataSyncDeleteTask,
    DataSyncStartTaskExecution,
    // ACM PCA
    AcmPcaListCertificateAuthorities,
    AcmPcaDescribeCertificateAuthority,
    AcmPcaCreateCertificateAuthority,
    AcmPcaDeleteCertificateAuthority,
    AcmPcaIssueCertificate,
    // Route53 Resolver
    Route53ResolverListResolverRules,
    Route53ResolverGetResolverRule,
    Route53ResolverCreateResolverRule,
    Route53ResolverDeleteResolverRule,
    Route53ResolverListResolverEndpoints,
    // VPC Lattice
    VpcLatticeListServiceNetworks,
    VpcLatticeCreateServiceNetwork,
    VpcLatticeDeleteServiceNetwork,
    VpcLatticeListServices,
    // Cloud Map
    CloudMapListNamespaces,
    CloudMapGetNamespace,
    CloudMapCreatePrivateDnsNamespace,
    CloudMapDeleteNamespace,
    CloudMapListServices,
    // Direct Connect
    DirectConnectDescribeConnections,
    DirectConnectDescribeVirtualInterfaces,
    DirectConnectCreateConnection,
    DirectConnectDeleteConnection,
    // Verified Permissions
    VerifiedPermissionsListPolicyStores,
    VerifiedPermissionsCreatePolicyStore,
    VerifiedPermissionsIsAuthorized,
    VerifiedPermissionsCreatePolicy,
    // Detective
    DetectiveListGraphs,
    DetectiveCreateGraph,
    DetectiveDeleteGraph,
    DetectiveListMembers,
    // Keyspaces
    KeyspacesListKeyspaces,
    KeyspacesGetKeyspace,
    KeyspacesCreateKeyspace,
    KeyspacesDeleteKeyspace,
    KeyspacesListTables,
    // Neptune Analytics
    NeptuneAnalyticsListGraphs,
    NeptuneAnalyticsGetGraph,
    NeptuneAnalyticsCreateGraph,
    NeptuneAnalyticsDeleteGraph,
    NeptuneAnalyticsExecuteQuery,
    // Clean Rooms
    CleanRoomsListCollaborations,
    CleanRoomsGetCollaboration,
    CleanRoomsCreateCollaboration,
    CleanRoomsDeleteCollaboration,
    // DataZone
    DataZoneListDomains,
    DataZoneGetDomain,
    DataZoneCreateDomain,
    DataZoneDeleteDomain,
    // IVS
    IvsListChannels,
    IvsGetChannel,
    IvsCreateChannel,
    IvsDeleteChannel,
    IvsListStreams,
    // GameLift
    GameLiftListFleets,
    GameLiftDescribeFleet,
    GameLiftCreateFleet,
    GameLiftDeleteFleet,
    GameLiftDescribeGameSessions,
    // IoT Analytics
    IotAnalyticsListChannels,
    IotAnalyticsDescribeChannel,
    IotAnalyticsListDatasets,
    IotAnalyticsCreateChannel,
    // IoT Events
    IotEventsListDetectorModels,
    IotEventsDescribeDetectorModel,
    IotEventsCreateDetectorModel,
    IotEventsDeleteDetectorModel,
    // Kinesis Video Streams
    KinesisVideoListStreams,
    KinesisVideoDescribeStream,
    KinesisVideoCreateStream,
    KinesisVideoDeleteStream,
    // Managed Grafana
    ManagedGrafanaListWorkspaces,
    ManagedGrafanaDescribeWorkspace,
    ManagedGrafanaCreateWorkspace,
    ManagedGrafanaDeleteWorkspace,
    // AMP
    AmpListWorkspaces,
    AmpDescribeWorkspace,
    AmpCreateWorkspace,
    AmpDeleteWorkspace,
    // OpsWorks
    OpsWorksDescribeStacks,
    OpsWorksDescribeLayers,
    OpsWorksCreateStack,
    OpsWorksDeleteStack,
    // SWF
    SwfListDomains,
    SwfDescribeDomain,
    SwfRegisterDomain,
    SwfDeprecateDomain,
    // Elastic Transcoder
    ElasticTranscoderListPipelines,
    ElasticTranscoderCreatePipeline,
    ElasticTranscoderDeletePipeline,
    ElasticTranscoderCreateJob,
    ElasticTranscoderReadJob,
    // EFS
    EfsCreateFileSystem,
    EfsDescribeFileSystems,
    EfsDeleteFileSystem,
    EfsCreateMountTarget,
    EfsDescribeMountTargets,
    // AppRunner
    AppRunnerListServices,
    AppRunnerDescribeService,
    AppRunnerCreateService,
    AppRunnerDeleteService,
    AppRunnerPauseService,
    // Amplify
    AmplifyListApps,
    AmplifyGetApp,
    AmplifyCreateApp,
    AmplifyDeleteApp,
    AmplifyListBranches,
    // Snowball
    SnowballListJobs,
    SnowballDescribeJob,
    SnowballCreateJob,
    SnowballCancelJob,
    // CloudHSM v2
    CloudHsmV2ListClusters,
    CloudHsmV2DescribeCluster,
    CloudHsmV2CreateCluster,
    CloudHsmV2DeleteCluster,
    CloudHsmV2InitializeCluster,
    // Location
    LocationListMaps,
    LocationDescribeMap,
    LocationCreateMap,
    LocationDeleteMap,
    LocationSearchPlaceIndexForText,
    // Network Manager
    NetworkManagerListCoreNetworks,
    NetworkManagerGetCoreNetwork,
    NetworkManagerCreateCoreNetwork,
    NetworkManagerDeleteCoreNetwork,
    // AppFlow
    AppFlowListFlows,
    AppFlowDescribeFlow,
    AppFlowCreateFlow,
    AppFlowDeleteFlow,
    AppFlowStartFlow,
    // Redshift Serverless
    RedshiftServerlessListWorkgroups,
    RedshiftServerlessGetWorkgroup,
    RedshiftServerlessCreateWorkgroup,
    RedshiftServerlessDeleteWorkgroup,
    RedshiftServerlessListNamespaces,
    // HealthLake
    HealthLakeListFhirDatastores,
    HealthLakeDescribeFhirDatastore,
    HealthLakeCreateFhirDatastore,
    HealthLakeDeleteFhirDatastore,
    // Fraud Detector
    FraudDetectorListDetectors,
    FraudDetectorGetDetectors,
    FraudDetectorCreateDetector,
    FraudDetectorDeleteDetector,
    FraudDetectorGetEventTypes,
    // Lookout for Metrics
    LookoutMetricsListAnomalyDetectors,
    LookoutMetricsDescribeAnomalyDetector,
    LookoutMetricsCreateAnomalyDetector,
    LookoutMetricsDeleteAnomalyDetector,
    // Lookout for Vision
    LookoutVisionListProjects,
    LookoutVisionDescribeProject,
    LookoutVisionCreateProject,
    LookoutVisionDeleteProject,
    // Lookout for Equipment
    LookoutEquipmentListDatasets,
    LookoutEquipmentDescribeDataset,
    LookoutEquipmentCreateDataset,
    LookoutEquipmentDeleteDataset,
    // IoT SiteWise
    IotSiteWiseListAssets,
    IotSiteWiseDescribeAsset,
    IotSiteWiseCreateAsset,
    IotSiteWiseDeleteAsset,
    IotSiteWiseListAssetModels,
    // IoT Greengrass v2
    GreengrassV2ListCoreDevices,
    GreengrassV2GetCoreDevice,
    GreengrassV2ListComponents,
    GreengrassV2DeleteCoreDevice,
    // Panorama
    PanoramaListDevices,
    PanoramaDescribeDevice,
    PanoramaProvisionDevice,
    PanoramaDeleteDevice,
    // CodeGuru Reviewer
    CodeGuruListRepositoryAssociations,
    CodeGuruAssociateRepository,
    CodeGuruDisassociateRepository,
    CodeGuruListCodeReviews,
    // DevOps Guru
    DevOpsGuruListInsights,
    DevOpsGuruDescribeInsight,
    DevOpsGuruListRecommendations,
    DevOpsGuruListAnomaliesForInsight,
    // Proton
    ProtonListEnvironments,
    ProtonGetEnvironment,
    ProtonCreateEnvironment,
    ProtonDeleteEnvironment,
    // WorkMail
    WorkMailListOrganizations,
    WorkMailDescribeOrganization,
    WorkMailCreateOrganization,
    WorkMailDeleteOrganization,
    // WorkDocs
    WorkDocsDescribeRootFolders,
    WorkDocsDescribeFolderContents,
    WorkDocsGetDocument,
    WorkDocsInitiateDocumentVersionUpload,
    // Braket
    BraketGetDevice,
    BraketSearchDevices,
    BraketCreateQuantumTask,
    BraketGetQuantumTask,
    BraketCancelQuantumTask,
    // RoboMaker
    RoboMakerListSimulationJobs,
    RoboMakerDescribeSimulationJob,
    RoboMakerCreateSimulationJob,
    RoboMakerCancelSimulationJob,
    // Ground Station
    GroundStationListContacts,
    GroundStationListGroundStations,
    GroundStationReserveContact,
    GroundStationCancelContact,
    // Migration Hub
    MigrationHubListApplicationStates,
    MigrationHubDescribeApplicationState,
    MigrationHubListDiscoveredResources,
    MigrationHubListCreatedArtifacts,
    // Application Discovery
    ApplicationDiscoveryDescribeAgents,
    ApplicationDiscoveryGetDiscoverySummary,
    ApplicationDiscoveryListConfigurations,
    ApplicationDiscoveryDescribeApplications,
    // Elastic Disaster Recovery
    DrsDescribeJobs,
    DrsDescribeSourceServers,
    DrsDeleteSourceServer,
    DrsListStagingAccounts,
    // Resilience Hub
    ResilienceHubListApps,
    ResilienceHubDescribeApp,
    ResilienceHubCreateApp,
    ResilienceHubDeleteApp,
    // Managed Blockchain
    ManagedBlockchainListNetworks,
    ManagedBlockchainGetNetwork,
    ManagedBlockchainListMembers,
    ManagedBlockchainGetMember,
    // IAM Identity Center (SSO Admin)
    SsoAdminListInstances,
    SsoAdminListPermissionSets,
    SsoAdminDescribePermissionSet,
    SsoAdminCreatePermissionSet,
    SsoAdminListAccountAssignments,
    // CodeStar Connections
    CodeStarConnectionsListConnections,
    CodeStarConnectionsGetConnection,
    CodeStarConnectionsCreateConnection,
    CodeStarConnectionsDeleteConnection,
    // EMR Serverless
    EmrServerlessListApplications,
    EmrServerlessGetApplication,
    EmrServerlessCreateApplication,
    EmrServerlessDeleteApplication,
    EmrServerlessStartJobRun,
    // EventBridge Scheduler
    SchedulerListSchedules,
    SchedulerGetSchedule,
    SchedulerCreateSchedule,
    SchedulerDeleteSchedule,
    // Glue DataBrew
    DataBrewListProjects,
    DataBrewDescribeProject,
    DataBrewCreateProject,
    DataBrewDeleteProject,
    DataBrewListDatasets,
    // Security Lake
    SecurityLakeListDataLakes,
    SecurityLakeCreateDataLake,
    SecurityLakeDeleteDataLake,
    SecurityLakeListLogSources,
    SecurityLakeCreateSubscriber,
    // S3 Control
    S3ControlListBuckets,
    S3ControlListAccessPoints,
    S3ControlCreateAccessPoint,
    S3ControlDeleteAccessPoint,
    S3ControlGetAccessPoint,
    // Bedrock Agent
    BedrockAgentListAgents,
    BedrockAgentGetAgent,
    BedrockAgentCreateAgent,
    BedrockAgentDeleteAgent,
    BedrockAgentListAgentAliases,
    // CloudWatch Evidently
    EvidentlyListProjects,
    EvidentlyGetProject,
    EvidentlyCreateProject,
    EvidentlyDeleteProject,
    EvidentlyListFeatures,
    // CloudWatch RUM
    RumListAppMonitors,
    RumGetAppMonitor,
    RumCreateAppMonitor,
    RumDeleteAppMonitor,
    // CloudWatch Internet Monitor
    InternetMonitorListMonitors,
    InternetMonitorGetMonitor,
    InternetMonitorCreateMonitor,
    InternetMonitorDeleteMonitor,
    // Compute Optimizer
    ComputeOptimizerGetEc2InstanceRecommendations,
    ComputeOptimizerGetLambdaFunctionRecommendations,
    ComputeOptimizerGetAutoScalingGroupRecommendations,
    ComputeOptimizerGetRecommendationSummaries,
    // Systems Manager Incidents
    SsmIncidentsListIncidentRecords,
    SsmIncidentsGetIncidentRecord,
    SsmIncidentsCreateReplicationSet,
    SsmIncidentsDeleteIncidentRecord,
    // Resource Groups Tagging
    ResourceGroupsTaggingGetResources,
    ResourceGroupsTaggingTagResources,
    ResourceGroupsTaggingUntagResources,
    ResourceGroupsTaggingGetTagKeys,
    // IoT TwinMaker
    IotTwinMakerListWorkspaces,
    IotTwinMakerGetWorkspace,
    IotTwinMakerCreateWorkspace,
    IotTwinMakerDeleteWorkspace,
    // AWS Support
    SupportDescribeCases,
    SupportCreateCase,
    SupportResolveCase,
    SupportDescribeServices,
    // AWS Health
    HealthDescribeEvents,
    HealthDescribeEventDetails,
    HealthDescribeAffectedEntities,
    HealthDescribeAffectedAccountsForOrganization,
    // AWS Budgets
    BudgetsDescribeBudgets,
    BudgetsCreateBudget,
    BudgetsDeleteBudget,
    BudgetsDescribeBudgetPerformanceHistory,
    // License Manager
    LicenseManagerListLicenses,
    LicenseManagerGetLicense,
    LicenseManagerListReceivedLicenses,
    LicenseManagerListResourceInventory,
    // Savings Plans
    SavingsPlansDescribeSavingsPlans,
    SavingsPlansDescribeSavingsPlansOfferings,
    SavingsPlansCreateSavingsPlan,
    SavingsPlansListTagsForResource,
    // Resource Groups
    ResourceGroupsListGroups,
    ResourceGroupsGetGroup,
    ResourceGroupsCreateGroup,
    ResourceGroupsDeleteGroup,
    ResourceGroupsListGroupResources,
    // Resource Explorer
    ResourceExplorerSearch,
    ResourceExplorerListIndexes,
    ResourceExplorerCreateIndex,
    ResourceExplorerDeleteIndex,
    // FIS (Fault Injection Simulator)
    FisListExperimentTemplates,
    FisGetExperimentTemplate,
    FisCreateExperimentTemplate,
    FisDeleteExperimentTemplate,
    FisStartExperiment,
    // CloudWatch Synthetics
    SyntheticsDescribeCanaries,
    SyntheticsGetCanary,
    SyntheticsCreateCanary,
    SyntheticsDeleteCanary,
    SyntheticsStartCanary,
    // KMS
    KmsCreateKey,
    KmsDescribeKey,
    KmsListKeys,
    KmsEncrypt,
    KmsDecrypt,
    KmsGenerateDataKey,
    KmsScheduleKeyDeletion,
    KmsListAliases,
    KmsCreateAlias,
    // SES v2
    SesV2SendEmail,
    SesV2CreateEmailIdentity,
    SesV2DeleteEmailIdentity,
    SesV2ListEmailIdentities,
    SesV2GetEmailIdentity,
    SesV2GetAccount,
    SesV2ListContactLists,
    SesV2CreateContactList,
}

impl AwsApi {
    pub fn name() -> String {
        "AwsApi".to_string()
    }

    pub fn db_kind() -> String {
        "aws".to_string()
    }
}

impl Display for AwsApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Custom => f.write_str("custom"),
            Self::Query => f.write_str("query"),
            Self::JsonTarget => f.write_str("json_target"),
            // EC2
            Self::Ec2DescribeInstances => f.write_str("ec2_describe_instances"),
            Self::Ec2DescribeVpcs => f.write_str("ec2_describe_vpcs"),
            Self::Ec2DescribeSubnets => f.write_str("ec2_describe_subnets"),
            Self::Ec2DescribeSecurityGroups => f.write_str("ec2_describe_security_groups"),
            Self::Ec2DescribeImages => f.write_str("ec2_describe_images"),
            Self::Ec2DescribeKeyPairs => f.write_str("ec2_describe_key_pairs"),
            Self::Ec2DescribeVolumes => f.write_str("ec2_describe_volumes"),
            Self::Ec2DescribeRegions => f.write_str("ec2_describe_regions"),
            Self::Ec2DescribeAvailabilityZones => f.write_str("ec2_describe_availability_zones"),
            Self::Ec2StartInstances => f.write_str("ec2_start_instances"),
            Self::Ec2StopInstances => f.write_str("ec2_stop_instances"),
            Self::Ec2TerminateInstances => f.write_str("ec2_terminate_instances"),
            Self::Ec2RunInstances => f.write_str("ec2_run_instances"),
            Self::Ec2DescribeNetworkInterfaces => f.write_str("ec2_describe_network_interfaces"),
            Self::Ec2DescribeNatGateways => f.write_str("ec2_describe_nat_gateways"),
            Self::Ec2DescribeRouteTables => f.write_str("ec2_describe_route_tables"),
            Self::Ec2DescribeAddresses => f.write_str("ec2_describe_addresses"),
            Self::Ec2CreateVpc => f.write_str("ec2_create_vpc"),
            Self::Ec2DeleteVpc => f.write_str("ec2_delete_vpc"),
            Self::Ec2CreateSubnet => f.write_str("ec2_create_subnet"),
            Self::Ec2DeleteSubnet => f.write_str("ec2_delete_subnet"),
            Self::Ec2CreateSecurityGroup => f.write_str("ec2_create_security_group"),
            Self::Ec2DeleteSecurityGroup => f.write_str("ec2_delete_security_group"),
            Self::Ec2AuthorizeSecurityGroupIngress => f.write_str("ec2_authorize_security_group_ingress"),
            Self::Ec2RevokeSecurityGroupIngress => f.write_str("ec2_revoke_security_group_ingress"),
            Self::Ec2AllocateAddress => f.write_str("ec2_allocate_address"),
            Self::Ec2ReleaseAddress => f.write_str("ec2_release_address"),
            Self::Ec2AssociateAddress => f.write_str("ec2_associate_address"),
            Self::Ec2DisassociateAddress => f.write_str("ec2_disassociate_address"),
            Self::Ec2CreateKeyPair => f.write_str("ec2_create_key_pair"),
            Self::Ec2DeleteKeyPair => f.write_str("ec2_delete_key_pair"),
            Self::Ec2ImportKeyPair => f.write_str("ec2_import_key_pair"),
            Self::Ec2CreateNatGateway => f.write_str("ec2_create_nat_gateway"),
            Self::Ec2DeleteNatGateway => f.write_str("ec2_delete_nat_gateway"),
            Self::Ec2CreateInternetGateway => f.write_str("ec2_create_internet_gateway"),
            Self::Ec2AttachInternetGateway => f.write_str("ec2_attach_internet_gateway"),
            Self::Ec2DetachInternetGateway => f.write_str("ec2_detach_internet_gateway"),
            Self::Ec2CreateRoute => f.write_str("ec2_create_route"),
            Self::Ec2DeleteRoute => f.write_str("ec2_delete_route"),
            Self::Ec2CreateRouteTable => f.write_str("ec2_create_route_table"),
            Self::Ec2DeleteRouteTable => f.write_str("ec2_delete_route_table"),
            Self::Ec2CreateSnapshot => f.write_str("ec2_create_snapshot"),
            Self::Ec2DeleteSnapshot => f.write_str("ec2_delete_snapshot"),
            Self::Ec2DescribeSnapshots => f.write_str("ec2_describe_snapshots"),
            Self::Ec2CreateVolume => f.write_str("ec2_create_volume"),
            Self::Ec2DeleteVolume => f.write_str("ec2_delete_volume"),
            Self::Ec2AttachVolume => f.write_str("ec2_attach_volume"),
            Self::Ec2DetachVolume => f.write_str("ec2_detach_volume"),
            Self::Ec2ModifyInstanceAttribute => f.write_str("ec2_modify_instance_attribute"),
            Self::Ec2DescribeInstanceStatus => f.write_str("ec2_describe_instance_status"),
            Self::Ec2CreateTags => f.write_str("ec2_create_tags"),
            Self::Ec2DeleteTags => f.write_str("ec2_delete_tags"),
            Self::Ec2DescribeTags => f.write_str("ec2_describe_tags"),
            Self::Ec2RebootInstances => f.write_str("ec2_reboot_instances"),
            Self::Ec2DescribeInternetGateways => f.write_str("ec2_describe_internet_gateways"),
            Self::Ec2CreateVpcPeeringConnection => f.write_str("ec2_create_vpc_peering_connection"),
            Self::Ec2DescribeVpcPeeringConnections => f.write_str("ec2_describe_vpc_peering_connections"),
            Self::Ec2AcceptVpcPeeringConnection => f.write_str("ec2_accept_vpc_peering_connection"),
            Self::Ec2DeleteVpcPeeringConnection => f.write_str("ec2_delete_vpc_peering_connection"),
            Self::Ec2CreateLaunchTemplate => f.write_str("ec2_create_launch_template"),
            Self::Ec2DescribeLaunchTemplates => f.write_str("ec2_describe_launch_templates"),
            Self::Ec2DeleteLaunchTemplate => f.write_str("ec2_delete_launch_template"),
            Self::Ec2CreateFlowLogs => f.write_str("ec2_create_flow_logs"),
            Self::Ec2DescribeFlowLogs => f.write_str("ec2_describe_flow_logs"),
            Self::Ec2DeleteFlowLogs => f.write_str("ec2_delete_flow_logs"),
            Self::Ec2CreateVpcEndpoint => f.write_str("ec2_create_vpc_endpoint"),
            Self::Ec2DescribeVpcEndpoints => f.write_str("ec2_describe_vpc_endpoints"),
            Self::Ec2DeleteVpcEndpoints => f.write_str("ec2_delete_vpc_endpoints"),
            Self::Ec2AuthorizeSecurityGroupEgress => f.write_str("ec2_authorize_security_group_egress"),
            Self::Ec2RevokeSecurityGroupEgress => f.write_str("ec2_revoke_security_group_egress"),
            Self::Ec2DescribeInstanceTypes => f.write_str("ec2_describe_instance_types"),
            Self::Ec2DescribeNetworkAcls => f.write_str("ec2_describe_network_acls"),
            Self::Ec2ModifyVpcAttribute => f.write_str("ec2_modify_vpc_attribute"),
            Self::Ec2CopySnapshot => f.write_str("ec2_copy_snapshot"),
            Self::Ec2CopyImage => f.write_str("ec2_copy_image"),
            Self::Ec2DeregisterImage => f.write_str("ec2_deregister_image"),
            Self::Ec2ModifyVolume => f.write_str("ec2_modify_volume"),
            Self::Ec2GetConsoleOutput => f.write_str("ec2_get_console_output"),
            Self::Ec2DescribeTransitGateways => f.write_str("ec2_describe_transit_gateways"),
            Self::Ec2CreateTransitGateway => f.write_str("ec2_create_transit_gateway"),
            Self::Ec2DeleteTransitGateway => f.write_str("ec2_delete_transit_gateway"),
            Self::Ec2RequestSpotInstances => f.write_str("ec2_request_spot_instances"),
            Self::Ec2DescribeSpotInstanceRequests => f.write_str("ec2_describe_spot_instance_requests"),
            Self::Ec2CancelSpotInstanceRequests => f.write_str("ec2_cancel_spot_instance_requests"),
            Self::Ec2CreateVpnGateway => f.write_str("ec2_create_vpn_gateway"),
            Self::Ec2DescribeVpnGateways => f.write_str("ec2_describe_vpn_gateways"),
            Self::Ec2CreateVpnConnection => f.write_str("ec2_create_vpn_connection"),
            Self::Ec2DescribeVpnConnections => f.write_str("ec2_describe_vpn_connections"),
            Self::Ec2CreateNetworkInterface => f.write_str("ec2_create_network_interface"),
            Self::Ec2DeleteNetworkInterface => f.write_str("ec2_delete_network_interface"),
            Self::Ec2AttachNetworkInterface => f.write_str("ec2_attach_network_interface"),
            Self::Ec2DetachNetworkInterface => f.write_str("ec2_detach_network_interface"),
            Self::Ec2AssociateRouteTable => f.write_str("ec2_associate_route_table"),
            Self::Ec2DisassociateRouteTable => f.write_str("ec2_disassociate_route_table"),
            Self::Ec2CreateNetworkAcl => f.write_str("ec2_create_network_acl"),
            Self::Ec2DeleteNetworkAcl => f.write_str("ec2_delete_network_acl"),
            Self::Ec2CreateNetworkAclEntry => f.write_str("ec2_create_network_acl_entry"),
            Self::Ec2DeleteNetworkAclEntry => f.write_str("ec2_delete_network_acl_entry"),
            Self::Ec2CreatePlacementGroup => f.write_str("ec2_create_placement_group"),
            Self::Ec2DeletePlacementGroup => f.write_str("ec2_delete_placement_group"),
            Self::Ec2DescribePlacementGroups => f.write_str("ec2_describe_placement_groups"),
            Self::Ec2RegisterImage => f.write_str("ec2_register_image"),
            Self::Ec2ModifySubnetAttribute => f.write_str("ec2_modify_subnet_attribute"),
            Self::Ec2DeleteVpnGateway => f.write_str("ec2_delete_vpn_gateway"),
            Self::Ec2DeleteVpnConnection => f.write_str("ec2_delete_vpn_connection"),
            Self::Ec2AttachVpnGateway => f.write_str("ec2_attach_vpn_gateway"),
            Self::Ec2DetachVpnGateway => f.write_str("ec2_detach_vpn_gateway"),
            Self::Ec2CreateCustomerGateway => f.write_str("ec2_create_customer_gateway"),
            Self::Ec2DescribeCustomerGateways => f.write_str("ec2_describe_customer_gateways"),
            Self::Ec2DeleteCustomerGateway => f.write_str("ec2_delete_customer_gateway"),
            Self::Ec2DescribeReservedInstances => f.write_str("ec2_describe_reserved_instances"),
            Self::Ec2ModifyInstanceMetadataOptions => f.write_str("ec2_modify_instance_metadata_options"),
            Self::Ec2DescribeTransitGatewayAttachments => f.write_str("ec2_describe_transit_gateway_attachments"),
            // IAM
            Self::IamListUsers => f.write_str("iam_list_users"),
            Self::IamGetUser => f.write_str("iam_get_user"),
            Self::IamListRoles => f.write_str("iam_list_roles"),
            Self::IamGetRole => f.write_str("iam_get_role"),
            Self::IamListGroups => f.write_str("iam_list_groups"),
            Self::IamListPolicies => f.write_str("iam_list_policies"),
            Self::IamGetAccountSummary => f.write_str("iam_get_account_summary"),
            Self::IamListAccountAliases => f.write_str("iam_list_account_aliases"),
            Self::IamCreateRole => f.write_str("iam_create_role"),
            Self::IamDeleteRole => f.write_str("iam_delete_role"),
            Self::IamCreateUser => f.write_str("iam_create_user"),
            Self::IamDeleteUser => f.write_str("iam_delete_user"),
            Self::IamAttachRolePolicy => f.write_str("iam_attach_role_policy"),
            Self::IamDetachRolePolicy => f.write_str("iam_detach_role_policy"),
            Self::IamCreateAccessKey => f.write_str("iam_create_access_key"),
            Self::IamDeleteAccessKey => f.write_str("iam_delete_access_key"),
            Self::IamListAttachedRolePolicies => f.write_str("iam_list_attached_role_policies"),
            Self::IamPutRolePolicy => f.write_str("iam_put_role_policy"),
            Self::IamDeleteRolePolicy => f.write_str("iam_delete_role_policy"),
            Self::IamCreatePolicy => f.write_str("iam_create_policy"),
            Self::IamDeletePolicy => f.write_str("iam_delete_policy"),
            Self::IamGetPolicy => f.write_str("iam_get_policy"),
            Self::IamCreateGroup => f.write_str("iam_create_group"),
            Self::IamDeleteGroup => f.write_str("iam_delete_group"),
            Self::IamAddUserToGroup => f.write_str("iam_add_user_to_group"),
            Self::IamRemoveUserFromGroup => f.write_str("iam_remove_user_from_group"),
            Self::IamCreateInstanceProfile => f.write_str("iam_create_instance_profile"),
            Self::IamAddRoleToInstanceProfile => f.write_str("iam_add_role_to_instance_profile"),
            Self::IamListAccessKeys => f.write_str("iam_list_access_keys"),
            Self::IamUpdateAccessKey => f.write_str("iam_update_access_key"),
            Self::IamAttachUserPolicy => f.write_str("iam_attach_user_policy"),
            Self::IamDetachUserPolicy => f.write_str("iam_detach_user_policy"),
            Self::IamGetPolicyVersion => f.write_str("iam_get_policy_version"),
            Self::IamListPolicyVersions => f.write_str("iam_list_policy_versions"),
            Self::IamChangePassword => f.write_str("iam_change_password"),
            Self::IamUpdateLoginProfile => f.write_str("iam_update_login_profile"),
            Self::IamCreateLoginProfile => f.write_str("iam_create_login_profile"),
            Self::IamGetLoginProfile => f.write_str("iam_get_login_profile"),
            Self::IamDeleteLoginProfile => f.write_str("iam_delete_login_profile"),
            Self::IamCreateServiceLinkedRole => f.write_str("iam_create_service_linked_role"),
            Self::IamDeleteServiceLinkedRole => f.write_str("iam_delete_service_linked_role"),
            Self::IamListMfaDevices => f.write_str("iam_list_mfa_devices"),
            Self::IamGetRolePolicy => f.write_str("iam_get_role_policy"),
            Self::IamListRolePolicies => f.write_str("iam_list_role_policies"),
            Self::IamListUserPolicies => f.write_str("iam_list_user_policies"),
            Self::IamListGroupPolicies => f.write_str("iam_list_group_policies"),
            Self::IamListAttachedUserPolicies => f.write_str("iam_list_attached_user_policies"),
            Self::IamCreatePolicyVersion => f.write_str("iam_create_policy_version"),
            Self::IamDeletePolicyVersion => f.write_str("iam_delete_policy_version"),
            Self::IamSetDefaultPolicyVersion => f.write_str("iam_set_default_policy_version"),
            Self::IamListInstanceProfiles => f.write_str("iam_list_instance_profiles"),
            Self::IamListInstanceProfilesForRole => f.write_str("iam_list_instance_profiles_for_role"),
            Self::IamRemoveRoleFromInstanceProfile => f.write_str("iam_remove_role_from_instance_profile"),
            Self::IamDeleteInstanceProfile => f.write_str("iam_delete_instance_profile"),
            Self::IamGenerateCredentialReport => f.write_str("iam_generate_credential_report"),
            Self::IamGetCredentialReport => f.write_str("iam_get_credential_report"),
            Self::IamTagRole => f.write_str("iam_tag_role"),
            Self::IamUntagRole => f.write_str("iam_untag_role"),
            Self::IamTagUser => f.write_str("iam_tag_user"),
            Self::IamUntagUser => f.write_str("iam_untag_user"),
            Self::IamPutUserPolicy => f.write_str("iam_put_user_policy"),
            Self::IamGetUserPolicy => f.write_str("iam_get_user_policy"),
            Self::IamDeleteUserPolicy => f.write_str("iam_delete_user_policy"),
            Self::IamPutGroupPolicy => f.write_str("iam_put_group_policy"),
            Self::IamGetGroupPolicy => f.write_str("iam_get_group_policy"),
            Self::IamDeleteGroupPolicy => f.write_str("iam_delete_group_policy"),
            Self::IamGetAccountPasswordPolicy => f.write_str("iam_get_account_password_policy"),
            Self::IamUpdateAccountPasswordPolicy => f.write_str("iam_update_account_password_policy"),
            // STS
            Self::StsGetCallerIdentity => f.write_str("sts_get_caller_identity"),
            Self::StsAssumeRole => f.write_str("sts_assume_role"),
            Self::StsGetSessionToken => f.write_str("sts_get_session_token"),
            Self::StsGetAccessKeyInfo => f.write_str("sts_get_access_key_info"),
            Self::StsDecodeAuthorizationMessage => f.write_str("sts_decode_authorization_message"),
            Self::StsAssumeRoleWithSaml => f.write_str("sts_assume_role_with_saml"),
            Self::StsAssumeRoleWithWebIdentity => f.write_str("sts_assume_role_with_web_identity"),
            // Lambda
            Self::LambdaListFunctions => f.write_str("lambda_list_functions"),
            Self::LambdaGetFunction => f.write_str("lambda_get_function"),
            Self::LambdaInvokeFunction => f.write_str("lambda_invoke_function"),
            Self::LambdaCreateFunction => f.write_str("lambda_create_function"),
            Self::LambdaUpdateFunctionCode => f.write_str("lambda_update_function_code"),
            Self::LambdaDeleteFunction => f.write_str("lambda_delete_function"),
            Self::LambdaListEventSourceMappings => f.write_str("lambda_list_event_source_mappings"),
            Self::LambdaAddPermission => f.write_str("lambda_add_permission"),
            Self::LambdaRemovePermission => f.write_str("lambda_remove_permission"),
            Self::LambdaGetPolicy => f.write_str("lambda_get_policy"),
            Self::LambdaUpdateFunctionConfiguration => f.write_str("lambda_update_function_configuration"),
            Self::LambdaPublishVersion => f.write_str("lambda_publish_version"),
            Self::LambdaListAliases => f.write_str("lambda_list_aliases"),
            Self::LambdaCreateAlias => f.write_str("lambda_create_alias"),
            Self::LambdaListLayers => f.write_str("lambda_list_layers"),
            Self::LambdaGetLayerVersion => f.write_str("lambda_get_layer_version"),
            Self::LambdaListTags => f.write_str("lambda_list_tags"),
            Self::LambdaTagResource => f.write_str("lambda_tag_resource"),
            Self::LambdaDeleteAlias => f.write_str("lambda_delete_alias"),
            Self::LambdaUpdateAlias => f.write_str("lambda_update_alias"),
            Self::LambdaCreateEventSourceMapping => f.write_str("lambda_create_event_source_mapping"),
            Self::LambdaDeleteEventSourceMapping => f.write_str("lambda_delete_event_source_mapping"),
            Self::LambdaUpdateEventSourceMapping => f.write_str("lambda_update_event_source_mapping"),
            Self::LambdaGetEventSourceMapping => f.write_str("lambda_get_event_source_mapping"),
            Self::LambdaGetFunctionConfiguration => f.write_str("lambda_get_function_configuration"),
            Self::LambdaPutFunctionConcurrency => f.write_str("lambda_put_function_concurrency"),
            Self::LambdaDeleteFunctionConcurrency => f.write_str("lambda_delete_function_concurrency"),
            Self::LambdaDeleteLayerVersion => f.write_str("lambda_delete_layer_version"),
            Self::LambdaGetAccountSettings => f.write_str("lambda_get_account_settings"),
            Self::LambdaListVersionsByFunction => f.write_str("lambda_list_versions_by_function"),
            Self::LambdaCreateFunctionUrlConfig => f.write_str("lambda_create_function_url_config"),
            Self::LambdaGetFunctionUrlConfig => f.write_str("lambda_get_function_url_config"),
            Self::LambdaUpdateFunctionUrlConfig => f.write_str("lambda_update_function_url_config"),
            Self::LambdaDeleteFunctionUrlConfig => f.write_str("lambda_delete_function_url_config"),
            Self::LambdaUntagResource => f.write_str("lambda_untag_resource"),
            // S3
            Self::S3ListBuckets => f.write_str("s3_list_buckets"),
            Self::S3ListObjects => f.write_str("s3_list_objects"),
            Self::S3GetObject => f.write_str("s3_get_object"),
            Self::S3PutObject => f.write_str("s3_put_object"),
            Self::S3DeleteObject => f.write_str("s3_delete_object"),
            Self::S3CreateBucket => f.write_str("s3_create_bucket"),
            Self::S3DeleteBucket => f.write_str("s3_delete_bucket"),
            Self::S3HeadObject => f.write_str("s3_head_object"),
            Self::S3CopyObject => f.write_str("s3_copy_object"),
            Self::S3ListObjectVersions => f.write_str("s3_list_object_versions"),
            Self::S3GetBucketPolicy => f.write_str("s3_get_bucket_policy"),
            Self::S3PutBucketPolicy => f.write_str("s3_put_bucket_policy"),
            Self::S3GetBucketVersioning => f.write_str("s3_get_bucket_versioning"),
            Self::S3PutBucketVersioning => f.write_str("s3_put_bucket_versioning"),
            Self::S3GetBucketEncryption => f.write_str("s3_get_bucket_encryption"),
            Self::S3PutBucketEncryption => f.write_str("s3_put_bucket_encryption"),
            Self::S3GetBucketTagging => f.write_str("s3_get_bucket_tagging"),
            Self::S3PutBucketTagging => f.write_str("s3_put_bucket_tagging"),
            Self::S3GetBucketLocation => f.write_str("s3_get_bucket_location"),
            Self::S3PutBucketLifecycleConfiguration => f.write_str("s3_put_bucket_lifecycle_configuration"),
            Self::S3GetObjectTagging => f.write_str("s3_get_object_tagging"),
            Self::S3PutObjectTagging => f.write_str("s3_put_object_tagging"),
            Self::S3ListMultipartUploads => f.write_str("s3_list_multipart_uploads"),
            Self::S3AbortMultipartUpload => f.write_str("s3_abort_multipart_upload"),
            Self::S3CompleteMultipartUpload => f.write_str("s3_complete_multipart_upload"),
            Self::S3CreateMultipartUpload => f.write_str("s3_create_multipart_upload"),
            Self::S3DeleteObjects => f.write_str("s3_delete_objects"),
            Self::S3GetBucketAcl => f.write_str("s3_get_bucket_acl"),
            Self::S3PutBucketAcl => f.write_str("s3_put_bucket_acl"),
            Self::S3GetBucketCors => f.write_str("s3_get_bucket_cors"),
            Self::S3PutBucketCors => f.write_str("s3_put_bucket_cors"),
            Self::S3GetBucketLogging => f.write_str("s3_get_bucket_logging"),
            Self::S3PutBucketLogging => f.write_str("s3_put_bucket_logging"),
            Self::S3GetBucketNotificationConfiguration => f.write_str("s3_get_bucket_notification_configuration"),
            Self::S3PutBucketNotificationConfiguration => f.write_str("s3_put_bucket_notification_configuration"),
            Self::S3RestoreObject => f.write_str("s3_restore_object"),
            Self::S3UploadPart => f.write_str("s3_upload_part"),
            Self::S3DeleteBucketPolicy => f.write_str("s3_delete_bucket_policy"),
            Self::S3DeleteBucketEncryption => f.write_str("s3_delete_bucket_encryption"),
            Self::S3DeleteBucketLifecycleConfiguration => f.write_str("s3_delete_bucket_lifecycle_configuration"),
            Self::S3DeleteBucketTagging => f.write_str("s3_delete_bucket_tagging"),
            Self::S3DeleteBucketCors => f.write_str("s3_delete_bucket_cors"),
            Self::S3HeadBucket => f.write_str("s3_head_bucket"),
            Self::S3ListParts => f.write_str("s3_list_parts"),
            Self::S3SelectObjectContent => f.write_str("s3_select_object_content"),
            Self::S3GetObjectLockConfiguration => f.write_str("s3_get_object_lock_configuration"),
            Self::S3PutBucketReplication => f.write_str("s3_put_bucket_replication"),
            // DynamoDB
            Self::DynamoDbListTables => f.write_str("dynamodb_list_tables"),
            Self::DynamoDbDescribeTable => f.write_str("dynamodb_describe_table"),
            Self::DynamoDbGetItem => f.write_str("dynamodb_get_item"),
            Self::DynamoDbPutItem => f.write_str("dynamodb_put_item"),
            Self::DynamoDbDeleteItem => f.write_str("dynamodb_delete_item"),
            Self::DynamoDbQuery => f.write_str("dynamodb_query"),
            Self::DynamoDbScan => f.write_str("dynamodb_scan"),
            Self::DynamoDbCreateTable => f.write_str("dynamodb_create_table"),
            Self::DynamoDbDeleteTable => f.write_str("dynamodb_delete_table"),
            Self::DynamoDbUpdateItem => f.write_str("dynamodb_update_item"),
            Self::DynamoDbBatchGetItem => f.write_str("dynamodb_batch_get_item"),
            Self::DynamoDbBatchWriteItem => f.write_str("dynamodb_batch_write_item"),
            Self::DynamoDbUpdateTable => f.write_str("dynamodb_update_table"),
            Self::DynamoDbDescribeTimeToLive => f.write_str("dynamodb_describe_time_to_live"),
            Self::DynamoDbUpdateTimeToLive => f.write_str("dynamodb_update_time_to_live"),
            Self::DynamoDbTransactGetItems => f.write_str("dynamodb_transact_get_items"),
            Self::DynamoDbTransactWriteItems => f.write_str("dynamodb_transact_write_items"),
            Self::DynamoDbCreateBackup => f.write_str("dynamodb_create_backup"),
            Self::DynamoDbDeleteBackup => f.write_str("dynamodb_delete_backup"),
            Self::DynamoDbDescribeBackup => f.write_str("dynamodb_describe_backup"),
            Self::DynamoDbListBackups => f.write_str("dynamodb_list_backups"),
            Self::DynamoDbRestoreTableFromBackup => f.write_str("dynamodb_restore_table_from_backup"),
            Self::DynamoDbDescribeContinuousBackups => f.write_str("dynamodb_describe_continuous_backups"),
            Self::DynamoDbUpdateContinuousBackups => f.write_str("dynamodb_update_continuous_backups"),
            Self::DynamoDbRestoreTableToPointInTime => f.write_str("dynamodb_restore_table_to_point_in_time"),
            Self::DynamoDbListTagsOfResource => f.write_str("dynamodb_list_tags_of_resource"),
            Self::DynamoDbTagResource => f.write_str("dynamodb_tag_resource"),
            Self::DynamoDbUntagResource => f.write_str("dynamodb_untag_resource"),
            Self::DynamoDbCreateGlobalTable => f.write_str("dynamodb_create_global_table"),
            Self::DynamoDbDescribeGlobalTable => f.write_str("dynamodb_describe_global_table"),
            Self::DynamoDbListGlobalTables => f.write_str("dynamodb_list_global_tables"),
            Self::DynamoDbExportTableToPointInTime => f.write_str("dynamodb_export_table_to_point_in_time"),
            Self::DynamoDbDescribeEndpoints => f.write_str("dynamodb_describe_endpoints"),
            Self::DynamoDbDescribeLimits => f.write_str("dynamodb_describe_limits"),
            // CloudFormation
            Self::CfDescribeStacks => f.write_str("cloudformation_describe_stacks"),
            Self::CfListStacks => f.write_str("cloudformation_list_stacks"),
            Self::CfCreateStack => f.write_str("cloudformation_create_stack"),
            Self::CfDeleteStack => f.write_str("cloudformation_delete_stack"),
            Self::CfUpdateStack => f.write_str("cloudformation_update_stack"),
            Self::CfDescribeStackResources => f.write_str("cloudformation_describe_stack_resources"),
            Self::CfDescribeStackEvents => f.write_str("cloudformation_describe_stack_events"),
            Self::CfGetTemplate => f.write_str("cloudformation_get_template"),
            Self::CfValidateTemplate => f.write_str("cloudformation_validate_template"),
            Self::CfListStackResources => f.write_str("cloudformation_list_stack_resources"),
            Self::CfListExports => f.write_str("cloudformation_list_exports"),
            Self::CfCreateChangeSet => f.write_str("cloudformation_create_change_set"),
            Self::CfDeleteChangeSet => f.write_str("cloudformation_delete_change_set"),
            Self::CfDescribeChangeSet => f.write_str("cloudformation_describe_change_set"),
            Self::CfExecuteChangeSet => f.write_str("cloudformation_execute_change_set"),
            Self::CfListChangeSets => f.write_str("cloudformation_list_change_sets"),
            Self::CfGetTemplateSummary => f.write_str("cloudformation_get_template_summary"),
            Self::CfDetectStackDrift => f.write_str("cloudformation_detect_stack_drift"),
            Self::CfCancelUpdateStack => f.write_str("cloudformation_cancel_update_stack"),
            Self::CfDescribeStackDriftDetectionStatus => f.write_str("cloudformation_describe_stack_drift_detection_status"),
            Self::CfContinueUpdateRollback => f.write_str("cloudformation_continue_update_rollback"),
            // SQS
            Self::SqsListQueues => f.write_str("sqs_list_queues"),
            Self::SqsCreateQueue => f.write_str("sqs_create_queue"),
            Self::SqsSendMessage => f.write_str("sqs_send_message"),
            Self::SqsReceiveMessage => f.write_str("sqs_receive_message"),
            Self::SqsDeleteQueue => f.write_str("sqs_delete_queue"),
            Self::SqsGetQueueUrl => f.write_str("sqs_get_queue_url"),
            Self::SqsGetQueueAttributes => f.write_str("sqs_get_queue_attributes"),
            Self::SqsSetQueueAttributes => f.write_str("sqs_set_queue_attributes"),
            Self::SqsPurgeQueue => f.write_str("sqs_purge_queue"),
            Self::SqsDeleteMessage => f.write_str("sqs_delete_message"),
            Self::SqsChangeMessageVisibility => f.write_str("sqs_change_message_visibility"),
            Self::SqsTagQueue => f.write_str("sqs_tag_queue"),
            Self::SqsUntagQueue => f.write_str("sqs_untag_queue"),
            Self::SqsListQueueTags => f.write_str("sqs_list_queue_tags"),
            // SNS
            Self::SnsListTopics => f.write_str("sns_list_topics"),
            Self::SnsCreateTopic => f.write_str("sns_create_topic"),
            Self::SnsDeleteTopic => f.write_str("sns_delete_topic"),
            Self::SnsPublish => f.write_str("sns_publish"),
            Self::SnsSubscribe => f.write_str("sns_subscribe"),
            Self::SnsUnsubscribe => f.write_str("sns_unsubscribe"),
            Self::SnsListSubscriptions => f.write_str("sns_list_subscriptions"),
            Self::SnsSetSubscriptionAttributes => f.write_str("sns_set_subscription_attributes"),
            Self::SnsGetSubscriptionAttributes => f.write_str("sns_get_subscription_attributes"),
            Self::SnsSetTopicAttributes => f.write_str("sns_set_topic_attributes"),
            Self::SnsGetTopicAttributes => f.write_str("sns_get_topic_attributes"),
            Self::SnsListSubscriptionsByTopic => f.write_str("sns_list_subscriptions_by_topic"),
            Self::SnsTagResource => f.write_str("sns_tag_resource"),
            Self::SnsUntagResource => f.write_str("sns_untag_resource"),
            Self::SnsConfirmSubscription => f.write_str("sns_confirm_subscription"),
            Self::SnsListTagsForResource => f.write_str("sns_list_tags_for_resource"),
            // AutoScaling
            Self::AutoScalingDescribeAutoScalingGroups => f.write_str("autoscaling_describe_auto_scaling_groups"),
            Self::AutoScalingDescribeLaunchConfigurations => f.write_str("autoscaling_describe_launch_configurations"),
            Self::AutoScalingSetDesiredCapacity => f.write_str("autoscaling_set_desired_capacity"),
            Self::AutoScalingUpdateAutoScalingGroup => f.write_str("autoscaling_update_auto_scaling_group"),
            Self::AutoScalingDeleteAutoScalingGroup => f.write_str("autoscaling_delete_auto_scaling_group"),
            Self::AutoScalingCreateAutoScalingGroup => f.write_str("autoscaling_create_auto_scaling_group"),
            Self::AutoScalingCreateLaunchConfiguration => f.write_str("autoscaling_create_launch_configuration"),
            Self::AutoScalingDescribeScalingActivities => f.write_str("autoscaling_describe_scaling_activities"),
            Self::AutoScalingDescribePolicies => f.write_str("autoscaling_describe_policies"),
            Self::AutoScalingPutScalingPolicy => f.write_str("autoscaling_put_scaling_policy"),
            Self::AutoScalingExecutePolicy => f.write_str("autoscaling_execute_policy"),
            // RDS
            Self::RdsDescribeDbInstances => f.write_str("rds_describe_db_instances"),
            Self::RdsDescribeDbClusters => f.write_str("rds_describe_db_clusters"),
            Self::RdsCreateDbInstance => f.write_str("rds_create_db_instance"),
            Self::RdsDeleteDbInstance => f.write_str("rds_delete_db_instance"),
            Self::RdsDescribeDbSnapshots => f.write_str("rds_describe_db_snapshots"),
            Self::RdsModifyDbInstance => f.write_str("rds_modify_db_instance"),
            Self::RdsRebootDbInstance => f.write_str("rds_reboot_db_instance"),
            Self::RdsCreateDbCluster => f.write_str("rds_create_db_cluster"),
            Self::RdsDeleteDbCluster => f.write_str("rds_delete_db_cluster"),
            Self::RdsModifyDbCluster => f.write_str("rds_modify_db_cluster"),
            Self::RdsCreateDbSnapshot => f.write_str("rds_create_db_snapshot"),
            Self::RdsRestoreDbInstanceFromDbSnapshot => f.write_str("rds_restore_db_instance_from_db_snapshot"),
            Self::RdsDescribeDbSubnetGroups => f.write_str("rds_describe_db_subnet_groups"),
            Self::RdsDescribeEvents => f.write_str("rds_describe_events"),
            Self::RdsStartDbInstance => f.write_str("rds_start_db_instance"),
            Self::RdsStopDbInstance => f.write_str("rds_stop_db_instance"),
            Self::RdsCreateDbSubnetGroup => f.write_str("rds_create_db_subnet_group"),
            Self::RdsDeleteDbSubnetGroup => f.write_str("rds_delete_db_subnet_group"),
            Self::RdsModifyDbSubnetGroup => f.write_str("rds_modify_db_subnet_group"),
            Self::RdsCreateDbClusterSnapshot => f.write_str("rds_create_db_cluster_snapshot"),
            Self::RdsDeleteDbSnapshot => f.write_str("rds_delete_db_snapshot"),
            Self::RdsDescribeDbClusterSnapshots => f.write_str("rds_describe_db_cluster_snapshots"),
            Self::RdsDescribeDbEngineVersions => f.write_str("rds_describe_db_engine_versions"),
            Self::RdsCreateDbInstanceReadReplica => f.write_str("rds_create_db_instance_read_replica"),
            Self::RdsFailoverDbCluster => f.write_str("rds_failover_db_cluster"),
            Self::RdsCopyDbSnapshot => f.write_str("rds_copy_db_snapshot"),
            Self::RdsAddTagsToResource => f.write_str("rds_add_tags_to_resource"),
            Self::RdsRemoveTagsFromResource => f.write_str("rds_remove_tags_from_resource"),
            Self::RdsPromoteReadReplica => f.write_str("rds_promote_read_replica"),
            Self::RdsListTagsForResource => f.write_str("rds_list_tags_for_resource"),
            Self::RdsDescribeOrderableDbInstanceOptions => f.write_str("rds_describe_orderable_db_instance_options"),
            // ElastiCache
            Self::ElastiCacheDescribeCacheClusters => f.write_str("elasticache_describe_cache_clusters"),
            Self::ElastiCacheDescribeCacheSubnetGroups => f.write_str("elasticache_describe_cache_subnet_groups"),
            Self::ElastiCacheDescribeReplicationGroups => f.write_str("elasticache_describe_replication_groups"),
            Self::ElastiCacheCreateCacheCluster => f.write_str("elasticache_create_cache_cluster"),
            Self::ElastiCacheDeleteCacheCluster => f.write_str("elasticache_delete_cache_cluster"),
            Self::ElastiCacheModifyCacheCluster => f.write_str("elasticache_modify_cache_cluster"),
            Self::ElastiCacheCreateReplicationGroup => f.write_str("elasticache_create_replication_group"),
            Self::ElastiCacheDeleteReplicationGroup => f.write_str("elasticache_delete_replication_group"),
            Self::ElastiCacheCreateCacheSubnetGroup => f.write_str("elasticache_create_cache_subnet_group"),
            Self::ElastiCacheDeleteCacheSubnetGroup => f.write_str("elasticache_delete_cache_subnet_group"),
            Self::ElastiCacheModifyReplicationGroup => f.write_str("elasticache_modify_replication_group"),
            Self::ElastiCacheCreateSnapshot => f.write_str("elasticache_create_snapshot"),
            Self::ElastiCacheDeleteSnapshot => f.write_str("elasticache_delete_snapshot"),
            Self::ElastiCacheDescribeSnapshots => f.write_str("elasticache_describe_snapshots"),
            Self::ElastiCacheAddTagsToResource => f.write_str("elasticache_add_tags_to_resource"),
            Self::ElastiCacheRemoveTagsFromResource => f.write_str("elasticache_remove_tags_from_resource"),
            Self::ElastiCacheListTagsForResource => f.write_str("elasticache_list_tags_for_resource"),
            // Redshift
            Self::RedshiftDescribeClusters => f.write_str("redshift_describe_clusters"),
            Self::RedshiftDescribeClusterSubnetGroups => f.write_str("redshift_describe_cluster_subnet_groups"),
            Self::RedshiftCreateCluster => f.write_str("redshift_create_cluster"),
            Self::RedshiftDeleteCluster => f.write_str("redshift_delete_cluster"),
            Self::RedshiftModifyCluster => f.write_str("redshift_modify_cluster"),
            Self::RedshiftResizeCluster => f.write_str("redshift_resize_cluster"),
            Self::RedshiftCreateClusterSubnetGroup => f.write_str("redshift_create_cluster_subnet_group"),
            Self::RedshiftDescribeClusterSnapshots => f.write_str("redshift_describe_cluster_snapshots"),
            Self::RedshiftPauseCluster => f.write_str("redshift_pause_cluster"),
            Self::RedshiftResumeCluster => f.write_str("redshift_resume_cluster"),
            Self::RedshiftDescribeClusterParameterGroups => f.write_str("redshift_describe_cluster_parameter_groups"),
            Self::RedshiftCreateClusterParameterGroup => f.write_str("redshift_create_cluster_parameter_group"),
            Self::RedshiftEnableLogging => f.write_str("redshift_enable_logging"),
            Self::RedshiftDisableLogging => f.write_str("redshift_disable_logging"),
            Self::RedshiftDescribeLoggingStatus => f.write_str("redshift_describe_logging_status"),
            Self::RedshiftCreateTags => f.write_str("redshift_create_tags"),
            Self::RedshiftDeleteTags => f.write_str("redshift_delete_tags"),
            // CloudWatch
            Self::CloudWatchListMetrics => f.write_str("cloudwatch_list_metrics"),
            Self::CloudWatchGetMetricStatistics => f.write_str("cloudwatch_get_metric_statistics"),
            Self::CloudWatchDescribeAlarms => f.write_str("cloudwatch_describe_alarms"),
            Self::CloudWatchPutMetricAlarm => f.write_str("cloudwatch_put_metric_alarm"),
            Self::CloudWatchDeleteAlarms => f.write_str("cloudwatch_delete_alarms"),
            Self::CloudWatchPutMetricData => f.write_str("cloudwatch_put_metric_data"),
            Self::CloudWatchGetMetricData => f.write_str("cloudwatch_get_metric_data"),
            Self::CloudWatchDescribeAlarmHistory => f.write_str("cloudwatch_describe_alarm_history"),
            Self::CloudWatchSetAlarmState => f.write_str("cloudwatch_set_alarm_state"),
            Self::CloudWatchGetDashboard => f.write_str("cloudwatch_get_dashboard"),
            Self::CloudWatchListDashboards => f.write_str("cloudwatch_list_dashboards"),
            Self::CloudWatchPutDashboard => f.write_str("cloudwatch_put_dashboard"),
            Self::CloudWatchEnableAlarmActions => f.write_str("cloudwatch_enable_alarm_actions"),
            Self::CloudWatchDisableAlarmActions => f.write_str("cloudwatch_disable_alarm_actions"),
            Self::CloudWatchTagResource => f.write_str("cloudwatch_tag_resource"),
            Self::CloudWatchUntagResource => f.write_str("cloudwatch_untag_resource"),
            Self::CloudWatchListTagsForResource => f.write_str("cloudwatch_list_tags_for_resource"),
            Self::CloudWatchDescribeAnomalyDetectors => f.write_str("cloudwatch_describe_anomaly_detectors"),
            // ELBv2
            Self::ElbV2DescribeLoadBalancers => f.write_str("elbv2_describe_load_balancers"),
            Self::ElbV2DescribeListeners => f.write_str("elbv2_describe_listeners"),
            Self::ElbV2DescribeTargetGroups => f.write_str("elbv2_describe_target_groups"),
            Self::ElbV2DescribeTargetHealth => f.write_str("elbv2_describe_target_health"),
            Self::ElbV2CreateLoadBalancer => f.write_str("elbv2_create_load_balancer"),
            Self::ElbV2DeleteLoadBalancer => f.write_str("elbv2_delete_load_balancer"),
            Self::ElbV2CreateTargetGroup => f.write_str("elbv2_create_target_group"),
            Self::ElbV2DeleteTargetGroup => f.write_str("elbv2_delete_target_group"),
            Self::ElbV2RegisterTargets => f.write_str("elbv2_register_targets"),
            Self::ElbV2DeregisterTargets => f.write_str("elbv2_deregister_targets"),
            Self::ElbV2CreateListener => f.write_str("elbv2_create_listener"),
            Self::ElbV2DeleteListener => f.write_str("elbv2_delete_listener"),
            Self::ElbV2ModifyListener => f.write_str("elbv2_modify_listener"),
            Self::ElbV2CreateRule => f.write_str("elbv2_create_rule"),
            Self::ElbV2DeleteRule => f.write_str("elbv2_delete_rule"),
            Self::ElbV2ModifyLoadBalancerAttributes => f.write_str("elbv2_modify_load_balancer_attributes"),
            Self::ElbV2DescribeLoadBalancerAttributes => f.write_str("elbv2_describe_load_balancer_attributes"),
            Self::ElbV2ModifyTargetGroup => f.write_str("elbv2_modify_target_group"),
            Self::ElbV2DescribeTargetGroupAttributes => f.write_str("elbv2_describe_target_group_attributes"),
            Self::ElbV2AddTags => f.write_str("elbv2_add_tags"),
            Self::ElbV2RemoveTags => f.write_str("elbv2_remove_tags"),
            Self::ElbV2DescribeTags => f.write_str("elbv2_describe_tags"),
            Self::ElbV2SetSecurityGroups => f.write_str("elbv2_set_security_groups"),
            Self::ElbV2SetSubnets => f.write_str("elbv2_set_subnets"),
            Self::ElbV2ModifyRule => f.write_str("elbv2_modify_rule"),
            // EMR
            Self::EmrListClusters => f.write_str("emr_list_clusters"),
            Self::EmrDescribeCluster => f.write_str("emr_describe_cluster"),
            Self::EmrRunJobFlow => f.write_str("emr_run_job_flow"),
            Self::EmrTerminateJobFlows => f.write_str("emr_terminate_job_flows"),
            // Kinesis
            Self::KinesisListStreams => f.write_str("kinesis_list_streams"),
            Self::KinesisDescribeStream => f.write_str("kinesis_describe_stream"),
            Self::KinesisCreateStream => f.write_str("kinesis_create_stream"),
            Self::KinesisDeleteStream => f.write_str("kinesis_delete_stream"),
            Self::KinesisPutRecord => f.write_str("kinesis_put_record"),
            Self::KinesisPutRecords => f.write_str("kinesis_put_records"),
            Self::KinesisGetShardIterator => f.write_str("kinesis_get_shard_iterator"),
            Self::KinesisGetRecords => f.write_str("kinesis_get_records"),
            // Firehose
            Self::FirehoseListDeliveryStreams => f.write_str("firehose_list_delivery_streams"),
            Self::FirehoseDescribeDeliveryStream => f.write_str("firehose_describe_delivery_stream"),
            Self::FirehosePutRecord => f.write_str("firehose_put_record"),
            Self::FirehosePutRecordBatch => f.write_str("firehose_put_record_batch"),
            // CloudWatch Logs
            Self::CloudWatchLogsDescribeLogGroups => f.write_str("cloudwatchlogs_describe_log_groups"),
            Self::CloudWatchLogsDescribeLogStreams => f.write_str("cloudwatchlogs_describe_log_streams"),
            Self::CloudWatchLogsGetLogEvents => f.write_str("cloudwatchlogs_get_log_events"),
            Self::CloudWatchLogsFilterLogEvents => f.write_str("cloudwatchlogs_filter_log_events"),
            Self::CloudWatchLogsCreateLogGroup => f.write_str("cloudwatchlogs_create_log_group"),
            Self::CloudWatchLogsDeleteLogGroup => f.write_str("cloudwatchlogs_delete_log_group"),
            Self::CloudWatchLogsPutLogEvents => f.write_str("cloudwatchlogs_put_log_events"),
            Self::CloudWatchLogsPutRetentionPolicy => f.write_str("cloudwatchlogs_put_retention_policy"),
            Self::CloudWatchLogsDeleteRetentionPolicy => f.write_str("cloudwatchlogs_delete_retention_policy"),
            Self::CloudWatchLogsPutSubscriptionFilter => f.write_str("cloudwatchlogs_put_subscription_filter"),
            Self::CloudWatchLogsDescribeSubscriptionFilters => f.write_str("cloudwatchlogs_describe_subscription_filters"),
            Self::CloudWatchLogsDeleteSubscriptionFilter => f.write_str("cloudwatchlogs_delete_subscription_filter"),
            Self::CloudWatchLogsCreateLogStream => f.write_str("cloudwatchlogs_create_log_stream"),
            // Step Functions
            Self::SfnListStateMachines => f.write_str("sfn_list_state_machines"),
            Self::SfnDescribeStateMachine => f.write_str("sfn_describe_state_machine"),
            Self::SfnStartExecution => f.write_str("sfn_start_execution"),
            Self::SfnStopExecution => f.write_str("sfn_stop_execution"),
            Self::SfnListExecutions => f.write_str("sfn_list_executions"),
            Self::SfnDescribeExecution => f.write_str("sfn_describe_execution"),
            // CodePipeline
            Self::CodePipelineListPipelines => f.write_str("codepipeline_list_pipelines"),
            Self::CodePipelineGetPipeline => f.write_str("codepipeline_get_pipeline"),
            Self::CodePipelineStartPipelineExecution => f.write_str("codepipeline_start_pipeline_execution"),
            Self::CodePipelineGetPipelineExecution => f.write_str("codepipeline_get_pipeline_execution"),
            // CodeDeploy
            Self::CodeDeployListApplications => f.write_str("codedeploy_list_applications"),
            Self::CodeDeployListDeployments => f.write_str("codedeploy_list_deployments"),
            Self::CodeDeployGetDeployment => f.write_str("codedeploy_get_deployment"),
            Self::CodeDeployCreateDeployment => f.write_str("codedeploy_create_deployment"),
            // WAF
            Self::WafListWebAcls => f.write_str("waf_list_web_acls"),
            Self::WafGetWebAcl => f.write_str("waf_get_web_acl"),
            // CodeBuild
            Self::CodeBuildListProjects => f.write_str("codebuild_list_projects"),
            Self::CodeBuildBatchGetProjects => f.write_str("codebuild_batch_get_projects"),
            Self::CodeBuildListBuildsForProject => f.write_str("codebuild_list_builds_for_project"),
            Self::CodeBuildStartBuild => f.write_str("codebuild_start_build"),
            // Secrets Manager
            Self::SecretsManagerGetSecretValue => f.write_str("secretsmanager_get_secret_value"),
            Self::SecretsManagerListSecrets => f.write_str("secretsmanager_list_secrets"),
            Self::SecretsManagerCreateSecret => f.write_str("secretsmanager_create_secret"),
            Self::SecretsManagerDeleteSecret => f.write_str("secretsmanager_delete_secret"),
            Self::SecretsManagerUpdateSecret => f.write_str("secretsmanager_update_secret"),
            Self::SecretsManagerPutSecretValue => f.write_str("secretsmanager_put_secret_value"),
            Self::SecretsManagerRotateSecret => f.write_str("secretsmanager_rotate_secret"),
            Self::SecretsManagerDescribeSecret => f.write_str("secretsmanager_describe_secret"),
            Self::SecretsManagerRestoreSecret => f.write_str("secretsmanager_restore_secret"),
            Self::SecretsManagerGetRandomPassword => f.write_str("secretsmanager_get_random_password"),
            Self::SecretsManagerListSecretVersionIds => f.write_str("secretsmanager_list_secret_version_ids"),
            Self::SecretsManagerTagResource => f.write_str("secretsmanager_tag_resource"),
            Self::SecretsManagerUntagResource => f.write_str("secretsmanager_untag_resource"),
            // ECS
            Self::EcsListClusters => f.write_str("ecs_list_clusters"),
            Self::EcsDescribeClusters => f.write_str("ecs_describe_clusters"),
            Self::EcsListServices => f.write_str("ecs_list_services"),
            Self::EcsDescribeServices => f.write_str("ecs_describe_services"),
            Self::EcsListTasks => f.write_str("ecs_list_tasks"),
            Self::EcsDescribeTasks => f.write_str("ecs_describe_tasks"),
            Self::EcsRunTask => f.write_str("ecs_run_task"),
            Self::EcsStopTask => f.write_str("ecs_stop_task"),
            Self::EcsCreateCluster => f.write_str("ecs_create_cluster"),
            Self::EcsDeleteCluster => f.write_str("ecs_delete_cluster"),
            Self::EcsUpdateService => f.write_str("ecs_update_service"),
            Self::EcsCreateService => f.write_str("ecs_create_service"),
            Self::EcsDeleteService => f.write_str("ecs_delete_service"),
            Self::EcsRegisterTaskDefinition => f.write_str("ecs_register_task_definition"),
            Self::EcsDeregisterTaskDefinition => f.write_str("ecs_deregister_task_definition"),
            Self::EcsDescribeTaskDefinition => f.write_str("ecs_describe_task_definition"),
            Self::EcsListTaskDefinitions => f.write_str("ecs_list_task_definitions"),
            Self::EcsExecuteCommand => f.write_str("ecs_execute_command"),
            Self::EcsUpdateCluster => f.write_str("ecs_update_cluster"),
            Self::EcsListContainerInstances => f.write_str("ecs_list_container_instances"),
            Self::EcsDescribeContainerInstances => f.write_str("ecs_describe_container_instances"),
            Self::EcsTagResource => f.write_str("ecs_tag_resource"),
            Self::EcsUntagResource => f.write_str("ecs_untag_resource"),
            Self::EcsListTagsForResource => f.write_str("ecs_list_tags_for_resource"),
            Self::EcsCreateCapacityProvider => f.write_str("ecs_create_capacity_provider"),
            // EKS
            Self::EksListClusters => f.write_str("eks_list_clusters"),
            Self::EksDescribeCluster => f.write_str("eks_describe_cluster"),
            Self::EksListNodegroups => f.write_str("eks_list_nodegroups"),
            Self::EksDescribeNodegroup => f.write_str("eks_describe_nodegroup"),
            Self::EksCreateCluster => f.write_str("eks_create_cluster"),
            Self::EksDeleteCluster => f.write_str("eks_delete_cluster"),
            Self::EksUpdateClusterConfig => f.write_str("eks_update_cluster_config"),
            Self::EksCreateNodegroup => f.write_str("eks_create_nodegroup"),
            Self::EksDeleteNodegroup => f.write_str("eks_delete_nodegroup"),
            Self::EksListAddons => f.write_str("eks_list_addons"),
            Self::EksDescribeAddon => f.write_str("eks_describe_addon"),
            Self::EksCreateAddon => f.write_str("eks_create_addon"),
            Self::EksDeleteAddon => f.write_str("eks_delete_addon"),
            Self::EksUpdateAddon => f.write_str("eks_update_addon"),
            Self::EksCreateFargateProfile => f.write_str("eks_create_fargate_profile"),
            Self::EksDeleteFargateProfile => f.write_str("eks_delete_fargate_profile"),
            Self::EksDescribeFargateProfile => f.write_str("eks_describe_fargate_profile"),
            Self::EksListFargateProfiles => f.write_str("eks_list_fargate_profiles"),
            Self::EksUpdateNodegroupConfig => f.write_str("eks_update_nodegroup_config"),
            Self::EksUpdateClusterVersion => f.write_str("eks_update_cluster_version"),
            Self::EksTagResource => f.write_str("eks_tag_resource"),
            Self::EksUntagResource => f.write_str("eks_untag_resource"),
            Self::EksListTagsForResource => f.write_str("eks_list_tags_for_resource"),
            // API Gateway
            Self::ApiGatewayGetRestApis => f.write_str("apigateway_get_rest_apis"),
            Self::ApiGatewayGetResources => f.write_str("apigateway_get_resources"),
            Self::ApiGatewayGetStages => f.write_str("apigateway_get_stages"),
            Self::ApiGatewayCreateRestApi => f.write_str("apigateway_create_rest_api"),
            Self::ApiGatewayDeleteRestApi => f.write_str("apigateway_delete_rest_api"),
            // Batch
            Self::BatchListJobs => f.write_str("batch_list_jobs"),
            Self::BatchDescribeJobs => f.write_str("batch_describe_jobs"),
            Self::BatchSubmitJob => f.write_str("batch_submit_job"),
            Self::BatchCancelJob => f.write_str("batch_cancel_job"),
            // CloudFront
            Self::CloudFrontListDistributions => f.write_str("cloudfront_list_distributions"),
            Self::CloudFrontGetDistribution => f.write_str("cloudfront_get_distribution"),
            Self::CloudFrontCreateDistribution => f.write_str("cloudfront_create_distribution"),
            Self::CloudFrontDeleteDistribution => f.write_str("cloudfront_delete_distribution"),
            Self::CloudFrontUpdateDistribution => f.write_str("cloudfront_update_distribution"),
            Self::CloudFrontGetDistributionConfig => f.write_str("cloudfront_get_distribution_config"),
            Self::CloudFrontCreateInvalidation => f.write_str("cloudfront_create_invalidation"),
            Self::CloudFrontListInvalidations => f.write_str("cloudfront_list_invalidations"),
            Self::CloudFrontGetInvalidation => f.write_str("cloudfront_get_invalidation"),
            Self::CloudFrontCreateOriginAccessControl => f.write_str("cloudfront_create_origin_access_control"),
            Self::CloudFrontGetOriginAccessControl => f.write_str("cloudfront_get_origin_access_control"),
            Self::CloudFrontListOriginAccessControls => f.write_str("cloudfront_list_origin_access_controls"),
            Self::CloudFrontDeleteOriginAccessControl => f.write_str("cloudfront_delete_origin_access_control"),
            Self::CloudFrontListCachePolicies => f.write_str("cloudfront_list_cache_policies"),
            Self::CloudFrontGetCachePolicy => f.write_str("cloudfront_get_cache_policy"),
            // Route 53
            Self::Route53ListHostedZones => f.write_str("route53_list_hosted_zones"),
            Self::Route53ListResourceRecordSets => f.write_str("route53_list_resource_record_sets"),
            Self::Route53ChangeResourceRecordSets => f.write_str("route53_change_resource_record_sets"),
            Self::Route53CreateHostedZone => f.write_str("route53_create_hosted_zone"),
            Self::Route53DeleteHostedZone => f.write_str("route53_delete_hosted_zone"),
            Self::Route53GetHostedZone => f.write_str("route53_get_hosted_zone"),
            Self::Route53ListHealthChecks => f.write_str("route53_list_health_checks"),
            Self::Route53CreateHealthCheck => f.write_str("route53_create_health_check"),
            Self::Route53DeleteHealthCheck => f.write_str("route53_delete_health_check"),
            Self::Route53GetHealthCheck => f.write_str("route53_get_health_check"),
            Self::Route53GetHostedZoneCount => f.write_str("route53_get_hosted_zone_count"),
            Self::Route53TestDnsAnswer => f.write_str("route53_test_dns_answer"),
            Self::Route53ListHostedZonesByName => f.write_str("route53_list_hosted_zones_by_name"),
            // ECR
            Self::EcrListRepositories => f.write_str("ecr_list_repositories"),
            Self::EcrDescribeRepositories => f.write_str("ecr_describe_repositories"),
            Self::EcrGetAuthorizationToken => f.write_str("ecr_get_authorization_token"),
            Self::EcrCreateRepository => f.write_str("ecr_create_repository"),
            Self::EcrDeleteRepository => f.write_str("ecr_delete_repository"),
            Self::EcrListImages => f.write_str("ecr_list_images"),
            Self::EcrDescribeImages => f.write_str("ecr_describe_images"),
            Self::EcrBatchGetImage => f.write_str("ecr_batch_get_image"),
            Self::EcrBatchDeleteImage => f.write_str("ecr_batch_delete_image"),
            Self::EcrPutImage => f.write_str("ecr_put_image"),
            Self::EcrGetLifecyclePolicy => f.write_str("ecr_get_lifecycle_policy"),
            Self::EcrPutLifecyclePolicy => f.write_str("ecr_put_lifecycle_policy"),
            Self::EcrGetRepositoryPolicy => f.write_str("ecr_get_repository_policy"),
            // SSM
            Self::SsmGetParameter => f.write_str("ssm_get_parameter"),
            Self::SsmGetParameters => f.write_str("ssm_get_parameters"),
            Self::SsmPutParameter => f.write_str("ssm_put_parameter"),
            Self::SsmDeleteParameter => f.write_str("ssm_delete_parameter"),
            Self::SsmDescribeParameters => f.write_str("ssm_describe_parameters"),
            Self::SsmGetParametersByPath => f.write_str("ssm_get_parameters_by_path"),
            Self::SsmSendCommand => f.write_str("ssm_send_command"),
            Self::SsmListCommands => f.write_str("ssm_list_commands"),
            Self::SsmGetCommandInvocation => f.write_str("ssm_get_command_invocation"),
            Self::SsmListAssociations => f.write_str("ssm_list_associations"),
            Self::SsmCreateAssociation => f.write_str("ssm_create_association"),
            Self::SsmDescribeInstanceInformation => f.write_str("ssm_describe_instance_information"),
            Self::SsmGetParameterHistory => f.write_str("ssm_get_parameter_history"),
            Self::SsmDeleteParameters => f.write_str("ssm_delete_parameters"),
            Self::SsmStartAutomationExecution => f.write_str("ssm_start_automation_execution"),
            Self::SsmDescribeAutomationExecutions => f.write_str("ssm_describe_automation_executions"),
            Self::SsmStartSession => f.write_str("ssm_start_session"),
            // EventBridge
            Self::EventBridgeListRules => f.write_str("eventbridge_list_rules"),
            Self::EventBridgePutRule => f.write_str("eventbridge_put_rule"),
            Self::EventBridgeDeleteRule => f.write_str("eventbridge_delete_rule"),
            Self::EventBridgePutEvents => f.write_str("eventbridge_put_events"),
            Self::EventBridgeListTargetsByRule => f.write_str("eventbridge_list_targets_by_rule"),
            Self::EventBridgePutTargets => f.write_str("eventbridge_put_targets"),
            Self::EventBridgeRemoveTargets => f.write_str("eventbridge_remove_targets"),
            // Cognito
            Self::CognitoListUserPools => f.write_str("cognito_list_user_pools"),
            Self::CognitoDescribeUserPool => f.write_str("cognito_describe_user_pool"),
            Self::CognitoListUsers => f.write_str("cognito_list_users"),
            Self::CognitoAdminCreateUser => f.write_str("cognito_admin_create_user"),
            Self::CognitoAdminDeleteUser => f.write_str("cognito_admin_delete_user"),
            Self::CognitoAdminGetUser => f.write_str("cognito_admin_get_user"),
            Self::CognitoInitiateAuth => f.write_str("cognito_initiate_auth"),
            Self::CognitoSignUp => f.write_str("cognito_sign_up"),
            Self::CognitoConfirmSignUp => f.write_str("cognito_confirm_sign_up"),
            Self::CognitoForgotPassword => f.write_str("cognito_forgot_password"),
            Self::CognitoConfirmForgotPassword => f.write_str("cognito_confirm_forgot_password"),
            Self::CognitoAdminSetUserPassword => f.write_str("cognito_admin_set_user_password"),
            Self::CognitoAdminDisableUser => f.write_str("cognito_admin_disable_user"),
            Self::CognitoAdminEnableUser => f.write_str("cognito_admin_enable_user"),
            Self::CognitoCreateUserPool => f.write_str("cognito_create_user_pool"),
            Self::CognitoDeleteUserPool => f.write_str("cognito_delete_user_pool"),
            Self::CognitoCreateUserPoolClient => f.write_str("cognito_create_user_pool_client"),
            Self::CognitoDescribeUserPoolClient => f.write_str("cognito_describe_user_pool_client"),
            Self::CognitoListUserPoolClients => f.write_str("cognito_list_user_pool_clients"),
            Self::CognitoDeleteUserPoolClient => f.write_str("cognito_delete_user_pool_client"),
            Self::CognitoCreateGroup => f.write_str("cognito_create_group"),
            Self::CognitoDeleteGroup => f.write_str("cognito_delete_group"),
            Self::CognitoListGroups => f.write_str("cognito_list_groups"),
            Self::CognitoAdminAddUserToGroup => f.write_str("cognito_admin_add_user_to_group"),
            Self::CognitoAdminRemoveUserFromGroup => f.write_str("cognito_admin_remove_user_from_group"),
            // SES
            Self::SesListIdentities => f.write_str("ses_list_identities"),
            Self::SesSendEmail => f.write_str("ses_send_email"),
            Self::SesVerifyEmailIdentity => f.write_str("ses_verify_email_identity"),
            Self::SesDeleteIdentity => f.write_str("ses_delete_identity"),
            Self::SesGetSendQuota => f.write_str("ses_get_send_quota"),
            // ACM
            Self::AcmListCertificates => f.write_str("acm_list_certificates"),
            Self::AcmDescribeCertificate => f.write_str("acm_describe_certificate"),
            Self::AcmRequestCertificate => f.write_str("acm_request_certificate"),
            Self::AcmDeleteCertificate => f.write_str("acm_delete_certificate"),
            Self::AcmGetCertificate => f.write_str("acm_get_certificate"),
            // CloudTrail
            Self::CloudTrailDescribeTrails => f.write_str("cloudtrail_describe_trails"),
            Self::CloudTrailGetTrail => f.write_str("cloudtrail_get_trail"),
            Self::CloudTrailGetTrailStatus => f.write_str("cloudtrail_get_trail_status"),
            Self::CloudTrailLookupEvents => f.write_str("cloudtrail_lookup_events"),
            Self::CloudTrailStartLogging => f.write_str("cloudtrail_start_logging"),
            Self::CloudTrailStopLogging => f.write_str("cloudtrail_stop_logging"),
            // API Gateway V2
            Self::ApiGatewayV2GetApis => f.write_str("apigatewayv2_get_apis"),
            Self::ApiGatewayV2GetApi => f.write_str("apigatewayv2_get_api"),
            Self::ApiGatewayV2CreateApi => f.write_str("apigatewayv2_create_api"),
            Self::ApiGatewayV2DeleteApi => f.write_str("apigatewayv2_delete_api"),
            Self::ApiGatewayV2GetStages => f.write_str("apigatewayv2_get_stages"),
            Self::ApiGatewayV2GetRoutes => f.write_str("apigatewayv2_get_routes"),
            Self::ApiGatewayV2GetIntegrations => f.write_str("apigatewayv2_get_integrations"),
            Self::ApiGatewayV2GetDeployments => f.write_str("apigatewayv2_get_deployments"),
            // Athena
            Self::AthenaStartQueryExecution => f.write_str("athena_start_query_execution"),
            Self::AthenaGetQueryExecution => f.write_str("athena_get_query_execution"),
            Self::AthenaGetQueryResults => f.write_str("athena_get_query_results"),
            Self::AthenaStopQueryExecution => f.write_str("athena_stop_query_execution"),
            // Glue
            Self::GlueGetDatabases => f.write_str("glue_get_databases"),
            Self::GlueGetTables => f.write_str("glue_get_tables"),
            Self::GlueStartJobRun => f.write_str("glue_start_job_run"),
            Self::GlueGetJobRun => f.write_str("glue_get_job_run"),
            Self::GlueListJobs => f.write_str("glue_list_jobs"),
            Self::GlueGetJob => f.write_str("glue_get_job"),
            Self::GlueCreateJob => f.write_str("glue_create_job"),
            Self::GlueDeleteJob => f.write_str("glue_delete_job"),
            Self::GlueGetCrawlers => f.write_str("glue_get_crawlers"),
            Self::GlueStartCrawler => f.write_str("glue_start_crawler"),
            Self::GlueCreateCrawler => f.write_str("glue_create_crawler"),
            Self::GlueGetPartitions => f.write_str("glue_get_partitions"),
            Self::GlueGetTable => f.write_str("glue_get_table"),
            // Organizations
            Self::OrganizationsListAccounts => f.write_str("organizations_list_accounts"),
            Self::OrganizationsDescribeAccount => f.write_str("organizations_describe_account"),
            Self::OrganizationsListRoots => f.write_str("organizations_list_roots"),
            Self::OrganizationsListOrganizationalUnitsForParent => f.write_str("organizations_list_organizational_units_for_parent"),
            // GuardDuty
            Self::GuardDutyListDetectors => f.write_str("guardduty_list_detectors"),
            Self::GuardDutyListFindings => f.write_str("guardduty_list_findings"),
            Self::GuardDutyGetFindings => f.write_str("guardduty_get_findings"),
            Self::GuardDutyCreateDetector => f.write_str("guardduty_create_detector"),
            Self::GuardDutyDeleteDetector => f.write_str("guardduty_delete_detector"),
            // X-Ray
            Self::XRayGetTraceSummaries => f.write_str("xray_get_trace_summaries"),
            Self::XRayGetTraceGraph => f.write_str("xray_get_trace_graph"),
            Self::XRayPutTraceSegments => f.write_str("xray_put_trace_segments"),
            // SageMaker
            Self::SageMakerCreateTrainingJob => f.write_str("sagemaker_create_training_job"),
            Self::SageMakerDescribeTrainingJob => f.write_str("sagemaker_describe_training_job"),
            Self::SageMakerListTrainingJobs => f.write_str("sagemaker_list_training_jobs"),
            Self::SageMakerCreateEndpoint => f.write_str("sagemaker_create_endpoint"),
            Self::SageMakerDescribeEndpoint => f.write_str("sagemaker_describe_endpoint"),
            Self::SageMakerDeleteEndpoint => f.write_str("sagemaker_delete_endpoint"),
            Self::SageMakerListEndpoints => f.write_str("sagemaker_list_endpoints"),
            Self::SageMakerCreateModel => f.write_str("sagemaker_create_model"),
            Self::SageMakerDeleteModel => f.write_str("sagemaker_delete_model"),
            Self::SageMakerDescribeModel => f.write_str("sagemaker_describe_model"),
            Self::SageMakerListModels => f.write_str("sagemaker_list_models"),
            Self::SageMakerCreateNotebookInstance => f.write_str("sagemaker_create_notebook_instance"),
            Self::SageMakerDeleteNotebookInstance => f.write_str("sagemaker_delete_notebook_instance"),
            Self::SageMakerDescribeNotebookInstance => f.write_str("sagemaker_describe_notebook_instance"),
            Self::SageMakerListNotebookInstances => f.write_str("sagemaker_list_notebook_instances"),
            Self::SageMakerCreateProcessingJob => f.write_str("sagemaker_create_processing_job"),
            Self::SageMakerDescribeProcessingJob => f.write_str("sagemaker_describe_processing_job"),
            Self::SageMakerListProcessingJobs => f.write_str("sagemaker_list_processing_jobs"),
            Self::SageMakerStopTrainingJob => f.write_str("sagemaker_stop_training_job"),
            Self::SageMakerCreateTransformJob => f.write_str("sagemaker_create_transform_job"),
            Self::SageMakerListTransformJobs => f.write_str("sagemaker_list_transform_jobs"),
            // Bedrock
            Self::BedrockListFoundationModels => f.write_str("bedrock_list_foundation_models"),
            Self::BedrockInvokeModel => f.write_str("bedrock_invoke_model"),
            Self::BedrockGetFoundationModel => f.write_str("bedrock_get_foundation_model"),
            Self::BedrockListCustomModels => f.write_str("bedrock_list_custom_models"),
            Self::BedrockCreateModelCustomizationJob => f.write_str("bedrock_create_model_customization_job"),
            Self::BedrockListGuardrails => f.write_str("bedrock_list_guardrails"),
            Self::BedrockCreateGuardrail => f.write_str("bedrock_create_guardrail"),
            // Config
            Self::ConfigListDiscoveredResources => f.write_str("config_list_discovered_resources"),
            Self::ConfigGetResourceConfigHistory => f.write_str("config_get_resource_config_history"),
            Self::ConfigDescribeConfigRules => f.write_str("config_describe_config_rules"),
            // Cost Explorer
            Self::CostExplorerGetCostAndUsage => f.write_str("cost_explorer_get_cost_and_usage"),
            Self::CostExplorerGetCostForecast => f.write_str("cost_explorer_get_cost_forecast"),
            // WAFv2
            Self::WafV2ListWebAcls => f.write_str("wafv2_list_web_acls"),
            Self::WafV2GetWebAcl => f.write_str("wafv2_get_web_acl"),
            Self::WafV2CreateWebAcl => f.write_str("wafv2_create_web_acl"),
            Self::WafV2DeleteWebAcl => f.write_str("wafv2_delete_web_acl"),
            // OpenSearch
            Self::OpenSearchListDomains => f.write_str("opensearch_list_domains"),
            Self::OpenSearchDescribeDomain => f.write_str("opensearch_describe_domain"),
            Self::OpenSearchCreateDomain => f.write_str("opensearch_create_domain"),
            Self::OpenSearchDeleteDomain => f.write_str("opensearch_delete_domain"),
            Self::OpenSearchDescribeDomainConfig => f.write_str("opensearch_describe_domain_config"),
            Self::OpenSearchUpdateDomainConfig => f.write_str("opensearch_update_domain_config"),
            Self::OpenSearchAddTags => f.write_str("opensearch_add_tags"),
            Self::OpenSearchRemoveTags => f.write_str("opensearch_remove_tags"),
            Self::OpenSearchListTags => f.write_str("opensearch_list_tags"),
            // MSK
            Self::MskListClusters => f.write_str("msk_list_clusters"),
            Self::MskDescribeCluster => f.write_str("msk_describe_cluster"),
            Self::MskCreateCluster => f.write_str("msk_create_cluster"),
            Self::MskDeleteCluster => f.write_str("msk_delete_cluster"),
            Self::MskListKafkaVersions => f.write_str("msk_list_kafka_versions"),
            // CodeCommit
            Self::CodeCommitListRepositories => f.write_str("codecommit_list_repositories"),
            Self::CodeCommitGetRepository => f.write_str("codecommit_get_repository"),
            Self::CodeCommitCreateRepository => f.write_str("codecommit_create_repository"),
            Self::CodeCommitDeleteRepository => f.write_str("codecommit_delete_repository"),
            Self::CodeCommitListBranches => f.write_str("codecommit_list_branches"),
            // Security Hub
            Self::SecurityHubGetFindings => f.write_str("securityhub_get_findings"),
            Self::SecurityHubBatchImportFindings => f.write_str("securityhub_batch_import_findings"),
            Self::SecurityHubEnableSecurityHub => f.write_str("securityhub_enable_security_hub"),
            Self::SecurityHubDisableSecurityHub => f.write_str("securityhub_disable_security_hub"),
            // Inspector v2
            Self::InspectorListFindings => f.write_str("inspector_list_findings"),
            Self::InspectorListCoverage => f.write_str("inspector_list_coverage"),
            Self::InspectorEnable => f.write_str("inspector_enable"),
            Self::InspectorDisable => f.write_str("inspector_disable"),
            // RAM
            Self::RamListResources => f.write_str("ram_list_resources"),
            Self::RamListResourceShares => f.write_str("ram_list_resource_shares"),
            Self::RamCreateResourceShare => f.write_str("ram_create_resource_share"),
            Self::RamDeleteResourceShare => f.write_str("ram_delete_resource_share"),
            // Comprehend
            Self::ComprehendDetectSentiment => f.write_str("comprehend_detect_sentiment"),
            Self::ComprehendDetectEntities => f.write_str("comprehend_detect_entities"),
            Self::ComprehendDetectDominantLanguage => f.write_str("comprehend_detect_dominant_language"),
            Self::ComprehendBatchDetectSentiment => f.write_str("comprehend_batch_detect_sentiment"),
            // Rekognition
            Self::RekognitionDetectLabels => f.write_str("rekognition_detect_labels"),
            Self::RekognitionDetectFaces => f.write_str("rekognition_detect_faces"),
            Self::RekognitionIndexFaces => f.write_str("rekognition_index_faces"),
            Self::RekognitionListCollections => f.write_str("rekognition_list_collections"),
            Self::RekognitionSearchFacesByImage => f.write_str("rekognition_search_faces_by_image"),
            // Transcribe
            Self::TranscribeStartTranscriptionJob => f.write_str("transcribe_start_transcription_job"),
            Self::TranscribeGetTranscriptionJob => f.write_str("transcribe_get_transcription_job"),
            Self::TranscribeListTranscriptionJobs => f.write_str("transcribe_list_transcription_jobs"),
            Self::TranscribeDeleteTranscriptionJob => f.write_str("transcribe_delete_transcription_job"),
            // Translate
            Self::TranslateTranslateText => f.write_str("translate_translate_text"),
            Self::TranslateListTextTranslationJobs => f.write_str("translate_list_text_translation_jobs"),
            Self::TranslateStartTextTranslationJob => f.write_str("translate_start_text_translation_job"),
            // Textract
            Self::TextractDetectDocumentText => f.write_str("textract_detect_document_text"),
            Self::TextractAnalyzeDocument => f.write_str("textract_analyze_document"),
            Self::TextractStartDocumentAnalysis => f.write_str("textract_start_document_analysis"),
            Self::TextractGetDocumentAnalysis => f.write_str("textract_get_document_analysis"),
            // Polly
            Self::PollyDescribeVoices => f.write_str("polly_describe_voices"),
            Self::PollySynthesizeSpeech => f.write_str("polly_synthesize_speech"),
            Self::PollyListLexicons => f.write_str("polly_list_lexicons"),
            // Service Quotas
            Self::ServiceQuotasListServices => f.write_str("servicequotas_list_services"),
            Self::ServiceQuotasListServiceQuotas => f.write_str("servicequotas_list_service_quotas"),
            Self::ServiceQuotasGetServiceQuota => f.write_str("servicequotas_get_service_quota"),
            Self::ServiceQuotasRequestServiceQuotaIncrease => f.write_str("servicequotas_request_service_quota_increase"),
            // Control Tower
            Self::ControlTowerListEnabledControls => f.write_str("controltower_list_enabled_controls"),
            Self::ControlTowerListLandingZones => f.write_str("controltower_list_landing_zones"),
            Self::ControlTowerGetLandingZone => f.write_str("controltower_get_landing_zone"),
            // Network Firewall
            Self::NetworkFirewallListFirewalls => f.write_str("networkfirewall_list_firewalls"),
            Self::NetworkFirewallDescribeFirewall => f.write_str("networkfirewall_describe_firewall"),
            Self::NetworkFirewallCreateFirewall => f.write_str("networkfirewall_create_firewall"),
            Self::NetworkFirewallDeleteFirewall => f.write_str("networkfirewall_delete_firewall"),
            // Global Accelerator
            Self::GlobalAcceleratorListAccelerators => f.write_str("globalaccelerator_list_accelerators"),
            Self::GlobalAcceleratorDescribeAccelerator => f.write_str("globalaccelerator_describe_accelerator"),
            Self::GlobalAcceleratorCreateAccelerator => f.write_str("globalaccelerator_create_accelerator"),
            Self::GlobalAcceleratorDeleteAccelerator => f.write_str("globalaccelerator_delete_accelerator"),
            // IoT
            Self::IotListThings => f.write_str("iot_list_things"),
            Self::IotDescribeThing => f.write_str("iot_describe_thing"),
            Self::IotCreateThing => f.write_str("iot_create_thing"),
            Self::IotDeleteThing => f.write_str("iot_delete_thing"),
            Self::IotListThingGroups => f.write_str("iot_list_thing_groups"),
            // MediaLive
            Self::MediaLiveListChannels => f.write_str("medialive_list_channels"),
            Self::MediaLiveDescribeChannel => f.write_str("medialive_describe_channel"),
            Self::MediaLiveCreateChannel => f.write_str("medialive_create_channel"),
            Self::MediaLiveDeleteChannel => f.write_str("medialive_delete_channel"),
            Self::MediaLiveStartChannel => f.write_str("medialive_start_channel"),
            Self::MediaLiveStopChannel => f.write_str("medialive_stop_channel"),
            // MediaConvert
            Self::MediaConvertListJobs => f.write_str("mediaconvert_list_jobs"),
            Self::MediaConvertGetJob => f.write_str("mediaconvert_get_job"),
            Self::MediaConvertCreateJob => f.write_str("mediaconvert_create_job"),
            Self::MediaConvertCancelJob => f.write_str("mediaconvert_cancel_job"),
            Self::MediaConvertListJobTemplates => f.write_str("mediaconvert_list_job_templates"),
            // WorkSpaces
            Self::WorkSpacesDescribeWorkspaces => f.write_str("workspaces_describe_workspaces"),
            Self::WorkSpacesDescribeWorkspaceDirectories => f.write_str("workspaces_describe_workspace_directories"),
            Self::WorkSpacesCreateWorkspaces => f.write_str("workspaces_create_workspaces"),
            Self::WorkSpacesTerminateWorkspaces => f.write_str("workspaces_terminate_workspaces"),
            // Directory Service
            Self::DsDescribeDirectories => f.write_str("ds_describe_directories"),
            Self::DsCreateDirectory => f.write_str("ds_create_directory"),
            Self::DsDeleteDirectory => f.write_str("ds_delete_directory"),
            Self::DsListTagsForResource => f.write_str("ds_list_tags_for_resource"),
            // Lex
            Self::LexListBots => f.write_str("lex_list_bots"),
            Self::LexDescribeBotVersion => f.write_str("lex_describe_bot_version"),
            Self::LexCreateBot => f.write_str("lex_create_bot"),
            Self::LexDeleteBot => f.write_str("lex_delete_bot"),
            // Personalize
            Self::PersonalizeListDatasets => f.write_str("personalize_list_datasets"),
            Self::PersonalizeDescribeDataset => f.write_str("personalize_describe_dataset"),
            Self::PersonalizeListCampaigns => f.write_str("personalize_list_campaigns"),
            Self::PersonalizeDescribeCampaign => f.write_str("personalize_describe_campaign"),
            // Forecast
            Self::ForecastListDatasets => f.write_str("forecast_list_datasets"),
            Self::ForecastDescribeDataset => f.write_str("forecast_describe_dataset"),
            Self::ForecastListPredictors => f.write_str("forecast_list_predictors"),
            Self::ForecastCreatePredictor => f.write_str("forecast_create_predictor"),
            // Macie
            Self::MacieListFindings => f.write_str("macie_list_findings"),
            Self::MacieGetFindings => f.write_str("macie_get_findings"),
            Self::MacieDescribeBuckets => f.write_str("macie_describe_buckets"),
            Self::MacieEnableMacie => f.write_str("macie_enable_macie"),
            Self::MacieDisableMacie => f.write_str("macie_disable_macie"),
            // Shield
            Self::ShieldListProtections => f.write_str("shield_list_protections"),
            Self::ShieldDescribeProtection => f.write_str("shield_describe_protection"),
            Self::ShieldCreateProtection => f.write_str("shield_create_protection"),
            Self::ShieldDeleteProtection => f.write_str("shield_delete_protection"),
            // Firewall Manager
            Self::FmsListPolicies => f.write_str("fms_list_policies"),
            Self::FmsGetPolicy => f.write_str("fms_get_policy"),
            Self::FmsPutPolicy => f.write_str("fms_put_policy"),
            Self::FmsDeletePolicy => f.write_str("fms_delete_policy"),
            // AppSync
            Self::AppSyncListGraphqlApis => f.write_str("appsync_list_graphql_apis"),
            Self::AppSyncGetGraphqlApi => f.write_str("appsync_get_graphql_api"),
            Self::AppSyncCreateGraphqlApi => f.write_str("appsync_create_graphql_api"),
            Self::AppSyncDeleteGraphqlApi => f.write_str("appsync_delete_graphql_api"),
            // Backup
            Self::BackupListBackupPlans => f.write_str("backup_list_backup_plans"),
            Self::BackupGetBackupPlan => f.write_str("backup_get_backup_plan"),
            Self::BackupCreateBackupPlan => f.write_str("backup_create_backup_plan"),
            Self::BackupDeleteBackupPlan => f.write_str("backup_delete_backup_plan"),
            Self::BackupListBackupVaults => f.write_str("backup_list_backup_vaults"),
            // CodeArtifact
            Self::CodeArtifactListDomains => f.write_str("codeartifact_list_domains"),
            Self::CodeArtifactListRepositories => f.write_str("codeartifact_list_repositories"),
            Self::CodeArtifactGetRepositoryEndpoint => f.write_str("codeartifact_get_repository_endpoint"),
            Self::CodeArtifactDeleteDomain => f.write_str("codeartifact_delete_domain"),
            // DMS
            Self::DmsDescribeReplicationInstances => f.write_str("dms_describe_replication_instances"),
            Self::DmsCreateReplicationInstance => f.write_str("dms_create_replication_instance"),
            Self::DmsDeleteReplicationInstance => f.write_str("dms_delete_replication_instance"),
            Self::DmsDescribeEndpoints => f.write_str("dms_describe_endpoints"),
            // DocumentDB
            Self::DocDbDescribeDbClusters => f.write_str("docdb_describe_db_clusters"),
            Self::DocDbCreateDbCluster => f.write_str("docdb_create_db_cluster"),
            Self::DocDbDeleteDbCluster => f.write_str("docdb_delete_db_cluster"),
            Self::DocDbDescribeDbInstances => f.write_str("docdb_describe_db_instances"),
            // Elastic Beanstalk
            Self::ElasticBeanstalkDescribeApplications => f.write_str("elasticbeanstalk_describe_applications"),
            Self::ElasticBeanstalkCreateApplication => f.write_str("elasticbeanstalk_create_application"),
            Self::ElasticBeanstalkDeleteApplication => f.write_str("elasticbeanstalk_delete_application"),
            Self::ElasticBeanstalkDescribeEnvironments => f.write_str("elasticbeanstalk_describe_environments"),
            // FSx
            Self::FSxDescribeFileSystems => f.write_str("fsx_describe_file_systems"),
            Self::FSxCreateFileSystem => f.write_str("fsx_create_file_system"),
            Self::FSxDeleteFileSystem => f.write_str("fsx_delete_file_system"),
            Self::FSxListTagsForResource => f.write_str("fsx_list_tags_for_resource"),
            // Kendra
            Self::KendraListIndices => f.write_str("kendra_list_indices"),
            Self::KendraDescribeIndex => f.write_str("kendra_describe_index"),
            Self::KendraCreateIndex => f.write_str("kendra_create_index"),
            Self::KendraDeleteIndex => f.write_str("kendra_delete_index"),
            Self::KendraQuery => f.write_str("kendra_query"),
            // Kinesis Data Analytics v2
            Self::KinesisAnalyticsListApplications => f.write_str("kinesisanalyticsv2_list_applications"),
            Self::KinesisAnalyticsDescribeApplication => f.write_str("kinesisanalyticsv2_describe_application"),
            Self::KinesisAnalyticsCreateApplication => f.write_str("kinesisanalyticsv2_create_application"),
            Self::KinesisAnalyticsDeleteApplication => f.write_str("kinesisanalyticsv2_delete_application"),
            // Lake Formation
            Self::LakeFormationGetDataLakeSettings => f.write_str("lakeformation_get_data_lake_settings"),
            Self::LakeFormationPutDataLakeSettings => f.write_str("lakeformation_put_data_lake_settings"),
            Self::LakeFormationGrantPermissions => f.write_str("lakeformation_grant_permissions"),
            Self::LakeFormationListPermissions => f.write_str("lakeformation_list_permissions"),
            // Lightsail
            Self::LightsailGetInstances => f.write_str("lightsail_get_instances"),
            Self::LightsailGetInstance => f.write_str("lightsail_get_instance"),
            Self::LightsailCreateInstances => f.write_str("lightsail_create_instances"),
            Self::LightsailDeleteInstance => f.write_str("lightsail_delete_instance"),
            Self::LightsailGetBundles => f.write_str("lightsail_get_bundles"),
            // MemoryDB
            Self::MemoryDbDescribeClusters => f.write_str("memorydb_describe_clusters"),
            Self::MemoryDbCreateCluster => f.write_str("memorydb_create_cluster"),
            Self::MemoryDbDeleteCluster => f.write_str("memorydb_delete_cluster"),
            Self::MemoryDbDescribeSubnetGroups => f.write_str("memorydb_describe_subnet_groups"),
            // MQ
            Self::MqListBrokers => f.write_str("mq_list_brokers"),
            Self::MqDescribeBroker => f.write_str("mq_describe_broker"),
            Self::MqCreateBroker => f.write_str("mq_create_broker"),
            Self::MqDeleteBroker => f.write_str("mq_delete_broker"),
            // Neptune
            Self::NeptuneDescribeDbClusters => f.write_str("neptune_describe_db_clusters"),
            Self::NeptuneCreateDbCluster => f.write_str("neptune_create_db_cluster"),
            Self::NeptuneDeleteDbCluster => f.write_str("neptune_delete_db_cluster"),
            Self::NeptuneDescribeDbInstances => f.write_str("neptune_describe_db_instances"),
            // QLDB
            Self::QldbListLedgers => f.write_str("qldb_list_ledgers"),
            Self::QldbDescribeLedger => f.write_str("qldb_describe_ledger"),
            Self::QldbCreateLedger => f.write_str("qldb_create_ledger"),
            Self::QldbDeleteLedger => f.write_str("qldb_delete_ledger"),
            // QuickSight
            Self::QuickSightListDashboards => f.write_str("quicksight_list_dashboards"),
            Self::QuickSightDescribeDashboard => f.write_str("quicksight_describe_dashboard"),
            Self::QuickSightListDataSets => f.write_str("quicksight_list_data_sets"),
            Self::QuickSightCreateDashboard => f.write_str("quicksight_create_dashboard"),
            // Service Catalog
            Self::ServiceCatalogListPortfolios => f.write_str("servicecatalog_list_portfolios"),
            Self::ServiceCatalogSearchProducts => f.write_str("servicecatalog_search_products"),
            Self::ServiceCatalogDescribeProduct => f.write_str("servicecatalog_describe_product"),
            Self::ServiceCatalogProvisionProduct => f.write_str("servicecatalog_provision_product"),
            // Storage Gateway
            Self::StorageGatewayListGateways => f.write_str("storagegateway_list_gateways"),
            Self::StorageGatewayDescribeGatewayInformation => f.write_str("storagegateway_describe_gateway_information"),
            Self::StorageGatewayActivateGateway => f.write_str("storagegateway_activate_gateway"),
            Self::StorageGatewayDeleteGateway => f.write_str("storagegateway_delete_gateway"),
            // Timestream
            Self::TimestreamListDatabases => f.write_str("timestream_list_databases"),
            Self::TimestreamCreateDatabase => f.write_str("timestream_create_database"),
            Self::TimestreamDeleteDatabase => f.write_str("timestream_delete_database"),
            Self::TimestreamWriteRecords => f.write_str("timestream_write_records"),
            Self::TimestreamQuery => f.write_str("timestream_query"),
            // Transfer Family
            Self::TransferListServers => f.write_str("transfer_list_servers"),
            Self::TransferDescribeServer => f.write_str("transfer_describe_server"),
            Self::TransferCreateServer => f.write_str("transfer_create_server"),
            Self::TransferDeleteServer => f.write_str("transfer_delete_server"),
            // Connect
            Self::ConnectListInstances => f.write_str("connect_list_instances"),
            Self::ConnectDescribeInstance => f.write_str("connect_describe_instance"),
            Self::ConnectCreateContactFlow => f.write_str("connect_create_contact_flow"),
            Self::ConnectListContactFlows => f.write_str("connect_list_contact_flows"),
            Self::ConnectListQueues => f.write_str("connect_list_queues"),
            // Pinpoint
            Self::PinpointGetApps => f.write_str("pinpoint_get_apps"),
            Self::PinpointCreateApp => f.write_str("pinpoint_create_app"),
            Self::PinpointDeleteApp => f.write_str("pinpoint_delete_app"),
            Self::PinpointSendMessages => f.write_str("pinpoint_send_messages"),
            Self::PinpointGetEndpoint => f.write_str("pinpoint_get_endpoint"),
            // DataSync
            Self::DataSyncListTasks => f.write_str("datasync_list_tasks"),
            Self::DataSyncDescribeTask => f.write_str("datasync_describe_task"),
            Self::DataSyncCreateTask => f.write_str("datasync_create_task"),
            Self::DataSyncDeleteTask => f.write_str("datasync_delete_task"),
            Self::DataSyncStartTaskExecution => f.write_str("datasync_start_task_execution"),
            // ACM PCA
            Self::AcmPcaListCertificateAuthorities => f.write_str("acmpca_list_certificate_authorities"),
            Self::AcmPcaDescribeCertificateAuthority => f.write_str("acmpca_describe_certificate_authority"),
            Self::AcmPcaCreateCertificateAuthority => f.write_str("acmpca_create_certificate_authority"),
            Self::AcmPcaDeleteCertificateAuthority => f.write_str("acmpca_delete_certificate_authority"),
            Self::AcmPcaIssueCertificate => f.write_str("acmpca_issue_certificate"),
            // Route53 Resolver
            Self::Route53ResolverListResolverRules => f.write_str("route53resolver_list_resolver_rules"),
            Self::Route53ResolverGetResolverRule => f.write_str("route53resolver_get_resolver_rule"),
            Self::Route53ResolverCreateResolverRule => f.write_str("route53resolver_create_resolver_rule"),
            Self::Route53ResolverDeleteResolverRule => f.write_str("route53resolver_delete_resolver_rule"),
            Self::Route53ResolverListResolverEndpoints => f.write_str("route53resolver_list_resolver_endpoints"),
            // VPC Lattice
            Self::VpcLatticeListServiceNetworks => f.write_str("vpclattice_list_service_networks"),
            Self::VpcLatticeCreateServiceNetwork => f.write_str("vpclattice_create_service_network"),
            Self::VpcLatticeDeleteServiceNetwork => f.write_str("vpclattice_delete_service_network"),
            Self::VpcLatticeListServices => f.write_str("vpclattice_list_services"),
            // Cloud Map
            Self::CloudMapListNamespaces => f.write_str("cloudmap_list_namespaces"),
            Self::CloudMapGetNamespace => f.write_str("cloudmap_get_namespace"),
            Self::CloudMapCreatePrivateDnsNamespace => f.write_str("cloudmap_create_private_dns_namespace"),
            Self::CloudMapDeleteNamespace => f.write_str("cloudmap_delete_namespace"),
            Self::CloudMapListServices => f.write_str("cloudmap_list_services"),
            // Direct Connect
            Self::DirectConnectDescribeConnections => f.write_str("directconnect_describe_connections"),
            Self::DirectConnectDescribeVirtualInterfaces => f.write_str("directconnect_describe_virtual_interfaces"),
            Self::DirectConnectCreateConnection => f.write_str("directconnect_create_connection"),
            Self::DirectConnectDeleteConnection => f.write_str("directconnect_delete_connection"),
            // Verified Permissions
            Self::VerifiedPermissionsListPolicyStores => f.write_str("verifiedpermissions_list_policy_stores"),
            Self::VerifiedPermissionsCreatePolicyStore => f.write_str("verifiedpermissions_create_policy_store"),
            Self::VerifiedPermissionsIsAuthorized => f.write_str("verifiedpermissions_is_authorized"),
            Self::VerifiedPermissionsCreatePolicy => f.write_str("verifiedpermissions_create_policy"),
            // Detective
            Self::DetectiveListGraphs => f.write_str("detective_list_graphs"),
            Self::DetectiveCreateGraph => f.write_str("detective_create_graph"),
            Self::DetectiveDeleteGraph => f.write_str("detective_delete_graph"),
            Self::DetectiveListMembers => f.write_str("detective_list_members"),
            // Keyspaces
            Self::KeyspacesListKeyspaces => f.write_str("keyspaces_list_keyspaces"),
            Self::KeyspacesGetKeyspace => f.write_str("keyspaces_get_keyspace"),
            Self::KeyspacesCreateKeyspace => f.write_str("keyspaces_create_keyspace"),
            Self::KeyspacesDeleteKeyspace => f.write_str("keyspaces_delete_keyspace"),
            Self::KeyspacesListTables => f.write_str("keyspaces_list_tables"),
            // Neptune Analytics
            Self::NeptuneAnalyticsListGraphs => f.write_str("neptuneanalytics_list_graphs"),
            Self::NeptuneAnalyticsGetGraph => f.write_str("neptuneanalytics_get_graph"),
            Self::NeptuneAnalyticsCreateGraph => f.write_str("neptuneanalytics_create_graph"),
            Self::NeptuneAnalyticsDeleteGraph => f.write_str("neptuneanalytics_delete_graph"),
            Self::NeptuneAnalyticsExecuteQuery => f.write_str("neptuneanalytics_execute_query"),
            // Clean Rooms
            Self::CleanRoomsListCollaborations => f.write_str("cleanrooms_list_collaborations"),
            Self::CleanRoomsGetCollaboration => f.write_str("cleanrooms_get_collaboration"),
            Self::CleanRoomsCreateCollaboration => f.write_str("cleanrooms_create_collaboration"),
            Self::CleanRoomsDeleteCollaboration => f.write_str("cleanrooms_delete_collaboration"),
            // DataZone
            Self::DataZoneListDomains => f.write_str("datazone_list_domains"),
            Self::DataZoneGetDomain => f.write_str("datazone_get_domain"),
            Self::DataZoneCreateDomain => f.write_str("datazone_create_domain"),
            Self::DataZoneDeleteDomain => f.write_str("datazone_delete_domain"),
            // IVS
            Self::IvsListChannels => f.write_str("ivs_list_channels"),
            Self::IvsGetChannel => f.write_str("ivs_get_channel"),
            Self::IvsCreateChannel => f.write_str("ivs_create_channel"),
            Self::IvsDeleteChannel => f.write_str("ivs_delete_channel"),
            Self::IvsListStreams => f.write_str("ivs_list_streams"),
            // GameLift
            Self::GameLiftListFleets => f.write_str("gamelift_list_fleets"),
            Self::GameLiftDescribeFleet => f.write_str("gamelift_describe_fleet"),
            Self::GameLiftCreateFleet => f.write_str("gamelift_create_fleet"),
            Self::GameLiftDeleteFleet => f.write_str("gamelift_delete_fleet"),
            Self::GameLiftDescribeGameSessions => f.write_str("gamelift_describe_game_sessions"),
            // IoT Analytics
            Self::IotAnalyticsListChannels => f.write_str("iotanalytics_list_channels"),
            Self::IotAnalyticsDescribeChannel => f.write_str("iotanalytics_describe_channel"),
            Self::IotAnalyticsListDatasets => f.write_str("iotanalytics_list_datasets"),
            Self::IotAnalyticsCreateChannel => f.write_str("iotanalytics_create_channel"),
            // IoT Events
            Self::IotEventsListDetectorModels => f.write_str("iotevents_list_detector_models"),
            Self::IotEventsDescribeDetectorModel => f.write_str("iotevents_describe_detector_model"),
            Self::IotEventsCreateDetectorModel => f.write_str("iotevents_create_detector_model"),
            Self::IotEventsDeleteDetectorModel => f.write_str("iotevents_delete_detector_model"),
            // Kinesis Video Streams
            Self::KinesisVideoListStreams => f.write_str("kinesisvideo_list_streams"),
            Self::KinesisVideoDescribeStream => f.write_str("kinesisvideo_describe_stream"),
            Self::KinesisVideoCreateStream => f.write_str("kinesisvideo_create_stream"),
            Self::KinesisVideoDeleteStream => f.write_str("kinesisvideo_delete_stream"),
            // Managed Grafana
            Self::ManagedGrafanaListWorkspaces => f.write_str("managedgrafana_list_workspaces"),
            Self::ManagedGrafanaDescribeWorkspace => f.write_str("managedgrafana_describe_workspace"),
            Self::ManagedGrafanaCreateWorkspace => f.write_str("managedgrafana_create_workspace"),
            Self::ManagedGrafanaDeleteWorkspace => f.write_str("managedgrafana_delete_workspace"),
            // AMP
            Self::AmpListWorkspaces => f.write_str("amp_list_workspaces"),
            Self::AmpDescribeWorkspace => f.write_str("amp_describe_workspace"),
            Self::AmpCreateWorkspace => f.write_str("amp_create_workspace"),
            Self::AmpDeleteWorkspace => f.write_str("amp_delete_workspace"),
            // OpsWorks
            Self::OpsWorksDescribeStacks => f.write_str("opsworks_describe_stacks"),
            Self::OpsWorksDescribeLayers => f.write_str("opsworks_describe_layers"),
            Self::OpsWorksCreateStack => f.write_str("opsworks_create_stack"),
            Self::OpsWorksDeleteStack => f.write_str("opsworks_delete_stack"),
            // SWF
            Self::SwfListDomains => f.write_str("swf_list_domains"),
            Self::SwfDescribeDomain => f.write_str("swf_describe_domain"),
            Self::SwfRegisterDomain => f.write_str("swf_register_domain"),
            Self::SwfDeprecateDomain => f.write_str("swf_deprecate_domain"),
            // Elastic Transcoder
            Self::ElasticTranscoderListPipelines => f.write_str("elastictranscoder_list_pipelines"),
            Self::ElasticTranscoderCreatePipeline => f.write_str("elastictranscoder_create_pipeline"),
            Self::ElasticTranscoderDeletePipeline => f.write_str("elastictranscoder_delete_pipeline"),
            Self::ElasticTranscoderCreateJob => f.write_str("elastictranscoder_create_job"),
            Self::ElasticTranscoderReadJob => f.write_str("elastictranscoder_read_job"),
            // EFS
            Self::EfsCreateFileSystem => f.write_str("efs_create_file_system"),
            Self::EfsDescribeFileSystems => f.write_str("efs_describe_file_systems"),
            Self::EfsDeleteFileSystem => f.write_str("efs_delete_file_system"),
            Self::EfsCreateMountTarget => f.write_str("efs_create_mount_target"),
            Self::EfsDescribeMountTargets => f.write_str("efs_describe_mount_targets"),
            // AppRunner
            Self::AppRunnerListServices => f.write_str("apprunner_list_services"),
            Self::AppRunnerDescribeService => f.write_str("apprunner_describe_service"),
            Self::AppRunnerCreateService => f.write_str("apprunner_create_service"),
            Self::AppRunnerDeleteService => f.write_str("apprunner_delete_service"),
            Self::AppRunnerPauseService => f.write_str("apprunner_pause_service"),
            // Amplify
            Self::AmplifyListApps => f.write_str("amplify_list_apps"),
            Self::AmplifyGetApp => f.write_str("amplify_get_app"),
            Self::AmplifyCreateApp => f.write_str("amplify_create_app"),
            Self::AmplifyDeleteApp => f.write_str("amplify_delete_app"),
            Self::AmplifyListBranches => f.write_str("amplify_list_branches"),
            // Snowball
            Self::SnowballListJobs => f.write_str("snowball_list_jobs"),
            Self::SnowballDescribeJob => f.write_str("snowball_describe_job"),
            Self::SnowballCreateJob => f.write_str("snowball_create_job"),
            Self::SnowballCancelJob => f.write_str("snowball_cancel_job"),
            // CloudHSM v2
            Self::CloudHsmV2ListClusters => f.write_str("cloudhsmv2_list_clusters"),
            Self::CloudHsmV2DescribeCluster => f.write_str("cloudhsmv2_describe_cluster"),
            Self::CloudHsmV2CreateCluster => f.write_str("cloudhsmv2_create_cluster"),
            Self::CloudHsmV2DeleteCluster => f.write_str("cloudhsmv2_delete_cluster"),
            Self::CloudHsmV2InitializeCluster => f.write_str("cloudhsmv2_initialize_cluster"),
            // Location
            Self::LocationListMaps => f.write_str("location_list_maps"),
            Self::LocationDescribeMap => f.write_str("location_describe_map"),
            Self::LocationCreateMap => f.write_str("location_create_map"),
            Self::LocationDeleteMap => f.write_str("location_delete_map"),
            Self::LocationSearchPlaceIndexForText => f.write_str("location_search_place_index_for_text"),
            // Network Manager
            Self::NetworkManagerListCoreNetworks => f.write_str("networkmanager_list_core_networks"),
            Self::NetworkManagerGetCoreNetwork => f.write_str("networkmanager_get_core_network"),
            Self::NetworkManagerCreateCoreNetwork => f.write_str("networkmanager_create_core_network"),
            Self::NetworkManagerDeleteCoreNetwork => f.write_str("networkmanager_delete_core_network"),
            // AppFlow
            Self::AppFlowListFlows => f.write_str("appflow_list_flows"),
            Self::AppFlowDescribeFlow => f.write_str("appflow_describe_flow"),
            Self::AppFlowCreateFlow => f.write_str("appflow_create_flow"),
            Self::AppFlowDeleteFlow => f.write_str("appflow_delete_flow"),
            Self::AppFlowStartFlow => f.write_str("appflow_start_flow"),
            // Redshift Serverless
            Self::RedshiftServerlessListWorkgroups => f.write_str("redshiftserverless_list_workgroups"),
            Self::RedshiftServerlessGetWorkgroup => f.write_str("redshiftserverless_get_workgroup"),
            Self::RedshiftServerlessCreateWorkgroup => f.write_str("redshiftserverless_create_workgroup"),
            Self::RedshiftServerlessDeleteWorkgroup => f.write_str("redshiftserverless_delete_workgroup"),
            Self::RedshiftServerlessListNamespaces => f.write_str("redshiftserverless_list_namespaces"),
            // HealthLake
            Self::HealthLakeListFhirDatastores => f.write_str("healthlake_list_fhir_datastores"),
            Self::HealthLakeDescribeFhirDatastore => f.write_str("healthlake_describe_fhir_datastore"),
            Self::HealthLakeCreateFhirDatastore => f.write_str("healthlake_create_fhir_datastore"),
            Self::HealthLakeDeleteFhirDatastore => f.write_str("healthlake_delete_fhir_datastore"),
            // Fraud Detector
            Self::FraudDetectorListDetectors => f.write_str("frauddetector_list_detectors"),
            Self::FraudDetectorGetDetectors => f.write_str("frauddetector_get_detectors"),
            Self::FraudDetectorCreateDetector => f.write_str("frauddetector_create_detector"),
            Self::FraudDetectorDeleteDetector => f.write_str("frauddetector_delete_detector"),
            Self::FraudDetectorGetEventTypes => f.write_str("frauddetector_get_event_types"),
            // Lookout for Metrics
            Self::LookoutMetricsListAnomalyDetectors => f.write_str("lookoutmetrics_list_anomaly_detectors"),
            Self::LookoutMetricsDescribeAnomalyDetector => f.write_str("lookoutmetrics_describe_anomaly_detector"),
            Self::LookoutMetricsCreateAnomalyDetector => f.write_str("lookoutmetrics_create_anomaly_detector"),
            Self::LookoutMetricsDeleteAnomalyDetector => f.write_str("lookoutmetrics_delete_anomaly_detector"),
            // Lookout for Vision
            Self::LookoutVisionListProjects => f.write_str("lookoutvision_list_projects"),
            Self::LookoutVisionDescribeProject => f.write_str("lookoutvision_describe_project"),
            Self::LookoutVisionCreateProject => f.write_str("lookoutvision_create_project"),
            Self::LookoutVisionDeleteProject => f.write_str("lookoutvision_delete_project"),
            // Lookout for Equipment
            Self::LookoutEquipmentListDatasets => f.write_str("lookoutequipment_list_datasets"),
            Self::LookoutEquipmentDescribeDataset => f.write_str("lookoutequipment_describe_dataset"),
            Self::LookoutEquipmentCreateDataset => f.write_str("lookoutequipment_create_dataset"),
            Self::LookoutEquipmentDeleteDataset => f.write_str("lookoutequipment_delete_dataset"),
            // IoT SiteWise
            Self::IotSiteWiseListAssets => f.write_str("iotsitewise_list_assets"),
            Self::IotSiteWiseDescribeAsset => f.write_str("iotsitewise_describe_asset"),
            Self::IotSiteWiseCreateAsset => f.write_str("iotsitewise_create_asset"),
            Self::IotSiteWiseDeleteAsset => f.write_str("iotsitewise_delete_asset"),
            Self::IotSiteWiseListAssetModels => f.write_str("iotsitewise_list_asset_models"),
            // IoT Greengrass v2
            Self::GreengrassV2ListCoreDevices => f.write_str("greengrassv2_list_core_devices"),
            Self::GreengrassV2GetCoreDevice => f.write_str("greengrassv2_get_core_device"),
            Self::GreengrassV2ListComponents => f.write_str("greengrassv2_list_components"),
            Self::GreengrassV2DeleteCoreDevice => f.write_str("greengrassv2_delete_core_device"),
            // Panorama
            Self::PanoramaListDevices => f.write_str("panorama_list_devices"),
            Self::PanoramaDescribeDevice => f.write_str("panorama_describe_device"),
            Self::PanoramaProvisionDevice => f.write_str("panorama_provision_device"),
            Self::PanoramaDeleteDevice => f.write_str("panorama_delete_device"),
            // CodeGuru Reviewer
            Self::CodeGuruListRepositoryAssociations => f.write_str("codeguru_list_repository_associations"),
            Self::CodeGuruAssociateRepository => f.write_str("codeguru_associate_repository"),
            Self::CodeGuruDisassociateRepository => f.write_str("codeguru_disassociate_repository"),
            Self::CodeGuruListCodeReviews => f.write_str("codeguru_list_code_reviews"),
            // DevOps Guru
            Self::DevOpsGuruListInsights => f.write_str("devopsguru_list_insights"),
            Self::DevOpsGuruDescribeInsight => f.write_str("devopsguru_describe_insight"),
            Self::DevOpsGuruListRecommendations => f.write_str("devopsguru_list_recommendations"),
            Self::DevOpsGuruListAnomaliesForInsight => f.write_str("devopsguru_list_anomalies_for_insight"),
            // Proton
            Self::ProtonListEnvironments => f.write_str("proton_list_environments"),
            Self::ProtonGetEnvironment => f.write_str("proton_get_environment"),
            Self::ProtonCreateEnvironment => f.write_str("proton_create_environment"),
            Self::ProtonDeleteEnvironment => f.write_str("proton_delete_environment"),
            // WorkMail
            Self::WorkMailListOrganizations => f.write_str("workmail_list_organizations"),
            Self::WorkMailDescribeOrganization => f.write_str("workmail_describe_organization"),
            Self::WorkMailCreateOrganization => f.write_str("workmail_create_organization"),
            Self::WorkMailDeleteOrganization => f.write_str("workmail_delete_organization"),
            // WorkDocs
            Self::WorkDocsDescribeRootFolders => f.write_str("workdocs_describe_root_folders"),
            Self::WorkDocsDescribeFolderContents => f.write_str("workdocs_describe_folder_contents"),
            Self::WorkDocsGetDocument => f.write_str("workdocs_get_document"),
            Self::WorkDocsInitiateDocumentVersionUpload => f.write_str("workdocs_initiate_document_version_upload"),
            // Braket
            Self::BraketGetDevice => f.write_str("braket_get_device"),
            Self::BraketSearchDevices => f.write_str("braket_search_devices"),
            Self::BraketCreateQuantumTask => f.write_str("braket_create_quantum_task"),
            Self::BraketGetQuantumTask => f.write_str("braket_get_quantum_task"),
            Self::BraketCancelQuantumTask => f.write_str("braket_cancel_quantum_task"),
            // RoboMaker
            Self::RoboMakerListSimulationJobs => f.write_str("robomaker_list_simulation_jobs"),
            Self::RoboMakerDescribeSimulationJob => f.write_str("robomaker_describe_simulation_job"),
            Self::RoboMakerCreateSimulationJob => f.write_str("robomaker_create_simulation_job"),
            Self::RoboMakerCancelSimulationJob => f.write_str("robomaker_cancel_simulation_job"),
            // Ground Station
            Self::GroundStationListContacts => f.write_str("groundstation_list_contacts"),
            Self::GroundStationListGroundStations => f.write_str("groundstation_list_ground_stations"),
            Self::GroundStationReserveContact => f.write_str("groundstation_reserve_contact"),
            Self::GroundStationCancelContact => f.write_str("groundstation_cancel_contact"),
            // Migration Hub
            Self::MigrationHubListApplicationStates => f.write_str("migrationhub_list_application_states"),
            Self::MigrationHubDescribeApplicationState => f.write_str("migrationhub_describe_application_state"),
            Self::MigrationHubListDiscoveredResources => f.write_str("migrationhub_list_discovered_resources"),
            Self::MigrationHubListCreatedArtifacts => f.write_str("migrationhub_list_created_artifacts"),
            // Application Discovery
            Self::ApplicationDiscoveryDescribeAgents => f.write_str("applicationdiscovery_describe_agents"),
            Self::ApplicationDiscoveryGetDiscoverySummary => f.write_str("applicationdiscovery_get_discovery_summary"),
            Self::ApplicationDiscoveryListConfigurations => f.write_str("applicationdiscovery_list_configurations"),
            Self::ApplicationDiscoveryDescribeApplications => f.write_str("applicationdiscovery_describe_applications"),
            // Elastic Disaster Recovery
            Self::DrsDescribeJobs => f.write_str("drs_describe_jobs"),
            Self::DrsDescribeSourceServers => f.write_str("drs_describe_source_servers"),
            Self::DrsDeleteSourceServer => f.write_str("drs_delete_source_server"),
            Self::DrsListStagingAccounts => f.write_str("drs_list_staging_accounts"),
            // Resilience Hub
            Self::ResilienceHubListApps => f.write_str("resiliencehub_list_apps"),
            Self::ResilienceHubDescribeApp => f.write_str("resiliencehub_describe_app"),
            Self::ResilienceHubCreateApp => f.write_str("resiliencehub_create_app"),
            Self::ResilienceHubDeleteApp => f.write_str("resiliencehub_delete_app"),
            // Managed Blockchain
            Self::ManagedBlockchainListNetworks => f.write_str("managedblockchain_list_networks"),
            Self::ManagedBlockchainGetNetwork => f.write_str("managedblockchain_get_network"),
            Self::ManagedBlockchainListMembers => f.write_str("managedblockchain_list_members"),
            Self::ManagedBlockchainGetMember => f.write_str("managedblockchain_get_member"),
            // IAM Identity Center (SSO Admin)
            Self::SsoAdminListInstances => f.write_str("ssoadmin_list_instances"),
            Self::SsoAdminListPermissionSets => f.write_str("ssoadmin_list_permission_sets"),
            Self::SsoAdminDescribePermissionSet => f.write_str("ssoadmin_describe_permission_set"),
            Self::SsoAdminCreatePermissionSet => f.write_str("ssoadmin_create_permission_set"),
            Self::SsoAdminListAccountAssignments => f.write_str("ssoadmin_list_account_assignments"),
            // CodeStar Connections
            Self::CodeStarConnectionsListConnections => f.write_str("codestarconnections_list_connections"),
            Self::CodeStarConnectionsGetConnection => f.write_str("codestarconnections_get_connection"),
            Self::CodeStarConnectionsCreateConnection => f.write_str("codestarconnections_create_connection"),
            Self::CodeStarConnectionsDeleteConnection => f.write_str("codestarconnections_delete_connection"),
            // EMR Serverless
            Self::EmrServerlessListApplications => f.write_str("emrserverless_list_applications"),
            Self::EmrServerlessGetApplication => f.write_str("emrserverless_get_application"),
            Self::EmrServerlessCreateApplication => f.write_str("emrserverless_create_application"),
            Self::EmrServerlessDeleteApplication => f.write_str("emrserverless_delete_application"),
            Self::EmrServerlessStartJobRun => f.write_str("emrserverless_start_job_run"),
            // EventBridge Scheduler
            Self::SchedulerListSchedules => f.write_str("scheduler_list_schedules"),
            Self::SchedulerGetSchedule => f.write_str("scheduler_get_schedule"),
            Self::SchedulerCreateSchedule => f.write_str("scheduler_create_schedule"),
            Self::SchedulerDeleteSchedule => f.write_str("scheduler_delete_schedule"),
            // Glue DataBrew
            Self::DataBrewListProjects => f.write_str("databrew_list_projects"),
            Self::DataBrewDescribeProject => f.write_str("databrew_describe_project"),
            Self::DataBrewCreateProject => f.write_str("databrew_create_project"),
            Self::DataBrewDeleteProject => f.write_str("databrew_delete_project"),
            Self::DataBrewListDatasets => f.write_str("databrew_list_datasets"),
            // Security Lake
            Self::SecurityLakeListDataLakes => f.write_str("securitylake_list_data_lakes"),
            Self::SecurityLakeCreateDataLake => f.write_str("securitylake_create_data_lake"),
            Self::SecurityLakeDeleteDataLake => f.write_str("securitylake_delete_data_lake"),
            Self::SecurityLakeListLogSources => f.write_str("securitylake_list_log_sources"),
            Self::SecurityLakeCreateSubscriber => f.write_str("securitylake_create_subscriber"),
            // S3 Control
            Self::S3ControlListBuckets => f.write_str("s3control_list_buckets"),
            Self::S3ControlListAccessPoints => f.write_str("s3control_list_access_points"),
            Self::S3ControlCreateAccessPoint => f.write_str("s3control_create_access_point"),
            Self::S3ControlDeleteAccessPoint => f.write_str("s3control_delete_access_point"),
            Self::S3ControlGetAccessPoint => f.write_str("s3control_get_access_point"),
            // Bedrock Agent
            Self::BedrockAgentListAgents => f.write_str("bedrockagent_list_agents"),
            Self::BedrockAgentGetAgent => f.write_str("bedrockagent_get_agent"),
            Self::BedrockAgentCreateAgent => f.write_str("bedrockagent_create_agent"),
            Self::BedrockAgentDeleteAgent => f.write_str("bedrockagent_delete_agent"),
            Self::BedrockAgentListAgentAliases => f.write_str("bedrockagent_list_agent_aliases"),
            // CloudWatch Evidently
            Self::EvidentlyListProjects => f.write_str("evidently_list_projects"),
            Self::EvidentlyGetProject => f.write_str("evidently_get_project"),
            Self::EvidentlyCreateProject => f.write_str("evidently_create_project"),
            Self::EvidentlyDeleteProject => f.write_str("evidently_delete_project"),
            Self::EvidentlyListFeatures => f.write_str("evidently_list_features"),
            // CloudWatch RUM
            Self::RumListAppMonitors => f.write_str("rum_list_app_monitors"),
            Self::RumGetAppMonitor => f.write_str("rum_get_app_monitor"),
            Self::RumCreateAppMonitor => f.write_str("rum_create_app_monitor"),
            Self::RumDeleteAppMonitor => f.write_str("rum_delete_app_monitor"),
            // CloudWatch Internet Monitor
            Self::InternetMonitorListMonitors => f.write_str("internetmonitor_list_monitors"),
            Self::InternetMonitorGetMonitor => f.write_str("internetmonitor_get_monitor"),
            Self::InternetMonitorCreateMonitor => f.write_str("internetmonitor_create_monitor"),
            Self::InternetMonitorDeleteMonitor => f.write_str("internetmonitor_delete_monitor"),
            // Compute Optimizer
            Self::ComputeOptimizerGetEc2InstanceRecommendations => f.write_str("computeoptimizer_get_ec2_instance_recommendations"),
            Self::ComputeOptimizerGetLambdaFunctionRecommendations => f.write_str("computeoptimizer_get_lambda_function_recommendations"),
            Self::ComputeOptimizerGetAutoScalingGroupRecommendations => {
                f.write_str("computeoptimizer_get_auto_scaling_group_recommendations")
            }
            Self::ComputeOptimizerGetRecommendationSummaries => f.write_str("computeoptimizer_get_recommendation_summaries"),
            // Systems Manager Incidents
            Self::SsmIncidentsListIncidentRecords => f.write_str("ssmincidents_list_incident_records"),
            Self::SsmIncidentsGetIncidentRecord => f.write_str("ssmincidents_get_incident_record"),
            Self::SsmIncidentsCreateReplicationSet => f.write_str("ssmincidents_create_replication_set"),
            Self::SsmIncidentsDeleteIncidentRecord => f.write_str("ssmincidents_delete_incident_record"),
            // Resource Groups Tagging
            Self::ResourceGroupsTaggingGetResources => f.write_str("resourcegroupstagging_get_resources"),
            Self::ResourceGroupsTaggingTagResources => f.write_str("resourcegroupstagging_tag_resources"),
            Self::ResourceGroupsTaggingUntagResources => f.write_str("resourcegroupstagging_untag_resources"),
            Self::ResourceGroupsTaggingGetTagKeys => f.write_str("resourcegroupstagging_get_tag_keys"),
            // IoT TwinMaker
            Self::IotTwinMakerListWorkspaces => f.write_str("iottwinmaker_list_workspaces"),
            Self::IotTwinMakerGetWorkspace => f.write_str("iottwinmaker_get_workspace"),
            Self::IotTwinMakerCreateWorkspace => f.write_str("iottwinmaker_create_workspace"),
            Self::IotTwinMakerDeleteWorkspace => f.write_str("iottwinmaker_delete_workspace"),
            // AWS Support
            Self::SupportDescribeCases => f.write_str("support_describe_cases"),
            Self::SupportCreateCase => f.write_str("support_create_case"),
            Self::SupportResolveCase => f.write_str("support_resolve_case"),
            Self::SupportDescribeServices => f.write_str("support_describe_services"),
            // AWS Health
            Self::HealthDescribeEvents => f.write_str("health_describe_events"),
            Self::HealthDescribeEventDetails => f.write_str("health_describe_event_details"),
            Self::HealthDescribeAffectedEntities => f.write_str("health_describe_affected_entities"),
            Self::HealthDescribeAffectedAccountsForOrganization => f.write_str("health_describe_affected_accounts_for_organization"),
            // AWS Budgets
            Self::BudgetsDescribeBudgets => f.write_str("budgets_describe_budgets"),
            Self::BudgetsCreateBudget => f.write_str("budgets_create_budget"),
            Self::BudgetsDeleteBudget => f.write_str("budgets_delete_budget"),
            Self::BudgetsDescribeBudgetPerformanceHistory => f.write_str("budgets_describe_budget_performance_history"),
            // License Manager
            Self::LicenseManagerListLicenses => f.write_str("licensemanager_list_licenses"),
            Self::LicenseManagerGetLicense => f.write_str("licensemanager_get_license"),
            Self::LicenseManagerListReceivedLicenses => f.write_str("licensemanager_list_received_licenses"),
            Self::LicenseManagerListResourceInventory => f.write_str("licensemanager_list_resource_inventory"),
            // Savings Plans
            Self::SavingsPlansDescribeSavingsPlans => f.write_str("savingsplans_describe_savings_plans"),
            Self::SavingsPlansDescribeSavingsPlansOfferings => f.write_str("savingsplans_describe_savings_plans_offerings"),
            Self::SavingsPlansCreateSavingsPlan => f.write_str("savingsplans_create_savings_plan"),
            Self::SavingsPlansListTagsForResource => f.write_str("savingsplans_list_tags_for_resource"),
            // Resource Groups
            Self::ResourceGroupsListGroups => f.write_str("resourcegroups_list_groups"),
            Self::ResourceGroupsGetGroup => f.write_str("resourcegroups_get_group"),
            Self::ResourceGroupsCreateGroup => f.write_str("resourcegroups_create_group"),
            Self::ResourceGroupsDeleteGroup => f.write_str("resourcegroups_delete_group"),
            Self::ResourceGroupsListGroupResources => f.write_str("resourcegroups_list_group_resources"),
            // Resource Explorer
            Self::ResourceExplorerSearch => f.write_str("resourceexplorer_search"),
            Self::ResourceExplorerListIndexes => f.write_str("resourceexplorer_list_indexes"),
            Self::ResourceExplorerCreateIndex => f.write_str("resourceexplorer_create_index"),
            Self::ResourceExplorerDeleteIndex => f.write_str("resourceexplorer_delete_index"),
            // FIS
            Self::FisListExperimentTemplates => f.write_str("fis_list_experiment_templates"),
            Self::FisGetExperimentTemplate => f.write_str("fis_get_experiment_template"),
            Self::FisCreateExperimentTemplate => f.write_str("fis_create_experiment_template"),
            Self::FisDeleteExperimentTemplate => f.write_str("fis_delete_experiment_template"),
            Self::FisStartExperiment => f.write_str("fis_start_experiment"),
            // CloudWatch Synthetics
            Self::SyntheticsDescribeCanaries => f.write_str("synthetics_describe_canaries"),
            Self::SyntheticsGetCanary => f.write_str("synthetics_get_canary"),
            Self::SyntheticsCreateCanary => f.write_str("synthetics_create_canary"),
            Self::SyntheticsDeleteCanary => f.write_str("synthetics_delete_canary"),
            Self::SyntheticsStartCanary => f.write_str("synthetics_start_canary"),
            // KMS
            Self::KmsCreateKey => f.write_str("kms_create_key"),
            Self::KmsDescribeKey => f.write_str("kms_describe_key"),
            Self::KmsListKeys => f.write_str("kms_list_keys"),
            Self::KmsEncrypt => f.write_str("kms_encrypt"),
            Self::KmsDecrypt => f.write_str("kms_decrypt"),
            Self::KmsGenerateDataKey => f.write_str("kms_generate_data_key"),
            Self::KmsScheduleKeyDeletion => f.write_str("kms_schedule_key_deletion"),
            Self::KmsListAliases => f.write_str("kms_list_aliases"),
            Self::KmsCreateAlias => f.write_str("kms_create_alias"),
            // SES v2
            Self::SesV2SendEmail => f.write_str("ses_v2_send_email"),
            Self::SesV2CreateEmailIdentity => f.write_str("ses_v2_create_email_identity"),
            Self::SesV2DeleteEmailIdentity => f.write_str("ses_v2_delete_email_identity"),
            Self::SesV2ListEmailIdentities => f.write_str("ses_v2_list_email_identities"),
            Self::SesV2GetEmailIdentity => f.write_str("ses_v2_get_email_identity"),
            Self::SesV2GetAccount => f.write_str("ses_v2_get_account"),
            Self::SesV2ListContactLists => f.write_str("ses_v2_list_contact_lists"),
            Self::SesV2CreateContactList => f.write_str("ses_v2_create_contact_list"),
        }
    }
}
