#![allow(clippy::upper_case_acronyms)]
// Intentional: protocol/command acronyms (ACL, GEO, etc.)
// TODO: Remove this once AvailableIn, ACL enums and Version struct are used or removed.
// These are placeholder types for future Redis feature support.
#![allow(dead_code)]

mod auto_suggest;
mod bitmap;
mod bloom_filter;
mod cluster_management;
mod connection_management;
mod count_min_sketch;
mod cuckoo_filter;
mod generic;
mod geospatial_indices;
pub mod hash;
mod hyper_log_log;
mod json;
mod list;
mod multi_command;
pub mod multi_key_policy;
mod pub_sub;
mod raw_command;
mod raw_command_read_only;
mod redis_query_engine;
mod scripting_and_functions;
mod server_management;
mod set;
mod sorted_set;
mod stream;
mod string;
mod t_digest;
mod time_series;
mod top_k;
mod transactions;
mod vector_set;

pub use auto_suggest::*;
pub use bitmap::*;
pub use bloom_filter::*;
#[allow(ambiguous_glob_reexports)]
pub use cluster_management::*;
pub use connection_management::*;
pub use count_min_sketch::*;
pub use cuckoo_filter::*;
#[allow(ambiguous_glob_reexports)]
pub use generic::*;
pub use geospatial_indices::*;
pub use hash::*;
pub use hyper_log_log::*;
pub use json::*;
#[allow(ambiguous_glob_reexports)]
pub use list::*;
pub use multi_command::MultiCommand;
pub use pub_sub::*;
pub use raw_command::*;
pub use raw_command_read_only::*;
pub use redis_query_engine::*;
pub use scripting_and_functions::*;
#[allow(ambiguous_glob_reexports)]
pub use server_management::*;
pub use set::*;
#[allow(ambiguous_glob_reexports)]
pub use sorted_set::*;
#[allow(ambiguous_glob_reexports)]
pub use stream::*;
pub use string::*;
pub use t_digest::*;
pub use time_series::*;
pub use top_k::*;
pub use transactions::*;
pub use vector_set::*;

#[cfg(test)]
mod test_errors;

use crate::api::lib::multi_key_policy::{ExecutionConstraint, FrameAction, RespWireVersion, plan_frame};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::ep::RedisEp;
use crate::protocol::RedisBytes;
use crate::protocol::RedisProtocol;
use crate::{EP, EpOutput, RedisOperation, ToOutput, redis_api_commands};
use eden_logger_internal::{ctx_with_trace, log_trace};
use endpoint_derive::{ApiBuilder, DocumentAPI};
use endpoint_types::protocol::EpProtocol;
use ep_core::settings::EdenSettings;
use error::{EpError, ResultEP};
use format::cache_uuid::EndpointCacheUuid;
use function_name::named;
use redis_core::{RedisAsync, RespBytes, RespResponse};
use redis_core::{RedisTx, config::MultiKeyExecution};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Display;
use std::str::FromStr;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use telemetry::guards::EndpointGuard;
use tokio::time::Instant;
use utoipa::ToSchema;

enum AvailableIn {
    Bloom(Version),
    Json(Version),
    Redis(Version),
    RedisStack,
    Search(Version),
    Timeseries(Version),
}

impl Display for AvailableIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AvailableIn::Bloom(version) => write!(f, "bloom, {}", version),
            AvailableIn::Json(version) => write!(f, "json, {}", version),
            AvailableIn::Redis(version) => write!(f, "redis, {}", version),
            AvailableIn::RedisStack => write!(f, "redis-stack"),
            AvailableIn::Search(version) => write!(f, "redis-search, {}", version),
            AvailableIn::Timeseries(version) => write!(f, "redis-timeseries, {}", version),
        }
    }
}

enum ACL {
    Admin,
    Bitmap,
    Blocking,
    Connection,
    Dangerous,
    Fast,
    Geo,
    Hash,
    Hyperloglog,
    Keyspace,
    List,
    Pubsub,
    Read,
    Scripting,
    Set,
    Slow,
    Sortedset,
    Stream,
    String,
    Write,
    Transaction,
}

impl Display for ACL {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ACL::Admin => write!(f, "@admin"),
            ACL::Bitmap => write!(f, "@bitmap"),
            ACL::Blocking => write!(f, "@blocking"),
            ACL::Connection => write!(f, "@connection"),
            ACL::Dangerous => write!(f, "@dangerous"),
            ACL::Fast => write!(f, "@fast"),
            ACL::Geo => write!(f, "@geo"),
            ACL::Hash => write!(f, "@hash"),
            ACL::Hyperloglog => write!(f, "@hyperloglog"),
            ACL::Keyspace => write!(f, "@keyspace"),
            ACL::List => write!(f, "@list"),
            ACL::Pubsub => write!(f, "@pubsub"),
            ACL::Read => write!(f, "@read"),
            ACL::Scripting => write!(f, "@scripting"),
            ACL::Set => write!(f, "@set"),
            ACL::Slow => write!(f, "@slow"),
            ACL::Sortedset => write!(f, "@sortedset"),
            ACL::Stream => write!(f, "@stream"),
            ACL::String => write!(f, "@string"),
            ACL::Write => write!(f, "@write"),
            ACL::Transaction => write!(f, "@transaction"),
        }
    }
}

struct Version {
    major: u8,
    minor: u8,
    revision: u8,
}

impl Version {
    pub fn new(major: u8, minor: u8, revision: u8) -> Self {
        Self { major, minor, revision }
    }
}

impl Display for Version {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.revision)
    }
}

pub trait RedisCommandInput: Send + Sync {
    fn kind(&self) -> RedisApi;
    fn keys(&self) -> Vec<RedisKey>;
    fn command(&self) -> bytes::Bytes;
    //TODO, implement a FROM RAW
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized;
    #[named]
    #[allow(async_fn_in_trait)]
    async fn run_async_generic(&self, context: RedisAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _t0 = Instant::now();
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));
        let _ctx = ctx_with_trace!().with_feature("redis");
        log_trace!(
            _ctx,
            "RedisRunAsyncGeneric: tracer initialized, {} µs",
            audience = eden_logger_internal::LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        let mut client = context.get().await.map_err(|e| {
            let ep_error = EpError::parse_redis_error(e);
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(ep_error.to_string()) });
            ep_error
        })?;
        let _ctx = ctx_with_trace!().with_feature("redis");
        log_trace!(
            _ctx,
            "RedisRunAsyncGeneric: got context, RedisConnectionManager, {} µs",
            audience = eden_logger_internal::LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        let labels = telemetry_wrapper.labels_low_cardinality();
        let labels_refs: Vec<(&str, &str)> = labels.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        let metrics = telemetry_wrapper.metrics();

        let _endpoint_guard = EndpointGuard::new(metrics.endpoint(), &labels_refs);
        let _ctx = ctx_with_trace!().with_feature("redis");
        log_trace!(
            _ctx,
            "RedisRunAsyncGeneric: metrics initialized, {} µs",
            audience = eden_logger_internal::LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        let cmd_bytes = self.command();
        let mode = client.multi_key_execution();

        // In Native mode short-circuit to the original send-and-go path —
        // no parsing, no extra allocation, byte-identical to direct forwarding.
        if matches!(mode, MultiKeyExecution::Native) {
            let result = client.send_command_raw(&cmd_bytes).await;
            return Ok(Box::new(
                result
                    .map_err(|e| {
                        let ep_error = EpError::parse_redis_error(e);
                        span.set_status(FastSpanStatus::Error { message: Cow::Owned(ep_error.to_string()) });
                        ep_error
                    })?
                    .0
                    .to_output(),
            ) as Box<dyn EpOutput>);
        }

        // Deconstruct mode: classify the command and either forward,
        // reject before contacting Redis, or fan out per-key sub-commands
        // on the same checked-out connection (so SameConnection holds
        // for WATCH).
        let response = match plan_frame(cmd_bytes.clone(), mode)? {
            FrameAction::Forward(_) => client.send_command_raw_no_reconnect(&cmd_bytes).await.map_err(|e| {
                let ep_error = EpError::parse_redis_error(e);
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(ep_error.to_string()) });
                ep_error
            })?,
            FrameAction::Reject(err_bytes) => RespResponse::from(RespBytes::Resp2(err_bytes)),
            FrameAction::Split { original: _, parts, combiner, constraint } => {
                match constraint {
                    ExecutionConstraint::SameConnection => {}
                    ExecutionConstraint::AnyConnection => {
                        // v1 keeps all split parts on this checked-out client.
                        // Future parallel read fan-out must preserve response
                        // ordering and transaction/session safety.
                    }
                }
                let mut part_responses = Vec::with_capacity(parts.len());
                let mut protocol_slot: Option<RespWireVersion> = None;
                for part in &parts {
                    let resp = client.send_command_raw_no_reconnect(part).await.map_err(|e| {
                        let ep_error = EpError::parse_redis_error(e);
                        span.set_status(FastSpanStatus::Error { message: Cow::Owned(ep_error.to_string()) });
                        ep_error
                    })?;
                    RespWireVersion::require_consistent(&mut protocol_slot, RespWireVersion::from_resp3_flag(resp.is_resp3()))?;
                    part_responses.push(resp.to_bytes());
                }
                let protocol = protocol_slot.ok_or_else(|| EpError::parse("split produced no parts"))?;
                let combined = combiner.combine_bytes(part_responses, protocol)?;
                let resp_bytes = match protocol {
                    RespWireVersion::Resp3 => RespBytes::Resp3(combined),
                    RespWireVersion::Resp2 => RespBytes::Resp2(combined),
                };
                RespResponse::from(resp_bytes)
            }
        };

        let _ctx = ctx_with_trace!().with_feature("redis");
        log_trace!(
            _ctx,
            "RedisRunAsyncGeneric: got send_command_raw results, {} µs",
            audience = eden_logger_internal::LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        Ok(Box::new(response.to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, context: &mut RedisTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        let (frame, _) = RedisProtocol::decode_buffer(&self.command())
            .unwrap_or_else(|| panic!("failed to decode Redis transaction command for {}", self.kind()));
        let parsed = crate::protocol::decoder::RedisCommandArgs::try_from(frame)
            .unwrap_or_else(|e| panic!("failed to parse Redis transaction command for {}: {e}", self.kind()));

        let mut cmd = redis::Cmd::new();
        cmd.arg(parsed.command().to_string());
        for arg in parsed.args() {
            cmd.arg(arg);
        }

        context.add_command(cmd);
    }
}

/// The trait for defining output structs for Redis commands
pub trait RedisCommandOutput: Send + Sync {
    fn kind(&self) -> RedisApi;
    fn decode(bytes: &[u8]) -> Result<Self, EpError>
    where
        Self: Sized;
}

#[derive(Debug, Default, Clone)]
pub struct RedisConflictData {
    keys: Vec<RedisKey>,
}

impl RedisConflictData {
    pub fn new(keys: Vec<RedisKey>) -> Self {
        Self { keys }
    }

    pub fn keys(self) -> Vec<RedisKey> {
        self.keys
    }
}

#[allow(deprecated)]
type SlaveofCompatInput = SlaveofInput;

redis_api_commands! {
    (AclCat, "ACL CAT", AclCatInput),
    (AclDeluser, "ACL DELUSER", AclDeluserInput),
    (AclDryrun, "ACL DRYRUN", AclDryrunInput),
    (AclGenpass, "ACL GENPASS", AclGenpassInput),
    (AclGetuser, "ACL GETUSER", AclGetuserInput),
    (AclList, "ACL LIST", AclListInput),
    (AclLoad, "ACL LOAD", AclLoadInput),
    (AclLog, "ACL LOG", AclLogInput),
    (AclSave, "ACL SAVE", AclSaveInput),
    (AclSetuser, "ACL SETUSER", AclSetuserInput),
    (AclUsers, "ACL USERS", AclUsersInput),
    (AclWhoami, "ACL WHOAMI", AclWhoamiInput),
    (Append, "APPEND", AppendInput),
    (Asking, "ASKING", AskingInput),
    (Auth, "AUTH", AuthInput),
    (BfAdd, "BF.ADD", BfAddInput),
    (BfCard, "BF.CARD", BfCardInput),
    (BfExists, "BF.EXISTS", BfExistsInput),
    (BfInfo, "BF.INFO", BfInfoInput),
    (BfInsert, "BF.INSERT", BfInsertInput),
    (BfLoadchunk, "BF.LOADCHUNK", BfLoadchunkInput),
    (BfMadd, "BF.MADD", BfMaddInput),
    (BfMexists, "BF.MEXISTS", BfMexistsInput),
    (BfReserve, "BF.RESERVE", BfReserveInput),
    (BfScandump, "BF.SCANDUMP", BfScandumpInput),
    (Bgrewriteaof, "BGREWRITEAOF", BgrewriteaofInput),
    (Bgsave, "BGSAVE", BgsaveInput),
    (Bitcount, "BITCOUNT", BitcountInput),
    (Bitfield, "BITFIELD", BitfieldInput),
    (BitfieldRo, "BITFIELD_RO", BitfieldRoInput),
    (Bitop, "BITOP", BitopInput),
    (Bitpos, "BITPOS", BitposInput),
    (Blmove, "BLMOVE", BlmoveInput),
    (Blmpop, "BLMPOP", BlmpopInput),
    (Blpop, "BLPOP", BlpopInput),
    (Brpop, "BRPOP", BrpopInput),
    (Brpoplpush, "BRPOPLPUSH", BrpoplpushInput),
    (Bzmpop, "BZMPOP", BzmpopInput),
    (Bzpopmax, "BZPOPMAX", BzpopmaxInput),
    (Bzpopmin, "BZPOPMIN", BzpopminInput),
    (CfAdd, "CF.ADD", CfAddInput),
    (CfAddnx, "CF.ADDNX", CfAddnxInput),
    (CfCount, "CF.COUNT", CfCountInput),
    (CfDel, "CF.DEL", CfDelInput),
    (CfExists, "CF.EXISTS", CfExistsInput),
    (CfInfo, "CF.INFO", CfInfoInput),
    (CfInsert, "CF.INSERT", CfInsertInput),
    (CfInsertnx, "CF.INSERTNX", CfInsertnxInput),
    (CfLoadchunk, "CF.LOADCHUNK", CfLoadchunkInput),
    (CfMexists, "CF.MEXISTS", CfMexistsInput),
    (CfReserve, "CF.RESERVE", CfReserveInput),
    (CfScandump, "CF.SCANDUMP", CfScandumpInput),
    (ClientCaching, "CLIENT CACHING", ClientCachingInput),
    (ClientGetname, "CLIENT GETNAME", ClientGetnameInput),
    (ClientGetredir, "CLIENT GETREDIR", ClientGetredirInput),
    (ClientId, "CLIENT ID", ClientIdInput),
    (ClientInfo, "CLIENT INFO", ClientInfoInput),
    (ClientKill, "CLIENT KILL", ClientKillInput),
    (ClientList, "CLIENT LIST", ClientListInput),
    (ClientNoEvict, "CLIENT NO-EVICT", ClientNoEvictInput),
    (ClientNoTouch, "CLIENT NO-TOUCH", ClientNoTouchInput),
    (ClientPause, "CLIENT PAUSE", ClientPauseInput),
    (ClientSetinfo, "CLIENT SETINFO", ClientSetinfoInput),
    (ClientSetname, "CLIENT SETNAME", ClientSetnameInput),
    (ClientTracking, "CLIENT TRACKING", ClientTrackingInput),
    (ClientTrackinginfo, "CLIENT TRACKINGINFO", ClientTrackinginfoInput),
    (ClientUnblock, "CLIENT UNBLOCK", ClientUnblockInput),
    (ClientUnpause, "CLIENT UNPAUSE", ClientUnpauseInput),
    (ClusterAddslots, "CLUSTER ADDSLOTS", ClusterAddslotsInput),
    (ClusterAddslotsrange, "CLUSTER ADDSLOTSRANGE", ClusterAddslotsrangeInput),
    (ClusterBumpepoch, "CLUSTER BUMPEPOCH", ClusterBumpepochInput),
    (ClusterCountFailureReports, "CLUSTER COUNT-FAILURE-REPORTS", ClusterCountFailureReportsInput),
    (ClusterCountkeysinslot, "CLUSTER COUNTKEYSINSLOT", ClusterCountkeysinslotInput),
    (ClusterDelslots, "CLUSTER DELSLOTS", ClusterDelslotsInput),
    (ClusterDelslotsrange, "CLUSTER DELSLOTSRANGE", ClusterDelslotsrangeInput),
    (ClusterFailover, "CLUSTER FAILOVER", ClusterFailoverInput),
    (ClusterFlushslots, "CLUSTER FLUSHSLOTS", ClusterFlushslotsInput),
    (ClusterForget, "CLUSTER FORGET", ClusterForgetInput),
    (ClusterGetkeysinslot, "CLUSTER GETKEYSINSLOT", ClusterGetkeysinslotInput),
    (ClusterInfo, "CLUSTER INFO", ClusterInfoInput),
    (ClusterKeyslot, "CLUSTER KEYSLOT", ClusterKeyslotInput),
    (ClusterLinks, "CLUSTER LINKS", ClusterLinksInput),
    (ClusterMeet, "CLUSTER MEET", ClusterMeetInput),
    (ClusterMyid, "CLUSTER MYID", ClusterMyidInput),
    (ClusterMyshardid, "CLUSTER MYSHARDID", ClusterMyshardidInput),
    (ClusterNodes, "CLUSTER NODES", ClusterNodesInput),
    (ClusterReplicas, "CLUSTER REPLICAS", ClusterReplicasInput),
    (ClusterReplicate, "CLUSTER REPLICATE", ClusterReplicateInput),
    (ClusterReset, "CLUSTER RESET", ClusterResetInput),
    (ClusterSaveconfig, "CLUSTER SAVECONFIG", ClusterSaveconfigInput),
    (ClusterSetConfigEpoch, "CLUSTER SET-CONFIG-EPOCH", ClusterSetConfigEpochInput),
    (ClusterSetslot, "CLUSTER SETSLOT", ClusterSetslotInput),
    (ClusterShards, "CLUSTER SHARDS", ClusterShardsInput),
    (ClusterSlaves, "CLUSTER SLAVES", ClusterSlavesInput),
    (ClusterSlots, "CLUSTER SLOTS", ClusterSlotsInput),
    (CmsIncrby, "CMS.INCRBY", CmsIncrbyInput),
    (CmsInfo, "CMS.INFO", CmsInfoInput),
    (CmsInitbydim, "CMS.INITBYDIM", CmsInitbydimInput),
    (CmsInitbyprob, "CMS.INITBYPROB", CmsInitbyprobInput),
    (CmsMerge, "CMS.MERGE", CmsMergeInput),
    (CmsQuery, "CMS.QUERY", CmsQueryInput),
    (Command, "COMMAND", CommandInput),
    (CommandCount, "COMMAND COUNT", CommandCountInput),
    (CommandDocs, "COMMAND DOCS", CommandDocsInput),
    (CommandGetkeys, "COMMAND GETKEYS", CommandGetkeysInput),
    (CommandGetkeysandflags, "COMMAND GETKEYSANDFLAGS", CommandGetkeysandflagsInput),
    (CommandInfo, "COMMAND INFO", CommandInfoInput),
    (CommandList, "COMMAND LIST", CommandListInput),
    (ConfigGet, "CONFIG GET", ConfigGetInput),
    (ConfigResetstat, "CONFIG RESETSTAT", ConfigResetstatInput),
    (ConfigRewrite, "CONFIG REWRITE", ConfigRewriteInput),
    (ConfigSet, "CONFIG SET", ConfigSetInput),
    (Copy, "COPY", CopyInput),
    (Dbsize, "DBSIZE", DbsizeInput),
    (Decr, "DECR", DecrInput),
    (Decrby, "DECRBY", DecrbyInput),
    (Del, "DEL", DelInput),
    (Discard, "DISCARD", DiscardInput),
    (Dump, "DUMP", DumpInput),
    (Echo, "ECHO", EchoInput),
    (Eval, "EVAL", EvalInput),
    (EvalRo, "EVAL_RO", EvalRoInput),
    (Evalsha, "EVALSHA", EvalshaInput),
    (EvalshaRo, "EVALSHA_RO", EvalshaRoInput),
    (Exec, "EXEC", ExecInput),
    (Exists, "EXISTS", ExistsInput),
    (Expire, "EXPIRE", ExpireInput),
    (Expireat, "EXPIREAT", ExpireatInput),
    (Expiretime, "EXPIRETIME", ExpiretimeInput),
    (Failover, "FAILOVER", FailoverInput),
    (Fcall, "FCALL", FcallInput),
    (FcallRo, "FCALL_RO", FcallRoInput),
    (Flushall, "FLUSHALL", FlushallInput),
    (Flushdb, "FLUSHDB", FlushdbInput),
    (FtList, "FT._LIST", FtListInput),
    (FtAggregate, "FT.AGGREGATE", FtAggregateInput),
    (FtAliasadd, "FT.ALIASADD", FtAliasaddInput),
    (FtAliasdel, "FT.ALIASDEL", FtAliasdelInput),
    (FtAliasupdate, "FT.ALIASUPDATE", FtAliasupdateInput),
    (FtAlter, "FT.ALTER", FtAlterInput),
    (FtConfigGet, "FT.CONFIG GET", FtConfigGetInput),
    (FtConfigSet, "FT.CONFIG SET", FtConfigSetInput),
    (FtCreate, "FT.CREATE", FtCreateInput),
    (FtCursorDel, "FT.CURSOR DEL", FtCursorDelInput),
    (FtCursorRead, "FT.CURSOR READ", FtCursorReadInput),
    (FtDictadd, "FT.DICTADD", FtDictaddInput),
    (FtDictdel, "FT.DICTDEL", FtDictdelInput),
    (FtDictdump, "FT.DICTDUMP", FtDictdumpInput),
    (FtDropindex, "FT.DROPINDEX", FtDropindexInput),
    (FtExplain, "FT.EXPLAIN", FtExplainInput),
    (FtExplaincli, "FT.EXPLAINCLI", FtExplaincliInput),
    (FtInfo, "FT.INFO", FtInfoInput),
    (FtProfile, "FT.PROFILE", FtProfileInput),
    (FtSearch, "FT.SEARCH", FtSearchInput),
    (FtSpellcheck, "FT.SPELLCHECK", FtSpellcheckInput),
    (FtSugadd, "FT.SUGADD", FtSugaddInput),
    (FtSugdel, "FT.SUGDEL", FtSugdelInput),
    (FtSugget, "FT.SUGGET", FtSuggetInput),
    (FtSuglen, "FT.SUGLEN", FtSuglenInput),
    (FtSyndump, "FT.SYNDUMP", FtSyndumpInput),
    (FtSynupdate, "FT.SYNUPDATE", FtSynupdateInput),
    (FtTagvals, "FT.TAGVALS", FtTagvalsInput),
    (FunctionDelete, "FUNCTION DELETE", FunctionDeleteInput),
    (FunctionDump, "FUNCTION DUMP", FunctionDumpInput),
    (FunctionFlush, "FUNCTION FLUSH", FunctionFlushInput),
    (FunctionKill, "FUNCTION KILL", FunctionKillInput),
    (FunctionList, "FUNCTION LIST", FunctionListInput),
    (FunctionLoad, "FUNCTION LOAD", FunctionLoadInput),
    (FunctionRestore, "FUNCTION RESTORE", FunctionRestoreInput),
    (FunctionStats, "FUNCTION STATS", FunctionStatsInput),
    (Geoadd, "GEOADD", GeoaddInput),
    (Geodist, "GEODIST", GeodistInput),
    (Geohash, "GEOHASH", GeohashInput),
    (Geopos, "GEOPOS", GeoposInput),
    (Georadius, "GEORADIUS", GeoradiusInput),
    (GeoradiusRo, "GEORADIUS_RO", GeoradiusRoInput),
    (Georadiusbymember, "GEORADIUSBYMEMBER", GeoradiusbymemberInput),
    (GeoradiusbymemberRo, "GEORADIUSBYMEMBER_RO", GeoradiusbymemberRoInput),
    (Geosearch, "GEOSEARCH", GeosearchInput),
    (Geosearchstore, "GEOSEARCHSTORE", GeosearchstoreInput),
    (Get, "GET", GetInput),
    (Getdel, "GETDEL", GetdelInput),
    (Getex, "GETEX", GetexInput),
    (Getbit, "GETBIT", GetbitInput),
    (Getrange, "GETRANGE", GetrangeInput),
    (Getset, "GETSET", GetsetInput),
    (Hdel, "HDEL", HdelInput),
    (Hello, "HELLO", HelloInput),
    (Hexists, "HEXISTS", HexistsInput),
    (Hexpire, "HEXPIRE", HexpireInput),
    (Hexpireat, "HEXPIREAT", HexpireatInput),
    (Hexpiretime, "HEXPIRETIME", HexpiretimeInput),
    (Hget, "HGET", HgetInput),
    (Hgetall, "HGETALL", HgetallInput),
    (Hgetdel, "HGETDEL", HgetdelInput),
    (Hgetex, "HGETEX", HgetexInput),
    (Hincrby, "HINCRBY", HincrbyInput),
    (Hincrbyfloat, "HINCRBYFLOAT", HincrbyfloatInput),
    (Hkeys, "HKEYS", HkeysInput),
    (Hlen, "HLEN", HlenInput),
    (Hmget, "HMGET", HmgetInput),
    (Hmset, "HMSET", HmsetInput),
    (Hpersist, "HPERSIST", HpersistInput),
    (Hpexpire, "HPEXPIRE", HpexpireInput),
    (Hpexpireat, "HPEXPIREAT", HpexpireatInput),
    (Hpexpiretime, "HPEXPIRETIME", HpexpiretimeInput),
    (Hpttl, "HPTTL", HpttlInput),
    (Hrandfield, "HRANDFIELD", HrandfieldInput),
    (Hscan, "HSCAN", HscanInput),
    (Hset, "HSET", HsetInput),
    (Hsetex, "HSETEX", HsetexInput),
    (Hsetnx, "HSETNX", HsetnxInput),
    (Hstrlen, "HSTRLEN", HstrlenInput),
    (Httl, "HTTL", HttlInput),
    (Hvals, "HVALS", HvalsInput),
    (Info, "INFO", InfoInput),
    (Incr, "INCR", IncrInput),
    (Incrby, "INCRBY", IncrbyInput),
    (Incrbyfloat, "INCRBYFLOAT", IncrbyfloatInput),
    (JsonArrappend, "JSON.ARRAPPEND", JsonArrappendInput),
    (JsonArrindex, "JSON.ARRINDEX", JsonArrindexInput),
    (JsonArrinsert, "JSON.ARRINSERT", JsonArrinsertInput),
    (JsonArrlen, "JSON.ARRLEN", JsonArrlenInput),
    (JsonArrpop, "JSON.ARRPOP", JsonArrpopInput),
    (JsonArrtrim, "JSON.ARRTRIM", JsonArrtrimInput),
    (JsonClear, "JSON.CLEAR", JsonClearInput),
    (JsonDebug, "JSON.DEBUG", JsonDebugInput),
    (JsonDebugMemory, "JSON.DEBUG MEMORY", JsonDebugMemoryInput),
    (JsonDel, "JSON.DEL", JsonDelInput),
    (JsonForget, "JSON.FORGET", JsonForgetInput),
    (JsonGet, "JSON.GET", JsonGetInput),
    (JsonMerge, "JSON.MERGE", JsonMergeInput),
    (JsonMget, "JSON.MGET", JsonMgetInput),
    (JsonMset, "JSON.MSET", JsonMsetInput),
    (JsonNumincrby, "JSON.NUMINCRBY", JsonNumincrbyInput),
    (JsonNummultby, "JSON.NUMMULTBY", JsonNummultbyInput),
    (JsonObjkeys, "JSON.OBJKEYS", JsonObjkeysInput),
    (JsonObjlen, "JSON.OBJLEN", JsonObjlenInput),
    (JsonResp, "JSON.RESP", JsonRespInput),
    (JsonSet, "JSON.SET", JsonSetInput),
    (JsonStrappend, "JSON.STRAPPEND", JsonStrappendInput),
    (JsonStrlen, "JSON.STRLEN", JsonStrlenInput),
    (JsonToggle, "JSON.TOGGLE", JsonToggleInput),
    (JsonType, "JSON.TYPE", JsonTypeInput),
    (Keys, "KEYS", KeysInput),
    (Lcs, "LCS", LcsInput),
    (Lastsave, "LASTSAVE", LastsaveInput),
    (LatencyDoctor, "LATENCY DOCTOR", LatencyDoctorInput),
    (LatencyGraph, "LATENCY GRAPH", LatencyGraphInput),
    (LatencyHistogram, "LATENCY HISTOGRAM", LatencyHistogramInput),
    (LatencyHistory, "LATENCY HISTORY", LatencyHistoryInput),
    (LatencyLatest, "LATENCY LATEST", LatencyLatestInput),
    (LatencyReset, "LATENCY RESET", LatencyResetInput),
    (Lindex, "LINDEX", LindexInput),
    (Linsert, "LINSERT", LinsertInput),
    (Llen, "LLEN", LlenInput),
    (Lmove, "LMOVE", LmoveInput),
    (Lmpop, "LMPOP", LmpopInput),
    (Lolwut, "LOLWUT", LolwutInput),
    (Lpop, "LPOP", LpopInput),
    (Lpos, "LPOS", LposInput),
    (Lpush, "LPUSH", LpushInput),
    (Lpushx, "LPUSHX", LpushxInput),
    (Lrange, "LRANGE", LrangeInput),
    (Lrem, "LREM", LremInput),
    (Lset, "LSET", LsetInput),
    (Ltrim, "LTRIM", LtrimInput),
    (MemoryDoctor, "MEMORY DOCTOR", MemoryDoctorInput),
    (MemoryMallocStats, "MEMORY MALLOC-STATS", MemoryMallocStatsInput),
    (MemoryPurge, "MEMORY PURGE", MemoryPurgeInput),
    (MemoryStats, "MEMORY STATS", MemoryStatsInput),
    (MemoryUsage, "MEMORY USAGE", MemoryUsageInput),
    (Mget, "MGET", MgetInput),
    (Migrate, "MIGRATE", MigrateInput),
    (ModuleList, "MODULE LIST", ModuleListInput),
    (ModuleLoad, "MODULE LOAD", ModuleLoadInput),
    (ModuleLoadex, "MODULE LOADEX", ModuleLoadexInput),
    (ModuleUnload, "MODULE UNLOAD", ModuleUnloadInput),
    (Monitor, "MONITOR", MonitorInput),
    (r#Move, "MOVE", MoveInput),
    (Mset, "MSET", MsetInput),
    (Msetnx, "MSETNX", MsetnxInput),
    (Multi, "MULTI", MultiInput),
    (ObjectEncoding, "OBJECT ENCODING", ObjectEncodingInput),
    (ObjectFreq, "OBJECT FREQ", ObjectFreqInput),
    (ObjectIdletime, "OBJECT IDLETIME", ObjectIdletimeInput),
    (ObjectRefcount, "OBJECT REFCOUNT", ObjectRefcountInput),
    (Persist, "PERSIST", PersistInput),
    (Pexpire, "PEXPIRE", PexpireInput),
    (Pexpireat, "PEXPIREAT", PexpireatInput),
    (Pexpiretime, "PEXPIRETIME", PexpiretimeInput),
    (Pfadd, "PFADD", PfaddInput),
    (Pfcount, "PFCOUNT", PfcountInput),
    (Pfdebug, "PFDEBUG", PfdebugInput),
    (Pfmerge, "PFMERGE", PfmergeInput),
    (Pfselftest, "PFSELFTEST", PfselftestInput),
    (Ping, "PING", PingInput),
    (Psetex, "PSETEX", PsetexInput),
    (Psubscribe, "PSUBSCRIBE", PsubscribeInput),
    (Psync, "PSYNC", PsyncInput),
    (Pttl, "PTTL", PttlInput),
    (Publish, "PUBLISH", PublishInput),
    (PubsubChannels, "PUBSUB CHANNELS", PubsubChannelsInput),
    (PubsubNumpat, "PUBSUB NUMPAT", PubsubNumpatInput),
    (PubsubNumsub, "PUBSUB NUMSUB", PubsubNumsubInput),
    (PubsubShardchannels, "PUBSUB SHARDCHANNELS", PubsubShardchannelsInput),
    (PubsubShardnumsub, "PUBSUB SHARDNUMSUB", PubsubShardnumsubInput),
    (Punsubscribe, "PUNSUBSCRIBE", PunsubscribeInput),
    (Quit, "QUIT", QuitInput),
    (Randomkey, "RANDOMKEY", RandomkeyInput),
    (Readonly, "READONLY", ReadonlyInput),
    (Readwrite, "READWRITE", ReadwriteInput),
    (Rename, "RENAME", RenameInput),
    (Renamenx, "RENAMENX", RenamenxInput),
    (Replconf, "REPLCONF", ReplconfInput),
    (Replicaof, "REPLICAOF", ReplicaofInput),
    (Reset, "RESET", ResetInput),
    (Restore, "RESTORE", RestoreInput),
    (RestoreAsking, "RESTORE-ASKING", RestoreAskingInput),
    (Role, "ROLE", RoleInput),
    (Rpop, "RPOP", RpopInput),
    (Rpoplpush, "RPOPLPUSH", RpoplpushInput),
    (Rpush, "RPUSH", RpushInput),
    (Rpushx, "RPUSHX", RpushxInput),
    (Sadd, "SADD", SaddInput),
    (Save, "SAVE", SaveInput),
    (Scan, "SCAN", ScanInput),
    (Scard, "SCARD", ScardInput),
    (ScriptDebug, "SCRIPT DEBUG", ScriptDebugInput),
    (ScriptExists, "SCRIPT EXISTS", ScriptExistsInput),
    (ScriptFlush, "SCRIPT FLUSH", ScriptFlushInput),
    (ScriptKill, "SCRIPT KILL", ScriptKillInput),
    (ScriptLoad, "SCRIPT LOAD", ScriptLoadInput),
    (Sdiff, "SDIFF", SdiffInput),
    (Sdiffstore, "SDIFFSTORE", SdiffstoreInput),
    (Select, "SELECT", SelectInput),
    (Set, "SET", SetInput),
    (Setex, "SETEX", SetexInput),
    (Setnx, "SETNX", SetnxInput),
    (Setbit, "SETBIT", SetbitInput),
    (Setrange, "SETRANGE", SetrangeInput),
    (Shutdown, "SHUTDOWN", ShutdownInput),
    (Sinter, "SINTER", SinterInput),
    (Sintercard, "SINTERCARD", SintercardInput),
    (Sinterstore, "SINTERSTORE", SinterstoreInput),
    (Sismember, "SISMEMBER", SismemberInput),
    #[allow(deprecated)]
    (Slaveof, "SLAVEOF", SlaveofCompatInput),
    (SlowlogGet, "SLOWLOG GET", SlowlogGetInput),
    (SlowlogLen, "SLOWLOG LEN", SlowlogLenInput),
    (SlowlogReset, "SLOWLOG RESET", SlowlogResetInput),
    (Smembers, "SMEMBERS", SmembersInput),
    (Smismember, "SMISMEMBER", SmismemberInput),
    (Smove, "SMOVE", SmoveInput),
    (Sort, "SORT", SortInput),
    (SortRo, "SORT_RO", SortRoInput),
    (Spop, "SPOP", SpopInput),
    (Spublish, "SPUBLISH", SpublishInput),
    (Srandmember, "SRANDMEMBER", SrandmemberInput),
    (Srem, "SREM", SremInput),
    (Sscan, "SSCAN", SscanInput),
    (Ssubscribe, "SSUBSCRIBE", SsubscribeInput),
    (Strlen, "STRLEN", StrlenInput),
    (Subscribe, "SUBSCRIBE", SubscribeInput),
    (Substr, "SUBSTR", SubstrInput),
    (Sunion, "SUNION", SunionInput),
    (Sunionstore, "SUNIONSTORE", SunionstoreInput),
    (Sunsubscribe, "SUNSUBSCRIBE", SunsubscribeInput),
    (Swapdb, "SWAPDB", SwapdbInput),
    (Sync, "SYNC", SyncInput),
    (TdigestAdd, "TDIGEST.ADD", TdigestAddInput),
    (TdigestByrank, "TDIGEST.BYRANK", TdigestByrankInput),
    (TdigestByrevrank, "TDIGEST.BYREVRANK", TdigestByrevrankInput),
    (TdigestCdf, "TDIGEST.CDF", TdigestCdfInput),
    (TdigestCreate, "TDIGEST.CREATE", TdigestCreateInput),
    (TdigestInfo, "TDIGEST.INFO", TdigestInfoInput),
    (TdigestMax, "TDIGEST.MAX", TdigestMaxInput),
    (TdigestMerge, "TDIGEST.MERGE", TdigestMergeInput),
    (TdigestMin, "TDIGEST.MIN", TdigestMinInput),
    (TdigestQuantile, "TDIGEST.QUANTILE", TdigestQuantileInput),
    (TdigestRank, "TDIGEST.RANK", TdigestRankInput),
    (TdigestReset, "TDIGEST.RESET", TdigestResetInput),
    (TdigestRevrank, "TDIGEST.REVRANK", TdigestRevrankInput),
    (TdigestTrimmedMean, "TDIGEST.TRIMMED_MEAN", TdigestTrimmedMeanInput),
    (Time, "TIME", TimeInput),
    (TopkAdd, "TOPK.ADD", TopkAddInput),
    (TopkCount, "TOPK.COUNT", TopkCountInput),
    (TopkIncrby, "TOPK.INCRBY", TopkIncrbyInput),
    (TopkInfo, "TOPK.INFO", TopkInfoInput),
    (TopkList, "TOPK.LIST", TopkListInput),
    (TopkQuery, "TOPK.QUERY", TopkQueryInput),
    (TopkReserve, "TOPK.RESERVE", TopkReserveInput),
    (Touch, "TOUCH", TouchInput),
    (TsAdd, "TS.ADD", TsAddInput),
    (TsAlter, "TS.ALTER", TsAlterInput),
    (TsCreate, "TS.CREATE", TsCreateInput),
    (TsCreaterule, "TS.CREATERULE", TsCreateruleInput),
    (TsDecrby, "TS.DECRBY", TsDecrbyInput),
    (TsDel, "TS.DEL", TsDelInput),
    (TsDeleterule, "TS.DELETERULE", TsDeleteruleInput),
    (TsGet, "TS.GET", TsGetInput),
    (TsIncrby, "TS.INCRBY", TsIncrbyInput),
    (TsInfo, "TS.INFO", TsInfoInput),
    (TsMadd, "TS.MADD", TsMaddInput),
    (TsMget, "TS.MGET", TsMgetInput),
    (TsMrange, "TS.MRANGE", TsMrangeInput),
    (TsMrevrange, "TS.MREVRANGE", TsMrevrangeInput),
    (TsQueryindex, "TS.QUERYINDEX", TsQueryindexInput),
    (TsRange, "TS.RANGE", TsRangeInput),
    (TsRevrange, "TS.REVRANGE", TsRevrangeInput),
    (Ttl, "TTL", TtlInput),
    (r#Type, "TYPE", TypeInput),
    (Unlink, "UNLINK", UnlinkInput),
    (Unsubscribe, "UNSUBSCRIBE", UnsubscribeInput),
    (Unwatch, "UNWATCH", UnwatchInput),
    (Vadd, "VADD", VaddInput),
    (Vcard, "VCARD", VcardInput),
    (Vdim, "VDIM", VdimInput),
    (Vemb, "VEMB", VembInput),
    (Vgetattr, "VGETATTR", VgetattrInput),
    (Vinfo, "VINFO", VinfoInput),
    (Vlinks, "VLINKS", VlinksInput),
    (Vrandmember, "VRANDMEMBER", VrandmemberInput),
    (Vrem, "VREM", VremInput),
    (Vsetattr, "VSETATTR", VsetattrInput),
    (Vsim, "VSIM", VsimInput),
    (Wait, "WAIT", WaitInput),
    (Waitaof, "WAITAOF", WaitaofInput),
    (Watch, "WATCH", WatchInput),
    (Xack, "XACK", XackInput),
    (Xadd, "XADD", XaddInput),
    (Xautoclaim, "XAUTOCLAIM", XautoclaimInput),
    (Xclaim, "XCLAIM", XclaimInput),
    (Xdel, "XDEL", XdelInput),
    (XgroupCreate, "XGROUP CREATE", XgroupCreateInput),
    (XgroupCreateconsumer, "XGROUP CREATECONSUMER", XgroupCreateconsumerInput),
    (XgroupDelconsumer, "XGROUP DELCONSUMER", XgroupDelconsumerInput),
    (XgroupDestroy, "XGROUP DESTROY", XgroupDestroyInput),
    (XgroupSetid, "XGROUP SETID", XgroupSetidInput),
    (XinfoConsumers, "XINFO CONSUMERS", XinfoConsumersInput),
    (XinfoGroups, "XINFO GROUPS", XinfoGroupsInput),
    (XinfoStream, "XINFO STREAM", XinfoStreamInput),
    (Xlen, "XLEN", XlenInput),
    (Xpending, "XPENDING", XpendingInput),
    (Xrange, "XRANGE", XrangeInput),
    (Xread, "XREAD", XreadInput),
    (Xreadgroup, "XREADGROUP", XreadgroupInput),
    (Xrevrange, "XREVRANGE", XrevrangeInput),
    (Xsetid, "XSETID", XsetidInput),
    (Xtrim, "XTRIM", XtrimInput),
    (Zadd, "ZADD", ZaddInput),
    (Zcard, "ZCARD", ZcardInput),
    (Zcount, "ZCOUNT", ZcountInput),
    (Zdiff, "ZDIFF", ZdiffInput),
    (Zdiffstore, "ZDIFFSTORE", ZdiffstoreInput),
    (Zincrby, "ZINCRBY", ZincrbyInput),
    (Zinter, "ZINTER", ZinterInput),
    (Zintercard, "ZINTERCARD", ZintercardInput),
    (Zinterstore, "ZINTERSTORE", ZinterstoreInput),
    (Zlexcount, "ZLEXCOUNT", ZlexcountInput),
    (Zmpop, "ZMPOP", ZmpopInput),
    (Zmscore, "ZMSCORE", ZmscoreInput),
    (Zpopmax, "ZPOPMAX", ZpopmaxInput),
    (Zpopmin, "ZPOPMIN", ZpopminInput),
    (Zrandmember, "ZRANDMEMBER", ZrandmemberInput),
    (Zrange, "ZRANGE", ZrangeInput),
    (Zrangebylex, "ZRANGEBYLEX", ZrangebylexInput),
    (Zrangebyscore, "ZRANGEBYSCORE", ZrangebyscoreInput),
    (Zrangestore, "ZRANGESTORE", ZrangestoreInput),
    (Zrank, "ZRANK", ZrankInput),
    (Zrem, "ZREM", ZremInput),
    (Zremrangebylex, "ZREMRANGEBYLEX", ZremrangebylexInput),
    (Zremrangebyrank, "ZREMRANGEBYRANK", ZremrangebyrankInput),
    (Zremrangebyscore, "ZREMRANGEBYSCORE", ZremrangebyscoreInput),
    (Zrevrange, "ZREVRANGE", ZrevrangeInput),
    (Zrevrangebylex, "ZREVRANGEBYLEX", ZrevrangebylexInput),
    (Zrevrangebyscore, "ZREVRANGEBYSCORE", ZrevrangebyscoreInput),
    (Zrevrank, "ZREVRANK", ZrevrankInput),
    (Zscan, "ZSCAN", ZscanInput),
    (Zscore, "ZSCORE", ZscoreInput),
    (Zunion, "ZUNION", ZunionInput),
    (Zunionstore, "ZUNIONSTORE", ZunionstoreInput),
    (RawCommand, "RAW_COMMAND", RawCommandInput),
    (RawCommandReadOnly, "RAW_COMMAND_READ_ONLY", RawCommandReadOnlyInput),

    // Special cases with noinput attribute
    // #[noinput]
    // (AclLogReset, "ACL LOGRESET", AclLogResetInput),
    // #[noinput]
    // (AclSetuserRules, "ACL SETUSER RULES", AclSetuserRulesInput),
    // #[noinput]
    // (Arg, "ARG", ArgInput),
    // #[noinput]
    // (ArgsIter, "ARGS ITER", ArgsIterInput),
    // #[noinput]
    // (ClientReply, "CLIENT REPLY", ClientReplyInput),
    // #[noinput]
    // (Command, "COMMAND", CommandInput),
    // #[noinput]
    // (CursorArg, "CURSOR ARG", CursorArgInput),
    // #[noinput]
    // (ExecAsync, "EXEC ASYNC", ExecAsyncInput),
    // #[noinput]
    // (Execute, "EXECUTE", ExecuteInput),
    // #[noinput]
    // (GetPackedCommand, "GETPACKEDCOMMAND", GetPackedCommandInput),
    // #[noinput]
    // (Hgetall, "HGETALL", HgetallInput),
    // #[noinput]
    // (Hpexpiretime, "HPEXPIRETIME", HpexpiretimeInput),
    // #[noinput]
    // (Hsetex, "HSETEX", HsetexInput),
    // #[noinput]
    // (InScanMode, "INSCANMODE", InScanModeInput),
    // #[noinput]
    // (InvokeScript, "INVOKESCRIPT", InvokeScriptInput),
    // #[noinput]
    // (IsNoResponse, "ISNORESP", IsNoResponseInput),
    // #[noinput]
    // (Iter, "ITER", IterInput),
    // #[noinput]
    // (IterAsync, "ITERASYNC", IterAsyncInput),
    // #[noinput]
    // (KeyType, "KEYTYPE", KeyTypeInput),
    // #[noinput]
    // (LinsertAfter, "LINSERTAFTER", LinsertAfterInput),
    // #[noinput]
    // (LinsertBefore, "LINSERTBEFORE", LinsertBeforeInput),
    // #[noinput]
    // (New, "NEW", NewInput),
    // #[noinput]
    // (PingMessage, "PING MESSAGE", PingMessageInput),
    // #[noinput]
    // (Query, "QUERY", QueryInput),
    // #[noinput]
    // (QueryAsync, "QUERY ASYNC", QueryAsyncInput),
    // #[noinput]
    // (SetNoResponse, "SETNORESP", SetNoResponseInput),
    // #[noinput]
    // (SetOptions, "SETOPTIONS", SetOptionsInput),
    // #[noinput]
    // (SrandmemberMultiple, "SRANDMEMBERS", SrandmemberMultipleInput),
    // #[noinput]
    // (TopkList, "TOPK.LIST", TopkListInput)
}

impl FromStr for RedisApi {
    type Err = EpError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, ToSchema, JsonSchema)]
pub enum RedisDataType {
    // ============ Core Redis Types ============
    /// Missing data
    None,

    /// Simple key-value pair. Stores binary-safe strings up to 512MB.
    /// Use cases: caching, counters, session storage
    String,

    /// Ordered collection of strings. Supports push/pop from both ends.
    /// Use cases: queues, activity feeds, recent items lists
    List,

    /// Unordered collection of unique strings. Supports set operations (union, intersection).
    /// Use cases: tags, unique visitors, relationships
    Set,

    /// Sorted set - collection of unique strings ordered by score (float64).
    /// Use cases: leaderboards, priority queues, time-series indexes
    ZSet,

    /// Collection of field-value pairs (like a nested object or struct).
    /// Use cases: user profiles, product attributes, configurations
    Hash,

    /// Append-only log with consumer groups. Supports time-based queries.
    /// Use cases: event sourcing, message queues, activity logs
    Stream,

    // ============ String-Encoded Types ============
    /// Bit array operations on strings. Supports bit-level get/set/count.
    /// Use cases: feature flags, user permissions, daily active users tracking
    Bitmap,

    /// Efficient integer array stored in strings. Supports signed/unsigned integers of various sizes.
    /// Use cases: storing multiple counters efficiently, compact integer arrays
    Bitfield,

    /// Probabilistic cardinality estimator using 12KB per key.
    /// Use cases: unique visitor counting, distinct element estimation
    HyperLogLog,

    // ============ ZSet-Encoded Types ============
    /// Latitude/longitude coordinates stored as sorted sets with geohash scores.
    /// Use cases: location-based services, proximity searches, geographic queries
    Geospatial,

    // ============ RedisJSON Module ============
    /// Native JSON document storage with path-based operations.
    /// Use cases: complex nested data, document databases, API responses
    JSON,

    // ============ RedisBloom Module ============
    /// Probabilistic set membership test. False positives possible, no false negatives.
    /// Use cases: cache filtering, duplicate detection, URL filtering
    BloomFilter,

    /// Alternative to Bloom filter supporting deletions with similar space efficiency.
    /// Use cases: same as Bloom filter but when removal is needed
    CuckooFilter,

    /// Probabilistic frequency counter for streaming data.
    /// Use cases: finding most frequent items, heavy hitters detection
    CountMinSketch,

    /// Tracks top K most frequent items in a stream.
    /// Use cases: trending topics, popular products, most active users
    TopK,

    /// Probabilistic data structure for estimating quantiles (percentiles).
    /// Use cases: latency percentiles (p50, p95, p99), distribution analysis
    TDigest,

    // ============ RedisTimeSeries Module ============
    /// Time-series data with downsampling and aggregation support.
    /// Use cases: metrics, IoT sensor data, financial tick data
    TimeSeries,

    // ============ RedisGraph Module ============
    /// Graph database using Cypher query language (deprecated in favor of FalkorDB/Memgraph).
    /// Use cases: social networks, recommendation engines, knowledge graphs
    Graph,

    // ============ RediSearch Module ============
    /// Full-text search index with aggregations and filtering.
    /// Use cases: product catalogs, log search, autocomplete
    SearchIndex,

    /// Document indexed by RediSearch with searchable fields.
    /// Use cases: searchable content, indexed records
    SearchDocument,

    // ============ RedisAI Module ============
    /// Multi-dimensional array for ML inference (FLOAT, DOUBLE, INT8, INT16, INT32, INT64).
    /// Use cases: feature vectors, embeddings, model inputs/outputs
    Tensor,

    /// Pre-trained ML model (TensorFlow, PyTorch, ONNX, TensorFlow Lite).
    /// Use cases: real-time inference, model serving, edge AI
    Model,

    /// TorchScript for PyTorch model execution.
    /// Use cases: custom PyTorch inference pipelines
    Script,

    // ============ RedisGears Module ============
    /// Python function executed on Redis data with MapReduce capabilities.
    /// Use cases: data transformation, complex aggregations, ETL pipelines
    GearsFunction,

    // ============ Redis Functions (Core 7.0+) ============
    /// Lua or JavaScript function stored in Redis for server-side execution.
    /// Use cases: atomic operations, complex transactions, custom commands
    Function,

    /// Collection of related functions loaded as a unit.
    /// Use cases: organizing related business logic, versioned function deployments
    Library,
}

impl Display for RedisDataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::None => "none",
            Self::String => "string",
            Self::List => "list",
            Self::Set => "set",
            Self::ZSet => "zset",
            Self::Hash => "hash",
            Self::Stream => "stream",
            Self::Bitmap => "string",
            Self::Bitfield => "string",
            Self::HyperLogLog => "string",
            Self::Geospatial => "zset",
            Self::JSON => "ReJSON-RL",
            Self::BloomFilter => "MBbloom--",
            Self::CuckooFilter => "MBbloomCF",
            Self::CountMinSketch => "CMSk-CT-",
            Self::TopK => "TopK-TYPE",
            Self::TDigest => "TDIS-TYPE",
            Self::TimeSeries => "TSDB-TYPE",
            Self::Graph => "graphdata",
            Self::SearchIndex => "ft_index0",
            Self::SearchDocument => "ft_invidx",
            Self::Tensor => "AI_TENSOR",
            Self::Model => "AI_MODEL",
            Self::Script => "AI_SCRIPT",
            Self::GearsFunction => "GEARS_FUNCTION",
            Self::Function => "function",
            Self::Library => "library",
        };
        write!(f, "{}", s)
    }
}

impl FromStr for RedisDataType {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "none" => Ok(Self::None),
            "string" => Ok(Self::String),
            "list" => Ok(Self::List),
            "set" => Ok(Self::Set),
            "zset" => Ok(Self::ZSet),
            "hash" => Ok(Self::Hash),
            "stream" => Ok(Self::Stream),
            "ReJSON-RL" => Ok(Self::JSON),
            "MBbloom--" => Ok(Self::BloomFilter),
            "MBbloomCF" => Ok(Self::CuckooFilter),
            "CMSk-CT-" => Ok(Self::CountMinSketch),
            "TopK-TYPE" => Ok(Self::TopK),
            "TDIS-TYPE" => Ok(Self::TDigest),
            "TSDB-TYPE" => Ok(Self::TimeSeries),
            "graphdata" => Ok(Self::Graph),
            "ft_index0" => Ok(Self::SearchIndex),
            "ft_invidx" => Ok(Self::SearchDocument),
            "AI_TENSOR" => Ok(Self::Tensor),
            "AI_MODEL" => Ok(Self::Model),
            "AI_SCRIPT" => Ok(Self::Script),
            "GEARS_FUNCTION" => Ok(Self::GearsFunction),
            "function" => Ok(Self::Function),
            "library" => Ok(Self::Library),
            _ => Err(EpError::parse(format!("Unknown Redis type: {}", s))),
        }
    }
}

impl TryFrom<String> for RedisDataType {
    type Error = EpError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        Self::from_str(&s)
    }
}

impl TryFrom<&str> for RedisDataType {
    type Error = EpError;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_str(s)
    }
}

impl RedisDataType {
    pub async fn detect_type(
        redis_ep: &RedisEp,
        endpoint: &EndpointCacheUuid,
        key: String,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Self> {
        let type_command = RedisBytes::from(TypeInputBuilder::default().key(key.into()).build().map_err(EpError::api)?.command());

        TypeOutput::decode(&redis_ep.raw_bytes(endpoint, type_command, settings, telemetry_wrapper).await?).map(|r| r.key_type())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ReqType;

    #[test]
    fn test_redis_api_request_type() {
        // Test parsing from string and getting request type
        assert_eq!(RedisApi::try_from("GET").unwrap().request_type(), ReqType::Read);
        assert_eq!(RedisApi::try_from("SET").unwrap().request_type(), ReqType::Write);
    }

    #[test]
    fn test_redis_api_byte_classifier_preserves_case_insensitive_behavior() {
        assert_eq!(RedisApi::try_from_case_insensitive_bytes(b"GET").unwrap(), RedisApi::Get);
        assert_eq!(RedisApi::try_from_case_insensitive_bytes(b"set").unwrap(), RedisApi::Set);
        assert_eq!(RedisApi::try_from_case_insensitive_bytes(b"PiNg").unwrap(), RedisApi::Ping);
        assert!(RedisApi::try_from_case_insensitive_bytes(b"\xff").is_err());
    }

    #[test]
    fn test_redis_api_command_words_bytes_prefers_two_word_commands() {
        assert_eq!(
            RedisApi::try_from_command_words_bytes(b"CLIENT", Some(b"SETINFO")).unwrap(),
            (RedisApi::ClientSetinfo, 2)
        );
        assert_eq!(
            RedisApi::try_from_command_words_bytes(b"command", Some(b"docs")).unwrap(),
            (RedisApi::CommandDocs, 2)
        );
        assert_eq!(RedisApi::try_from_command_words_bytes(b"COMMAND", None).unwrap(), (RedisApi::Command, 1));
    }

    #[test]
    fn test_redis_api_safe_for_direct_lane_pool() {
        assert!(RedisApi::Get.safe());
        assert!(RedisApi::Set.safe());
        assert!(!RedisApi::Auth.safe());
        assert!(!RedisApi::ClientSetname.safe());
        assert!(!RedisApi::Watch.safe());
        assert!(!RedisApi::Xread.safe());
        assert!(GetInput::safe());
        assert!(!AuthInput::safe());
    }
}
