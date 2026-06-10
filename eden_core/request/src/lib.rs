#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Request Types
//!
//! Request structures and command types for Eve operations.
//!
//! ## Overview
//!
//! Defines the request format for all Eden operations:
//! - Single commands
//! - Multi-command batches (sync/async/transactional)
//! - Authentication data
//! - Request metadata
//!
//! ## Core Types
//!
//! - [`Request`] - Top-level request with auth and metadata
//! - [`EdenRequest`] - Single or multi-command request
//! - [`EdenCommand`] - Individual operation command
//! - [`EdenMultiCommand`] - Batch operation mode
//!
//! ## Request Flow
//!
//! ```text
//! Client Request
//!       ↓
//! [Request with auth]
//!       ↓
//! [EdenRequest: Cmd/Mcmd]
//!       ↓
//! [EdenCommand: ReqEp/ReqPath/etc.]
//! ```

use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use error::RequestError;
use format::{EdenNodeUuid, EndpointId, hashtype::HashType, timestamp::Timestamp};
use serde::{Deserialize, Serialize};
use std::time::Duration;

pub mod headers;

pub use headers::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct InternalLlmSettings {
    pub provider: String,
    pub model: String,
    #[serde(default)]
    pub api_key: Option<String>,
    #[serde(default)]
    pub base_url: Option<String>,
    #[serde(default)]
    pub system_prompt: Option<String>,
    #[serde(default)]
    pub temperature: Option<f32>,
    #[serde(default)]
    pub max_tokens: Option<u32>,
}

pub struct ServerData {
    pub engine_url: String,
    pub public_key: EdenNodeUuid,
    pub new_org_token: Option<String>,
    pub tools_service_timeout_secs: Option<u64>,
    pub internal_llm: Option<InternalLlmSettings>,
}

impl ServerData {
    pub fn tools_service_timeout(&self) -> Option<Duration> {
        self.tools_service_timeout_secs.and_then(|secs| match secs {
            0 => None,
            value => Some(Duration::from_secs(value)),
        })
    }

    pub fn internal_llm(&self) -> Option<&InternalLlmSettings> {
        self.internal_llm.as_ref()
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug)]
pub struct Request {
    pub usr: String,      // username
    pub ath: String,      // user_auth
    pub unx: Timestamp,   // unix timestamp
    pub end: EndpointId,  // endpoint name
    pub oph: HashType,    // operation hash
    pub ops: EdenRequest, // transaction type
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub struct EdenTxRequest {
    pub auth: ReqAuth,
    pub req: EdenRequest,
    pub db_kind: String,
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum EdenRequest {
    Cmd(EdenCommand),       // single command
    Mcmd(EdenMultiCommand), // multi-command
}

impl EdenRequest {
    pub fn encode_request(&self) -> Result<Vec<u8>, EpError> {
        borsh::to_vec(self).map_err(|_| EpError::Request(RequestError::FailedToEncodeRequest))
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum EdenMultiCommand {
    Sync(Vec<EdenCommand>),  // ordered requests (some can fail)
    Async(Vec<EdenCommand>), // async request (some can fail)
    Tx(Vec<EdenCommand>),    // todo sync requests (w/ rollback)
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum EdenCommand {
    ReqEp(ReqEndpoint),
    ReqPath(ReqPath),
    ReqUser(ReqUser),
    ReqOrg(ReqOrganization),
    Tx(Vec<u8>),
    MultiTx(Vec<u8>),
    ReqTemp(ReqTemplate),
}

impl EdenCommand {
    pub fn kind(&self) -> String {
        match self {
            Self::ReqEp(req) => format!("ep:{}", req.kind()),
            Self::ReqTemp(req) => format!("template:{}", req.kind()),
            Self::ReqPath(req) => format!("path:{}", req.kind()),
            Self::ReqUser(req) => format!("user:{}", req.kind()),
            Self::ReqOrg(req) => format!("org:{}", req.kind()),
            Self::Tx(_) => "tx".to_string(),
            Self::MultiTx(_) => "multi_tx".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqTemplate {
    CreateTemplate(Vec<u8>), // request template
    RunTemplate(Vec<u8>),    // run template
    GetTemplate(Vec<u8>),    // get template info
    RemoveTemplate(Vec<u8>), // delete template
}
impl ReqTemplate {
    pub fn kind(&self) -> String {
        match self {
            Self::CreateTemplate(_) => "create_template".to_string(),
            Self::RunTemplate(_) => "run_template".to_string(),
            Self::GetTemplate(_) => "get_template".to_string(),
            Self::RemoveTemplate(_) => "remove_template".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqEndpoint {
    Connect(Vec<u8>),    // create a new endpoint connection
    Update(Vec<u8>),     // update endpoint connection
    Disconnect(Vec<u8>), // disconnect from endpoint
    Rename(Vec<u8>),     // todo rename endpoint
    Query(Vec<u8>),      // query (read) endpoint
    Execute(Vec<u8>),    // execute (write) endpoint
    Get(Vec<u8>),        // get info
    Test(),              // test connection
}

impl ReqEndpoint {
    pub fn kind(&self) -> String {
        match self {
            Self::Connect(_) => "connect".to_string(),
            Self::Update(_) => "update".to_string(),
            Self::Disconnect(_) => "disconnect".to_string(),
            Self::Rename(_) => "rename".to_string(),
            Self::Query(_) => "query".to_string(),
            Self::Execute(_) => "execute".to_string(),
            Self::Get(_) => "get".to_string(),
            Self::Test() => "test".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqPath {
    InsertDir(Vec<u8>), // insert new directory
    MoveDir(Vec<u8>),   // move dir to new path
    MoveEp(Vec<u8>),    // move ep to new path
    GetPath(Vec<u8>),   // get endpoint path
    GetDir(Vec<u8>),    // get directory data
    DeleteDir(Vec<u8>), // can only delete empty directory
    Test(),             // test connection
                        //* endpoints are deleted by disconnecting `ReqEndpoint::Disconnect()`
}

impl ReqPath {
    fn kind(&self) -> String {
        match self {
            Self::InsertDir(_) => "insert_dir".to_string(),
            Self::MoveDir(_) => "move_dir".to_string(),
            Self::MoveEp(_) => "move_endpoint".to_string(),
            Self::GetPath(_) => "get_path".to_string(),
            Self::GetDir(_) => "get_dir".to_string(),
            Self::DeleteDir(_) => "delete_dir".to_string(),
            Self::Test() => "test".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqOrganization {
    Insert(Vec<u8>), // add new organization
    Get(),           // get organization data
    Update(Vec<u8>), // update org data
    Delete(),        // remove organization
}

impl ReqOrganization {
    fn kind(&self) -> String {
        match self {
            Self::Insert(_) => "insert".to_string(),
            Self::Get() => "get".to_string(),
            Self::Update(_) => "update".to_string(),
            Self::Delete() => "delete".to_string(),
        }
    }
}

/// User Request Format
#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqUser {
    Insert(Vec<u8>), // add new user (to org)
    Get(Vec<u8>),    // get user data
    Update(Vec<u8>), // update user data
    Delete(Vec<u8>), // remove user
    SetOrgAdmin(Vec<u8>),
    SetOrgWriter(Vec<u8>),
    SetOrgReader(Vec<u8>),
    SetOrgNone(Vec<u8>),
    SetEpAdmin(Vec<u8>),
    SetEpWriter(Vec<u8>),
    SetEpReader(Vec<u8>),
    RemoveEpAuth(Vec<u8>),
}

impl ReqUser {
    fn kind(&self) -> String {
        match self {
            Self::Insert(_) => "insert".to_string(),
            Self::Get(_) => "get".to_string(),
            Self::Update(_) => "update".to_string(),
            Self::Delete(_) => "delete".to_string(),
            Self::SetOrgAdmin(_) => "set_org_admin".to_string(),
            Self::SetOrgWriter(_) => "set_org_writer".to_string(),
            Self::SetOrgReader(_) => "set_org_reader".to_string(),
            Self::SetOrgNone(_) => "set_org_none".to_string(),
            Self::SetEpAdmin(_) => "set_ep_admin".to_string(),
            Self::SetEpWriter(_) => "set_ep_writer".to_string(),
            Self::SetEpReader(_) => "set_ep_reader".to_string(),
            Self::RemoveEpAuth(_) => "remove_ep_auth".to_string(),
        }
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone)]
pub enum ReqAuth {
    //* insert
    OrgInsertUserAuth(Vec<u8>),  // insert organization user with auth
    DirInsertUserAuth(Vec<u8>),  // insert directory user with auth
    EpInsertUserAuth(Vec<u8>),   // insert endpoint user with auth
    OrgInsertBearerKey(Vec<u8>), // insert organization API KEY with auth
    DirInsertBearerKey(Vec<u8>), // insert directory API KEY with auth
    EpInsertBearerKey(Vec<u8>),  // insert endpoint API KEY with auth
    //* update
    OrgUpdateUserAuth(Vec<u8>),  // insert organization user with auth
    DirUpdateUserAuth(Vec<u8>),  // insert directory user with auth
    EpUpdateUserAuth(Vec<u8>),   // insert endpoint user with auth
    OrgUpdateBearerKey(Vec<u8>), // insert organization API KEY with auth
    DirUpdateBearerKey(Vec<u8>), // insert directory API KEY with auth
    EpUpdateBearerKey(Vec<u8>),  // insert endpoint API KEY with auth
    //* remove
    OrgDeleteUserAuth(Vec<u8>),  // insert organization user with auth
    DirDeleteUserAuth(Vec<u8>),  // insert directory user with auth
    EpDeleteUserAuth(Vec<u8>),   // insert endpoint user with auth
    OrgDeleteBearerKey(Vec<u8>), // insert organization API KEY with auth
    DirDeleteBearerKey(Vec<u8>), // insert directory API KEY with auth
    EpDeleteBearerKey(Vec<u8>),  // insert endpoint API KEY with auth
}
