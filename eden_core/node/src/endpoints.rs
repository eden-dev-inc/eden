use std::collections::{HashMap, HashSet};
use std::fmt;

use crate::PubKey;
use db::{ConnectionParameters, DB, DBKind};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_info, log_trace};
use error::{DBError, ResultDB};
use format::id::EndpointUrl;
use format::{EndpointId, hashtype::HashType, nonce::Nonce};
use function_name::named;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct Endpoint {
    pub node: PubKey, // node's public key (relay)
    pub endpoints: HashMap<EndpointId, EndpointInfo>,
}

// Example usage:
// <postgres, ...>
// <users, ...>
// endpoint.get_mut(uuid).add_node(node)
impl Endpoint {
    /// returns a mutable reference to EndpointInfo
    #[named]
    pub fn get_mut(&mut self, name: &str) -> Option<&mut EndpointInfo> {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.endpoints.get_mut(&EndpointId::from(name)) {
            Some(info) => {
                log_trace!(_ctx, "Passed mutable pointer to endpoint", audience = LogAudience::Internal, name = name);
                Some(info)
            }
            None => {
                log_trace!(_ctx, "No endpoint at name", audience = LogAudience::Internal, name = name);
                None
            }
        }
    }

    /// rename endpoint
    #[named]
    pub fn rename(&mut self, old_name: &EndpointId, new_name: &EndpointId, node: &PubKey) -> ResultDB<()> {
        let _ctx = ctx_with_trace!().with_feature("node");

        if node != &self.node {
            log_debug!(
                _ctx,
                "Rename for different node, ignoring",
                audience = LogAudience::Internal,
                old_name = old_name.to_string(),
                new_name = new_name.to_string(),
                target_node = node.to_string(),
                this_node = self.node.to_string()
            );
            return Err(DBError::Ignored);
        }
        log_debug!(
            _ctx.clone(),
            "Renaming endpoint",
            audience = LogAudience::Internal,
            old_name = old_name.to_string(),
            new_name = new_name.to_string(),
            node = node.to_string()
        );

        if !self.endpoints.contains_key(old_name) {
            let err_msg = format!("unknown endpoint {}", old_name);
            log_debug!(_ctx, "Unknown endpoint", audience = LogAudience::Internal, error = &err_msg);
            return Err(DBError::Command(err_msg));
        }
        if self.endpoints.contains_key(new_name) {
            let err_msg = format!("endpoint {} already exists", new_name);
            log_debug!(_ctx, "Endpoint already exists", audience = LogAudience::Internal, error = &err_msg);
            return Err(DBError::Command(err_msg));
        }
        let mut ei = self.endpoints.remove(old_name).unwrap_or_default();
        ei.name = new_name.to_owned();
        _ = self.endpoints.insert(new_name.to_owned(), ei);
        Ok(())
    }

    /// add new endpoint
    /// if there's no endpoint with this name, create a new (disconnected) endpoint and return it
    /// if an endpoint already exists, just return the endpoint as it is
    pub fn add(&mut self, conn_params: &ConnectionParameters) -> &mut EndpointInfo {
        let name = &conn_params.name;
        self.endpoints
            .entry(EndpointId::from(name.as_str()))
            .and_modify(|ei| ei.update(conn_params))
            .or_insert(EndpointInfo::new(conn_params, &self.node))
    }

    /// replace endpointinfo
    #[named]
    pub fn replace(&mut self, name: &str, ei: EndpointInfo) -> Option<EndpointInfo> {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.endpoints.insert(EndpointId::from(name), ei) {
            Some(endpoint) => {
                log_trace!(
                    _ctx,
                    "Updated endpoint",
                    audience = LogAudience::Internal,
                    name = name,
                    endpoint = endpoint.to_string()
                );
                Some(endpoint)
            }
            None => {
                log_trace!(_ctx, "Was not able to update the endpoint", audience = LogAudience::Internal, name = name);
                None
            }
        }
    }

    /// remove endpoint
    #[named]
    pub fn remove(&mut self, name: &str) -> Option<EndpointInfo> {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.endpoints.remove(&EndpointId::from(name)) {
            Some(endpoint) => {
                log_trace!(
                    _ctx,
                    "Removed endpoint",
                    audience = LogAudience::Internal,
                    name = name,
                    endpoint = endpoint.to_string()
                );
                Some(endpoint)
            }
            None => {
                log_trace!(_ctx, "Was not able to return the endpoint", audience = LogAudience::Internal, name = name);
                None
            }
        }
    }

    #[named]
    pub fn connect(&mut self, conn_params: &ConnectionParameters) -> ResultDB<&mut EndpointInfo> {
        let _ctx = ctx_with_trace!().with_feature("node");

        log_info!(
            _ctx.clone(),
            "Connection request",
            audience = LogAudience::Internal,
            conn_params = format!("{:?}", conn_params)
        );
        let node = PubKey::try_from(conn_params.node.as_str()).map_err(|_| {
            log_debug!(
                _ctx.clone(),
                "Invalid node identifier in connection parameters",
                audience = LogAudience::Internal,
                node = conn_params.node.as_str()
            );
            DBError::Command(format!("invalid node pubkey: {}", conn_params.node))
        })?;

        // ignore connect request, if it's not for our node
        if node != self.node {
            log_debug!(
                _ctx,
                "Connection for different node",
                audience = LogAudience::Internal,
                target_node = node.to_string(),
                this_node = self.node.to_string()
            );
            return Err(DBError::Ignored);
        }
        let already_connected = match self.endpoints.get(&EndpointId::from(conn_params.name.as_str())) {
            Some(ei) => ei.is_connected(),
            None => false,
        };
        if already_connected {
            log_debug!(
                _ctx.clone(),
                "Endpoint already connected",
                audience = LogAudience::Internal,
                name = conn_params.name.as_str()
            );
            return Err(DBError::Connect(format!("endpoint named {} is already connected", conn_params.name)));
        }
        log_debug!(_ctx, "Endpoint connected", audience = LogAudience::Internal, name = conn_params.name.as_str());
        Ok(self.add(conn_params))
    }

    /// disconnect endpoint - not as destructive as "remove", keeps track of the last nonce it has received
    #[named]
    pub fn disconnect(&mut self, name: &str) {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.endpoints.get_mut(&EndpointId::from(name)) {
            Some(endpoint) => {
                log_trace!(_ctx, "Disconnected endpoint", audience = LogAudience::Internal, name = name);
                endpoint.disconnect()
            }
            None => {
                log_trace!(_ctx, "Was not able to return the endpoint", audience = LogAudience::Internal, name = name);
            }
        }
    }

    #[named]
    pub fn store(&mut self, endpoint_id: &EndpointId, key: &str, value: &str) -> ResultDB<Option<String>> {
        let ei = match self.get_mut(endpoint_id) {
            Some(ei) => ei,
            None => {
                let _ctx = ctx_with_trace!().with_feature("node");

                log_debug!(
                    _ctx,
                    "Store for endpoint ignored",
                    audience = LogAudience::Internal,
                    endpoint_id = endpoint_id.to_string()
                );
                return Ok(None);
            }
        };
        if let Some(db) = &mut ei.connection {
            db.store(key, value)
        } else {
            Err(DBError::Connect(format!("endpoint {} not connected", endpoint_id)))
        }
    }

    #[named]
    pub fn load(&mut self, endpoint_id: &EndpointId, key: &str) -> ResultDB<String> {
        let ei = match self.get_mut(endpoint_id) {
            Some(ei) => ei,
            None => {
                let _ctx = ctx_with_trace!().with_feature("node");

                log_error!(
                    _ctx,
                    "Can't load from endpoint",
                    audience = LogAudience::Both,
                    endpoint_id = endpoint_id.to_string()
                );
                return Err(DBError::Connect(format!("invalid endpoint: {}", endpoint_id)));
            }
        };
        if let Some(db) = &mut ei.connection {
            db.load(key)
        } else {
            Err(DBError::Connect(format!("endpoint {} not connected", endpoint_id)))
        }
    }

    #[named]
    pub fn delete(&mut self, endpoint_id: &EndpointId, key: &str) -> ResultDB<()> {
        let ei = match self.get_mut(endpoint_id) {
            Some(ei) => ei,
            None => {
                let _ctx = ctx_with_trace!().with_feature("node");

                log_debug!(
                    _ctx,
                    "Delete for endpoint ignored",
                    audience = LogAudience::Internal,
                    endpoint_id = endpoint_id.to_string()
                );
                return Ok(());
            }
        };
        if let Some(db) = &mut ei.connection {
            db.delete(key)
        } else {
            Err(DBError::Connect(format!("endpoint {} not connected", endpoint_id)))
        }
    }

    // pub fn read_all(&mut self, endpoint_id: &EndpointId) -> ResultDB<String> {
    //     let ei = match self.get_mut(&endpoint_id) {
    //         Some(ei) => ei,
    //         None => {
    //             log::error!("can't load from endpoint: {}", endpoint_id);
    //             return Err(DBError::Connect(format!(
    //                 "invalid endpoint: {}",
    //                 endpoint_id
    //             )));
    //         }
    //     };
    //     if let Some(db) = &mut ei.connection {
    //         db.read_all()
    //     } else {
    //         Err(DBError::Connect(format!(
    //             "endpoint {} not connected",
    //             endpoint_id
    //         )))
    //     }
    // }
    #[named]
    pub fn query(&mut self, endpoint_id: &EndpointId, cmd: &str) -> ResultDB<String> {
        let ei = match self.get_mut(endpoint_id) {
            Some(ei) => ei,
            None => {
                let _ctx = ctx_with_trace!().with_feature("node");

                log_error!(
                    _ctx,
                    "Can't query from endpoint",
                    audience = LogAudience::Both,
                    endpoint_id = endpoint_id.to_string()
                );
                return Err(DBError::Connect(format!("invalid endpoint: {}", endpoint_id)));
            }
        };
        if let Some(db) = &mut ei.connection {
            db.query(cmd)
        } else {
            Err(DBError::Connect(format!("endpoint {} not connected", endpoint_id)))
        }
    }

    #[named]
    pub fn execute(&mut self, endpoint_id: &EndpointId, cmd: &str) -> ResultDB<String> {
        let _ctx = ctx_with_trace!().with_feature("node");

        let ei = match self.get_mut(endpoint_id) {
            Some(ei) => ei,
            None => {
                log_debug!(
                    _ctx,
                    "Execute for endpoint ignored",
                    audience = LogAudience::Internal,
                    endpoint_id = endpoint_id.to_string()
                );
                return Err(DBError::Ignored);
            }
        };
        if ei.read_only {
            return Err(DBError::Command(format!("{} is read-only, can't execute write commands", ei.name)));
        }
        if let Some(db) = &mut ei.connection {
            db.execute(cmd).map_err(|e| {
                log_error!(_ctx, "Error executing command", audience = LogAudience::Both, command = cmd, error = e.to_string());
                e
            })
        } else {
            Err(DBError::Connect(format!("endpoint {} not connected", endpoint_id)))
        }
    }
}

#[derive(Serialize, Deserialize, Default)]
pub struct EndpointInfo {
    uuid: HashType,
    pub name: EndpointId,
    pub url: EndpointUrl,
    pub db_type: DBKind,
    pub read_only: bool,
    #[serde(skip)]
    pub connection: Option<Box<dyn DB>>,
    #[serde(skip)]
    pub conn_params: ConnectionParameters,
    nodes: HashSet<PubKey>,
    pub last_nonce: Nonce,
}

impl PartialEq for EndpointInfo {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
            && self.name == other.name
            && self.url == other.url
            && self.db_type == other.db_type
            && self.nodes == other.nodes
    }
}

impl Eq for EndpointInfo {}

impl EndpointInfo {
    /// generate new 'EndpointInfo'
    #[named]
    fn new(conn_params: &ConnectionParameters, node: &PubKey) -> Self {
        let bytes = [
            conn_params.name.as_bytes(),
            conn_params.url.as_bytes(),
            conn_params.db_type.as_bytes(),
            if conn_params.read_only { &[1] } else { &[0] },
            node.as_ref(),
        ]
        .concat();

        let info = Self {
            uuid: HashType::hash(bytes.as_ref()),
            name: EndpointId::from(conn_params.name.as_str()),
            url: EndpointUrl::from(conn_params.url.as_str()),
            db_type: conn_params.db_type.clone(),
            read_only: conn_params.read_only,
            nodes: HashSet::from([node.clone()]),
            connection: None,
            conn_params: conn_params.clone(),
            last_nonce: Nonce::default(),
        };

        let _ctx = ctx_with_trace!().with_feature("node");

        log_trace!(_ctx, "Generated new endpoint info", audience = LogAudience::Internal, info = info.to_string());

        info
    }

    #[named]
    fn update(&mut self, conn_params: &ConnectionParameters) {
        self.name = EndpointId::from(conn_params.name.as_str());
        self.url = EndpointUrl::from(conn_params.url.as_str());
        self.db_type = conn_params.db_type.clone();
        self.read_only = conn_params.read_only;
        self.connection = None;
        self.conn_params = conn_params.clone();

        let _ctx = ctx_with_trace!().with_feature("node");

        log_trace!(_ctx, "Updated endpoint info", audience = LogAudience::Internal, info = self.to_string());
    }

    /// rename endpoint
    #[named]
    pub fn rename(&mut self, name: &str) -> EndpointId {
        let old = self.name.clone();
        self.name = EndpointId::from(name);

        let _ctx = ctx_with_trace!().with_feature("node");

        log_trace!(
            _ctx,
            "Replaced the endpoint name",
            audience = LogAudience::Internal,
            old_name = old.to_string(),
            new_name = name
        );
        old
    }

    /// update endpoint url
    #[named]
    pub fn update_url(&mut self, url: &str) -> EndpointUrl {
        let old = self.url.clone();
        self.url = EndpointUrl::from(url);

        let _ctx = ctx_with_trace!().with_feature("node");

        log_trace!(
            _ctx,
            "Replaced the endpoint url",
            audience = LogAudience::Internal,
            old_url = old.to_string(),
            new_url = url
        );
        old
    }

    /// add a node pubkey from 'HashSet'
    #[named]
    pub fn add_node(&mut self, node: PubKey) {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.nodes.insert(node.clone()) {
            true => {
                log_trace!(_ctx, "Added node", audience = LogAudience::Internal, node = node.to_string());
            }
            false => {
                log_trace!(_ctx, "Could not add node", audience = LogAudience::Internal, node = node.to_string());
            }
        }
    }

    /// remove a node pubkey from 'HashSet'
    #[named]
    pub fn remove_node(&mut self, node: &PubKey) -> bool {
        let _ctx = ctx_with_trace!().with_feature("node");

        match self.nodes.remove(node) {
            true => {
                log_trace!(_ctx, "Removed node", audience = LogAudience::Internal, node = node.to_string());
                true
            }
            false => {
                log_trace!(_ctx, "Could not remove node", audience = LogAudience::Internal, node = node.to_string());
                false
            }
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }
    /// connect stores the connection to the database
    #[named]
    pub fn connect(&mut self, db: Box<dyn DB>) {
        let _ctx = ctx_with_trace!().with_feature("node");

        log_debug!(
            _ctx,
            "Connected endpoint",
            audience = LogAudience::Internal,
            name = self.name.to_string(),
            db_kind = db.kind().to_string()
        );
        self.connection = Some(db);
    }

    /// disconnect disconnects the db and removes the connection to the database
    pub fn disconnect(&mut self) {
        if let Some(conn) = self.connection.as_mut() {
            _ = conn.disconnect();
            self.connection = None;
        }
    }
}

impl fmt::Display for EndpointInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}:{} ({}:{}:{}), {:?}",
            self.uuid,
            self.name,
            self.db_type,
            if self.read_only { "ro" } else { "rw" },
            self.url,
            self.nodes
        )
    }
}
