//! Redis Cluster response rewriting.

use super::*;

static CLUSTER_REDIRECT_UNMAPPABLE_RESPONSE: Lazy<Bytes> =
    Lazy::new(|| Bytes::from_static(b"-TRYAGAIN Redis cluster topology is refreshing\r\n"));

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ClusterRedirectRewrite {
    NotRedirect,
    Unmappable,
    Rewritten(Bytes),
}

pub(super) trait ClusterSlotsNodeFrames {
    fn backend_address(&self) -> Option<(String, u16)>;
    fn rewrite_virtual_node(&mut self, topology: &VirtualClusterTopology, virtual_node: &VirtualClusterNode);
}

pub(super) struct ClusterResponseRewriter;

impl ClusterResponseRewriter {
    pub(super) fn redirect_line(resp: &[u8]) -> Option<&str> {
        let line = std::str::from_utf8(resp).ok()?.lines().next()?;
        if !line.starts_with("-MOVED ") && !line.starts_with("-ASK ") {
            return None;
        }

        Some(line)
    }

    pub(super) fn redirect(resp: &[u8], topology: &VirtualClusterTopology) -> ClusterRedirectRewrite {
        if Self::redirect_line(resp).is_none() {
            return ClusterRedirectRewrite::NotRedirect;
        }

        let Some((kind, slot, host, port)) = Self::parse_redirect(resp) else {
            return ClusterRedirectRewrite::Unmappable;
        };
        let Some(virtual_node) = topology.node_for_backend_address(host, port).or_else(|| topology.node_for_slot(slot)) else {
            return ClusterRedirectRewrite::Unmappable;
        };

        ClusterRedirectRewrite::Rewritten(Bytes::from(format!(
            "-{} {} {}:{}\r\n",
            kind, slot, topology.advertise_host, virtual_node.advertise_port
        )))
    }

    pub(super) fn redirect_response(resp: Bytes, topology: &VirtualClusterTopology) -> Bytes {
        match Self::redirect(&resp, topology) {
            ClusterRedirectRewrite::NotRedirect => resp,
            ClusterRedirectRewrite::Unmappable => CLUSTER_REDIRECT_UNMAPPABLE_RESPONSE.clone(),
            ClusterRedirectRewrite::Rewritten(response) => response,
        }
    }

    pub(super) fn decode_nodes_payload(bytes: &[u8]) -> Result<String, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete CLUSTER NODES response"))?;
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data)) => {
                String::from_utf8(data).map_err(EpError::parse)
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. }) => {
                String::from_utf8(data).map_err(EpError::parse)
            }
            DecoderRespFrame::Resp3(Resp3Frame::VerbatimString { data, .. }) => String::from_utf8(data).map_err(EpError::parse),
            DecoderRespFrame::Resp2(Resp2Frame::Error(err)) => Err(EpError::parse(err)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected CLUSTER NODES response: {:?}", other))),
        }
    }

    pub(super) fn nodes_response(resp: &[u8], topology: &VirtualClusterTopology) -> Result<Bytes, EpError> {
        let payload = Self::decode_nodes_payload(resp)?;
        Self::encode_string_frame_like(resp, Self::nodes_payload(&payload, topology))
    }

    pub(super) fn nodes_payload(raw: &str, topology: &VirtualClusterTopology) -> String {
        let nodes_by_id: HashMap<&str, &VirtualClusterNode> =
            topology.nodes.iter().map(|node| (node.backend.node_id.as_str(), node)).collect();

        raw.lines()
            .map(|line| {
                let mut parts: Vec<String> = line.split_whitespace().map(ToString::to_string).collect();
                if parts.len() >= 2
                    && let Some(virtual_node) = nodes_by_id.get(parts[0].as_str())
                {
                    parts[0] = virtual_node.stable_node_id.clone();
                    let bus_port = virtual_node.backend.bus_port.unwrap_or_else(|| virtual_node.advertise_port.saturating_add(10_000));
                    parts[1] = format!("{}:{}@{}", topology.advertise_host, virtual_node.advertise_port, bus_port);
                    if parts.len() >= 4
                        && parts[3] != "-"
                        && let Some(master_node) = nodes_by_id.get(parts[3].as_str())
                    {
                        parts[3] = master_node.stable_node_id.clone();
                    }
                }
                for part in parts.iter_mut().skip(8) {
                    *part = Self::nodes_slot_token(part, &nodes_by_id);
                }
                parts.join(" ")
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub(super) fn rewrite_slots_node_resp3(node: &mut [Resp3Frame], topology: &VirtualClusterTopology) {
        if node.len() < 2 {
            return;
        }
        Self::rewrite_slots_node(node, topology);
    }

    pub(super) fn rewrite_slots_node_resp2(node: &mut [Resp2Frame], topology: &VirtualClusterTopology) {
        if node.len() < 2 {
            return;
        }
        Self::rewrite_slots_node(node, topology);
    }

    pub(super) fn slots_response(resp: &[u8], topology: &VirtualClusterTopology) -> Result<Bytes, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(resp).ok_or_else(|| EpError::parse("incomplete CLUSTER SLOTS response"))?;

        let encoded = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(mut slots)) => {
                for slot in &mut slots {
                    if let Resp2Frame::Array(slot_entry) = slot {
                        for node_entry in slot_entry.iter_mut().skip(2) {
                            if let Resp2Frame::Array(node) = node_entry {
                                Self::rewrite_slots_node_resp2(node, topology);
                            }
                        }
                    }
                }
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(Resp2Frame::Array(slots)))?
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { mut data, attributes }) => {
                for slot in &mut data {
                    if let Resp3Frame::Array { data: slot_entry, .. } = slot {
                        for node_entry in slot_entry.iter_mut().skip(2) {
                            if let Resp3Frame::Array { data: node, .. } = node_entry {
                                Self::rewrite_slots_node_resp3(node, topology);
                            }
                        }
                    }
                }
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(Resp3Frame::Array { data, attributes }))?
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(err)) => return Err(EpError::parse(err)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => return Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => return Err(EpError::parse(format!("unexpected CLUSTER SLOTS response: {:?}", other))),
        };

        Ok(Bytes::from(encoded))
    }

    pub(super) fn shards_response(resp: &[u8], topology: &VirtualClusterTopology) -> Result<Bytes, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(resp).ok_or_else(|| EpError::parse("incomplete CLUSTER SHARDS response"))?;

        let encoded = match frame {
            DecoderRespFrame::Resp3(mut frame) => {
                Self::rewrite_shards_frame_resp3(&mut frame, topology);
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(frame))?
            }
            DecoderRespFrame::Resp2(mut frame) => {
                Self::rewrite_shards_frame_resp2(&mut frame, topology);
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(frame))?
            }
        };

        Ok(Bytes::from(encoded))
    }

    fn parse_redirect(resp: &[u8]) -> Option<(&str, u16, &str, u16)> {
        let line = Self::redirect_line(resp)?;
        let mut parts = line[1..].split_whitespace();
        let kind = parts.next()?;
        let slot = parts.next()?.parse::<u16>().ok()?;
        let target = parts.next()?;
        let (host, port) = target.rsplit_once(':')?;
        Some((kind, slot, host, port.parse::<u16>().ok()?))
    }

    fn encode_string_frame_like(original: &[u8], payload: String) -> Result<Bytes, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(original).ok_or_else(|| EpError::parse("incomplete Redis response"))?;
        let encoded = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(_)) => {
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(Resp2Frame::BulkString(payload.into_bytes())))?
            }
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(_)) => {
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp2(Resp2Frame::SimpleString(payload.into_bytes())))?
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { .. }) => {
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(Resp3Frame::BlobString {
                    data: payload.into_bytes(),
                    attributes: None,
                }))?
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { .. }) => {
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(Resp3Frame::SimpleString {
                    data: payload.into_bytes(),
                    attributes: None,
                }))?
            }
            DecoderRespFrame::Resp3(Resp3Frame::VerbatimString { format, .. }) => {
                RedisProtocol::encode_to_buffer(&EncoderRespFrame::Resp3(Resp3Frame::VerbatimString {
                    format,
                    data: payload.into_bytes(),
                    attributes: None,
                }))?
            }
            other => {
                return Err(EpError::parse(format!("cannot encode rewritten Redis string frame from response {:?}", other)));
            }
        };
        Ok(Bytes::from(encoded))
    }

    fn nodes_slot_token(token: &str, nodes_by_id: &HashMap<&str, &VirtualClusterNode>) -> String {
        let Some(inner) = token.strip_prefix('[').and_then(|value| value.strip_suffix(']')) else {
            return token.to_string();
        };

        for marker in ["->-", "-<-"] {
            if let Some((slot, backend_node_id)) = inner.split_once(marker)
                && let Some(virtual_node) = nodes_by_id.get(backend_node_id)
            {
                return format!("[{}{}{}]", slot, marker, virtual_node.stable_node_id);
            }
        }

        token.to_string()
    }

    fn rewrite_slots_node(node: &mut (impl ClusterSlotsNodeFrames + ?Sized), topology: &VirtualClusterTopology) {
        let Some((host, port)) = node.backend_address() else {
            return;
        };

        if let Some(virtual_node) = topology.node_for_backend_address(&host, port) {
            node.rewrite_virtual_node(topology, virtual_node);
        }
    }

    fn resp3_frame_to_string(frame: &Resp3Frame) -> Option<String> {
        match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
                Some(String::from_utf8_lossy(data).to_string())
            }
            _ => None,
        }
    }

    fn resp3_frame_to_u16(frame: &Resp3Frame) -> Option<u16> {
        match frame {
            Resp3Frame::Number { data, .. } => u16::try_from(*data).ok(),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
                String::from_utf8_lossy(data).parse::<u16>().ok()
            }
            _ => None,
        }
    }

    fn rewrite_shards_resp3_array_map(data: &mut [Resp3Frame], topology: &VirtualClusterTopology) {
        if data.len() < 2 || !data.len().is_multiple_of(2) {
            return;
        }

        let mut key_lookup: HashMap<String, usize> = HashMap::new();
        for index in (0..data.len()).step_by(2) {
            if let Some(key) = Self::resp3_frame_to_string(&data[index]) {
                key_lookup.insert(key, index + 1);
            }
        }

        let host = key_lookup.get("ip").and_then(|index| data.get(*index)).and_then(Self::resp3_frame_to_string);
        let port = key_lookup.get("port").and_then(|index| data.get(*index)).and_then(Self::resp3_frame_to_u16);
        let endpoint = key_lookup.get("endpoint").and_then(|index| data.get(*index)).and_then(Self::resp3_frame_to_string);

        let address = match (host, port) {
            (Some(host), Some(port)) if !host.is_empty() => Some((host, port)),
            _ => endpoint.as_deref().and_then(Self::parse_host_port),
        };
        let Some((host, port)) = address else {
            return;
        };

        let Some(virtual_node) = topology.node_for_backend_address(&host, port) else {
            return;
        };

        if let Some(index) = key_lookup.get("id") {
            data[*index] = Resp3Frame::BlobString {
                data: virtual_node.stable_node_id.as_bytes().to_vec(),
                attributes: None,
            };
        }
        if let Some(index) = key_lookup.get("ip") {
            data[*index] = Resp3Frame::BlobString {
                data: topology.advertise_host.as_bytes().to_vec(),
                attributes: None,
            };
        }
        if let Some(index) = key_lookup.get("endpoint") {
            data[*index] = Resp3Frame::BlobString {
                data: format!("{}:{}", topology.advertise_host, virtual_node.advertise_port).into_bytes(),
                attributes: None,
            };
        }
        if let Some(index) = key_lookup.get("hostname") {
            data[*index] = Resp3Frame::BlobString {
                data: topology.advertise_host.as_bytes().to_vec(),
                attributes: None,
            };
        }
        if let Some(index) = key_lookup.get("port") {
            data[*index] = Resp3Frame::Number {
                data: i64::from(virtual_node.advertise_port),
                attributes: None,
            };
        }
    }

    fn rewrite_shards_frame_resp3(frame: &mut Resp3Frame, topology: &VirtualClusterTopology) {
        match frame {
            Resp3Frame::Array { data, .. } | Resp3Frame::Push { data, .. } => {
                Self::rewrite_shards_resp3_array_map(data, topology);
                for item in data {
                    Self::rewrite_shards_frame_resp3(item, topology);
                }
            }
            Resp3Frame::Map { data, .. } => {
                let mut key_lookup: HashMap<String, Resp3Frame> = HashMap::new();
                for key in data.keys() {
                    let key_str = match key {
                        Resp3Frame::BlobString { data, .. }
                        | Resp3Frame::SimpleString { data, .. }
                        | Resp3Frame::VerbatimString { data, .. } => Some(String::from_utf8_lossy(data).to_string()),
                        _ => None,
                    };
                    if let Some(key_str) = key_str {
                        key_lookup.insert(key_str, key.clone());
                    }
                }

                let host = key_lookup.get("ip").and_then(|key| data.get(key)).and_then(Self::resp3_frame_to_string);
                let port = key_lookup.get("port").and_then(|key| data.get(key)).and_then(Self::resp3_frame_to_u16);
                let endpoint = key_lookup.get("endpoint").and_then(|key| data.get(key)).and_then(Self::resp3_frame_to_string);

                let address = match (host, port) {
                    (Some(host), Some(port)) if !host.is_empty() => Some((host, port)),
                    _ => endpoint.as_deref().and_then(Self::parse_host_port),
                };

                if let Some((host, port)) = address
                    && let Some(virtual_node) = topology.node_for_backend_address(&host, port)
                {
                    if let Some(key) = key_lookup.get("id")
                        && let Some(value) = data.get_mut(key)
                    {
                        *value = Resp3Frame::BlobString {
                            data: virtual_node.stable_node_id.as_bytes().to_vec(),
                            attributes: None,
                        };
                    }
                    if let Some(key) = key_lookup.get("ip")
                        && let Some(value) = data.get_mut(key)
                    {
                        *value = Resp3Frame::BlobString {
                            data: topology.advertise_host.as_bytes().to_vec(),
                            attributes: None,
                        };
                    }
                    if let Some(key) = key_lookup.get("endpoint")
                        && let Some(value) = data.get_mut(key)
                    {
                        *value = Resp3Frame::BlobString {
                            data: format!("{}:{}", topology.advertise_host, virtual_node.advertise_port).into_bytes(),
                            attributes: None,
                        };
                    }
                    if let Some(key) = key_lookup.get("hostname")
                        && let Some(value) = data.get_mut(key)
                    {
                        *value = Resp3Frame::BlobString {
                            data: topology.advertise_host.as_bytes().to_vec(),
                            attributes: None,
                        };
                    }
                    if let Some(key) = key_lookup.get("port")
                        && let Some(value) = data.get_mut(key)
                    {
                        *value = Resp3Frame::Number {
                            data: i64::from(virtual_node.advertise_port),
                            attributes: None,
                        };
                    }
                }

                for value in data.values_mut() {
                    Self::rewrite_shards_frame_resp3(value, topology);
                }
            }
            Resp3Frame::Set { .. } => {}
            _ => {}
        }
    }

    fn resp2_frame_to_string(frame: &Resp2Frame) -> Option<String> {
        match frame {
            Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => Some(String::from_utf8_lossy(data).to_string()),
            _ => None,
        }
    }

    fn resp2_frame_to_u16(frame: &Resp2Frame) -> Option<u16> {
        match frame {
            Resp2Frame::Integer(port) => u16::try_from(*port).ok(),
            Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => String::from_utf8_lossy(data).parse::<u16>().ok(),
            _ => None,
        }
    }

    fn parse_host_port(address: &str) -> Option<(String, u16)> {
        let (host, port) = address.rsplit_once(':')?;
        Some((host.to_string(), port.parse::<u16>().ok()?))
    }

    fn rewrite_shards_resp2_map(data: &mut [Resp2Frame], topology: &VirtualClusterTopology) {
        if data.len() < 2 || !data.len().is_multiple_of(2) {
            return;
        }

        let mut key_lookup: HashMap<String, usize> = HashMap::new();
        for index in (0..data.len()).step_by(2) {
            if let Some(key) = Self::resp2_frame_to_string(&data[index]) {
                key_lookup.insert(key, index + 1);
            }
        }

        let host = key_lookup.get("ip").and_then(|index| data.get(*index)).and_then(Self::resp2_frame_to_string);
        let port = key_lookup.get("port").and_then(|index| data.get(*index)).and_then(Self::resp2_frame_to_u16);
        let endpoint = key_lookup.get("endpoint").and_then(|index| data.get(*index)).and_then(Self::resp2_frame_to_string);

        let address = match (host, port) {
            (Some(host), Some(port)) if !host.is_empty() => Some((host, port)),
            _ => endpoint.as_deref().and_then(Self::parse_host_port),
        };
        let Some((host, port)) = address else {
            return;
        };

        let Some(virtual_node) = topology.node_for_backend_address(&host, port) else {
            return;
        };

        if let Some(index) = key_lookup.get("id") {
            data[*index] = Resp2Frame::BulkString(virtual_node.stable_node_id.as_bytes().to_vec());
        }
        if let Some(index) = key_lookup.get("ip") {
            data[*index] = Resp2Frame::BulkString(topology.advertise_host.as_bytes().to_vec());
        }
        if let Some(index) = key_lookup.get("endpoint") {
            data[*index] = Resp2Frame::BulkString(format!("{}:{}", topology.advertise_host, virtual_node.advertise_port).into_bytes());
        }
        if let Some(index) = key_lookup.get("hostname") {
            data[*index] = Resp2Frame::BulkString(topology.advertise_host.as_bytes().to_vec());
        }
        if let Some(index) = key_lookup.get("port") {
            data[*index] = Resp2Frame::Integer(i64::from(virtual_node.advertise_port));
        }
    }

    fn rewrite_shards_frame_resp2(frame: &mut Resp2Frame, topology: &VirtualClusterTopology) {
        if let Resp2Frame::Array(data) = frame {
            Self::rewrite_shards_resp2_map(data, topology);
            for item in data {
                Self::rewrite_shards_frame_resp2(item, topology);
            }
        }
    }
}

impl ClusterSlotsNodeFrames for [Resp2Frame] {
    fn backend_address(&self) -> Option<(String, u16)> {
        if self.len() < 2 {
            return None;
        }

        let host = match &self[0] {
            Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => String::from_utf8_lossy(data).to_string(),
            _ => return None,
        };
        let port = match &self[1] {
            Resp2Frame::Integer(port) => u16::try_from(*port).ok()?,
            Resp2Frame::BulkString(data) | Resp2Frame::SimpleString(data) => String::from_utf8_lossy(data).parse::<u16>().ok()?,
            _ => return None,
        };

        Some((host, port))
    }

    fn rewrite_virtual_node(&mut self, topology: &VirtualClusterTopology, virtual_node: &VirtualClusterNode) {
        self[0] = Resp2Frame::BulkString(topology.advertise_host.as_bytes().to_vec());
        self[1] = Resp2Frame::Integer(i64::from(virtual_node.advertise_port));
        if let Some(node_id) = self.get_mut(2) {
            *node_id = Resp2Frame::BulkString(virtual_node.stable_node_id.as_bytes().to_vec());
        }
    }
}

impl ClusterSlotsNodeFrames for [Resp3Frame] {
    fn backend_address(&self) -> Option<(String, u16)> {
        if self.len() < 2 {
            return None;
        }

        let host = match &self[0] {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
                String::from_utf8_lossy(data).to_string()
            }
            _ => return None,
        };
        let port = match &self[1] {
            Resp3Frame::Number { data, .. } => u16::try_from(*data).ok()?,
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } | Resp3Frame::VerbatimString { data, .. } => {
                String::from_utf8_lossy(data).parse::<u16>().ok()?
            }
            _ => return None,
        };

        Some((host, port))
    }

    fn rewrite_virtual_node(&mut self, topology: &VirtualClusterTopology, virtual_node: &VirtualClusterNode) {
        self[0] = Resp3Frame::BlobString {
            data: topology.advertise_host.as_bytes().to_vec(),
            attributes: None,
        };
        self[1] = Resp3Frame::Number {
            data: i64::from(virtual_node.advertise_port),
            attributes: None,
        };
        if let Some(node_id) = self.get_mut(2) {
            *node_id = Resp3Frame::BlobString {
                data: virtual_node.stable_node_id.as_bytes().to_vec(),
                attributes: None,
            };
        }
    }
}
