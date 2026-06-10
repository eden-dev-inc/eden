use byteorder::{ByteOrder, LittleEndian, ReadBytesExt, WriteBytesExt};
use error::{EpError, ResultEP};
use mongodb::bson;
use mongodb::bson::Document;
use std::io::Cursor;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct WireMessage {
    pub message_length: i32,
    pub request_id: i32,
    pub response_to: i32,
    pub op_code: i32,
    pub body: Vec<u8>,
}

// Standard MongoDB wire protocol opcodes
#[allow(dead_code)]
pub const OP_REPLY: i32 = 1;
#[allow(dead_code)]
pub const OP_UPDATE: i32 = 2001;
#[allow(dead_code)]
pub const OP_INSERT: i32 = 2002;
#[allow(dead_code)]
pub const OP_QUERY: i32 = 2004;
#[allow(dead_code)]
pub const OP_GET_MORE: i32 = 2005;
#[allow(dead_code)]
pub const OP_DELETE: i32 = 2006;
#[allow(dead_code)]
pub const OP_KILL_CURSORS: i32 = 2007;
#[allow(dead_code)]
pub const OP_COMPRESSED: i32 = 2012;
#[allow(dead_code)]
pub const OP_MSG: i32 = 2013;

#[allow(dead_code)]
impl WireMessage {
    /// Encode wire message to bytes
    pub fn encode(&self) -> ResultEP<Vec<u8>> {
        let mut buffer = Vec::with_capacity(self.message_length as usize);

        buffer
            .write_i32::<LittleEndian>(self.message_length)
            .map_err(|e| EpError::parse(format!("failed to write message length: {}", e)))?;
        buffer
            .write_i32::<LittleEndian>(self.request_id)
            .map_err(|e| EpError::parse(format!("failed to write request id: {}", e)))?;
        buffer
            .write_i32::<LittleEndian>(self.response_to)
            .map_err(|e| EpError::parse(format!("failed to write response to: {}", e)))?;
        buffer.write_i32::<LittleEndian>(self.op_code).map_err(|e| EpError::parse(format!("failed to write op code: {}", e)))?;

        buffer.extend_from_slice(&self.body);

        Ok(buffer)
    }

    /// Decode bytes to wire message
    pub fn decode(buffer: &[u8]) -> ResultEP<Option<(Self, usize)>> {
        if buffer.len() < 16 {
            return Ok(None); // Need at least header
        }

        let mut cursor = Cursor::new(buffer);

        let message_length =
            cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read message length: {}", e)))?;

        if buffer.len() < message_length as usize {
            return Ok(None); // Incomplete message
        }

        let request_id = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read request id: {}", e)))?;
        let response_to = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read response to: {}", e)))?;
        let op_code = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read op code: {}", e)))?;

        let body_len = message_length as usize - 16;
        let body = buffer[16..16 + body_len].to_vec();

        Ok(Some((
            WireMessage { message_length, request_id, response_to, op_code, body },
            message_length as usize,
        )))
    }
}

#[allow(dead_code)]
/// Encode OP_MSG command
pub fn encode_op_msg(request_id: i32, command: Document) -> ResultEP<Vec<u8>> {
    let mut body = Vec::new();

    // Flag bits (0 for standard message)
    body.write_u32::<LittleEndian>(0).map_err(|e| EpError::parse(format!("failed to write flag bits: {}", e)))?;

    // Section type 0 (body)
    body.push(0);

    // Serialize BSON document
    let doc_bytes = bson::to_vec(&command).map_err(|e| EpError::parse(format!("failed to serialize document: {}", e)))?;
    body.extend_from_slice(&doc_bytes);

    let message = WireMessage {
        message_length: 16 + body.len() as i32,
        request_id,
        response_to: 0,
        op_code: OP_MSG,
        body,
    };

    message.encode()
}

#[allow(dead_code)]
/// Decode OP_MSG command with database and collection extraction
pub fn decode_op_msg(body: &[u8]) -> ResultEP<(Document, String, Option<String>)> {
    if body.len() < 5 {
        return Err(EpError::parse("OP_MSG body too short"));
    }

    let mut cursor = Cursor::new(body);

    // Read flag bits
    let _flags = cursor.read_u32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read flags: {}", e)))?;

    // Read section type
    let section_type = cursor.read_u8().map_err(|e| EpError::parse(format!("failed to read section type: {}", e)))?;

    if section_type != 0 {
        return Err(EpError::parse("unsupported section type"));
    }

    // Parse BSON document
    let doc_bytes = &body[5..];
    let command: Document = bson::from_slice(doc_bytes).map_err(|e| EpError::parse(format!("failed to parse BSON: {}", e)))?;

    // Extract database from $db field
    let database = command.get_str("$db").map_err(|_| EpError::parse("missing $db field"))?.to_string();

    // Extract collection name from command
    let collection = extract_collection_from_command(&command);

    Ok((command, database, collection))
}

#[allow(dead_code)]
fn extract_collection_from_command(command: &Document) -> Option<String> {
    // Skip metadata fields to find the actual command
    let skip_fields = ["$db", "$clusterTime", "lsid", "$readPreference"];

    command
        .iter()
        .find(|(key, _)| !skip_fields.contains(&key.as_str()))
        .and_then(|(_, value)| value.as_str())
        .map(|s| s.to_string())
}

#[allow(dead_code)]
/// Encode OP_REPLY response
pub fn encode_op_reply(request_id: i32, response_to: i32, documents: Vec<Document>) -> ResultEP<Vec<u8>> {
    let mut body = Vec::new();

    // Response flags (0 for success)
    body.write_u32::<LittleEndian>(0).map_err(|e| EpError::parse(format!("failed to write response flags: {}", e)))?;

    // Cursor ID (0 for no cursor)
    body.write_i64::<LittleEndian>(0).map_err(|e| EpError::parse(format!("failed to write cursor id: {}", e)))?;

    // Starting from (0)
    body.write_i32::<LittleEndian>(0).map_err(|e| EpError::parse(format!("failed to write starting from: {}", e)))?;

    // Number returned
    body.write_i32::<LittleEndian>(documents.len() as i32)
        .map_err(|e| EpError::parse(format!("failed to write number returned: {}", e)))?;

    // Serialize documents
    for doc in documents {
        let doc_bytes = bson::to_vec(&doc).map_err(|e| EpError::parse(format!("failed to serialize document: {}", e)))?;
        body.extend_from_slice(&doc_bytes);
    }

    let message = WireMessage {
        message_length: 16 + body.len() as i32,
        request_id,
        response_to,
        op_code: OP_REPLY,
        body,
    };

    message.encode()
}

#[allow(dead_code)]
/// Decode OP_REPLY response
pub fn decode_op_reply(body: &[u8]) -> ResultEP<Vec<Document>> {
    if body.len() < 20 {
        return Err(EpError::parse("OP_REPLY body too short"));
    }

    let mut cursor = Cursor::new(body);

    // Read response flags
    let _flags = cursor.read_u32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read response flags: {}", e)))?;

    // Read cursor ID
    let _cursor_id = cursor.read_i64::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read cursor id: {}", e)))?;

    // Read starting from
    let _starting_from = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read starting from: {}", e)))?;

    // Read number returned
    let number_returned =
        cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read number returned: {}", e)))?;

    let mut documents = Vec::new();
    let mut offset = 20;

    for _ in 0..number_returned {
        if offset >= body.len() {
            break;
        }

        let remaining = &body[offset..];
        if remaining.len() < 4 {
            break;
        }
        let doc_size = LittleEndian::read_i32(remaining) as usize;
        if doc_size < 5 || offset + doc_size > body.len() {
            return Err(EpError::parse("invalid BSON document length in OP_REPLY"));
        }
        let doc: Document =
            bson::from_slice(&remaining[..doc_size]).map_err(|e| EpError::parse(format!("failed to parse document: {}", e)))?;

        documents.push(doc);
        offset += doc_size;
    }

    Ok(documents)
}

#[allow(dead_code)]
/// Encode OP_QUERY request
pub fn encode_op_query(
    request_id: i32,
    collection_name: &str,
    query: Document,
    projection: Option<Document>,
    skip: i32,
    limit: i32,
) -> ResultEP<Vec<u8>> {
    let mut body = Vec::new();

    // Flags (0 for standard query)
    body.write_u32::<LittleEndian>(0).map_err(|e| EpError::parse(format!("failed to write flags: {}", e)))?;

    // Collection name (null-terminated)
    body.extend_from_slice(collection_name.as_bytes());
    body.push(0);

    // Number to skip
    body.write_i32::<LittleEndian>(skip).map_err(|e| EpError::parse(format!("failed to write skip: {}", e)))?;

    // Number to return
    body.write_i32::<LittleEndian>(limit).map_err(|e| EpError::parse(format!("failed to write limit: {}", e)))?;

    // Query document
    let query_bytes = bson::to_vec(&query).map_err(|e| EpError::parse(format!("failed to serialize query: {}", e)))?;
    body.extend_from_slice(&query_bytes);

    // Projection document (optional)
    if let Some(proj) = projection {
        let proj_bytes = bson::to_vec(&proj).map_err(|e| EpError::parse(format!("failed to serialize projection: {}", e)))?;
        body.extend_from_slice(&proj_bytes);
    }

    let message = WireMessage {
        message_length: 16 + body.len() as i32,
        request_id,
        response_to: 0,
        op_code: OP_QUERY,
        body,
    };

    message.encode()
}

#[allow(dead_code)]
/// Decode OP_QUERY request with database and collection extraction
pub fn decode_op_query(body: &[u8]) -> ResultEP<(String, String, Document, Option<Document>, i32, i32)> {
    if body.len() < 12 {
        return Err(EpError::parse("OP_QUERY body too short"));
    }

    let mut cursor = Cursor::new(body);

    // Read flags
    let _flags = cursor.read_u32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read flags: {}", e)))?;

    // Find collection name (null-terminated)
    let start_pos = cursor.position() as usize;
    let null_pos = body[start_pos..]
        .iter()
        .position(|&b| b == 0)
        .ok_or_else(|| EpError::parse("missing null terminator in collection name"))?;

    let collection_name = String::from_utf8_lossy(&body[start_pos..start_pos + null_pos]).to_string();
    cursor.set_position((start_pos + null_pos + 1) as u64);

    // Read skip and limit
    let skip = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read skip: {}", e)))?;
    let limit = cursor.read_i32::<LittleEndian>().map_err(|e| EpError::parse(format!("failed to read limit: {}", e)))?;

    let remaining = &body[cursor.position() as usize..];

    // Parse query document — read BSON length prefix to extract exactly one document
    if remaining.len() < 4 {
        return Err(EpError::parse("OP_QUERY body too short for query document"));
    }
    let query_size = LittleEndian::read_i32(remaining) as usize;
    if query_size < 5 || query_size > remaining.len() {
        return Err(EpError::parse("invalid BSON document length in OP_QUERY"));
    }
    let query: Document =
        bson::from_slice(&remaining[..query_size]).map_err(|e| EpError::parse(format!("failed to parse query: {}", e)))?;

    // Parse optional projection document
    let projection = if remaining.len() > query_size {
        let proj_bytes = &remaining[query_size..];
        Some(bson::from_slice(proj_bytes).map_err(|e| EpError::parse(format!("failed to parse projection: {}", e)))?)
    } else {
        None
    };

    // Split collection name into database and collection
    let parts: Vec<&str> = collection_name.splitn(2, '.').collect();
    if parts.len() != 2 {
        return Err(EpError::parse("invalid collection name format"));
    }

    let database = parts[0].to_string();
    let collection = parts[1].to_string();

    Ok((database, collection, query, projection, skip, limit))
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn test_wire_message_encode_decode() {
        let original = WireMessage {
            message_length: 20,
            request_id: 123,
            response_to: 456,
            op_code: OP_MSG,
            body: vec![1, 2, 3, 4],
        };

        let encoded = original.encode().unwrap();
        let (decoded, size) = WireMessage::decode(&encoded).unwrap().unwrap();

        assert_eq!(decoded.request_id, original.request_id);
        assert_eq!(decoded.response_to, original.response_to);
        assert_eq!(decoded.op_code, original.op_code);
        assert_eq!(decoded.body, original.body);
        assert_eq!(size, encoded.len());
    }

    #[test]
    fn test_op_msg_encode_decode() {
        let command = doc! {
            "find": "users",
            "filter": {"status": "active"},
            "$db": "mydb"
        };

        let encoded = encode_op_msg(123, command.clone()).unwrap();
        let message = WireMessage::decode(&encoded).unwrap().unwrap().0;
        let (decoded_command, database, collection) = decode_op_msg(&message.body).unwrap();

        assert_eq!(message.op_code, OP_MSG);
        assert_eq!(decoded_command.get_str("find").unwrap(), "users");
        assert_eq!(database, "mydb");
        assert_eq!(collection, Some("users".to_string()));
    }

    #[test]
    fn test_op_reply_encode_decode() {
        let docs = vec![doc! {"name": "Alice", "age": 30}, doc! {"name": "Bob", "age": 25}];

        let encoded = encode_op_reply(123, 456, docs.clone()).unwrap();
        let message = WireMessage::decode(&encoded).unwrap().unwrap().0;
        let decoded_docs = decode_op_reply(&message.body).unwrap();

        assert_eq!(message.op_code, OP_REPLY);
        assert_eq!(decoded_docs.len(), 2);
        assert_eq!(decoded_docs[0].get_str("name").unwrap(), "Alice");
    }

    #[test]
    fn test_op_query_encode_decode() {
        let query = doc! {"status": "active"};
        let projection = Some(doc! {"name": 1, "_id": 0});

        let encoded = encode_op_query(123, "test.users", query.clone(), projection.clone(), 10, 20).unwrap();
        let message = WireMessage::decode(&encoded).unwrap().unwrap().0;
        let (database, collection, decoded_query, decoded_projection, skip, limit) = decode_op_query(&message.body).unwrap();

        assert_eq!(database, "test");
        assert_eq!(collection, "users");
        assert_eq!(decoded_query.get_str("status").unwrap(), "active");
        assert_eq!(skip, 10);
        assert_eq!(limit, 20);
        assert!(decoded_projection.is_some());
    }
}
