use crate::api::{key::RedisKey, value::RedisJsonValue};
use lazy_static::lazy_static;
use std::collections::HashMap;

// Multi-word Redis commands with pre-split parts for fast lookup.
// Redis expects each word of a command to be sent as a separate RESP argument.
// For example, "PUBSUB NUMPAT" must be sent as two arguments: "PUBSUB" and "NUMPAT".
// The parts are pre-split at compile time to avoid runtime string splitting.
lazy_static! {
    static ref MULTI_WORD_COMMANDS: HashMap<&'static str, &'static [&'static str]> = {
        HashMap::from([
            // ACL commands
            ("ACL CAT", &["ACL", "CAT"][..]),
            ("ACL DELUSER", &["ACL", "DELUSER"][..]),
            ("ACL DRYRUN", &["ACL", "DRYRUN"][..]),
            ("ACL GENPASS", &["ACL", "GENPASS"][..]),
            ("ACL GETUSER", &["ACL", "GETUSER"][..]),
            ("ACL HELP", &["ACL", "HELP"][..]),
            ("ACL LIST", &["ACL", "LIST"][..]),
            ("ACL LOAD", &["ACL", "LOAD"][..]),
            ("ACL LOG", &["ACL", "LOG"][..]),
            ("ACL SAVE", &["ACL", "SAVE"][..]),
            ("ACL SETUSER", &["ACL", "SETUSER"][..]),
            ("ACL USERS", &["ACL", "USERS"][..]),
            ("ACL WHOAMI", &["ACL", "WHOAMI"][..]),
            // CLIENT commands
            ("CLIENT CACHING", &["CLIENT", "CACHING"][..]),
            ("CLIENT GETNAME", &["CLIENT", "GETNAME"][..]),
            ("CLIENT GETREDIR", &["CLIENT", "GETREDIR"][..]),
            ("CLIENT HELP", &["CLIENT", "HELP"][..]),
            ("CLIENT ID", &["CLIENT", "ID"][..]),
            ("CLIENT INFO", &["CLIENT", "INFO"][..]),
            ("CLIENT KILL", &["CLIENT", "KILL"][..]),
            ("CLIENT LIST", &["CLIENT", "LIST"][..]),
            ("CLIENT NO-EVICT", &["CLIENT", "NO-EVICT"][..]),
            ("CLIENT NO-TOUCH", &["CLIENT", "NO-TOUCH"][..]),
            ("CLIENT PAUSE", &["CLIENT", "PAUSE"][..]),
            ("CLIENT REPLY", &["CLIENT", "REPLY"][..]),
            ("CLIENT SETINFO", &["CLIENT", "SETINFO"][..]),
            ("CLIENT SETNAME", &["CLIENT", "SETNAME"][..]),
            ("CLIENT TRACKINGINFO", &["CLIENT", "TRACKINGINFO"][..]),
            ("CLIENT UNBLOCK", &["CLIENT", "UNBLOCK"][..]),
            ("CLIENT UNPAUSE", &["CLIENT", "UNPAUSE"][..]),
            // CLUSTER commands
            ("CLUSTER ADDSLOTS", &["CLUSTER", "ADDSLOTS"][..]),
            ("CLUSTER ADDSLOTSRANGE", &["CLUSTER", "ADDSLOTSRANGE"][..]),
            ("CLUSTER BUMPEPOCH", &["CLUSTER", "BUMPEPOCH"][..]),
            ("CLUSTER COUNT-FAILURE-REPORTS", &["CLUSTER", "COUNT-FAILURE-REPORTS"][..]),
            ("CLUSTER COUNTKEYSINSLOT", &["CLUSTER", "COUNTKEYSINSLOT"][..]),
            ("CLUSTER DELSLOTS", &["CLUSTER", "DELSLOTS"][..]),
            ("CLUSTER DELSLOTSRANGE", &["CLUSTER", "DELSLOTSRANGE"][..]),
            ("CLUSTER FAILOVER", &["CLUSTER", "FAILOVER"][..]),
            ("CLUSTER FLUSHSLOTS", &["CLUSTER", "FLUSHSLOTS"][..]),
            ("CLUSTER FORGET", &["CLUSTER", "FORGET"][..]),
            ("CLUSTER GETKEYSINSLOT", &["CLUSTER", "GETKEYSINSLOT"][..]),
            ("CLUSTER HELP", &["CLUSTER", "HELP"][..]),
            ("CLUSTER INFO", &["CLUSTER", "INFO"][..]),
            ("CLUSTER KEYSLOT", &["CLUSTER", "KEYSLOT"][..]),
            ("CLUSTER LINKS", &["CLUSTER", "LINKS"][..]),
            ("CLUSTER MEET", &["CLUSTER", "MEET"][..]),
            ("CLUSTER MYID", &["CLUSTER", "MYID"][..]),
            ("CLUSTER MYSHARDID", &["CLUSTER", "MYSHARDID"][..]),
            ("CLUSTER NODES", &["CLUSTER", "NODES"][..]),
            ("CLUSTER REPLICAS", &["CLUSTER", "REPLICAS"][..]),
            ("CLUSTER REPLICATE", &["CLUSTER", "REPLICATE"][..]),
            ("CLUSTER RESET", &["CLUSTER", "RESET"][..]),
            ("CLUSTER SAVECONFIG", &["CLUSTER", "SAVECONFIG"][..]),
            ("CLUSTER SET-CONFIG-EPOCH", &["CLUSTER", "SET-CONFIG-EPOCH"][..]),
            ("CLUSTER SETSLOT", &["CLUSTER", "SETSLOT"][..]),
            ("CLUSTER SHARDS", &["CLUSTER", "SHARDS"][..]),
            ("CLUSTER SLAVES", &["CLUSTER", "SLAVES"][..]),
            ("CLUSTER SLOTS", &["CLUSTER", "SLOTS"][..]),
            // CONFIG commands
            ("CONFIG GET", &["CONFIG", "GET"][..]),
            ("CONFIG HELP", &["CONFIG", "HELP"][..]),
            ("CONFIG RESETSTAT", &["CONFIG", "RESETSTAT"][..]),
            ("CONFIG REWRITE", &["CONFIG", "REWRITE"][..]),
            ("CONFIG SET", &["CONFIG", "SET"][..]),
            // COMMAND subcommands
            ("COMMAND COUNT", &["COMMAND", "COUNT"][..]),
            ("COMMAND DOCS", &["COMMAND", "DOCS"][..]),
            ("COMMAND GETKEYS", &["COMMAND", "GETKEYS"][..]),
            ("COMMAND GETKEYSANDFLAGS", &["COMMAND", "GETKEYSANDFLAGS"][..]),
            ("COMMAND INFO", &["COMMAND", "INFO"][..]),
            ("COMMAND LIST", &["COMMAND", "LIST"][..]),
            // DEBUG commands
            ("DEBUG OBJECT", &["DEBUG", "OBJECT"][..]),
            ("DEBUG SEGFAULT", &["DEBUG", "SEGFAULT"][..]),
            // FUNCTION commands
            ("FUNCTION DELETE", &["FUNCTION", "DELETE"][..]),
            ("FUNCTION DUMP", &["FUNCTION", "DUMP"][..]),
            ("FUNCTION FLUSH", &["FUNCTION", "FLUSH"][..]),
            ("FUNCTION HELP", &["FUNCTION", "HELP"][..]),
            ("FUNCTION KILL", &["FUNCTION", "KILL"][..]),
            ("FUNCTION LIST", &["FUNCTION", "LIST"][..]),
            ("FUNCTION LOAD", &["FUNCTION", "LOAD"][..]),
            ("FUNCTION RESTORE", &["FUNCTION", "RESTORE"][..]),
            ("FUNCTION STATS", &["FUNCTION", "STATS"][..]),
            // LATENCY commands
            ("LATENCY DOCTOR", &["LATENCY", "DOCTOR"][..]),
            ("LATENCY GRAPH", &["LATENCY", "GRAPH"][..]),
            ("LATENCY HELP", &["LATENCY", "HELP"][..]),
            ("LATENCY HISTOGRAM", &["LATENCY", "HISTOGRAM"][..]),
            ("LATENCY HISTORY", &["LATENCY", "HISTORY"][..]),
            ("LATENCY LATEST", &["LATENCY", "LATEST"][..]),
            ("LATENCY RESET", &["LATENCY", "RESET"][..]),
            // MEMORY commands
            ("MEMORY DOCTOR", &["MEMORY", "DOCTOR"][..]),
            ("MEMORY HELP", &["MEMORY", "HELP"][..]),
            ("MEMORY MALLOC-SIZE", &["MEMORY", "MALLOC-SIZE"][..]),
            ("MEMORY MALLOC-STATS", &["MEMORY", "MALLOC-STATS"][..]),
            ("MEMORY PURGE", &["MEMORY", "PURGE"][..]),
            ("MEMORY STATS", &["MEMORY", "STATS"][..]),
            ("MEMORY USAGE", &["MEMORY", "USAGE"][..]),
            // MODULE commands
            ("MODULE HELP", &["MODULE", "HELP"][..]),
            ("MODULE LIST", &["MODULE", "LIST"][..]),
            ("MODULE LOAD", &["MODULE", "LOAD"][..]),
            ("MODULE LOADEX", &["MODULE", "LOADEX"][..]),
            ("MODULE UNLOAD", &["MODULE", "UNLOAD"][..]),
            // OBJECT commands
            ("OBJECT ENCODING", &["OBJECT", "ENCODING"][..]),
            ("OBJECT FREQ", &["OBJECT", "FREQ"][..]),
            ("OBJECT HELP", &["OBJECT", "HELP"][..]),
            ("OBJECT IDLETIME", &["OBJECT", "IDLETIME"][..]),
            ("OBJECT REFCOUNT", &["OBJECT", "REFCOUNT"][..]),
            // PUBSUB commands
            ("PUBSUB CHANNELS", &["PUBSUB", "CHANNELS"][..]),
            ("PUBSUB HELP", &["PUBSUB", "HELP"][..]),
            ("PUBSUB NUMPAT", &["PUBSUB", "NUMPAT"][..]),
            ("PUBSUB NUMSUB", &["PUBSUB", "NUMSUB"][..]),
            ("PUBSUB SHARDCHANNELS", &["PUBSUB", "SHARDCHANNELS"][..]),
            ("PUBSUB SHARDNUMSUB", &["PUBSUB", "SHARDNUMSUB"][..]),
            // SCRIPT commands
            ("SCRIPT DEBUG", &["SCRIPT", "DEBUG"][..]),
            ("SCRIPT EXISTS", &["SCRIPT", "EXISTS"][..]),
            ("SCRIPT FLUSH", &["SCRIPT", "FLUSH"][..]),
            ("SCRIPT HELP", &["SCRIPT", "HELP"][..]),
            ("SCRIPT KILL", &["SCRIPT", "KILL"][..]),
            ("SCRIPT LOAD", &["SCRIPT", "LOAD"][..]),
            // SENTINEL commands
            ("SENTINEL CKQUORUM", &["SENTINEL", "CKQUORUM"][..]),
            ("SENTINEL CONFIG", &["SENTINEL", "CONFIG"][..]),
            ("SENTINEL DEBUG", &["SENTINEL", "DEBUG"][..]),
            ("SENTINEL FAILOVER", &["SENTINEL", "FAILOVER"][..]),
            ("SENTINEL FLUSHCONFIG", &["SENTINEL", "FLUSHCONFIG"][..]),
            ("SENTINEL GET-MASTER-ADDR-BY-NAME", &["SENTINEL", "GET-MASTER-ADDR-BY-NAME"][..]),
            ("SENTINEL HELP", &["SENTINEL", "HELP"][..]),
            ("SENTINEL INFO-CACHE", &["SENTINEL", "INFO-CACHE"][..]),
            ("SENTINEL IS-MASTER-DOWN-BY-ADDR", &["SENTINEL", "IS-MASTER-DOWN-BY-ADDR"][..]),
            ("SENTINEL MASTER", &["SENTINEL", "MASTER"][..]),
            ("SENTINEL MASTERS", &["SENTINEL", "MASTERS"][..]),
            ("SENTINEL MONITOR", &["SENTINEL", "MONITOR"][..]),
            ("SENTINEL MYID", &["SENTINEL", "MYID"][..]),
            ("SENTINEL PENDING-SCRIPTS", &["SENTINEL", "PENDING-SCRIPTS"][..]),
            ("SENTINEL REMOVE", &["SENTINEL", "REMOVE"][..]),
            ("SENTINEL REPLICAS", &["SENTINEL", "REPLICAS"][..]),
            ("SENTINEL RESET", &["SENTINEL", "RESET"][..]),
            ("SENTINEL SENTINELS", &["SENTINEL", "SENTINELS"][..]),
            ("SENTINEL SET", &["SENTINEL", "SET"][..]),
            ("SENTINEL SIMULATE-FAILURE", &["SENTINEL", "SIMULATE-FAILURE"][..]),
            ("SENTINEL SLAVES", &["SENTINEL", "SLAVES"][..]),
            // SLOWLOG commands
            ("SLOWLOG GET", &["SLOWLOG", "GET"][..]),
            ("SLOWLOG HELP", &["SLOWLOG", "HELP"][..]),
            ("SLOWLOG LEN", &["SLOWLOG", "LEN"][..]),
            ("SLOWLOG RESET", &["SLOWLOG", "RESET"][..]),
            // XGROUP commands
            ("XGROUP CREATE", &["XGROUP", "CREATE"][..]),
            ("XGROUP CREATECONSUMER", &["XGROUP", "CREATECONSUMER"][..]),
            ("XGROUP DELCONSUMER", &["XGROUP", "DELCONSUMER"][..]),
            ("XGROUP DESTROY", &["XGROUP", "DESTROY"][..]),
            ("XGROUP HELP", &["XGROUP", "HELP"][..]),
            ("XGROUP SETID", &["XGROUP", "SETID"][..]),
            // XINFO commands
            ("XINFO CONSUMERS", &["XINFO", "CONSUMERS"][..]),
            ("XINFO GROUPS", &["XINFO", "GROUPS"][..]),
            ("XINFO HELP", &["XINFO", "HELP"][..]),
            ("XINFO STREAM", &["XINFO", "STREAM"][..]),
        ])
    };
}

pub trait RedisWrite {
    fn write_arg(&mut self, arg: &[u8]);
    fn write_arg_fmt(&mut self, arg: impl std::fmt::Display);
}

#[derive(Clone, Debug)]
pub struct Cmd {
    data: bytes::BytesMut,
    args: Vec<usize>, // stores end positions of each arg
}

impl Cmd {
    pub fn new() -> Self {
        Self { data: bytes::BytesMut::new(), args: Vec::new() }
    }

    pub fn arg<T: ToRedisArgs>(&mut self, arg: T) -> &mut Self {
        arg.write_redis_args(self);
        self
    }

    pub fn get_packed_command(&self) -> bytes::Bytes {
        let mut cmd = bytes::BytesMut::new();
        self.write_packed_command(&mut cmd);
        cmd.freeze()
    }

    fn write_packed_command(&self, out: &mut bytes::BytesMut) {
        // Write array length using itoa for efficient integer formatting
        out.extend_from_slice(b"*");
        let mut buf = itoa::Buffer::new();
        out.extend_from_slice(buf.format(self.args.len()).as_bytes());
        out.extend_from_slice(b"\r\n");

        let mut prev = 0;
        for &end in &self.args {
            let arg = &self.data[prev..end];
            out.extend_from_slice(b"$");
            out.extend_from_slice(buf.format(arg.len()).as_bytes());
            out.extend_from_slice(b"\r\n");
            out.extend_from_slice(arg);
            out.extend_from_slice(b"\r\n");
            prev = end;
        }
    }
}

impl Default for Cmd {
    fn default() -> Self {
        Self::new()
    }
}

impl RedisWrite for Cmd {
    fn write_arg(&mut self, arg: &[u8]) {
        self.data.extend_from_slice(arg);
        self.args.push(self.data.len());
    }

    fn write_arg_fmt(&mut self, arg: impl std::fmt::Display) {
        // BytesMut doesn't implement std::fmt::Write, so we format to a string first
        let s = format!("{}", arg);
        self.data.extend_from_slice(s.as_bytes());
        self.args.push(self.data.len());
    }
}

pub trait ToRedisArgs {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W);
}

impl ToRedisArgs for bool {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(if *self { b"1" } else { b"0" });
    }
}

impl<T> ToRedisArgs for Option<T>
where
    T: ToRedisArgs,
{
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        match self {
            Some(v) => v.write_redis_args(out),
            None => out.write_arg(b""),
        }
    }
}

impl ToRedisArgs for () {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, _out: &mut W) {
        // Unit type writes nothing
    }
}

impl ToRedisArgs for &str {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes());
    }
}

impl ToRedisArgs for String {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes());
    }
}

impl ToRedisArgs for &String {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes());
    }
}

impl ToRedisArgs for i8 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for i16 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for i32 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for i64 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for i128 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for isize {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for u8 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for u16 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for u32 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for u64 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for u128 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for usize {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for f32 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for f64 {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg_fmt(self);
    }
}

impl ToRedisArgs for &Vec<u8> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self);
    }
}

impl ToRedisArgs for &[u8] {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self);
    }
}

impl ToRedisArgs for bytes::Bytes {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self);
    }
}

impl ToRedisArgs for &bytes::Bytes {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self);
    }
}

impl ToRedisArgs for Vec<String> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for &Vec<String> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for Vec<&str> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for &[String] {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for &[&str] {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for Vec<RedisJsonValue> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for &Vec<RedisJsonValue> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl ToRedisArgs for &[RedisJsonValue] {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            item.write_redis_args(out);
        }
    }
}

impl<T> ToRedisArgs for &Option<T>
where
    T: ToRedisArgs,
{
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        match self {
            Some(v) => v.write_redis_args(out),
            None => out.write_arg(b""),
        }
    }
}

impl ToRedisArgs for &RedisJsonValue {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        (*self).write_redis_args(out)
    }
}

impl ToRedisArgs for RedisJsonValue {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        match self {
            RedisJsonValue::Null => out.write_arg(b"nil"),
            RedisJsonValue::Bool(b) => b.write_redis_args(out),
            RedisJsonValue::Bytes(b) => b.write_redis_args(out),
            RedisJsonValue::Integer(i) => i.write_redis_args(out),
            RedisJsonValue::Float(f) => out.write_arg_fmt(f),
            RedisJsonValue::String(s) => s.write_redis_args(out),
            RedisJsonValue::Array(a) => {
                for item in a {
                    item.write_redis_args(out);
                }
            }
            RedisJsonValue::Object(o) => out.write_arg(serde_json::to_string(o).unwrap_or_default().as_bytes()),
        }
    }
}

impl ToRedisArgs for RedisKey {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes())
    }
}

impl ToRedisArgs for &RedisKey {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        out.write_arg(self.as_bytes())
    }
}

impl ToRedisArgs for Vec<RedisKey> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            out.write_arg(item.as_bytes())
        }
    }
}

impl ToRedisArgs for &Vec<RedisKey> {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            out.write_arg(item.as_bytes())
        }
    }
}

impl ToRedisArgs for &[RedisKey] {
    fn write_redis_args<W: RedisWrite + ?Sized>(&self, out: &mut W) {
        for item in self.iter() {
            out.write_arg(item.as_bytes())
        }
    }
}

/// Creates a new Redis command.
///
/// For multi-word commands (like "PUBSUB NUMPAT"), this function automatically
/// splits them into separate arguments as required by the Redis protocol.
/// Single-word commands are passed through unchanged.
pub fn cmd(name: &str) -> Cmd {
    let mut cmd = Cmd::new();

    if let Some(parts) = MULTI_WORD_COMMANDS.get(name) {
        for part in *parts {
            cmd.arg(*part);
        }
    } else {
        cmd.arg(name);
    }

    cmd
}

#[cfg(test)]
mod tests {
    use super::*;

    mod cmd_function {
        use super::*;

        #[test]
        fn test_single_word_command() {
            let command = cmd("GET").arg("mykey").get_packed_command();
            assert_eq!(command.to_vec(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_multi_word_command_cluster_bumpepoch() {
            let command = cmd("CLUSTER BUMPEPOCH").get_packed_command();
            // Should split into two arguments: CLUSTER and BUMPEPOCH
            assert_eq!(command.to_vec(), b"*2\r\n$7\r\nCLUSTER\r\n$9\r\nBUMPEPOCH\r\n");
        }

        #[test]
        fn test_multi_word_command_client_setname() {
            let command = cmd("CLIENT SETNAME").arg("myconnection").get_packed_command();
            // Should split into: CLIENT, SETNAME, myconnection
            assert_eq!(command.to_vec(), b"*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$12\r\nmyconnection\r\n");
        }

        #[test]
        fn test_multi_word_command_xgroup_create() {
            let command = cmd("XGROUP CREATE").arg("mystream").arg("mygroup").arg("$").get_packed_command();
            assert_eq!(
                command.to_vec(),
                b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n"
            );
        }

        #[test]
        fn test_multi_word_command_pubsub_numpat() {
            let command = cmd("PUBSUB NUMPAT").get_packed_command();
            assert_eq!(command.to_vec(), b"*2\r\n$6\r\nPUBSUB\r\n$6\r\nNUMPAT\r\n");
        }

        #[test]
        fn test_multi_word_command_config_get() {
            let command = cmd("CONFIG GET").arg("maxclients").get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$10\r\nmaxclients\r\n");
        }

        #[test]
        fn test_unknown_multi_word_treated_as_single() {
            // Unknown multi-word commands should be treated as single argument
            let command = cmd("UNKNOWN COMMAND").get_packed_command();
            assert_eq!(command.to_vec(), b"*1\r\n$15\r\nUNKNOWN COMMAND\r\n");
        }
    }

    mod multi_word_commands_map {
        use super::*;

        #[test]
        fn test_all_acl_commands_present() {
            let acl_commands = [
                "ACL CAT",
                "ACL DELUSER",
                "ACL DRYRUN",
                "ACL GENPASS",
                "ACL GETUSER",
                "ACL HELP",
                "ACL LIST",
                "ACL LOAD",
                "ACL LOG",
                "ACL SAVE",
                "ACL SETUSER",
                "ACL WHOAMI",
            ];
            for cmd_name in acl_commands {
                assert!(MULTI_WORD_COMMANDS.contains_key(cmd_name), "Missing command: {}", cmd_name);
            }
        }

        #[test]
        fn test_all_client_commands_present() {
            let client_commands = [
                "CLIENT CACHING",
                "CLIENT GETNAME",
                "CLIENT GETREDIR",
                "CLIENT HELP",
                "CLIENT ID",
                "CLIENT INFO",
                "CLIENT KILL",
                "CLIENT LIST",
                "CLIENT NO-EVICT",
                "CLIENT NO-TOUCH",
                "CLIENT PAUSE",
                "CLIENT REPLY",
                "CLIENT SETINFO",
                "CLIENT SETNAME",
                "CLIENT TRACKINGINFO",
                "CLIENT UNBLOCK",
                "CLIENT UNPAUSE",
            ];
            for cmd_name in client_commands {
                assert!(MULTI_WORD_COMMANDS.contains_key(cmd_name), "Missing command: {}", cmd_name);
            }
        }

        #[test]
        fn test_all_cluster_commands_present() {
            let cluster_commands = [
                "CLUSTER ADDSLOTS",
                "CLUSTER ADDSLOTSRANGE",
                "CLUSTER BUMPEPOCH",
                "CLUSTER COUNT-FAILURE-REPORTS",
                "CLUSTER COUNTKEYSINSLOT",
                "CLUSTER DELSLOTS",
                "CLUSTER DELSLOTSRANGE",
                "CLUSTER FAILOVER",
                "CLUSTER FLUSHSLOTS",
                "CLUSTER FORGET",
                "CLUSTER GETKEYSINSLOT",
                "CLUSTER HELP",
                "CLUSTER INFO",
                "CLUSTER KEYSLOT",
                "CLUSTER LINKS",
                "CLUSTER MEET",
                "CLUSTER MYID",
                "CLUSTER MYSHARDID",
                "CLUSTER NODES",
                "CLUSTER REPLICAS",
                "CLUSTER REPLICATE",
                "CLUSTER RESET",
                "CLUSTER SAVECONFIG",
                "CLUSTER SET-CONFIG-EPOCH",
                "CLUSTER SETSLOT",
                "CLUSTER SHARDS",
                "CLUSTER SLAVES",
                "CLUSTER SLOTS",
            ];
            for cmd_name in cluster_commands {
                assert!(MULTI_WORD_COMMANDS.contains_key(cmd_name), "Missing command: {}", cmd_name);
            }
        }

        #[test]
        fn test_all_xgroup_commands_present() {
            let xgroup_commands = [
                "XGROUP CREATE",
                "XGROUP CREATECONSUMER",
                "XGROUP DELCONSUMER",
                "XGROUP DESTROY",
                "XGROUP HELP",
                "XGROUP SETID",
            ];
            for cmd_name in xgroup_commands {
                assert!(MULTI_WORD_COMMANDS.contains_key(cmd_name), "Missing command: {}", cmd_name);
            }
        }

        #[test]
        fn test_all_xinfo_commands_present() {
            let xinfo_commands = ["XINFO CONSUMERS", "XINFO GROUPS", "XINFO HELP", "XINFO STREAM"];
            for cmd_name in xinfo_commands {
                assert!(MULTI_WORD_COMMANDS.contains_key(cmd_name), "Missing command: {}", cmd_name);
            }
        }

        #[test]
        fn test_command_parts_are_correct() {
            // Verify that the split parts are correct
            assert_eq!(MULTI_WORD_COMMANDS.get("CLIENT SETNAME").copied(), Some(&["CLIENT", "SETNAME"][..]));
            assert_eq!(MULTI_WORD_COMMANDS.get("CLUSTER BUMPEPOCH").copied(), Some(&["CLUSTER", "BUMPEPOCH"][..]));
            assert_eq!(MULTI_WORD_COMMANDS.get("XGROUP CREATE").copied(), Some(&["XGROUP", "CREATE"][..]));
            assert_eq!(MULTI_WORD_COMMANDS.get("CONFIG GET").copied(), Some(&["CONFIG", "GET"][..]));
        }

        #[test]
        fn test_hyphenated_commands() {
            // Commands with hyphens should be in the map
            assert!(MULTI_WORD_COMMANDS.contains_key("CLIENT NO-EVICT"));
            assert!(MULTI_WORD_COMMANDS.contains_key("CLIENT NO-TOUCH"));
            assert!(MULTI_WORD_COMMANDS.contains_key("CLUSTER COUNT-FAILURE-REPORTS"));
            assert!(MULTI_WORD_COMMANDS.contains_key("CLUSTER SET-CONFIG-EPOCH"));
            assert!(MULTI_WORD_COMMANDS.contains_key("MEMORY MALLOC-SIZE"));
            assert!(MULTI_WORD_COMMANDS.contains_key("SENTINEL GET-MASTER-ADDR-BY-NAME"));
            assert!(MULTI_WORD_COMMANDS.contains_key("SENTINEL IS-MASTER-DOWN-BY-ADDR"));
            assert!(MULTI_WORD_COMMANDS.contains_key("SENTINEL INFO-CACHE"));
            assert!(MULTI_WORD_COMMANDS.contains_key("SENTINEL PENDING-SCRIPTS"));
            assert!(MULTI_WORD_COMMANDS.contains_key("SENTINEL SIMULATE-FAILURE"));
        }
    }

    mod to_redis_args {
        use super::*;

        #[test]
        fn test_string_arg() {
            let command = cmd("SET").arg("key").arg("value").get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        }

        #[test]
        fn test_integer_args() {
            let command = cmd("SETEX").arg("key").arg(60i32).arg("value").get_packed_command();
            assert_eq!(command.to_vec(), b"*4\r\n$5\r\nSETEX\r\n$3\r\nkey\r\n$2\r\n60\r\n$5\r\nvalue\r\n");
        }

        #[test]
        fn test_bool_true() {
            let mut cmd = Cmd::new();
            cmd.arg(true);
            let packed = cmd.get_packed_command();
            assert_eq!(packed.to_vec(), b"*1\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_bool_false() {
            let mut cmd = Cmd::new();
            cmd.arg(false);
            let packed = cmd.get_packed_command();
            assert_eq!(packed.to_vec(), b"*1\r\n$1\r\n0\r\n");
        }

        #[test]
        fn test_bytes_arg() {
            let command = cmd("SET").arg("key").arg(b"binary\x00data".as_slice()).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nbinary\x00data\r\n");
        }

        #[test]
        fn test_vec_string_args() {
            let keys = vec!["key1".to_string(), "key2".to_string()];
            let command = cmd("MGET").arg(&keys).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n");
        }

        #[test]
        fn test_redis_json_value_string() {
            let val = RedisJsonValue::String("hello".to_string());
            let command = cmd("SET").arg("key").arg(val).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n");
        }

        #[test]
        fn test_redis_json_value_integer() {
            let val = RedisJsonValue::Integer(42);
            let command = cmd("SET").arg("key").arg(val).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$2\r\n42\r\n");
        }

        #[test]
        fn test_redis_json_value_null() {
            let val = RedisJsonValue::Null;
            let command = cmd("SET").arg("key").arg(val).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nnil\r\n");
        }

        #[test]
        fn test_redis_key() {
            let key = RedisKey::from("mykey");
            let command = cmd("GET").arg(key).get_packed_command();
            assert_eq!(command.to_vec(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_option_some() {
            let opt: Option<&str> = Some("value");
            let command = cmd("SET").arg("key").arg(opt).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n");
        }

        #[test]
        fn test_option_none() {
            let opt: Option<&str> = None;
            let command = cmd("SET").arg("key").arg(opt).get_packed_command();
            // None writes an empty argument
            assert_eq!(command.to_vec(), b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$0\r\n\r\n");
        }

        #[test]
        fn test_float_arg() {
            let command = cmd("INCRBYFLOAT").arg("key").arg(1.5f64).get_packed_command();
            assert_eq!(command.to_vec(), b"*3\r\n$11\r\nINCRBYFLOAT\r\n$3\r\nkey\r\n$3\r\n1.5\r\n");
        }
    }

    mod cmd_struct {
        use super::*;

        #[test]
        fn test_new_creates_empty_cmd() {
            let cmd = Cmd::new();
            assert!(cmd.data.is_empty());
            assert!(cmd.args.is_empty());
        }

        #[test]
        fn test_default_creates_empty_cmd() {
            let cmd = Cmd::default();
            assert!(cmd.data.is_empty());
            assert!(cmd.args.is_empty());
        }

        #[test]
        fn test_chained_args() {
            let command = cmd("ZADD").arg("myset").arg(1i32).arg("one").arg(2i32).arg("two").get_packed_command();
            assert_eq!(
                command.to_vec(),
                b"*6\r\n$4\r\nZADD\r\n$5\r\nmyset\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n"
            );
        }

        #[test]
        fn test_clone() {
            let mut cmd1 = cmd("GET");
            cmd1.arg("key");
            let cmd2 = cmd1.clone();
            assert_eq!(cmd1.get_packed_command(), cmd2.get_packed_command());
        }
    }
}
