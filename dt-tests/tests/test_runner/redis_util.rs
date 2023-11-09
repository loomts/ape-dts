use std::collections::HashMap;

use dt_common::config::config_token_parser::ConfigTokenParser;
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
use dt_meta::redis::redis_object::RedisCmd;
use redis::{Connection, ConnectionLike, Value};

const DELIMITERS: [char; 1] = [' '];
const DEFAULT_ESCAPE_PAIRS: [(char, char); 1] = [('"', '"')];
const SYSTEM_KEYS: [&str; 5] = [
    "backup1",
    "backup2",
    "backup3",
    "backup4",
    "ape_dts_heartbeat_key",
];

pub struct RedisUtil {
    escape_pairs: Vec<(char, char)>,
}

impl RedisUtil {
    pub fn new_default() -> Self {
        Self::new(DEFAULT_ESCAPE_PAIRS.to_vec())
    }

    pub fn new(escape_pairs: Vec<(char, char)>) -> Self {
        Self { escape_pairs }
    }

    pub fn get_hash_entry(&self, conn: &mut Connection, key: &str) -> HashMap<String, Value> {
        let cmd = format!("HGETALL {}", self.escape_key(key));
        let result = self.execute_cmd(conn, &cmd);

        let mut kvs = HashMap::new();
        if let redis::Value::Bulk(mut values) = result {
            for _i in (0..values.len()).step_by(2) {
                let k = values.remove(0);
                let v = values.remove(0);
                if let redis::Value::Data(k) = k {
                    kvs.insert(String::from_utf8(k).unwrap(), v);
                } else {
                    assert!(false);
                }
            }
        } else {
            assert!(false);
        }
        kvs
    }

    pub fn list_dbs(&self, conn: &mut Connection) -> Vec<String> {
        let mut dbs = Vec::new();
        let cmd = "INFO keyspace";
        match self.execute_cmd(conn, &cmd) {
            redis::Value::Data(data) => {
                let spaces = String::from_utf8(data).unwrap();
                for space in spaces.split("\r\n").collect::<Vec<&str>>() {
                    if space.contains("db") {
                        let tokens: Vec<&str> = space.split(":").collect::<Vec<&str>>();
                        dbs.push(tokens[0].trim_start_matches("db").to_string());
                    }
                }
            }
            _ => {}
        }
        dbs
    }

    pub fn list_keys(&self, conn: &mut Connection, match_pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let cmd = format!("KEYS {}", match_pattern);
        match self.execute_cmd(conn, &cmd) {
            redis::Value::Bulk(values) => {
                for v in values {
                    match v {
                        redis::Value::Data(data) => {
                            let key = String::from_utf8(data).unwrap();
                            if SYSTEM_KEYS.contains(&key.as_str()) {
                                continue;
                            }
                            keys.push(key)
                        }
                        _ => assert!(false),
                    }
                }
            }
            _ => assert!(false),
        }
        keys.sort();
        keys
    }

    pub fn get_key_type(&self, conn: &mut Connection, key: &str) -> String {
        let cmd = format!("type {}", self.escape_key(key));
        let value = self.execute_cmd(conn, &cmd);
        match value {
            redis::Value::Status(key_type) => {
                return key_type;
            }
            _ => assert!(false),
        }
        String::new()
    }

    pub fn escape_key(&self, key: &str) -> String {
        format!(
            "{}{}{}",
            self.escape_pairs[0].0, key, self.escape_pairs[0].1
        )
    }

    pub fn execute_cmds(&self, conn: &mut Connection, cmds: &Vec<String>) {
        for cmd in cmds.iter() {
            self.execute_cmd(conn, cmd);
        }
    }

    pub fn execute_cmd(&self, conn: &mut Connection, cmd: &str) -> Value {
        println!("execute cmd: {:?}", cmd);
        let packed_cmd = self.pack_cmd(cmd);
        conn.req_packed_command(&packed_cmd).unwrap()
    }

    fn pack_cmd(&self, cmd: &str) -> Vec<u8> {
        // parse cmd args
        let mut redis_cmd = RedisCmd::new();
        for arg in ConfigTokenParser::parse(cmd, &DELIMITERS, &self.escape_pairs) {
            let mut arg = arg.clone();
            for (left, right) in &self.escape_pairs {
                arg = arg
                    .trim_start_matches(*left)
                    .trim_end_matches(*right)
                    .to_string();
            }
            redis_cmd.add_str_arg(&arg);
        }
        CmdEncoder::encode(&redis_cmd)
    }
}
