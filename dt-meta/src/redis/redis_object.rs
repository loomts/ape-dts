use std::collections::HashMap;

use super::command::key_parser::KeyParser;

#[derive(Debug, Clone)]
pub enum RedisObject {
    String(StringObject),
    List(ListObject),
    Hash(HashObject),
    Set(SetObject),
    Zset(ZsetObject),
    Module(ModuleObject),
    Stream(StreamObject),
    Unknown,
}

#[derive(Debug, Clone)]
pub struct HashObject {
    pub key: RedisString,
    pub value: HashMap<RedisString, RedisString>,
}

impl HashObject {
    pub fn new() -> Self {
        HashObject {
            key: RedisString::new(),
            value: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListObject {
    pub key: RedisString,
    pub elements: Vec<RedisString>,
}

impl ListObject {
    pub fn new() -> Self {
        ListObject {
            key: RedisString::new(),
            elements: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct ModuleObject {}

impl ModuleObject {
    pub fn new() -> Self {
        ModuleObject {}
    }
}

#[derive(Debug, Clone)]
pub struct SetObject {
    pub key: RedisString,
    pub elements: Vec<RedisString>,
}

impl SetObject {
    pub fn new() -> Self {
        SetObject {
            key: RedisString::new(),
            elements: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamObject {
    pub key: RedisString,
    pub cmds: Vec<RedisCmd>,
}

impl StreamObject {
    pub fn new() -> Self {
        StreamObject {
            key: RedisString::new(),
            cmds: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct StringObject {
    pub key: RedisString,
    pub value: RedisString,
}

impl StringObject {
    pub fn new() -> Self {
        StringObject {
            key: RedisString::new(),
            value: RedisString::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZSetEntry {
    pub member: RedisString,
    pub score: RedisString,
}

#[derive(Debug, Clone)]
pub struct ZsetObject {
    pub key: RedisString,
    pub elements: Vec<ZSetEntry>,
}

impl ZsetObject {
    pub fn new() -> Self {
        ZsetObject {
            key: RedisString::new(),
            elements: vec![],
        }
    }
}

/// raw bytes
#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct RedisString {
    pub bytes: Vec<u8>,
}

impl RedisString {
    pub fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }
}

impl ToString for RedisString {
    fn to_string(&self) -> String {
        String::from_utf8_lossy(&self.bytes).to_string()
    }
}

impl From<Vec<u8>> for RedisString {
    fn from(bytes: Vec<u8>) -> Self {
        Self { bytes }
    }
}

impl From<String> for RedisString {
    fn from(str: String) -> Self {
        Self {
            bytes: str.as_bytes().to_vec(),
        }
    }
}

impl From<RedisString> for String {
    fn from(redis_string: RedisString) -> Self {
        String::from_utf8(redis_string.bytes).unwrap()
    }
}

#[derive(Debug, Clone, Default)]
pub struct RedisCmd {
    pub args: Vec<Vec<u8>>,
    pub name: String,
    pub group: String,
    pub keys: Vec<String>,
    pub key_indexes: Vec<usize>,
}

impl RedisCmd {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_args(args: Vec<Vec<u8>>) -> Self {
        let mut me = Self::new();
        for arg in args {
            me.args.push(arg);
        }
        me
    }

    pub fn from_str_args(args: &[&str]) -> Self {
        let mut me = Self::new();
        for arg in args.iter() {
            me.args.push(arg.as_bytes().to_vec());
        }
        me
    }

    pub fn add_arg(&mut self, arg: Vec<u8>) {
        self.args.push(arg);
    }

    pub fn add_str_arg(&mut self, arg: &str) {
        self.args.push(arg.as_bytes().to_vec());
    }

    pub fn add_redis_arg(&mut self, arg: &RedisString) {
        self.args.push(arg.as_bytes().to_vec());
    }

    pub fn get_name(&self) -> String {
        if self.name.is_empty() {
            return self.get_str_arg(0);
        }
        self.name.clone()
    }

    pub fn get_str_arg(&self, idx: usize) -> String {
        if self.args.len() <= idx {
            String::new()
        } else {
            String::from_utf8_lossy(&self.args[idx]).to_string()
        }
    }

    pub fn args_to_string(&self) -> Vec<String> {
        let mut str_args = Vec::new();
        for arg in self.args.iter() {
            str_args.push(String::from_utf8_lossy(arg).to_string());
        }
        str_args
    }

    pub fn get_malloc_size(&self) -> usize {
        let mut size = 0;
        for arg in &self.args {
            size += arg.len();
        }
        size
    }

    pub fn parse_keys(&mut self, key_parser: &KeyParser) {
        let args = self.args_to_string();
        let (cmd_name, group, keys, keys_indexes) = key_parser.parse_key_from_argv(&args).unwrap();
        self.name = cmd_name;
        self.group = group;
        self.keys = keys;
        self.key_indexes = keys_indexes;
    }
}

impl ToString for RedisCmd {
    fn to_string(&self) -> String {
        let str_args = self.args_to_string();
        str_args.join(" ")
    }
}

impl RedisObject {
    pub fn get_malloc_size(&self) -> usize {
        match self {
            RedisObject::String(v) => v.key.bytes.len() + v.value.bytes.len(),
            RedisObject::List(v) => {
                let mut size = 0;
                for i in v.elements.iter() {
                    size += i.bytes.len();
                }
                size + v.key.bytes.len()
            }
            RedisObject::Hash(v) => {
                let mut size = 0;
                for (key, value) in v.value.iter() {
                    size += key.bytes.len() + value.bytes.len();
                }
                size + v.key.bytes.len()
            }
            RedisObject::Set(v) => {
                let mut size = 0;
                for i in v.elements.iter() {
                    size += i.bytes.len();
                }
                size + v.key.bytes.len()
            }
            RedisObject::Zset(v) => {
                let mut size = 0;
                for i in v.elements.iter() {
                    size += i.member.bytes.len() + i.score.bytes.len()
                }
                size + v.key.bytes.len()
            }
            RedisObject::Stream(v) => {
                let mut size = 0;
                for cmd in v.cmds.iter() {
                    for arg in cmd.args.iter() {
                        size += arg.len();
                    }
                }
                size + v.key.bytes.len()
            }
            RedisObject::Module(_) => 0,
            RedisObject::Unknown => 0,
        }
    }
}
