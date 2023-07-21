use std::collections::HashMap;

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
    pub key: String,
    pub value: HashMap<String, String>,
}

impl HashObject {
    pub fn new() -> Self {
        HashObject {
            key: String::new(),
            value: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ListObject {
    pub key: String,
    pub elements: Vec<String>,
}

impl ListObject {
    pub fn new() -> Self {
        ListObject {
            key: String::new(),
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
    pub key: String,
    pub elements: Vec<String>,
}

impl SetObject {
    pub fn new() -> Self {
        SetObject {
            key: String::new(),
            elements: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct StreamObject {
    pub key: String,
    pub cmds: Vec<RedisCmd>,
}

impl StreamObject {
    pub fn new() -> Self {
        StreamObject {
            key: String::new(),
            cmds: vec![],
        }
    }
}

#[derive(Debug, Clone)]
pub struct StringObject {
    pub key: String,
    pub value: String,
}

impl StringObject {
    pub fn new() -> Self {
        StringObject {
            key: String::new(),
            value: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ZSetEntry {
    pub member: String,
    pub score: String,
}

#[derive(Debug, Clone)]
pub struct ZsetObject {
    pub key: String,
    pub elements: Vec<ZSetEntry>,
}

impl ZsetObject {
    pub fn new() -> Self {
        ZsetObject {
            key: String::new(),
            elements: vec![],
        }
    }
}

pub type RedisCmd = Vec<String>;
