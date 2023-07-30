use super::redis_object::RedisObject;

#[derive(Debug, Clone)]
pub struct RedisEntry {
    pub id: u64,
    pub is_base: bool,
    pub db_id: i64,
    pub argv: Vec<String>,
    pub timestamp_ms: u64,

    pub cmd_name: String,
    pub group: String,
    pub keys: Vec<String>,
    pub slots: Vec<i32>,

    pub offset: i64,
    pub encoded_size: u64,

    pub expire_ms: i64,
    pub key: String,
    pub value: RedisObject,
    pub value_type_byte: u8,
    pub raw_bytes: Vec<u8>,

    pub position: String,
}

impl RedisEntry {
    pub fn new() -> Self {
        Self {
            id: 0,
            is_base: false,
            db_id: 0,
            argv: Vec::new(),
            timestamp_ms: 0,

            cmd_name: String::new(),
            group: String::new(),
            keys: Vec::new(),
            slots: Vec::new(),

            offset: 0,
            encoded_size: 0,

            expire_ms: 0,
            key: String::new(),
            value: RedisObject::Unknown,
            raw_bytes: Vec::new(),
            value_type_byte: 0,

            position: String::new(),
        }
    }
}
