use dt_common::meta::redis::redis_object::{RedisString, StringObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct StringParser {}

impl StringParser {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        _type_byte: u8,
    ) -> anyhow::Result<StringObject> {
        let mut obj = StringObject::new();
        obj.key = key;
        obj.value = reader.read_string()?;
        Ok(obj)
    }
}
