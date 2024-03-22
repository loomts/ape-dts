use dt_common::error::Error;
use dt_meta::redis::redis_object::{ModuleObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct ModuleParser {}

const MODULE_TYPE_NAME_CHAR_SET: &str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

impl ModuleParser {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        type_byte: u8,
    ) -> Result<ModuleObject, Error> {
        if type_byte == super::RDB_TYPE_MODULE {
            return Err(Error::RedisRdbError(format!(
                "module type with version 1 is not supported, key=[{}]",
                String::from(key)
            )));
        }

        let module_id = reader.read_length()?;
        let module_name = Self::module_type_name_by_id(module_id);
        // Not supported
        Err(Error::RedisRdbError(format!(
            "unsupported module type: [{}]",
            module_name
        )))
    }

    pub fn module_type_name_by_id(module_id: u64) -> String {
        let mut name_list: Vec<u8> = vec![0; 9];
        let mut module_id = module_id >> 10;
        let name_char_set = MODULE_TYPE_NAME_CHAR_SET.chars().collect::<Vec<char>>();

        for i in (0..9).rev() {
            name_list[i] = name_char_set[(module_id & 63) as usize] as u8;
            module_id >>= 6;
        }
        String::from_utf8(name_list).unwrap()
    }
}
