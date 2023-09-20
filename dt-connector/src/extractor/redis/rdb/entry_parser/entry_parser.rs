use dt_common::error::Error;
use dt_meta::redis::redis_object::{RedisObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

use super::{
    hash_parser::HashLoader, list_parser::ListLoader, module2_parser::ModuleLoader,
    set_parser::SetLoader, stream_parser::StreamLoader, string_parser::StringLoader,
    zset_parser::ZsetLoader,
};

pub struct EntryParser {}

impl EntryParser {
    pub fn parse_object(
        reader: &mut RdbReader,
        type_byte: u8,
        key: RedisString,
    ) -> Result<RedisObject, Error> {
        let obj = match type_byte {
            super::RDB_TYPE_STRING => {
                RedisObject::String(StringLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_LIST
            | super::RDB_TYPE_LIST_ZIP_LIST
            | super::RDB_TYPE_LIST_QUICK_LIST
            | super::RDB_TYPE_LIST_QUICK_LIST_2 => {
                RedisObject::List(ListLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_SET | super::RDB_TYPE_SET_INT_SET => {
                RedisObject::Set(SetLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_ZSET
            | super::RDB_TYPE_ZSET_2
            | super::RDB_TYPE_ZSET_ZIP_LIST
            | super::RDB_TYPE_ZSET_LIST_PACK => {
                RedisObject::Zset(ZsetLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_HASH
            | super::RDB_TYPE_HASH_ZIP_MAP
            | super::RDB_TYPE_HASH_ZIP_LIST
            | super::RDB_TYPE_HASH_LIST_PACK => {
                RedisObject::Hash(HashLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_STREAM_LIST_PACKS | super::RDB_TYPE_STREAM_LIST_PACKS_2 => {
                RedisObject::Stream(StreamLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_MODULE | super::RDB_TYPE_MODULE_2 => {
                RedisObject::Module(ModuleLoader::load_from_buffer(reader, key, type_byte)?)
            }

            _ => {
                log::error!("unknown type byte: {}", type_byte);
                RedisObject::Unknown
            }
        };

        Ok(obj)
    }
}
