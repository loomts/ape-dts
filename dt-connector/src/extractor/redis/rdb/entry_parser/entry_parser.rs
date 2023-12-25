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
            | super::RDB_TYPE_LIST_ZIPLIST
            | super::RDB_TYPE_LIST_QUICKLIST
            | super::RDB_TYPE_LIST_QUICKLIST_2 => {
                RedisObject::List(ListLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_SET | super::RDB_TYPE_SET_INTSET | super::RDB_TYPE_SET_LISTPACK => {
                RedisObject::Set(SetLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_ZSET
            | super::RDB_TYPE_ZSET_2
            | super::RDB_TYPE_ZSET_ZIPLIST
            | super::RDB_TYPE_ZSET_LISTPACK => {
                RedisObject::Zset(ZsetLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_HASH
            | super::RDB_TYPE_HASH_ZIPMAP
            | super::RDB_TYPE_HASH_ZIPLIST
            | super::RDB_TYPE_HASH_LISTPACK => {
                RedisObject::Hash(HashLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_STREAM_LISTPACKS
            | super::RDB_TYPE_STREAM_LISTPACKS_2
            | super::RDB_TYPE_STREAM_LISTPACKS_3 => {
                RedisObject::Stream(StreamLoader::load_from_buffer(reader, key, type_byte)?)
            }

            super::RDB_TYPE_MODULE | super::RDB_TYPE_MODULE_2 => {
                RedisObject::Module(ModuleLoader::load_from_buffer(reader, key, type_byte)?)
            }

            _ => {
                log::error!(
                    "unknown type byte: {}, key: {}",
                    type_byte,
                    String::from(key)
                );
                RedisObject::Unknown
            }
        };

        Ok(obj)
    }
}
