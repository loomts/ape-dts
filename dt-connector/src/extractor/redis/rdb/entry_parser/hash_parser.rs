use dt_common::error::Error;
use dt_meta::redis::redis_object::{HashObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct HashLoader {}

impl HashLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        type_byte: u8,
    ) -> Result<HashObject, Error> {
        let mut obj = HashObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_HASH => Self::read_hash(&mut obj, reader)?,
            super::RDB_TYPE_HASH_ZIP_MAP => Self::read_hash_zip_map(&mut obj, reader)?,
            super::RDB_TYPE_HASH_ZIP_LIST => Self::read_hash_zip_list(&mut obj, reader)?,
            super::RDB_TYPE_HASH_LIST_PACK => Self::read_hash_list_pack(&mut obj, reader)?,
            _ => {
                return Err(Error::RedisRdbError(format!(
                    "unknown hash type. type_byte=[{}]",
                    type_byte
                )))
            }
        }
        Ok(obj)
    }

    fn read_hash(obj: &mut HashObject, reader: &mut RdbReader) -> Result<(), Error> {
        let size = reader.read_length()?;
        for _ in 0..size {
            let key = reader.read_string()?;
            let value = reader.read_string()?;
            obj.value.insert(key, value);
        }
        Ok(())
    }

    fn read_hash_zip_map(_obj: &mut HashObject, _reader: &mut RdbReader) -> Result<(), Error> {
        Err(Error::RedisRdbError(
            "not implemented rdb_type_zip_map".to_string(),
        ))
    }

    fn read_hash_zip_list(obj: &mut HashObject, reader: &mut RdbReader) -> Result<(), Error> {
        let list = reader.read_zip_list()?;
        let size = list.len();
        for i in (0..size).step_by(2) {
            let key = list[i].clone();
            let value = list[i + 1].clone();
            obj.value.insert(key, value);
        }
        Ok(())
    }

    pub fn read_hash_list_pack(obj: &mut HashObject, reader: &mut RdbReader) -> Result<(), Error> {
        let list = reader.read_list_pack()?;
        let size = list.len();
        for i in (0..size).step_by(2) {
            let key = list[i].clone();
            let value = list[i + 1].clone();
            obj.value.insert(key, value);
        }
        Ok(())
    }
}
