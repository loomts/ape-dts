use anyhow::bail;
use dt_common::error::Error;
use dt_common::meta::redis::redis_object::{RedisString, ZSetEntry, ZsetObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct ZsetParser {}

impl ZsetParser {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        type_byte: u8,
    ) -> anyhow::Result<ZsetObject> {
        let mut obj = ZsetObject::new();
        obj.key = key;

        match type_byte {
            super::RDB_TYPE_ZSET => Self::read_zset(&mut obj, reader, false)?,
            super::RDB_TYPE_ZSET_2 => Self::read_zset(&mut obj, reader, true)?,
            super::RDB_TYPE_ZSET_ZIPLIST => Self::read_zset_zip_list(&mut obj, reader)?,
            super::RDB_TYPE_ZSET_LISTPACK => Self::read_zset_list_pack(&mut obj, reader)?,
            _ => {
                bail! {Error::RedisRdbError(format!(
                    "unknown zset type. type_byte=[{}]",
                    type_byte
                ))}
            }
        }
        Ok(obj)
    }

    fn read_zset(
        obj: &mut ZsetObject,
        reader: &mut RdbReader,
        is_zset_2: bool,
    ) -> anyhow::Result<()> {
        let size = reader.read_length()?;
        for _ in 0..size {
            let member = reader.read_string()?;
            let score = if is_zset_2 {
                reader.read_double()?.to_string()
            } else {
                reader.read_float()?.to_string()
            };

            let entry = ZSetEntry {
                member,
                score: RedisString::from(score),
            };
            obj.elements.push(entry);
        }
        Ok(())
    }

    fn read_zset_zip_list(obj: &mut ZsetObject, reader: &mut RdbReader) -> anyhow::Result<()> {
        let list = reader.read_zip_list()?;
        Self::parse_zset_result(obj, list)
    }

    fn read_zset_list_pack(obj: &mut ZsetObject, reader: &mut RdbReader) -> anyhow::Result<()> {
        let list = reader.read_list_pack()?;
        Self::parse_zset_result(obj, list)
    }

    fn parse_zset_result(obj: &mut ZsetObject, list: Vec<RedisString>) -> anyhow::Result<()> {
        let size = list.len();
        if size % 2 != 0 {
            bail! {Error::RedisRdbError(format!(
                "zset list pack size is not even. size=[{}]",
                size
            ))}
        }

        for i in (0..size).step_by(2) {
            let member = list[i].clone();
            let score = list[i + 1].clone();
            obj.elements.push(ZSetEntry { member, score });
        }
        Ok(())
    }
}
