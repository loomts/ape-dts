use dt_common::error::Error;
use dt_meta::redis::redis_object::ListObject;

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

const QUICKLIST_NODE_CONTAINER_PLAIN: u64 = 1;
const QUICKLIST_NODE_CONTAINER_PACKED: u64 = 2;

pub struct ListLoader {}

impl ListLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: &str,
        type_byte: u8,
    ) -> Result<ListObject, Error> {
        let mut obj = ListObject::new();
        obj.key = key.to_string();

        match type_byte {
            super::RDB_TYPE_LIST => Self::read_list(&mut obj, reader)?,
            super::RDB_TYPE_LIST_ZIP_LIST => obj.elements = reader.read_zip_list()?,
            super::RDB_TYPE_LIST_QUICK_LIST => Self::read_quick_list(&mut obj, reader)?,
            super::RDB_TYPE_LIST_QUICK_LIST_2 => Self::read_quick_list_2(&mut obj, reader)?,
            _ => {
                return Err(Error::Unexpected {
                    error: format!("unknown list type {}", type_byte),
                })
            }
        }
        Ok(obj)
    }

    fn read_list(obj: &mut ListObject, reader: &mut RdbReader) -> Result<(), Error> {
        let size = reader.read_length()?;
        for _ in 0..size {
            let ele = reader.read_string()?;
            obj.elements.push(ele);
        }
        Ok(())
    }

    fn read_quick_list(obj: &mut ListObject, reader: &mut RdbReader) -> Result<(), Error> {
        let size = reader.read_length()?;
        for _ in 0..size {
            let zip_list_elements = reader.read_zip_list()?;
            obj.elements.extend(zip_list_elements);
        }
        Ok(())
    }

    fn read_quick_list_2(obj: &mut ListObject, reader: &mut RdbReader) -> Result<(), Error> {
        let size = reader.read_length()?;

        for _ in 0..size {
            let container = reader.read_length()?;
            match container {
                QUICKLIST_NODE_CONTAINER_PLAIN => {
                    let ele = reader.read_string()?;
                    obj.elements.push(ele);
                }

                QUICKLIST_NODE_CONTAINER_PACKED => {
                    let listpack_elements = reader.read_list_pack()?;
                    obj.elements.extend(listpack_elements);
                }

                _ => {
                    return Err(Error::Unexpected {
                        error: format!("unknown quicklist container {}", container),
                    });
                }
            }
        }
        Ok(())
    }
}
