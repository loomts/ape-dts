use dt_common::error::Error;
use dt_meta::redis::redis_object::{ModuleObject, RedisString};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct ModuleLoader {}

impl ModuleLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        key: RedisString,
        type_byte: u8,
    ) -> Result<ModuleObject, Error> {
        let obj = ModuleObject::new();

        if type_byte == super::RDB_TYPE_MODULE {
            return Err(Error::Unexpected {
                error: format!(
                    "module type with version 1 is not supported, key=[{}]",
                    String::from(key)
                ),
            });
        }

        let module_id = reader.read_length()?;
        let module_name = Self::module_type_name_by_id(module_id);
        let mut op_code = reader.read_byte()?;
        while op_code != super::RDB_MODULE_OPCODE_EOF {
            match op_code {
                super::RDB_MODULE_OPCODE_SINT | super::RDB_MODULE_OPCODE_UINT => {
                    reader.read_length()?;
                }

                super::RDB_MODULE_OPCODE_FLOAT => {
                    reader.read_float()?;
                }

                super::RDB_MODULE_OPCODE_DOUBLE => {
                    reader.read_double()?;
                }

                super::RDB_MODULE_OPCODE_STRING => {
                    reader.read_string()?;
                }

                _ => {
                    return Err(Error::Unexpected {
                        error: format!(
                            "unknown module opcode=[{}], module name=[{}]",
                            op_code, module_name
                        ),
                    });
                }
            }
            op_code = reader.read_byte()?;
        }

        Ok(obj)
    }

    fn module_type_name_by_id(module_id: u64) -> String {
        let mut name_list: Vec<u8> = vec![0; 9];
        let mut module_id = module_id >> 10;
        let name_char_set = super::MODULE_TYPE_NAME_CHAR_SET
            .chars()
            .collect::<Vec<char>>();

        for i in (0..9).rev() {
            name_list[i] = name_char_set[(module_id & 63) as usize] as u8;
            module_id >>= 6;
        }
        String::from_utf8(name_list).unwrap()
    }
}
