use dt_common::meta::redis::{redis_entry::RedisEntry, redis_object::RedisCmd};
use dt_common::{error::Error, log_debug, log_info};
use sqlx::types::chrono;

use crate::extractor::redis::{rdb::entry_parser::module2_parser::ModuleParser, StreamReader};

use super::{entry_parser::entry_parser::EntryParser, reader::rdb_reader::RdbReader};

const _K_FLAG_FUNCTION2: u8 = 245; // function library data
const _K_FLAG_FUNCTION: u8 = 246; // old function library data for 7.0 rc1 and rc2
const K_FLAG_MODULE_AUX: u8 = 247; // Module auxiliary data.
const K_FLAG_IDLE: u8 = 0xf8; // LRU idle time.
const K_FLAG_FREQ: u8 = 0xf9; // LFU frequency.
const K_FLAG_AUX: u8 = 0xfa; // RDB aux field.
const K_FLAG_RESIZE_DB: u8 = 0xfb; // Hash table resize hint.
const K_FLAG_EXPIRE_MS: u8 = 0xfc; // Expire time in milliseconds.
const K_FLAG_EXPIRE: u8 = 0xfd; // Old expire time in seconds.
const K_FLAG_SELECT: u8 = 0xfe; // DB number of the following keys.
const K_EOF: u8 = 0xff; // End of the RDB file.

const RDB_MODULE_OPCODE_EOF: u64 = 0; // End of module value.
const RDB_MODULE_OPCODE_SINT: u64 = 1; // Signed integer.
const RDB_MODULE_OPCODE_UINT: u64 = 2; // Unsigned integer.
const RDB_MODULE_OPCODE_FLOAT: u64 = 3; // Float.
const RDB_MODULE_OPCODE_DOUBLE: u64 = 4; // Double.
const RDB_MODULE_OPCODE_STRING: u64 = 5; // String.

pub struct RdbParser<'a> {
    pub reader: RdbReader<'a>,
    pub repl_stream_db_id: i64,
    pub now_db_id: i64,
    pub expire_ms: i64,
    pub idle: i64,
    pub freq: i64,

    pub is_end: bool,
}

impl RdbParser<'_> {
    pub fn load_meta(&mut self) -> Result<String, Error> {
        // magic
        let mut buf = self.reader.read_bytes(5)?;
        let magic = String::from_utf8(buf).unwrap();
        if magic != "REDIS" {
            return Err(Error::RedisRdbError("invalid rdb format".to_string()));
        }

        // version
        buf = self.reader.read_bytes(4)?;
        let version = String::from_utf8(buf).unwrap();
        Ok(version)
    }

    pub fn load_entry(&mut self) -> Result<Option<RedisEntry>, Error> {
        let type_byte = self.reader.read_byte()?;
        log_debug!("rdb type_byte: {}", type_byte);

        match type_byte {
            K_FLAG_MODULE_AUX => {
                let module_id = self.reader.read_length()?; // module id
                let module_name = ModuleParser::module_type_name_by_id(module_id);
                log_info!(
                    "RDB module aux: module_id=[{}], module_name=[{}]",
                    module_id,
                    module_name
                );
                // refer: https://github.com/redis/redis/blob/unstable/src/rdb.c#L3183
                let _when_opcode = self.reader.read_length()?;
                let _when = self.reader.read_length()?;
                let mut opcode = self.reader.read_length()?;
                while opcode != RDB_MODULE_OPCODE_EOF {
                    match opcode {
                        RDB_MODULE_OPCODE_SINT | RDB_MODULE_OPCODE_UINT => {
                            self.reader.read_length()?;
                        }
                        RDB_MODULE_OPCODE_FLOAT => {
                            self.reader.read_float()?;
                        }
                        RDB_MODULE_OPCODE_DOUBLE => {
                            self.reader.read_double()?;
                        }
                        RDB_MODULE_OPCODE_STRING => {
                            self.reader.read_string()?;
                        }
                        _ => {
                            return Err(Error::RedisRdbError(format!(
                                "module aux opcode not found. module_name=[{}], opcode=[{}]",
                                module_name, opcode
                            )));
                        }
                    }
                    opcode = self.reader.read_length()?;
                }
            }

            K_FLAG_IDLE => {
                // OBJECT IDELTIME NOT captured in rdb snapshot
                self.idle = self.reader.read_length()? as i64;
            }

            K_FLAG_FREQ => {
                // OBJECT FREQ NOT captured in rdb snapshot
                self.freq = self.reader.read_u8()? as i64;
            }

            K_FLAG_AUX => {
                let key = String::from(self.reader.read_string()?);
                let value = self.reader.read_string()?;
                match key.as_str() {
                    "repl-stream-db" => {
                        let value = String::from(value);
                        self.repl_stream_db_id = value.parse::<i64>().unwrap();
                        log_info!("RDB repl-stream-db: {}", self.repl_stream_db_id);
                    }

                    "lua" => {
                        let mut cmd = RedisCmd::new();
                        cmd.add_str_arg("script");
                        cmd.add_str_arg("load");
                        cmd.add_redis_arg(&value);
                        log_info!("LUA script: {:?}", value);

                        let mut entry = RedisEntry::new();
                        entry.is_base = true;
                        entry.db_id = self.now_db_id;
                        entry.cmd = cmd;
                        return Ok(Some(entry));
                    }

                    _ => {
                        log_info!("RDB AUX fields. key=[{}], value=[{:?}]", key, value);
                    }
                }
            }

            K_FLAG_RESIZE_DB => {
                let db_size = self.reader.read_length()?;
                let expire_size = self.reader.read_length()?;
                log_info!(
                    "RDB resize db. db_size=[{}], expire_size=[{}]",
                    db_size,
                    expire_size
                )
            }

            K_FLAG_EXPIRE_MS => {
                let mut expire_ms = self.reader.read_u64()? as i64;
                expire_ms -= chrono::Utc::now().timestamp_millis();
                if expire_ms < 0 {
                    expire_ms = 1
                }
                self.expire_ms = expire_ms;
            }

            K_FLAG_EXPIRE => {
                let mut expire_ms = self.reader.read_u32()? as i64 * 1000;
                expire_ms -= chrono::Utc::now().timestamp_millis();
                if expire_ms < 0 {
                    expire_ms = 1
                }
                self.expire_ms = expire_ms;
            }

            K_FLAG_SELECT => {
                self.now_db_id = self.reader.read_length()? as i64;
            }

            K_EOF => {
                self.is_end = true;
                self.reader
                    .read_bytes(self.reader.rdb_length - self.reader.position)?;
            }

            _ => {
                let key = self.reader.read_string()?;
                self.reader.copy_raw = true;
                let value = EntryParser::parse_object(&mut self.reader, type_byte, key.clone());
                self.reader.copy_raw = false;

                if let Err(error) = value {
                    return Err(Error::RedisRdbError(format!(
                        "parsing rdb failed, type_byte: {}, key: {}, error: {:?}",
                        type_byte,
                        String::from(key),
                        error
                    )));
                } else {
                    let mut entry = RedisEntry::new();
                    entry.is_base = true;
                    entry.db_id = self.now_db_id;
                    entry.raw_bytes = self.reader.drain_raw_bytes();
                    entry.key = key;
                    entry.value = value.unwrap();
                    entry.value_type_byte = type_byte;
                    entry.expire_ms = self.expire_ms;
                    // reset expire_ms
                    self.expire_ms = 0;
                    return Ok(Some(entry));
                }
            }
        }

        Ok(None)
    }
}
