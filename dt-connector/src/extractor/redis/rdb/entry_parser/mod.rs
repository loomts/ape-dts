pub mod entry_parser;
pub mod hash_parser;
pub mod list_parser;
pub mod module2_parser;
pub mod set_parser;
pub mod stream_parser;
pub mod string_parser;
pub mod zset_parser;

const RDB_TYPE_STRING: u8 = 0;
const RDB_TYPE_LIST: u8 = 1;
const RDB_TYPE_SET: u8 = 2;
const RDB_TYPE_ZSET: u8 = 3;
const RDB_TYPE_HASH: u8 = 4;
const RDB_TYPE_ZSET_2: u8 = 5;
const RDB_TYPE_MODULE: u8 = 6;
const RDB_TYPE_MODULE_2: u8 = 7;

const RDB_TYPE_HASH_ZIP_MAP: u8 = 9;
const RDB_TYPE_LIST_ZIP_LIST: u8 = 10;
const RDB_TYPE_SET_INT_SET: u8 = 11;
const RDB_TYPE_ZSET_ZIP_LIST: u8 = 12;
const RDB_TYPE_HASH_ZIP_LIST: u8 = 13;
const RDB_TYPE_LIST_QUICK_LIST: u8 = 14;
const RDB_TYPE_STREAM_LIST_PACKS: u8 = 15;
const RDB_TYPE_HASH_LIST_PACK: u8 = 16;
const RDB_TYPE_ZSET_LIST_PACK: u8 = 17;
const RDB_TYPE_LIST_QUICK_LIST_2: u8 = 18;
const RDB_TYPE_STREAM_LIST_PACKS_2: u8 = 19;

const MODULE_TYPE_NAME_CHAR_SET: &'static str =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

const RDB_MODULE_OPCODE_EOF: u8 = 0;
const RDB_MODULE_OPCODE_SINT: u8 = 1;
const RDB_MODULE_OPCODE_UINT: u8 = 2;
const RDB_MODULE_OPCODE_FLOAT: u8 = 3;
const RDB_MODULE_OPCODE_DOUBLE: u8 = 4;
const RDB_MODULE_OPCODE_STRING: u8 = 5;

// const STRING_TYPE: &str = "string";
// const LIST_TYPE: &str = "list";
// const SET_TYPE: &str = "set";
// const HASH_TYPE: &str = "hash";
// const ZSET_TYPE: &str = "zset";
// const AUX_TYPE: &str = "aux";
// const DB_SIZE_TYPE: &str = "dbsize";

// pub trait RedisObject {
//     fn load_from_buffer(
//         &mut self,
//         reader: &mut RdbReader,
//         key: &str,
//         type_byte: u8,
//     ) -> Result<(), Error>;
//     fn rewrite(&self) -> Result<Vec<RedisCmd>, Error>;
// }
