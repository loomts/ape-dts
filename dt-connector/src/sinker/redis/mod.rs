pub mod cmd_encoder;
pub mod entry_rewriter;
pub mod redis_sinker;
pub mod redis_statistic_sinker;

/// redis resp protocol data type
pub const RESP_STATUS: u8 = b'+'; // +<string>\r\n
pub const RESP_ERROR: u8 = b'-'; // -<string>\r\n
pub const RESP_STRING: u8 = b'$'; // $<length>\r\n<bytes>\r\n
pub const RESP_INT: u8 = b':'; // :<number>\r\n
pub const RESP_NIL: u8 = b'_'; // _\r\n
pub const RESP_FLOAT: u8 = b','; // ,<floating-point-number>\r\n (golang float)
pub const RESP_BOOL: u8 = b'#'; // true: #t\r\n false: #f\r\n
pub const RESP_BLOB_ERROR: u8 = b'!'; // !<length>\r\n<bytes>\r\n
pub const RESP_VERBATIM: u8 = b'='; // =<length>\r\nFORMAT:<bytes>\r\n
pub const RESP_BIG_INT: u8 = b'('; // (<big number>\r\n
pub const RESP_ARRAY: u8 = b'*'; // *<len>\r\n... (same as resp2)
pub const RESP_MAP: u8 = b'%'; // %<len>\r\n(key)\r\n(value)\r\n... (golang map)
pub const RESP_SET: u8 = b'~'; // ~<len>\r\n... (same as Array)
pub const RESP_ATTR: u8 = b'|'; // |<len>\r\n(key)\r\n(value)\r\n... + command reply
pub const RESP_PUSH: u8 = b'>'; // ><len>\r\n... (same as Array)
