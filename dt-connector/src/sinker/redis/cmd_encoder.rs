use byteorder::WriteBytesExt;
use dt_meta::redis::redis_object::RedisCmd;
use std::io::Write;

pub struct CmdEncoder {}

impl CmdEncoder {
    pub fn encode(cmd: &RedisCmd) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.write_u8(super::RESP_ARRAY).unwrap();
        // write array length
        Self::write_length(&mut buf, cmd.args.len());

        for arg in cmd.args.iter() {
            Self::write_arg(&mut buf, arg);
        }
        buf
    }

    pub fn write_arg(buf: &mut Vec<u8>, arg: &[u8]) {
        buf.write_u8(super::RESP_STRING).unwrap();
        // write arg length
        Self::write_length(buf, arg.len());
        // write arg data
        buf.write(arg).unwrap();
        // write crlf
        Self::write_crlf(buf);
    }

    fn write_length(buf: &mut Vec<u8>, len: usize) {
        buf.write(len.to_string().as_bytes()).unwrap();
        Self::write_crlf(buf);
    }

    fn write_crlf(buf: &mut Vec<u8>) {
        buf.write(b"\r\n").unwrap();
    }
}
