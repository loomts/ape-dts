use dt_common::error::Error;
use dt_meta::redis::redis_object::RedisString;

use crate::extractor::redis::RawByteReader;

use super::rdb_reader::RdbReader;

const RDB_ENC_INT8: u8 = 0;
const RDB_ENC_INT16: u8 = 1;
const RDB_ENC_INT32: u8 = 2;
const RDB_ENC_LZF: u8 = 3;

impl RdbReader<'_> {
    pub fn read_string(&mut self) -> Result<RedisString, Error> {
        let (len, special) = self.read_encoded_length()?;
        let bytes = if special {
            match len as u8 {
                RDB_ENC_INT8 => self.read_i8()?.to_string().as_bytes().to_vec(),

                RDB_ENC_INT16 => self.read_i16()?.to_string().as_bytes().to_vec(),

                RDB_ENC_INT32 => self.read_i32()?.to_string().as_bytes().to_vec(),

                RDB_ENC_LZF => {
                    let in_len = self.read_length()?;
                    let out_len = self.read_length()?;
                    let in_buf = self.read_raw(in_len as usize)?;
                    self.lzf_decompress(&in_buf, out_len as usize)?
                }

                _ => {
                    return Err(Error::Unexpected {
                        error: format!("Unknown string encode type {}", len).to_string(),
                    })
                }
            }
        } else {
            self.read_raw(len as usize)?
        };
        Ok(RedisString { bytes })
    }

    fn lzf_decompress(&self, in_buf: &[u8], out_len: usize) -> Result<Vec<u8>, Error> {
        let mut out = vec![0u8; out_len];

        let mut i = 0;
        let mut o = 0;
        while i < in_buf.len() {
            let ctrl = in_buf[i] as usize;
            i += 1;
            if ctrl < 32 {
                for _x in 0..=ctrl {
                    out[o] = in_buf[i];
                    i += 1;
                    o += 1;
                }
            } else {
                let mut length = ctrl >> 5;
                if length == 7 {
                    length += in_buf[i] as usize;
                    i += 1;
                }

                let mut ref_ = o - ((ctrl & 0x1f) << 8) - in_buf[i] as usize - 1;
                i += 1;

                for _x in 0..=length + 1 {
                    out[o] = out[ref_];
                    ref_ += 1;
                    o += 1;
                }
            }
        }

        if o != out_len {
            Err(Error::Unexpected {
                error: format!("lzf decompress failed: out_len: {}, o: {}", out_len, o).to_string(),
            })
        } else {
            Ok(out)
        }
    }
}
