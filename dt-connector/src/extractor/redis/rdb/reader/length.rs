use crate::extractor::redis::RawByteReader;

use super::rdb_reader::RdbReader;
use byteorder::{BigEndian, ByteOrder};
use dt_common::error::Error;

const RDB_6_BIT_LEN: u8 = 0;
const RDB_14_BIT_LEN: u8 = 1;
const RDB_32_OR_64_BIT_LEN: u8 = 2;
const RDB_SPECIAL_LEN: u8 = 3;
const RDB_32_BIT_LEN: u8 = 0x80;
const RDB_64_BIT_LEN: u8 = 0x81;

impl RdbReader<'_> {
    pub fn read_length(&mut self) -> Result<u64, Error> {
        let (len, special) = self.read_encoded_length()?;
        if special {
            Err(Error::RedisRdbError("illegal length special=true".into()))
        } else {
            Ok(len)
        }
    }

    pub fn read_encoded_length(&mut self) -> Result<(u64, bool), Error> {
        let first_byte = self.read_byte()?;
        let first_2_bits = (first_byte & 0xc0) >> 6;
        match first_2_bits {
            RDB_6_BIT_LEN => {
                let len = u64::from(first_byte) & 0x3f;
                Ok((len, false))
            }

            RDB_14_BIT_LEN => {
                let next_byte = self.read_byte()?;
                let len = (u64::from(first_byte) & 0x3f) << 8 | u64::from(next_byte);
                Ok((len, false))
            }

            RDB_32_OR_64_BIT_LEN => match first_byte {
                RDB_32_BIT_LEN => {
                    let next_bytes = self.read_raw(4)?;
                    let len = BigEndian::read_u32(&next_bytes) as u64;
                    Ok((len, false))
                }

                RDB_64_BIT_LEN => {
                    let next_bytes = self.read_raw(8)?;
                    let len = BigEndian::read_u64(&next_bytes) as u64;
                    Ok((len, false))
                }

                _ => Err(Error::RedisRdbError(format!(
                    "illegal length encoding: {:x}",
                    first_byte
                ))),
            },

            RDB_SPECIAL_LEN => {
                let len = u64::from(first_byte) & 0x3f;
                Ok((len, true))
            }

            _ => Err(Error::RedisRdbError(format!(
                "illegal length encoding: {:x}",
                first_byte
            ))),
        }
    }
}
