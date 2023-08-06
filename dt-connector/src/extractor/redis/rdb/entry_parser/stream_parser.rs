use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};
use dt_common::error::Error;
use dt_meta::redis::redis_object::{RedisCmd, RedisString, StreamObject};

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct StreamLoader {}

impl StreamLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        master_key: RedisString,
        type_byte: u8,
    ) -> Result<StreamObject, Error> {
        let mut obj = StreamObject::new();
        obj.key = master_key.clone();

        // 1. length(number of listpack), k1, v1, k2, v2, ..., number, ms, seq

        // Load the number of Listpack.
        let n_list_pack = reader.read_length()?;
        for _ in 0..n_list_pack {
            // Load key
            // key is streamId, like: 1612181627287-0
            let key = reader.read_string()?;
            let master_ms = BigEndian::read_i64(&key.as_bytes()[..8]); // ms
            let master_seq = BigEndian::read_i64(&key.as_bytes()[8..]);

            // value is a listpack
            let elements = reader.read_list_pack()?;
            let mut inx = 0usize;

            // The front of stream listpack is master entry
            // Parse the master entry
            let mut count = Self::next_integer(&mut inx, &elements); // count
            let mut deleted = Self::next_integer(&mut inx, &elements); // deleted
            let num_fields = Self::next_integer(&mut inx, &elements) as usize; // num-fields

            let fields = &elements[3..3 + num_fields]; // fields
            inx = 3 + num_fields;

            // master entry end by zero
            let last_entry = String::from(Self::next(&mut inx, &elements).clone());
            if last_entry != "0" {
                return Err(Error::Unexpected {
                    error: format!("master entry not ends by zero. lastEntry=[{}]", last_entry)
                        .to_string(),
                });
            }

            // Parse entries
            while count != 0 || deleted != 0 {
                let flags = Self::next_integer(&mut inx, &elements); // [is_same_fields|is_deleted]
                let entry_ms = Self::next_integer(&mut inx, &elements);
                let entry_seq = Self::next_integer(&mut inx, &elements);
                let value = &format!("{}-{}", entry_ms + master_ms, entry_seq + master_seq);

                let mut cmd = RedisCmd::new();
                cmd.add_str_arg("xadd");
                cmd.add_redis_arg(&master_key);
                cmd.add_str_arg(&value);

                if flags & 2 == 2 {
                    // same fields, get field from master entry.
                    for j in 0..num_fields {
                        cmd.add_redis_arg(&fields[j]);
                        cmd.add_redis_arg(Self::next(&mut inx, &elements));
                    }
                } else {
                    // get field by lp.Next()
                    let num = Self::next_integer(&mut inx, &elements) as usize;
                    let _ = elements[inx..inx + num * 2]
                        .iter()
                        .map(|i| cmd.add_redis_arg(i));
                    inx += num * 2;
                }

                Self::next(&mut inx, &elements); // lp_count

                if flags & 1 == 1 {
                    // is_deleted
                    deleted -= 1;
                } else {
                    count -= 1;
                    obj.cmds.push(cmd);
                }
            }
        }

        // Load total number of items inside the stream.
        reader.read_length()?;
        // Load the last entry ID.
        let last_ms = reader.read_length()?;
        let last_seq = reader.read_length()?;
        let last_id = format!("{}-{}", last_ms, last_seq);
        if n_list_pack == 0 {
            // Use the XADD MAXLEN 0 trick to generate an empty stream if
            // the key we are serializing is an empty string, which is possible
            // for the Stream type.
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("xadd");
            cmd.add_redis_arg(&master_key);
            cmd.add_str_arg("MAXLEN");
            cmd.add_str_arg("0");
            cmd.add_str_arg(&last_id);
            cmd.add_str_arg("x");
            cmd.add_str_arg("y");
            obj.cmds.push(cmd);
        }

        // Append XSETID after XADD, make sure lastid is correct,
        // in case of XDEL lastid.
        let mut cmd = RedisCmd::new();
        cmd.add_str_arg("xsetid");
        cmd.add_redis_arg(&master_key);
        cmd.add_str_arg(&last_id);
        obj.cmds.push(cmd);

        if type_byte == super::RDB_TYPE_STREAM_LIST_PACKS_2 {
            // Load the first entry ID.
            let _ = reader.read_length()?; // first_ms
            let _ = reader.read_length()?; // first_seq

            /* Load the maximal deleted entry ID. */
            let _ = reader.read_length()?; // max_deleted_ms
            let _ = reader.read_length()?; // max_deleted_seq

            /* Load the offset. */
            let _ = reader.read_length()?; // offset
        }

        // 2. nConsumerGroup, groupName, ms, seq, PEL, Consumers

        // Load the number of groups.
        let n_consumer_group = reader.read_length()?;
        for _i in 0..n_consumer_group {
            // Load groupName
            let group_name = reader.read_string()?;

            /* Load the last ID */
            let last_ms = reader.read_length()?;
            let last_seq = reader.read_length()?;
            let last_id = format!("{}-{}", last_ms, last_seq);

            /* Create Group */
            let mut cmd = RedisCmd::new();
            cmd.add_str_arg("CREATE");
            cmd.add_redis_arg(&master_key);
            cmd.add_redis_arg(&group_name);
            cmd.add_str_arg(&last_id);
            obj.cmds.push(cmd);

            /* Load group offset. */
            if type_byte == super::RDB_TYPE_STREAM_LIST_PACKS_2 {
                reader.read_length()?; // offset
            }

            /* Load the global PEL */
            let n_pel = u64::from(reader.read_length()?);
            let mut map_id_to_time = HashMap::new();
            let mut map_id_to_count = HashMap::new();

            for _ in 0..n_pel {
                // Load streamId
                let ms = reader.read_be_u64()?;
                let seq = reader.read_be_u64()?;
                let stream_id = format!("{}-{}", ms, seq);

                // Load deliveryTime
                let delivery_time = reader.read_u64()?.to_string();

                // Load deliveryCount
                let delivery_count = reader.read_length()?.to_string();

                // Save deliveryTime and deliveryCount
                map_id_to_time.insert(stream_id.clone(), delivery_time);
                map_id_to_count.insert(stream_id, delivery_count);
            }

            // Generate XCLAIMs for each consumer that happens to
            // have pending entries. Empty consumers are discarded.
            let n_consumer = reader.read_length()?;
            for _i in 0..n_consumer {
                /* Load consumerName */
                let consumer_name = reader.read_string()?;

                /* Load lastSeenTime */
                let _ = reader.read_u64()?;

                /* Consumer PEL */
                let n_pel = reader.read_length()?;
                for _i in 0..n_pel {
                    // Load streamId
                    let ms = reader.read_be_u64()?;
                    let seq = reader.read_be_u64()?;
                    let stream_id = format!("{}-{}", ms, seq);

                    /* Send */
                    let mut cmd = RedisCmd::new();
                    cmd.add_str_arg("xclaim");
                    cmd.add_redis_arg(&master_key);
                    cmd.add_redis_arg(&group_name);
                    cmd.add_redis_arg(&consumer_name);
                    cmd.add_str_arg("0");
                    cmd.add_str_arg(&stream_id);
                    cmd.add_str_arg("TIME");
                    cmd.add_str_arg(map_id_to_time.get(&stream_id).unwrap());
                    cmd.add_str_arg("RETRYCOUNT");
                    cmd.add_str_arg(map_id_to_count.get(&stream_id).unwrap());
                    cmd.add_str_arg("JUSTID");
                    cmd.add_str_arg("FORCE");
                    obj.cmds.push(cmd);
                }
            }
        }

        Ok(obj)
    }

    fn next_integer(inx: &mut usize, elements: &Vec<RedisString>) -> i64 {
        let ele = &elements[*inx];
        *inx += 1;
        String::from(ele.clone()).parse::<i64>().unwrap()
    }

    fn next<'a>(inx: &mut usize, elements: &'a Vec<RedisString>) -> &'a RedisString {
        let ele = &elements[*inx as usize];
        *inx += 1;
        &ele
    }
}
