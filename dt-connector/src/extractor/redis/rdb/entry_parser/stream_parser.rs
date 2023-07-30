use std::collections::HashMap;

use byteorder::{BigEndian, ByteOrder};
use dt_common::error::Error;
use dt_meta::redis::redis_object::StreamObject;

use crate::extractor::redis::rdb::reader::rdb_reader::RdbReader;

pub struct StreamLoader {}

impl StreamLoader {
    pub fn load_from_buffer(
        reader: &mut RdbReader,
        master_key: &str,
        type_byte: u8,
    ) -> Result<StreamObject, Error> {
        let mut obj = StreamObject::new();
        obj.key = master_key.to_string();

        // 1. length(number of listpack), k1, v1, k2, v2, ..., number, ms, seq

        // Load the number of Listpack.
        let n_list_pack = reader.read_length()?;
        for _ in 0..n_list_pack {
            // Load key
            let key = reader.read_string_raw()?;

            // key is streamId, like: 1612181627287-0
            let master_ms = BigEndian::read_i64(&key[..8]); // ms
            let master_seq = BigEndian::read_i64(&key[8..]);

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
            let last_entry = Self::next_string(&mut inx, &elements);
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

                let mut args = vec![
                    "xadd".to_string(),
                    master_key.to_string(),
                    format!("{}-{}", entry_ms + master_ms, entry_seq + master_seq),
                ];

                if flags & 2 == 2 {
                    // same fields, get field from master entry.
                    for j in 0..num_fields {
                        args.push(fields[j].to_string());
                        args.push(Self::next_string(&mut inx, &elements).to_string());
                    }
                } else {
                    // get field by lp.Next()
                    let num = Self::next_integer(&mut inx, &elements) as usize;
                    args.extend_from_slice(&elements[inx..inx + num * 2]);
                    inx += num * 2;
                }

                Self::next_string(&mut inx, &elements); // lp_count

                if flags & 1 == 1 {
                    // is_deleted
                    deleted -= 1;
                } else {
                    count -= 1;
                    obj.cmds.push(args);
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
            let args = vec!["xadd", master_key, "MAXLEN", "0", &last_id, "x", "y"];
            obj.cmds.push(args.iter().map(|s| s.to_string()).collect());
        }

        // Append XSETID after XADD, make sure lastid is correct,
        // in case of XDEL lastid.
        let cmd = vec!["xsetid", master_key, &last_id];
        obj.cmds.push(cmd.iter().map(|s| s.to_string()).collect());

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
            let cmd = vec!["CREATE", master_key, &group_name, &last_id];
            obj.cmds.push(cmd.iter().map(|s| s.to_string()).collect());

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
                let ms = reader.read_u64()?;
                let seq = reader.read_u64()?;
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
                    let ms = reader.read_u64()?;
                    let seq = reader.read_u64()?;
                    let stream_id = format!("{}-{}", ms, seq);

                    /* Send */
                    let cmd = [
                        "xclaim",
                        master_key,
                        &group_name,
                        &consumer_name,
                        "0",
                        &stream_id,
                        "TIME",
                        map_id_to_time.get(&stream_id).unwrap(),
                        "RETRYCOUNT",
                        map_id_to_count.get(&stream_id).unwrap(),
                        "JUSTID",
                        "FORCE",
                    ];
                    obj.cmds.push(cmd.iter().map(|s| s.to_string()).collect());
                }
            }
        }

        Ok(obj)
    }

    fn next_integer(inx: &mut usize, elements: &Vec<String>) -> i64 {
        let ele = &elements[*inx];
        *inx += 1;
        let i = ele
            .parse::<i64>()
            .expect(&format!("integer is not a number. ele=[{}]", ele));
        i
    }

    fn next_string<'a>(inx: &mut usize, elements: &'a Vec<String>) -> &'a str {
        let ele = &elements[*inx as usize];
        *inx += 1;
        ele
    }
}
