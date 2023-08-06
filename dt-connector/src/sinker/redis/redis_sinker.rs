use async_trait::async_trait;
use dt_common::error::Error;
use dt_meta::dt_data::DtData;
use dt_meta::redis::redis_object::RedisCmd;
use redis::Connection;
use redis::ConnectionLike;

use crate::call_batch_fn;
use crate::Sinker;

use super::cmd_encoder::CmdEncoder;
use super::entry_rewriter::EntryRewriter;

pub struct RedisSinker {
    pub batch_size: usize,
    pub conn: Connection,
    pub now_db_id: i64,
}

#[async_trait]
impl Sinker for RedisSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        call_batch_fn!(self, data, Self::batch_sink);
        Ok(())
    }
}

impl RedisSinker {
    async fn batch_sink(
        &mut self,
        data: &mut [DtData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let mut packed_cmds = Vec::new();
        for dt_data in data.iter().skip(start_index).take(batch_size) {
            packed_cmds.extend_from_slice(&self.pack_entry(dt_data)?);
        }

        // TODO, check the result and add retry logic, if write failed, self.db_id should also be reset
        let _ = self
            .conn
            .req_packed_commands(&packed_cmds, 0, batch_size)
            .unwrap();
        Ok(())
    }

    fn pack_entry(&mut self, dt_data: &DtData) -> Result<Vec<u8>, Error> {
        let mut packed_cmds = Vec::new();
        match dt_data {
            DtData::Redis { entry } => {
                if entry.db_id != self.now_db_id {
                    let db_id = &entry.db_id.to_string();
                    let args = vec!["SELECT", db_id];
                    let cmd = RedisCmd::from_str_args(&args);

                    packed_cmds.extend_from_slice(&CmdEncoder::encode(&cmd));
                    self.now_db_id = entry.db_id;
                }

                if entry.is_rdb() {
                    let cmd = EntryRewriter::rewrite_as_restore(&entry)?;
                    packed_cmds.extend_from_slice(&CmdEncoder::encode(&cmd));
                } else {
                    packed_cmds.extend_from_slice(&CmdEncoder::encode(&entry.cmd));
                }
            }
            _ => {}
        }
        Ok(packed_cmds)
    }
}
