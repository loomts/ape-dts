use async_trait::async_trait;
use dt_common::error::Error;
use dt_meta::dt_data::DtData;
use redis::Connection;
use redis::ConnectionLike;

use crate::call_batch_fn;
use crate::Sinker;

use super::entry_rewriter::EntryRewriter;

pub struct RedisSinker {
    pub batch_size: usize,
    pub conn: Connection,
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
        let mut cmds = Vec::new();
        for i in data.iter().skip(start_index).take(batch_size) {
            match i {
                DtData::Redis { entry } => {
                    let cmd = EntryRewriter::rewrite_as_restore(&entry)?;
                    cmds.extend_from_slice(&cmd);
                }
                _ => {}
            }
        }

        // TODO, check the result and add retry logic
        let _ = self.conn.req_packed_commands(&cmds, 0, batch_size).unwrap();
        Ok(())
    }
}
