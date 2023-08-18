use async_trait::async_trait;
use dt_common::error::Error;
use dt_meta::dt_data::DtData;
use dt_meta::redis::redis_object::RedisCmd;
use dt_meta::redis::redis_object::RedisObject;
use dt_meta::redis::redis_write_method::RedisWriteMethod;
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
    pub version: f32,
    pub method: RedisWriteMethod,
}

#[async_trait]
impl Sinker for RedisSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        if self.batch_size > 1 {
            call_batch_fn!(self, data, Self::batch_sink);
        } else {
            self.serial_sink(&mut data).await?;
        }
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
        for dt_data in data.iter_mut().skip(start_index).take(batch_size) {
            cmds.extend_from_slice(&self.rewrite_entry(dt_data)?);
        }

        let mut packed_cmds = Vec::new();
        for cmd in cmds {
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&cmd));
        }

        let result = self.conn.req_packed_commands(&packed_cmds, 0, batch_size);
        if let Err(error) = result {
            return Err(Error::SinkerError(format!(
                "batch sink failed, error: {:?}",
                error
            )));
        }
        Ok(())
    }

    async fn serial_sink(&mut self, data: &mut [DtData]) -> Result<(), Error> {
        for dt_data in data.iter_mut() {
            let cmds = self.rewrite_entry(dt_data)?;
            for cmd in cmds {
                let result = self.conn.req_packed_command(&CmdEncoder::encode(&cmd));
                if let Err(error) = result {
                    return Err(Error::SinkerError(format!(
                        "serial sink failed, error: {:?}, cmd: {}",
                        error,
                        cmd.to_string()
                    )));
                }
            }
        }
        Ok(())
    }

    fn rewrite_entry(&mut self, dt_data: &mut DtData) -> Result<Vec<RedisCmd>, Error> {
        let mut cmds = Vec::new();
        match dt_data {
            DtData::Redis { ref mut entry } => {
                if entry.db_id != self.now_db_id {
                    let db_id = &entry.db_id.to_string();
                    let args = vec!["SELECT", db_id];
                    let cmd = RedisCmd::from_str_args(&args);
                    cmds.push(cmd);
                    self.now_db_id = entry.db_id;
                }

                match self.method {
                    RedisWriteMethod::Restore => {
                        if entry.is_raw() {
                            let cmd = EntryRewriter::rewrite_as_restore(&entry, self.version)?;
                            cmds.push(cmd);
                        } else {
                            cmds.push(entry.cmd.clone());
                        }
                    }

                    RedisWriteMethod::Rewrite => {
                        let rewrite_cmds = match entry.value {
                            RedisObject::String(ref mut obj) => EntryRewriter::rewrite_string(obj),
                            RedisObject::List(ref mut obj) => EntryRewriter::rewrite_list(obj),
                            RedisObject::Set(ref mut obj) => EntryRewriter::rewrite_set(obj),
                            RedisObject::Hash(ref mut obj) => EntryRewriter::rewrite_hash(obj),
                            RedisObject::Zset(ref mut obj) => EntryRewriter::rewrite_zset(obj),
                            RedisObject::Stream(ref mut obj) => Ok(obj.cmds.drain(..).collect()),
                            RedisObject::Module(_) => {
                                let cmd = EntryRewriter::rewrite_as_restore(&entry, self.version)?;
                                Ok(vec![cmd])
                            }
                            _ => return Err(Error::SinkerError("rewrite not implemented".into())),
                        }?;
                        cmds.extend(rewrite_cmds);
                    }
                }
            }
            _ => {}
        }
        Ok(cmds)
    }
}
