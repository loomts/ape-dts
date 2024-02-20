use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Instant;

use async_trait::async_trait;
use dt_common::error::Error;
use dt_common::monitor::monitor::Monitor;
use dt_meta::dt_data::DtData;
use dt_meta::rdb_meta_manager::RdbMetaManager;
use dt_meta::redis::redis_object::RedisCmd;
use dt_meta::redis::redis_object::RedisObject;
use dt_meta::redis::redis_write_method::RedisWriteMethod;
use dt_meta::row_data::RowData;
use dt_meta::row_type::RowType;
use redis::Connection;
use redis::ConnectionLike;

use crate::call_batch_fn;
use crate::data_marker::DataMarker;
use crate::sinker::base_sinker::BaseSinker;
use crate::Sinker;

use super::cmd_encoder::CmdEncoder;
use super::entry_rewriter::EntryRewriter;

pub struct RedisSinker {
    pub id: String,
    pub batch_size: usize,
    pub conn: Connection,
    pub now_db_id: i64,
    pub version: f32,
    pub method: RedisWriteMethod,
    pub meta_manager: Option<RdbMetaManager>,
    pub monitor: Arc<Mutex<Monitor>>,
    pub data_marker: Option<Arc<RwLock<DataMarker>>>,
}

#[async_trait]
impl Sinker for RedisSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtData>, _batch: bool) -> Result<(), Error> {
        if self.batch_size > 1 {
            call_batch_fn!(self, data, Self::batch_sink_raw);
        } else {
            self.serial_sink_raw(&mut data).await?;
        }
        Ok(())
    }

    async fn sink_dml(&mut self, mut data: Vec<RowData>, _batch: bool) -> Result<(), Error> {
        if self.batch_size > 1 {
            call_batch_fn!(self, data, Self::batch_sink_dml);
        } else {
            self.serial_sink_dml(&mut data).await?;
        }
        Ok(())
    }

    fn get_id(&self) -> String {
        self.id.clone()
    }
}

/// sink raw
impl RedisSinker {
    async fn batch_sink_raw(
        &mut self,
        data: &mut [DtData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let mut cmds = Vec::new();
        for dt_data in data.iter_mut().skip(start_index).take(batch_size) {
            data_size += dt_data.get_data_size();
            cmds.extend_from_slice(&self.rewrite_entry(dt_data)?);
        }
        self.batch_sink(cmds, batch_size).await.unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn serial_sink_raw(&mut self, data: &mut [DtData]) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        for dt_data in data.iter_mut() {
            data_size += dt_data.get_data_size();
            let cmds = self.rewrite_entry(dt_data)?;
            self.serial_sink(cmds).await.unwrap()
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
            .await
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
                        let mut rewrite_cmds = match entry.value {
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
                        if let Some(expire_cmd) = EntryRewriter::rewrite_expire(&entry)? {
                            rewrite_cmds.push(expire_cmd)
                        }
                        cmds.extend(rewrite_cmds);
                    }
                }
            }
            _ => {}
        }
        Ok(cmds)
    }
}

/// sink dml
impl RedisSinker {
    async fn batch_sink_dml(
        &mut self,
        data: &mut [RowData],
        start_index: usize,
        batch_size: usize,
    ) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        let mut cmds = Vec::new();
        for row_data in data.iter_mut().skip(start_index).take(batch_size) {
            data_size += row_data.data_size;
            if let Some(cmd) = self.dml_to_redis_cmd(row_data).await? {
                cmds.push(cmd);
            }
        }
        self.batch_sink(cmds, batch_size).await.unwrap();

        BaseSinker::update_batch_monitor(&mut self.monitor, batch_size, data_size, start_time).await
    }

    async fn serial_sink_dml(&mut self, data: &mut [RowData]) -> Result<(), Error> {
        let start_time = Instant::now();
        let mut data_size = 0;

        for row_data in data.iter_mut() {
            data_size += row_data.data_size;
            if let Some(cmd) = self.dml_to_redis_cmd(row_data).await? {
                self.serial_sink(vec![cmd]).await?
            }
        }

        BaseSinker::update_serial_monitor(&mut self.monitor, data.len(), data_size, start_time)
            .await
    }

    async fn dml_to_redis_cmd(&mut self, row_data: &RowData) -> Result<Option<RedisCmd>, Error> {
        if self.meta_manager.is_none() {
            return Ok(None);
        }

        let tb_meta = self
            .meta_manager
            .as_mut()
            .unwrap()
            .get_tb_meta(&row_data.schema, &row_data.tb)
            .await?;

        // no single primary / unique key exists, do not sink to redis
        if tb_meta.order_col.is_none() {
            return Ok(None);
        }

        let key = if let Some(col) = &tb_meta.order_col {
            match row_data.row_type {
                RowType::Insert | RowType::Update => row_data.after.as_ref().unwrap().get(col),
                RowType::Delete => row_data.before.as_ref().unwrap().get(col),
            }
        } else {
            None
        };

        let key = if let Some(v) = key {
            if let Some(v) = v.to_option_string() {
                format!("{}.{}.{}", row_data.schema, row_data.tb, v)
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        };

        let mut cmd = RedisCmd::new();
        match row_data.row_type {
            RowType::Insert | RowType::Update => {
                cmd.add_str_arg("hset");
                cmd.add_str_arg(&key);
                for (col, col_value) in row_data.after.as_ref().unwrap() {
                    cmd.add_str_arg(&col);
                    if let Some(v) = col_value.to_option_string() {
                        cmd.add_str_arg(&v);
                    } else {
                        cmd.add_str_arg(&String::new());
                    }
                }
            }
            RowType::Delete => {
                cmd.add_str_arg("del");
                cmd.add_str_arg(&key);
            }
        }
        Ok(Some(cmd))
    }
}

impl RedisSinker {
    async fn batch_sink(&mut self, cmds: Vec<RedisCmd>, batch_size: usize) -> Result<(), Error> {
        if cmds.is_empty() {
            return Ok(());
        }

        let mut packed_cmds = Vec::new();
        if let Some(data_marker_cmd) = self.get_data_marker_cmd() {
            let multi_cmd = self.get_multi_cmd();
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&multi_cmd));
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&data_marker_cmd));
        }

        for cmd in cmds {
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&cmd));
        }

        if self.data_marker.is_some() {
            let exec_cmd = self.get_exec_cmd();
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&exec_cmd));
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

    async fn serial_sink(&mut self, cmds: Vec<RedisCmd>) -> Result<(), Error> {
        if let Some(data_marker_cmd) = self.get_data_marker_cmd() {
            let multi_cmd = self.get_multi_cmd();
            let mut packed_cmds = Vec::new();
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&multi_cmd));
            packed_cmds.extend_from_slice(&CmdEncoder::encode(&data_marker_cmd));
            self.conn.req_packed_commands(&packed_cmds, 0, 2).unwrap();
        }

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

        if self.data_marker.is_some() {
            let exec_cmd = self.get_exec_cmd();
            self.conn
                .req_packed_command(&CmdEncoder::encode(&exec_cmd))
                .unwrap();
        }
        Ok(())
    }

    fn get_multi_cmd(&self) -> RedisCmd {
        RedisCmd::from_str_args(&vec!["MULTI"])
    }

    fn get_exec_cmd(&self) -> RedisCmd {
        RedisCmd::from_str_args(&vec!["EXEC"])
    }

    fn get_data_marker_cmd(&self) -> Option<RedisCmd> {
        if let Some(data_marker) = &self.data_marker {
            let data_marker = data_marker.read().unwrap();
            let cmd = RedisCmd::from_str_args(&vec![
                "SET",
                &data_marker.marker,
                &data_marker.data_origin_node,
            ]);
            return Some(cmd);
        }
        None
    }
}
