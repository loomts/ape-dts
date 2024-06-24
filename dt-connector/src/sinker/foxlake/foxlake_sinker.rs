use anyhow::Ok;
use async_trait::async_trait;
use dt_common::{
    log_info,
    meta::{
        ddl_data::DdlData,
        ddl_type::DdlType,
        dt_data::{DtData, DtItem},
        mysql::mysql_meta_manager::MysqlMetaManager,
        position::Position,
        sql_parser::ddl_parser::DdlParser,
    },
    monitor::monitor::Monitor,
};
use sqlx::{
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
    MySql, Pool,
};
use std::{
    str::FromStr,
    sync::{Arc, Mutex},
    time::Instant,
};

use crate::{close_conn_pool, rdb_router::RdbRouter, sinker::base_sinker::BaseSinker, Sinker};

use super::{foxlake_merger::FoxlakeMerger, foxlake_pusher::FoxlakePusher};

pub struct FoxlakeSinker {
    pub url: String,
    pub batch_size: usize,
    pub meta_manager: MysqlMetaManager,
    pub monitor: Arc<Mutex<Monitor>>,
    pub conn_pool: Pool<MySql>,
    pub router: RdbRouter,
    pub pusher: FoxlakePusher,
    pub merger: FoxlakeMerger,
}

#[async_trait]
impl Sinker for FoxlakeSinker {
    async fn sink_raw(&mut self, mut data: Vec<DtItem>, _batch: bool) -> anyhow::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        self.batch_sink(&mut data).await
    }

    async fn sink_ddl(&mut self, data: Vec<DdlData>, _batch: bool) -> anyhow::Result<()> {
        for ddl_data in data.iter() {
            let mut sql = ddl_data.query.clone();
            // db mapping
            let db = self.router.get_db_map(&ddl_data.schema);
            if db != ddl_data.schema {
                let mut parsed_ddl = DdlParser::parse(&ddl_data.query)?;
                if parsed_ddl.schema.is_some() {
                    parsed_ddl.schema = Some(db.to_string());
                    sql = parsed_ddl.to_sql();
                    log_info!(
                        "sink ddl, origin sql: {}, mapped sql: {}",
                        ddl_data.query,
                        sql
                    );
                }
            }

            log_info!("sink ddl: {}", sql);
            let query = sqlx::query(&sql);

            // create a tmp connection with databse since sqlx conn pool does NOT support `USE db`
            let mut conn_options = MySqlConnectOptions::from_str(&self.url)?;
            if !db.is_empty() {
                match ddl_data.ddl_type {
                    DdlType::CreateDatabase | DdlType::DropDatabase | DdlType::AlterDatabase => {}
                    _ => {
                        conn_options = conn_options.database(db);
                    }
                }
            }

            let conn_pool = MySqlPoolOptions::new()
                .max_connections(1)
                .connect_with(conn_options)
                .await?;
            query.execute(&conn_pool).await?;
            conn_pool.close().await;

            self.meta_manager.invalidate_cache(db, &ddl_data.tb);
        }
        Ok(())
    }

    async fn close(&mut self) -> anyhow::Result<()> {
        self.meta_manager.close().await?;
        return close_conn_pool!(self);
    }
}

impl FoxlakeSinker {
    async fn batch_sink(&mut self, data: &mut [DtItem]) -> anyhow::Result<()> {
        let start_time = Instant::now();

        // push to s3
        let (s3_file_metas, _) = self.pusher.batch_push(data, 0, data.len(), true).await?;

        // merge to foxlake
        let mut dt_items = Vec::new();
        for file_meta in s3_file_metas {
            let dt_item = DtItem {
                dt_data: DtData::Foxlake { file_meta },
                position: Position::None,
                data_origin_node: String::new(),
            };
            dt_items.push(dt_item);
        }
        let (all_data_size, all_row_count) = self.merger.batch_merge(dt_items).await?;

        BaseSinker::update_batch_monitor(
            &mut self.monitor,
            all_row_count,
            all_data_size,
            start_time,
        )
        .await
    }
}
