use async_trait::async_trait;
use dt_common::{error::Error, log_info, utils::rdb_filter::RdbFilter};

use dt_meta::{
    ddl_data::DdlData, ddl_type::DdlType, dt_data::DtData,
    mysql::mysql_meta_manager::MysqlMetaManager, position::Position,
    struct_meta::statement::struct_statement::StructStatement,
};
use sqlx::{MySql, Pool};

use crate::{
    extractor::base_extractor::BaseExtractor,
    meta_fetcher::mysql::mysql_struct_fetcher::MysqlStructFetcher, Extractor,
};

pub struct MysqlStructExtractor {
    pub base_extractor: BaseExtractor,
    pub conn_pool: Pool<MySql>,
    pub db: String,
    pub filter: RdbFilter,
}

#[async_trait]
impl Extractor for MysqlStructExtractor {
    async fn extract(&mut self) -> Result<(), Error> {
        log_info!("MysqlStructExtractor starts, schema: {}", self.db,);
        self.extract_internal().await
    }
}

impl MysqlStructExtractor {
    pub async fn extract_internal(&mut self) -> Result<(), Error> {
        let meta_manager = MysqlMetaManager::new(self.conn_pool.clone()).init().await?;
        let mut pg_fetcher = MysqlStructFetcher {
            conn_pool: self.conn_pool.to_owned(),
            db: self.db.clone(),
            filter: Some(self.filter.to_owned()),
            meta_manager,
        };

        // database
        let database = pg_fetcher.get_create_database_statement().await.unwrap();
        let statement = StructStatement::MysqlCreateDatabase {
            statement: database,
        };
        self.push_dt_data(statement).await;

        // tables
        for statement in pg_fetcher.get_create_table_statements("").await.unwrap() {
            self.push_dt_data(StructStatement::MysqlCreateTable { statement })
                .await;
        }

        self.base_extractor.wait_task_finish().await
    }

    pub async fn push_dt_data(&mut self, statement: StructStatement) {
        let ddl_data = DdlData {
            schema: self.db.clone(),
            tb: String::new(),
            query: String::new(),
            statement: Some(statement),
            ddl_type: DdlType::Unknown,
        };

        self.base_extractor
            .push_dt_data(DtData::Ddl { ddl_data }, Position::None)
            .await
            .unwrap()
    }
}
