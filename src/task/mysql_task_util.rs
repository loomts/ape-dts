use sqlx::{mysql::MySqlPoolOptions, MySql, Pool};

use crate::{
    config::rdb_to_rdb_config::RdbToRdbConfig, error::Error, extractor::filter::Filter,
    sinker::router::Router,
};

pub struct MysqlTaskUtil {}

impl MysqlTaskUtil {
    pub async fn init_components(
        config: &RdbToRdbConfig,
    ) -> Result<(Filter, Router, Pool<MySql>, Pool<MySql>), Error> {
        let filter = Filter::from_config(&config.filter).unwrap();
        let router = Router::from_config(&config.route).unwrap();

        let src_conn_pool = MySqlPoolOptions::new()
            .max_connections(config.src_pool_size)
            .connect(&config.src_url)
            .await?;

        let dst_conn_pool = MySqlPoolOptions::new()
            .max_connections(config.dst_pool_size)
            .connect(&config.dst_url)
            .await?;

        Ok((filter, router, src_conn_pool, dst_conn_pool))
    }
}
