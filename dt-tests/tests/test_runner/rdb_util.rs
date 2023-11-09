use dt_common::error::Error;
use dt_connector::rdb_query_builder::RdbQueryBuilder;
use dt_meta::{
    mysql::{mysql_meta_manager::MysqlMetaManager, mysql_tb_meta::MysqlTbMeta},
    pg::{pg_meta_manager::PgMetaManager, pg_tb_meta::PgTbMeta},
    row_data::RowData,
};
use futures::TryStreamExt;
use sqlx::{MySql, Pool, Postgres};

pub struct RdbUtil {}

impl RdbUtil {
    pub async fn fetch_data_mysql(
        conn_pool: &Pool<MySql>,
        db_tb: &(String, String),
    ) -> Result<Vec<RowData>, Error> {
        let tb_meta = Self::get_tb_meta_mysql(conn_pool, db_tb).await?;
        let sql = format!(
            "SELECT * FROM `{}`.`{}` ORDER BY `{}` ASC",
            &db_tb.0, &db_tb.1, &tb_meta.basic.cols[0],
        );

        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);
        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_mysql_row(&row, &tb_meta);
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn fetch_data_pg(
        conn_pool: &Pool<Postgres>,
        db_tb: &(String, String),
    ) -> Result<Vec<RowData>, Error> {
        let tb_meta = Self::get_tb_meta_pg(conn_pool, db_tb).await?;
        let query_builder = RdbQueryBuilder::new_for_pg(&tb_meta);

        let tb_meta = Self::get_tb_meta_pg(conn_pool, db_tb).await?;
        let cols_str = query_builder.build_extract_cols_str().unwrap();
        let sql = format!(
            r#"SELECT {} FROM "{}"."{}" ORDER BY "{}" ASC"#,
            cols_str, &db_tb.0, &db_tb.1, &tb_meta.basic.cols[0],
        );
        let query = sqlx::query(&sql);
        let mut rows = query.fetch(conn_pool);

        let mut result = Vec::new();
        while let Some(row) = rows.try_next().await.unwrap() {
            let row_data = RowData::from_pg_row(&row, &tb_meta);
            result.push(row_data);
        }

        Ok(result)
    }

    pub async fn get_tb_meta_mysql(
        conn_pool: &Pool<MySql>,
        db_tb: &(String, String),
    ) -> Result<MysqlTbMeta, Error> {
        let mut meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;
        meta_manager.get_tb_meta(&db_tb.0, &db_tb.1).await
    }

    pub async fn get_tb_meta_pg(
        conn_pool: &Pool<Postgres>,
        db_tb: &(String, String),
    ) -> Result<PgTbMeta, Error> {
        let mut meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;
        meta_manager.get_tb_meta(&db_tb.0, &db_tb.1).await
    }

    pub async fn execute_sqls_mysql(
        conn_pool: &Pool<MySql>,
        sqls: &Vec<String>,
    ) -> Result<(), Error> {
        for sql in sqls {
            println!("executing sql: {}", sql);
            let query = sqlx::query(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }

    pub async fn execute_sqls_pg(
        conn_pool: &Pool<Postgres>,
        sqls: &Vec<String>,
    ) -> Result<(), Error> {
        for sql in sqls.iter() {
            println!("executing sql: {}", sql);
            let query = sqlx::query(sql);
            query.execute(conn_pool).await.unwrap();
        }
        Ok(())
    }
}
