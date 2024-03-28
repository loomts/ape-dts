use std::{
    str::FromStr,
    sync::{Arc, Mutex},
};

use dt_common::{error::Error, utils::rdb_filter::RdbFilter};
use dt_connector::extractor::{
    base_extractor::BaseExtractor,
    kafka::kafka_extractor::KafkaExtractor,
    mongo::{
        mongo_cdc_extractor::MongoCdcExtractor, mongo_check_extractor::MongoCheckExtractor,
        mongo_snapshot_extractor::MongoSnapshotExtractor,
    },
    mysql::{
        mysql_cdc_extractor::MysqlCdcExtractor, mysql_check_extractor::MysqlCheckExtractor,
        mysql_snapshot_extractor::MysqlSnapshotExtractor,
        mysql_struct_extractor::MysqlStructExtractor,
    },
    pg::{
        pg_cdc_extractor::PgCdcExtractor, pg_check_extractor::PgCheckExtractor,
        pg_snapshot_extractor::PgSnapshotExtractor, pg_struct_extractor::PgStructExtractor,
    },
    redis::{
        redis_cdc_extractor::RedisCdcExtractor, redis_client::RedisClient,
        redis_scan_extractor::RedisScanExtractor, redis_snapshot_extractor::RedisSnapshotExtractor,
        redis_snapshot_file_extractor::RedisSnapshotFileExtractor,
    },
    snapshot_resumer::SnapshotResumer,
};
use dt_meta::{
    avro::avro_converter::AvroConverter, mongo::mongo_cdc_source::MongoCdcSource,
    mysql::mysql_meta_manager::MysqlMetaManager, pg::pg_meta_manager::PgMetaManager,
    rdb_meta_manager::RdbMetaManager, redis::redis_statistic_type::RedisStatisticType,
    syncer::Syncer,
};

use super::task_util::TaskUtil;

pub struct ExtractorUtil {}

impl ExtractorUtil {
    pub async fn create_mysql_cdc_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        binlog_filename: &str,
        binlog_position: u32,
        server_id: u64,
        heartbeat_interval_secs: u64,
        heartbeat_tb: &str,
        filter: RdbFilter,
        log_level: &str,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<MysqlCdcExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlCdcExtractor {
            meta_manager,
            filter,
            conn_pool,
            url: url.to_string(),
            binlog_filename: binlog_filename.to_string(),
            binlog_position,
            server_id,
            heartbeat_interval_secs,
            heartbeat_tb: heartbeat_tb.to_string(),
            syncer,
            base_extractor,
        })
    }

    pub async fn create_pg_cdc_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        slot_name: &str,
        pub_name: &str,
        start_lsn: &str,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_tb: &str,
        filter: RdbFilter,
        log_level: &str,
        ddl_command_tb: &str,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<PgCdcExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCdcExtractor {
            meta_manager,
            filter,
            url: url.to_string(),
            conn_pool,
            slot_name: slot_name.to_string(),
            pub_name: pub_name.to_string(),
            start_lsn: start_lsn.to_string(),
            syncer,
            keepalive_interval_secs,
            heartbeat_interval_secs,
            heartbeat_tb: heartbeat_tb.to_string(),
            ddl_command_tb: ddl_command_tb.to_string(),
            base_extractor,
        })
    }

    pub async fn create_mysql_snapshot_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        db: &str,
        tb: &str,
        slice_size: usize,
        sample_interval: usize,
        resumer: SnapshotResumer,
        log_level: &str,
    ) -> Result<MysqlSnapshotExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // max_connections: 1 for extracting data from table, 1 for db-meta-manager
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            resumer,
            db: db.to_string(),
            tb: tb.to_string(),
            slice_size,
            sample_interval,
            base_extractor,
        })
    }

    pub async fn create_mysql_check_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        check_log_dir: &str,
        batch_size: usize,
        log_level: &str,
    ) -> Result<MysqlCheckExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = MysqlMetaManager::new(conn_pool.clone()).init().await?;

        Ok(MysqlCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            check_log_dir: check_log_dir.to_string(),
            batch_size,
            base_extractor,
        })
    }

    pub async fn create_pg_check_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        check_log_dir: &str,
        batch_size: usize,
        log_level: &str,
    ) -> Result<PgCheckExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgCheckExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            check_log_dir: check_log_dir.to_string(),
            batch_size,
            base_extractor,
        })
    }

    pub async fn create_pg_snapshot_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        db: &str,
        tb: &str,
        slice_size: usize,
        sample_interval: usize,
        resumer: SnapshotResumer,
        log_level: &str,
    ) -> Result<PgSnapshotExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;
        let meta_manager = PgMetaManager::new(conn_pool.clone()).init().await?;

        Ok(PgSnapshotExtractor {
            conn_pool: conn_pool.clone(),
            meta_manager,
            resumer,
            slice_size,
            sample_interval,
            schema: db.to_string(),
            tb: tb.to_string(),
            base_extractor,
        })
    }

    pub async fn create_mongo_snapshot_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        app_name: &str,
        db: &str,
        tb: &str,
        resumer: SnapshotResumer,
    ) -> Result<MongoSnapshotExtractor, Error> {
        let mongo_client = TaskUtil::create_mongo_client(url, app_name).await.unwrap();
        Ok(MongoSnapshotExtractor {
            resumer,
            db: db.to_string(),
            tb: tb.to_string(),
            mongo_client,
            base_extractor,
        })
    }

    pub async fn create_mongo_cdc_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        app_name: &str,
        resume_token: &str,
        start_timestamp: &u32,
        source: &str,
        filter: RdbFilter,
        heartbeat_interval_secs: u64,
        heartbeat_tb: &str,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<MongoCdcExtractor, Error> {
        let mongo_client = TaskUtil::create_mongo_client(url, app_name).await.unwrap();
        Ok(MongoCdcExtractor {
            filter,
            resume_token: resume_token.to_string(),
            start_timestamp: *start_timestamp,
            source: MongoCdcSource::from_str(source)?,
            mongo_client,
            app_name: app_name.to_string(),
            base_extractor,
            heartbeat_interval_secs,
            heartbeat_tb: heartbeat_tb.to_string(),
            syncer,
        })
    }

    pub async fn create_mongo_check_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        app_name: &str,
        check_log_dir: &str,
        batch_size: usize,
    ) -> Result<MongoCheckExtractor, Error> {
        let mongo_client = TaskUtil::create_mongo_client(url, app_name).await.unwrap();
        Ok(MongoCheckExtractor {
            mongo_client,
            check_log_dir: check_log_dir.to_string(),
            batch_size,
            base_extractor,
        })
    }

    pub async fn create_mysql_struct_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        db: &str,
        filter: RdbFilter,
        log_level: &str,
    ) -> Result<MysqlStructExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // TODO, pass max_connections as parameter
        let conn_pool = TaskUtil::create_mysql_conn_pool(url, 2, enable_sqlx_log).await?;

        Ok(MysqlStructExtractor {
            conn_pool,
            db: db.to_string(),
            filter,
            base_extractor,
        })
    }

    pub async fn create_pg_struct_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        schema: &str,
        filter: RdbFilter,
        log_level: &str,
    ) -> Result<PgStructExtractor, Error> {
        let enable_sqlx_log = TaskUtil::check_enable_sqlx_log(log_level);
        // TODO, pass max_connections as parameter
        let conn_pool = TaskUtil::create_pg_conn_pool(url, 2, enable_sqlx_log).await?;

        Ok(PgStructExtractor {
            conn_pool,
            schema: schema.to_string(),
            filter,
            base_extractor,
        })
    }

    pub async fn create_redis_snapshot_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        repl_port: u64,
        filter: RdbFilter,
    ) -> Result<RedisSnapshotExtractor, Error> {
        // let conn = TaskUtil::create_redis_conn(url).await?;
        let conn = RedisClient::new(url).await.unwrap();
        Ok(RedisSnapshotExtractor {
            conn,
            repl_port,
            filter,
            base_extractor,
        })
    }

    pub async fn create_redis_snapshot_file_extractor(
        base_extractor: BaseExtractor,
        file_path: &str,
        filter: RdbFilter,
    ) -> Result<RedisSnapshotFileExtractor, Error> {
        Ok(RedisSnapshotFileExtractor {
            file_path: file_path.to_string(),
            filter,
            base_extractor,
        })
    }

    pub async fn create_redis_scan_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        statistic_type: &str,
        scan_count: u64,
        filter: RdbFilter,
    ) -> Result<RedisScanExtractor, Error> {
        let conn = RedisClient::new(url).await.unwrap();
        let statistic_type = RedisStatisticType::from_str(statistic_type).unwrap();
        Ok(RedisScanExtractor {
            conn,
            statistic_type,
            scan_count,
            filter,
            base_extractor,
        })
    }

    pub async fn create_redis_cdc_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        repl_id: &str,
        repl_offset: u64,
        repl_port: u64,
        now_db_id: i64,
        keepalive_interval_secs: u64,
        heartbeat_interval_secs: u64,
        heartbeat_key: &str,
        filter: RdbFilter,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<RedisCdcExtractor, Error> {
        // let conn = TaskUtil::create_redis_conn(url).await?;
        let conn = RedisClient::new(url).await.unwrap();
        Ok(RedisCdcExtractor {
            conn,
            repl_id: repl_id.to_string(),
            repl_offset,
            keepalive_interval_secs,
            heartbeat_interval_secs,
            heartbeat_key: heartbeat_key.into(),
            syncer,
            repl_port,
            now_db_id,
            filter,
            base_extractor,
        })
    }

    pub async fn create_kafka_extractor(
        base_extractor: BaseExtractor,
        url: &str,
        group: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        ack_interval_secs: u64,
        meta_manager: Option<RdbMetaManager>,
        syncer: Arc<Mutex<Syncer>>,
    ) -> Result<KafkaExtractor, Error> {
        let avro_converter = AvroConverter::new(meta_manager);
        Ok(KafkaExtractor {
            url: url.into(),
            group: group.into(),
            topic: topic.into(),
            partition,
            offset,
            ack_interval_secs,
            avro_converter,
            syncer,
            base_extractor,
        })
    }
}
