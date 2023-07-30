use super::base_test_runner::BaseTestRunner;
use dt_common::{
    config::{
        config_token_parser::ConfigTokenParser, extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
    utils::time_util::TimeUtil,
};
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
use dt_meta::redis::redis_object::RedisCmd;
use dt_task::task_util::TaskUtil;
use redis::{Connection, ConnectionLike, Value};

const ARG_QUOTER: char = '"';

pub struct RedisTestRunner {
    pub base: BaseTestRunner,
    src_conn: Option<Connection>,
    dst_conn: Option<Connection>,
}

impl RedisTestRunner {
    pub async fn new(relative_test_dir: &str) -> Result<Self, Error> {
        let base = BaseTestRunner::new(relative_test_dir).await.unwrap();

        let mut src_conn = None;
        let mut dst_conn = None;

        let config = TaskConfig::new(&base.task_config_file);
        match config.extractor {
            ExtractorConfig::RedisSnapshot { url, .. } | ExtractorConfig::RedisCdc { url, .. } => {
                src_conn = Some(TaskUtil::create_redis_conn(&url).await.unwrap());
            }
            _ => {}
        }

        match config.sinker {
            SinkerConfig::Redis { url, .. } => {
                dst_conn = Some(TaskUtil::create_redis_conn(&url).await.unwrap());
            }
            _ => {}
        }

        Ok(Self {
            base,
            src_conn,
            dst_conn,
        })
    }

    pub async fn run_snapshot_test(&mut self) -> Result<(), Error> {
        self.execute_test_ddl_sqls()?;

        let mut src_conn = self.src_conn.as_mut().unwrap();
        Self::execute_cmds(&mut src_conn, &self.base.src_dml_sqls);
        self.base.start_task().await?;

        self.compare_all_data()
    }

    pub async fn run_cdc_test(
        &mut self,
        start_millis: u64,
        parse_millis: u64,
    ) -> Result<(), Error> {
        self.execute_test_ddl_sqls()?;

        let task = self.base.spawn_task().await?;
        TimeUtil::sleep_millis(start_millis).await;

        let mut src_conn = self.src_conn.as_mut().unwrap();
        Self::execute_cmds(&mut src_conn, &self.base.src_dml_sqls);

        TimeUtil::sleep_millis(parse_millis).await;
        self.compare_all_data()?;

        self.base.wait_task_finish(&task).await
    }

    fn execute_test_ddl_sqls(&mut self) -> Result<(), Error> {
        let mut src_conn = self.src_conn.as_mut().unwrap();
        let mut dst_conn = self.dst_conn.as_mut().unwrap();
        Self::execute_cmds(&mut src_conn, &self.base.src_ddl_sqls);
        Self::execute_cmds(&mut dst_conn, &self.base.dst_ddl_sqls);
        Ok(())
    }

    fn compare_all_data(&mut self) -> Result<(), Error> {
        let src_conn = self.src_conn.as_mut().unwrap();
        let dbs = Self::list_dbs(src_conn);
        for db in dbs {
            println!("compare data for db: {}", db);
            self.compare_data(db)?;
        }
        Ok(())
    }

    fn compare_data(&mut self, db: String) -> Result<(), Error> {
        let src_conn = self.src_conn.as_mut().unwrap();
        let dst_conn = self.dst_conn.as_mut().unwrap();

        Self::execute_cmd(src_conn, &format!("SELECT {}", db));
        Self::execute_cmd(dst_conn, &format!("SELECT {}", db));

        let mut string_keys = Vec::new();
        let mut hash_keys = Vec::new();
        let mut list_keys = Vec::new();
        let mut stream_keys = Vec::new();
        let mut set_keys = Vec::new();
        let mut zset_keys = Vec::new();

        let keys = Self::list_keys(src_conn, "*");
        for key in keys {
            let key_type = Self::get_key_type(src_conn, &key);
            match key_type.as_str() {
                "string" => string_keys.push(key),
                "hash" => hash_keys.push(key),
                "zset" => zset_keys.push(key),
                "stream" => stream_keys.push(key),
                "set" => set_keys.push(key),
                "list" => list_keys.push(key),
                _ => string_keys.push(key),
            }
        }

        Self::compare_string_entries(src_conn, dst_conn, &string_keys);
        Self::compare_hash_entries(src_conn, dst_conn, &hash_keys);
        Self::compare_list_entries(src_conn, dst_conn, &list_keys);
        Self::compare_set_entries(src_conn, dst_conn, &set_keys);
        Self::compare_zset_entries(src_conn, dst_conn, &zset_keys);
        Self::compare_stream_entries(src_conn, dst_conn, &stream_keys);
        Ok(())
    }

    fn compare_string_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("GET \"{}\"", key);
            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_hash_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("HGETALL \"{}\"", key);
            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_list_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("LRANGE \"{}\" 0 -1", key);
            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_set_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("SORT \"{}\" ALPHA", key);
            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_zset_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("ZRANGE \"{}\" 0 -1 WITHSCORES", key);
            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_stream_entries(
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        keys: &Vec<String>,
    ) {
        for key in keys {
            let cmd = format!("XRANGE \"{}\" - +", key);

            Self::compare_cmd_results(src_conn, dst_conn, &cmd);
        }
    }

    fn compare_cmd_results(src_conn: &mut Connection, dst_conn: &mut Connection, cmd: &str) {
        let src_result = Self::execute_cmd(src_conn, cmd);
        let dst_result = Self::execute_cmd(dst_conn, cmd);
        println!(
            "compare results for cmd: {}, src_result: {:?}, dst_result: {:?}",
            cmd, src_result, dst_result
        );
        assert_eq!(src_result, dst_result);
    }

    fn list_dbs(conn: &mut Connection) -> Vec<String> {
        let mut dbs = Vec::new();
        let cmd = "INFO keyspace";
        match Self::execute_cmd(conn, &cmd) {
            redis::Value::Data(data) => {
                let spaces = String::from_utf8(data).unwrap();
                for space in spaces.split("\r\n").collect::<Vec<&str>>() {
                    if space.contains("db") {
                        let tokens: Vec<&str> = space.split(":").collect::<Vec<&str>>();
                        dbs.push(tokens[0].trim_start_matches("db").to_string());
                    }
                }
            }
            _ => {}
        }
        dbs
    }

    fn list_keys(conn: &mut Connection, match_pattern: &str) -> Vec<String> {
        let mut keys = Vec::new();
        let cmd = format!("KEYS {}", match_pattern);
        match Self::execute_cmd(conn, &cmd) {
            redis::Value::Bulk(values) => {
                for v in values {
                    match v {
                        redis::Value::Data(data) => keys.push(String::from_utf8(data).unwrap()),
                        _ => assert!(false),
                    }
                }
            }
            _ => assert!(false),
        }
        keys.sort();
        keys
    }

    fn get_key_type(conn: &mut Connection, key: &str) -> String {
        let cmd = format!("type \"{}\"", key);
        let value = Self::execute_cmd(conn, &cmd);
        match value {
            redis::Value::Status(key_type) => {
                return key_type;
            }
            _ => assert!(false),
        }
        String::new()
    }

    fn execute_cmds(conn: &mut Connection, cmds: &Vec<String>) {
        for cmd in cmds.iter() {
            Self::execute_cmd(conn, cmd);
        }
    }

    fn execute_cmd(conn: &mut Connection, cmd: &str) -> Value {
        // println!("execute cmd: {:?}", cmd);
        let packed_cmd = Self::pack_cmd(cmd);
        conn.req_packed_command(&packed_cmd).unwrap()
    }

    fn pack_cmd(cmd: &str) -> Vec<u8> {
        // parse cmd args
        let delimiters = vec![' '];
        let escape_pairs = vec![(ARG_QUOTER, ARG_QUOTER)];
        let args = ConfigTokenParser::parse(cmd, &delimiters, &escape_pairs);

        let args: Vec<&str> = args
            .iter()
            .map(|arg| {
                arg.trim_start_matches(ARG_QUOTER)
                    .trim_end_matches(ARG_QUOTER)
            })
            .collect();

        let redis_cmd = RedisCmd::from_str_args(&args);
        CmdEncoder::encode(&redis_cmd)
    }
}
