use super::base_test_runner::BaseTestRunner;
use dt_common::{
    config::{
        config_token_parser::ConfigTokenParser, extractor_config::ExtractorConfig,
        sinker_config::SinkerConfig, task_config::TaskConfig,
    },
    error::Error,
};
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
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

    pub async fn run_snapshot_test(&mut self, compare_data: bool) -> Result<(), Error> {
        self.execute_test_ddl_sqls()?;

        let mut src_conn = self.src_conn.as_mut().unwrap();
        Self::execute_cmds(&mut src_conn, &self.base.src_dml_sqls);
        self.base.start_task().await?;

        if compare_data {
            // self.compare_data_for_tbs().await;
            self.compare_data()?;
        }

        Ok(())
    }

    fn execute_test_ddl_sqls(&mut self) -> Result<(), Error> {
        let mut src_conn = self.src_conn.as_mut().unwrap();
        let mut dst_conn = self.dst_conn.as_mut().unwrap();
        Self::execute_cmds(&mut src_conn, &self.base.src_ddl_sqls);
        Self::execute_cmds(&mut dst_conn, &self.base.dst_ddl_sqls);
        Ok(())
    }

    fn compare_data(&mut self) -> Result<(), Error> {
        let src_conn = self.src_conn.as_mut().unwrap();
        let dst_conn = self.dst_conn.as_mut().unwrap();

        // string entries
        let set_keys = Self::list_keys(src_conn, "set_key_*");
        Self::compare_string_entries(src_conn, dst_conn, &set_keys);

        let mset_keys = Self::list_keys(src_conn, "mset_key_*");
        Self::compare_string_entries(src_conn, dst_conn, &mset_keys);

        // hash entries
        let hset_keys = Self::list_keys(src_conn, "hset_key_*");
        Self::compare_hash_entries(src_conn, dst_conn, &hset_keys);

        let hmset_keys = Self::list_keys(src_conn, "hmset_key_*");
        Self::compare_hash_entries(src_conn, dst_conn, &hmset_keys);

        // list entries
        let list_keys = Self::list_keys(src_conn, "list_key_*");
        Self::compare_list_entries(src_conn, dst_conn, &list_keys);

        // set entries
        let sets_keys = Self::list_keys(src_conn, "sets_key_*");
        Self::compare_set_entries(src_conn, dst_conn, &sets_keys);

        // zset entries
        let zset_keys = Self::list_keys(src_conn, "zset_key_*");
        Self::compare_zset_entries(src_conn, dst_conn, &zset_keys);

        // stream entries
        let stream_keys = Self::list_keys(src_conn, "stream_key_*");
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
        keys
    }

    fn execute_cmds(conn: &mut Connection, cmds: &Vec<String>) {
        for cmd in cmds.iter() {
            Self::execute_cmd(conn, cmd);
        }
    }

    fn execute_cmd(conn: &mut Connection, cmd: &str) -> Value {
        println!("execute cmd: {:?}", cmd);
        let packed_cmd = Self::pack_cmd(cmd);
        conn.req_packed_command(&packed_cmd).unwrap()
    }

    fn pack_cmd(cmd: &str) -> Vec<u8> {
        // parse cmd args
        let delimiters = vec![' '];
        let escape_pairs = vec![(ARG_QUOTER, ARG_QUOTER)];
        let args = ConfigTokenParser::parse(cmd, &delimiters, &escape_pairs);

        let args = args
            .iter()
            .map(|arg| {
                arg.trim_start_matches(ARG_QUOTER)
                    .trim_end_matches(ARG_QUOTER)
                    .to_string()
            })
            .collect::<Vec<String>>();

        // turn args vec into bytes vec
        let mut cmd_bytes = vec![];
        for arg in args {
            cmd_bytes.push(arg.as_bytes().to_vec());
        }

        // encode cmd bytes
        CmdEncoder::encode(&cmd_bytes)
    }
}
