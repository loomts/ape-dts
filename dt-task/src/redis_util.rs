use dt_common::error::Error;
use dt_connector::sinker::redis::cmd_encoder::CmdEncoder;
use dt_meta::redis::redis_object::RedisCmd;
use redis::ConnectionLike;
use regex::Regex;
use std::str::FromStr;

pub struct RedisUtil {}

const SLOTS_COUNT: u16 = 16384;

impl RedisUtil {
    pub async fn create_redis_conn(url: &str) -> Result<redis::Connection, Error> {
        let conn = redis::Client::open(url)
            .unwrap()
            .get_connection()
            .unwrap_or_else(|_| panic!("can not connect: {}", url));
        Ok(conn)
    }

    pub fn get_cluster_nodes(
        conn: &mut redis::Connection,
    ) -> Result<(Vec<String>, Vec<Vec<u16>>), Error> {
        let cmd = RedisCmd::from_str_args(&["cluster", "nodes"]);
        let value = conn.req_packed_command(&CmdEncoder::encode(&cmd)).unwrap();
        if let redis::Value::Data(data) = value {
            let cluster_nodes = String::from_utf8(data).unwrap();
            Self::parse_cluster_nodes(&cluster_nodes)
        } else {
            Err(Error::Unexpected("can not get redis cluster nodes".into()))
        }
    }

    pub fn get_redis_version(conn: &mut redis::Connection) -> Result<f32, Error> {
        let cmd = RedisCmd::from_str_args(&["INFO"]);
        let value = conn.req_packed_command(&CmdEncoder::encode(&cmd)).unwrap();
        if let redis::Value::Data(data) = value {
            let info = String::from_utf8(data).unwrap();
            let re = Regex::new(r"redis_version:(\S+)").unwrap();
            let cap = re.captures(&info).unwrap();

            let version_str = cap[1].to_string();
            let tokens: Vec<&str> = version_str.split('.').collect();
            if tokens.is_empty() {
                return Err(Error::Unexpected(
                    "can not get redis version by INFO".into(),
                ));
            }

            let mut version = tokens[0].to_string();
            if tokens.len() > 1 {
                version = format!("{}.{}", tokens[0], tokens[1]);
            }
            return Ok(f32::from_str(&version).unwrap());
        }
        Err(Error::Unexpected(
            "can not get redis version by INFO".into(),
        ))
    }

    fn parse_cluster_nodes(cluster_nodes: &str) -> Result<(Vec<String>, Vec<Vec<u16>>), Error> {
        // refer: https://github.com/tair-opensource/RedisShake/blob/v4/internal/utils/cluster_nodes.go
        let mut addresses: Vec<String> = Vec::new();
        let mut slots: Vec<Vec<u16>> = Vec::new();
        let mut slots_count = 0;

        for line in cluster_nodes.lines() {
            let line = line.trim();
            let words: Vec<&str> = line.split_whitespace().collect();

            if !words[2].contains("master") {
                continue;
            }

            if words.len() < 8 {
                panic!("invalid cluster nodes line: {}", line);
            }

            let mut address = words[1].split('@').next().unwrap().to_string();
            let tokens: Vec<&str> = address.split(':').collect();

            if tokens.len() > 2 {
                let port = tokens.last().unwrap();
                let ipv6_addr = tokens[..tokens.len() - 1].join(":");
                address = format!("[{}]:{}", ipv6_addr, port);
            }

            if words.len() < 9 {
                log::warn!(
                    "the current master node does not hold any slots. address=[{}]",
                    address
                );
                continue;
            }

            addresses.push(address);

            let mut slot = Vec::new();
            for word in words.iter().skip(8) {
                if word.starts_with('[') {
                    break;
                }

                let range: Vec<&str> = word.split('-').collect();
                let (start, end) = if range.len() > 1 {
                    (
                        range[0].parse::<u16>().expect("failed to parse slot start"),
                        range[1].parse::<u16>().expect("failed to parse slot end"),
                    )
                } else {
                    let slot_num = word.parse::<u16>().expect("failed to parse slot number");
                    (slot_num, slot_num)
                };

                for j in start..=end {
                    slot.push(j);
                    slots_count += 1;
                }
            }

            slots.push(slot);
        }

        if slots_count != SLOTS_COUNT {
            Err(Error::Unexpected(format!(
                "invalid cluster nodes slots. slots_count={}, cluster_nodes={}",
                slots_count, cluster_nodes
            )))
        } else {
            Ok((addresses, slots))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_cluster_nodes() {
        let cluster_nodes = r#"1f40c5791e5024d1dded03af9cab600a416d3137 172.28.0.25:6379@16379 slave 957edc3536c01c6d1a0669ff3f4c7d39732ab482 0 1705325345720 1 connected
92640aed71fece7d80a2754056b2e01e9bc2f1f5 172.28.0.22:6379@16379 master - 0 1705325346552 2 connected 5461-10922
957edc3536c01c6d1a0669ff3f4c7d39732ab482 172.28.0.21:6379@16379 myself,master - 0 1705325345000 1 connected 0-5460
ccb7888f9767cdff73ad9fc177ccaf69c3f44f49 172.28.0.26:6379@16379 slave 92640aed71fece7d80a2754056b2e01e9bc2f1f5 0 1705325346000 2 connected
a29ce0030c679affbd83c70c5547f481ac2fb2a9 172.28.0.23:6379@16379 master - 0 1705325346971 3 connected 10923-16383
fb755a0d0b2318ab89a56a4653c7be9fcdbe7252 172.28.0.24:6379@16379 slave a29ce0030c679affbd83c70c5547f481ac2fb2a9 0 1705325345000 3 connected"#;
        let (addresses, slots) = RedisUtil::parse_cluster_nodes(cluster_nodes).unwrap();

        assert_eq!(addresses.len(), 3);
        assert_eq!(addresses[0], "172.28.0.22:6379");
        assert_eq!(addresses[1], "172.28.0.21:6379");
        assert_eq!(addresses[2], "172.28.0.23:6379");

        assert_eq!(slots.len(), 3);
        assert_eq!(slots[0][0], 5461);
        assert_eq!(*slots[0].last().unwrap(), 10922);
        assert_eq!(slots[1][0], 0);
        assert_eq!(*slots[1].last().unwrap(), 5460);
        assert_eq!(slots[2][0], 10923);
        assert_eq!(*slots[2].last().unwrap(), 16383);
    }
}
