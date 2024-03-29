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
        let mut all_slots: Vec<Vec<u16>> = Vec::new();
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

            let mut cur_node_slots = Vec::new();
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
                    cur_node_slots.push(j);
                    slots_count += 1;
                }
            }

            all_slots.push(cur_node_slots);
        }

        if slots_count != SLOTS_COUNT {
            Err(Error::Unexpected(format!(
                "invalid cluster nodes slots. slots_count={}, cluster_nodes={}",
                slots_count, cluster_nodes
            )))
        } else {
            Ok((addresses, all_slots))
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_parse_cluster_nodes() {
        let cluster_nodes = r#"09596be5c2150ad93c51fdca1ff9116d1077e042 172.28.0.17:6379@16379 master - 0 1711678515085 7 connected 0-1671 2268 5461-7127 8620 10923-12588 15759
        0e9d360631a20c27f629267bf3e01de8e8c4cbec 172.28.0.11:6379@16379 myself,master - 0 1711678514000 1 connected 1672-2267 2269-5460
        5bafc7277da3038a8fbf01873179260351ed0a0a 172.28.0.13:6379@16379 master - 0 1711678515180 3 connected 12589-15758 15760-16383
        66e84ed6d7f28971cdf59d530c490561c64dda61 172.28.0.16:6379@16379 slave c02d3f6210367e1b7bbfd131b5c2269520ef4f73 0 1711678514561 2 connected
        7dd62287c3543b076551b7412cd7425f8251809d 172.28.0.18:6379@16379 slave 09596be5c2150ad93c51fdca1ff9116d1077e042 0 1711678514000 7 connected
        c02d3f6210367e1b7bbfd131b5c2269520ef4f73 172.28.0.12:6379@16379 master - 0 1711678514044 2 connected 7128-8619 8621-10922
        76d90b851f7692358d9a01d783cf64c1ac673ef5 172.28.0.15:6379@16379 slave 0e9d360631a20c27f629267bf3e01de8e8c4cbec 0 1711678514562 1 connected
        587ec020a7cd63397afe33d6e92ee975b4ab79a2 172.28.0.14:6379@16379 slave 5bafc7277da3038a8fbf01873179260351ed0a0a 0 1711678514562 3 connected"#;
        let (addresses, slots) = RedisUtil::parse_cluster_nodes(cluster_nodes).unwrap();

        assert_eq!(addresses.len(), 4);

        assert_eq!(slots.len(), 4);
        assert_eq!(slots[0].len(), 5008);
        assert_eq!(slots[1].len(), 3788);
        assert_eq!(slots[2].len(), 3794);
        assert_eq!(slots[3].len(), 3794);

        assert!(slots[0].contains(&0));
        assert!(slots[0].contains(&1671));
        assert!(slots[0].contains(&15759));

        assert!(slots[1].contains(&1672));
        assert!(slots[1].contains(&2267));
        assert!(slots[1].contains(&2269));
        assert!(slots[1].contains(&5460));
    }
}
