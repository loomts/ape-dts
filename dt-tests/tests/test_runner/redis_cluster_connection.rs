use std::collections::{HashMap, HashSet};

use dt_common::error::Error;
use dt_common::meta::redis::command::key_parser::KeyParser;
use dt_task::{redis_util::RedisUtil, task_util::TaskUtil};
use redis::Connection;
use url::Url;

pub struct RedisClusterConnection {
    slot_node_map: HashMap<u16, &'static str>,
    node_conn_map: HashMap<String, Connection>,
    default_conn: Connection,
    key_parser: KeyParser,
}

impl RedisClusterConnection {
    pub async fn new(url: &str, is_cluster: bool) -> Result<Self, Error> {
        let mut slot_node_map = HashMap::new();
        let mut node_conn_map = HashMap::new();
        let mut conn = TaskUtil::create_redis_conn(url).await?;

        if is_cluster {
            let (nodes, slots) = RedisUtil::get_cluster_nodes(&mut conn)?;
            for i in 0..nodes.len() {
                let node: &'static str = Box::leak(nodes[i].clone().into_boxed_str());
                for slot in slots[i].iter() {
                    slot_node_map.insert(*slot, node);
                }
            }

            let url_info = Url::parse(url).unwrap();
            let username = url_info.username();
            let password = if let Some(password) = url_info.password() {
                password.to_string()
            } else {
                String::new()
            };

            let (addresses, _slots) = RedisUtil::get_cluster_nodes(&mut conn)?;
            println!("redis cluster nodes: {:?}", addresses);

            for address in addresses {
                let new_url = format!("redis://{}:{}@{}", username, password, address);
                let conn = RedisUtil::create_redis_conn(&new_url).await?;
                node_conn_map.insert(address.clone(), conn);
            }
        }

        Ok(Self {
            slot_node_map,
            node_conn_map,
            default_conn: conn,
            key_parser: KeyParser::new(),
        })
    }

    pub fn get_default_conn(&mut self) -> &mut Connection {
        &mut self.default_conn
    }

    pub fn get_node_conn_by_key(&mut self, key: &str) -> &mut Connection {
        if self.slot_node_map.is_empty() {
            return self.get_default_conn();
        }

        let slot = KeyParser::calc_slot(key.as_bytes());
        let node = *self.slot_node_map.get(&slot).unwrap();
        println!("get redis node: {} by key: {:?}", node, key);

        self.node_conn_map.get_mut(node).unwrap()
    }

    pub fn get_node_conns_by_cmd<'a>(&'a mut self, args: &Vec<String>) -> Vec<&'a mut Connection> {
        if self.slot_node_map.is_empty() {
            return vec![self.get_default_conn()];
        }

        let (_cmd_name, _group, keys, _keys_indexes) =
            self.key_parser.parse_key_from_argv(args).unwrap();
        if keys.is_empty() {
            // some cmd has no key, and should be executed in all nodes, example: SWAPDB 0 1
            let conns: Vec<&mut Connection> = self.node_conn_map.iter_mut().map(|i| i.1).collect();
            conns
        } else {
            let mut nodes = HashSet::new();
            for key in keys.iter() {
                let slot = KeyParser::calc_slot(key.as_bytes());
                nodes.insert(self.slot_node_map.get(&slot).unwrap().to_string());
            }
            println!("get redis nodes: {:?} by keys: {:?}", nodes, keys);

            let mut conns = Vec::new();
            for (node, conn) in self.node_conn_map.iter_mut() {
                if nodes.contains(node) {
                    conns.push(conn);
                }
            }
            conns
        }
    }
}
