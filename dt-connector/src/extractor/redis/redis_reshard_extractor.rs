use std::collections::HashMap;

use async_trait::async_trait;
use dt_common::{
    log_debug, log_info,
    meta::redis::{
        cluster_node::ClusterNode, command::cmd_encoder::CmdEncoder, redis_object::RedisCmd,
    },
    utils::redis_util::RedisUtil,
};
use redis::{Connection, ConnectionLike};
use serde_json::json;
use url::Url;

use crate::{extractor::base_extractor::BaseExtractor, Extractor};

const SLOTS_COUNT: usize = 16384;

pub struct RedisReshardExtractor {
    pub base_extractor: BaseExtractor,
    pub url: String,
    pub to_node_ids: Vec<String>,
}

#[async_trait]
impl Extractor for RedisReshardExtractor {
    async fn extract(&mut self) -> anyhow::Result<()> {
        log_info!("RedisReshardExtractor starts");
        self.reshard().await.unwrap();
        self.base_extractor.wait_task_finish().await
    }
}

impl RedisReshardExtractor {
    pub async fn reshard(&self) -> anyhow::Result<()> {
        let mut conn = RedisUtil::create_redis_conn(&self.url).await?;
        let nodes = RedisUtil::get_cluster_master_nodes(&mut conn)?;
        let slot_address_map = RedisUtil::get_slot_address_map(&nodes);

        let avg_node_count = SLOTS_COUNT / nodes.len();
        let mut node_slot_count = HashMap::new();
        let mut move_out_slots = Vec::new();

        // calculate slots to be moved out from existing nodes
        for node in nodes.iter() {
            node_slot_count.insert(node.id.clone(), node.slots.len());

            if self.to_node_ids.contains(&node.id) {
                continue;
            }

            for i in avg_node_count..node.slots.len() {
                move_out_slots.push(node.slots[i]);
            }
        }

        let mut node_move_in_slots = HashMap::new();
        let mut i = 0;
        for node_id in self.to_node_ids.iter() {
            let exist_slot_count = *node_slot_count.get(node_id).unwrap();
            let mut move_in_slots = Vec::new();

            while i < move_out_slots.len() {
                if move_in_slots.len() + exist_slot_count >= avg_node_count {
                    break;
                }
                move_in_slots.push(move_out_slots[i]);
                i += 1;
            }

            log_info!(
                "will move slots to {}, slots: {}",
                node_id,
                json!(move_in_slots),
            );
            node_move_in_slots.insert(node_id.to_owned(), move_in_slots);
        }

        self.move_slots(&nodes, &node_move_in_slots, &slot_address_map)
            .await?;

        Ok(())
    }

    async fn move_slots(
        &self,
        nodes: &[ClusterNode],
        node_move_in_slots: &HashMap<String, Vec<u16>>,
        slot_address_map: &HashMap<u16, &str>,
    ) -> anyhow::Result<()> {
        for (dst_node_id, move_in_slots) in node_move_in_slots.iter() {
            // get dst_node by id
            let dst_node = nodes.iter().find(|i| i.id == *dst_node_id).unwrap();
            let mut dst_conn = self.get_node_conn(dst_node).await.unwrap();

            let mut cur_src_node: Option<ClusterNode> = None;
            let mut cur_src_conn: Option<Connection> = None;
            for slot in move_in_slots.iter() {
                // get src_node by address
                let src_address = slot_address_map.get(slot).unwrap().to_string();
                let src_node = nodes.iter().find(|i| i.address == *src_address).unwrap();

                // get src conn
                let src_node_changed =
                    cur_src_node.is_none() || src_node.id != cur_src_node.as_ref().unwrap().id;
                if src_node_changed {
                    cur_src_node = Some(src_node.clone());
                    cur_src_conn = Some(self.get_node_conn(src_node).await.unwrap());
                }

                // move slot
                self.setslot_and_migrate(
                    src_node,
                    dst_node,
                    cur_src_conn.as_mut().unwrap(),
                    &mut dst_conn,
                    *slot,
                )
                .await
                .unwrap();
            }
        }
        Ok(())
    }

    async fn setslot_and_migrate(
        &self,
        src_node: &ClusterNode,
        dst_node: &ClusterNode,
        src_conn: &mut Connection,
        dst_conn: &mut Connection,
        slot: u16,
    ) -> anyhow::Result<()> {
        log_info!(
            "moving slot {} from {} to {}",
            slot,
            src_node.id,
            dst_node.id
        );

        let keys = Self::get_keys_in_slot(src_conn, slot).unwrap();
        log_info!("slot {} has {} keys", slot, keys.len());

        // cluster setslot importing
        let dst_cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "importing",
            &src_node.id,
        ]);
        // cluster setslot migrating
        let src_cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "migrating",
            &dst_node.id,
        ]);
        dst_conn
            .req_packed_command(&CmdEncoder::encode(&dst_cmd))
            .unwrap();
        src_conn
            .req_packed_command(&CmdEncoder::encode(&src_cmd))
            .unwrap();

        // migrate
        for key in keys.iter() {
            log_debug!(
                "migrating key: [{}] in slot {} from {} to {}",
                key,
                slot,
                src_node.id,
                dst_node.id
            );
            let cmd = RedisCmd::from_str_args(&[
                "migrate",
                &dst_node.host,
                &dst_node.port,
                "",
                "0",
                "5000",
                "keys",
                key,
            ]);
            src_conn
                .req_packed_command(&CmdEncoder::encode(&cmd))
                .unwrap();
        }

        // cluster setslot node
        let cmd = RedisCmd::from_str_args(&[
            "cluster",
            "setslot",
            &slot.to_string(),
            "node",
            &dst_node.id,
        ]);
        dst_conn
            .req_packed_command(&CmdEncoder::encode(&cmd))
            .unwrap();
        src_conn
            .req_packed_command(&CmdEncoder::encode(&cmd))
            .unwrap();
        log_info!(
            "moved slot {} from {} to {}",
            slot,
            src_node.id,
            dst_node.id
        );

        Ok(())
    }

    fn get_keys_in_slot(conn: &mut Connection, slot: u16) -> anyhow::Result<Vec<String>> {
        // get all keys in slot
        let cmd =
            RedisCmd::from_str_args(&["cluster", "getkeysinslot", &slot.to_string(), "100000000"]);
        let packed_cmd = &CmdEncoder::encode(&cmd);
        let result = conn.req_packed_command(packed_cmd).unwrap();
        RedisUtil::parse_result_as_string(result)
    }

    async fn get_node_conn(&self, node: &ClusterNode) -> anyhow::Result<Connection> {
        let url_info = Url::parse(&self.url).unwrap();
        let username = url_info.username();
        let password = url_info.password().unwrap_or("").to_string();
        let url = format!("redis://{}:{}@{}", username, password, node.address);
        RedisUtil::create_redis_conn(&url).await
    }
}
