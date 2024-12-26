use std::collections::HashMap;

#[derive(Clone)]
pub struct ClusterNode {
    pub is_master: bool,
    pub id: String,
    // if current node is slave, master_id refers to the master it follows
    pub master_id: String,
    pub host: String,
    pub port: String,
    pub address: String,
    pub slots: Vec<u16>,
    pub slot_hash_tag_map: HashMap<u16, String>,
}
