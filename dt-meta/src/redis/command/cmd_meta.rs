use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Default)]
pub struct CmdMeta {
    pub name: String,
    pub group: String,
    pub key_spec: Vec<KeySpec>,
}

/// refer: https://redis.io/docs/reference/key-specs/
#[derive(Serialize, Deserialize, Default)]
pub struct KeySpec {
    pub begin_search_type: String,
    pub begin_search_index: i32,
    pub begin_search_keyword: String,
    pub begin_search_start_from: i32,
    pub find_keys_type: String,
    pub find_keys_range_last_key: i32,
    pub find_keys_range_key_step: usize,
    pub find_keys_range_limit: i32,
    pub find_keys_keynum_index: i32,
    pub find_keys_keynum_first_key: i32,
    pub find_keys_keynum_key_step: usize,
}
