#[derive(Clone, Default)]
pub struct DataMarkerConfig {
    pub topo_name: String,
    pub topo_nodes: String,
    pub src_node: String,
    pub dst_node: String,
    pub do_nodes: String,
    pub ignore_nodes: String,
    pub marker: String,
}
