use std::collections::HashMap;

use anyhow::Result;
use k8s_openapi::api::core::v1::Node;
use kube::{
    api::{Api, ListParams},
    Client,
};

#[derive(Debug, Default)]
pub struct ClusterState {
    /// key: node_name, value: node_state
    pub nodes: HashMap<String, NodeState>,
    /// the number of cpu core
    pub total_core: u32,
    /// the number of memory
    pub total_mem_mb: u32,
}

fn reserved_core(nr_node: u32) -> u32 {
    if nr_node == 1 {
        3
    } else {
        3 + (nr_node - 2)
    }
}

fn reserved_mem(nr_node: u32) -> u32 {
    5 * 1024 * nr_node
}

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct NodeState {
    /// the cpu core
    cpu: u32,
    /// the memory in mb
    mem_mb: u32,
    /// the network bandwidth to storage node
    network_bandwidth_to_storage: Option<u32>,
    /// key: node_name, value: network_bandwidth
    network_bandwidth_to_other_nodes: Option<HashMap<String, u32>>,
}

/// Get the current kubernetes cluster state through kube-api
pub async fn get_cluster_state() -> Result<ClusterState> {
    let mut cluster_state = ClusterState::default();

    // Create a new Kubernetes client
    let client = Client::try_default().await?;
    let nodes: Api<Node> = Api::all(client);

    // List the nodes and print CPU and memory
    let node_list = nodes.list(&ListParams::default()).await?;
    for node in node_list {
        let name = node.metadata.name.unwrap();
        let cpu_capacity = node
            .status
            .as_ref()
            .and_then(|status| {
                status
                    .allocatable
                    .as_ref()
                    .and_then(|allocatable| allocatable.get("cpu").map(|cpu| &cpu.0))
            })
            .expect("(ABNORMAL) failed to get cpu capacity");

        let memory_capacity = node
            .status
            .as_ref()
            .and_then(|status| {
                status
                    .allocatable
                    .as_ref()
                    .and_then(|allocatable| allocatable.get("memory").map(|memory| &memory.0))
            })
            .expect("(ABNORMAL) failed to get memory capacity")
            .chars()
            .filter(|c| c.is_numeric())
            .collect::<String>();
        let mem_mb = memory_capacity.parse::<u32>().unwrap() / 1024;

        let state = NodeState {
            cpu: cpu_capacity.parse::<u32>().unwrap(),
            mem_mb,
            network_bandwidth_to_storage: None,
            network_bandwidth_to_other_nodes: None,
        };
        cluster_state.nodes.insert(name, state);
        cluster_state.total_core += cpu_capacity.parse::<u32>().unwrap();
        cluster_state.total_mem_mb += mem_mb;
    }

    // minus the reserved resources
    cluster_state.total_core -= reserved_core(cluster_state.nodes.len() as u32);
    cluster_state.total_mem_mb -= reserved_mem(cluster_state.nodes.len() as u32);

    Ok(cluster_state)
}
