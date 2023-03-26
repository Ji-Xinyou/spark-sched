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
    nodes: HashMap<String, NodeState>,
}

#[derive(Debug, Default)]
pub struct NodeState {
    /// the cpu core
    cpu: u32,
    /// the memory in kib
    mem_kib: u32,
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
        let mem_kib = memory_capacity.parse::<u32>().unwrap();

        let state = NodeState {
            cpu: cpu_capacity.parse::<u32>().unwrap(),
            mem_kib,
            network_bandwidth_to_storage: None,
            network_bandwidth_to_other_nodes: None,
        };
        cluster_state.nodes.insert(name, state);
    }

    Ok(cluster_state)
}

fn hardcoded_network_bandwidth(node_name: String) -> HashMap<String, u32> {
    todo!()
}
