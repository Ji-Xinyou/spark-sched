use std::collections::HashMap;

use async_trait::async_trait;
use k8s_openapi::api::core::v1::Pod;
use rand::Rng;

use crate::sched::{NodeResource, PodResource, SchedHistory};

/// Gives filtered node_names
#[async_trait]
pub(crate) trait Predicate: Send + Sync {
    async fn judge(
        &self,
        node_resource_map: &HashMap<String, NodeResource>,
        pod_resource: PodResource,
    ) -> Vec<String>;
}

pub(crate) trait Priority: Send + Sync {
    fn priority(
        &self,
        node: &str,
        node_resource_map: &HashMap<String, NodeResource>,
        pod: &Pod,
        prev_sched: &SchedHistory,
    ) -> u32;
}

/// EnoughResourcePredicate filters the nodes that have enough resources to
/// schedule the pod.
#[derive(Debug, Default)]
pub(crate) struct EnoughResourcePredicate;

#[async_trait]
impl Predicate for EnoughResourcePredicate {
    async fn judge(
        &self,
        node_resource_map: &HashMap<String, NodeResource>,
        pod_resource: PodResource,
    ) -> Vec<String> {
        let mut node_names = vec![];
        for (node_name, resource) in node_resource_map {
            let cpu = resource.cpu;
            let mem_kb = resource.mem_kb;

            let pod_cpu = pod_resource.cpu;
            let pod_mem_kb = pod_resource.mem_kb;

            if cpu >= pod_cpu && mem_kb >= pod_mem_kb {
                node_names.push(node_name.to_string());
            }
        }
        println!(
            "\njudging\n  resource: {:#?}\n  filtered: {:#?}\n",
            node_resource_map, node_names
        );
        node_names
    }
}

#[derive(Debug, Default)]
pub(crate) struct RandomPriority;

/// Gang Priority is a priority function that prioritizes nodes based on the
/// sched history of the pods. For those with the same uuid, we give me highest
/// priority to the node that has the most pods with the same uuid.
///
/// todo: consider network
#[derive(Debug, Default)]
pub(crate) struct GangPriority;

/// Based on GangPriority, this also takes network speed into account, it will give the
/// highest priority to the pod that has the fastest network bandwidth with the storage node.
/// It also consider the network speed between candidate node and the nodes having its peer.
#[derive(Debug, Default)]
pub(crate) struct NetworkAwareGangPriority;

impl Priority for RandomPriority {
    fn priority(
        &self,
        _node_name: &str,
        _node_resource_map: &HashMap<String, NodeResource>,
        _pod: &Pod,
        _prev_sched: &SchedHistory,
    ) -> u32 {
        let mut rng = rand::thread_rng();
        let random_int = rng.gen_range(0..=100);
        random_int
    }
}

impl Priority for GangPriority {
    fn priority(
        &self,
        node_name: &str,
        node_resource_map: &HashMap<String, NodeResource>,
        pod: &Pod,
        prev_sched: &SchedHistory,
    ) -> u32 {
        let uuid = pod
            .clone()
            .metadata
            .labels
            .unwrap()
            .get("spark-uuid")
            .unwrap()
            .clone();

        // Some pods of this uuid has already been sched, check if the node_name is in it
        let is_peer_sched = prev_sched.get(&uuid);
        if is_peer_sched.is_some() {
            let peer_sched = is_peer_sched.unwrap();
            for alloc in peer_sched {
                if alloc.node_name == node_name {
                    return 100;
                }
            }
        }

        // either no pod of this uuid has been sched, or this node_name has no pod of this uuid
        // return the one with the most cpu
        node_resource_map.get(node_name).unwrap().cpu * 10
    }
}

impl Priority for NetworkAwareGangPriority {
    fn priority(
        &self,
        _node_name: &str,
        _node_resource_map: &HashMap<String, NodeResource>,
        _pod: &Pod,
        _prev_sched: &SchedHistory,
    ) -> u32 {
        unimplemented!()
    }
}
