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
    fn priority(&self, node: &str, pod: &Pod, prev_sched: &SchedHistory) -> i32;
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
        node_names
    }
}

#[derive(Debug, Default)]
pub(crate) struct RandomPriority;

#[derive(Debug, Default)]
pub(crate) struct GangPriority;

impl Priority for RandomPriority {
    fn priority(&self, _node_name: &str, _pod: &Pod, _prev_sched: &SchedHistory) -> i32 {
        let mut rng = rand::thread_rng();
        let random_int = rng.gen_range(0..=100);
        random_int
    }
}
