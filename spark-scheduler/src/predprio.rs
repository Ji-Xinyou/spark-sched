use k8s_openapi::api::core::v1::{Node, Pod};

use crate::sched::SchedHistory;

pub(crate) trait Predicate: Send + Sync {
    fn predicate(&self, node: &Node, pod: &Pod) -> bool;
}

pub(crate) trait Priority: Send + Sync {
    fn priority(&self, node: &Node, pod: &Pod, prev_sched: &SchedHistory) -> i32;
}
