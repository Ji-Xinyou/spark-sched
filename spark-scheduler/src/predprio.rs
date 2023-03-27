use k8s_openapi::api::core::v1::{Node, Pod};
use rand::Rng;

use crate::sched::SchedHistory;

pub(crate) trait Predicate: Send + Sync {
    fn predicate(&self, node: &Node, pod: &Pod) -> bool;
}

pub(crate) trait Priority: Send + Sync {
    fn priority(&self, node: &Node, pod: &Pod, prev_sched: &SchedHistory) -> i32;
}

#[derive(Debug, Default)]
pub(crate) struct RandomPredicate;

#[derive(Debug, Default)]
pub(crate) struct EnoughResourcePredicate;

impl Predicate for RandomPredicate {
    fn predicate(&self, _node: &Node, _pod: &Pod) -> bool {
        rand::thread_rng().gen_bool(0.5)
    }
}

#[derive(Debug, Default)]
pub(crate) struct RandomPriority;

#[derive(Debug, Default)]
pub(crate) struct GangPriority;

impl Priority for RandomPriority {
    fn priority(&self, _node: &Node, _pod: &Pod, _prev_sched: &SchedHistory) -> i32 {
        let mut rng = rand::thread_rng();
        let random_int = rng.gen_range(0..=100);
        random_int
    }
}
