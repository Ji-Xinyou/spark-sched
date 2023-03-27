use anyhow::{anyhow, Result};
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::{Node, Pod};
use kube::Api;
use kube::{
    api::ListParams,
    runtime::{watcher, WatchStreamExt},
    Client,
};
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio::sync::RwLock;

use std::collections::HashMap;
use std::sync::Arc;

use crate::ops::{EmitParameters, PodBindParameters};
use crate::predprio::{Predicate, Priority, RandomPredicate, RandomPriority};

const SCHEDULER_NAME: &str = "spark-sched";
const SPARK_NAMESPACE: &str = "spark";

#[derive(Debug, Default, Clone)]
pub(crate) struct Alloc {
    node: String,
    nr: i32,
}

pub(crate) type SchedHistory = HashMap<String, Vec<Alloc>>;

pub(crate) struct Scheduler {
    pub(crate) client: Client,
    pub(crate) namespace: String,
    pub(crate) node_list: Arc<RwLock<Vec<Node>>>,
    pub(crate) predicates: Vec<Arc<dyn Predicate>>,
    pub(crate) priorities: Vec<Arc<dyn Priority>>,
    pub(crate) prev_sched: SchedHistory,
}

impl Scheduler {
    pub async fn new(client: Client) -> Self {
        // Get a node list
        let node_api: Api<Node> = Api::all(client.clone());
        let nodes = node_api.list(&ListParams::default()).await.unwrap().items;

        Scheduler {
            client,
            namespace: SPARK_NAMESPACE.to_string(),
            node_list: Arc::new(RwLock::new(nodes)),
            predicates: vec![Arc::new(RandomPredicate::default())],
            priorities: vec![Arc::new(RandomPriority::default())],
            prev_sched: HashMap::new(),
        }
    }

    pub async fn run(self) -> Result<()> {
        let (tx, mut rx) = unbounded_channel();

        // the thread that watches for new pods added event
        self.start_pod_watcher(tx);

        let sched = Arc::new(self);

        loop {
            println!("Waiting to schedule pod...");
            let pod = rx.recv().await.expect("the pod queue is closed");
            let sched = sched.clone();

            tokio::spawn(async move {
                let ok = sched.sched_pod(pod).await;
                println!("pod scheduled success??: {}", ok);
            });
        }
    }

    fn start_pod_watcher(&self, tx: UnboundedSender<Pod>) {
        // List params to only obtain pods that are unscheduled/not bound to a node and
        // has the specified scheduler name set
        let unscheduled_lp = ListParams::default()
            .fields(format!("spec.schedulerName={},spec.nodeName=", SCHEDULER_NAME).as_str());
        let client = self.client.clone();
        let namespace = self.namespace.clone();

        println!("starting pod watcher, watching namespace {}...", namespace);
        tokio::spawn(async move {
            let pods: Api<Pod> = Api::namespaced(client, &namespace);
            let watcher = watcher(pods, unscheduled_lp);
            watcher
                .applied_objects()
                .try_for_each(|p| async {
                    tx.send(p).expect("failed to send pod to the queue");
                    Ok(())
                })
                .await
                .expect("failed to watch pods");

            println!("[NOTICE] the watcher is closed??");
            unreachable!()
        });
    }

    /// schedule a pod, return true if the pod is scheduled successfully
    async fn sched_pod(&self, pod: Pod) -> bool {
        let pod_name = pod.metadata.name.as_ref().expect("empty pod name");
        let pod_namespace = pod
            .metadata
            .namespace
            .as_ref()
            .expect("empty pod namespace");

        println!("found a pod to schedule: {}/{}", &pod_namespace, &pod_name);

        let node_name = self.find_best_node_for(&pod).await;
        if node_name.is_err() {
            println!(
                "cannot find node that fits pod {}/{}: {}",
                &pod_namespace,
                &pod_name,
                node_name.err().unwrap()
            );
            return false;
        }

        // bind the pod to the node
        let node_name = node_name.unwrap();
        let bind_params = PodBindParameters {
            node_name: node_name.clone(),
            pod: pod.clone(),
            scheduler_name: SCHEDULER_NAME.to_string(),
        };
        let bind_result = self.bind_pod_to_node(bind_params).await;
        if bind_result.is_err() {
            println!(
                "failed to bind pod {}/{}: {}",
                &pod_namespace,
                &pod_name,
                bind_result.err().unwrap()
            );
            return false;
        }

        let message = format!(
            "Placed pod [{}/{}] on {}\n",
            &pod_namespace, &pod_name, &node_name
        );
        println!("{}", &message.trim_end());

        // emit the event the the pod has been binded
        let emit_params = EmitParameters {
            pod,
            scheduler_name: SCHEDULER_NAME.to_string(),
            message,
        };
        let event_result = self.emit_event(emit_params).await;
        if event_result.is_err() {
            println!(
                "failed to emit scheduled event: {}",
                event_result.err().unwrap()
            );
        }

        true
    }
}

// utilities
impl Scheduler {
    async fn find_best_node_for(&self, pod: &Pod) -> Result<String> {
        let nodes = self.node_list.clone();
        let nodes = nodes.read().await;

        println!("Before filtering we have {} nodes", nodes.len());
        let filtered_nodes = self.predicate_filtered_nodes(pod, &nodes);
        println!("After filtering we have {} nodes", filtered_nodes.len());

        if filtered_nodes.is_empty() {
            return Err(anyhow!(format!(
                "failed to find node that fits pod {}/{}",
                pod.metadata.namespace.as_ref().unwrap(),
                pod.metadata.name.as_ref().unwrap()
            )));
        }

        let priorities = self.prioritize(&filtered_nodes, pod, &self.prev_sched, &self.priorities);
        let best_node = self.find_best_node(&priorities);
        Ok(best_node)
    }

    fn predicate_filtered_nodes(&self, pod: &Pod, nodes: &[Node]) -> Vec<Node> {
        nodes
            .into_iter()
            .filter(|node| self.predicate_ok(pod, node))
            .cloned()
            .collect()
    }

    fn predicate_ok(&self, pod: &Pod, node: &Node) -> bool {
        for predicate in &self.predicates {
            if !predicate.predicate(node, pod) {
                return false;
            }
        }
        true
    }

    fn prioritize(
        &self,
        nodes: &[Node],
        pod: &Pod,
        prev_sched: &SchedHistory,
        priorities: &[Arc<dyn Priority>],
    ) -> HashMap<String, i32> {
        let mut result = HashMap::new();
        for node in nodes {
            let mut score = 0;
            for priority in priorities {
                score += priority.priority(node, pod, prev_sched);
            }
            result.insert(node.metadata.name.clone().unwrap(), score);
        }
        result
    }

    fn find_best_node(&self, priorities: &HashMap<String, i32>) -> String {
        let mut max_p = i32::MIN;
        let mut best_node = String::new();
        for (node, p) in priorities {
            if *p > max_p {
                max_p = *p;
                best_node = node.clone();
            }
        }
        best_node
    }
}
