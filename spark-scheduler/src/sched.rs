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
use tokio::sync::{Mutex, RwLock};

use std::collections::HashMap;
use std::sync::Arc;

use crate::ops::{EmitParameters, PodBindParameters};
use crate::predprio::{EnoughResourcePredicate, Predicate, Priority, RandomPriority};

const SCHEDULER_NAME: &str = "spark-sched";
const SPARK_NAMESPACE: &str = "spark";

/// The allocation scene, the semantic is that #nr pods are allocated to #node
#[derive(Debug, Default, Clone)]
pub(crate) struct Alloc {
    node: String,
    nr: i32,
}

#[derive(Debug, Default, Clone)]
pub(crate) struct NodeResource {
    pub(crate) cpu: u32,
    pub(crate) mem_kb: u32,
}

/// use uuid as key, the vector of alloc is explained below
///
/// Each spark job has a uuid string with key spark-uuid (can be seen in spark-submmiter/src/cmd.rs)
/// with the uuid string as key, you can query the allocation history of the job.
/// The Scheduler bookkeeps the allocation history, each entry of Vec<Alloc> corresponds to a node
/// and the number of pods allocated to it.
pub(crate) type SchedHistory = HashMap<String, Vec<Alloc>>;

pub(crate) struct Scheduler {
    pub(crate) client: Client,
    pub(crate) namespace: String,
    pub(crate) predicate: Arc<dyn Predicate>,
    pub(crate) priority: Arc<dyn Priority>,

    pub(crate) node_resource_map: Mutex<HashMap<String, NodeResource>>,
    pub(crate) prev_sched: RwLock<SchedHistory>,
}

impl Scheduler {
    pub async fn new(client: Client) -> Self {
        let node_resource_map = Mutex::new(HashMap::new());

        // Get a node list
        let node_api: Api<Node> = Api::all(client.clone());
        let nodes = node_api.list(&ListParams::default()).await.unwrap().items;
        for n in nodes {
            let name = n.metadata.name.unwrap();
            let mut cpu = n
                .status
                .as_ref()
                .and_then(|status| {
                    status
                        .allocatable
                        .as_ref()
                        .and_then(|allocatable| allocatable.get("cpu").map(|cpu| &cpu.0))
                })
                .expect("(ABNORMAL) failed to get cpu capacity")
                .parse::<u32>()
                .unwrap();

            // HACK: master node reserves more..
            if &name == "node02" {
                cpu -= 1;
            }

            let mem_kb =
                n.status
                    .as_ref()
                    .and_then(|status| {
                        status.allocatable.as_ref().and_then(|allocatable| {
                            allocatable.get("memory").map(|memory| &memory.0)
                        })
                    })
                    .expect("(ABNORMAL) failed to get memory capacity")
                    .chars()
                    .filter(|c| c.is_numeric())
                    .collect::<String>()
                    .parse::<u32>()
                    .unwrap();

            node_resource_map.lock().await.insert(
                name,
                NodeResource {
                    cpu: cpu - 1,
                    mem_kb: mem_kb - 5 * 1024 * 1024,
                },
            );
        }

        Scheduler {
            client,
            namespace: SPARK_NAMESPACE.to_string(),
            predicate: Arc::new(EnoughResourcePredicate::default()),
            priority: Arc::new(RandomPriority::default()),
            prev_sched: RwLock::new(HashMap::new()),
            node_resource_map,
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

        let node_name = self.eval_and_bind(&pod).await;
        if node_name.is_err() {
            return false;
        }
        let node_name = node_name.unwrap();

        let message = format!(
            "Placed pod [{}/{}] on {}\n",
            &pod_namespace, &pod_name, &node_name
        );
        println!("{}", &message.trim_end());

        self.update_sched_hist();

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
    fn update_sched_hist(&self) {
        //todo
    }

    async fn eval_and_bind(&self, pod: &Pod) -> Result<String> {
        let mut node_resource_map = self.node_resource_map.lock().await;

        let pod_resource = pod_resource(pod);
        let filtered_node_names = self.predicate.judge(&node_resource_map, pod_resource).await;

        if filtered_node_names.is_empty() {
            return Err(anyhow!(format!(
                "failed to find node that fits pod {}/{}",
                pod.metadata.namespace.as_ref().unwrap(),
                pod.metadata.name.as_ref().unwrap()
            )));
        }

        let prev_sched = self.prev_sched.read().await;
        let priorities = self.prioritize(&filtered_node_names, pod, &prev_sched);
        let best_node = self.find_best_node(&priorities);

        // bind the pod to the node
        let bind_params = PodBindParameters {
            node_name: best_node.clone(),
            pod: pod.clone(),
            scheduler_name: SCHEDULER_NAME.to_string(),
        };
        let bind_result = self.bind_pod_to_node(bind_params).await;

        let pod_name = pod.metadata.name.as_ref().expect("empty pod name");
        let pod_namespace = pod
            .metadata
            .namespace
            .as_ref()
            .expect("empty pod namespace");

        match bind_result {
            Ok(_) => {
                // update node resource map
                node_resource_map.get_mut(&best_node).unwrap().cpu -= pod_resource.cpu;
                node_resource_map.get_mut(&best_node).unwrap().mem_kb -= pod_resource.mem_kb;
            }
            Err(e) => {
                println!(
                    "failed to bind pod {}/{} to node {}: {}",
                    &pod_namespace, &pod_name, &best_node, e
                );
            }
        }

        Ok(best_node)
    }

    fn prioritize(
        &self,
        node_names: &[String],
        pod: &Pod,
        prev_sched: &SchedHistory,
    ) -> HashMap<String, i32> {
        let mut result = HashMap::new();
        for node_name in node_names {
            let score = self.priority.priority(node_name, pod, prev_sched);
            result.insert(node_name.clone(), score);
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

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PodResource {
    pub(crate) cpu: u32,
    pub(crate) mem_kb: u32,
}

pub(crate) fn pod_resource(pod: &Pod) -> PodResource {
    let pod_req = pod
        .spec
        .as_ref()
        .unwrap()
        .containers
        .get(0)
        .unwrap()
        .resources
        .as_ref()
        .unwrap()
        .requests
        .as_ref()
        .unwrap();

    let cpu = pod_req.get("cpu").unwrap().0.parse::<u32>().unwrap();
    let mem_kb = pod_req
        .get("memory")
        .unwrap()
        .0
        .chars()
        .filter(|c| c.is_numeric())
        .collect::<String>()
        .parse::<u32>()
        .unwrap();
    PodResource { cpu, mem_kb }
}
