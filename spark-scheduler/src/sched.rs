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
use crate::predprio::{EnoughResourcePredicate, Predicate, Priority};

const SCHEDULER_NAME: &str = "spark-sched";
const SPARK_NAMESPACE: &str = "spark";
const DEFAULT_MASTER_NODE_NAME: &str = "node02";

/// The allocation scene, the semantic is that #nr pods are allocated to #node
#[derive(Debug, Default, Clone)]
pub(crate) struct Alloc {
    pub(crate) node_name: String,
    pub(crate) nr: i32,
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

    pub(crate) bandwidth_map: HashMap<(String, String), u32>,
    pub(crate) next_choice: RwLock<HashMap<String, u32>>,

    pub(crate) node_resource_map: Mutex<HashMap<String, NodeResource>>,
    pub(crate) prev_sched: RwLock<SchedHistory>,
}

impl Scheduler {
    pub async fn new(client: Client) -> Self {
        let node_resource_map = Mutex::new(HashMap::new());

        let sched = Scheduler {
            client,
            namespace: SPARK_NAMESPACE.to_string(),
            predicate: Arc::new(EnoughResourcePredicate::default()),
            priority: Arc::new(crate::predprio::NetworkAwarePriority::default()),
            bandwidth_map: hard_coded_network_bandwidth_map(),
            next_choice: RwLock::new(HashMap::new()),
            prev_sched: RwLock::new(HashMap::new()),
            node_resource_map,
        };

        sched.renew_resource_map().await;

        sched
    }

    pub async fn run(self) -> Result<()> {
        let (tx, mut rx) = unbounded_channel();
        let tx_c = tx.clone();

        // the thread that watches for new pods added event

        let sched = Arc::new(self);
        sched.clone().start_pod_watcher(tx);

        loop {
            println!("Waiting to schedule pod...");
            let pod = rx.recv().await.expect("the pod queue is closed");
            let sched = sched.clone();

            let tx_c = tx_c.clone();
            tokio::spawn(async move {
                let ok = sched.sched_pod(&pod).await;
                // if failed to schedule, put it back to the rx
                // if !ok {
                //     let tx = tx_c.clone();
                //     tx.send(pod).unwrap();
                // }
                println!("pod scheduled success??: {}", ok);
            });
        }
    }

    fn start_pod_watcher(self: Arc<Self>, tx: UnboundedSender<Pod>) {
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

        tokio::spawn(async move {
            let sched = self.clone();
            loop {
                sched.renew_resource_map_if_no_pod().await;
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            }
        });
    }

    /// schedule a pod, return true if the pod is scheduled successfully
    async fn sched_pod(&self, pod: &Pod) -> bool {
        let pod_name = pod.metadata.name.as_ref().expect("empty pod name");
        let pod_namespace = pod
            .metadata
            .namespace
            .as_ref()
            .expect("empty pod namespace");

        println!("found a pod to schedule: {}/{}", &pod_namespace, &pod_name);

        let node_name = self.eval_and_bind(&pod).await;
        if node_name.is_err() {
            println!("failed to schedule pod, err: {}", node_name.unwrap_err());
            return false;
        }
        let node_name = node_name.unwrap();

        let message = format!(
            "Placed pod [{}/{}] on {}\n",
            &pod_namespace, &pod_name, &node_name
        );
        println!("{}", &message.trim_end());

        let uuid = pod
            .clone()
            .metadata
            .labels
            .unwrap()
            .get("spark-uuid")
            .unwrap()
            .clone();

        self.update_sched_hist(uuid, node_name).await;

        // emit the event the the pod has been binded
        let emit_params = EmitParameters {
            pod: pod.clone(),
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
    async fn update_sched_hist(&self, uuid: String, node_name: String) {
        let mut hist = self.prev_sched.write().await;
        let alloc = hist.get_mut(&uuid);
        match alloc {
            // a seen spark job, update the alloc
            Some(alloc_hist) => {
                for alloc in alloc_hist.iter_mut() {
                    if alloc.node_name == node_name {
                        alloc.nr += 1;
                        println!("updated sched hist: {:?}", hist);
                        return;
                    }
                }
                // not returned, so it is a new node
                alloc_hist.push(Alloc { node_name, nr: 1 });
            }
            // an unseen spark job
            None => {
                hist.insert(uuid, vec![Alloc { node_name, nr: 1 }]);
            }
        };

        println!("updated sched hist: {:?}", hist);
    }

    async fn renew_resource_map_if_no_pod(&self) {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let pods = pods.list(&ListParams::default()).await.unwrap().items;
        if pods.is_empty() {
            self.renew_resource_map().await;
            self.prev_sched.write().await.clear();
            self.next_choice.write().await.clear();
            println!("Node resource map renewed!");
        }
    }

    async fn renew_resource_map(&self) {
        // Get a node list
        let node_api: Api<Node> = Api::all(self.client.clone());
        let nodes = node_api.list(&ListParams::default()).await.unwrap().items;
        let mut map = self.node_resource_map.lock().await;
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
            if &name == DEFAULT_MASTER_NODE_NAME {
                cpu -= 2;
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

            map.insert(
                name,
                NodeResource {
                    cpu: cpu - 1,
                    mem_kb: mem_kb - 5 * 1024 * 1024,
                },
            );
        }
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
        let mut choice = self.next_choice.write().await;
        let priorities = self.prioritize(
            &filtered_node_names,
            &node_resource_map,
            pod,
            &mut choice,
            &prev_sched,
        );
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
        node_resource_map: &HashMap<String, NodeResource>,
        pod: &Pod,
        choice: &mut HashMap<String, u32>,
        prev_sched: &SchedHistory,
    ) -> HashMap<String, u32> {
        self.priority.priority(
            node_names,
            node_resource_map,
            pod,
            &self.bandwidth_map.clone(),
            choice,
            prev_sched,
        )
    }

    fn find_best_node(&self, priorities: &HashMap<String, u32>) -> String {
        let mut max_p = 0;
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

pub(crate) fn hard_coded_network_bandwidth_map() -> HashMap<(String, String), u32> {
    let node1 = String::from("node1");
    let node2 = String::from("node02");
    let node3 = String::from("node03");
    let node4 = String::from("xyji");

    let b12 = 100;
    let b13 = 100;
    let b14 = 5;
    let b23 = 100;
    let b24 = 20;
    let b34 = 25;

    let mut map = HashMap::new();
    map.insert((node1.clone(), node2.clone()), b12);
    map.insert((node2.clone(), node1.clone()), b12);

    map.insert((node1.clone(), node3.clone()), b13);
    map.insert((node3.clone(), node1.clone()), b13);

    map.insert((node1.clone(), node4.clone()), b14);
    map.insert((node4.clone(), node1.clone()), b14);

    map.insert((node2.clone(), node3.clone()), b23);
    map.insert((node3.clone(), node2.clone()), b23);

    map.insert((node2.clone(), node4.clone()), b24);
    map.insert((node4.clone(), node2.clone()), b24);

    map.insert((node3.clone(), node4.clone()), b34);
    map.insert((node4.clone(), node3.clone()), b34);

    for n in [node1, node2, node3, node4] {
        map.insert((n.clone(), n.clone()), u32::MAX);
    }

    println!("bandwidth map: {:?}", map);

    map
}
