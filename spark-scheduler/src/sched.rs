use anyhow::{anyhow, Result};
use futures::TryStreamExt;
use k8s_openapi::api::core::v1::Pod;
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
use crate::predprio::{
    quantity_to_kibytes, quantity_to_millicores, EnoughResourcePredicate, Predicate, Priority, get_pod_uuid,
};

const SCHEDULER_NAME: &str = "spark-sched";
const SPARK_NAMESPACE: &str = "spark";

pub(crate) struct Scheduler {
    pub(crate) client: Client,
    pub(crate) namespace: String,

    pub(crate) predicate: Arc<dyn Predicate>,
    pub(crate) priority: Arc<dyn Priority>,

    pub(crate) bandwidth_map: HashMap<(String, String), u32>,
    pub(crate) next_choice: RwLock<HashMap<String, u32>>,
    pub(crate) sched_hist: RwLock<HashMap<String, Vec<String>>>,
}

impl Scheduler {
    pub async fn new(client: Client) -> Self {

        let sched = Scheduler {
            client,
            namespace: SPARK_NAMESPACE.to_string(),
            predicate: Arc::new(EnoughResourcePredicate::default()),
            priority: Arc::new(crate::predprio::WorkloadNetworkAwarePriority::default()),
            bandwidth_map: hard_coded_network_bandwidth_map(),
            next_choice: RwLock::new(HashMap::new()),
            sched_hist: RwLock::new(HashMap::new()),
        };

        sched
    }

    pub async fn run(self) -> Result<()> {
        let (tx, mut rx) = unbounded_channel();
        let tx_c = tx.clone();

        // the thread that watches for new pods added event

        let sched = Arc::new(self);
        sched.clone().start_pod_watcher(tx);

        loop {
            println!("\nWaiting to schedule pod...");
            let pod = rx.recv().await.expect("the pod queue is closed");
            let sched = sched.clone();

            let ok = sched.sched_pod(&pod).await;
            println!("pod scheduled success??: {}\n", ok);

            let sched_hist = sched.sched_hist.read().await;
            println!("sched hist: {:#?}", sched_hist);

            if !ok {
                tx_c.send(pod).unwrap();
            }
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
                sched.renew_if_no_pod().await;
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

        let uuid = get_pod_uuid(pod);
        self.sched_hist
            .write()
            .await
            .entry(uuid)
            .or_insert_with(Vec::new)
            .push(node_name.clone());

        let message = format!(
            "Placed pod [{}/{}] on {}\n",
            &pod_namespace, &pod_name, &node_name
        );
        println!("{}", &message.trim_end());

        let _uuid = pod
            .clone()
            .metadata
            .labels
            .unwrap()
            .get("spark-uuid")
            .unwrap()
            .clone();

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
    async fn renew_if_no_pod(&self) {
        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.namespace);
        let pods = pods.list(&ListParams::default()).await.unwrap().items;
        if pods.is_empty() {
            self.next_choice.write().await.clear();
        }
    }

    async fn eval_and_bind(&self, pod: &Pod) -> Result<String> {
        let pod_resource = pod_resource(pod);
        let filtered_node_names = self.predicate.judge(&self.client, pod_resource).await;

        if filtered_node_names.is_empty() {
            return Err(anyhow!(format!(
                "failed to find node that fits pod {}/{}",
                pod.metadata.namespace.as_ref().unwrap(),
                pod.metadata.name.as_ref().unwrap()
            )));
        }

        let mut choice = self.next_choice.write().await;
        let priorities = self
            .prioritize(&filtered_node_names, pod, &mut choice)
            .await;
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

        if let Err(e) = bind_result {
            println!(
                "failed to bind pod {}/{} to node {}: {}",
                &pod_namespace, &pod_name, &best_node, e
            );
        }

        Ok(best_node)
    }

    async fn prioritize(
        &self,
        node_names: &[String],
        pod: &Pod,
        choice: &mut HashMap<String, u32>,
    ) -> HashMap<String, u32> {
        self.priority
            .priority(
                self.client.clone(),
                node_names,
                pod,
                choice,
            )
            .await
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

#[derive(Debug, Clone, Default)]
pub(crate) struct PodResource {
    pub(crate) name: String,
    pub(crate) millicore: u64,
    pub(crate) mem_kb: u64,
}

pub(crate) fn pod_resource(pod: &Pod) -> PodResource {
    let name = pod.metadata.name.as_ref().unwrap().clone();
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

    let cpu = pod_req.get("cpu").unwrap();
    let mem_kb = pod_req.get("memory").unwrap();

    let millicore = quantity_to_millicores(cpu.clone()).unwrap();
    let mem_kb = quantity_to_kibytes(mem_kb.clone()).unwrap();

    PodResource { name, millicore, mem_kb }
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
