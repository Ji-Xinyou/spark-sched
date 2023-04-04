use std::{collections::HashMap, error::Error};

use async_trait::async_trait;
use k8s_openapi::{
    api::core::v1::{Node, Pod},
    apimachinery::pkg::api::resource::Quantity,
};
use kube::{api::ListParams, Api, Client};

use crate::sched::PodResource;

const DEFAULT_UUID_KEY: &str = "spark-uuid";
const DEFAULT_WORKLOAD_TYPE_KEY: &str = "spark-workload-type";
const DEFAULT_COMPUTE_WORKLOAD: &str = "compute";

/// Gives filtered node_names
#[async_trait]
pub(crate) trait Predicate: Send + Sync {
    async fn judge(&self, client: &Client, pod_resource: PodResource) -> Vec<String>;
}

#[async_trait]
pub(crate) trait Priority: Send + Sync {
    async fn priority(
        &self,
        client: Client,
        node_name: &[String],
        pod: &Pod,
        bw_map: &HashMap<(String, String), u32>,
        choice: &mut HashMap<String, u32>,
    ) -> HashMap<String, u32>;
}

/// EnoughResourcePredicate filters the nodes that have enough resources to
/// schedule the pod.
#[derive(Debug, Default)]
pub(crate) struct EnoughResourcePredicate;

#[async_trait]
impl Predicate for EnoughResourcePredicate {
    async fn judge(&self, client: &Client, pod_resource: PodResource) -> Vec<String> {
        let mut node_names = vec![];
        let nodes: Api<Node> = Api::all(client.clone());
        let lp = ListParams::default();
        let node_list = nodes.list(&lp).await.expect("failed to list pods");

        println!("|pod {}| request milicores: {}, mem_kib: {}", pod_resource.name, pod_resource.millicore, pod_resource.mem_kb);
        for node in node_list {
            let node_name = node.metadata.name.unwrap();
            let (remaining_milicores, remaining_mem_ki) =
                get_remaining_resources(client.clone(), &node_name)
                    .await
                    .unwrap();

            println!(
                "|node {}| remaining milicores: {}, mem_kib: {}",
                &node_name, remaining_milicores, remaining_mem_ki
            );

            if remaining_milicores >= pod_resource.millicore
                && remaining_mem_ki >= pod_resource.mem_kb
            {
                node_names.push(node_name.to_string());
            }
        }
        println!("filtered: {:#?}\n", node_names);

        node_names
    }
}

#[derive(Debug, Default)]
pub(crate) struct NetworkAwarePriority;

#[derive(Debug, Default)]
pub(crate) struct WorkloadNetworkAwarePriority;

#[async_trait]
impl Priority for NetworkAwarePriority {
    async fn priority(
        &self,
        client: Client,
        node_name: &[String],
        pod: &Pod,
        bw_map: &HashMap<(String, String), u32>,
        choice: &mut HashMap<String, u32>,
    ) -> HashMap<String, u32> {
        let mut m = HashMap::new();
        for node in node_name {
            m.insert(node.to_string(), 0);
        }

        let nodes: Api<Node> = Api::all(client.clone());
        let lp = ListParams::default();
        let node_list = nodes.list(&lp).await.expect("failed to list pods");
        let nr_node = node_list.items.len();

        let uuid = get_pod_uuid(pod);

        let this_choice = choice.get(&uuid);
        let mut c = match this_choice {
            Some(c) => *c,
            None => 0,
        };

        // candidate bws
        let mut bws = vec![];
        for node in node_name {
            let bw_to_storage = bw_map.get(&(node.to_string(), "xyji".to_string())).unwrap();
            bws.push((node.to_string(), *bw_to_storage));
        }

        // find the lowest bw >= choice
        let mut all_bws = vec![];
        for node in node_list {
            let name = node.metadata.name.unwrap();
            let bw_to_storage = bw_map.get(&(name.clone(), "xyji".to_string())).unwrap();
            all_bws.push((name, *bw_to_storage));
        }
        all_bws.sort_by(|a, b| a.1.cmp(&b.1));

        let mut node_with_index = vec![];
        for bw in bws {
            // find the bw's index in all_bws
            let mut index = 0;
            for (i, all_bw) in all_bws.iter().enumerate() {
                if bw.0 == all_bw.0 {
                    index = i;
                    break;
                }
            }
            node_with_index.push((bw.0, index));
        }
        node_with_index.sort_by(|a, b| a.1.cmp(&b.1));

        // make decision based on sorted nodes, ascending
        let mut flag = false;
        for (i, node) in node_with_index.iter().enumerate() {
            if i >= c as usize {
                flag = true;
                m.insert(node.0.to_string(), 100);
                c = ((i + 1) % nr_node) as u32;
                break;
            }
        }
        // if not found chose the largest one
        if !flag {
            let node = node_with_index.last().unwrap();
            m.insert(node.0.to_string(), 100);
            c = ((node.1 + 1) % nr_node) as u32;
        }

        // update the choice
        let _choice = choice.get_mut(&uuid);
        match _choice {
            Some(ch) => *ch = c,
            None => {
                choice.insert(uuid, c);
            }
        };

        m
    }
}

#[async_trait]
impl Priority for WorkloadNetworkAwarePriority {
    async fn priority(
        &self,
        client: Client,
        node_name: &[String],
        pod: &Pod,
        bw_map: &HashMap<(String, String), u32>,
        choice: &mut HashMap<String, u32>,
    ) -> HashMap<String, u32> {
        let mut m = HashMap::new();
        for node in node_name {
            m.insert(node.to_string(), 0);
        }

        let nodes: Api<Node> = Api::all(client.clone());
        let lp = ListParams::default();
        let node_list = nodes.list(&lp).await.expect("failed to list pods");
        let nr_node = node_list.items.len();

        let uuid = get_pod_uuid(pod);
        let workload_type = get_pod_workload_type(pod);

        let this_choice = choice.get(&uuid);
        let mut c = match this_choice {
            Some(c) => *c,
            None => 0,
        };

        // candidate bws
        let mut bws = vec![];
        for node in node_name {
            let bw_to_storage = bw_map.get(&(node.to_string(), "xyji".to_string())).unwrap();
            bws.push((node.to_string(), *bw_to_storage));
        }

        // find the lowest bw >= choice
        let mut all_bws = vec![];
        for node in node_list {
            let name = node.metadata.name.unwrap();
            let bw_to_storage = bw_map.get(&(name.clone(), "xyji".to_string())).unwrap();
            all_bws.push((name, *bw_to_storage));
        }
        all_bws.sort_by(|a, b| a.1.cmp(&b.1));

        if workload_type == DEFAULT_COMPUTE_WORKLOAD {
            for bw in &all_bws {
                if node_name.contains(&bw.0) {
                    println!("a compute workload, pod: {:?} bounded to node: {}", pod.metadata.name, bw.0);
                    m.insert(bw.0.clone(), 100);
                    return m;
                }
            }
        }

        let mut node_with_index = vec![];
        for bw in bws {
            // find the bw's index in all_bws
            let mut index = 0;
            for (i, all_bw) in all_bws.iter().enumerate() {
                if bw.0 == all_bw.0 {
                    index = i;
                    break;
                }
            }
            node_with_index.push((bw.0, index));
        }
        node_with_index.sort_by(|a, b| a.1.cmp(&b.1));

        // make decision based on sorted nodes, ascending
        let mut flag = false;
        for (i, node) in node_with_index.iter().enumerate() {
            if i >= c as usize {
                flag = true;
                m.insert(node.0.to_string(), 100);
                c = ((i + 1) % nr_node) as u32;
                break;
            }
        }
        // if not found chose the largest one
        if !flag {
            let node = node_with_index.last().unwrap();
            m.insert(node.0.to_string(), 100);
            c = ((node.1 + 1) % nr_node) as u32;
        }

        // update the choice
        let _choice = choice.get_mut(&uuid);
        match _choice {
            Some(ch) => *ch = c,
            None => {
                choice.insert(uuid, c);
            }
        };

        m
    }
}


fn get_pod_workload_type(pod: &Pod) -> String {
    pod
        .clone()
        .metadata
        .labels
        .unwrap()
        .get(DEFAULT_WORKLOAD_TYPE_KEY)
        .unwrap()
        .clone()
}

fn get_pod_uuid(pod: &Pod) -> String {
    pod
        .clone()
        .metadata
        .labels
        .unwrap()
        .get(DEFAULT_UUID_KEY)
        .unwrap()
        .clone()
}

async fn get_remaining_resources(
    client: Client,
    node_name: &str,
) -> Result<(u64, u64), Box<dyn Error>> {
    let (cpu_allocatable_millicores, memory_allocatable_ki) =
        get_allocatable_resources(client.clone(), node_name).await?;
    let (cpu_allocated, memory_allocated_ki) =
        get_allocated_resources(client.clone(), node_name).await?;
    Ok((
        cpu_allocatable_millicores.saturating_sub(cpu_allocated),
        memory_allocatable_ki.saturating_sub(memory_allocated_ki),
    ))
}

async fn get_allocatable_resources(
    client: Client,
    node_name: &str,
) -> Result<(u64, u64), Box<dyn Error>> {
    let node_api: Api<Node> = Api::all(client.clone());
    let node = node_api.get(node_name).await.expect("failed to get node");
    let allocatable = node.status.as_ref().unwrap().allocatable.as_ref().unwrap();
    let cpu_allocatable = allocatable["cpu"].clone();
    let memory_allocatable = allocatable["memory"].clone();

    let cpu_allocatable_millicores = quantity_to_millicores(cpu_allocatable).unwrap();
    let memory_allocatable_ki = quantity_to_kibytes(memory_allocatable).unwrap();

    Ok((cpu_allocatable_millicores, memory_allocatable_ki))
}

async fn get_allocated_resources(
    client: Client,
    node_name: &str,
) -> Result<(u64, u64), Box<dyn Error>> {
    let pods: Api<Pod> = Api::all(client);
    let lp = ListParams::default();
    let pod_list = pods.list(&lp).await?;

    let mut cpu_allocated_millicores = 0;
    let mut memory_allocated_kibytes = 0;

    for pod in pod_list.into_iter() {
        if pod
            .spec
            .as_ref()
            .unwrap()
            .node_name
            .as_ref()
            .unwrap_or(&String::new())
            == node_name
        {
            let containers = &pod.spec.as_ref().unwrap().containers;
            for container in containers {
                if let Some(resources) = container.resources.as_ref() {
                    if let Some(requests) = resources.requests.as_ref() {
                        if let Some(cpu) = requests.get("cpu") {
                            cpu_allocated_millicores += quantity_to_millicores(cpu.clone())?;
                        }
                        if let Some(memory) = requests.get("memory") {
                            memory_allocated_kibytes += quantity_to_kibytes(memory.clone())?;
                        }
                    }
                }
            }
        }
    }

    Ok((cpu_allocated_millicores, memory_allocated_kibytes))
}

pub fn quantity_to_millicores(q: Quantity) -> Result<u64, Box<dyn Error>> {
    let s = q.0.to_string();
    if s.ends_with("m") {
        let val = s.trim_end_matches('m').parse::<u64>()?;
        Ok(val)
    } else {
        let val = s.parse::<u64>()?;
        Ok(val * 1000)
    }
}

pub fn quantity_to_kibytes(q: Quantity) -> Result<u64, Box<dyn Error>> {
    let s = q.0.to_string();
    if s.ends_with("Ki") {
        let val = s.trim_end_matches("Ki").parse::<u64>()?;
        Ok(val)
    } else if s.ends_with("Mi") {
        let val = s.trim_end_matches("Mi").parse::<u64>()?;
        Ok(val * 1024)
    } else if s.ends_with("Gi") {
        let val = s.trim_end_matches("Gi").parse::<u64>()?;
        Ok(val * 1024 * 1024)
    } else {
        Err("Unsupported memory unit".into())
    }
}
