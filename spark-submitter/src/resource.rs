use std::collections::HashMap;

use crate::cluster::ClusterState;

const COMPUTE_WORKLOAD_WEIGHT: f64 = 0.3;
const STORAGE_WORKLOAD_WEIGHT: f64 = 0.7;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WorkloadType {
    /// the workload mainly uses cpu, use bandwidth less
    Compute,
    /// the workload mainly uses bandwidth, use less cpu, often takes more time
    Storage,
}

impl WorkloadType {
    pub fn to_string(&self) -> String {
        match self {
            WorkloadType::Compute => "compute".to_string(),
            WorkloadType::Storage => "storage".to_string(),
        }
    }
}

fn workload_type_to_weight(workload_type: &WorkloadType) -> f64 {
    match workload_type {
        WorkloadType::Compute => COMPUTE_WORKLOAD_WEIGHT,
        WorkloadType::Storage => STORAGE_WORKLOAD_WEIGHT,
    }
}

pub trait Planner {
    fn plan(state: &mut ClusterState, workload_types: &[WorkloadType]) -> Vec<ResourcePlan>;
}

/// Fair Planner is a planner that treats all workload the same
/// For example, consider the case below
///     node1:         CPU core = 8, Memory = 8G
///     node2(master): CPU core = 8, Memory = 8G
///     node3        : CPU core = 8, Memory = 8G
/// And I have 4 workloads to be scheduled.
/// Since the master node uses two cpu cores and two gigs of memory,
/// we can say we have 22 cores of cpu and 22 gigs of memory.
///
/// A FairPlanner will schedule each workload with (5, 5, 6, 6) cpus
/// the FairPlanner tends to maximize the parallelism of the pods,
/// hence it will normally schedule the workload with the most nexec
pub struct FairPlanner;
pub struct WorkloadAwareFairPlanner;

/// estimately the master node uses 2 cpus and 2GB of memory
/// when we schedule, we need to take that into account
impl Planner for FairPlanner {
    fn plan(state: &mut ClusterState, workload_types: &[WorkloadType]) -> Vec<ResourcePlan> {
        let mut n_workload = workload_types.len() as u32;
        let mut plans = vec![];

        while n_workload > 0 {
            let core = state.total_core / n_workload;
            let mem_mb = state.total_mem_mb / n_workload;
            n_workload -= 1;

            println!("core: {}, mem_mb: {}", core, mem_mb);

            let plan = ResourcePlan {
                driver_cpu: 1,
                driver_mem_mb: 1024,
                exec_cpu: 1,
                exec_mem_mb: (mem_mb - 1024) / (core - 1),
                nexec: core - 1,
            };

            state.total_core -= core;
            state.total_mem_mb -= mem_mb;

            plans.push(plan);
        }

        plans
    }
}

impl Planner for WorkloadAwareFairPlanner {
    fn plan(state: &mut ClusterState, workload_types: &[WorkloadType]) -> Vec<ResourcePlan> {
        println!("Planning with WorkloadAwareFairPlanner, cluster state: {:#?}", &state);
        let mut plans = vec![ResourcePlan::default(); workload_types.len()];

        let n_workload = workload_types.len() as u32;
        let n_compute= workload_types
            .iter()
            .filter(|workload_type| **workload_type == WorkloadType::Compute)
            .count();
        let n_storage= workload_types
            .iter()
            .filter(|workload_type| **workload_type == WorkloadType::Storage)
            .count();

        let denom = COMPUTE_WORKLOAD_WEIGHT * n_compute as f64 + STORAGE_WORKLOAD_WEIGHT * n_storage as f64;
        let c = (COMPUTE_WORKLOAD_WEIGHT as f64) / denom;
        let s = (STORAGE_WORKLOAD_WEIGHT as f64) / denom;

        // generate plans for compute workloads and storage workloads
        let c_core= (c * state.total_core as f64).ceil() as u32;
        let c_core = if c_core > 2 { c_core } else { 2 };

        let c_mem = (c * state.total_mem_mb as f64).ceil() as u32;
        let c_mem = if c_mem > 2048 { c_mem } else { 2048 };
            
        let s_core = (s * state.total_core as f64).ceil() as u32;
        let s_core = if s_core > 2 { s_core } else { 2 };
        let s_mem = (s * state.total_mem_mb as f64).ceil() as u32;
        let s_mem = if s_mem > 2048 { s_mem } else { 2048 };

        for (i, ty) in workload_types.iter().enumerate() {
            match ty {
                WorkloadType::Compute => {
                    let plan = ResourcePlan {
                        driver_cpu: 1,
                        driver_mem_mb: 1024,
                        exec_cpu: 1,
                        exec_mem_mb: (c_mem - 1024) / (c_core - 1),
                        nexec: c_core - 1,
                    };
                    state.total_core -= c_core;
                    state.total_mem_mb -= c_mem;
                    plans[i] = plan;
                },
                _ => {}
           };
        }

        let mut max_core = 0;
        let mut core_gap: HashMap<usize, u32> = HashMap::new();
        for (i, ty) in workload_types.iter().enumerate() {
            match ty {
                WorkloadType::Storage => {
                    let core = s_core;
                    let core = if core > state.total_core { state.total_core } else { core };
                    let mem = s_mem;
                    let mem = if mem > state.total_mem_mb { state.total_mem_mb } else { mem };

                    max_core = if core > max_core { core } else { max_core };
                    let gap = max_core - core;
                    if gap > 0 {
                        core_gap.insert(i, gap);
                    }

                    let plan = ResourcePlan {
                        driver_cpu: 1,
                        driver_mem_mb: 1024,
                        exec_cpu: 1,
                        exec_mem_mb: (mem - 1024) / (core - 1),
                        nexec: core - 1,
                    };
                    state.total_core -= core;
                    state.total_mem_mb -= mem;
                    plans[i] = plan;
                },
                _ => {}
           };
        }

        // rebalance by stealing from compute workloads
        let mut ptr = 0;
        for (idx, gap) in core_gap.iter_mut() {
            while *gap > 0 {
                // if no workload could be stolen from, break
                let mut stole = false;
                for i in 0..n_workload {
                    match workload_types[i as usize] {
                        WorkloadType::Compute => {
                            if plans[i as usize].nexec > 1 {
                                stole = true;
                            }
                        },
                        _ => {}
                    };
                }
                if !stole {
                    break;
                }

                for (i, ty) in workload_types.iter().enumerate() {
                    // start from previous point
                    if i != ptr {
                        continue;
                    }

                    // circularly iterate through the workload types
                    ptr = if ptr == workload_types.len() - 1 {
                        0
                    } else {
                        ptr + 1
                    };

                    match ty {
                        WorkloadType::Compute => {
                            if plans[i].nexec > 1 {
                                plans[i].nexec -= 1;
                                plans[*idx].nexec += 1;

                                *gap -= 1;
                                if *gap == 0 {
                                    break;
                                }
                            }
                        },
                        _ => {}
                    };
                }
            }
        }

        plans
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ResourcePlan {
    driver_cpu: u32,
    driver_mem_mb: u32,
    exec_cpu: u32,
    exec_mem_mb: u32,
    nexec: u32,
}

impl Default for ResourcePlan {
    fn default() -> Self {
        Self {
            driver_cpu: 1,
            driver_mem_mb: 1024,
            exec_cpu: 2,
            exec_mem_mb: 2048,
            nexec: 4,
        }
    }
}

impl ResourcePlan {
    pub fn driver_cpu(&self) -> String {
        self.driver_cpu.to_string()
    }

    pub fn driver_mem_mb(&self) -> String {
        format!("{}m", self.driver_mem_mb.to_string())
    }

    pub fn exec_cpu(&self) -> String {
        self.exec_cpu.to_string()
    }

    pub fn exec_mem_mb(&self) -> String {
        format!("{}m", self.exec_mem_mb.to_string())
    }

    pub fn nexec(&self) -> String {
        self.nexec.to_string()
    }
}
