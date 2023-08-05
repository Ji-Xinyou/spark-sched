use std::collections::HashMap;

use crate::{cluster::ClusterState, DEFAULT_DRIVER_CORE};

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

pub trait Planner {
    fn plan(
        state: &mut ClusterState,
        workload_types: &[WorkloadType],
        meta: Vec<String>,
    ) -> Vec<ResourcePlan>;
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
    fn plan(
        state: &mut ClusterState,
        workload_types: &[WorkloadType],
        _meta: Vec<String>,
    ) -> Vec<ResourcePlan> {
        let mut n_workload = workload_types.len() as u32;
        let mut plans = vec![];

        while n_workload > 0 {
            let core = state.total_core / n_workload;
            let mem_mb = state.total_mem_mb / n_workload;
            n_workload -= 1;

            let plan = ResourcePlan {
                driver_cpu: 1,
                driver_mem_mb: 1024,
                exec_cpu: 1,
                exec_mem_mb: 1024,
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
    fn plan(
        state: &mut ClusterState,
        workload_types: &[WorkloadType],
        _meta: Vec<String>,
    ) -> Vec<ResourcePlan> {
        println!(
            "Planning with WorkloadAwareFairPlanner, cluster state: {:#?}",
            &state
        );
        let mut plans = vec![ResourcePlan::default(); workload_types.len()];

        let n_workload = workload_types.len() as u32;
        let n_compute = workload_types
            .iter()
            .filter(|workload_type| **workload_type == WorkloadType::Compute)
            .count();
        let n_storage = workload_types
            .iter()
            .filter(|workload_type| **workload_type == WorkloadType::Storage)
            .count();

        let denom =
            COMPUTE_WORKLOAD_WEIGHT * n_compute as f64 + STORAGE_WORKLOAD_WEIGHT * n_storage as f64;
        let c = (COMPUTE_WORKLOAD_WEIGHT as f64) / denom;
        let s = (STORAGE_WORKLOAD_WEIGHT as f64) / denom;

        // generate plans for compute workloads and storage workloads
        let c_core = (c * state.total_core as f64).ceil() as u32;
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
                        exec_mem_mb: 1024,
                        nexec: c_core - 1,
                    };
                    state.total_core -= c_core;
                    state.total_mem_mb -= c_mem;
                    plans[i] = plan;
                }
                _ => {}
            };
        }

        let mut max_core = 0;
        let mut core_gap: HashMap<usize, u32> = HashMap::new();
        for (i, ty) in workload_types.iter().enumerate() {
            match ty {
                WorkloadType::Storage => {
                    let core = s_core;
                    let core = if core > state.total_core {
                        state.total_core
                    } else {
                        core
                    };
                    let mem = s_mem;
                    let mem = if mem > state.total_mem_mb {
                        state.total_mem_mb
                    } else {
                        mem
                    };

                    max_core = if core > max_core { core } else { max_core };
                    let gap = max_core - core;
                    if gap > 0 {
                        core_gap.insert(i, gap);
                    }

                    let plan = ResourcePlan {
                        driver_cpu: 1,
                        driver_mem_mb: 1024,
                        exec_cpu: 1,
                        exec_mem_mb: 1024,
                        nexec: core - 1,
                    };
                    state.total_core -= core;
                    state.total_mem_mb -= mem;
                    plans[i] = plan;
                }
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
                        }
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
                        }
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
    pub driver_cpu: u32,
    pub driver_mem_mb: u32,
    pub exec_cpu: u32,
    pub exec_mem_mb: u32,
    pub nexec: u32,
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

pub(crate) struct ProfiledPlanner;

impl Planner for ProfiledPlanner {
    fn plan(
        state: &mut ClusterState,
        workload_types: &[WorkloadType],
        meta: Vec<String>,
    ) -> Vec<ResourcePlan> {
        from_profiled(state, workload_types.to_vec(), meta)
    }
}

pub(crate) fn from_profiled(
    state: &mut ClusterState,
    _workload_types: Vec<WorkloadType>,
    meta: Vec<String>,
) -> Vec<ResourcePlan> {
    let mut plans = vec![ResourcePlan::default(); meta.len() as usize];
    let ncore = state.total_core as usize;
    let nworkload = meta.len();

    let (_, nexecs) = min_execution_time(
        &meta,
        &profiled_table(),
        ncore - nworkload * (DEFAULT_DRIVER_CORE as usize),
    );

    for (i, nexec) in nexecs.iter().enumerate() {
        let plan = ResourcePlan {
            driver_cpu: 1,
            driver_mem_mb: 1024,
            exec_cpu: 1,
            exec_mem_mb: 1024,
            nexec: *nexec,
        };
        plans[i] = plan;
    }

    plans
}

fn min_execution_time(
    workloads: &[String],
    execution_times: &HashMap<(String, u32), u64>,
    max_exec: usize,
) -> (u64, Vec<u32>) {
    let mut dp = vec![vec![u64::MAX; max_exec + 1]; workloads.len()];
    let mut decision = vec![vec![0; max_exec + 1]; workloads.len()];

    // handle the workload 0..i
    for i in 0..workloads.len() {
        let workload = &workloads[i];

        // total_cores
        for nexec in i + 1..=max_exec {
            dp[i][nexec] = u64::MAX;

            for workload_nexec in 1..=nexec {
                // gives this workload workload_nexec cores
                let time = execution_times
                    .get(&(workload.clone(), workload_nexec as u32))
                    .unwrap();

                // transition
                if i == 0 {
                    if *time < dp[i][nexec] {
                        dp[i][nexec] = *time;
                        decision[i][nexec] = workload_nexec as u32;
                    }
                    continue;
                }

                let new_time = u64::max(dp[i - 1][nexec - workload_nexec], *time);
                if new_time < dp[i][nexec] {
                    dp[i][nexec] = new_time;
                    decision[i][nexec] = workload_nexec as u32;
                }
            }
        }
    }

    let (optimal_total_nexec, min_time) = dp[workloads.len() - 1]
        .iter()
        .enumerate()
        .min_by_key(|&(_, &time)| time)
        .unwrap();

    let optimal_nexecs = reconstruct_nexecs(&decision, workloads.len(), optimal_total_nexec);

    (*min_time, optimal_nexecs)
}

fn reconstruct_nexecs(
    decision: &[Vec<u32>],
    num_workloads: usize,
    optimal_total_nexec: usize,
) -> Vec<u32> {
    let mut nexecs = vec![0; num_workloads];
    let mut remaining_exec = optimal_total_nexec;

    for i in (0..num_workloads).rev() {
        nexecs[i] = decision[i][remaining_exec];
        remaining_exec -= nexecs[i] as usize;
    }

    nexecs
}

// <WorkloadType, nexec> -> time
fn profiled_table() -> HashMap<(String, u32), u64> {
    let mut m = HashMap::default();
    m.insert(("wc".to_string(), 1), 82250);
    m.insert(("wc".to_string(), 2), 67000);
    m.insert(("wc".to_string(), 3), 66000);
    m.insert(("wc".to_string(), 4), 72500);
    m.insert(("wc".to_string(), 5), 67000);
    m.insert(("wc".to_string(), 6), 65500);
    m.insert(("wc".to_string(), 7), 65000);
    m.insert(("wc".to_string(), 8), 85000);
    m.insert(("wc".to_string(), 9), 66000);
    m.insert(("wc".to_string(), 10), 67000);
    m.insert(("wc".to_string(), 11), 71500);
    m.insert(("wc".to_string(), 12), 78000);
    m.insert(("wc".to_string(), 13), 78000);
    m.insert(("wc".to_string(), 14), 79500);
    m.insert(("wc".to_string(), 15), 94000);
    m.insert(("wc".to_string(), 16), 95000);
    m.insert(("wc".to_string(), 17), 97500);
    m.insert(("wc".to_string(), 18), 117000);
    m.insert(("wc".to_string(), 19), 115000);
    m.insert(("wc".to_string(), 20), 111000);
    m.insert(("wc".to_string(), 21), 130000);

    m.insert(("pi".to_string(), 1), 103000);
    m.insert(("pi".to_string(), 2), 67100);
    m.insert(("pi".to_string(), 3), 57500);
    m.insert(("pi".to_string(), 4), 55000);
    m.insert(("pi".to_string(), 5), 53000);
    m.insert(("pi".to_string(), 6), 54000);
    m.insert(("pi".to_string(), 7), 50000);
    m.insert(("pi".to_string(), 8), 50000);
    m.insert(("pi".to_string(), 9), 48000);
    m.insert(("pi".to_string(), 10), 46000);
    m.insert(("pi".to_string(), 11), 45500);
    m.insert(("pi".to_string(), 12), 46000);
    m.insert(("pi".to_string(), 13), 44500);
    m.insert(("pi".to_string(), 14), 44000);
    m.insert(("pi".to_string(), 15), 43000);
    m.insert(("pi".to_string(), 16), 42400);
    m.insert(("pi".to_string(), 17), 43250);
    m.insert(("pi".to_string(), 18), 43400);
    m.insert(("pi".to_string(), 19), 44600);
    m.insert(("pi".to_string(), 20), 43500);
    m.insert(("pi".to_string(), 21), 43400);

    m.insert(("svm".to_string(), 1), 71300);
    m.insert(("svm".to_string(), 2), 74800);
    m.insert(("svm".to_string(), 3), 80000);
    m.insert(("svm".to_string(), 4), 85000);
    m.insert(("svm".to_string(), 5), 87000);
    m.insert(("svm".to_string(), 6), 91500);
    m.insert(("svm".to_string(), 7), 95000);
    m.insert(("svm".to_string(), 8), 92000);
    m.insert(("svm".to_string(), 9), 95500);
    m.insert(("svm".to_string(), 10), 95000);
    m.insert(("svm".to_string(), 11), 96500);
    m.insert(("svm".to_string(), 12), 101000);
    m.insert(("svm".to_string(), 13), 102500);
    m.insert(("svm".to_string(), 14), 107000);
    m.insert(("svm".to_string(), 15), 140000);
    m.insert(("svm".to_string(), 16), 200000);
    m.insert(("svm".to_string(), 17), 220000);
    m.insert(("svm".to_string(), 18), 260000);
    m.insert(("svm".to_string(), 19), 310000);
    m.insert(("svm".to_string(), 20), 390000);
    m.insert(("svm".to_string(), 21), 420000);

    m.insert(("sort".to_string(), 1), 280000);
    m.insert(("sort".to_string(), 2), 162000);
    m.insert(("sort".to_string(), 3), 121500);
    m.insert(("sort".to_string(), 4), 128000);
    m.insert(("sort".to_string(), 5), 100000);
    m.insert(("sort".to_string(), 6), 100000);
    m.insert(("sort".to_string(), 7), 94000);
    m.insert(("sort".to_string(), 8), 105000);
    m.insert(("sort".to_string(), 9), 97000);
    m.insert(("sort".to_string(), 10), 103000);
    m.insert(("sort".to_string(), 11), 95000);
    m.insert(("sort".to_string(), 12), 89000);
    m.insert(("sort".to_string(), 13), 88000);
    m.insert(("sort".to_string(), 14), 94000);
    m.insert(("sort".to_string(), 15), 104000);
    m.insert(("sort".to_string(), 16), 115000);
    m.insert(("sort".to_string(), 17), 112000);
    m.insert(("sort".to_string(), 18), 129000);
    m.insert(("sort".to_string(), 19), 132000);
    m.insert(("sort".to_string(), 20), 140000);
    m.insert(("sort".to_string(), 21), 140000);

    m
}
