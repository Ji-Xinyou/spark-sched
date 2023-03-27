use crate::cluster::ClusterState;

pub trait Planner {
    fn plan(state: &mut ClusterState, n_workload: &mut u32) -> ResourcePlan;
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
pub struct DefaultPlanner;

/// estimately the master node uses 2 cpus and 2GB of memory
/// when we schedule, we need to take that into account
impl Planner for FairPlanner {
    fn plan(state: &mut ClusterState, n_workload: &mut u32) -> ResourcePlan {
        let core = state.total_core / *n_workload;
        let mem_mb = state.total_mem_mb / *n_workload;
        *n_workload -= 1;

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

        plan
    }
}

impl Planner for DefaultPlanner {
    fn plan(_state: &mut ClusterState, _n_workload: &mut u32) -> ResourcePlan {
        return ResourcePlan::default();
    }
}

#[derive(Debug)]
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
