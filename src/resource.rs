use crate::cluster::ClusterState;

pub struct ResourcePlan {
    driver_cpu: u32,
    driver_mem_gb: u32,
    exec_cpu: u32,
    exec_mem_gb: u32,
    nexec: u32,
}

impl Default for ResourcePlan {
    fn default() -> Self {
        Self {
            driver_cpu: 1,
            driver_mem_gb: 1,
            exec_cpu: 2,
            exec_mem_gb: 2,
            nexec: 5,
        }
    }
}

/// Make a resource plan and update the state
/// the next call to this will base on the updated state
pub fn plan(state: &mut ClusterState) -> ResourcePlan {
    todo!()
}

impl ResourcePlan {
    pub fn driver_cpu(&self) -> String {
        self.driver_cpu.to_string()
    }

    pub fn driver_mem_gb(&self) -> String {
        format!("{}G", self.driver_mem_gb.to_string())
    }

    pub fn exec_cpu(&self) -> String {
        self.exec_cpu.to_string()
    }

    pub fn exec_mem_gb(&self) -> String {
        format!("{}G", self.exec_mem_gb.to_string())
    }

    pub fn nexec(&self) -> String {
        self.nexec.to_string()
    }
}
