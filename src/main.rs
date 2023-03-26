mod cluster;
mod cmd;
mod resource;

use clap::Parser;
use cmd::PysparkSubmitBuilder;

use std::time::Instant;

use crate::{cluster::get_cluster_state, resource::ResourcePlan};

/// Notice, the cpu core, memory of driver and executor are not specified by the user
/// The program will calculate the correct resource(cpu, mem, nexec) to use for the user
///
/// Also, each workload will be assigned with a universally unique id for the spark-sched to identify
/// the spark-sched will schedule the pods of the wordload as close as possible
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// the number of workload to be run
    #[arg(long, default_value_t = 1)]
    n_workload: u32,

    /// the spark-submit path
    #[arg(long)]
    path: String,

    /// the master url
    #[arg(long)]
    master: String,

    /// the deploy mode of spark cluster
    #[arg(long, default_value_t = String::from("cluster"))]
    deploy_mode: String,

    /// the namespace of the spark cluster
    #[arg(long, default_value_t = String::from("spark"))]
    ns: String,

    /// the service account of the spark cluster
    #[arg(long, default_value_t = String::from("spark"))]
    service_account: String,

    /// the image repository of spark driver and executors
    #[arg(long)]
    image: String,

    /// the parallelism of the spark job
    #[arg(long)]
    parallelism: u32,

    /// the pvc name of the spark
    #[arg(long, default_value_t = String::from("spark-local-dir-1"))]
    pvc_name: String,

    /// the pvc name in the kubernetes cluster, which should be pre-created ahead of submission
    #[arg(long)]
    pvc_claim_name: String,

    /// the mount path of the pvc in the spark driver and executors
    #[arg(long, default_value_t = String::from("/mnt"))]
    pvc_mount_path: String,

    /// the program executable(or script) to run
    #[arg(long)]
    prog: String,

    /// the argument of the program
    #[arg(long)]
    args: Vec<String>,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut cmds = vec![];
    let mut state = get_cluster_state().await.unwrap();

    println!("Running {} workloads", args.n_workload);
    println!("Cluster state: {:#?}", state);

    for _ in 0..args.n_workload {
        // let plan = plan(&mut state);
        let plan = ResourcePlan::default();

        let driver_cpu = plan.driver_cpu();
        let driver_mem = plan.driver_mem_gb();
        let exec_cpu = plan.exec_cpu();
        let exec_mem = plan.exec_mem_gb();
        let nexec = plan.nexec();

        let driver_args = cmd::PySparkDriverParams {
            core: String::from(driver_cpu),
            memory: String::from(driver_mem),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let exec_args = cmd::PySparkExecutorParams {
            core: String::from(exec_cpu),
            memory: String::from(exec_mem),
            nr: String::from(nexec),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let cmd = PysparkSubmitBuilder::new()
            .path(args.path.clone())
            .master(args.master.clone())
            .deploy_mode(args.deploy_mode.clone())
            .ns(args.ns.clone())
            .service_account(args.service_account.clone())
            .image(args.image.clone())
            .parallelism(args.parallelism)
            .driver_args(driver_args)
            .exec_args(exec_args)
            .prog(args.prog.clone())
            .args(args.args.clone())
            .build()
            .into_command();

        cmds.push(cmd)
    }

    let mut childs = vec![];
    for mut cmd in cmds {
        childs.push(cmd.cmd.spawn().unwrap());
    }

    let elapsed_ms = measure(|| {
        for mut child in childs {
            child.wait().unwrap();
        }
    });

    println!("\nSubmitter exits, elapsed time: {} ms", elapsed_ms);
}

fn measure<F>(f: F) -> u128
where
    F: FnOnce(),
{
    let start_time = Instant::now();
    f();
    let end_time = Instant::now();
    (end_time - start_time).as_millis()
}
