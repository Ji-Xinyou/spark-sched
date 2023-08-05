mod cluster;
mod cmd;
mod resource;

use awaitgroup::WaitGroup;
use clap::Parser;
use cluster::ClusterState;
use cmd::PysparkSubmitBuilder;

use std::time::Instant;

use crate::cluster::get_cluster_state;
use crate::resource::{
    FairPlanner, Planner, ProfiledPlanner, ResourcePlan, WorkloadAwareFairPlanner,
};

const DEFAULT_DRIVER_CORE: u32 = 1;

/// Notice, the cpu core, memory of driver and executor are not specified by the user
/// The program will calculate the correct resource(cpu, mem, nexec) to use for the user
///
/// !
/// ! Also, each workload will be assigned with a universally unique id(UUID) with key "spark-uuid"
/// ! for the spark-sched to identify, the spark-sched will schedule the pods of the wordload as close as possible
/// ! The pods deployed here for each load are symmetrical, if some of the pods are deployed on the storage
/// ! node, they should use more cpu cores on that node
/// !
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
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

    /// the pvc name of the spark
    #[arg(long, default_value_t = String::from("spark-local-dir-1"))]
    pvc_name: String,

    /// the pvc name in the kubernetes cluster, which should be pre-created ahead of submission
    #[arg(long)]
    pvc_claim_name: String,

    /// the mount path of the pvc in the spark driver and executors
    #[arg(long, default_value_t = String::from("/mnt"))]
    pvc_mount_path: String,

    /// tags, which will be used to identify the workload, it HAS TO BE
    /// IN THE SAME ORDER as the progs
    #[arg(long, value_parser, num_args = 1..,)]
    tags: Vec<String>,

    /// the programs executable(or script) to run with its argument
    #[arg(long, value_parser, num_args = 1..,)]
    progs: Vec<String>,

    #[arg(long, value_parser, num_args = 1..,)]
    meta: Vec<String>,

    /// whether to show log in the stdio
    #[arg(long, default_value_t = false)]
    show_log: bool,

    /// which planner to use, (default, fair)
    #[arg(long, default_value_t = String::from("default"))]
    planner: String,

    #[arg(long, default_value_t = String::from(""))]
    scheduler_name: String,

    /// if set, the command will not run, this is for debugging
    #[arg(long, default_value_t = false)]
    no_run: bool,

    #[arg(long, default_value_t = false)]
    no_exit: bool,

    #[arg(long, default_value_t = false)]
    debug: bool,

    /// whether is for profiling
    #[arg(long, default_value_t = false)]
    profile: bool,

    #[arg(long, default_value_t = 1)]
    profile_start: u32,

    #[arg(long, default_value_t = false)]
    time: bool,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if args.profile {
        println!("profiling");
        profile(args).await;
        return;
    }

    if args.time {
        let start_time = Instant::now();
        sched(args).await;
        let end_time = Instant::now();
        let e = (end_time - start_time).as_millis();
        println!("elapsed time: {} ms", e);
    } else {
        sched(args).await;
    }
}

async fn sched(args: Args) {
    let mut cmds = vec![];

    let n_workload = args.progs.len() as u32;
    let mut state = get_cluster_state().await.unwrap();

    // has to be the same
    assert_eq!(n_workload, args.tags.len() as u32);

    println!("\nRunning {} workloads", n_workload);
    println!("Using {} planner", args.planner);
    let plannerfunc = match args.planner.as_str() {
        "fair" => FairPlanner::plan,
        "workload" => WorkloadAwareFairPlanner::plan,
        "profile" => ProfiledPlanner::plan,
        _ => panic!("Unknown planner: {}", args.planner),
    };

    let workload_types = args
        .tags
        .iter()
        .map(|t| match t.as_str() {
            "compute" => resource::WorkloadType::Compute,
            "storage" => resource::WorkloadType::Storage,
            _ => panic!("Unknown workload type: {}", t),
        })
        .collect::<Vec<resource::WorkloadType>>();

    let workload_types = if workload_types.is_empty() {
        vec![resource::WorkloadType::Compute; n_workload as usize]
    } else {
        workload_types
    };

    let plans = plannerfunc(&mut state, &workload_types, args.meta);

    for (i, prog) in args.progs.iter().enumerate() {
        let plan = plans[i];
        if args.debug {
            println!(
                "For the {}-th workload, typed: {:?}, emitting plan: {:#?}",
                i, args.tags[i], &plan
            );
        }

        let driver_cpu = plan.driver_cpu();
        let driver_mem = plan.driver_mem_mb();
        let exec_cpu = plan.exec_cpu();
        let exec_mem = plan.exec_mem_mb();
        let nexec = plan.nexec();

        let driver_args = cmd::PySparkDriverParams {
            core: String::from(&driver_cpu),
            memory: String::from(&driver_mem),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let exec_args = cmd::PySparkExecutorParams {
            core: String::from(&exec_cpu),
            memory: String::from(&exec_mem),
            nr: String::from(&nexec),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let parallelism = parallelism_func(driver_cpu, exec_cpu, nexec);
        let mut cmd = PysparkSubmitBuilder::new()
            .path(args.path.clone())
            .master(args.master.clone())
            .deploy_mode(args.deploy_mode.clone())
            .ns(args.ns.clone())
            .service_account(args.service_account.clone())
            .image(args.image.clone())
            .parallelism(parallelism)
            .scheduler(args.scheduler_name.clone())
            .driver_args(driver_args)
            .exec_args(exec_args)
            .workload_type(workload_types[i].to_string())
            .prog(prog.clone())
            .build()
            .into_command();

        if !args.show_log {
            cmd.cmd.stdout(std::process::Stdio::null());
            cmd.cmd.stderr(std::process::Stdio::null());
        }

        cmds.push(cmd)
    }

    if args.no_run {
        println!("no_run is set, exiting");
        return;
    }

    let mut childs = vec![];
    for (i, cmd) in cmds.iter_mut().enumerate() {
        if workload_types[i] == resource::WorkloadType::Compute {
            if args.debug {
                println!("Spawning one compute workload");
            }
            childs.push(cmd.cmd.spawn().unwrap());
        }
    }

    for (i, cmd) in cmds.iter_mut().enumerate() {
        if workload_types[i] == resource::WorkloadType::Storage {
            if args.debug {
                println!("Spawning one storage workload");
            }
            childs.push(cmd.cmd.spawn().unwrap());
        }
    }

    let mut wg = WaitGroup::new();
    for mut child in childs {
        let worker = wg.worker();
        tokio::spawn(async move {
            measure(|| {
                child.wait().unwrap();
            });
            worker.done();
        });
    }
    wg.wait().await;

    if !args.no_exit {
        cleanup();
    }
}

async fn profile(args: Args) {
    let n_workload = args.progs.len() as u32;
    let state = get_cluster_state().await.unwrap();

    // has to be the same
    assert_eq!(n_workload, args.tags.len() as u32);

    println!("\nRunning {} workloads", n_workload);

    let workload_types = args
        .tags
        .iter()
        .map(|t| match t.as_str() {
            "compute" => resource::WorkloadType::Compute,
            "storage" => resource::WorkloadType::Storage,
            _ => panic!("Unknown workload type: {}", t),
        })
        .collect::<Vec<resource::WorkloadType>>();

    let workload_type = workload_types.get(0).unwrap();

    let prog = args.progs.get(0).unwrap();
    // run under nexec from 1 to ncpu
    for nexec in args.profile_start..=(state.total_core - DEFAULT_DRIVER_CORE) {
        println!("running nexec {}", nexec);
        let plan = ResourcePlan {
            driver_cpu: DEFAULT_DRIVER_CORE,
            driver_mem_mb: 1024,
            exec_cpu: 1,
            exec_mem_mb: 1024,
            nexec,
        };

        let driver_cpu = plan.driver_cpu();
        let driver_mem = plan.driver_mem_mb();
        let exec_cpu = plan.exec_cpu();
        let exec_mem = plan.exec_mem_mb();
        let nexec = plan.nexec();

        let driver_args = cmd::PySparkDriverParams {
            core: String::from(&driver_cpu),
            memory: String::from(&driver_mem),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let exec_args = cmd::PySparkExecutorParams {
            core: String::from(&exec_cpu),
            memory: String::from(&exec_mem),
            nr: String::from(&nexec),
            pvc: cmd::PvcParams {
                name: args.pvc_name.clone(),
                claim_name: args.pvc_claim_name.clone(),
                mount_path: args.pvc_mount_path.clone(),
            },
        };

        let parallelism = parallelism_func(driver_cpu, exec_cpu, nexec);
        let mut cmd = PysparkSubmitBuilder::new()
            .path(args.path.clone())
            .master(args.master.clone())
            .deploy_mode(args.deploy_mode.clone())
            .ns(args.ns.clone())
            .service_account(args.service_account.clone())
            .image(args.image.clone())
            .parallelism(parallelism)
            .scheduler(args.scheduler_name.clone())
            .driver_args(driver_args)
            .exec_args(exec_args)
            .workload_type(workload_type.to_string())
            .prog(prog.clone())
            .build()
            .into_command();

        if !args.show_log {
            cmd.cmd.stdout(std::process::Stdio::null());
            cmd.cmd.stderr(std::process::Stdio::null());
        }

        let mut wg = WaitGroup::new();

        let worker = wg.worker();
        tokio::spawn(async move {
            measure(|| {
                cmd.cmd.spawn().unwrap().wait().unwrap();
            });
            worker.done();
        });

        wg.wait().await;

        cleanup();
    }
}

fn cleanup() {
    println!("cleaning up");
    // cleanup
    std::process::Command::new("kubectl")
        .arg("delete")
        .arg("pods")
        .arg("--all")
        .arg("-n")
        .arg("spark")
        .output()
        .expect("Failed to execute command");
}

fn measure<F>(f: F)
where
    F: FnOnce(),
{
    let start_time = Instant::now();
    f();
    let end_time = Instant::now();

    let e = (end_time - start_time).as_millis();
    println!("One workload exits, elapsed time: {} ms", e);
}

fn measure_no_stdout<F>(f: F)
where
    F: FnOnce(),
{
    let start_time = Instant::now();
    f();
    let end_time = Instant::now();

    let e = (end_time - start_time).as_millis();
    println!("elapsed time: {} ms", e);
}

fn parallelism_func(driver_cpu: String, exec_cpu: String, nexec: String) -> u32 {
    let dcore = driver_cpu.parse::<u32>().unwrap();
    let ecore = exec_cpu.parse::<u32>().unwrap();
    let nexec = nexec.parse::<u32>().unwrap();
    let total_core = dcore + ecore * nexec;
    5 * total_core
}
