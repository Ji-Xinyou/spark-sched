use std::process::Command;

const DEFAULT_DEPLOY_MODE: &str = "cluster";
const DEFAULT_NS: &str = "spark";
const DEFAULT_SERVICE_ACCOUNT: &str = "spark";

#[derive(Debug, Default)]
pub struct PysparkSubmitBuilder {
    /// The spark-submit path
    path: Option<String>,
    /// The master url
    master: Option<String>,
    /// The deploy mode of spark cluster
    deploy_mode: Option<String>,
    /// The namespace of the spark cluster
    ns: Option<String>,
    /// The service account of the spark cluster
    service_account: Option<String>,
    /// The image repository of spark driver and executors
    image: Option<String>,
    /// The parallelism of the spark job
    parallelism: Option<u32>,
    /// The parameters of spark driver
    driver_args: Option<PySparkDriverParams>,
    /// The parameters of spark executor
    exec_args: Option<PySparkExecutorParams>,
    /// The program executable(or script) to run
    prog: Option<String>,
    /// The argument of the program
    args: Option<Vec<String>>,
}

impl PysparkSubmitBuilder {
    pub fn new() -> Self {
        Self {
            path: None,
            master: None,
            deploy_mode: None,
            ns: None,
            service_account: None,
            image: None,
            parallelism: None,
            driver_args: None,
            exec_args: None,
            prog: None,
            args: None,
        }
    }

    pub fn path(mut self, path: String) -> Self {
        self.path = Some(path);
        self
    }

    pub fn master(mut self, master: String) -> Self {
        self.master = Some(master);
        self
    }

    pub fn deploy_mode(mut self, deploy_mode: String) -> Self {
        self.deploy_mode = Some(deploy_mode);
        self
    }

    pub fn ns(mut self, ns: String) -> Self {
        self.ns = Some(ns);
        self
    }

    pub fn service_account(mut self, service_account: String) -> Self {
        self.service_account = Some(service_account);
        self
    }

    pub fn image(mut self, image: String) -> Self {
        self.image = Some(image);
        self
    }

    pub fn parallelism(mut self, parallelism: u32) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    pub fn driver_args(mut self, driver_args: PySparkDriverParams) -> Self {
        self.driver_args = Some(driver_args);
        self
    }

    pub fn exec_args(mut self, exec_args: PySparkExecutorParams) -> Self {
        self.exec_args = Some(exec_args);
        self
    }

    pub fn prog(mut self, prog: String) -> Self {
        self.prog = Some(prog);
        self
    }

    pub fn args(mut self, args: Vec<String>) -> Self {
        self.args = Some(args);
        self
    }

    pub fn build(self) -> PySparkSubmit {
        PySparkSubmit {
            path: self.path.unwrap_or_default(),
            master: self.master.unwrap_or_default(),
            deploy_mode: self
                .deploy_mode
                .unwrap_or_else(|| DEFAULT_DEPLOY_MODE.to_string()),
            ns: self.ns.unwrap_or_else(|| DEFAULT_NS.to_string()),
            service_account: self
                .service_account
                .unwrap_or_else(|| DEFAULT_SERVICE_ACCOUNT.to_string()),
            image: self.image.unwrap_or_default(),
            parallelism: self.parallelism.unwrap_or_default(),
            driver_args: self.driver_args.unwrap_or_default(),
            exec_args: self.exec_args.unwrap_or_default(),
            prog: self.prog.unwrap_or_default(),
            args: self.args.unwrap_or_default(),
        }
    }
}

#[derive(Debug)]
pub struct PySparkSubmit {
    /// The spark submit path
    path: String,
    /// The master url
    master: String,
    /// The deploy mode of spark cluster
    deploy_mode: String,
    /// The namespace of the spark cluster
    ns: String,
    /// The service account of the spark cluster
    service_account: String,
    /// The image repository of spark driver and executors
    image: String,
    /// The parallelism of the spark job
    parallelism: u32,
    /// The parameters of spark driver
    driver_args: PySparkDriverParams,
    /// The parameters of spark executor
    exec_args: PySparkExecutorParams,
    /// The program executable(or script) to run
    prog: String,
    /// The argument of the program
    args: Vec<String>,
}

impl PySparkSubmit {
    pub fn into_command(self) -> PySparkCommand {
        PySparkCommand::new(&self.path)
            .add_kv("--master", &self.master)
            .add_kv("--deploy-mode", &self.deploy_mode)
            .add_kv("--name", "spark")
            .add_conf(&format!("spark.kubernetes.namespace={}", self.ns))
            .add_conf(&format!(
                "spark.kubernetes.authenticate.driver.serviceAccountName={}",
                self.service_account
            ))
            .add_conf(&format!("spark.kubernetes.container.image={}", self.image))
            .add_conf(&format!("spark.default.parallelism={}", self.parallelism))
            .add_conf(&format!("spark.driver.cores={}", self.driver_args.core))
            .add_conf(&format!("spark.driver.memory={}", self.driver_args.memory))
            .add_conf(&format!(
                "spark.kubernetes.driver.volumes.persistentVolumeClaim.{}.options.claimName={}",
                self.driver_args.pvc.name, self.driver_args.pvc.claim_name
            ))
            .add_conf(&format!(
                "spark.kubernetes.driver.volumes.persistentVolumeClaim.{}.mount.path={}",
                self.driver_args.pvc.name, self.driver_args.pvc.mount_path
            ))
            .add_conf(&format!("spark.executor.instances={}", self.exec_args.nr))
            .add_conf(&format!("spark.executor.cores={}", self.exec_args.core))
            .add_conf(&format!("spark.executor.memory={}", self.exec_args.memory))
            .add_conf(&format!(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.{}.options.claimName={}",
                self.exec_args.pvc.name, self.exec_args.pvc.claim_name
            ))
            .add_conf(&format!(
                "spark.kubernetes.executor.volumes.persistentVolumeClaim.{}.mount.path={}",
                self.exec_args.pvc.name, self.exec_args.pvc.mount_path
            ))
            .arg(&self.prog)
            .arg(&self.args.join(" "))
    }
}

pub struct PySparkCommand {
    pub cmd: Command,
}

impl PySparkCommand {
    fn new(prog: &str) -> Self {
        Self {
            cmd: Command::new(prog),
        }
    }

    fn add_kv(mut self, key: &str, value: &str) -> Self {
        self.cmd.arg(key).arg(value);
        self
    }

    fn add_conf(mut self, conf: &str) -> Self {
        self.cmd.arg("--conf").arg(conf);
        self
    }

    fn arg(mut self, arg: &str) -> Self {
        self.cmd.arg(arg);
        self
    }
}

#[derive(Debug, Default)]
pub struct PySparkDriverParams {
    pub core: String,
    pub memory: String,
    pub pvc: PvcParams,
}

#[derive(Debug, Default)]
pub struct PySparkExecutorParams {
    pub core: String,
    pub memory: String,
    pub nr: String,
    pub pvc: PvcParams,
}

#[derive(Debug, Default)]
pub struct PvcParams {
    pub name: String,
    pub claim_name: String,
    pub mount_path: String,
}
