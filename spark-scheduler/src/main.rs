mod ops;
mod predprio;
mod sched;

use anyhow::Result;
use kube::Client;
use tokio::time::{sleep, Duration};

use sched::Scheduler;

#[tokio::main]
async fn main() -> Result<()> {
    let client = Client::try_default().await?;

    tokio::spawn(async move {
        Scheduler::new_and_then_run(client)
            .await
            .expect("failed to run scheduler");
    });

    // block
    loop {
        sleep(Duration::from_secs(1)).await;
    }
}
