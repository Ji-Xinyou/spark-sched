mod ops;
mod predprio;
mod sched;

use clap::Parser;
use kube::Client;

use sched::Scheduler;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(long)]
    prio: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    let client = Client::try_default()
        .await
        .expect("failed to create client");

    let sched = Scheduler::new(client, args).await;

    let handle = tokio::spawn(async move {
        sched.run().await.expect("scheduler failed");
    });

    handle.await.expect("join handle panicked");
}
