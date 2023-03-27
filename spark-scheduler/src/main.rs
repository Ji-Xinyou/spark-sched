mod ops;
mod predprio;
mod sched;

use kube::Client;

use sched::Scheduler;

#[tokio::main]
async fn main() {
    let client = Client::try_default()
        .await
        .expect("failed to create client");

    let handle = tokio::spawn(async move {
        Scheduler::run(client)
            .await
            .expect("failed to run scheduler");
    });

    handle.await.expect("join handle panicked");
}
