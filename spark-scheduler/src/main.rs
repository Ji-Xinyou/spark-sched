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

    let sched = Scheduler::new(client).await;

    let handle = tokio::spawn(async move {
        sched.run().await.expect("scheduler failed");
    });

    handle.await.expect("join handle panicked");
}
