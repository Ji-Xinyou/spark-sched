use crate::sched::Scheduler;

use anyhow::{anyhow, Result};
use k8s_openapi::{
    api::core::v1::{Binding, Event, EventSource, ObjectReference, Pod},
    apimachinery::pkg::apis::meta::v1::{Status, Time},
    chrono::Utc,
    serde_json,
};

use kube::{api::PostParams, core::ObjectMeta, Api};

pub(crate) struct PodBindParameters {
    pub(crate) node_name: String,
    pub(crate) pod: Pod,
    pub(crate) scheduler_name: String,
}

pub(crate) struct EmitParameters {
    pub(crate) pod: Pod,
    pub(crate) scheduler_name: String,
    pub(crate) message: String,
}

impl Scheduler {
    pub(crate) async fn emit_event(&self, params: EmitParameters) -> Result<()> {
        let client = self.client.clone();
        let EmitParameters {
            pod,
            scheduler_name,
            message,
        } = params;

        let pod_name = pod.metadata.name.expect("empty pod name");
        let pod_ns = pod.metadata.namespace.expect("empty pod namespace");

        let event = Event {
            count: Some(1),
            message: Some(message.to_string()),
            reason: Some("Scheduled".to_string()),
            last_timestamp: Some(Time(Utc::now())),
            first_timestamp: Some(Time(Utc::now())),
            type_: Some("Normal".to_string()),
            source: Some(EventSource {
                component: Some(scheduler_name),
                ..Default::default()
            }),
            involved_object: ObjectReference {
                kind: Some(String::from("Pod")),
                name: Some(pod_name.clone()),
                namespace: Some(pod_ns.clone()),
                uid: None,
                ..Default::default()
            },
            metadata: ObjectMeta {
                generate_name: Some(format!("{}-", &pod_name)),
                ..Default::default()
            },
            ..Default::default()
        };

        Api::namespaced(client, &pod_ns)
            .create(&PostParams::default(), &event)
            .await?;

        Ok(())
    }

    pub(crate) async fn bind_pod_to_node(&self, params: PodBindParameters) -> Result<()> {
        let client = self.client.clone();
        let PodBindParameters {
            node_name,
            pod,
            scheduler_name,
        } = params;

        let pod_name = pod.metadata.name.expect("empty pod name");
        let pod_namespace = pod.metadata.namespace.expect("empty pod namespace");

        let pods: Api<Pod> = Api::namespaced(client.clone(), &pod_namespace);
        let res: Result<Status, kube::Error> = pods
            .create_subresource(
                "binding",
                &pod_name.clone(),
                &PostParams {
                    field_manager: Some(scheduler_name.clone()),
                    ..Default::default()
                },
                serde_json::to_vec(&Binding {
                    metadata: kube::core::ObjectMeta {
                        name: Some(pod_name.clone()),
                        ..Default::default()
                    },
                    target: k8s_openapi::api::core::v1::ObjectReference {
                        api_version: Some("v1".to_owned()),
                        kind: Some("Node".to_owned()),
                        name: Some(node_name.clone()),
                        ..Default::default()
                    },
                })?,
            )
            .await;

        let status = res?;

        let code = match status.code {
            Some(code) => code,
            None => {
                return Err(anyhow!(
                    "Could not obtain status code from kubernetes response"
                ))
            }
        };

        if code >= 200 && code <= 202 {
            Ok(())
        } else {
            Err(anyhow!(
                "An error occurred while trying to bind pod to node: {status:?}"
            ))
        }
    }
}
