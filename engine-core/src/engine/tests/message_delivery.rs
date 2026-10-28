use super::helpers::InstrumentedPersistence;
use super::message_routing::{deploy, key, metadata, receiver, sender, wait};
use crate::{
    BpmnElement, InstanceState, MessageDelivery, ProcessDefinitionBuilder, WorkflowEngine,
    WorkflowPersistence,
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

async fn restore(p: Arc<InstrumentedPersistence>) -> WorkflowEngine {
    let engine = WorkflowEngine::new().with_persistence(p.clone());
    for c in p.list_definition_collections().await.unwrap() {
        engine.restore_definition_collection(&c).unwrap();
    }
    for instance in p.list_instances().await.unwrap() {
        engine.restore_instance(instance).await;
    }
    for task in p.list_user_tasks().await.unwrap() {
        engine.restore_user_task(task);
    }
    for task in p.list_service_tasks().await.unwrap() {
        engine.restore_service_task(task);
    }
    for timer in p.list_timers().await.unwrap() {
        engine.restore_timer(timer);
    }
    for catch in p.list_message_catches().await.unwrap() {
        engine.restore_message_catch(catch);
    }
    engine
}
fn persistence() -> Arc<InstrumentedPersistence> {
    Arc::new(InstrumentedPersistence::new(Duration::ZERO, Duration::ZERO))
}
fn counting_receiver(start: bool) -> crate::ProcessDefinition {
    let mut target = receiver(start);
    target.variable_defaults.insert("received".into(), json!(0));
    target.listeners.insert(
        "receive".into(),
        vec![crate::ExecutionListener {
            event: crate::ListenerEvent::End,
            script: "received += 1;".into(),
        }],
    );
    target
}
async fn replay(engine: &WorkflowEngine, delivery: &MessageDelivery) {
    let instance = engine
        .instances
        .get(&delivery.source_instance_id)
        .await
        .unwrap();
    instance
        .write()
        .await
        .event_state
        .as_mut()
        .unwrap()
        .message_outbox
        .insert(delivery.id, delivery.clone());
    engine
        .persist_instance_required(delivery.source_instance_id)
        .await
        .unwrap();
}
#[tokio::test]
async fn committed_sender_outbox_survives_crash_and_starts_only_pinned_receiver() {
    let p = persistence();
    let engine = WorkflowEngine::new().with_persistence(p.clone());
    let batch = deploy(&engine, counting_receiver(true)).await;
    let newer = deploy(&engine, receiver(true)).await;
    let dispatch = engine.message_dispatch_lock.lock().await;
    let source = wait(&engine, key(&batch, "sender"), "order-1").await;
    let rows = engine.pending_internal_messages().await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].source_instance_id, source);
    assert_eq!(rows[0].source_revision, 1);
    assert_eq!(rows[0].route.target.definition_key, key(&batch, "receiver"));
    assert_ne!(rows[0].route.target.definition_key, key(&newer, "receiver"));
    drop(dispatch);
    engine.shutdown().await;
    let restored = restore(p.clone()).await;
    assert_eq!(restored.process_internal_messages().await.unwrap(), 1);
    assert!(
        restored
            .pending_internal_messages()
            .await
            .unwrap()
            .is_empty()
    );
    let target = restored.get_instance_or_archived(rows[0].id).await.unwrap();
    assert_eq!(target.variables["received"], 1);
    assert_eq!(target.business_key, "order-1");
    let receipt = p.get_message_receipt(rows[0].id).await.unwrap().unwrap();
    replay(&restored, &receipt.delivery).await;
    assert_eq!(restored.process_internal_messages().await.unwrap(), 1);
    assert_eq!(
        restored
            .get_instance_or_archived(rows[0].id)
            .await
            .unwrap()
            .variables["received"],
        1
    );
    assert_eq!(restored.pending_user_tasks.len(), 1);
    assert!(
        restored
            .get_instance_or_archived(rows[0].id)
            .await
            .unwrap()
            .event_state
            .unwrap()
            .message_inbox
            .is_empty()
    );
    let mut conflict = receipt.delivery.clone();
    conflict.receiver = Some(crate::MessageReceiver::Start {
        instance_id: uuid::Uuid::new_v4(),
    });
    replay(&restored, &conflict).await;
    assert!(restored.process_internal_messages().await.is_err());
    replay(&restored, &receipt.delivery).await;
    assert_eq!(restored.process_internal_messages().await.unwrap(), 1);
    restored.shutdown().await;
}
#[tokio::test]
async fn receiver_commit_before_ack_deduplicates_across_recovery_and_archival() {
    let p = persistence();
    let engine = WorkflowEngine::new().with_persistence(p.clone());
    let batch = deploy(&engine, counting_receiver(true)).await;
    p.receipt_failures.store(1, Ordering::SeqCst);
    wait(&engine, key(&batch, "sender"), "order-1").await;
    let row = engine
        .pending_internal_messages()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(p.get_message_receipt(row.id).await.unwrap().is_none());
    let target = p.get_active_instance(row.id).await.unwrap().unwrap();
    assert_eq!(target.variables["received"], 1);
    assert!(
        target
            .event_state
            .unwrap()
            .message_inbox
            .contains_key(&row.id)
    );
    engine.shutdown().await;
    let restored = restore(p.clone()).await;
    assert_eq!(restored.process_internal_messages().await.unwrap(), 1);
    assert_eq!(
        restored
            .get_instance_or_archived(row.id)
            .await
            .unwrap()
            .variables["received"],
        1
    );
    let task = restored.pending_user_tasks.iter().next().unwrap().task_id;
    restored
        .complete_user_task(task, HashMap::new())
        .await
        .unwrap();
    let receipt = p.get_message_receipt(row.id).await.unwrap().unwrap();
    replay(&restored, &receipt.delivery).await;
    assert_eq!(restored.process_internal_messages().await.unwrap(), 1);
    assert_eq!(
        restored
            .get_instance_or_archived(row.id)
            .await
            .unwrap()
            .state,
        InstanceState::Completed
    );
    assert_eq!(
        p.get_completed_instance(&row.id.to_string())
            .await
            .unwrap()
            .unwrap()
            .variables["received"],
        1
    );
    assert!(restored.pending_user_tasks.is_empty());
    restored.shutdown().await;
}
#[tokio::test]
async fn missing_and_ambiguous_receivers_are_retained_then_correlated_exactly() {
    let p = persistence();
    let engine = WorkflowEngine::new().with_persistence(p.clone());
    let batch = deploy(&engine, counting_receiver(false)).await;
    let source = wait(&engine, key(&batch, "sender"), "order-1").await;
    assert_eq!(engine.pending_internal_messages().await.unwrap().len(), 1);
    assert!(engine.delete_instance(source).await.is_err());
    let dispatch = engine.message_dispatch_lock.lock().await;
    let a = wait(&engine, key(&batch, "receiver"), "order-1").await;
    let b = wait(&engine, key(&batch, "receiver"), "order-1").await;
    wait(&engine, key(&batch, "receiver"), "other-order").await;
    drop(dispatch);
    assert_eq!(engine.process_internal_messages().await.unwrap(), 0);
    engine.suspend_instance(b).await.unwrap();
    assert_eq!(engine.process_internal_messages().await.unwrap(), 1);
    assert_eq!(
        engine.get_instance_or_archived(a).await.unwrap().variables["received"],
        1
    );
    assert_eq!(
        engine.get_instance_or_archived(b).await.unwrap().variables["received"],
        0
    );
    assert_eq!(engine.pending_message_catches.len(), 2);
    engine.shutdown().await;
}
#[tokio::test]
async fn sender_and_receiver_save_failures_expose_no_external_work_before_commit() {
    let p = persistence();
    let engine = WorkflowEngine::new().with_persistence(p.clone());
    let batch = deploy(&engine, counting_receiver(true)).await;
    p.outbox_failures.store(1, Ordering::SeqCst);
    assert!(
        engine
            .start_instance_with_variables(
                key(&batch, "sender"),
                HashMap::from([("business_key".into(), json!("order-1"))])
            )
            .await
            .is_err()
    );
    assert!(engine.pending_user_tasks.is_empty());
    assert!(engine.pending_internal_messages().await.unwrap().is_empty());
    p.inbox_failures.store(1, Ordering::SeqCst);
    assert!(engine.process_internal_messages().await.is_err());
    assert!(engine.pending_user_tasks.is_empty());
    let row = engine
        .pending_internal_messages()
        .await
        .unwrap()
        .pop()
        .unwrap();
    assert!(p.get_message_receipt(row.id).await.unwrap().is_none());
    assert_eq!(engine.process_internal_messages().await.unwrap(), 1);
    assert_eq!(engine.pending_user_tasks.len(), 1);
    engine.shutdown().await;
}
#[tokio::test]
async fn concurrent_dispatchers_and_repeated_send_occurrences_are_distinct() {
    let engine = Arc::new(WorkflowEngine::new());
    let mut source = sender();
    source.nodes.insert(
        "again".into(),
        BpmnElement::SendTask {
            message_name: "order".into(),
            multi_instance: None,
        },
    );
    source.flows.get_mut("send").unwrap()[0].target = "again".into();
    source.flows.insert(
        "again".into(),
        vec![crate::SequenceFlow {
            target: "e".into(),
            condition: None,
        }],
    );
    let mut data = metadata();
    let mut flow = data[0]["message_flows"][0].clone();
    flow["id"] = json!("flow2");
    flow["source"]["element_id"] = json!("again");
    data[0]["message_flows"].as_array_mut().unwrap().push(flow);
    let batch = engine
        .deploy_collection(vec![source, receiver(true)], "xml".into(), data)
        .await
        .unwrap();
    let dispatch = engine.message_dispatch_lock.lock().await;
    wait(&engine, key(&batch, "sender"), "order-1").await;
    let rows = engine.pending_internal_messages().await.unwrap();
    assert_eq!(rows.len(), 2);
    assert_ne!(rows[0].id, rows[1].id);
    assert_ne!(rows[0].source_revision, rows[1].source_revision);
    drop(dispatch);
    let (a, b) = tokio::join!(
        engine.process_internal_messages(),
        engine.process_internal_messages()
    );
    assert_eq!(a.unwrap() + b.unwrap(), 2);
    assert_eq!(engine.pending_user_tasks.len(), 2);
    assert_eq!(engine.process_internal_messages().await.unwrap(), 0);
}
#[tokio::test]
async fn gateway_and_interrupting_boundary_delivery_cancel_exact_siblings() {
    for boundary in [false, true] {
        let engine = WorkflowEngine::new();
        let builder = ProcessDefinitionBuilder::new("receiver")
            .node("s", BpmnElement::StartEvent)
            .node("e", BpmnElement::EndEvent);
        let target = if boundary {
            builder
                .node("work", BpmnElement::UserTask("worker".into()))
                .node(
                    "receive",
                    BpmnElement::BoundaryMessageEvent {
                        attached_to: "work".into(),
                        message_name: "order".into(),
                        cancel_activity: true,
                    },
                )
                .flow("s", "work")
                .flow("work", "e")
                .flow("receive", "e")
                .build()
                .unwrap()
        } else {
            builder
                .node("g", BpmnElement::EventBasedGateway)
                .node(
                    "receive",
                    BpmnElement::MessageCatchEvent {
                        message_name: "order".into(),
                    },
                )
                .node(
                    "timer",
                    BpmnElement::TimerCatchEvent(crate::TimerDefinition::Duration(
                        std::time::Duration::from_secs(3600),
                    )),
                )
                .flow("s", "g")
                .flow("g", "receive")
                .flow("g", "timer")
                .flow("receive", "e")
                .flow("timer", "e")
                .build()
                .unwrap()
        };
        let batch = deploy(&engine, target).await;
        let target = wait(&engine, key(&batch, "receiver"), "order-1").await;
        wait(&engine, key(&batch, "sender"), "order-1").await;
        assert_eq!(
            engine.get_instance_or_archived(target).await.unwrap().state,
            InstanceState::Completed
        );
        assert!(engine.pending_user_tasks.is_empty());
        assert!(engine.pending_timers.is_empty());
        assert!(engine.pending_message_catches.is_empty());
        assert!(engine.pending_internal_messages().await.unwrap().is_empty());
    }
}

#[tokio::test]
async fn sequence_flow_revisits_of_one_send_node_get_distinct_durable_occurrences() {
    let engine = WorkflowEngine::new();
    let source = ProcessDefinitionBuilder::new("sender")
        .node("s", BpmnElement::StartEvent)
        .node(
            "send",
            BpmnElement::SendTask {
                message_name: "order".into(),
                multi_instance: None,
            },
        )
        .node(
            "step",
            BpmnElement::ScriptTask {
                script: "count += 1;".into(),
                multi_instance: None,
            },
        )
        .node(
            "choice",
            BpmnElement::ExclusiveGateway {
                default: Some("e".into()),
            },
        )
        .node("e", BpmnElement::EndEvent)
        .flow("s", "send")
        .flow("send", "step")
        .flow("step", "choice")
        .conditional_flow("choice", "send", "count < 3")
        .flow("choice", "e")
        .build()
        .unwrap();
    let mut source = source;
    source.variable_defaults.insert("count".into(), json!(0));
    let batch = engine
        .deploy_collection(vec![source, receiver(true)], "xml".into(), metadata())
        .await
        .unwrap();
    let dispatch = engine.message_dispatch_lock.lock().await;
    wait(&engine, key(&batch, "sender"), "order-1").await;
    let rows = engine.pending_internal_messages().await.unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows.iter().map(|r| r.source_revision).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        rows.iter()
            .map(|r| r.id)
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );
    assert_eq!(
        rows.iter()
            .map(|r| r.variables["count"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    drop(dispatch);
    assert_eq!(engine.process_internal_messages().await.unwrap(), 3);
    assert_eq!(engine.pending_user_tasks.len(), 3);
}

#[tokio::test]
async fn unavailable_prefix_cannot_starve_ready_delivery_beyond_the_batch_limit() {
    let engine = WorkflowEngine::new();
    let batch = deploy(&engine, receiver(false)).await;
    let dispatch = engine.message_dispatch_lock.lock().await;
    let source = wait(&engine, key(&batch, "sender"), "missing").await;
    let base = engine
        .pending_internal_messages()
        .await
        .unwrap()
        .pop()
        .unwrap();
    let instance = engine.instances.get(&source).await.unwrap();
    {
        let mut snapshot = instance.write().await;
        let outbox = &mut snapshot.event_state.as_mut().unwrap().message_outbox;
        outbox.clear();
        for revision in 1..=257 {
            let mut row = base.clone();
            row.id = uuid::Uuid::new_v4();
            row.source_revision = revision;
            if revision == 257 {
                row.business_key = "ready".into();
            }
            outbox.insert(row.id, row);
        }
    }
    let target = wait(&engine, key(&batch, "receiver"), "ready").await;
    drop(dispatch);
    assert_eq!(engine.process_internal_messages().await.unwrap(), 0);
    assert_eq!(engine.process_internal_messages().await.unwrap(), 1);
    assert!(
        engine
            .pending_user_tasks
            .iter()
            .any(|t| t.instance_id == target)
    );
    assert_eq!(engine.pending_internal_messages().await.unwrap().len(), 256);
}

#[tokio::test]
async fn noninterrupting_boundary_rearms_without_cancelling_the_original_activity() {
    let engine = WorkflowEngine::new();
    let target = ProcessDefinitionBuilder::new("receiver")
        .node("s", BpmnElement::StartEvent)
        .node("work", BpmnElement::UserTask("worker".into()))
        .node(
            "receive",
            BpmnElement::BoundaryMessageEvent {
                attached_to: "work".into(),
                message_name: "order".into(),
                cancel_activity: false,
            },
        )
        .node("e", BpmnElement::EndEvent)
        .flow("s", "work")
        .flow("work", "e")
        .flow("receive", "e")
        .build()
        .unwrap();
    let mut target = target;
    target.variable_defaults.insert("received".into(), json!(0));
    target.listeners.insert(
        "receive".into(),
        vec![crate::ExecutionListener {
            event: crate::ListenerEvent::End,
            script: "received += 1;".into(),
        }],
    );
    let batch = deploy(&engine, target).await;
    let target = wait(&engine, key(&batch, "receiver"), "order-1").await;
    let original = engine.pending_user_tasks.iter().next().unwrap().task_id;
    for _ in 0..2 {
        wait(&engine, key(&batch, "sender"), "order-1").await;
        assert!(engine.pending_user_tasks.contains_key(&original));
        assert_eq!(engine.pending_message_catches.len(), 1);
    }
    assert_eq!(
        engine
            .get_instance_or_archived(target)
            .await
            .unwrap()
            .variables["received"],
        2
    );
    engine
        .complete_user_task(original, HashMap::new())
        .await
        .unwrap();
    assert!(engine.pending_message_catches.is_empty());
    assert_eq!(
        engine.get_instance_or_archived(target).await.unwrap().state,
        InstanceState::Completed
    );
}
