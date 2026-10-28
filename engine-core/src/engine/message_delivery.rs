//! Durable internal messages: sender continuation + outbox and receiver
//! continuation + inbox are each committed in one instance snapshot. Permanent
//! receipts bridge the receiver commit / sender acknowledgment crash window.
use super::{PinnedMessageRoute, PlannedMessageReceiver, WorkflowEngine};
use crate::{
    BpmnElement, EngineError, EngineResult, InstanceState, ProcessDefinition, ProcessInstance,
    Token,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MessageReceiver {
    Start {
        instance_id: Uuid,
    },
    Catch {
        instance_id: Uuid,
        catch_id: Uuid,
        token_id: Uuid,
    },
}
impl MessageReceiver {
    fn instance_id(&self) -> Uuid {
        match self {
            Self::Start { instance_id } | Self::Catch { instance_id, .. } => *instance_id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageDelivery {
    #[serde(default = "message_protocol_version")]
    pub version: u32,
    pub id: Uuid,
    pub source_instance_id: Uuid,
    pub source_token_id: Uuid,
    pub source_revision: u64,
    pub route: PinnedMessageRoute,
    pub business_key: String,
    pub variables: HashMap<String, Value>,
    pub receiver: Option<MessageReceiver>,
}
fn message_protocol_version() -> u32 {
    1
}

impl MessageDelivery {
    fn same_message(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.receiver = None;
        right.receiver = None;
        left == right
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageReceipt {
    pub delivery: MessageDelivery,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MessageCleanup {
    #[serde(default)]
    pub rearm: Option<(crate::PendingMessageCatch, String)>,
    pub messages: Vec<Uuid>,
    pub timers: Vec<Uuid>,
    pub users: Vec<Uuid>,
    pub services: Vec<Uuid>,
    pub children: Vec<Uuid>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MessageInbox {
    pub receipt: MessageReceipt,
    pub cleanup: MessageCleanup,
}

impl WorkflowEngine {
    pub(crate) async fn is_internal_send(&self, id: Uuid, node: &str) -> bool {
        let Some(instance) = self.instances.get(&id).await else {
            return false;
        };
        let key = instance.read().await.definition_key;
        self.internal_routes
            .read()
            .unwrap()
            .get(&key)
            .is_some_and(|routes| routes.iter().any(|r| r.source.element_id == node))
    }
    /// Caller owns persist_locks and the instance write guard. Memory retains an
    /// unacknowledged candidate on I/O failure so retries keep the same identities.
    async fn save_message_snapshot(&self, snapshot: &ProcessInstance) -> EngineResult<()> {
        if let Some(p) = &self.persistence {
            let payload = self.encode_checked_instance(snapshot)?;
            self.message_unconfirmed.insert(snapshot.id, ());
            p.save_instance_payload(snapshot, &payload).await?;
            if matches!(
                snapshot.state,
                InstanceState::Completed | InstanceState::CompletedWithError { .. }
            ) {
                p.save_completed_instance(snapshot).await?;
            }
        }
        self.message_unconfirmed.remove(&snapshot.id);
        Ok(())
    }
    pub(crate) async fn commit_internal_send(
        &self,
        id: Uuid,
        token: &mut Token,
        definition: &ProcessDefinition,
        node: &str,
    ) -> EngineResult<bool> {
        let routes: Vec<_> = self
            .internal_routes
            .read()
            .unwrap()
            .get(&definition.key)
            .into_iter()
            .flatten()
            .filter(|r| r.source.element_id == node)
            .cloned()
            .collect();
        if routes.is_empty() {
            return Ok(false);
        }
        // Verify backend capability before committing any durable outbox entry.
        if let Some(p) = &self.persistence {
            p.get_message_receipt(Uuid::nil()).await?;
        }
        let _persist = self.persist_locks.lock(id).await;
        let instance = self
            .instances
            .get(&id)
            .await
            .ok_or(EngineError::NoSuchInstance(id))?;
        let mut current = instance.write().await;
        if !super::event_subprocess::token_valid(&current, token)
            || current.tokens.get(&token.id).is_some_and(|t| {
                t.message_revision != token.message_revision || t.current_node != node
            })
        {
            return Err(EngineError::AlreadyCompleted);
        }
        let mut candidate = current.clone();
        let mut next = token.clone();
        let state = candidate.event_state.get_or_insert_with(Default::default);
        if next.event_context.is_none() {
            next.event_context = Some(Box::new(crate::domain::token::EventContext {
                generation: state.generation,
                activation: None,
                started_node: Some(node.into()),
                baseline: candidate.variables.clone(),
            }));
        }
        crate::scripting::run_end_scripts(
            &self.scripts,
            id,
            &mut next,
            definition,
            node,
            &mut candidate.audit_log,
            &mut candidate.variables,
        )
        .await?;
        next.message_revision = next.message_revision.checked_add(1).ok_or_else(|| {
            EngineError::InvalidDefinition("Message execution revision exhausted".into())
        })?;
        let payload = next.process_variables();
        let business = candidate.business_key.clone();
        if business.trim().is_empty() {
            return Err(EngineError::InvalidDefinition(
                "Internal sends require a nonempty business key".into(),
            ));
        }
        let state = candidate.event_state.as_mut().unwrap();
        for route in routes {
            let delivery = MessageDelivery {
                version: 1,
                id: Uuid::new_v4(),
                source_instance_id: id,
                source_token_id: next.id,
                source_revision: next.message_revision,
                route,
                business_key: business.clone(),
                variables: payload.clone(),
                receiver: None,
            };
            state.message_outbox.insert(delivery.id, delivery);
        }
        next.current_node =
            super::executor::resolve_next_target(definition, node, &next.variables)?;
        candidate.current_node = next.current_node.clone();
        candidate.tokens.insert(next.id, next.clone());
        candidate
            .audit_log
            .push(format!("Internal message outbox committed at '{node}'"));
        Self::evaluate_conditionals(&mut candidate, definition)?;
        self.encode_checked_instance(&candidate)?;
        *current = candidate;
        self.save_message_snapshot(&current).await?;
        *token = next;
        self.timer_notify.notify_one();
        Ok(true)
    }
    async fn delivery_receipt(&self, id: Uuid) -> EngineResult<Option<MessageReceipt>> {
        match &self.persistence {
            Some(p) => p.get_message_receipt(id).await,
            None => Ok(self.message_receipts.get(&id).map(|r| r.clone())),
        }
    }
    async fn retain_message_receipt(&self, receipt: &MessageReceipt) -> EngineResult<()> {
        if let Some(p) = &self.persistence {
            if p.reserve_message_receipt(receipt).await? != *receipt {
                return Err(EngineError::PersistenceError(
                    "Conflicting internal delivery receipt".into(),
                ));
            }
        } else {
            let existing = self
                .message_receipts
                .entry(receipt.delivery.id)
                .or_insert_with(|| receipt.clone());
            if *existing != *receipt {
                return Err(EngineError::PersistenceError(
                    "Conflicting internal delivery receipt".into(),
                ));
            }
        }
        Ok(())
    }
    /// Catalog includes archives: completing a sender must not drop its outbox.
    /// Correctness-first scan; a dedicated durable pending index is a later optimization.
    async fn message_catalog(&self) -> EngineResult<HashMap<Uuid, ProcessInstance>> {
        let mut snapshots = HashMap::new();
        if let Some(p) = &self.persistence {
            for snapshot in p
                .query_completed_instances(crate::CompletedInstanceQuery::default())
                .await?
            {
                snapshots.insert(snapshot.id, snapshot);
            }
            for snapshot in p.list_instances().await? {
                snapshots.insert(snapshot.id, snapshot);
            }
        } else {
            for (id, instance) in self.instances.all().await {
                snapshots.insert(id, instance.read().await.clone());
            }
        }
        Ok(snapshots)
    }
    pub async fn pending_internal_messages(&self) -> EngineResult<Vec<MessageDelivery>> {
        let mut messages = Vec::new();
        for snapshot in self.message_catalog().await?.into_values() {
            if let Some(state) = snapshot.event_state {
                for (id, row) in state.message_outbox {
                    if id != row.id
                        || row.id.is_nil()
                        || row.source_instance_id != snapshot.id
                        || row.source_revision == 0
                        || row.version != 1
                    {
                        return Err(EngineError::PersistenceError(
                            "Invalid or unsupported internal message outbox record".into(),
                        ));
                    }
                    messages.push(row);
                }
            }
        }
        messages.sort_by_key(|message| {
            (
                message.source_instance_id,
                message.source_token_id,
                message.source_revision,
                message.id,
            )
        });
        Ok(messages)
    }
    async fn update_message_source(
        &self,
        delivery: &MessageDelivery,
        receiver: Option<MessageReceiver>,
        acknowledge: bool,
    ) -> EngineResult<()> {
        let id = delivery.source_instance_id;
        let _step = self.message_instance_locks.lock(id).await;
        let _persist = self.persist_locks.lock(id).await;
        if let Some(instance) = self.instances.get(&id).await {
            let mut current = instance.write().await;
            let Some(state) = current.event_state.as_mut() else {
                return Err(EngineError::PersistenceError(
                    "Missing sender outbox".into(),
                ));
            };
            if acknowledge {
                state.message_outbox.remove(&delivery.id);
            } else {
                let row = state.message_outbox.get_mut(&delivery.id).ok_or_else(|| {
                    EngineError::PersistenceError("Missing delivery record".into())
                })?;
                if row.receiver.is_some() && row.receiver != receiver {
                    return Err(EngineError::PersistenceError(
                        "Cannot rebind an internal receiver".into(),
                    ));
                }
                row.receiver = receiver;
            }
            return self.save_message_snapshot(&current).await;
        }
        let p = self
            .persistence
            .as_ref()
            .ok_or(EngineError::NoSuchInstance(id))?;
        let mut archived = p
            .get_completed_instance(&id.to_string())
            .await?
            .ok_or(EngineError::NoSuchInstance(id))?;
        let state = archived.event_state.as_mut().ok_or_else(|| {
            EngineError::PersistenceError("Missing archived sender outbox".into())
        })?;
        if acknowledge {
            state.message_outbox.remove(&delivery.id);
        } else {
            state
                .message_outbox
                .get_mut(&delivery.id)
                .ok_or_else(|| EngineError::PersistenceError("Missing archived delivery".into()))?
                .receiver = receiver;
        }
        p.save_completed_instance(&archived).await
    }
    async fn cleanup_message_jobs(&self, inbox: &MessageInbox) -> EngineResult<()> {
        let c = &inbox.cleanup;
        for id in &c.messages {
            self.pending_message_catches.remove(id);
            if let Some(p) = &self.persistence {
                p.delete_message_catch(*id).await?;
            }
        }
        for id in &c.timers {
            self.remove_pending_timer(id);
            if let Some(p) = &self.persistence {
                p.delete_timer(*id).await?;
            }
        }
        for id in &c.users {
            self.pending_user_tasks.remove(id);
            if let Some(p) = &self.persistence {
                p.delete_user_task(*id).await?;
            }
        }
        for id in &c.services {
            self.remove_pending_service_task(id);
            if let Some(p) = &self.persistence {
                p.delete_service_task(*id).await?;
            }
        }
        if let Some((catch, attached_to)) = &c.rearm
            && let Some(instance) = self.instances.get(&catch.instance_id).await
        {
            let current = instance.read().await;
            let key = format!("{}/{}", catch.token_id, catch.node_id);
            if current
                .event_state
                .as_ref()
                .is_some_and(|s| s.message_boundaries.get(&key) == Some(&catch.id))
                && current.tokens.get(&catch.token_id).is_some_and(|t| {
                    t.current_node == *attached_to
                        && super::event_subprocess::token_valid(&current, t)
                })
            {
                if let Some(p) = &self.persistence {
                    p.save_message_catch(catch).await?;
                }
                self.pending_message_catches.insert(catch.id, catch.clone());
            }
        }
        for id in &c.children {
            self.abort_event_child(*id).await;
            if self.instances.get(id).await.is_some() {
                self.persist_instance_required(*id).await?;
            }
        }
        Ok(())
    }
    /// Used before deleting a receiver: a permanent receipt must outlive snapshots.
    pub(crate) async fn retain_instance_message_receipts(
        &self,
        instance: &ProcessInstance,
    ) -> EngineResult<()> {
        if let Some(state) = &instance.event_state {
            if !state.message_outbox.is_empty() {
                return Err(EngineError::InvalidDefinition(
                    "Instance has pending internal message deliveries".into(),
                ));
            }
            for inbox in state.message_inbox.values() {
                self.cleanup_message_jobs(inbox).await?;
                self.retain_message_receipt(&inbox.receipt).await?;
            }
        }
        Ok(())
    }
    /// Once the permanent receipt exists, the per-instance bridge can be
    /// removed. Long-running receivers must not accumulate whole payload copies.
    async fn prune_message_inbox(&self, id: Uuid, delivery: Uuid) -> EngineResult<()> {
        let _step = self.message_instance_locks.lock(id).await;
        let _persist = self.persist_locks.lock(id).await;
        if let Some(instance) = self.instances.get(&id).await {
            let mut current = instance.write().await;
            if let Some(state) = current.event_state.as_mut()
                && state.message_inbox.remove(&delivery).is_some()
            {
                self.save_message_snapshot(&current).await?;
            }
        }
        Ok(())
    }
    async fn accept_internal_delivery(&self, delivery: &MessageDelivery) -> EngineResult<bool> {
        let receiver = delivery
            .receiver
            .as_ref()
            .ok_or_else(|| EngineError::PersistenceError("Unbound delivery".into()))?;
        let id = receiver.instance_id();
        let _step = self.message_instance_locks.lock(id).await;
        let _persist = self.persist_locks.lock(id).await;
        // A receipt in an archived receiver is authoritative even before its
        // permanent dedup record was written.
        if let Some(p) = &self.persistence
            && let Some(archive) = p.get_completed_instance(&id.to_string()).await?
            && let Some(inbox) = archive
                .event_state
                .as_ref()
                .and_then(|s| s.message_inbox.get(&delivery.id))
        {
            if inbox.receipt.delivery != *delivery {
                return Err(EngineError::PersistenceError(
                    "Conflicting archived delivery".into(),
                ));
            }
            drop(_persist);
            drop(_step);
            self.cleanup_message_jobs(inbox).await?;
            self.retain_message_receipt(&inbox.receipt).await?;
            self.prune_message_inbox(id, delivery.id).await?;
            return Ok(true);
        }
        if self.instances.get(&id).await.is_none()
            && let Some(p) = &self.persistence
            && let Some(snapshot) = p.get_active_instance(id).await?
        {
            self.restore_instance(snapshot).await;
        }
        if self.instances.get(&id).await.is_none() {
            if !matches!(receiver, MessageReceiver::Start { .. }) {
                return Ok(false);
            }
            let definition = self
                .definitions
                .get(&delivery.route.target.definition_key)
                .ok_or(EngineError::NoSuchDefinition(
                    delivery.route.target.definition_key,
                ))?;
            let start = &delivery.route.target.element_id;
            if !matches!(
                definition.nodes.get(start),
                Some(BpmnElement::MessageStartEvent { .. })
            ) {
                return Err(EngineError::InvalidDefinition(
                    "Pinned message start is missing".into(),
                ));
            }
            let mut variables = definition.variable_defaults.clone();
            variables.extend(delivery.variables.clone());
            variables.remove("business_key");
            let mut snapshot =
                Self::build_initial_instance(definition.key, id, start, variables.clone(), None);
            snapshot.business_key = delivery.business_key.clone();
            let mut token = Token::with_variables(start, variables);
            token.id = delivery.id;
            Self::prepare_initial_event_state(&definition, &mut snapshot, &mut token, None)?;
            crate::engine::data_mappings::enter(&mut token, &definition, start)?;
            crate::scripting::run_node_scripts(
                &self.scripts,
                id,
                &mut token,
                &definition,
                start,
                crate::ListenerEvent::Start,
                &mut snapshot.audit_log,
            )
            .await?;
            crate::scripting::run_end_scripts(
                &self.scripts,
                id,
                &mut token,
                &definition,
                start,
                &mut snapshot.audit_log,
                &mut snapshot.variables,
            )
            .await?;
            token.current_node =
                super::executor::resolve_next_target(&definition, start, &token.variables)?;
            snapshot.current_node = token.current_node.clone();
            snapshot.tokens.insert(token.id, token.clone());
            for active in &mut snapshot.active_tokens {
                if active.token.id == token.id {
                    active.token = token.clone();
                }
            }
            let inbox = MessageInbox {
                receipt: MessageReceipt {
                    delivery: delivery.clone(),
                },
                cleanup: MessageCleanup::default(),
            };
            snapshot
                .event_state
                .as_mut()
                .unwrap()
                .message_inbox
                .insert(delivery.id, inbox);
            self.encode_checked_instance(&snapshot)?;
            self.instances.insert(id, snapshot).await;
        }
        let instance = self
            .instances
            .get(&id)
            .await
            .ok_or(EngineError::NoSuchInstance(id))?;
        let mut current = instance.write().await;
        if let Some(inbox) = current
            .event_state
            .as_ref()
            .and_then(|s| s.message_inbox.get(&delivery.id))
            .cloned()
        {
            if inbox.receipt.delivery != *delivery {
                return Err(EngineError::PersistenceError(
                    "Conflicting receiver delivery".into(),
                ));
            }
            self.save_message_snapshot(&current).await?;
            drop(current);
            drop(_persist);
            drop(_step);
            self.cleanup_message_jobs(&inbox).await?;
            self.retain_message_receipt(&inbox.receipt).await?;
            self.prune_message_inbox(id, delivery.id).await?;
            return Ok(true);
        }
        let MessageReceiver::Catch {
            catch_id, token_id, ..
        } = receiver
        else {
            return Err(EngineError::PersistenceError(
                "Reserved start identity already exists without its receipt".into(),
            ));
        };
        if matches!(
            current.state,
            InstanceState::Suspended { .. }
                | InstanceState::Completed
                | InstanceState::CompletedWithError { .. }
        ) || current.definition_key != delivery.route.target.definition_key
            || current.business_key != delivery.business_key
        {
            return Ok(false);
        }
        let definition = self
            .definitions
            .get(&current.definition_key)
            .ok_or(EngineError::NoSuchDefinition(current.definition_key))?;
        let Some(catch) = self
            .pending_message_catches
            .get(catch_id)
            .map(|r| r.clone())
        else {
            return Ok(false);
        };
        if catch.token_id != *token_id
            || catch.instance_id != id
            || catch.node_id != delivery.route.target.element_id
            || catch.message_name != delivery.route.message_name
            || catch.kind != crate::CatchKind::Message
        {
            return Ok(false);
        }
        let Some(original) = current.tokens.get(token_id).cloned() else {
            return Ok(false);
        };
        if !super::event_subprocess::token_valid(&current, &original) {
            return Ok(false);
        }
        let boundary = match definition.nodes.get(&catch.node_id) {
            Some(BpmnElement::BoundaryMessageEvent {
                attached_to,
                cancel_activity,
                ..
            }) => Some((attached_to.clone(), *cancel_activity)),
            _ => None,
        };
        let group = current
            .event_state
            .as_ref()
            .and_then(|s| s.gateway_waits.get(token_id))
            .cloned();
        let valid_gateway = group.as_ref().is_some_and(|g| {
            g.gateway == original.current_node
                && g.winner.is_none()
                && g.jobs.get(catch_id) == Some(&catch.node_id)
        });
        let expected = boundary
            .as_ref()
            .map(|b| b.0.as_str())
            .unwrap_or(&catch.node_id);
        if original.current_node != expected && !valid_gateway {
            return Ok(false);
        }
        if current.tokens.len() == 1
            && matches!(current.state,InstanceState::WaitingOnMessage { message_id } if message_id!=*catch_id)
        {
            return Ok(false);
        }
        if let Some(activation) = current
            .event_state
            .as_ref()
            .and_then(|s| s.boundary_activations.get(token_id))
            && boundary.is_some()
            && (original.boundary_activation != Some(activation.id)
                || !activation.jobs.contains(catch_id))
        {
            return Ok(false);
        }
        if let Some(live) = current.event_state.as_ref().and_then(|s| {
            s.message_boundaries
                .get(&format!("{token_id}/{}", catch.node_id))
        }) && *live != *catch_id
        {
            return Ok(false);
        }
        let mut candidate = current.clone();
        let mut token = original.clone();
        token.id = delivery.id;
        token.boundary_activation = None;
        token.conditional_wait = None;
        token.discard_data_scope();
        token.variables = candidate.variables.clone();
        token.variables.extend(delivery.variables.clone());
        if let Some(context) = &mut token.event_context {
            context.baseline = candidate.variables.clone();
            context.started_node = None;
        }
        let interrupt = boundary.as_ref().is_none_or(|b| b.1);
        let mut cleanup = MessageCleanup {
            messages: vec![*catch_id],
            ..Default::default()
        };
        if interrupt {
            candidate.tokens.remove(token_id);
            for active in &mut candidate.active_tokens {
                if active.token.id == *token_id {
                    active.completed = true;
                }
            }
            cleanup.messages = self
                .pending_message_catches
                .iter()
                .filter(|c| c.instance_id == id && c.token_id == *token_id)
                .map(|c| c.id)
                .collect();
            cleanup.timers = self
                .pending_timers
                .iter()
                .filter(|t| t.instance_id == id && t.token_id == *token_id)
                .map(|t| t.id)
                .collect();
            cleanup.users = self
                .pending_user_tasks
                .iter()
                .filter(|t| t.instance_id == id && t.token_id == *token_id)
                .map(|t| t.task_id)
                .collect();
            cleanup.services = self
                .pending_service_tasks
                .iter()
                .filter(|t| t.instance_id == id && t.token_id == *token_id)
                .map(|t| t.id)
                .collect();
            cleanup.children = candidate
                .outstanding_calls
                .iter()
                .filter(|(_, call)| call.token.id == *token_id)
                .map(|(id, _)| *id)
                .collect();
            candidate
                .outstanding_calls
                .retain(|id, _| !cleanup.children.contains(id));
        }
        let state = candidate.event_state.get_or_insert_with(Default::default);
        if interrupt {
            state.boundary_activations.remove(token_id);
            state
                .message_boundaries
                .retain(|key, _| !key.starts_with(&format!("{token_id}/")));
        } else if let Some((attached_to, _)) = &boundary {
            let mut rearm = catch.clone();
            rearm.id = Uuid::new_v4();
            state
                .message_boundaries
                .insert(format!("{token_id}/{}", catch.node_id), rearm.id);
            if let Some(activation) = state.boundary_activations.get_mut(token_id) {
                activation.jobs.remove(catch_id);
                activation.jobs.insert(rearm.id);
                activation.messages.retain(|m| m.id != *catch_id);
                activation.messages.push(rearm.clone());
            }
            cleanup.rearm = Some((rearm, attached_to.clone()));
        }
        if let Some(group) = group.filter(|_| valid_gateway) {
            state.gateway_waits.remove(token_id);
            state
                .conditional_waits
                .retain(|_, w| w.group != Some(group.id));
        }
        if token.event_context.is_none() {
            token.event_context = Some(Box::new(crate::domain::token::EventContext {
                generation: state.generation,
                activation: None,
                started_node: None,
                baseline: candidate.variables.clone(),
            }));
        }
        crate::engine::data_mappings::enter(&mut token, &definition, &catch.node_id)?;
        crate::scripting::run_end_scripts(
            &self.scripts,
            id,
            &mut token,
            &definition,
            &catch.node_id,
            &mut candidate.audit_log,
            &mut candidate.variables,
        )
        .await?;
        token.current_node =
            super::executor::resolve_next_target(&definition, &catch.node_id, &token.variables)?;
        candidate.tokens.insert(token.id, token.clone());
        candidate.active_tokens.push(crate::runtime::ActiveToken {
            token: token.clone(),
            fork_id: None,
            branch_index: 0,
            completed: false,
        });
        candidate.state = if candidate.tokens.len() > 1 {
            InstanceState::ParallelExecution {
                active_token_count: candidate.tokens.len(),
            }
        } else {
            InstanceState::Running
        };
        candidate.current_node = token.current_node.clone();
        candidate.audit_log.push(format!(
            "Internal message '{}' accepted ({})",
            delivery.route.flow_id, delivery.id
        ));
        let inbox = MessageInbox {
            receipt: MessageReceipt {
                delivery: delivery.clone(),
            },
            cleanup,
        };
        candidate
            .event_state
            .as_mut()
            .unwrap()
            .message_inbox
            .insert(delivery.id, inbox.clone());
        Self::evaluate_conditionals(&mut candidate, &definition)?;
        self.encode_checked_instance(&candidate)?;
        *current = candidate;
        self.save_message_snapshot(&current).await?;
        drop(current);
        drop(_persist);
        drop(_step);
        self.cleanup_message_jobs(&inbox).await?;
        self.retain_message_receipt(&inbox.receipt).await?;
        self.prune_message_inbox(id, delivery.id).await?;
        Ok(true)
    }
    /// Retry committed outbox records. No public name broadcast is used. One
    /// engine writer per store; concurrent/reentrant dispatchers coalesce.
    pub async fn process_internal_messages(&self) -> EngineResult<usize> {
        if self
            .internal_routes
            .read()
            .unwrap()
            .values()
            .all(Vec::is_empty)
            && self.message_unconfirmed.is_empty()
        {
            return Ok(0);
        }
        let Ok(mut dispatch) = self.message_dispatch_lock.try_lock() else {
            return Ok(0);
        };
        // Rotate bounded passes so a prefix of unavailable receivers cannot
        // indefinitely hide ready deliveries later in the catalog.
        let offset = *dispatch;
        *dispatch = offset.wrapping_add(256);
        let unconfirmed: Vec<_> = self
            .message_unconfirmed
            .iter()
            .map(|id| *id.key())
            .collect();
        for id in unconfirmed {
            self.persist_instance_required(id).await?;
            self.message_unconfirmed.remove(&id);
            self.reconcile_event_instance(id).await?;
        }
        let mut count = 0;
        let mut attempted = HashSet::new();
        let mut failure = None;
        loop {
            let mut rows = self.pending_internal_messages().await?;
            if !rows.is_empty() {
                let start = offset % rows.len();
                rows.rotate_left(start);
            }
            let mut progress = false;
            for mut delivery in rows {
                if attempted.len() >= 256 {
                    break;
                }
                if !attempted.insert(delivery.id) {
                    continue;
                }
                let result: EngineResult<bool> = async {
                    if let Some(receipt) = self.delivery_receipt(delivery.id).await? {
                        if !receipt.delivery.same_message(&delivery)
                            || (delivery.receiver.is_some()
                                && delivery.receiver != receipt.delivery.receiver)
                        {
                            return Err(EngineError::PersistenceError(
                                "Conflicting internal message identity".into(),
                            ));
                        }
                        self.update_message_source(&delivery, None, true).await?;
                        if let Some(receiver) = receipt.delivery.receiver {
                            self.reconcile_event_instance(receiver.instance_id())
                                .await?;
                        }
                        return Ok(true);
                    }
                    if !self
                        .internal_routes
                        .read()
                        .unwrap()
                        .get(&delivery.route.source.definition_key)
                        .is_some_and(|routes| routes.contains(&delivery.route))
                    {
                        return Err(EngineError::PersistenceError(
                            "Internal delivery route does not match its committed collection"
                                .into(),
                        ));
                    }
                    if delivery.receiver.is_none() {
                        let planned = self
                            .plan_pinned_message_receiver(&delivery.route, &delivery.business_key)
                            .await;
                        let receiver = match planned {
                            PlannedMessageReceiver::Start => MessageReceiver::Start {
                                instance_id: delivery.id,
                            },
                            PlannedMessageReceiver::Catch {
                                instance_id,
                                catch_id,
                                token_id,
                            } => MessageReceiver::Catch {
                                instance_id,
                                catch_id,
                                token_id,
                            },
                            PlannedMessageReceiver::Unavailable(_) => return Ok(false),
                        };
                        self.update_message_source(&delivery, Some(receiver.clone()), false)
                            .await?;
                        delivery.receiver = Some(receiver);
                    }
                    if !self
                        .definitions
                        .contains_key(&delivery.route.target.definition_key)
                    {
                        return Ok(false);
                    }
                    if !self.accept_internal_delivery(&delivery).await? {
                        return Ok(false);
                    }
                    self.update_message_source(&delivery, None, true).await?;
                    let target = delivery.receiver.as_ref().unwrap().instance_id();
                    self.reconcile_event_instance(target).await?;
                    Ok(true)
                }
                .await;
                match result {
                    Ok(true) => {
                        count += 1;
                        progress = true;
                    }
                    Ok(false) => {}
                    Err(error) => {
                        tracing::warn!(delivery_id=%delivery.id,"Internal delivery retained for retry: {error}");
                        if failure.is_none() {
                            failure = Some(error);
                        }
                    }
                }
            }
            if !progress || attempted.len() >= 256 {
                break;
            }
        }
        if let Some(error) = failure {
            return Err(error);
        }
        Ok(count)
    }
}
