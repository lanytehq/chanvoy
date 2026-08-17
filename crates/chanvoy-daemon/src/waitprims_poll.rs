//! A2 grok-bot / no-listener poll: one `run_poll_cycle` per wake.
//!
//! Events and cursor advances stay uncommitted until `poll_cycle_ack`.
//! Cancel, bound exhaustion, and a restart between outcome and ack
//! replay from the last acknowledged cursor.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{CoreError, Message};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use waitprims_async::{
    run_poll_cycle, BindHandle, Cancel, Observation, Observer, POLL_ACK_RETENTION,
};
use waitprims_core::{
    ActorRef, AgentWaitMessage, Anchor, AnchorKind, AuthnMode, BaselinePolicy, Canonicalization,
    CapabilityToken, DigestAlgorithm, IdToken, JcsDigest, OpaqueRef, PollCycleAck,
    PollCycleOutcome, PollCycleRequest, PredicateRef, Registration, RegistrationSet, WaitBound,
};

use crate::wait::{
    channel_is_monitored, establish_baseline, wait_push_from_cursor, wait_rest_from_cursor,
    WaitPredicate,
};
use crate::waitprims_hold::{
    authenticate_sidecar_message, channel_subject, classify_bind_core_error, contract_internal,
    cursor_from_baseline, event_from_foreign_message, map_waitprims_err, scan_cursor_from_bind,
    timestamp_now, ChanvoyBind, LeaseRelease, MessageSidecar, WallClock, METHOD_ID,
};
use crate::AppState;

#[cfg(test)]
pub(crate) fn poll_ack_retention_is_fail_closed() -> bool {
    POLL_ACK_RETENTION.contains("not committed until poll_cycle_ack")
}

/// Last-acked cursors. Never advanced by an unacked outcome. Persisted
/// under the profile config dir so a restart cannot skip an unacked cycle.
#[derive(Clone)]
pub(crate) struct PollCursorStore {
    profile: String,
    inner: Arc<Mutex<BTreeMap<String, String>>>,
}

impl PollCursorStore {
    pub(crate) fn load(profile: &str) -> Self {
        let map = load_persisted(profile);
        Self {
            profile: profile.to_string(),
            inner: Arc::new(Mutex::new(map)),
        }
    }

    pub(crate) fn get(&self, registration_id: &str) -> Option<Anchor> {
        self.inner
            .lock()
            .ok()
            .and_then(|map| map.get(registration_id).cloned())
            .map(|value| Anchor {
                kind: AnchorKind::ProviderOpaque,
                value: IdToken::new(value),
            })
    }

    pub(crate) fn commit(&self, anchors: BTreeMap<String, Anchor>) {
        if let Ok(mut map) = self.inner.lock() {
            for (key, anchor) in anchors {
                map.insert(key, anchor.value.as_str().to_string());
            }
            persist(&self.profile, &map);
        }
    }
}

fn poll_cursor_path(profile: &str) -> std::path::PathBuf {
    chanvoy_core::default_chanvoy_config_dir().join(format!("poll-cursors-{profile}.json"))
}

fn load_persisted(profile: &str) -> BTreeMap<String, String> {
    let path = poll_cursor_path(profile);
    std::fs::read_to_string(path)
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

fn persist(profile: &str, map: &BTreeMap<String, String>) {
    let path = poll_cursor_path(profile);
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    if let Ok(raw) = serde_json::to_string(map) {
        let _ = std::fs::write(path, raw);
    }
}

pub(crate) struct PollCycleResult {
    pub messages: Vec<Message>,
    pub outcome: PollCycleOutcome,
}

/// One no-listener poll cycle. Does not commit cursors. Caller must
/// [`ack_poll_cycle`] after consuming messages.
pub(crate) async fn poll_channel_once(
    state: &AppState,
    store: &PollCursorStore,
    channel: &str,
    channel_id: &str,
    after: Option<&str>,
    deadline: Instant,
) -> Result<PollCycleResult, CoreError> {
    let _retention = POLL_ACK_RETENTION;
    let _ = _retention;
    let predicate = WaitPredicate::compile(&state.my_user_id, channel_id, None, None)?;
    let sidecar = MessageSidecar::new();
    let last_error = Arc::new(Mutex::new(None));
    let inner_cancel = CancellationToken::new();
    let observer = PollObserver {
        state: state.clone(),
        channel: channel.to_string(),
        channel_id: channel_id.to_string(),
        after: after.map(str::to_string),
        monitored: channel_is_monitored(state, channel),
        predicate,
        deadline,
        my_user_id: state.my_user_id.clone(),
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        inner_cancel: inner_cancel.clone(),
        restored: Mutex::new(Vec::new()),
    };
    let clock = WallClock::new();
    let waiter = format!("poll:{}", channel_id);
    let registration_id = format!("reg:{waiter}");
    let (set, request) = build_poll_documents(
        &waiter,
        &state.my_user_id,
        channel_id,
        after,
        store.get(&registration_id),
        clock.project_deadline(deadline),
    )?;
    let set_json = serde_json::to_string(&AgentWaitMessage::RegistrationSet(set.clone()))
        .map_err(|err| contract_internal(channel, format!("poll set encode: {err}")))?;
    let req_json = serde_json::to_string(&AgentWaitMessage::PollCycleRequest(request.clone()))
        .map_err(|err| contract_internal(channel, format!("poll request encode: {err}")))?;
    let admitted = waitprims_core::validate_raw_documents([&set_json, &req_json])
        .map_err(|err| contract_internal(channel, format!("poll admission: {err}")))?;
    let mut set = None;
    let mut request = None;
    for msg in admitted {
        match msg.into_inner() {
            AgentWaitMessage::RegistrationSet(v) => set = Some(v),
            AgentWaitMessage::PollCycleRequest(v) => request = Some(v),
            other => {
                return Err(contract_internal(
                    channel,
                    format!("unexpected {}", other.message_type().as_str()),
                ));
            }
        }
    }
    let set = set.ok_or_else(|| contract_internal(channel, "missing poll set"))?;
    let request = request.ok_or_else(|| contract_internal(channel, "missing poll request"))?;
    let cancel = Cancel::new();
    let outcome = run_poll_cycle(&set, &request, &observer, &clock, &cancel)
        .await
        .map_err(|err| map_waitprims_err(channel, err))?;
    inner_cancel.cancel();
    let mut messages = Vec::new();
    for event in &outcome.events {
        if let Some(message) = sidecar.take(event.payload.payload_ref.as_str()) {
            authenticate_sidecar_message(channel, &message, &event.payload.content_digest)?;
            messages.push(message);
        }
    }
    Ok(PollCycleResult { messages, outcome })
}

/// Commit only the supplied anchors. An unacked outcome must not have
/// called this.
pub(crate) fn ack_poll_cycle(
    store: &PollCursorStore,
    outcome: &PollCycleOutcome,
    waiter: &str,
    my_user_id: &str,
) -> Result<(), CoreError> {
    let ack = PollCycleAck {
        capabilities: vec![CapabilityToken::new("contract: agent-wait/v0")],
        message_id: IdToken::new(format!("ack:{}", outcome.message_id.as_str())),
        correlation_id: IdToken::new(waiter),
        created_at: timestamp_now(),
        actor_ref: ActorRef::new(format!("actor:{my_user_id}")),
        causation_id: None,
        grant_ref: None,
        verification_receipt_ref: None,
        policy_decision_ref: None,
        waiter_id: IdToken::new(waiter),
        outcome_ref: outcome.message_id.clone(),
        committed_anchors: outcome.retained_through.clone(),
        retained_events: outcome.retained_events.clone(),
    };
    let json = serde_json::to_string(&AgentWaitMessage::PollCycleAck(ack))
        .map_err(|err| contract_internal("poll", format!("ack encode: {err}")))?;
    waitprims_core::validate_message(&json)
        .map_err(|err| contract_internal("poll", format!("ack admission: {err}")))?;
    store.commit(outcome.retained_through.clone());
    Ok(())
}

fn build_poll_documents(
    waiter: &str,
    my_user_id: &str,
    channel_id: &str,
    after: Option<&str>,
    acked: Option<Anchor>,
    deadline: waitprims_core::Timestamp,
) -> Result<(RegistrationSet, PollCycleRequest), CoreError> {
    let now = timestamp_now();
    let lease_expires = deadline.saturating_add(Duration::from_secs(3600));
    let waiter_id = IdToken::new(waiter);
    let set_id = IdToken::new(format!("regset:{waiter}"));
    let revision = IdToken::new(format!("rev:{waiter}"));
    let registration_id = IdToken::new(format!("reg:{waiter}"));
    let actor = ActorRef::new(format!("actor:{my_user_id}"));
    let capabilities = vec![CapabilityToken::new("contract: agent-wait/v0")];
    let (start_anchor, baseline_policy) = match acked.clone().or_else(|| {
        after.map(|cursor| Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(cursor),
        })
    }) {
        Some(anchor) => (Some(anchor), None),
        None => (None, Some(BaselinePolicy::Latest)),
    };
    let registration = Registration {
        registration_id: registration_id.clone(),
        method_id: IdToken::new(METHOD_ID),
        subject_kind: IdToken::new("channel"),
        subject_id: channel_subject(channel_id),
        required: true,
        source_instance_ref: OpaqueRef::new("source:chanvoy-daemon"),
        predicate_ref: PredicateRef::new("pred:chanvoy-wait"),
        capability_ref: OpaqueRef::new("cap:wait"),
        lease_expires_at: lease_expires,
        bounds: WaitBound {
            max_events: 32,
            max_bytes: 1_048_576,
        },
        start_anchor,
        baseline_policy,
    };
    let digest = crate::waitprims_hold::registration_digest_hex(&registration)?;
    let set = RegistrationSet {
        capabilities: capabilities.clone(),
        message_id: set_id.clone(),
        correlation_id: waiter_id.clone(),
        created_at: now.clone(),
        actor_ref: actor.clone(),
        causation_id: None,
        grant_ref: None,
        verification_receipt_ref: None,
        policy_decision_ref: None,
        principal_ref: actor.clone(),
        waiter_id: waiter_id.clone(),
        seat_ref: OpaqueRef::new(format!("seat:{my_user_id}")),
        registration_revision: revision.clone(),
        logical_deadline: deadline.clone(),
        authn_mode: AuthnMode::Disabled,
        aggregate_limits: WaitBound {
            max_events: 32,
            max_bytes: 1_048_576,
        },
        registration_digest: JcsDigest {
            canonicalization: Canonicalization::Rfc8785,
            algorithm: DigestAlgorithm::Sha256,
            value: digest,
        },
        registrations: vec![registration],
    };
    let mut acknowledged_anchors = BTreeMap::new();
    if let Some(anchor) = acked {
        acknowledged_anchors.insert(registration_id.as_str().to_string(), anchor);
    }
    let request = PollCycleRequest {
        capabilities,
        message_id: IdToken::new(format!("preq:{waiter}")),
        correlation_id: waiter_id.clone(),
        created_at: now,
        actor_ref: actor,
        causation_id: None,
        grant_ref: None,
        verification_receipt_ref: None,
        policy_decision_ref: None,
        waiter_id,
        registration_set_ref: set_id,
        registration_revision: revision,
        logical_deadline: deadline.clone(),
        run_deadline: deadline,
        required_arms: vec![registration_id],
        fairness_cursor: IdToken::new(format!("fair:{waiter}")),
        acknowledged_anchors,
        activation_ref: OpaqueRef::new("activation:none"),
        cycle_id: IdToken::new(format!("cycle:{waiter}")),
        bound: None,
    };
    Ok((set, request))
}

struct PollObserver {
    state: AppState,
    channel: String,
    channel_id: String,
    after: Option<String>,
    monitored: bool,
    predicate: WaitPredicate,
    deadline: Instant,
    my_user_id: String,
    sidecar: MessageSidecar,
    last_error: Arc<Mutex<Option<CoreError>>>,
    inner_cancel: CancellationToken,
    restored: Mutex<Vec<Observation>>,
}

impl Observer for PollObserver {
    type Bind = ChanvoyBind;

    async fn bind(
        &self,
        registration: &waitprims_core::Registration,
    ) -> waitprims_core::Result<Self::Bind> {
        let after = registration
            .start_anchor
            .as_ref()
            .map(|anchor| anchor.value.as_str().to_string());
        let (scan, baseline) = establish_baseline(
            &self.state,
            &self.channel,
            &self.channel_id,
            after.as_deref(),
            self.deadline,
        )
        .await
        .map_err(|err| {
            let _ = self
                .last_error
                .lock()
                .map(|mut slot| *slot = Some(classify_bind_core_error(err)));
            waitprims_core::ValidationError::new("/bind", "provider")
        })?;
        let (resolved_start, rest_baseline) = cursor_from_baseline(scan, baseline);
        Ok(ChanvoyBind {
            registration_id: registration.registration_id.clone(),
            subject_id: registration.subject_id.clone(),
            resolved_start,
            rest_baseline,
            release: Arc::new(LeaseRelease::noop()),
            inner_cancel: self.inner_cancel.clone(),
        })
    }

    async fn next(&self, bind: &Self::Bind) -> waitprims_core::Result<Observation> {
        if let Some(obs) = self.restored.lock().ok().and_then(|mut q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        }) {
            return Ok(obs);
        }
        let engine = async {
            if self.monitored {
                wait_push_from_cursor(
                    &self.state,
                    &self.channel,
                    &self.predicate,
                    self.after.as_deref(),
                    scan_cursor_from_bind(bind.resolved_start()),
                    self.deadline,
                )
                .await
            } else {
                wait_rest_from_cursor(
                    &self.state,
                    &self.channel,
                    &self.predicate,
                    scan_cursor_from_bind(bind.resolved_start()),
                    bind.rest_baseline.clone(),
                    self.deadline,
                )
                .await
            }
        };
        let result = tokio::select! {
            biased;
            _ = self.inner_cancel.cancelled() => return Ok(Observation::Idle),
            res = engine => res,
        };
        match result {
            Ok(wr) => {
                let Some(message) = wr.messages.into_iter().next() else {
                    return Ok(Observation::Idle);
                };
                match event_from_foreign_message(
                    &message,
                    &self.my_user_id,
                    bind.registration_id(),
                    &channel_subject(
                        bind.subject_id
                            .as_str()
                            .strip_prefix("channel:")
                            .unwrap_or(bind.subject_id.as_str()),
                    ),
                    bind.resolved_start(),
                    &self.sidecar,
                ) {
                    Ok(Some(event)) => Ok(Observation::Event(Box::new(event))),
                    Ok(None) => Ok(Observation::Idle),
                    Err(err) => {
                        let _ = self.last_error.lock().map(|mut slot| *slot = Some(err));
                        Ok(Observation::Failed {
                            reason_code: IdToken::new("sidecar_digest"),
                        })
                    }
                }
            }
            Err(CoreError::WaitTimeout(_)) => Ok(Observation::Idle),
            Err(err) => {
                let _ = self.last_error.lock().map(|mut slot| *slot = Some(err));
                Ok(Observation::Degraded {
                    reason_code: IdToken::new("provider_degraded"),
                })
            }
        }
    }

    async fn cancel(&self, _bind: &Self::Bind) -> waitprims_core::Result<()> {
        self.inner_cancel.cancel();
        Ok(())
    }

    fn poll_ready(&self, _bind: &Self::Bind) -> Option<Observation> {
        self.restored.lock().ok().and_then(|mut q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        })
    }

    fn restore_ready(&self, _bind: &Self::Bind, obs: Observation) -> waitprims_core::Result<()> {
        if matches!(obs, Observation::Idle) {
            return Ok(());
        }
        self.restored
            .lock()
            .map_err(|_| waitprims_core::ValidationError::new("/restore_ready", "lock_poisoned"))?
            .push(obs);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn poll_ack_retention_names_commit_rule() {
        assert!(poll_ack_retention_is_fail_closed());
        assert!(POLL_ACK_RETENTION.contains("must not silently advance"));
    }

    #[test]
    fn unacked_outcome_does_not_advance_store() {
        let profile = format!("poll-test-{}", std::process::id());
        let store = PollCursorStore::load(&profile);
        assert!(store.get("reg:poll:ch").is_none());
        let proposed = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("post-new"),
        };
        store.commit(BTreeMap::from([("reg:poll:ch".into(), proposed)]));
        let reloaded = PollCursorStore::load(&profile);
        assert_eq!(
            reloaded
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-new".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }
}
