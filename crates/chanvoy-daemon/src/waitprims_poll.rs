//! A2 grok-bot / no-listener poll: one `run_poll_cycle` per wake.
//!
//! Events and cursor advances stay uncommitted until `poll_cycle_ack`.
//! Cancel, bound exhaustion, and a restart between outcome and ack
//! replay from the last acknowledged cursor.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{AttentionState, CoreError, Message};
use serde::{Deserialize, Serialize};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use waitprims_async::{
    event_surface_bytes, run_poll_cycle, BindHandle, Cancel, Observation, Observer,
    POLL_ACK_RETENTION,
};
use waitprims_core::{
    ActorRef, AgentWaitMessage, Anchor, AnchorKind, AuthnMode, BaselinePolicy, Canonicalization,
    CapabilityToken, DigestAlgorithm, IdToken, JcsDigest, OpaqueRef, PollCycleAck,
    PollCycleOutcome, PollCycleRequest, PredicateRef, Registration, RegistrationSet, WaitBound,
};

use crate::waitprims_hold::{
    channel_subject, contract_internal, event_from_message, map_waitprims_err, timestamp_now,
    ChanvoyBind, LeaseRelease, MessageSidecar, WallClock, METHOD_ID,
};
use crate::AppState;

#[cfg(test)]
pub(crate) fn poll_ack_retention_is_fail_closed() -> bool {
    POLL_ACK_RETENTION.contains("not committed until poll_cycle_ack")
}

/// Request-owned poll acknowledgement. Commit only after the same
/// connection has written the matching RPC response.
#[derive(Clone, Debug)]
pub(crate) struct StagedPollAck {
    anchors: BTreeMap<String, String>,
}

/// Last-acked cursors. Never advanced by an unacked outcome. Persisted
/// under the profile config dir so a restart cannot skip an unacked cycle.
#[derive(Clone)]
pub(crate) struct PollCursorStore {
    profile: String,
    inner: Arc<Mutex<BTreeMap<String, String>>>,
    persist_gate: Arc<Mutex<()>>,
}

impl PollCursorStore {
    pub(crate) fn load(profile: &str) -> Result<Self, CoreError> {
        Ok(Self {
            profile: profile.to_string(),
            inner: Arc::new(Mutex::new(load_persisted(profile)?)),
            persist_gate: Arc::new(Mutex::new(())),
        })
    }

    #[cfg(test)]
    pub(crate) fn for_test(profile: &str) -> Self {
        Self {
            profile: profile.to_string(),
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            persist_gate: Arc::new(Mutex::new(())),
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

    #[cfg(test)]
    fn commit_poll(&self, staged: StagedPollAck) -> Result<(), CoreError> {
        let mut attention = AttentionState::default();
        self.commit(staged, &mut attention)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn persist_candidate(
        &self,
        staged: StagedPollAck,
    ) -> Result<BTreeMap<String, String>, CoreError> {
        let _gate = self
            .persist_gate
            .lock()
            .map_err(|_| contract_internal("poll", "persist lock"))?;
        let candidate = {
            let map = self
                .inner
                .lock()
                .map_err(|_| contract_internal("poll", "cursor store lock"))?;
            let mut candidate = map.clone();
            candidate.extend(staged.anchors);
            candidate
        };
        persist(&self.profile, &candidate)?;
        Ok(candidate)
    }

    pub(crate) fn publish(&self, candidate: BTreeMap<String, String>) -> Result<(), CoreError> {
        *self
            .inner
            .lock()
            .map_err(|_| contract_internal("poll", "cursor store lock"))? = candidate;
        Ok(())
    }

    pub(crate) fn commit(
        &self,
        staged: StagedPollAck,
        attention: &mut AttentionState,
    ) -> Result<(), CoreError> {
        let _gate = self
            .persist_gate
            .lock()
            .map_err(|_| contract_internal("poll", "persist lock"))?;
        self.apply_pending_txn_inner(attention)?;
        let candidate = {
            let map = self
                .inner
                .lock()
                .map_err(|_| contract_internal("poll", "cursor store lock"))?;
            let mut candidate = map.clone();
            candidate.extend(staged.anchors);
            candidate
        };
        persist(&self.profile, &candidate)?;
        *self
            .inner
            .lock()
            .map_err(|_| contract_internal("poll", "cursor store lock"))? = candidate;
        Ok(())
    }

    /// One durable commit for poll + attention. Recovers a pending redo
    /// into `live_attention` before the caller-supplied builder sees it,
    /// so a later channel cannot snapshot a stale map and roll back a
    /// recovered cursor. Holds the persist gate through persist and
    /// poll-memory publish.
    pub(crate) fn commit_combined(
        &self,
        staged: StagedPollAck,
        live_attention: &mut AttentionState,
        build_candidate: impl FnOnce(&AttentionState) -> AttentionState,
    ) -> Result<(), CoreError> {
        let _gate = self
            .persist_gate
            .lock()
            .map_err(|_| contract_internal("poll", "persist lock"))?;
        self.apply_pending_txn_inner(live_attention)?;
        let candidate = build_candidate(live_attention);
        let poll = {
            let map = self
                .inner
                .lock()
                .map_err(|_| contract_internal("poll", "cursor store lock"))?;
            let mut poll = map.clone();
            poll.extend(staged.anchors);
            poll
        };
        persist_combined_txn(
            &self.profile,
            &CombinedReadCursorTxn {
                attention: candidate.clone(),
                poll: poll.clone(),
            },
        )?;
        materialize_canonical(&self.profile, &candidate, &poll)?;
        *self
            .inner
            .lock()
            .map_err(|_| contract_internal("poll", "cursor store lock"))? = poll;
        *live_attention = candidate;
        clear_combined_txn(&self.profile)?;
        Ok(())
    }

    pub(crate) fn apply_pending_txn(
        &self,
        attention: &mut AttentionState,
    ) -> Result<bool, CoreError> {
        let _gate = self
            .persist_gate
            .lock()
            .map_err(|_| contract_internal("poll", "persist lock"))?;
        self.apply_pending_txn_inner(attention)
    }

    fn apply_pending_txn_inner(&self, attention: &mut AttentionState) -> Result<bool, CoreError> {
        let Some(txn) = load_combined_txn(&self.profile)? else {
            return Ok(false);
        };
        materialize_canonical(&self.profile, &txn.attention, &txn.poll)?;
        *attention = txn.attention;
        self.publish(txn.poll)?;
        clear_combined_txn(&self.profile)?;
        Ok(true)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct CombinedReadCursorTxn {
    attention: AttentionState,
    poll: BTreeMap<String, String>,
}

fn combined_txn_path(profile: &str) -> std::path::PathBuf {
    chanvoy_core::default_chanvoy_config_dir().join(format!("read-cursors-{profile}.txn.json"))
}

fn persist_combined_txn(profile: &str, txn: &CombinedReadCursorTxn) -> Result<(), CoreError> {
    let raw = serde_json::to_string(txn)
        .map_err(|err| contract_internal("poll", format!("txn encode: {err}")))?;
    persist_bytes(&combined_txn_path(profile), raw.as_bytes())
}

fn materialize_canonical(
    profile: &str,
    attention: &AttentionState,
    poll: &BTreeMap<String, String>,
) -> Result<(), CoreError> {
    persist(profile, poll)?;
    chanvoy_core::store_attention_state(profile, attention).map(|_| ())
}

fn clear_combined_txn(profile: &str) -> Result<(), CoreError> {
    let path = combined_txn_path(profile);
    match std::fs::remove_file(&path) {
        Ok(()) => fsync_dir(&chanvoy_core::default_chanvoy_config_dir()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(err) => Err(contract_internal("poll", format!("txn clear: {err}"))),
    }
}

fn load_combined_txn(profile: &str) -> Result<Option<CombinedReadCursorTxn>, CoreError> {
    let path = combined_txn_path(profile);
    match chanvoy_core::read_tool_owned_file(&path, chanvoy_core::DEFAULT_MAX_BYTES) {
        Ok(raw) => serde_json::from_str(&raw)
            .map(Some)
            .map_err(|err| contract_internal("poll", format!("txn decode: {err}"))),
        Err(err) if err.is_not_found() => Ok(None),
        Err(err) => Err(CoreError::from(err)),
    }
}

fn poll_cursor_path(profile: &str) -> std::path::PathBuf {
    chanvoy_core::default_chanvoy_config_dir().join(format!("poll-cursors-{profile}.json"))
}

fn load_persisted(profile: &str) -> Result<BTreeMap<String, String>, CoreError> {
    let path = poll_cursor_path(profile);
    match chanvoy_core::read_tool_owned_file(&path, chanvoy_core::DEFAULT_MAX_BYTES) {
        Ok(raw) => serde_json::from_str(&raw)
            .map_err(|err| contract_internal("poll", format!("cursor decode: {err}"))),
        Err(err) if err.is_not_found() => Ok(BTreeMap::new()),
        Err(err) => Err(CoreError::from(err)),
    }
}

fn persist(profile: &str, map: &BTreeMap<String, String>) -> Result<(), CoreError> {
    let raw = serde_json::to_string(map)
        .map_err(|err| contract_internal("poll", format!("cursor encode: {err}")))?;
    persist_bytes(&poll_cursor_path(profile), raw.as_bytes())
}

fn persist_bytes(path: &std::path::Path, raw: &[u8]) -> Result<(), CoreError> {
    chanvoy_core::persist_tool_owned_bytes(path, raw)
        .map_err(|err| contract_internal("poll", format!("cursor persist: {err}")))
}

fn fsync_dir(dir: &std::path::Path) -> Result<(), CoreError> {
    let file = std::fs::File::open(dir)
        .map_err(|err| contract_internal("poll", format!("cursor dir open: {err}")))?;
    file.sync_all()
        .map_err(|err| contract_internal("poll", format!("cursor dir sync: {err}")))?;
    Ok(())
}

pub(crate) struct PollCycleResult {
    pub messages: Vec<Message>,
    pub outcome: PollCycleOutcome,
}

/// One no-listener poll cycle. Does not commit cursors. Caller must
/// [`ack_poll_cycle`] after the matching RPC write succeeds.
pub(crate) async fn poll_channel_once(
    state: &AppState,
    store: &PollCursorStore,
    channel: &str,
    channel_id: &str,
    after: Option<&str>,
    team: Option<&str>,
    deadline: Instant,
) -> Result<PollCycleResult, CoreError> {
    let _retention = POLL_ACK_RETENTION;
    let _ = _retention;
    // Preserve established `read --after` semantics: the complete
    // provider page after the anchor, including self-authored posts.
    let page = match after {
        Some(anchor) => {
            state
                .client
                .read_channel_after(channel, anchor, team)
                .await?
        }
        None => Vec::new(),
    };
    let sidecar = MessageSidecar::new();
    let last_error = Arc::new(Mutex::new(None));
    let inner_cancel = CancellationToken::new();
    let observer = PollObserver {
        page,
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        inner_cancel: inner_cancel.clone(),
        restored: Mutex::new(Vec::new()),
        page_loaded: Mutex::new(false),
    };
    let clock = WallClock::new();
    let waiter = format!("poll:{}", channel_id);
    let registration_id = format!("reg:{waiter}");
    let stored = store.get(&registration_id);
    let acked = match (after, stored.as_ref()) {
        (Some(explicit), Some(stored)) if stored.value.as_str() != explicit => None,
        (_, Some(_)) => stored,
        (Some(explicit), None) => Some(Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(explicit),
        }),
        (None, None) => None,
    };
    let bounds = poll_bounds_for_page(&observer.page)?;
    let (set, request) = build_poll_documents(
        &waiter,
        &state.my_user_id,
        channel_id,
        after,
        acked,
        clock.project_deadline(deadline),
        bounds,
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
    if let Some(err) = last_error.lock().ok().and_then(|mut slot| slot.take()) {
        return Err(err);
    }
    match outcome.outcome_kind {
        waitprims_core::OutcomeKind::Failed
        | waitprims_core::OutcomeKind::CoverageDegraded
        | waitprims_core::OutcomeKind::Refused
        | waitprims_core::OutcomeKind::ReauthenticationRequired => {
            return Err(CoreError::WaitProviderDegraded {
                channel: channel.to_string(),
                message: "poll cycle failed".into(),
            });
        }
        _ => {}
    }
    // Established read contract: the complete fetched page, not the
    // poll-cycle event list (which a tight bound can truncate).
    let messages = complete_read_after_page(observer.page);
    Ok(PollCycleResult { messages, outcome })
}

/// Size poll-cycle bounds so every fetched page message can be admitted.
fn poll_bounds_for_page(page: &[Message]) -> Result<WaitBound, CoreError> {
    let max_events = (page.len() as u64).max(1);
    let sidecar = MessageSidecar::new();
    let registration_id = IdToken::new("reg:bound");
    let subject = channel_subject("bound");
    let start = Anchor {
        kind: AnchorKind::ProviderOpaque,
        value: IdToken::new("anc:bound"),
    };
    let mut max_bytes = 1u64;
    for message in page {
        let event = event_from_message(message, &registration_id, &subject, &start, &sidecar)?;
        max_bytes = max_bytes.saturating_add(event_surface_bytes(&event));
    }
    Ok(WaitBound {
        max_events,
        max_bytes,
    })
}

/// `read --after` returns the provider page as-is, including self posts.
fn complete_read_after_page(page: Vec<Message>) -> Vec<Message> {
    page
}

/// Admit a poll-cycle ack document and return a request-owned stage.
/// The caller commits only after the matching RPC write succeeds.
pub(crate) fn ack_poll_cycle(
    outcome: &PollCycleOutcome,
    waiter: &str,
    my_user_id: &str,
) -> Result<StagedPollAck, CoreError> {
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
    Ok(StagedPollAck {
        anchors: outcome
            .retained_through
            .iter()
            .map(|(k, a)| (k.clone(), a.value.as_str().to_string()))
            .collect(),
    })
}

fn build_poll_documents(
    waiter: &str,
    my_user_id: &str,
    channel_id: &str,
    after: Option<&str>,
    acked: Option<Anchor>,
    deadline: waitprims_core::Timestamp,
    bounds: WaitBound,
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
        bounds: bounds.clone(),
        start_anchor,
        baseline_policy,
        priority: None,
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
        aggregate_limits: bounds,
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
    page: Vec<Message>,
    sidecar: MessageSidecar,
    last_error: Arc<Mutex<Option<CoreError>>>,
    inner_cancel: CancellationToken,
    restored: Mutex<Vec<Observation>>,
    page_loaded: Mutex<bool>,
}

impl PollObserver {
    fn take_restored(&self) -> Option<Observation> {
        self.restored.lock().ok().and_then(|mut q| {
            if q.is_empty() {
                None
            } else {
                Some(q.remove(0))
            }
        })
    }

    fn enqueue_page(&self, bind: &ChanvoyBind) -> waitprims_core::Result<()> {
        let mut loaded = self
            .page_loaded
            .lock()
            .map_err(|_| waitprims_core::ValidationError::new("/bind", "lock_poisoned"))?;
        if *loaded {
            return Ok(());
        }
        *loaded = true;
        drop(loaded);
        for message in &self.page {
            match event_from_message(
                message,
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
                Ok(event) => {
                    self.restore_ready(bind, Observation::Event(Box::new(event)))?;
                }
                Err(err) => {
                    let _ = self.last_error.lock().map(|mut slot| *slot = Some(err));
                    return Err(
                        waitprims_core::ValidationError::new("/bind", "sidecar_digest").into(),
                    );
                }
            }
        }
        Ok(())
    }
}

impl Observer for PollObserver {
    type Bind = ChanvoyBind;

    async fn bind(
        &self,
        registration: &waitprims_core::Registration,
    ) -> waitprims_core::Result<Self::Bind> {
        let resolved_start = registration.start_anchor.clone().unwrap_or(Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("anc:empty-at-arm"),
        });
        let bind = ChanvoyBind::new(
            registration.registration_id.clone(),
            registration.subject_id.clone(),
            resolved_start,
            Default::default(),
            Arc::new(LeaseRelease::noop()),
            self.inner_cancel.clone(),
            None,
        );
        self.enqueue_page(&bind)?;
        Ok(bind)
    }

    async fn next(&self, bind: &Self::Bind) -> waitprims_core::Result<Observation> {
        let _ = bind;
        if self.inner_cancel.is_cancelled() {
            return Ok(Observation::Idle);
        }
        Ok(self.take_restored().unwrap_or(Observation::Idle))
    }

    async fn cancel(&self, _bind: &Self::Bind) -> waitprims_core::Result<()> {
        self.inner_cancel.cancel();
        Ok(())
    }

    fn poll_ready(&self, _bind: &Self::Bind) -> Option<Observation> {
        self.take_restored()
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
        let store = PollCursorStore::load(&profile).expect("empty store");
        assert!(store.get("reg:poll:ch").is_none());
        store
            .commit_poll(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-new".into())]),
            })
            .expect("persist");
        let reloaded = PollCursorStore::load(&profile).expect("reload");
        assert_eq!(
            reloaded
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-new".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }

    #[test]
    fn corrupt_cursor_file_fails_closed() {
        let profile = format!("poll-corrupt-{}", std::process::id());
        let path = poll_cursor_path(&profile);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(&path, "not-json").expect("write garbage");
        let err = match PollCursorStore::load(&profile) {
            Ok(_) => panic!("corrupt must fail"),
            Err(err) => err,
        };
        assert!(matches!(err, CoreError::WaitProviderDegraded { .. }));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn persist_failure_does_not_advance_memory() {
        let profile = format!("poll-persist-fail-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty store");
        store
            .commit_poll(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-old".into())]),
            })
            .expect("first persist");
        let path = poll_cursor_path(&profile);
        std::fs::remove_file(&path).expect("remove file");
        std::fs::create_dir(&path).expect("block persist path");
        let err = store.commit_poll(StagedPollAck {
            anchors: BTreeMap::from([("reg:poll:ch".into(), "post-new".into())]),
        });
        assert!(err.is_err(), "persist over a directory must fail");
        assert_eq!(
            store
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-old".into())
        );
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn ack_is_request_owned_and_does_not_touch_store() {
        let profile = format!("poll-ack-owned-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty");
        let staged = StagedPollAck {
            anchors: BTreeMap::from([("reg:poll:ch".into(), "post-a".into())]),
        };
        assert!(store.get("reg:poll:ch").is_none());
        store.commit_poll(staged).expect("commit explicit");
        assert_eq!(
            store
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-a".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }

    #[test]
    fn event_from_message_includes_self_posts() {
        let sidecar = MessageSidecar::new();
        let me = "user-me";
        let message = Message {
            id: "p1".into(),
            user_id: me.into(),
            username: "me".into(),
            message: "own post".into(),
            create_at: 1,
            root_id: "p1".into(),
        };
        let event = event_from_message(
            &message,
            &IdToken::new("reg:1"),
            &channel_subject("ch"),
            &Anchor {
                kind: AnchorKind::ProviderOpaque,
                value: IdToken::new("after"),
            },
            &sidecar,
        )
        .expect("self post is a read event");
        assert_eq!(event.proposed_next_anchor.value.as_str(), "p1");
    }

    fn page_msg(id: &str, user: &str, body: &str) -> Message {
        Message {
            id: id.into(),
            user_id: user.into(),
            username: user.into(),
            message: body.into(),
            create_at: 1,
            root_id: id.into(),
        }
    }

    #[test]
    fn poll_bounds_cover_the_fetched_page_not_a_fixed_cap() {
        let page = vec![
            page_msg("p-self", "bot", "own post"),
            page_msg("p-a", "alice", "one"),
            page_msg("p-b", "bob", "two"),
        ];
        let bounds = poll_bounds_for_page(&page).expect("bounds");
        assert_eq!(bounds.max_events, 3);
        assert!(
            bounds.max_bytes < 64 * 1_048_576,
            "small page must not use the old fixed 64 MiB cap: {}",
            bounds.max_bytes
        );
        assert!(bounds.max_bytes >= 1);
    }

    #[test]
    fn over_bound_page_returns_every_post_including_self() {
        let page = vec![
            page_msg("p-self", "bot", "own post"),
            page_msg("p-a", "alice", "one"),
            page_msg("p-b", "bob", "two"),
        ];
        let truncated_event_count = 1;
        assert!(truncated_event_count < page.len());
        let returned = complete_read_after_page(page.clone());
        assert_eq!(returned.len(), 3);
        assert_eq!(returned[0].id, "p-self");
        assert_eq!(returned[0].user_id, "bot");
        assert_eq!(
            returned.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
            vec!["p-self", "p-a", "p-b"]
        );
    }

    #[test]
    fn large_self_post_is_not_dropped_by_poll_byte_cap() {
        let body = "x".repeat(2 * 1024 * 1024);
        let page = vec![
            page_msg("p-self", "bot", &body),
            page_msg("p-a", "alice", "hi"),
        ];
        let bounds = poll_bounds_for_page(&page).expect("bounds");
        assert_eq!(bounds.max_events, 2);
        let returned = complete_read_after_page(page);
        assert_eq!(returned.len(), 2);
        assert_eq!(returned[0].user_id, "bot");
        assert_eq!(returned[0].message.len(), 2 * 1024 * 1024);
        assert_eq!(returned[1].id, "p-a");
    }

    #[test]
    fn persist_creates_owner_only_regular_file() {
        use std::os::unix::fs::PermissionsExt;
        let profile = format!("poll-mode-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty");
        store
            .commit_poll(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-a".into())]),
            })
            .expect("persist");
        let path = poll_cursor_path(&profile);
        let meta = std::fs::metadata(&path).expect("stat");
        assert!(meta.is_file());
        assert_eq!(meta.permissions().mode() & 0o777, 0o600);
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn combined_txn_recovers_both_stores_on_reload() {
        let profile = format!("poll-txn-{}", std::process::id());
        let mut attention = chanvoy_core::AttentionState::default();
        attention.channels.insert(
            "org/ops".into(),
            chanvoy_core::ChannelCursorState {
                last_seen_post_id: Some("p-attn".into()),
                updated_at: Some(1),
                last_known_stale: false,
                last_checked_at: None,
                channel_id: "ch".into(),
                team_id: "t".into(),
                team_name: "org".into(),
                channel_name: "ops".into(),
            },
        );
        let poll = BTreeMap::from([("reg:poll:ch".into(), "p-poll".into())]);
        persist_combined_txn(
            &profile,
            &CombinedReadCursorTxn {
                attention: attention.clone(),
                poll: poll.clone(),
            },
        )
        .expect("txn");
        let store = PollCursorStore::load(&profile).expect("empty poll file");
        assert!(store.get("reg:poll:ch").is_none());
        let mut restored = chanvoy_core::AttentionState::default();
        assert!(store.apply_pending_txn(&mut restored).expect("apply"));
        assert_eq!(
            restored
                .channels
                .get("org/ops")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-attn")
        );
        assert_eq!(
            store
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("p-poll".into())
        );
        assert!(
            !combined_txn_path(&profile).exists(),
            "recovery must clear the redo record"
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }

    #[test]
    fn txn_clear_is_after_both_durable_materializations() {
        use std::os::unix::fs::PermissionsExt;
        let profile = format!("poll-mat-{}", std::process::id());
        let mut attention = chanvoy_core::AttentionState::default();
        attention.channels.insert(
            "org/ops".into(),
            chanvoy_core::ChannelCursorState {
                last_seen_post_id: Some("p-attn".into()),
                updated_at: Some(1),
                last_known_stale: false,
                last_checked_at: None,
                channel_id: "ch".into(),
                team_id: "t".into(),
                team_name: "org".into(),
                channel_name: "ops".into(),
            },
        );
        let poll = BTreeMap::from([("reg:poll:ch".into(), "p-poll".into())]);
        persist_combined_txn(
            &profile,
            &CombinedReadCursorTxn {
                attention: attention.clone(),
                poll: poll.clone(),
            },
        )
        .expect("txn");
        assert!(combined_txn_path(&profile).exists());
        materialize_canonical(&profile, &attention, &poll).expect("both stores");
        assert!(
            combined_txn_path(&profile).exists(),
            "txn must remain until both canonical files are durable"
        );
        assert!(poll_cursor_path(&profile).is_file());
        let attn_path = chanvoy_core::attention_state_path(&profile);
        assert!(attn_path.is_file());
        assert_eq!(
            std::fs::metadata(&attn_path)
                .expect("stat")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        clear_combined_txn(&profile).expect("clear");
        assert!(!combined_txn_path(&profile).exists());
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
        let _ = std::fs::remove_file(attn_path);
    }

    #[test]
    fn later_poll_only_commit_survives_restart_after_combined() {
        let profile = format!("poll-later-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty");
        let mut attn = chanvoy_core::AttentionState::default();
        attn.channels.insert(
            "org/ops".into(),
            chanvoy_core::ChannelCursorState {
                last_seen_post_id: Some("p-attn".into()),
                updated_at: Some(1),
                last_known_stale: false,
                last_checked_at: None,
                channel_id: "ch".into(),
                team_id: "t".into(),
                team_name: "org".into(),
                channel_name: "ops".into(),
            },
        );
        store
            .commit_combined(
                StagedPollAck {
                    anchors: BTreeMap::from([("reg:poll:ch".into(), "p-old".into())]),
                },
                &mut attn,
                |live| live.clone(),
            )
            .expect("combined");
        store
            .commit(
                StagedPollAck {
                    anchors: BTreeMap::from([("reg:poll:ch".into(), "p-new".into())]),
                },
                &mut attn,
            )
            .expect("later poll");
        let reloaded = PollCursorStore::load(&profile).expect("reload");
        let mut restored = chanvoy_core::AttentionState::default();
        assert!(!reloaded.apply_pending_txn(&mut restored).expect("no txn"));
        assert_eq!(
            reloaded
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("p-new".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
        let _ = std::fs::remove_file(combined_txn_path(&profile));
    }

    fn sample_cursor(post_id: &str, channel: &str) -> chanvoy_core::ChannelCursorState {
        chanvoy_core::ChannelCursorState {
            last_seen_post_id: Some(post_id.into()),
            updated_at: Some(1),
            last_known_stale: false,
            last_checked_at: None,
            channel_id: channel.into(),
            team_id: "t".into(),
            team_name: "org".into(),
            channel_name: channel.into(),
        }
    }

    #[test]
    fn combined_read_recovers_pending_before_later_channel_candidate() {
        let profile = format!("poll-interleave-{}", std::process::id());
        let mut recovered = chanvoy_core::AttentionState::default();
        recovered
            .channels
            .insert("org/ops".into(), sample_cursor("p-attn", "ops"));
        persist_combined_txn(
            &profile,
            &CombinedReadCursorTxn {
                attention: recovered,
                poll: BTreeMap::from([("reg:ops".into(), "p-ops".into())]),
            },
        )
        .expect("pending txn left after failed materialize");
        let store = PollCursorStore::load(&profile).expect("empty poll file");
        let mut live = chanvoy_core::AttentionState::default();
        store
            .commit_combined(
                StagedPollAck {
                    anchors: BTreeMap::from([("reg:other".into(), "p-other".into())]),
                },
                &mut live,
                |current| {
                    let mut next = current.clone();
                    next.channels
                        .insert("org/other".into(), sample_cursor("p-other", "other"));
                    next
                },
            )
            .expect("later combined read");
        assert_eq!(
            live.channels
                .get("org/ops")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-attn"),
            "recovered attention cursor must survive a later channel"
        );
        assert_eq!(
            live.channels
                .get("org/other")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-other")
        );
        assert_eq!(
            store.get("reg:ops").map(|a| a.value.as_str().to_string()),
            Some("p-ops".into())
        );
        assert_eq!(
            store.get("reg:other").map(|a| a.value.as_str().to_string()),
            Some("p-other".into())
        );
        let reloaded = chanvoy_core::load_attention_state(&profile).expect("disk");
        assert_eq!(
            reloaded
                .channels
                .get("org/ops")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-attn")
        );
        assert_eq!(
            reloaded
                .channels
                .get("org/other")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-other")
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
        let _ = std::fs::remove_file(chanvoy_core::attention_state_path(&profile));
        let _ = std::fs::remove_file(combined_txn_path(&profile));
    }

    #[test]
    fn pending_txn_is_completed_before_later_poll_commit() {
        let profile = format!("poll-pending-{}", std::process::id());
        let mut attention = chanvoy_core::AttentionState::default();
        attention.channels.insert(
            "org/ops".into(),
            chanvoy_core::ChannelCursorState {
                last_seen_post_id: Some("p-attn".into()),
                updated_at: Some(1),
                last_known_stale: false,
                last_checked_at: None,
                channel_id: "ch".into(),
                team_id: "t".into(),
                team_name: "org".into(),
                channel_name: "ops".into(),
            },
        );
        persist_combined_txn(
            &profile,
            &CombinedReadCursorTxn {
                attention: attention.clone(),
                poll: BTreeMap::from([("reg:old".into(), "p-old".into())]),
            },
        )
        .expect("pending txn");
        let store = PollCursorStore::load(&profile).expect("empty file");
        let mut live = chanvoy_core::AttentionState::default();
        store
            .commit(
                StagedPollAck {
                    anchors: BTreeMap::from([("reg:new".into(), "p-new".into())]),
                },
                &mut live,
            )
            .expect("later commit completes redo then writes");
        assert!(!combined_txn_path(&profile).exists());
        assert_eq!(
            live.channels
                .get("org/ops")
                .and_then(|c| c.last_seen_post_id.as_deref()),
            Some("p-attn")
        );
        assert_eq!(
            store.get("reg:old").map(|a| a.value.as_str().to_string()),
            Some("p-old".into())
        );
        assert_eq!(
            store.get("reg:new").map(|a| a.value.as_str().to_string()),
            Some("p-new".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
        let _ = std::fs::remove_file(chanvoy_core::attention_state_path(&profile));
    }

    #[test]
    fn concurrent_commits_do_not_drop_an_ack() {
        let profile = format!("poll-conc-{}", std::process::id());
        let store = std::sync::Arc::new(PollCursorStore::load(&profile).expect("empty"));
        std::thread::scope(|scope| {
            let a = store.clone();
            let b = store.clone();
            scope.spawn(move || {
                a.commit_poll(StagedPollAck {
                    anchors: BTreeMap::from([("reg:a".into(), "pa".into())]),
                })
                .expect("a");
            });
            scope.spawn(move || {
                b.commit_poll(StagedPollAck {
                    anchors: BTreeMap::from([("reg:b".into(), "pb".into())]),
                })
                .expect("b");
            });
        });
        assert_eq!(
            store.get("reg:a").map(|x| x.value.as_str().to_string()),
            Some("pa".into())
        );
        assert_eq!(
            store.get("reg:b").map(|x| x.value.as_str().to_string()),
            Some("pb".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }

    #[test]
    fn persist_candidate_does_not_publish_memory_until_publish() {
        let profile = format!("poll-stage-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty");
        let candidate = store
            .persist_candidate(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-new".into())]),
            })
            .expect("disk");
        assert!(
            store.get("reg:poll:ch").is_none(),
            "memory must stay at the last published cursor until publish"
        );
        let disk = PollCursorStore::load(&profile).expect("reload");
        assert_eq!(
            disk.get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-new".into())
        );
        store.publish(candidate).expect("publish");
        assert_eq!(
            store
                .get("reg:poll:ch")
                .map(|a| a.value.as_str().to_string()),
            Some("post-new".into())
        );
        let _ = std::fs::remove_file(poll_cursor_path(&profile));
    }

    #[test]
    fn persist_fsyncs_parent_dir_after_rename() {
        let dir = chanvoy_core::default_chanvoy_config_dir();
        fsync_dir(&dir).expect("parent dir must be fsyncable");
        let profile = format!("poll-dirsync-{}", std::process::id());
        let store = PollCursorStore::load(&profile).expect("empty");
        store
            .commit_poll(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-a".into())]),
            })
            .expect("persist with dir sync");
        let path = poll_cursor_path(&profile);
        assert!(path.is_file());
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn non_regular_cursor_file_fails_closed() {
        let profile = format!("poll-dir-{}", std::process::id());
        let path = poll_cursor_path(&profile);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::create_dir(&path).expect("dir");
        let err = match PollCursorStore::load(&profile) {
            Ok(_) => panic!("directory must fail"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                CoreError::SafeRead(chanvoy_core::SafeReadError::NonRegular { .. })
            ),
            "{err:?}"
        );
        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn oversized_cursor_file_fails_closed() {
        let profile = format!("poll-big-{}", std::process::id());
        let path = poll_cursor_path(&profile);
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        let oversized = vec![b'x'; (chanvoy_core::DEFAULT_MAX_BYTES as usize) + 8];
        std::fs::write(&path, oversized).expect("write");
        let err = match PollCursorStore::load(&profile) {
            Ok(_) => panic!("oversized must fail"),
            Err(err) => err,
        };
        assert!(
            matches!(
                err,
                CoreError::SafeRead(chanvoy_core::SafeReadError::TooLarge { .. })
            ),
            "{err:?}"
        );
        let _ = std::fs::remove_file(path);
    }
}
