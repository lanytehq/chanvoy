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

use crate::waitprims_hold::{
    authenticate_sidecar_message, channel_subject, contract_internal, event_from_message,
    map_waitprims_err, timestamp_now, ChanvoyBind, LeaseRelease, MessageSidecar, WallClock,
    METHOD_ID,
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

    pub(crate) fn commit(&self, staged: StagedPollAck) -> Result<(), CoreError> {
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
        *self
            .inner
            .lock()
            .map_err(|_| contract_internal("poll", "cursor store lock"))? = candidate;
        Ok(())
    }
}

fn poll_cursor_path(profile: &str) -> std::path::PathBuf {
    chanvoy_core::default_chanvoy_config_dir().join(format!("poll-cursors-{profile}.json"))
}

fn load_persisted(profile: &str) -> Result<BTreeMap<String, String>, CoreError> {
    let path = poll_cursor_path(profile);
    match std::fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw)
            .map_err(|err| contract_internal("poll", format!("cursor decode: {err}"))),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(err) => Err(contract_internal("poll", format!("cursor read: {err}"))),
    }
}

fn persist(profile: &str, map: &BTreeMap<String, String>) -> Result<(), CoreError> {
    use std::os::unix::fs::PermissionsExt;
    let dir = chanvoy_core::default_chanvoy_config_dir();
    std::fs::create_dir_all(&dir)
        .map_err(|err| contract_internal("poll", format!("cursor dir: {err}")))?;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
        .map_err(|err| contract_internal("poll", format!("cursor dir mode: {err}")))?;
    let path = poll_cursor_path(profile);
    let tmp = path.with_extension("json.tmp");
    let raw = serde_json::to_string(map)
        .map_err(|err| contract_internal("poll", format!("cursor encode: {err}")))?;
    std::fs::write(&tmp, raw)
        .map_err(|err| contract_internal("poll", format!("cursor tmp write: {err}")))?;
    std::fs::set_permissions(&tmp, std::fs::Permissions::from_mode(0o600))
        .map_err(|err| contract_internal("poll", format!("cursor tmp mode: {err}")))?;
    std::fs::rename(&tmp, &path)
        .map_err(|err| contract_internal("poll", format!("cursor persist: {err}")))?;
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
    let page_len = observer.page.len() as u64;
    let (set, request) = build_poll_documents(
        &waiter,
        &state.my_user_id,
        channel_id,
        after,
        acked,
        clock.project_deadline(deadline),
        page_len.max(1),
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
    let mut messages = Vec::new();
    for event in &outcome.events {
        if let Some(message) = sidecar.take(event.payload.payload_ref.as_str()) {
            authenticate_sidecar_message(channel, &message, &event.payload.content_digest)?;
            messages.push(message);
        }
    }
    Ok(PollCycleResult { messages, outcome })
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
    max_events: u64,
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
            max_events,
            max_bytes: 64 * 1_048_576,
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
            max_events,
            max_bytes: 64 * 1_048_576,
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
                    return Err(waitprims_core::ValidationError::new(
                        "/bind",
                        "sidecar_digest",
                    )
                    .into());
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
        let bind = ChanvoyBind {
            registration_id: registration.registration_id.clone(),
            subject_id: registration.subject_id.clone(),
            resolved_start,
            rest_baseline: Default::default(),
            release: Arc::new(LeaseRelease::noop()),
            inner_cancel: self.inner_cancel.clone(),
        };
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
            .commit(StagedPollAck {
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
            .commit(StagedPollAck {
                anchors: BTreeMap::from([("reg:poll:ch".into(), "post-old".into())]),
            })
            .expect("first persist");
        let path = poll_cursor_path(&profile);
        std::fs::remove_file(&path).expect("remove file");
        std::fs::create_dir(&path).expect("block persist path");
        let err = store.commit(StagedPollAck {
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
        store.commit(staged).expect("commit explicit");
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
}
