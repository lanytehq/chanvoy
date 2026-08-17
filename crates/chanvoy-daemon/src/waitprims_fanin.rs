//! A2 fan-in: `wait_channels_v1` through one `run_first_match` set.
//!
//! Tie rule is waitprims `TIE_RULE`: same-instant winner is the earliest
//! arm in `registration_set.registrations` (request-arm order).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{
    validate_wait_channels_params, CoreError, WaitChannelSelector, WaitChannelsParams,
    WaitChannelsResult,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use waitprims_async::{run_first_match, BindHandle, Cancel, Observation, Observer, TIE_RULE};
use waitprims_core::{
    ActorRef, Anchor, AnchorKind, AuthnMode, BaselinePolicy, Canonicalization, CapabilityToken,
    DigestAlgorithm, IdToken, JcsDigest, LiveWaitOutcome, LiveWaitRequest, OpaqueRef, OutcomeKind,
    PredicateRef, Registration, RegistrationSet, WaitBound,
};

use crate::wait::{
    channel_is_monitored, drain_bus, establish_baseline, inbound_to_message, provider_retry,
    wait_push_from_cursor, wait_rest_from_cursor, WaitPredicate,
};
use crate::wait_owner::{WaitGuard, WaitSession};
use crate::waitprims_hold::{
    admit_set_and_request, admit_wait_result, authenticate_sidecar_message, channel_subject,
    classify_bind_core_error, contract_internal, cursor_from_baseline, event_from_foreign_message,
    map_waitprims_err, resolve_bind_cursor, scan_cursor_from_bind, timestamp_now, CancelForward,
    ChanvoyBind, LeaseRelease, MessageSidecar, WallClock, METHOD_ID,
};
use crate::AppState;

#[cfg(test)]
pub(crate) fn tie_rule_is_registration_order() -> bool {
    TIE_RULE.contains("registration_set.registrations")
}

struct FanArm {
    selector: WaitChannelSelector,
    channel_id: String,
    channel: String,
    after: Option<String>,
    prebound: Option<(Anchor, HashSet<String>)>,
    monitored: bool,
    predicate: WaitPredicate,
    retained: Vec<chanvoy_core::Message>,
}

struct MultiRelease {
    guards: Mutex<Vec<WaitGuard>>,
}

impl MultiRelease {
    fn new(guards: Vec<WaitGuard>) -> Arc<Self> {
        Arc::new(Self {
            guards: Mutex::new(guards),
        })
    }

    fn release(&self) {
        if let Ok(mut slot) = self.guards.lock() {
            slot.clear();
        }
    }
}

struct FanInObserver {
    state: AppState,
    arms: HashMap<String, FanArm>,
    deadline: Instant,
    my_user_id: String,
    sidecar: MessageSidecar,
    last_error: Arc<Mutex<Option<CoreError>>>,
    keys: Arc<MultiRelease>,
    session: WaitSession,
    inner_cancel: CancellationToken,
    observed: Mutex<HashSet<String>>,
    restored: Mutex<HashMap<String, VecDeque<Observation>>>,
}

pub(crate) async fn wait_channels_first_match(
    state: &AppState,
    params: WaitChannelsParams,
) -> Result<WaitChannelsResult, CoreError> {
    let _tie = TIE_RULE;
    let _ = _tie;
    validate_wait_channels_params(&params)?;
    WaitPredicate::compile(
        "pending",
        "pending",
        params.contains.as_deref(),
        params.pattern.as_deref(),
    )?;
    let deadline = Instant::now() + Duration::from_secs(params.timeout_secs);
    let mut rx = state.event_bus.subscribe();
    let mut bus = VecDeque::new();
    drain_bus(&mut rx, &mut bus, "fan-in")?;

    let mut resolved_keys = Vec::new();
    let mut arms = Vec::new();
    let mut seen = HashSet::new();
    for arm in &params.arms {
        let selector = arm.selector();
        let qualified = selector.qualified();
        let resolve = provider_retry(state, &qualified, deadline, || async {
            state.client.resolve_channel(&qualified, None).await
        });
        let resolved = crate::wait_channels::with_bus_drain(&mut rx, &mut bus, "fan-in", resolve)
            .await
            .map_err(|err| map_arm_err(&selector, err))?;
        if !seen.insert(resolved.channel_id.clone()) {
            return Err(CoreError::WaitFilterInvalid(format!(
                "duplicate wait arm {qualified} (same canonical channel as another arm)"
            )));
        }
        let baseline_fut = establish_baseline(
            state,
            &qualified,
            &resolved.channel_id,
            arm.after.as_deref(),
            deadline,
        );
        let (scan, mut baseline) =
            crate::wait_channels::with_bus_drain(&mut rx, &mut bus, "fan-in", baseline_fut)
                .await
                .map_err(|err| map_arm_err(&selector, err))?;
        drain_bus(&mut rx, &mut bus, "fan-in")?;
        let retained: Vec<chanvoy_core::Message> = bus
            .iter()
            .filter_map(|ev| match &ev.payload {
                chanvoy_core::DaemonEventPayloadInner::Inbound(p)
                    if p.channel_id == resolved.channel_id =>
                {
                    Some(inbound_to_message(p))
                }
                _ => None,
            })
            .collect();
        for msg in &retained {
            baseline.remove(&msg.id);
        }
        let prebound = Some(cursor_from_baseline(scan, baseline));
        let predicate = WaitPredicate::compile(
            &state.my_user_id,
            &resolved.channel_id,
            params.contains.as_deref(),
            params.pattern.as_deref(),
        )?;
        resolved_keys.push((
            resolved.channel_id.clone(),
            resolved.team_name.clone(),
            resolved.channel_name.clone(),
        ));
        arms.push(FanArm {
            selector,
            channel_id: resolved.channel_id,
            channel: qualified,
            after: arm.after.clone(),
            prebound,
            monitored: channel_is_monitored(state, &arm.selector().channel),
            predicate,
            retained,
        });
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let leases = state
        .wait_owners
        .acquire_all(&resolved_keys, remaining)
        .await?;
    state.wait_owners.note_arm();
    let mut guards = Vec::new();
    let mut session = None;
    let mut member_cancels = Vec::new();
    for lease in leases {
        let (sess, guard) = lease.into_guard();
        member_cancels.push(sess.cancel.clone());
        if session.is_none() {
            session = Some(sess);
        }
        guards.push(guard);
    }
    let session = session.expect("fan-in requires arms");
    let keys = MultiRelease::new(guards);

    let mut arm_map = HashMap::new();
    let mut subjects = Vec::new();
    for arm in arms {
        let subject = channel_subject(&arm.channel_id);
        subjects.push((
            subject.as_str().to_string(),
            arm.selector.clone(),
            arm.channel_id.clone(),
        ));
        arm_map.insert(subject.as_str().to_string(), arm);
    }

    let sidecar = MessageSidecar::new();
    let last_error = Arc::new(Mutex::new(None));
    let inner_cancel = CancellationToken::new();
    let clock = WallClock::new();
    let (set, request) = build_fanin_documents(
        &session,
        &state.my_user_id,
        &subjects,
        &params,
        clock.project_deadline(deadline),
    )?;
    let docs = admit_set_and_request("fan-in", set, request)?;
    let lease_cancel = session.cancel.clone();
    let observer = FanInObserver {
        state: state.clone(),
        arms: arm_map,
        deadline,
        my_user_id: state.my_user_id.clone(),
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        keys: Arc::clone(&keys),
        session,
        inner_cancel: inner_cancel.clone(),
        observed: Mutex::new(HashSet::new()),
        restored: Mutex::new(HashMap::new()),
    };
    let wp_cancel = Cancel::new();
    let _fwd = CancelForward::spawn(lease_cancel, wp_cancel.clone());
    let any_cancel = wp_cancel.clone();
    tokio::spawn(async move {
        let mut set = tokio::task::JoinSet::new();
        for token in member_cancels {
            set.spawn(async move {
                token.cancelled().await;
            });
        }
        let _ = set.join_next().await;
        any_cancel.trigger();
    });
    let outcome = run_first_match(&docs.set, &docs.request, &observer, &clock, &wp_cancel)
        .await
        .map_err(|err| map_waitprims_err("fan-in", err))?;
    let outcome = admit_wait_result(&docs, outcome)?;
    inner_cancel.cancel();
    keys.release();
    translate_fanin_outcome(outcome, &params, &sidecar, &last_error, &subjects)
}

fn map_arm_err(selector: &WaitChannelSelector, err: CoreError) -> CoreError {
    let q = selector.qualified();
    match err {
        CoreError::WaitFilterInvalid(message) if message.contains(&q) => {
            CoreError::WaitFilterInvalid(message)
        }
        CoreError::WaitFilterInvalid(message) => {
            CoreError::WaitFilterInvalid(format!("wait arm {q}: {message}"))
        }
        CoreError::WaitProviderDegraded { message, .. } => CoreError::WaitProviderDegraded {
            channel: q,
            message,
        },
        CoreError::WaitTimeout(_) => CoreError::WaitTimeout(q),
        CoreError::ChannelNotFoundInAnyTeam { .. } => CoreError::WaitFilterInvalid(format!(
            "wait arm {q}: resolve failed (channel not found)"
        )),
        CoreError::NotAMemberOfTeam { .. } => CoreError::WaitFilterInvalid(format!(
            "wait arm {q}: resolve failed (team not a member)"
        )),
        CoreError::AmbiguousChannel { .. } => {
            CoreError::WaitFilterInvalid(format!("wait arm {q}: resolve failed (ambiguous)"))
        }
        other => other,
    }
}

fn build_fanin_documents(
    session: &WaitSession,
    my_user_id: &str,
    subjects: &[(String, WaitChannelSelector, String)],
    params: &WaitChannelsParams,
    deadline: waitprims_core::Timestamp,
) -> Result<(RegistrationSet, LiveWaitRequest), CoreError> {
    let now = timestamp_now();
    let lease_expires = deadline.saturating_add(Duration::from_secs(3600));
    let waiter_id = IdToken::new(session.wait_id.clone());
    let set_id = IdToken::new(format!("regset:{}", session.wait_id));
    let revision = IdToken::new(format!("rev:{}", session.wait_id));
    let actor = ActorRef::new(format!("actor:{my_user_id}"));
    let capabilities = vec![CapabilityToken::new("contract: agent-wait/v0")];

    let mut registrations = Vec::new();
    for (idx, (subject, _, _)) in subjects.iter().enumerate() {
        let after = params.arms.get(idx).and_then(|arm| arm.after.as_deref());
        let (start_anchor, baseline_policy) = match after {
            Some(cursor) => (
                Some(Anchor {
                    kind: AnchorKind::ProviderOpaque,
                    value: IdToken::new(cursor),
                }),
                None,
            ),
            None => (None, Some(BaselinePolicy::Latest)),
        };
        registrations.push(Registration {
            registration_id: IdToken::new(format!("reg:{}:{idx}", session.wait_id)),
            method_id: IdToken::new(METHOD_ID),
            subject_kind: IdToken::new("channel"),
            subject_id: IdToken::new(subject.clone()),
            required: true,
            source_instance_ref: OpaqueRef::new("source:chanvoy-daemon"),
            predicate_ref: PredicateRef::new("pred:chanvoy-wait"),
            capability_ref: OpaqueRef::new("cap:wait"),
            lease_expires_at: lease_expires.clone(),
            bounds: WaitBound {
                max_events: 1,
                max_bytes: 1_048_576,
            },
            start_anchor,
            baseline_policy,
        });
    }
    let digest = registration_digest_all(&registrations)?;
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
            max_events: 1,
            max_bytes: 1_048_576,
        },
        registration_digest: JcsDigest {
            canonicalization: Canonicalization::Rfc8785,
            algorithm: DigestAlgorithm::Sha256,
            value: digest,
        },
        registrations,
    };
    let request = LiveWaitRequest {
        capabilities,
        message_id: IdToken::new(format!("req:{}", session.wait_id)),
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
    };
    Ok((set, request))
}

fn registration_digest_all(registrations: &[Registration]) -> Result<String, CoreError> {
    let json = serde_json::to_string(registrations).map_err(|err| {
        contract_internal("fan-in", format!("registration digest serialize: {err}"))
    })?;
    waitprims_core::registration_digest(&json)
        .map_err(|err| contract_internal("fan-in", format!("registration digest: {err}")))
}

fn translate_fanin_outcome(
    outcome: LiveWaitOutcome,
    params: &WaitChannelsParams,
    sidecar: &MessageSidecar,
    last_error: &Mutex<Option<CoreError>>,
    subjects: &[(String, WaitChannelSelector, String)],
) -> Result<WaitChannelsResult, CoreError> {
    let channels: Vec<WaitChannelSelector> = params.arms.iter().map(|a| a.selector()).collect();
    match outcome.outcome_kind {
        OutcomeKind::Events | OutcomeKind::Partial => {
            let event = outcome
                .events
                .and_then(|events| events.into_iter().next())
                .ok_or_else(|| contract_internal("fan-in", "events outcome missing event"))?;
            let message = sidecar
                .take(event.payload.payload_ref.as_str())
                .ok_or_else(|| {
                    contract_internal("fan-in", "matched wait event missing sidecar message")
                })?;
            authenticate_sidecar_message("fan-in", &message, &event.payload.content_digest)?;
            let subject = event.subject_id.as_str();
            let selector = subjects
                .iter()
                .find(|(s, _, _)| s == subject)
                .map(|(_, sel, _)| sel.clone())
                .ok_or_else(|| contract_internal("fan-in", "matched subject has no arm"))?;
            Ok(WaitChannelsResult::match_one(channels, selector, message))
        }
        OutcomeKind::LogicalDeadman | OutcomeKind::NoChange => {
            Err(CoreError::WaitTimeout("fan-in".into()))
        }
        OutcomeKind::Cancelled => Err(CoreError::WaitReplaced {
            wait_id: String::new(),
            replaced_by_wait_id: String::new(),
        }),
        _ => {
            if let Some(err) = last_error.lock().ok().and_then(|mut slot| slot.take()) {
                return Err(err);
            }
            Err(contract_internal("fan-in", "wait failed"))
        }
    }
}

impl FanInObserver {
    fn take_restored(&self, bind: &ChanvoyBind) -> Option<Observation> {
        let key = bind.registration_id().as_str();
        self.restored
            .lock()
            .ok()?
            .get_mut(key)
            .and_then(VecDeque::pop_front)
    }

    fn remember(&self, err: CoreError) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(err);
        }
    }
}

impl Observer for FanInObserver {
    type Bind = ChanvoyBind;

    async fn bind(
        &self,
        registration: &waitprims_core::Registration,
    ) -> waitprims_core::Result<Self::Bind> {
        let subject = registration.subject_id.as_str();
        let arm = self.arms.get(subject).ok_or_else(|| {
            waitprims_core::ValidationError::new("/subject_id", "unknown_fan_in_arm")
        })?;
        let (resolved_start, rest_baseline) = match &arm.prebound {
            Some(bound) => bound.clone(),
            None => resolve_bind_cursor(
                &self.state,
                &self.session,
                &arm.channel,
                &arm.channel_id,
                arm.after.as_deref(),
                self.deadline,
            )
            .await
            .map_err(|err| {
                self.remember(classify_bind_core_error(err));
                waitprims_core::ValidationError::new("/bind", "provider")
            })?,
        };
        let bind = ChanvoyBind {
            registration_id: registration.registration_id.clone(),
            subject_id: registration.subject_id.clone(),
            resolved_start: resolved_start.clone(),
            rest_baseline,
            release: Arc::new(LeaseRelease::noop()),
            inner_cancel: self.inner_cancel.clone(),
        };
        for message in &arm.retained {
            if !arm.predicate.matches_message(message) {
                continue;
            }
            if let Ok(Some(event)) = event_from_foreign_message(
                message,
                &self.my_user_id,
                &registration.registration_id,
                &registration.subject_id,
                &resolved_start,
                &self.sidecar,
            ) {
                let _ = self.restore_ready(&bind, Observation::Event(Box::new(event)));
            }
        }
        Ok(bind)
    }

    async fn next(&self, bind: &Self::Bind) -> waitprims_core::Result<Observation> {
        if let Some(obs) = self.take_restored(bind) {
            return Ok(obs);
        }
        let arm = match self.arms.get(bind.subject_id.as_str()) {
            Some(arm) => arm,
            None => {
                return Ok(Observation::Failed {
                    reason_code: IdToken::new("unknown_arm"),
                });
            }
        };
        {
            let mut seen = match self.observed.lock() {
                Ok(seen) => seen,
                Err(_) => {
                    return Ok(Observation::Failed {
                        reason_code: IdToken::new("observed_lock"),
                    });
                }
            };
            if !seen.insert(bind.registration_id().as_str().to_string()) {
                return Ok(Observation::Idle);
            }
        }
        let engine = async {
            self.state.wait_owners.note_provider_io();
            if arm.monitored {
                wait_push_from_cursor(
                    &self.state,
                    &arm.channel,
                    &arm.predicate,
                    arm.after.as_deref(),
                    scan_cursor_from_bind(bind.resolved_start()),
                    self.deadline,
                )
                .await
            } else {
                wait_rest_from_cursor(
                    &self.state,
                    &arm.channel,
                    &arm.predicate,
                    scan_cursor_from_bind(bind.resolved_start()),
                    bind.rest_baseline.clone(),
                    self.deadline,
                )
                .await
            }
        };
        let result = tokio::select! {
            biased;
            _ = self.inner_cancel.cancelled() => {
                return Ok(Observation::Idle);
            }
            res = engine => res,
        };
        Ok(observation_from_wait(
            &self.my_user_id,
            bind.registration_id(),
            &channel_subject(&arm.channel_id),
            bind.resolved_start(),
            &self.sidecar,
            &self.last_error,
            result,
        ))
    }

    async fn cancel(&self, _bind: &Self::Bind) -> waitprims_core::Result<()> {
        self.inner_cancel.cancel();
        self.keys.release();
        Ok(())
    }

    fn poll_ready(&self, bind: &Self::Bind) -> Option<Observation> {
        self.take_restored(bind)
    }

    fn restore_ready(&self, bind: &Self::Bind, obs: Observation) -> waitprims_core::Result<()> {
        if matches!(obs, Observation::Idle) {
            return Ok(());
        }
        let key = bind.registration_id().as_str().to_string();
        let mut slots = self
            .restored
            .lock()
            .map_err(|_| waitprims_core::ValidationError::new("/restore_ready", "lock_poisoned"))?;
        slots.entry(key).or_default().push_back(obs);
        Ok(())
    }
}

fn observation_from_wait(
    my_user_id: &str,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
    last_error: &Mutex<Option<CoreError>>,
    result: Result<chanvoy_core::WaitResult, CoreError>,
) -> Observation {
    match result {
        Ok(wr) => {
            let Some(message) = wr.messages.into_iter().next() else {
                return Observation::Idle;
            };
            match event_from_foreign_message(
                &message,
                my_user_id,
                registration_id,
                subject_id,
                start,
                sidecar,
            ) {
                Ok(Some(event)) => Observation::Event(Box::new(event)),
                Ok(None) => Observation::Idle,
                Err(err) => {
                    let _ = last_error.lock().map(|mut slot| *slot = Some(err));
                    Observation::Failed {
                        reason_code: IdToken::new("sidecar_digest"),
                    }
                }
            }
        }
        Err(CoreError::WaitTimeout(_)) => Observation::Idle,
        Err(err) => {
            let _ = last_error.lock().map(|mut slot| *slot = Some(err));
            Observation::Degraded {
                reason_code: IdToken::new("provider_degraded"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a2_tie_rule_is_registration_set_order() {
        assert!(tie_rule_is_registration_order());
        assert!(TIE_RULE.contains("earliest arm"));
    }

    #[test]
    fn fan_in_preserves_provider_and_deadman_classes() {
        let sel = WaitChannelSelector::new("org-lanytehq", "ops");
        let degraded = map_arm_err(
            &sel,
            CoreError::WaitProviderDegraded {
                channel: "other".into(),
                message: "deadline reached while provider observation was failing".into(),
            },
        );
        assert!(
            matches!(degraded, CoreError::WaitProviderDegraded { .. }),
            "provider must not become WaitFilterInvalid: {degraded:?}"
        );
        let timeout = map_arm_err(&sel, CoreError::WaitTimeout("other".into()));
        assert!(matches!(timeout, CoreError::WaitTimeout(_)));
    }
}
