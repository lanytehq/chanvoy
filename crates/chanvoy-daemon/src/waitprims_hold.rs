//! A1: drive single-channel `wait_channel_v3` through
//! `waitprims_async::run_first_match`.
//!
//! The adapter lives only in this crate. `chanvoy-core` public types and
//! `chanvoy-mcp` take no waitprims dependency. Legacy `wait_channel` /
//! `wait_channel_v2` and fan-in (`wait_channels_v1`) stay on their
//! established engines. Observation uses the existing Mattermost client
//! (REST) from a bind-resolved cursor — no per-wait event-bus subscribe.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{CoreError, Message, WaitResult};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use waitprims_async::{run_first_match, BindHandle, Cancel, Clock, Observation, Observer};
use waitprims_core::{
    ActorRef, Anchor, AnchorKind, AuthnMode, BaselinePolicy, Canonicalization, CapabilityToken,
    ContentDigest, DigestAlgorithm, IdToken, JcsDigest, LiveWaitOutcome, LiveWaitRequest,
    OpaqueRef, OutcomeKind, PayloadRef, PredicateRef, Registration, RegistrationSet, ReplayStatus,
    Timestamp, WaitBound, WaitEvent,
};

use crate::wait::{establish_baseline, one_message_result, wait_rest_from_cursor, WaitPredicate};
use crate::wait_owner::{WaitGuard, WaitSession};
use crate::AppState;

/// Public method_id on the waitprims registration.
pub(crate) const METHOD_ID: &str = "chanvoy_wait";
const EMPTY_AT_ARM_CURSOR: &str = "anc:empty-at-arm";
const REASON_OVERFLOW: &str = "buffer_overflow";
const REASON_CURSOR: &str = "cursor_uncertain";
const REASON_DEGRADED: &str = "provider_degraded";
const REASON_REPLACED: &str = "replaced";

pub(crate) struct FirstMatchWait<'a> {
    pub channel: &'a str,
    pub after: Option<&'a str>,
    pub predicate: WaitPredicate,
    pub deadline: Instant,
    pub session: &'a WaitSession,
    pub guard: WaitGuard,
}

/// Run one `run_first_match` registration and translate the admitted
/// `live_wait_outcome` back to the existing `WaitResult` / `CoreError` surface.
pub(crate) async fn run_single_channel_first_match(
    state: &AppState,
    wait: FirstMatchWait<'_>,
) -> Result<WaitResult, CoreError> {
    let release = Arc::new(LeaseRelease::new(wait.guard));
    let sidecar = MessageSidecar::new();
    let last_error = Arc::new(Mutex::new(None));
    let inner_cancel = CancellationToken::new();
    let observer = ChanvoyWaitObserver {
        state: state.clone(),
        channel: wait.channel.to_string(),
        after: wait.after.map(str::to_string),
        predicate: wait.predicate,
        deadline: wait.deadline,
        my_user_id: state.my_user_id.clone(),
        registration_id: IdToken::new(format!("reg:{}", wait.session.wait_id)),
        subject_id: IdToken::new(format!("channel:{}", wait.channel)),
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        release: Arc::clone(&release),
        inner_cancel: inner_cancel.clone(),
        observed: AtomicBool::new(false),
    };

    let (set, request) = build_live_documents(
        wait.session,
        &state.my_user_id,
        wait.channel,
        wait.after,
        deadline_from_instant(wait.deadline),
    )?;

    let clock = WallClock;
    let wp_cancel = Cancel::new();
    {
        let lease_cancel = wait.session.cancel.clone();
        let wp = wp_cancel.clone();
        tokio::spawn(async move {
            lease_cancel.cancelled().await;
            wp.trigger();
        });
    }

    let outcome = run_first_match(&set, &request, &observer, &clock, &wp_cancel)
        .await
        .map_err(map_waitprims_err)?;

    inner_cancel.cancel();
    release.release();
    translate_outcome(outcome, wait.channel, &sidecar, wait.session, &last_error)
}

/// Production clock. Does not depend on `waitprims-testkit`.
pub(crate) struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> Timestamp {
        timestamp_now()
    }

    fn sleep_until(&self, deadline: &Timestamp) -> impl std::future::Future<Output = ()> + Send {
        let delay = timestamp_now().duration_until(deadline);
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// Convert the RPC Instant deadline into a waitprims timestamp using
/// **remaining** budget so pre-bind work cannot extend the caller's timeout.
pub(crate) fn deadline_from_instant(deadline: Instant) -> Timestamp {
    timestamp_now().saturating_add(deadline.saturating_duration_since(Instant::now()))
}

pub(crate) fn timestamp_now() -> Timestamp {
    let raw = chrono::Utc::now()
        .format("%Y-%m-%dT%H:%M:%S%.6fZ")
        .to_string();
    Timestamp::parse(&raw).unwrap_or_else(|_| {
        Timestamp::parse("1970-01-01T00:00:00Z").expect("epoch is a valid RFC3339 timestamp")
    })
}

/// Idempotent lease release owned by the bind handle. Drop of the handle
/// (or an explicit cancel) releases the PER-040 key; a second call is a no-op.
pub(crate) struct LeaseRelease {
    guard: Mutex<Option<WaitGuard>>,
}

impl LeaseRelease {
    pub(crate) fn new(guard: WaitGuard) -> Self {
        Self {
            guard: Mutex::new(Some(guard)),
        }
    }

    pub(crate) fn release(&self) {
        if let Ok(mut slot) = self.guard.lock() {
            slot.take();
        }
    }

    #[cfg(test)]
    pub(crate) fn is_released(&self) -> bool {
        self.guard.lock().map(|slot| slot.is_none()).unwrap_or(true)
    }
}

impl Drop for LeaseRelease {
    fn drop(&mut self) {
        self.release();
    }
}

/// Daemon-local sidecar: opaque payload_ref → already-observed `Message`.
/// A match reconstructs `WaitResult` from this map; it does not refetch.
#[derive(Clone, Default)]
pub(crate) struct MessageSidecar {
    inner: Arc<Mutex<HashMap<String, Message>>>,
}

impl MessageSidecar {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn store(&self, payload_ref: &str, message: Message) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(payload_ref.to_string(), message);
        }
    }

    pub(crate) fn take(&self, payload_ref: &str) -> Option<Message> {
        self.inner
            .lock()
            .ok()
            .and_then(|mut map| map.remove(payload_ref))
    }
}

pub(crate) fn payload_ref_for(message: &Message) -> String {
    format!("msg:{}", message.id)
}

pub(crate) fn content_digest_for(message: &Message) -> ContentDigest {
    let mut hasher = Sha256::new();
    hasher.update(message.id.as_bytes());
    hasher.update([0]);
    hasher.update(message.user_id.as_bytes());
    hasher.update([0]);
    hasher.update(message.message.as_bytes());
    hasher.update(message.create_at.to_le_bytes());
    ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        value: hex_lower(&hasher.finalize()),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Filter self-authored posts before constructing `Observation::Event`.
pub(crate) fn event_from_foreign_message(
    message: &Message,
    my_user_id: &str,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
) -> Option<WaitEvent> {
    if message.user_id == my_user_id {
        return None;
    }
    let payload_ref = payload_ref_for(message);
    sidecar.store(&payload_ref, message.clone());
    let observed = timestamp_now();
    Some(WaitEvent {
        event_id: IdToken::new(format!("evt:{}", message.id)),
        registration_id: registration_id.clone(),
        source_instance_ref: OpaqueRef::new("source:chanvoy-daemon"),
        method_id: IdToken::new(METHOD_ID),
        subject_kind: IdToken::new("channel"),
        subject_id: subject_id.clone(),
        occurred_at: observed.clone(),
        observed_at: observed,
        start_anchor: start.clone(),
        proposed_next_anchor: Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(message.id.clone()),
        },
        replay_status: ReplayStatus::Fresh,
        correlation_id: IdToken::new(format!("corr:{}", message.id)),
        causation_id: None,
        payload: PayloadRef {
            payload_ref: OpaqueRef::new(payload_ref),
            content_digest: content_digest_for(message),
            media_type: None,
        },
        delivery_ref: None,
        activation_ref: None,
    })
}

pub(crate) fn translate_outcome(
    outcome: LiveWaitOutcome,
    channel: &str,
    sidecar: &MessageSidecar,
    session: &WaitSession,
    last_error: &Mutex<Option<CoreError>>,
) -> Result<WaitResult, CoreError> {
    match outcome.outcome_kind {
        OutcomeKind::Events | OutcomeKind::Partial => {
            let event = outcome
                .events
                .and_then(|events| events.into_iter().next())
                .ok_or_else(|| CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "waitprims events outcome missing event".into(),
                })?;
            let key = event.payload.payload_ref.as_str();
            let message = sidecar
                .take(key)
                .ok_or_else(|| CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "matched wait event missing sidecar message".into(),
                })?;
            Ok(one_message_result(channel, message))
        }
        OutcomeKind::LogicalDeadman | OutcomeKind::NoChange => {
            Err(CoreError::WaitTimeout(channel.to_string()))
        }
        OutcomeKind::Cancelled => Err(CoreError::WaitReplaced {
            wait_id: session.wait_id.clone(),
            replaced_by_wait_id: session.replaced_by_id(),
        }),
        OutcomeKind::Failed
        | OutcomeKind::CoverageDegraded
        | OutcomeKind::Refused
        | OutcomeKind::ReauthenticationRequired => {
            if let Some(err) = last_error.lock().ok().and_then(|mut slot| slot.take()) {
                return Err(err);
            }
            let reason = outcome
                .reason_code
                .as_ref()
                .map(IdToken::as_str)
                .unwrap_or("failed");
            Err(named_failure(channel, reason))
        }
    }
}

pub(crate) fn named_failure(channel: &str, reason: &str) -> CoreError {
    match reason {
        REASON_OVERFLOW => CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "wait buffer overflow".into(),
        },
        REASON_CURSOR => CoreError::WaitFilterInvalid("wait cursor uncertain".into()),
        REASON_DEGRADED => CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "deadline reached while provider observation was failing".into(),
        },
        REASON_REPLACED => CoreError::WaitReplaced {
            wait_id: String::new(),
            replaced_by_wait_id: String::new(),
        },
        other => CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: format!("wait failed: {other}"),
        },
    }
}

fn map_waitprims_err(err: waitprims_core::Error) -> CoreError {
    CoreError::WaitFilterInvalid(format!("waitprims request: {err}"))
}

pub(crate) fn build_live_documents(
    session: &WaitSession,
    my_user_id: &str,
    channel: &str,
    after_cursor: Option<&str>,
    deadline: Timestamp,
) -> Result<(RegistrationSet, LiveWaitRequest), CoreError> {
    let now = timestamp_now();
    let lease_expires = deadline.saturating_add(Duration::from_secs(3600));
    let waiter_id = IdToken::new(session.wait_id.clone());
    let set_id = IdToken::new(format!("regset:{}", session.wait_id));
    let revision = IdToken::new(format!("rev:{}", session.wait_id));
    let request_id = IdToken::new(format!("req:{}", session.wait_id));
    let actor = ActorRef::new(format!("actor:{my_user_id}"));
    let capabilities = vec![CapabilityToken::new("contract: agent-wait/v0")];
    let registration_id = IdToken::new(format!("reg:{}", session.wait_id));

    let (start_anchor, baseline_policy) = match after_cursor {
        Some(cursor) => (
            Some(Anchor {
                kind: AnchorKind::ProviderOpaque,
                value: IdToken::new(cursor),
            }),
            None,
        ),
        None => (None, Some(BaselinePolicy::Latest)),
    };

    let registration = Registration {
        registration_id,
        method_id: IdToken::new(METHOD_ID),
        subject_kind: IdToken::new("channel"),
        subject_id: IdToken::new(format!("channel:{channel}")),
        required: true,
        source_instance_ref: OpaqueRef::new("source:chanvoy-daemon"),
        predicate_ref: PredicateRef::new("pred:chanvoy-wait"),
        capability_ref: OpaqueRef::new("cap:wait"),
        lease_expires_at: lease_expires,
        bounds: WaitBound {
            max_events: 1,
            max_bytes: 1_048_576,
        },
        start_anchor,
        baseline_policy,
    };

    let digest = registration_digest_hex(&registration)?;

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
        registrations: vec![registration],
    };

    let request = LiveWaitRequest {
        capabilities,
        message_id: request_id,
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

pub(crate) fn registration_digest_hex(registration: &Registration) -> Result<String, CoreError> {
    let json = serde_json::to_string(std::slice::from_ref(registration)).map_err(|err| {
        CoreError::WaitFilterInvalid(format!("registration digest serialize: {err}"))
    })?;
    waitprims_core::registration_digest(&json)
        .map_err(|err| CoreError::WaitFilterInvalid(format!("registration digest: {err}")))
}

/// Map a bind-resolved exclusive cursor to the REST scan start.
/// The synthetic empty-at-arm token is not a provider post id.
pub(crate) fn scan_cursor_from_bind(start: &Anchor) -> Option<String> {
    let value = start.value.as_str();
    if value == EMPTY_AT_ARM_CURSOR {
        None
    } else {
        Some(value.to_string())
    }
}

struct ChanvoyWaitObserver {
    state: AppState,
    channel: String,
    after: Option<String>,
    predicate: WaitPredicate,
    deadline: Instant,
    my_user_id: String,
    registration_id: IdToken,
    subject_id: IdToken,
    sidecar: MessageSidecar,
    last_error: Arc<Mutex<Option<CoreError>>>,
    release: Arc<LeaseRelease>,
    inner_cancel: CancellationToken,
    observed: AtomicBool,
}

struct ChanvoyBind {
    registration_id: IdToken,
    resolved_start: Anchor,
    rest_baseline: HashSet<String>,
    release: Arc<LeaseRelease>,
    inner_cancel: CancellationToken,
}

impl Drop for ChanvoyBind {
    fn drop(&mut self) {
        self.inner_cancel.cancel();
        self.release.release();
    }
}

impl BindHandle for ChanvoyBind {
    fn registration_id(&self) -> &IdToken {
        &self.registration_id
    }

    fn resolved_start(&self) -> &Anchor {
        &self.resolved_start
    }
}

impl ChanvoyWaitObserver {
    fn remember(&self, err: CoreError) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(err);
        }
    }

    fn observation_from_wait(
        &self,
        start: &Anchor,
        result: Result<WaitResult, CoreError>,
    ) -> Observation {
        match result {
            Ok(wr) => {
                let Some(message) = wr.messages.into_iter().next() else {
                    return Observation::Idle;
                };
                match event_from_foreign_message(
                    &message,
                    &self.my_user_id,
                    &self.registration_id,
                    &self.subject_id,
                    start,
                    &self.sidecar,
                ) {
                    Some(event) => Observation::Event(Box::new(event)),
                    None => Observation::Idle,
                }
            }
            Err(CoreError::WaitTimeout(_)) => Observation::Idle,
            Err(CoreError::WaitReplaced { .. }) => {
                self.remember(CoreError::WaitReplaced {
                    wait_id: String::new(),
                    replaced_by_wait_id: String::new(),
                });
                Observation::Failed {
                    reason_code: IdToken::new(REASON_REPLACED),
                }
            }
            Err(CoreError::WaitFilterInvalid(message)) => {
                let uncertain = message.contains("cursor") || message.contains("--after");
                self.remember(CoreError::WaitFilterInvalid(message));
                if uncertain {
                    Observation::CursorUncertain {
                        reason_code: IdToken::new(REASON_CURSOR),
                    }
                } else {
                    Observation::Failed {
                        reason_code: IdToken::new("filter_invalid"),
                    }
                }
            }
            Err(err) => {
                let overflow = matches!(&err, CoreError::WaitProviderDegraded { message, .. } if message.contains("lagged")
                    || message.contains("overflow"));
                self.remember(err);
                if overflow {
                    Observation::Overflow
                } else {
                    Observation::Degraded {
                        reason_code: IdToken::new(REASON_DEGRADED),
                    }
                }
            }
        }
    }
}

impl Observer for ChanvoyWaitObserver {
    type Bind = ChanvoyBind;

    async fn bind(&self, registration: &Registration) -> waitprims_core::Result<Self::Bind> {
        let (resolved, rest_baseline) = resolve_bind_cursor(
            &self.state,
            &self.channel,
            self.predicate.channel_id(),
            self.after.as_deref(),
            self.deadline,
        )
        .await
        .map_err(|err| waitprims_core::ValidationError::new("/start_anchor", err.to_string()))?;

        Ok(ChanvoyBind {
            registration_id: registration.registration_id.clone(),
            resolved_start: resolved,
            rest_baseline,
            release: Arc::clone(&self.release),
            inner_cancel: self.inner_cancel.clone(),
        })
    }

    async fn next(&self, bind: &Self::Bind) -> waitprims_core::Result<Observation> {
        if self.observed.swap(true, Ordering::SeqCst) {
            return Ok(Observation::Idle);
        }

        let engine = async {
            self.state.wait_owners.note_provider_io();
            wait_rest_from_cursor(
                &self.state,
                &self.channel,
                &self.predicate,
                scan_cursor_from_bind(bind.resolved_start()),
                bind.rest_baseline.clone(),
                self.deadline,
            )
            .await
        };

        // Replacement is a runner cancel (`waitprims` Cancel), not an
        // observation. Mapping lease-cancel here would lose
        // `replaced_by_wait_id` on the existing WaitReplaced surface.
        let result = tokio::select! {
            biased;
            _ = self.inner_cancel.cancelled() => {
                return Ok(Observation::Idle);
            }
            res = engine => res,
        };

        Ok(self.observation_from_wait(bind.resolved_start(), result))
    }

    async fn cancel(&self, _bind: &Self::Bind) -> waitprims_core::Result<()> {
        self.inner_cancel.cancel();
        self.release.release();
        Ok(())
    }

    fn poll_ready(&self, _bind: &Self::Bind) -> Option<Observation> {
        None
    }

    fn restore_ready(&self, _bind: &Self::Bind, _obs: Observation) {
        // A1 does not dequeue from poll_ready. Required by the Observer
        // contract; single-registration first-match never needs replay.
    }
}

pub(crate) async fn resolve_bind_cursor(
    state: &AppState,
    channel: &str,
    channel_id: &str,
    after: Option<&str>,
    deadline: Instant,
) -> Result<(Anchor, HashSet<String>), CoreError> {
    if let Some(anchor) = after {
        if anchor.is_empty() {
            return Err(CoreError::WaitFilterInvalid("wait cursor uncertain".into()));
        }
    }
    let (scan, baseline) = establish_baseline(state, channel, channel_id, after, deadline).await?;
    let value = scan.unwrap_or_else(|| EMPTY_AT_ARM_CURSOR.to_string());
    Ok((
        Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(value),
        },
        baseline,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_owner::WaitOwnerRegistry;
    use std::sync::Arc;

    fn msg(id: &str, user: &str, body: &str) -> Message {
        Message {
            id: id.into(),
            user_id: user.into(),
            username: user.into(),
            message: body.into(),
            create_at: 1,
            root_id: id.into(),
        }
    }

    fn dummy_session(wait_id: &str) -> WaitSession {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let lease = futures_executor_block(registry.acquire(
            "ch-1",
            "org",
            "ops",
            None,
            Duration::from_secs(5),
        ))
        .expect("lease");
        let (session, guard) = lease.into_guard();
        drop(guard);
        let _ = wait_id;
        session
    }

    fn futures_executor_block<T>(fut: impl std::future::Future<Output = T>) -> T {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt")
            .block_on(fut)
    }

    #[test]
    fn self_posts_are_not_events() {
        let sidecar = MessageSidecar::new();
        let start = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("anc:tip"),
        };
        let reg = IdToken::new("reg:1");
        let subject = IdToken::new("channel:ops");
        assert!(event_from_foreign_message(
            &msg("p1", "bot", "hello"),
            "bot",
            &reg,
            &subject,
            &start,
            &sidecar
        )
        .is_none());
        assert!(sidecar.take("msg:p1").is_none());
    }

    #[test]
    fn foreign_match_is_sidecared_without_delivery_refs() {
        let sidecar = MessageSidecar::new();
        let start = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("anc:tip"),
        };
        let reg = IdToken::new("reg:1");
        let subject = IdToken::new("channel:ops");
        let event = event_from_foreign_message(
            &msg("p2", "alice", "ASSENT"),
            "bot",
            &reg,
            &subject,
            &start,
            &sidecar,
        )
        .expect("foreign");
        assert_eq!(event.method_id.as_str(), METHOD_ID);
        assert_eq!(event.subject_id.as_str(), "channel:ops");
        assert_ne!(event.subject_id.as_str(), "channel:p2");
        assert!(event.delivery_ref.is_none());
        assert!(event.activation_ref.is_none());
        let stored = sidecar
            .take(event.payload.payload_ref.as_str())
            .expect("sidecar");
        assert_eq!(stored.id, "p2");
        assert_eq!(stored.message, "ASSENT");
    }

    #[test]
    fn named_overflow_and_cursor_are_not_timeouts() {
        assert!(matches!(
            named_failure("ops", REASON_OVERFLOW),
            CoreError::WaitProviderDegraded { message, .. } if message.contains("overflow")
        ));
        assert!(matches!(
            named_failure("ops", REASON_CURSOR),
            CoreError::WaitFilterInvalid(message) if message.contains("cursor")
        ));
        assert!(matches!(
            named_failure("ops", "other"),
            CoreError::WaitProviderDegraded { .. }
        ));
        assert!(!matches!(
            named_failure("ops", REASON_OVERFLOW),
            CoreError::WaitTimeout(_)
        ));
    }

    #[test]
    fn translate_events_uses_sidecar_not_a_refetch() {
        let sidecar = MessageSidecar::new();
        let start = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("anc:tip"),
        };
        let event = event_from_foreign_message(
            &msg("p3", "alice", "hi"),
            "bot",
            &IdToken::new("reg:1"),
            &IdToken::new("channel:ops"),
            &start,
            &sidecar,
        )
        .unwrap();
        let session = dummy_session("wait_aa");
        let outcome = LiveWaitOutcome {
            capabilities: vec![],
            message_id: IdToken::new("out"),
            correlation_id: IdToken::new("c"),
            created_at: timestamp_now(),
            actor_ref: ActorRef::new("actor:bot"),
            causation_id: None,
            grant_ref: None,
            verification_receipt_ref: None,
            policy_decision_ref: None,
            waiter_id: IdToken::new("wait_aa"),
            request_ref: IdToken::new("req"),
            completed_at: timestamp_now(),
            outcome_kind: OutcomeKind::Events,
            logical_deadline: None,
            events: Some(vec![event]),
            proposed_next_anchor: None,
            coverage_complete: None,
            arms: None,
            reason_code: None,
        };
        let wr = translate_outcome(outcome, "ops", &sidecar, &session, &Mutex::new(None)).unwrap();
        assert_eq!(wr.channel, "ops");
        assert_eq!(wr.messages.len(), 1);
        assert_eq!(wr.messages[0].id, "p3");
    }

    #[test]
    fn translate_deadman_is_wait_timeout() {
        let session = dummy_session("wait_bb");
        let outcome = LiveWaitOutcome {
            capabilities: vec![],
            message_id: IdToken::new("out"),
            correlation_id: IdToken::new("c"),
            created_at: timestamp_now(),
            actor_ref: ActorRef::new("actor:bot"),
            causation_id: None,
            grant_ref: None,
            verification_receipt_ref: None,
            policy_decision_ref: None,
            waiter_id: IdToken::new("wait_bb"),
            request_ref: IdToken::new("req"),
            completed_at: timestamp_now(),
            outcome_kind: OutcomeKind::LogicalDeadman,
            logical_deadline: None,
            events: Some(vec![]),
            proposed_next_anchor: None,
            coverage_complete: Some(true),
            arms: None,
            reason_code: None,
        };
        let err = translate_outcome(
            outcome,
            "ops",
            &MessageSidecar::new(),
            &session,
            &Mutex::new(None),
        )
        .unwrap_err();
        assert!(matches!(err, CoreError::WaitTimeout(ch) if ch == "ops"));
    }

    #[test]
    fn lease_release_is_idempotent() {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let lease = futures_executor_block(registry.acquire(
            "ch-1",
            "org",
            "ops",
            None,
            Duration::from_secs(5),
        ))
        .expect("lease");
        let (_session, guard) = lease.into_guard();
        assert!(registry.snapshot("ch-1").is_some());
        let release = LeaseRelease::new(guard);
        release.release();
        assert!(release.is_released());
        release.release();
        assert!(registry.snapshot("ch-1").is_none());
        let again = futures_executor_block(registry.acquire(
            "ch-1",
            "org",
            "ops",
            None,
            Duration::from_secs(1),
        ));
        assert!(again.is_ok(), "next waiter must rebind after drop");
    }

    #[test]
    fn bind_cursor_from_validated_after_is_exclusive() {
        let cursor = "post-abc";
        let anchor = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(cursor),
        };
        assert_eq!(anchor.value.as_str(), "post-abc");
        assert_eq!(anchor.kind, AnchorKind::ProviderOpaque);
        assert_eq!(scan_cursor_from_bind(&anchor).as_deref(), Some("post-abc"));
    }

    #[test]
    fn empty_at_arm_bind_is_not_a_provider_post_id() {
        let anchor = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(EMPTY_AT_ARM_CURSOR),
        };
        assert!(scan_cursor_from_bind(&anchor).is_none());
    }

    #[test]
    fn waitprims_deadline_uses_remaining_budget() {
        let start = Instant::now();
        let remaining = Duration::from_secs(4);
        let ts = deadline_from_instant(start + remaining);
        let span = timestamp_now().duration_until(&ts);
        assert!(
            span <= remaining,
            "waitprims deadline {span:?} must not exceed remaining {remaining:?}"
        );
        assert!(
            span >= Duration::from_secs(3),
            "deadline should keep most of the budget, got {span:?}"
        );
    }

    #[test]
    fn registration_digest_is_admissible_sha256() {
        let session = dummy_session("wait_digest");
        let (set, _req) = build_live_documents(
            &session,
            "bot",
            "ops",
            Some("post-abc"),
            timestamp_now().saturating_add(Duration::from_secs(30)),
        )
        .expect("docs");
        assert_ne!(set.registration_digest.value, "0".repeat(64));
        assert_eq!(set.registration_digest.value.len(), 64);
        assert!(set
            .registration_digest
            .value
            .chars()
            .all(|c| c.is_ascii_hexdigit()));
        let recomputed = registration_digest_hex(&set.registrations[0]).expect("recompute");
        assert_eq!(recomputed, set.registration_digest.value);
        let json = serde_json::to_string(&waitprims_core::AgentWaitMessage::RegistrationSet(set))
            .expect("json");
        waitprims_core::validate_message(&json).expect("registration_set must admit");
    }
}
