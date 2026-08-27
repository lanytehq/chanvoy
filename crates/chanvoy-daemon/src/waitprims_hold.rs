//! A1: drive single-channel `wait_channel_v3` through
//! `waitprims_async::run_first_match`, and held `wait_follow_v1`
//! through `waitprims_async::run_follow`.
//!
//! The adapter lives only in this crate. `chanvoy-core` public types and
//! `chanvoy-mcp` take no waitprims dependency. Legacy `wait_channel` /
//! `wait_channel_v2` stay on their established engines. A2 fan-in
//! (`wait_channels_v1`) uses multi-arm `run_first_match`. Observation
//! uses the existing daemon stream: the event bus on monitored
//! channels, REST from a bind-resolved cursor otherwise. No second
//! Mattermost client or per-wait WebSocket.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chanvoy_core::{
    CoreError, Message, WaitFollowEvent, WaitFollowEventKind, WaitFollowFailureReason,
    WaitFollowMode, WaitFollowResult, WaitFollowResultKind, WaitResult,
};
use sha2::{Digest, Sha256};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use waitprims_async::{
    run_first_match, run_follow, BindHandle, Cancel, Clock, FollowEnd, Observation, Observer,
};
use waitprims_core::{
    validate_raw_documents, ActorRef, AgentWaitMessage, Anchor, AnchorKind, AuthnMode,
    BaselinePolicy, Canonicalization, CapabilityToken, ContentDigest, DigestAlgorithm, IdToken,
    JcsDigest, LiveWaitOutcome, LiveWaitRequest, OpaqueRef, OutcomeKind, PayloadRef, PredicateRef,
    Registration, RegistrationSet, ReplayStatus, Timestamp, WaitBound, WaitEvent,
};

use crate::wait::{
    empty_at_arm_observation, establish_baseline, one_message_result, provider_retry,
    wait_push_from_cursor, wait_rest_from_cursor, WaitPredicate,
};
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
    pub channel_id: &'a str,
    pub after: Option<&'a str>,
    /// Explicit `--after` bound before ownership acquire. When set, the
    /// runner must consume this cursor and must not fetch again.
    pub prebound_after: Option<(Anchor, HashSet<String>)>,
    pub monitored: bool,
    pub predicate: WaitPredicate,
    pub deadline: Instant,
    pub session: &'a WaitSession,
    pub guard: WaitGuard,
}

/// Forwards lease cancellation into waitprims `Cancel`. Abort on drop so a
/// completed wait does not leave a dormant task parked on the lease token.
pub(crate) struct CancelForward {
    handle: tokio::task::JoinHandle<()>,
}

impl CancelForward {
    pub(crate) fn spawn(lease_cancel: CancellationToken, wp: Cancel) -> Self {
        Self {
            handle: tokio::spawn(async move {
                lease_cancel.cancelled().await;
                wp.trigger();
            }),
        }
    }
}

impl Drop for CancelForward {
    fn drop(&mut self) {
        self.handle.abort();
    }
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
    // Explicit `--after` is bound before acquire. Tip-at-arm still
    // resolves here so provider/auth/degraded CoreErrors never pass
    // through waitprims ValidationError, and replacement cancel can
    // interrupt that remaining provider work.
    let (resolved_start, rest_baseline) = match wait.prebound_after {
        Some(bound) => bound,
        None => resolve_bind_cursor(
            state,
            wait.session,
            wait.channel,
            wait.predicate.channel_id(),
            wait.after,
            wait.deadline,
        )
        .await
        .map_err(classify_bind_core_error)?,
    };

    let observer = ChanvoyWaitObserver {
        state: state.clone(),
        channel: wait.channel.to_string(),
        after: wait.after.map(str::to_string),
        monitored: wait.monitored,
        predicate: wait.predicate,
        deadline: wait.deadline,
        my_user_id: state.my_user_id.clone(),
        registration_id: IdToken::new(format!("reg:{}", wait.session.wait_id)),
        subject_id: channel_subject(wait.channel_id),
        resolved_start,
        rest_baseline,
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        release: Arc::clone(&release),
        inner_cancel: inner_cancel.clone(),
        follow: false,
        follow_rx: Mutex::new(None),
        bind_ready: Mutex::new(None),
        observed: AtomicBool::new(false),
        restored: Mutex::new(HashMap::new()),
    };

    let clock = WallClock::new();
    let (set, request) = build_live_documents(
        wait.session,
        &state.my_user_id,
        wait.channel_id,
        wait.after,
        clock.project_deadline(wait.deadline),
    )?;
    let docs = admit_set_and_request(wait.channel, set, request)?;

    let wp_cancel = Cancel::new();
    let _cancel_fwd = CancelForward::spawn(wait.session.cancel.clone(), wp_cancel.clone());

    let outcome = run_first_match(&docs.set, &docs.request, &observer, &clock, &wp_cancel)
        .await
        .map_err(|err| map_waitprims_err(wait.channel, err))?;
    let outcome = admit_wait_result(&docs, outcome)?;

    inner_cancel.cancel();
    release.release();
    translate_outcome(outcome, wait.channel, &sidecar, wait.session, &last_error)
}

/// Held single-channel wait. The waitprims runner owns one bind for the
/// session; emitted bursts are projected to the daemon stream without
/// re-binding or opening another provider connection.
pub(crate) async fn run_single_channel_follow(
    state: &AppState,
    wait: FirstMatchWait<'_>,
    stream: crate::wait::FollowStreamSender,
) -> Result<WaitFollowResult, CoreError> {
    let release = Arc::new(LeaseRelease::new(wait.guard));
    let sidecar = MessageSidecar::new();
    let last_error = Arc::new(Mutex::new(None));
    let inner_cancel = CancellationToken::new();
    // Subscribe before the remaining bind work so monitored delivery
    // cannot fall into a baseline-to-subscribe seam.
    let follow_rx = wait.monitored.then(|| state.event_bus.subscribe());
    let (resolved_start, rest_baseline) = match wait.prebound_after {
        Some(bound) => bound,
        None => resolve_bind_cursor(
            state,
            wait.session,
            wait.channel,
            wait.predicate.channel_id(),
            wait.after,
            wait.deadline,
        )
        .await
        .map_err(classify_bind_core_error)?,
    };

    let tip_state = Arc::new(Mutex::new(None));
    let (bind_ready_tx, bind_ready_rx) = tokio::sync::oneshot::channel();

    let observer = ChanvoyWaitObserver {
        state: state.clone(),
        channel: wait.channel.to_string(),
        after: wait.after.map(str::to_string),
        monitored: wait.monitored,
        predicate: wait.predicate,
        deadline: wait.deadline,
        my_user_id: state.my_user_id.clone(),
        registration_id: IdToken::new(format!("reg:{}", wait.session.wait_id)),
        subject_id: channel_subject(wait.channel_id),
        resolved_start,
        rest_baseline,
        sidecar: sidecar.clone(),
        last_error: Arc::clone(&last_error),
        release: Arc::clone(&release),
        inner_cancel: inner_cancel.clone(),
        follow: true,
        follow_rx: Mutex::new(follow_rx),
        bind_ready: Mutex::new(Some(bind_ready_tx)),
        observed: AtomicBool::new(false),
        restored: Mutex::new(HashMap::new()),
    };

    let clock = WallClock::new();
    let (set, request) = build_live_documents(
        wait.session,
        &state.my_user_id,
        wait.channel_id,
        wait.after,
        clock.project_deadline(wait.deadline),
    )?;
    let docs = admit_set_and_request(wait.channel, set, request)?;
    let wp_cancel = Cancel::new();
    let _cancel_fwd = CancelForward::spawn(wait.session.cancel.clone(), wp_cancel.clone());

    let sink_stream = stream.clone();
    let sink_sidecar = sidecar.clone();
    let sink_error = Arc::clone(&last_error);
    let sink_tip = Arc::clone(&tip_state);
    let sink_channel = wait.channel.to_string();
    let sink_wait_id = wait.session.wait_id.clone();
    let follow = run_follow(
        &observer,
        &clock,
        &wp_cancel,
        &docs.set,
        &docs.request,
        move |burst| {
            let stream = sink_stream.clone();
            let sidecar = sink_sidecar.clone();
            let last_error = Arc::clone(&sink_error);
            let tip_state = Arc::clone(&sink_tip);
            let channel = sink_channel.clone();
            let wait_id = sink_wait_id.clone();
            async move {
                let event_count = burst.events.len();
                for (index, event) in burst.events.into_iter().enumerate() {
                    let key = event.payload.payload_ref.as_str();
                    let Some(entry) = sidecar.take_entry(key) else {
                        let err = CoreError::WaitProviderDegraded {
                            channel: channel.clone(),
                            message: "held wait event missing sidecar message".into(),
                        };
                        if let Ok(mut slot) = last_error.lock() {
                            *slot = Some(err);
                        }
                        return Err(waitprims_core::ValidationError::new(
                            "/follow_sink",
                            "sidecar_missing",
                        )
                        .into());
                    };
                    if let Err(err) = authenticate_sidecar_message(
                        &channel,
                        &entry.message,
                        &event.payload.content_digest,
                    ) {
                        if let Ok(mut slot) = last_error.lock() {
                            *slot = Some(err);
                        }
                        return Err(waitprims_core::ValidationError::new(
                            "/follow_sink",
                            "sidecar_digest",
                        )
                        .into());
                    }
                    let proposed_tip = event.proposed_next_anchor.value.as_str();
                    if proposed_tip != entry.message.id {
                        if let Ok(mut slot) = last_error.lock() {
                            *slot = Some(CoreError::WaitProviderDegraded {
                                channel: channel.clone(),
                                message: "held wait tip does not equal its sole message id".into(),
                            });
                        }
                        return Err(waitprims_core::ValidationError::new(
                            "/follow_sink",
                            "tip_message_mismatch",
                        )
                        .into());
                    }
                    let mode = match entry.phase {
                        FollowObservationPhase::Backlog => WaitFollowMode::Backlog,
                        FollowObservationPhase::Live => WaitFollowMode::Live,
                    };
                    let record = WaitFollowEvent::message(
                        wait_id.clone(),
                        mode,
                        entry.message,
                        mode == WaitFollowMode::Backlog && index + 1 < event_count,
                    )
                    .map_err(|_| {
                        waitprims_core::ValidationError::new(
                            "/follow_sink",
                            "invalid_event_document",
                        )
                    })?;
                    emit_follow_event(&stream, record).await.map_err(|err| {
                        if let Ok(mut slot) = last_error.lock() {
                            *slot = Some(err);
                        }
                        waitprims_async::Error::from(waitprims_core::ValidationError::new(
                            "/follow_sink",
                            "stream_write_failed",
                        ))
                    })?;
                    if let Ok(mut current) = tip_state.lock() {
                        *current = Some(proposed_tip.to_string());
                    }
                }
                Ok(())
            }
        },
    );
    tokio::pin!(follow);

    let early_end = tokio::select! {
        bound = bind_ready_rx => {
            bound.map_err(|_| CoreError::WaitProviderDegraded {
                channel: wait.channel.to_string(),
                message: "held wait ended before observer bind".into(),
            })?;
            None
        }
        end = &mut follow => Some(end),
    };
    if let Some(end) = early_end {
        inner_cancel.cancel();
        release.release();
        return Err(match end {
            Ok(_) => CoreError::WaitProviderDegraded {
                channel: wait.channel.to_string(),
                message: "held wait ended before armed receipt".into(),
            },
            Err(err) => map_waitprims_err(wait.channel, err),
        });
    }

    emit_follow_event(
        &stream,
        WaitFollowEvent::armed(
            wait.session.wait_id.clone(),
            wait.session.replaced_wait_id.clone(),
        ),
    )
    .await?;

    let end = follow.await;
    let end = match end {
        Ok(end) => end,
        Err(err) => {
            let reason = last_error
                .lock()
                .ok()
                .and_then(|slot| slot.as_ref().map(follow_failure_reason))
                .unwrap_or(WaitFollowFailureReason::ProviderFailed);
            emit_follow_event(
                &stream,
                WaitFollowEvent::terminal(
                    wait.session.wait_id.clone(),
                    WaitFollowEventKind::Failed {
                        reason_code: reason,
                    },
                ),
            )
            .await?;
            inner_cancel.cancel();
            release.release();
            if let Some(saved) = last_error.lock().ok().and_then(|mut slot| slot.take()) {
                return Err(saved);
            }
            return Err(map_waitprims_err(wait.channel, err));
        }
    };

    let tip = tip_state.lock().ok().and_then(|current| current.clone());
    let (terminal_kind, result_kind) = match end {
        FollowEnd::Deadline => (
            WaitFollowEventKind::Deadman,
            WaitFollowResultKind::Deadman { tip },
        ),
        FollowEnd::Cancel => {
            let replaced_by_wait_id = wait.session.replaced_by_id();
            if replaced_by_wait_id.is_empty() {
                emit_follow_event(
                    &stream,
                    WaitFollowEvent::terminal(
                        wait.session.wait_id.clone(),
                        WaitFollowEventKind::Canceled,
                    ),
                )
                .await?;
                inner_cancel.cancel();
                release.release();
                return Err(CoreError::WaitProviderDegraded {
                    channel: wait.channel.to_string(),
                    message: "held wait canceled".into(),
                });
            } else {
                (
                    WaitFollowEventKind::Replaced {
                        replaced_by_wait_id: replaced_by_wait_id.clone(),
                    },
                    WaitFollowResultKind::Replaced {
                        replaced_by_wait_id,
                        tip,
                    },
                )
            }
        }
        FollowEnd::TerminalArm { reason_code, .. } => {
            let reason = follow_failure_reason_code(reason_code.as_str());
            emit_follow_event(
                &stream,
                WaitFollowEvent::terminal(
                    wait.session.wait_id.clone(),
                    WaitFollowEventKind::Failed {
                        reason_code: reason,
                    },
                ),
            )
            .await?;
            inner_cancel.cancel();
            release.release();
            if let Some(saved) = last_error.lock().ok().and_then(|mut slot| slot.take()) {
                return Err(saved);
            }
            return Err(named_failure(wait.channel, reason_code.as_str()));
        }
    };

    let result = WaitFollowResult {
        wait_id: wait.session.wait_id.clone(),
        kind: result_kind,
    };
    result
        .validate()
        .map_err(|message| CoreError::WaitProviderDegraded {
            channel: wait.channel.to_string(),
            message: message.into(),
        })?;
    // The terminal line must cross the UDS write boundary before the
    // ownership guard is released or the terminal response is returned.
    emit_follow_event(
        &stream,
        WaitFollowEvent::terminal(wait.session.wait_id.clone(), terminal_kind),
    )
    .await?;
    inner_cancel.cancel();
    release.release();
    Ok(result)
}

async fn emit_follow_event(
    stream: &crate::wait::FollowStreamSender,
    event: WaitFollowEvent,
) -> Result<(), CoreError> {
    event
        .validate()
        .map_err(|message| CoreError::WaitProviderDegraded {
            channel: "follow".into(),
            message: message.into(),
        })?;
    let (written, receipt) = tokio::sync::oneshot::channel();
    stream
        .send(crate::wait::FollowStreamRecord { event, written })
        .await
        .map_err(|_| CoreError::WaitProviderDegraded {
            channel: "follow".into(),
            message: "held wait stream closed".into(),
        })?;
    receipt
        .await
        .map_err(|_| CoreError::WaitProviderDegraded {
            channel: "follow".into(),
            message: "held wait stream closed before write acknowledgement".into(),
        })?
        .map_err(|message| CoreError::WaitProviderDegraded {
            channel: "follow".into(),
            message,
        })
}

fn follow_failure_reason(error: &CoreError) -> WaitFollowFailureReason {
    match error {
        CoreError::WaitFilterInvalid(message)
            if message.contains("cursor") || message.contains("--after") =>
        {
            WaitFollowFailureReason::CursorUncertain
        }
        CoreError::WaitReplaced { .. } => WaitFollowFailureReason::OwnershipLost,
        CoreError::WaitProviderDegraded { message, .. }
            if message.contains("overflow") || message.contains("lagged") =>
        {
            WaitFollowFailureReason::ProviderOverflow
        }
        CoreError::WaitProviderDegraded { message, .. } if message.contains("outage") => {
            WaitFollowFailureReason::ProviderOutage
        }
        CoreError::WaitProviderDegraded { .. } => WaitFollowFailureReason::ProviderDegraded,
        _ => WaitFollowFailureReason::ProviderFailed,
    }
}

fn follow_failure_reason_code(reason: &str) -> WaitFollowFailureReason {
    match reason {
        REASON_OVERFLOW => WaitFollowFailureReason::ProviderOverflow,
        REASON_CURSOR => WaitFollowFailureReason::CursorUncertain,
        REASON_DEGRADED => WaitFollowFailureReason::ProviderDegraded,
        REASON_REPLACED => WaitFollowFailureReason::OwnershipLost,
        "provider_outage" => WaitFollowFailureReason::ProviderOutage,
        _ => WaitFollowFailureReason::ProviderFailed,
    }
}

/// Production clock: one RFC3339 origin plus monotonic Instant elapsed.
/// Wall-clock steps after construction cannot change the wait budget.
pub(crate) struct WallClock {
    pub(crate) origin_instant: Instant,
    pub(crate) origin_ts: Timestamp,
}

impl WallClock {
    pub(crate) fn new() -> Self {
        Self {
            origin_instant: Instant::now(),
            origin_ts: timestamp_now(),
        }
    }

    pub(crate) fn project_deadline(&self, deadline: Instant) -> Timestamp {
        self.origin_ts
            .saturating_add(deadline.saturating_duration_since(self.origin_instant))
    }
}

impl Clock for WallClock {
    fn now(&self) -> Timestamp {
        self.origin_ts.saturating_add(self.origin_instant.elapsed())
    }

    fn sleep_until(&self, deadline: &Timestamp) -> impl std::future::Future<Output = ()> + Send {
        let delay = self.now().duration_until(deadline);
        async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
    }
}

pub(crate) fn channel_subject(channel_id: &str) -> IdToken {
    IdToken::new(format!("channel:{channel_id}"))
}

/// Provider occurrence time from Mattermost `create_at` (unix ms).
pub(crate) fn timestamp_from_create_at(create_at: i64) -> Timestamp {
    let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp_millis(create_at) else {
        return timestamp_now();
    };
    Timestamp::parse(&dt.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string())
        .unwrap_or_else(|_| timestamp_now())
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

    pub(crate) fn noop() -> Self {
        Self {
            guard: Mutex::new(None),
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
    inner: Arc<Mutex<HashMap<String, MessageSidecarEntry>>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FollowObservationPhase {
    Backlog,
    Live,
}

#[derive(Clone)]
struct MessageSidecarEntry {
    message: Message,
    phase: FollowObservationPhase,
}

impl MessageSidecar {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    fn store_with_phase(&self, payload_ref: &str, message: Message, phase: FollowObservationPhase) {
        if let Ok(mut map) = self.inner.lock() {
            map.insert(
                payload_ref.to_string(),
                MessageSidecarEntry { message, phase },
            );
        }
    }

    pub(crate) fn take(&self, payload_ref: &str) -> Option<Message> {
        self.take_entry(payload_ref).map(|entry| entry.message)
    }

    fn take_entry(&self, payload_ref: &str) -> Option<MessageSidecarEntry> {
        self.inner
            .lock()
            .ok()
            .and_then(|mut map| map.remove(payload_ref))
    }

    pub(crate) fn get(&self, payload_ref: &str) -> Option<Message> {
        self.inner
            .lock()
            .ok()
            .and_then(|map| map.get(payload_ref).map(|entry| entry.message.clone()))
    }
}

pub(crate) fn payload_ref_for(message: &Message) -> String {
    format!("msg:{}", message.id)
}

pub(crate) fn contract_internal(channel: &str, detail: impl std::fmt::Display) -> CoreError {
    CoreError::WaitProviderDegraded {
        channel: channel.to_string(),
        message: format!("waitprims contract: {detail}"),
    }
}

pub(crate) fn sidecar_message_bytes(message: &Message) -> Result<Vec<u8>, CoreError> {
    serde_json::to_vec(message)
        .map_err(|err| contract_internal("wait", format!("sidecar message bytes: {err}")))
}

pub(crate) fn content_digest_for(message: &Message) -> Result<ContentDigest, CoreError> {
    let bytes = sidecar_message_bytes(message)?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(ContentDigest {
        algorithm: DigestAlgorithm::Sha256,
        value: hex_lower(&hasher.finalize()),
    })
}

fn hex_lower(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Sidecar a provider message as a waitprims event. Includes self-authored
/// posts — callers that must ignore self use [`event_from_foreign_message`].
pub(crate) fn event_from_message(
    message: &Message,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
) -> Result<WaitEvent, CoreError> {
    event_from_message_with_phase(
        message,
        registration_id,
        subject_id,
        start,
        sidecar,
        FollowObservationPhase::Live,
    )
}

fn event_from_message_with_phase(
    message: &Message,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
    phase: FollowObservationPhase,
) -> Result<WaitEvent, CoreError> {
    let payload_ref = payload_ref_for(message);
    sidecar.store_with_phase(&payload_ref, message.clone(), phase);
    let observed = timestamp_now();
    let digest = content_digest_for(message)?;
    Ok(WaitEvent {
        event_id: IdToken::new(format!("evt:{}", message.id)),
        registration_id: registration_id.clone(),
        source_instance_ref: OpaqueRef::new("source:chanvoy-daemon"),
        method_id: IdToken::new(METHOD_ID),
        subject_kind: IdToken::new("channel"),
        subject_id: subject_id.clone(),
        occurred_at: timestamp_from_create_at(message.create_at),
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
            content_digest: digest,
            media_type: Some("application/json".into()),
        },
        delivery_ref: None,
        activation_ref: None,
    })
}

/// Filter self-authored posts before constructing `Observation::Event`.
pub(crate) fn event_from_foreign_message(
    message: &Message,
    my_user_id: &str,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
) -> Result<Option<WaitEvent>, CoreError> {
    event_from_foreign_message_with_phase(
        message,
        my_user_id,
        registration_id,
        subject_id,
        start,
        sidecar,
        FollowObservationPhase::Live,
    )
}

#[allow(clippy::too_many_arguments)]
fn event_from_foreign_message_with_phase(
    message: &Message,
    my_user_id: &str,
    registration_id: &IdToken,
    subject_id: &IdToken,
    start: &Anchor,
    sidecar: &MessageSidecar,
    phase: FollowObservationPhase,
) -> Result<Option<WaitEvent>, CoreError> {
    if message.user_id == my_user_id {
        return Ok(None);
    }
    event_from_message_with_phase(message, registration_id, subject_id, start, sidecar, phase)
        .map(Some)
}

pub(crate) fn authenticate_sidecar_message(
    channel: &str,
    message: &Message,
    declared: &ContentDigest,
) -> Result<(), CoreError> {
    let computed = content_digest_for(message)?;
    if declared.algorithm != DigestAlgorithm::Sha256
        || declared.algorithm != computed.algorithm
        || declared.value != computed.value
    {
        return Err(CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "sidecar message digest mismatch".into(),
        });
    }
    Ok(())
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
            authenticate_sidecar_message(channel, &message, &event.payload.content_digest)?;
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

pub(crate) fn map_waitprims_err(channel: &str, _err: waitprims_core::Error) -> CoreError {
    CoreError::WaitProviderDegraded {
        channel: channel.to_string(),
        message: "waitprims runner failed".into(),
    }
}

/// Bind-time `CoreError` values are returned unchanged. Do not rewrite
/// provider/auth/transport/degraded failures as `WaitFilterInvalid`.
pub(crate) fn classify_bind_core_error(err: CoreError) -> CoreError {
    err
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
        priority: None,
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

pub(crate) struct AdmittedWaitDocs {
    pub channel: String,
    pub set: RegistrationSet,
    pub request: LiveWaitRequest,
    pub set_json: String,
    pub request_json: String,
}

pub(crate) fn admit_set_and_request(
    channel: &str,
    set: RegistrationSet,
    request: LiveWaitRequest,
) -> Result<AdmittedWaitDocs, CoreError> {
    let set_json = serde_json::to_string(&AgentWaitMessage::RegistrationSet(set))
        .map_err(|err| contract_internal(channel, format!("set encode: {err}")))?;
    let req_json = serde_json::to_string(&AgentWaitMessage::LiveWaitRequest(request))
        .map_err(|err| contract_internal(channel, format!("request encode: {err}")))?;
    let admitted = validate_raw_documents([&set_json, &req_json])
        .map_err(|err| contract_internal(channel, format!("set/request admission: {err}")))?;
    let mut set = None;
    let mut request = None;
    for msg in admitted {
        match msg.into_inner() {
            AgentWaitMessage::RegistrationSet(value) => set = Some(value),
            AgentWaitMessage::LiveWaitRequest(value) => request = Some(value),
            other => {
                return Err(contract_internal(
                    channel,
                    format!("unexpected {}", other.message_type().as_str()),
                ));
            }
        }
    }
    let set = set.ok_or_else(|| contract_internal(channel, "missing set"))?;
    let request = request.ok_or_else(|| contract_internal(channel, "missing request"))?;
    let set_json = serde_json::to_string(&AgentWaitMessage::RegistrationSet(set.clone()))
        .map_err(|err| contract_internal(channel, format!("admitted set encode: {err}")))?;
    let request_json =
        serde_json::to_string(&AgentWaitMessage::LiveWaitRequest(request.clone()))
            .map_err(|err| contract_internal(channel, format!("admitted request encode: {err}")))?;
    Ok(AdmittedWaitDocs {
        channel: channel.to_string(),
        set,
        request,
        set_json,
        request_json,
    })
}

pub(crate) fn admit_wait_result(
    docs: &AdmittedWaitDocs,
    outcome: LiveWaitOutcome,
) -> Result<LiveWaitOutcome, CoreError> {
    let channel = docs.channel.as_str();
    let outcome_json = serde_json::to_string(&AgentWaitMessage::LiveWaitOutcome(outcome))
        .map_err(|err| contract_internal(channel, format!("outcome encode: {err}")))?;
    let admitted = validate_raw_documents([&docs.set_json, &docs.request_json, &outcome_json])
        .map_err(|err| {
            contract_internal(channel, format!("set/request/outcome admission: {err}"))
        })?;
    for msg in admitted {
        if let AgentWaitMessage::LiveWaitOutcome(value) = msg.into_inner() {
            return Ok(value);
        }
    }
    Err(contract_internal(channel, "missing admitted outcome"))
}

#[cfg(test)]
pub(crate) fn admit_raw(raw: &str) -> Result<AgentWaitMessage, CoreError> {
    waitprims_core::validate_message(raw)
        .map(|admitted| admitted.into_inner())
        .map_err(|err| contract_internal("wait", err))
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
    monitored: bool,
    predicate: WaitPredicate,
    deadline: Instant,
    my_user_id: String,
    registration_id: IdToken,
    subject_id: IdToken,
    resolved_start: Anchor,
    rest_baseline: HashSet<String>,
    sidecar: MessageSidecar,
    last_error: Arc<Mutex<Option<CoreError>>>,
    release: Arc<LeaseRelease>,
    inner_cancel: CancellationToken,
    follow: bool,
    follow_rx: Mutex<Option<tokio::sync::broadcast::Receiver<Arc<chanvoy_core::DaemonEvent>>>>,
    bind_ready: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    observed: AtomicBool,
    restored: Mutex<HashMap<String, std::collections::VecDeque<Observation>>>,
}

pub(crate) struct ChanvoyBind {
    pub(crate) registration_id: IdToken,
    pub(crate) subject_id: IdToken,
    pub(crate) resolved_start: Anchor,
    current_cursor: Mutex<Anchor>,
    pub(crate) rest_baseline: HashSet<String>,
    backlog_ids: HashSet<String>,
    follow_push: tokio::sync::Mutex<Option<FollowPushState>>,
    release_on_drop: bool,
    pub(crate) release: Arc<LeaseRelease>,
    pub(crate) inner_cancel: CancellationToken,
}

struct FollowPushState {
    rx: tokio::sync::broadcast::Receiver<Arc<chanvoy_core::DaemonEvent>>,
    buffer: VecDeque<Arc<chanvoy_core::DaemonEvent>>,
}

pub(crate) struct FollowBindState {
    backlog_ids: HashSet<String>,
    rx: Option<tokio::sync::broadcast::Receiver<Arc<chanvoy_core::DaemonEvent>>>,
}

impl ChanvoyBind {
    pub(crate) fn new(
        registration_id: IdToken,
        subject_id: IdToken,
        resolved_start: Anchor,
        rest_baseline: HashSet<String>,
        release: Arc<LeaseRelease>,
        inner_cancel: CancellationToken,
        follow: Option<FollowBindState>,
    ) -> Self {
        let release_on_drop = follow.is_none();
        let (backlog_ids, follow_rx) = follow
            .map(|state| (state.backlog_ids, state.rx))
            .unwrap_or_default();
        Self {
            registration_id,
            subject_id,
            current_cursor: Mutex::new(resolved_start.clone()),
            resolved_start,
            rest_baseline,
            backlog_ids,
            follow_push: tokio::sync::Mutex::new(follow_rx.map(|rx| FollowPushState {
                rx,
                buffer: VecDeque::new(),
            })),
            release_on_drop,
            release,
            inner_cancel,
        }
    }
}

impl Drop for ChanvoyBind {
    fn drop(&mut self) {
        self.inner_cancel.cancel();
        if self.release_on_drop {
            self.release.release();
        }
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
    fn take_restored(&self, bind: &ChanvoyBind) -> Option<Observation> {
        let key = bind.registration_id().as_str();
        self.restored
            .lock()
            .ok()?
            .get_mut(key)
            .and_then(std::collections::VecDeque::pop_front)
    }

    fn remember(&self, err: CoreError) {
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(err);
        }
    }

    fn observation_from_wait(
        &self,
        start: &Anchor,
        result: Result<WaitResult, CoreError>,
        phase: FollowObservationPhase,
    ) -> Observation {
        match result {
            Ok(wr) => {
                let Some(message) = wr.messages.into_iter().next() else {
                    return Observation::Idle;
                };
                match event_from_foreign_message_with_phase(
                    &message,
                    &self.my_user_id,
                    &self.registration_id,
                    &self.subject_id,
                    start,
                    &self.sidecar,
                    phase,
                ) {
                    Ok(Some(event)) => Observation::Event(Box::new(event)),
                    Ok(None) => Observation::Idle,
                    Err(err) => {
                        self.remember(err);
                        Observation::Failed {
                            reason_code: IdToken::new("sidecar_digest"),
                        }
                    }
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
        let follow_rx = self
            .follow_rx
            .lock()
            .map_err(|_| waitprims_core::ValidationError::new("/bind", "receiver_lock_poisoned"))?
            .take();
        let backlog_ids = if self.follow {
            let scan = scan_cursor_from_bind(&self.resolved_start);
            let page = match scan {
                Some(anchor) => {
                    let channel_id = self.predicate.channel_id().to_string();
                    provider_retry(&self.state, &self.channel, self.deadline, || {
                        let channel_id = channel_id.clone();
                        let anchor = anchor.clone();
                        async move {
                            self.state
                                .client
                                .posts_after_by_channel_id(&channel_id, &anchor)
                                .await
                        }
                    })
                    .await
                }
                None => {
                    empty_at_arm_observation(
                        &self.state,
                        &self.channel,
                        &self.predicate,
                        self.deadline,
                    )
                    .await
                }
            };
            match page {
                Ok(messages) => messages.into_iter().map(|message| message.id).collect(),
                Err(error) => {
                    self.remember(error);
                    return Err(waitprims_core::ValidationError::new(
                        "/bind",
                        "backlog_snapshot_failed",
                    )
                    .into());
                }
            }
        } else {
            HashSet::new()
        };
        let bind = ChanvoyBind::new(
            registration.registration_id.clone(),
            registration.subject_id.clone(),
            self.resolved_start.clone(),
            self.rest_baseline.clone(),
            Arc::clone(&self.release),
            self.inner_cancel.clone(),
            self.follow.then_some(FollowBindState {
                backlog_ids,
                rx: follow_rx,
            }),
        );
        if let Some(ready) = self.bind_ready.lock().ok().and_then(|mut slot| slot.take()) {
            let _ = ready.send(());
        }
        Ok(bind)
    }

    async fn next(&self, bind: &Self::Bind) -> waitprims_core::Result<Observation> {
        if let Some(obs) = self.take_restored(bind) {
            return Ok(obs);
        }
        if !self.follow && self.observed.swap(true, Ordering::SeqCst) {
            return Ok(Observation::Idle);
        }

        let start = if self.follow {
            bind.current_cursor
                .lock()
                .map_err(|_| waitprims_core::ValidationError::new("/bind", "cursor_lock_poisoned"))?
                .clone()
        } else {
            bind.resolved_start().clone()
        };

        let engine = async {
            self.state.wait_owners.note_provider_io();
            if self.monitored {
                if self.follow {
                    let mut push = bind.follow_push.lock().await;
                    let push = push
                        .as_mut()
                        .ok_or_else(|| CoreError::WaitProviderDegraded {
                            channel: self.channel.clone(),
                            message: "held wait event subscription missing".into(),
                        })?;
                    crate::wait::wait_push_after_baseline(
                        &self.state,
                        &self.channel,
                        &self.predicate,
                        self.after.as_deref(),
                        scan_cursor_from_bind(&start),
                        self.deadline,
                        &mut push.rx,
                        &mut push.buffer,
                    )
                    .await
                } else {
                    wait_push_from_cursor(
                        &self.state,
                        &self.channel,
                        &self.predicate,
                        self.after.as_deref(),
                        scan_cursor_from_bind(&start),
                        self.deadline,
                    )
                    .await
                }
            } else {
                wait_rest_from_cursor(
                    &self.state,
                    &self.channel,
                    &self.predicate,
                    scan_cursor_from_bind(&start),
                    bind.rest_baseline.clone(),
                    self.deadline,
                )
                .await
            }
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

        if self.follow {
            if let Ok(wait_result) = &result {
                if let Some(message) = wait_result.messages.first() {
                    if let Ok(mut cursor) = bind.current_cursor.lock() {
                        *cursor = Anchor {
                            kind: AnchorKind::ProviderOpaque,
                            value: IdToken::new(message.id.clone()),
                        };
                    }
                }
            }
        }
        let phase = result
            .as_ref()
            .ok()
            .and_then(|wait_result| wait_result.messages.first())
            .map(|message| {
                if bind.backlog_ids.contains(&message.id) {
                    FollowObservationPhase::Backlog
                } else {
                    FollowObservationPhase::Live
                }
            })
            .unwrap_or(FollowObservationPhase::Live);
        Ok(self.observation_from_wait(&start, result, phase))
    }

    async fn cancel(&self, _bind: &Self::Bind) -> waitprims_core::Result<()> {
        self.inner_cancel.cancel();
        if !self.follow {
            self.release.release();
        }
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

/// Race pre-run baseline work against replacement cancel so the lease
/// guard can drop instead of ignoring cancel until the provider returns.
pub(crate) async fn race_bind_work<T, F>(session: &WaitSession, work: F) -> Result<T, CoreError>
where
    F: std::future::Future<Output = Result<T, CoreError>>,
{
    tokio::select! {
        biased;
        _ = session.cancel.cancelled() => Err(CoreError::WaitReplaced {
            wait_id: session.wait_id.clone(),
            replaced_by_wait_id: session.replaced_by_id(),
        }),
        res = work => res,
    }
}

pub(crate) async fn resolve_bind_cursor(
    state: &AppState,
    session: &WaitSession,
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
    race_bind_work(session, async {
        let (scan, baseline) =
            establish_baseline(state, channel, channel_id, after, deadline).await?;
        Ok(cursor_from_baseline(scan, baseline))
    })
    .await
}

pub(crate) fn cursor_from_baseline(
    scan: Option<String>,
    baseline: HashSet<String>,
) -> (Anchor, HashSet<String>) {
    let value = scan.unwrap_or_else(|| EMPTY_AT_ARM_CURSOR.to_string());
    (
        Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new(value),
        },
        baseline,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_owner::WaitOwnerRegistry;
    use std::sync::Arc;
    use tokio_util::sync::CancellationToken;

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
        .expect("digest")
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
        .expect("digest")
        .expect("foreign");
        assert_eq!(event.method_id.as_str(), METHOD_ID);
        assert_eq!(event.subject_id.as_str(), "channel:ops");
        assert_ne!(event.subject_id.as_str(), "channel:p2");
        assert_eq!(
            event.occurred_at,
            timestamp_from_create_at(1),
            "occurred_at must be provider create_at"
        );
        assert_ne!(
            event.occurred_at, event.observed_at,
            "observed_at stays wall-clock, not create_at"
        );
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
        .expect("digest")
        .expect("foreign");
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
    fn translate_rejects_sidecar_digest_mismatch() {
        let sidecar = MessageSidecar::new();
        let start = Anchor {
            kind: AnchorKind::ProviderOpaque,
            value: IdToken::new("anc:tip"),
        };
        let mut event = event_from_foreign_message(
            &msg("p4", "alice", "hi"),
            "bot",
            &IdToken::new("reg:1"),
            &IdToken::new("channel:ops"),
            &start,
            &sidecar,
        )
        .expect("digest")
        .expect("foreign");
        event.payload.content_digest.value = "ab".repeat(32);
        let session = dummy_session("wait_cc");
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
            waiter_id: IdToken::new("wait_cc"),
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
        let err =
            translate_outcome(outcome, "ops", &sidecar, &session, &Mutex::new(None)).unwrap_err();
        assert!(
            matches!(err, CoreError::WaitProviderDegraded { ref message, .. } if message.contains("digest")),
            "mismatched sidecar digest must fail closed, got {err:?}"
        );
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
    fn cursor_from_baseline_consumes_explicit_scan() {
        let mut baseline = HashSet::new();
        baseline.insert("post-abc".into());
        let (anchor, rest) = cursor_from_baseline(Some("post-abc".into()), baseline);
        assert_eq!(anchor.kind, AnchorKind::ProviderOpaque);
        assert_eq!(anchor.value.as_str(), "post-abc");
        assert!(rest.contains("post-abc"));
        assert!(scan_cursor_from_bind(&anchor).as_deref() == Some("post-abc"));
    }

    #[test]
    fn waitprims_deadline_uses_remaining_budget() {
        let start = Instant::now();
        let remaining = Duration::from_secs(4);
        let ts = WallClock::new().project_deadline(start + remaining);
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
        let (set, request) = build_live_documents(
            &session,
            "bot",
            "ch-canonical",
            Some("post-abc"),
            timestamp_now().saturating_add(Duration::from_secs(30)),
        )
        .expect("docs");
        assert_eq!(
            set.registrations[0].subject_id.as_str(),
            "channel:ch-canonical"
        );
        assert_ne!(set.registrations[0].subject_id.as_str(), "channel:ops");
        assert_ne!(set.registration_digest.value, "0".repeat(64));
        assert_eq!(set.registration_digest.value.len(), 64);
        assert!(set
            .registration_digest
            .value
            .chars()
            .all(|c| c.is_ascii_hexdigit()));
        let recomputed = registration_digest_hex(&set.registrations[0]).expect("recompute");
        assert_eq!(recomputed, set.registration_digest.value);
        let docs = admit_set_and_request("ops", set, request).expect("admit pair");
        assert_eq!(
            docs.set.registrations[0].subject_id.as_str(),
            "channel:ch-canonical"
        );
        assert_eq!(
            docs.request.registration_set_ref.as_str(),
            docs.set.message_id.as_str()
        );
    }

    #[test]
    fn post_admission_runner_failure_is_provider_degraded() {
        let err = map_waitprims_err("ops", waitprims_core::Error::MalformedJson);
        match err {
            CoreError::WaitProviderDegraded { channel, message } => {
                assert_eq!(channel, "ops");
                assert_eq!(message, "waitprims runner failed");
                assert!(!message.contains("malformed"));
            }
            other => panic!("runner failure must be provider-degraded, got {other:?}"),
        }
    }

    #[test]
    fn bind_preserves_provider_auth_and_degraded_classes() {
        use reqwest::StatusCode;
        let degraded = classify_bind_core_error(CoreError::WaitProviderDegraded {
            channel: "ops".into(),
            message: "deadline reached before a successful observation".into(),
        });
        assert!(
            matches!(degraded, CoreError::WaitProviderDegraded { .. }),
            "degraded must not become WaitFilterInvalid: {degraded:?}"
        );
        let auth = classify_bind_core_error(CoreError::Api {
            status: StatusCode::UNAUTHORIZED,
            message: "token refused".into(),
        });
        assert!(
            matches!(auth, CoreError::Api { status, .. } if status == StatusCode::UNAUTHORIZED),
            "auth must not become WaitFilterInvalid: {auth:?}"
        );
        let forbidden = classify_bind_core_error(CoreError::Api {
            status: StatusCode::FORBIDDEN,
            message: "no".into(),
        });
        assert!(matches!(
            forbidden,
            CoreError::Api {
                status: StatusCode::FORBIDDEN,
                ..
            }
        ));
        let timeout = classify_bind_core_error(CoreError::WaitTimeout("ops".into()));
        assert!(matches!(timeout, CoreError::WaitTimeout(_)));
        let invalid = classify_bind_core_error(CoreError::WaitFilterInvalid(
            "wait --after post not found: p1".into(),
        ));
        assert!(matches!(invalid, CoreError::WaitFilterInvalid(_)));
    }

    #[test]
    fn cancel_forward_aborts_on_drop() {
        futures_executor_block(async {
            let lease = CancellationToken::new();
            let wp = Cancel::new();
            let fwd = CancelForward::spawn(lease.clone(), wp.clone());
            drop(fwd);
            assert!(!wp.is_cancelled());
            assert!(!lease.is_cancelled());
        });
    }

    #[test]
    fn equivalent_selectors_share_canonical_subject() {
        assert_eq!(
            channel_subject("abc123").as_str(),
            channel_subject("abc123").as_str()
        );
        assert_ne!(channel_subject("abc123").as_str(), "channel:ops-updates");
    }

    #[tokio::test]
    async fn bind_baseline_cancel_releases_lease_for_replacement() {
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
        let registry = Arc::new(WaitOwnerRegistry::new());
        let old = registry
            .acquire("ch-1", "org", "ops", None, Duration::from_secs(5))
            .await
            .expect("admit");
        let old_id = old.wait_id.clone();
        let (session, guard) = old.into_guard();
        let parked = Arc::new(AtomicBool::new(false));
        let continued = Arc::new(AtomicU64::new(0));
        let hold = Arc::new(tokio::sync::Notify::new());
        let work = {
            let parked = Arc::clone(&parked);
            let continued = Arc::clone(&continued);
            let hold = Arc::clone(&hold);
            async move {
                race_bind_work(&session, async {
                    parked.store(true, Ordering::SeqCst);
                    hold.notified().await;
                    continued.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                .await
            }
        };
        let old_task = tokio::spawn(work);
        let mark = Instant::now() + Duration::from_millis(200);
        while !parked.load(Ordering::SeqCst) {
            assert!(Instant::now() < mark, "baseline work never parked");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        let replacing = {
            let registry = Arc::clone(&registry);
            let old_id = old_id.clone();
            tokio::spawn(async move {
                registry
                    .acquire("ch-1", "org", "ops", Some(&old_id), Duration::from_secs(5))
                    .await
            })
        };
        let old_result = old_task.await.expect("join old");
        assert!(
            matches!(old_result, Err(CoreError::WaitReplaced { ref wait_id, .. }) if wait_id == &old_id),
            "old waiter must surface WaitReplaced, got {old_result:?}"
        );
        drop(guard);
        let new_lease = replacing.await.expect("join replace").expect("rebind");
        assert_ne!(new_lease.wait_id, old_id);
        assert_eq!(
            continued.load(Ordering::SeqCst),
            0,
            "cancelled baseline must not continue provider work"
        );
        hold.notify_waiters();
        assert_eq!(continued.load(Ordering::SeqCst), 0);
        drop(new_lease);
    }

    #[test]
    fn wall_clock_is_monotonic_projection_of_origin() {
        let clock = WallClock::new();
        std::thread::sleep(Duration::from_millis(15));
        let now = clock.now();
        let projected = clock
            .origin_ts
            .saturating_add(clock.origin_instant.elapsed());
        let skew = now
            .duration_until(&projected)
            .max(projected.duration_until(&now));
        assert!(
            skew <= Duration::from_millis(5),
            "now() must be origin + Instant elapsed, skew={skew:?}"
        );
        let later = Instant::now() + Duration::from_secs(8);
        let budget = clock.project_deadline(later);
        let remaining = clock.now().duration_until(&budget);
        assert!(remaining <= Duration::from_secs(8));
        assert!(remaining >= Duration::from_secs(6));
    }

    #[test]
    fn content_digest_covers_every_sidecar_field() {
        let base = msg("p1", "alice", "hello");
        let base_digest = content_digest_for(&base).expect("digest").value;
        let mut username = base.clone();
        username.username = "bob".into();
        let mut root = base.clone();
        root.root_id = "other-root".into();
        let mut user = base.clone();
        user.user_id = "other-user".into();
        let mut body = base.clone();
        body.message = "goodbye".into();
        let mut created = base.clone();
        created.create_at = 99;
        let mut id = base.clone();
        id.id = "p-other".into();
        for (label, variant) in [
            ("id", id),
            ("username", username),
            ("root_id", root),
            ("user_id", user),
            ("message", body),
            ("create_at", created),
        ] {
            assert_ne!(
                content_digest_for(&variant).expect("digest").value,
                base_digest,
                "{label} must affect content digest"
            );
        }
    }

    #[test]
    fn malformed_registration_set_fails_closed() {
        let err = admit_raw(r#"{"message_type":"registration_set"}"#).unwrap_err();
        assert!(matches!(err, CoreError::WaitProviderDegraded { .. }));
    }

    #[test]
    fn malformed_outcome_fails_closed() {
        let err = admit_raw(r#"{"message_type":"live_wait_outcome"}"#).unwrap_err();
        assert!(matches!(err, CoreError::WaitProviderDegraded { .. }));
    }

    #[test]
    fn expired_lease_set_and_outcome_fail_closed_together() {
        let session = dummy_session("wait_lease");
        let (mut set, request) = build_live_documents(
            &session,
            "bot",
            "ch-canonical",
            Some("post-abc"),
            timestamp_now().saturating_add(Duration::from_secs(30)),
        )
        .expect("docs");
        set.registrations[0].lease_expires_at =
            Timestamp::parse("1970-01-01T00:00:01Z").expect("epoch");
        let docs = match admit_set_and_request("ops", set, request) {
            Ok(docs) => docs,
            Err(err) => {
                assert!(matches!(err, CoreError::WaitProviderDegraded { .. }));
                return;
            }
        };
        let outcome = LiveWaitOutcome {
            capabilities: docs.request.capabilities.clone(),
            message_id: IdToken::new(format!("{}:outcome", docs.request.message_id.as_str())),
            correlation_id: docs.request.correlation_id.clone(),
            created_at: timestamp_now(),
            actor_ref: docs.request.actor_ref.clone(),
            causation_id: Some(docs.request.message_id.clone()),
            grant_ref: None,
            verification_receipt_ref: None,
            policy_decision_ref: None,
            waiter_id: docs.request.waiter_id.clone(),
            request_ref: docs.request.message_id.clone(),
            completed_at: timestamp_now(),
            outcome_kind: OutcomeKind::LogicalDeadman,
            logical_deadline: Some(docs.request.logical_deadline.clone()),
            events: Some(Vec::new()),
            proposed_next_anchor: None,
            coverage_complete: Some(true),
            arms: None,
            reason_code: None,
        };
        let err = admit_wait_result(&docs, outcome).unwrap_err();
        assert!(
            matches!(err, CoreError::WaitProviderDegraded { .. }),
            "cross-document lease/admission must fail closed as internal, got {err:?}"
        );
    }

    #[tokio::test]
    async fn follow_callback_waits_for_actual_writer_ack() {
        let (stream, mut records) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            emit_follow_event(
                &stream,
                WaitFollowEvent::armed("wait_0123456789abcdef0123456789abcdef", None),
            )
            .await
        });
        let record = records.recv().await.expect("record");
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert!(
            !task.is_finished(),
            "runner callback advanced before the UDS writer acknowledged"
        );
        record.written.send(Ok(())).expect("writer ack");
        task.await.expect("task").expect("emit");
    }

    #[tokio::test]
    async fn follow_bind_teardown_holds_owner_until_terminal_write_ack() {
        let registry = Arc::new(WaitOwnerRegistry::new());
        let lease = registry
            .acquire(
                "channel-1",
                "org-lanytehq",
                "release-floor",
                None,
                Duration::from_secs(5),
            )
            .await
            .expect("lease");
        let wait_id = lease.wait_id.clone();
        let (_session, guard) = lease.into_guard();
        let release = Arc::new(LeaseRelease::new(guard));
        let bind = ChanvoyBind::new(
            IdToken::new(format!("reg:{wait_id}")),
            channel_subject("channel-1"),
            Anchor {
                kind: AnchorKind::ProviderOpaque,
                value: IdToken::new(EMPTY_AT_ARM_CURSOR),
            },
            HashSet::new(),
            Arc::clone(&release),
            CancellationToken::new(),
            Some(FollowBindState {
                backlog_ids: HashSet::new(),
                rx: None,
            }),
        );
        drop(bind);
        assert!(
            registry.snapshot("channel-1").is_some(),
            "normal waitprims bind teardown released the held owner"
        );

        let (stream, mut records) = tokio::sync::mpsc::channel(1);
        let emit_wait_id = wait_id.clone();
        let task = tokio::spawn(async move {
            emit_follow_event(
                &stream,
                WaitFollowEvent::terminal(emit_wait_id, WaitFollowEventKind::Deadman),
            )
            .await
        });
        let record = records.recv().await.expect("terminal record");
        let conflict = registry
            .acquire(
                "channel-1",
                "org-lanytehq",
                "release-floor",
                None,
                Duration::from_secs(1),
            )
            .await;
        assert!(matches!(conflict, Err(CoreError::WaitAlreadyActive { .. })));
        record.written.send(Ok(())).expect("terminal write ack");
        task.await.expect("task").expect("emit");
        release.release();
        assert!(
            registry
                .acquire(
                    "channel-1",
                    "org-lanytehq",
                    "release-floor",
                    None,
                    Duration::from_secs(1),
                )
                .await
                .is_ok(),
            "owner remained held after terminal write ack and explicit release"
        );
    }
}
