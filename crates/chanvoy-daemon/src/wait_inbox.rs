//! Inbox wait: any direct message to this bot.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use chanvoy_core::{
    cursor_uncertain, inbox_capacity, peer_user_id_from_dm_name, CoreError, DaemonEvent,
    DaemonEventPayloadInner, DirectCatalogEntry, InboundEventPayload, InboxCursorV1, Message,
    WaitFollowMode, WaitInboxFailureReason, WaitInboxFollowEvent, WaitInboxFollowEventKind,
    WaitInboxFollowResult, WaitInboxFollowResultKind, WaitInboxV1Result, INBOX_MAX_BACKFILL,
    INBOX_MAX_DMS,
};
use tokio::sync::broadcast;
use tokio::time::{timeout_at, Instant};

use crate::wait::{inbound_to_message, WaitPredicate};
use crate::wait_owner::{WaitGuard, WaitSession};
use crate::AppState;

pub struct InboxFollowStreamRecord {
    pub event: WaitInboxFollowEvent,
    pub written: tokio::sync::oneshot::Sender<Result<(), String>>,
}

pub type InboxFollowStreamSender = tokio::sync::mpsc::Sender<InboxFollowStreamRecord>;

struct InboxArmGuard {
    gate: Arc<AtomicU64>,
}

impl InboxArmGuard {
    fn arm(gate: &Arc<AtomicU64>) -> Self {
        gate.fetch_add(1, Ordering::SeqCst);
        Self {
            gate: Arc::clone(gate),
        }
    }
}

impl Drop for InboxArmGuard {
    fn drop(&mut self) {
        self.gate.fetch_sub(1, Ordering::SeqCst);
    }
}

pub struct WaitInboxRequest<'a> {
    pub timeout_secs: u64,
    pub contains: Option<&'a str>,
    pub pattern: Option<&'a str>,
    pub mention: bool,
    pub after: Option<&'a str>,
    pub replace_wait_id: Option<&'a str>,
    pub deadline: Instant,
}

#[derive(Clone)]
struct CataloguedDm {
    id: String,
    name: String,
    peer_username: String,
}

pub async fn wait_with_params_inbox(
    state: &AppState,
    req: WaitInboxRequest<'_>,
) -> Result<WaitInboxV1Result, CoreError> {
    let outcome = run_inbox(state, req, None).await?;
    match outcome {
        InboxOutcome::Match(result) => Ok(result),
        InboxOutcome::Follow(_) => Err(cursor_uncertain("inbox one-shot produced a follow result")),
    }
}

pub async fn wait_with_params_inbox_follow(
    state: &AppState,
    req: WaitInboxRequest<'_>,
    stream: InboxFollowStreamSender,
) -> Result<WaitInboxFollowResult, CoreError> {
    let outcome = run_inbox(state, req, Some(stream)).await?;
    match outcome {
        InboxOutcome::Follow(result) => Ok(result),
        InboxOutcome::Match(_) => Err(cursor_uncertain("inbox follow produced a one-shot result")),
    }
}

enum InboxOutcome {
    Match(WaitInboxV1Result),
    Follow(WaitInboxFollowResult),
}

async fn run_inbox(
    state: &AppState,
    req: WaitInboxRequest<'_>,
    stream: Option<InboxFollowStreamSender>,
) -> Result<InboxOutcome, CoreError> {
    crate::wait::validate_wait_timeout_secs(req.timeout_secs)?;
    WaitPredicate::compile(
        "pending",
        "pending",
        req.contains,
        req.pattern,
        req.mention,
        &state.profile.bot_username,
    )?;
    let decoded_after = match req.after {
        Some(raw) => Some(InboxCursorV1::decode(
            raw,
            &state.profile.name,
            &state.my_user_id,
        )?),
        None => None,
    };
    let reconnects_at_arm = crate::wait::admit_push_observation(state, "inbox").await?;

    let _arm = InboxArmGuard::arm(&state.inbox_armed);
    let mut rx = state.event_bus.subscribe();
    let mut bus_buffer: VecDeque<Arc<DaemonEvent>> = VecDeque::new();
    drain_inbox_bus(&mut rx, &mut bus_buffer)?;

    let catalog = provider_list(state, req.deadline).await?;
    if catalog.len() > INBOX_MAX_DMS {
        return Err(inbox_capacity(format!(
            "inbox catalog exceeds {INBOX_MAX_DMS} direct channels"
        )));
    }

    let mut dms = resolve_catalog(state, &catalog, req.deadline).await?;
    drain_inbox_bus(&mut rx, &mut bus_buffer)?;
    if let Some(cursor) = decoded_after.as_ref() {
        prove_inbox_cursor(state, cursor, &catalog, req.deadline).await?;
    }

    let remaining = req.deadline.saturating_duration_since(Instant::now());
    let lease = state
        .wait_owners
        .acquire_inbox(req.replace_wait_id, remaining)
        .await?;
    state.wait_owners.note_arm();
    let (session, _guard) = lease.into_guard();
    let _guard: WaitGuard = _guard;

    let mut cursor = match decoded_after {
        Some(cursor) => cursor,
        None => InboxCursorV1::empty(&state.profile.name, &state.my_user_id),
    };
    let mut proven = req.after.map(str::to_string);
    let predicate = WaitPredicate::compile(
        &state.my_user_id,
        "inbox",
        req.contains,
        req.pattern,
        req.mention,
        &state.profile.bot_username,
    )?;

    if let Some(stream) = stream.as_ref() {
        emit_inbox(
            stream,
            WaitInboxFollowEvent::armed(session.wait_id.clone(), session.replaced_wait_id.clone()),
        )
        .await?;
    }

    let armed = run_inbox_armed(
        state,
        &req,
        stream.as_ref(),
        &session,
        &mut cursor,
        &mut proven,
        &predicate,
        &mut dms,
        &catalog,
        &mut rx,
        &mut bus_buffer,
        reconnects_at_arm,
    )
    .await;
    if let Err(err) = &armed {
        if stream.is_some() {
            let _ = fail_outcome(
                &session,
                stream.as_ref(),
                proven.clone(),
                inbox_fail_reason(err),
                CoreError::WaitFilterInvalid("inbox follow failed".into()),
            )
            .await;
        }
    }
    armed
}

#[allow(clippy::too_many_arguments)]
async fn run_inbox_armed(
    state: &AppState,
    req: &WaitInboxRequest<'_>,
    stream: Option<&InboxFollowStreamSender>,
    session: &WaitSession,
    cursor: &mut InboxCursorV1,
    proven: &mut Option<String>,
    predicate: &WaitPredicate,
    dms: &mut HashMap<String, CataloguedDm>,
    catalog: &[DirectCatalogEntry],
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus_buffer: &mut VecDeque<Arc<DaemonEvent>>,
    mut reconnects: u64,
) -> Result<InboxOutcome, CoreError> {
    if req.after.is_none() {
        let live_ids = buffered_post_ids(bus_buffer);
        *cursor = establish_arm_cursor(state, catalog, dms, req.deadline, &live_ids).await?;
        *proven = Some(cursor.encode()?);
    }
    let mut observer = cursor.clone();

    let backfill = collect_backfill(state, dms, &mut observer, predicate, req.deadline).await?;
    for (entry, message) in backfill {
        match deliver(
            state,
            session,
            stream,
            cursor,
            proven,
            &entry,
            message,
            WaitFollowMode::Backlog,
        )
        .await?
        {
            Some(outcome) if stream.is_none() => return Ok(outcome),
            _ => {}
        }
    }

    let buffered = std::mem::take(bus_buffer);
    for event in buffered {
        if let Some(outcome) = handle_event(
            state,
            session,
            stream,
            dms,
            cursor,
            &mut observer,
            proven,
            predicate,
            event,
            req.deadline,
        )
        .await?
        {
            if stream.is_none() {
                return Ok(outcome);
            }
        }
    }

    crate::wait::refuse_current_ws_failure(state, "inbox").await?;
    if current_reconnects(state).await > reconnects {
        reconnects = current_reconnects(state).await;
        recatalog(state, dms, req.deadline).await?;
        let backfill = collect_backfill(state, dms, &mut observer, predicate, req.deadline).await?;
        for (entry, message) in backfill {
            match deliver(
                state,
                session,
                stream,
                cursor,
                proven,
                &entry,
                message,
                WaitFollowMode::Backlog,
            )
            .await?
            {
                Some(outcome) if stream.is_none() => return Ok(outcome),
                _ => {}
            }
        }
    }

    loop {
        if Instant::now() >= req.deadline {
            return timeout_outcome(state, session, stream, proven.clone()).await;
        }
        if session.cancel.is_cancelled() {
            return replaced_outcome(session, stream, proven.clone()).await;
        }
        let now_reconnects = current_reconnects(state).await;
        if now_reconnects > reconnects {
            reconnects = now_reconnects;
            recatalog(state, dms, req.deadline).await?;
            let backfill =
                collect_backfill(state, dms, &mut observer, predicate, req.deadline).await?;
            for (entry, message) in backfill {
                match deliver(
                    state,
                    session,
                    stream,
                    cursor,
                    proven,
                    &entry,
                    message,
                    WaitFollowMode::Backlog,
                )
                .await?
                {
                    Some(outcome) if stream.is_none() => return Ok(outcome),
                    _ => {}
                }
            }
        }

        match timeout_at(req.deadline, rx.recv()).await {
            Ok(Ok(event)) => {
                if let Some(outcome) = handle_event(
                    state,
                    session,
                    stream,
                    dms,
                    cursor,
                    &mut observer,
                    proven,
                    predicate,
                    event,
                    req.deadline,
                )
                .await?
                {
                    if stream.is_none() {
                        return Ok(outcome);
                    }
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: "inbox".into(),
                    message: "inbox event bus lagged".into(),
                });
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: "inbox".into(),
                    message: "inbox event bus closed".into(),
                });
            }
            Err(_) => {
                return timeout_outcome(state, session, stream, proven.clone()).await;
            }
        }
    }
}

async fn provider_list(
    state: &AppState,
    deadline: Instant,
) -> Result<Vec<DirectCatalogEntry>, CoreError> {
    crate::wait::provider_retry(state, "inbox", deadline, || async {
        state.client.list_direct_channels().await
    })
    .await
}

async fn resolve_catalog(
    state: &AppState,
    catalog: &[DirectCatalogEntry],
    deadline: Instant,
) -> Result<HashMap<String, CataloguedDm>, CoreError> {
    let remaining = deadline.saturating_duration_since(Instant::now());
    if remaining.is_zero() {
        return Err(cursor_uncertain(
            "inbox catalog peer lookup exceeded the wait deadline",
        ));
    }
    let work = async {
        let mut dms = HashMap::new();
        for entry in catalog {
            let peer_id = peer_user_id_from_dm_name(&entry.name, &state.my_user_id)
                .ok_or_else(|| cursor_uncertain("inbox catalog entry is not a DM for this bot"))?;
            let peer_username = state.client.required_username(peer_id).await?;
            dms.insert(
                entry.id.clone(),
                CataloguedDm {
                    id: entry.id.clone(),
                    name: entry.name.clone(),
                    peer_username,
                },
            );
        }
        Ok(dms)
    };
    tokio::time::timeout(remaining, work)
        .await
        .map_err(|_| cursor_uncertain("inbox catalog peer lookup exceeded the wait deadline"))?
}

async fn prove_inbox_cursor(
    state: &AppState,
    cursor: &InboxCursorV1,
    catalog: &[DirectCatalogEntry],
    deadline: Instant,
) -> Result<(), CoreError> {
    if cursor.watermark == 0 {
        if !cursor.observed_ids.is_empty() {
            return Err(cursor_uncertain(
                "inbox cursor observed ids at zero watermark are unprovable",
            ));
        }
        return Ok(());
    }
    if cursor.observed_ids.is_empty() {
        return Err(cursor_uncertain(
            "inbox cursor positive watermark with empty observed-id set is unprovable",
        ));
    }
    let catalog_max = catalog
        .iter()
        .map(|entry| entry.last_post_at)
        .max()
        .unwrap_or(0);
    if cursor.watermark > catalog_max {
        return Err(cursor_uncertain(
            "inbox cursor watermark is ahead of the authenticated catalog",
        ));
    }
    let mut remaining = INBOX_MAX_BACKFILL;
    let mut found = HashSet::new();
    let mut contradictory = false;
    let mut saw_watermark_post = false;
    for entry in catalog {
        let since = cursor.watermark.saturating_sub(1);
        let (posts, consumed) = crate::wait::provider_retry(state, "inbox", deadline, || {
            let id = entry.id.clone();
            async move {
                state
                    .client
                    .inbox_history_since(&id, since, remaining)
                    .await
            }
        })
        .await?;
        remaining = remaining.saturating_sub(consumed);
        for message in posts {
            if message.create_at == cursor.watermark {
                saw_watermark_post = true;
            }
            if !cursor.observed_ids.contains(&message.id) {
                continue;
            }
            if message.create_at != cursor.watermark {
                contradictory = true;
            } else {
                found.insert(message.id);
            }
        }
    }
    if !saw_watermark_post {
        return Err(cursor_uncertain(
            "inbox cursor watermark is not present in authenticated history",
        ));
    }
    if contradictory {
        return Err(cursor_uncertain(
            "inbox cursor observed ids contradict authenticated history",
        ));
    }
    if found.len() != cursor.observed_ids.len() {
        return Err(cursor_uncertain(
            "inbox cursor observed ids are not present in authenticated history",
        ));
    }
    Ok(())
}

async fn recatalog(
    state: &AppState,
    dms: &mut HashMap<String, CataloguedDm>,
    deadline: Instant,
) -> Result<(), CoreError> {
    let catalog = provider_list(state, deadline).await?;
    if catalog.len() > INBOX_MAX_DMS {
        return Err(inbox_capacity(format!(
            "inbox catalog exceeds {INBOX_MAX_DMS} direct channels"
        )));
    }
    *dms = resolve_catalog(state, &catalog, deadline).await?;
    Ok(())
}

fn inbox_fail_reason(err: &CoreError) -> WaitInboxFailureReason {
    match err {
        CoreError::WaitFilterInvalid(message)
            if message.contains("exceeds") || message.contains("capacity") =>
        {
            WaitInboxFailureReason::Capacity
        }
        CoreError::WaitFilterInvalid(_) => WaitInboxFailureReason::CursorUncertain,
        CoreError::WaitProviderDegraded { message, .. }
            if message.contains("lag") || message.contains("overflow") =>
        {
            WaitInboxFailureReason::ProviderOverflow
        }
        CoreError::WaitProviderDegraded { .. } => WaitInboxFailureReason::ProviderFailed,
        _ => WaitInboxFailureReason::ProviderFailed,
    }
}

fn buffered_post_ids(buffer: &VecDeque<Arc<DaemonEvent>>) -> HashSet<String> {
    buffer
        .iter()
        .filter_map(|event| inbound_payload(event).map(|payload| payload.post_id.clone()))
        .collect()
}

async fn establish_arm_cursor(
    state: &AppState,
    catalog: &[DirectCatalogEntry],
    dms: &HashMap<String, CataloguedDm>,
    deadline: Instant,
    live_ids: &HashSet<String>,
) -> Result<InboxCursorV1, CoreError> {
    let mut cursor = InboxCursorV1::empty(&state.profile.name, &state.my_user_id);
    let watermark = catalog
        .iter()
        .map(|entry| entry.last_post_at)
        .max()
        .unwrap_or(0);
    if watermark <= 0 {
        return Ok(cursor);
    }
    cursor.watermark = watermark;
    let mut remaining = INBOX_MAX_BACKFILL;
    for entry in catalog
        .iter()
        .filter(|entry| entry.last_post_at == watermark)
    {
        let Some(dm) = dms.get(&entry.id) else {
            continue;
        };
        let since = watermark.saturating_sub(1);
        let (posts, consumed) = crate::wait::provider_retry(state, "inbox", deadline, || {
            let id = dm.id.clone();
            async move {
                state
                    .client
                    .inbox_history_since(&id, since, remaining)
                    .await
            }
        })
        .await?;
        remaining = remaining.saturating_sub(consumed);
        for message in posts {
            if message.create_at == watermark && !live_ids.contains(&message.id) {
                cursor.observed_ids.insert(message.id);
            }
        }
        if cursor.observed_ids.len() > chanvoy_core::INBOX_MAX_WATERMARK_IDS {
            return Err(cursor_uncertain(
                "inbox equal-watermark observed-id set exceeds 128",
            ));
        }
    }
    Ok(cursor)
}

async fn collect_backfill(
    state: &AppState,
    dms: &HashMap<String, CataloguedDm>,
    scan: &mut InboxCursorV1,
    predicate: &WaitPredicate,
    deadline: Instant,
) -> Result<Vec<(CataloguedDm, Message)>, CoreError> {
    let mut admitted = Vec::new();
    let mut remaining = INBOX_MAX_BACKFILL;
    for entry in dms.values() {
        let since = scan.watermark.saturating_sub(1);
        let (posts, consumed) = crate::wait::provider_retry(state, "inbox", deadline, || {
            let id = entry.id.clone();
            async move {
                state
                    .client
                    .inbox_history_since(&id, since, remaining)
                    .await
            }
        })
        .await?;
        remaining = remaining.saturating_sub(consumed);
        for message in posts {
            if !scan.admits(message.create_at, &message.id) {
                continue;
            }
            *scan = scan.advance(message.create_at, &message.id)?;
            if predicate.matches_message(&message) {
                admitted.push((entry.clone(), message));
            }
        }
    }
    admitted.sort_by(|left, right| {
        left.1
            .create_at
            .cmp(&right.1.create_at)
            .then_with(|| left.1.id.cmp(&right.1.id))
    });
    Ok(admitted)
}

#[allow(clippy::too_many_arguments)]
async fn handle_event(
    state: &AppState,
    session: &WaitSession,
    stream: Option<&InboxFollowStreamSender>,
    dms: &mut HashMap<String, CataloguedDm>,
    cursor: &mut InboxCursorV1,
    observer: &mut InboxCursorV1,
    proven: &mut Option<String>,
    predicate: &WaitPredicate,
    event: Arc<DaemonEvent>,
    deadline: Instant,
) -> Result<Option<InboxOutcome>, CoreError> {
    let Some(payload) = inbound_payload(&event) else {
        return Ok(None);
    };
    if payload.sender_id == state.my_user_id {
        return Ok(None);
    }
    let known = dms.contains_key(&payload.channel_id);
    if !known && payload.channel_type != "D" {
        return Ok(None);
    }
    let entry = if let Some(existing) = dms.get(&payload.channel_id) {
        existing.clone()
    } else {
        if payload.channel_type != "D" {
            return Ok(None);
        }
        if dms.len() >= INBOX_MAX_DMS {
            return Err(inbox_capacity(format!(
                "inbox catalog exceeds {INBOX_MAX_DMS} direct channels"
            )));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(cursor_uncertain("new DM lookup exceeded the wait deadline"));
        }
        let looked_up = tokio::time::timeout(
            remaining,
            state.client.get_direct_channel(&payload.channel_id),
        )
        .await
        .map_err(|_| cursor_uncertain("new DM lookup exceeded the wait deadline"))??;
        let peer_id = peer_user_id_from_dm_name(&looked_up.name, &state.my_user_id)
            .ok_or_else(|| cursor_uncertain("new DM lookup failed"))?;
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(cursor_uncertain("new DM lookup exceeded the wait deadline"));
        }
        let peer_username =
            tokio::time::timeout(remaining, state.client.required_username(peer_id))
                .await
                .map_err(|_| cursor_uncertain("new DM lookup exceeded the wait deadline"))??;
        let entry = CataloguedDm {
            id: looked_up.id,
            name: looked_up.name,
            peer_username,
        };
        dms.insert(entry.id.clone(), entry.clone());
        entry
    };
    let message = inbound_to_message(payload);
    if !observer.admits(message.create_at, &message.id) {
        return Ok(None);
    }
    *observer = observer.advance(message.create_at, &message.id)?;
    if !predicate.matches_message(&message) {
        return Ok(None);
    }
    deliver(
        state,
        session,
        stream,
        cursor,
        proven,
        &entry,
        message,
        WaitFollowMode::Live,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn deliver(
    state: &AppState,
    session: &WaitSession,
    stream: Option<&InboxFollowStreamSender>,
    cursor: &mut InboxCursorV1,
    proven: &mut Option<String>,
    entry: &CataloguedDm,
    message: Message,
    mode: WaitFollowMode,
) -> Result<Option<InboxOutcome>, CoreError> {
    let next = cursor.advance(message.create_at, &message.id)?;
    let encoded = next.encode()?;
    if let Some(stream) = stream {
        let event = WaitInboxFollowEvent::message(
            session.wait_id.clone(),
            mode,
            entry.peer_username.clone(),
            entry.name.clone(),
            encoded.clone(),
            message.clone(),
            false,
        )
        .map_err(cursor_uncertain)?;
        emit_inbox(stream, event).await?;
        *cursor = next;
        *proven = Some(encoded);
        return Ok(None);
    }
    *cursor = next;
    *proven = Some(encoded.clone());
    let _ = state;
    Ok(Some(InboxOutcome::Match(WaitInboxV1Result {
        peer_username: entry.peer_username.clone(),
        dm_name: entry.name.clone(),
        matched_post_id: message.id.clone(),
        next_inbox_cursor: encoded,
        messages: vec![message],
        wait_id: Some(session.wait_id.clone()),
        replaced_wait_id: session.replaced_wait_id.clone(),
    })))
}

async fn emit_inbox(
    stream: &InboxFollowStreamSender,
    event: WaitInboxFollowEvent,
) -> Result<(), CoreError> {
    let (written, receipt) = tokio::sync::oneshot::channel();
    stream
        .send(InboxFollowStreamRecord { event, written })
        .await
        .map_err(|_| CoreError::WaitProviderDegraded {
            channel: "inbox".into(),
            message: "inbox follow sink closed".into(),
        })?;
    match receipt.await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(CoreError::WaitProviderDegraded {
            channel: "inbox".into(),
            message: format!("inbox follow sink write failed: {err}"),
        }),
        Err(_) => Err(CoreError::WaitProviderDegraded {
            channel: "inbox".into(),
            message: "inbox follow sink acknowledgement dropped".into(),
        }),
    }
}

async fn timeout_outcome(
    state: &AppState,
    session: &WaitSession,
    stream: Option<&InboxFollowStreamSender>,
    proven: Option<String>,
) -> Result<InboxOutcome, CoreError> {
    if crate::wait::refuse_current_ws_failure(state, "inbox")
        .await
        .is_err()
    {
        return Err(CoreError::WaitProviderDegraded {
            channel: "inbox".into(),
            message: "websocket observation is currently unavailable; inbox deadline is not a clean deadman".into(),
        });
    }
    if let Some(stream) = stream {
        emit_inbox(
            stream,
            WaitInboxFollowEvent {
                schema: chanvoy_core::WaitInboxFollowSchema::V1,
                wait_id: session.wait_id.clone(),
                kind: WaitInboxFollowEventKind::Deadman {
                    inbox_cursor: proven.clone(),
                },
            },
        )
        .await?;
        return Ok(InboxOutcome::Follow(WaitInboxFollowResult {
            wait_id: session.wait_id.clone(),
            kind: WaitInboxFollowResultKind::Deadman {
                inbox_cursor: proven,
            },
        }));
    }
    Err(CoreError::WaitTimeout("inbox".into()))
}

async fn replaced_outcome(
    session: &WaitSession,
    stream: Option<&InboxFollowStreamSender>,
    proven: Option<String>,
) -> Result<InboxOutcome, CoreError> {
    let replaced_by = session.replaced_by_id();
    if let Some(stream) = stream {
        emit_inbox(
            stream,
            WaitInboxFollowEvent {
                schema: chanvoy_core::WaitInboxFollowSchema::V1,
                wait_id: session.wait_id.clone(),
                kind: WaitInboxFollowEventKind::Replaced {
                    replaced_by_wait_id: replaced_by.clone(),
                    inbox_cursor: proven.clone(),
                },
            },
        )
        .await?;
        return Ok(InboxOutcome::Follow(WaitInboxFollowResult {
            wait_id: session.wait_id.clone(),
            kind: WaitInboxFollowResultKind::Replaced {
                replaced_by_wait_id: replaced_by,
                inbox_cursor: proven,
            },
        }));
    }
    Err(CoreError::WaitReplaced {
        wait_id: session.wait_id.clone(),
        replaced_by_wait_id: replaced_by,
    })
}

async fn fail_outcome(
    session: &WaitSession,
    stream: Option<&InboxFollowStreamSender>,
    proven: Option<String>,
    reason: WaitInboxFailureReason,
    err: CoreError,
) -> Result<InboxOutcome, CoreError> {
    if let Some(stream) = stream {
        let _ = emit_inbox(
            stream,
            WaitInboxFollowEvent {
                schema: chanvoy_core::WaitInboxFollowSchema::V1,
                wait_id: session.wait_id.clone(),
                kind: WaitInboxFollowEventKind::Failed {
                    reason_code: reason,
                    inbox_cursor: proven,
                },
            },
        )
        .await;
    }
    Err(err)
}

fn inbound_payload(event: &DaemonEvent) -> Option<&InboundEventPayload> {
    match &event.payload {
        DaemonEventPayloadInner::Inbound(payload)
            if matches!(
                event.kind,
                chanvoy_core::DaemonEventKind::InboundMessage
                    | chanvoy_core::DaemonEventKind::InboundMention
            ) =>
        {
            Some(payload)
        }
        _ => None,
    }
}

fn drain_inbox_bus(
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    buffer: &mut VecDeque<Arc<DaemonEvent>>,
) -> Result<(), CoreError> {
    loop {
        match rx.try_recv() {
            Ok(event) => buffer.push_back(event),
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: "inbox".into(),
                    message: "inbox event bus lagged".into(),
                });
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: "inbox".into(),
                    message: "inbox event bus closed".into(),
                });
            }
        }
    }
}

async fn current_reconnects(state: &AppState) -> u64 {
    let holder = state.ws_state_holder.lock().await;
    holder
        .as_ref()
        .map(|ws| ws.reconnect_count.load(Ordering::Relaxed))
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wait_owner::WaitOwnerRegistry;
    use crate::AppState;
    use chanvoy_core::{
        canonical_dm_name, AttentionState, CapabilityClass, CredentialMode, DaemonEventKind,
        EventBus, InboundEventPayload, MattermostClient, Profile, Provider, WsConnectionState,
        WsState, INBOX_PAGE_SIZE,
    };
    use std::sync::atomic::AtomicBool;
    use std::time::Duration;
    use tokio::sync::Mutex;
    use tokio::time::Instant;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    const BOT_ID: &str = "userid00000000000000000000";
    const BOT_USER: &str = "agent-bravo-devlead";
    const PEER_A_ID: &str = "userid00000000000000000001";
    const PEER_A: &str = "dave-3leaps";
    const PEER_C_ID: &str = "userid00000000000000000003";
    const PEER_C: &str = "agent-secrev";
    const POST_A: &str = "postid0000000000000000000a";
    const POST_C: &str = "postid0000000000000000000c";
    const DM_A: &str = "dmid000000000000000000000a";
    const DM_C: &str = "dmid000000000000000000000c";

    fn dm_a() -> String {
        canonical_dm_name(BOT_ID, PEER_A_ID)
    }

    fn dm_c() -> String {
        canonical_dm_name(BOT_ID, PEER_C_ID)
    }

    async fn healthy_state(server: &MockServer) -> AppState {
        let profile = Profile {
            name: "wait-inbox-engine".into(),
            role: "test".into(),
            scope: "test".into(),
            provider: Provider::Mattermost,
            bot_username: BOT_USER.into(),
            team_name: "org".into(),
            server_url: server.uri(),
            env_name: "TEST_TOKEN".into(),
            env_file: None,
            credential_mode: CredentialMode::EnvName,
            capability_class: CapabilityClass::Standard,
            monitored_channels: Vec::new(),
            ipc: None,
            reduce: None,
        };
        let client = MattermostClient::new(&profile, "test-token".into()).expect("client");
        let ws = Arc::new(WsState::new());
        ws.set_state(WsConnectionState::Healthy).await;
        AppState {
            profile,
            client,
            socket_path: "/tmp/chanvoy-inbox-test.sock".into(),
            my_user_id: BOT_ID.into(),
            event_bus: Arc::new(EventBus::new(32)),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
            ws_state_holder: Arc::new(Mutex::new(Some(ws))),
            ipc_state: None,
            attention_state: Arc::new(Mutex::new(AttentionState::default())),
            identity_drift: Arc::new(AtomicBool::new(false)),
            reduce_writer: None,
            wait_owners: Arc::new(WaitOwnerRegistry::new()),
            poll_cursors: crate::waitprims_poll::PollCursorStore::for_test("wait-inbox-engine"),
            fanin_replay: crate::waitprims_fanin::FanInReplayStore::new(),
            inbox_armed: Arc::new(AtomicU64::new(0)),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn inbound(
        channel_id: &str,
        channel_name: &str,
        channel_type: &str,
        post_id: &str,
        user_id: &str,
        username: &str,
        create_at: i64,
        message: &str,
    ) -> DaemonEvent {
        DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "wait-inbox-engine".into(),
                provider: Provider::Mattermost,
                channel_id: channel_id.into(),
                channel_name: channel_name.into(),
                channel_type: channel_type.into(),
                post_id: post_id.into(),
                root_id: post_id.into(),
                sender_id: user_id.into(),
                sender_username: username.into(),
                message: message.into(),
                create_at,
                received_at: create_at,
                mentioned: false,
                mention_user_ids: None,
            }),
        }
    }

    async fn mount_baseline(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/v4/users/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": BOT_ID,
                "username": BOT_USER,
                "is_bot": true
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{PEER_A_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": PEER_A_ID,
                "username": PEER_A
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{PEER_C_ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": PEER_C_ID,
                "username": PEER_C
            })))
            .mount(server)
            .await;
    }

    async fn mount_one_dm_catalog(server: &MockServer, delay_ms: u64) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_millis(delay_ms))
                    .set_body_json(serde_json::json!([{
                        "id": DM_A,
                        "name": dm_a(),
                        "type": "D",
                        "last_post_at": 1_780_000_000_100i64
                    }])),
            )
            .mount(server)
            .await;
    }

    async fn mount_posts(
        server: &MockServer,
        channel_id: &str,
        post_id: &str,
        user_id: &str,
        create_at: i64,
        body: &str,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{channel_id}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    post_id: {
                        "id": post_id,
                        "channel_id": channel_id,
                        "user_id": user_id,
                        "message": body,
                        "create_at": create_at,
                        "root_id": ""
                    }
                }
            })))
            .mount(server)
            .await;
    }

    struct CatalogPages {
        total: usize,
    }

    impl Respond for CatalogPages {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let page = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "page")
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(0);
            let start = page.saturating_mul(INBOX_PAGE_SIZE);
            if start >= self.total {
                return ResponseTemplate::new(200).set_body_json(serde_json::json!([]));
            }
            let end = (start + INBOX_PAGE_SIZE).min(self.total);
            let rows: Vec<serde_json::Value> = (start..end)
                .map(|index| {
                    let n = index + 1;
                    let peer = format!("userid{n:020}");
                    serde_json::json!({
                        "id": format!("dmid{n:022}"),
                        "name": canonical_dm_name(BOT_ID, &peer),
                        "type": "D",
                        "last_post_at": 1
                    })
                })
                .collect();
            ResponseTemplate::new(200).set_body_json(rows)
        }
    }

    struct HistoryPages {
        count: usize,
        user_id: String,
        channel_id: String,
        start_ts: i64,
        prefix: char,
    }

    impl Respond for HistoryPages {
        fn respond(&self, request: &Request) -> ResponseTemplate {
            let page = request
                .url
                .query_pairs()
                .find(|(key, _)| key == "page")
                .and_then(|(_, value)| value.parse::<usize>().ok())
                .unwrap_or(0);
            let start = page.saturating_mul(INBOX_PAGE_SIZE);
            if start >= self.count {
                return ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "posts": {}
                }));
            }
            let end = (start + INBOX_PAGE_SIZE).min(self.count);
            let mut posts = serde_json::Map::new();
            for i in start..end {
                let pid = format!("{}{i:025}", self.prefix);
                posts.insert(
                    pid.clone(),
                    serde_json::json!({
                        "id": pid,
                        "channel_id": self.channel_id,
                        "user_id": self.user_id,
                        "message": "x",
                        "create_at": self.start_ts + i as i64,
                        "root_id": ""
                    }),
                );
            }
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "posts": posts }))
        }
    }

    async fn mount_two_dm_catalog(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": DM_A,
                    "name": dm_a(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_100i64
                },
                {
                    "id": DM_C,
                    "name": dm_c(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_200i64
                }
            ])))
            .mount(server)
            .await;
    }

    async fn mount_history_pages(
        server: &MockServer,
        channel_id: &str,
        count: usize,
        user_id: &str,
        prefix: char,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{channel_id}/posts")))
            .respond_with(HistoryPages {
                count,
                user_id: user_id.to_string(),
                channel_id: channel_id.to_string(),
                start_ts: 1_780_000_000_100,
                prefix,
            })
            .mount(server)
            .await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn degraded_ws_refuses_before_catalog() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&server)
            .await;
        let mut state = healthy_state(&server).await;
        {
            let ws = state.ws_state_holder.lock().await.clone().unwrap();
            ws.set_state(WsConnectionState::Disconnected).await;
            ws.set_error("transport").await;
        }
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: None,
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitProviderDegraded { .. }),
            "{err}"
        );
        let _ = &mut state;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn subscribe_post_catalog_delivers_once() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 120).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_100, "race").await;
        let state = Arc::new(healthy_state(&server).await);
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(30)).await;
        state.event_bus.emit(inbound(
            DM_A,
            &dm_a(),
            "D",
            POST_A,
            PEER_A_ID,
            PEER_A,
            1_780_000_000_100,
            "race",
        ));
        let result = task.await.unwrap().expect("match");
        assert_eq!(result.matched_post_id, POST_A);
        assert_eq!(result.peer_username, PEER_A);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn empty_type_team_event_is_ignored_then_typed_dm_wakes() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        Mock::given(method("GET"))
            .and(path("/api/v4/channels/teamchan000000000000000001"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "teamchan000000000000000001",
                "name": "town-square",
                "type": "O",
                "last_post_at": 1
            })))
            .expect(0)
            .mount(&server)
            .await;
        let state = Arc::new(healthy_state(&server).await);
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        state.event_bus.emit(inbound(
            "teamchan000000000000000001",
            "town-square",
            "",
            "postid0000000000000000000t",
            "userid00000000000000000009",
            "human",
            1_780_000_000_900,
            "team noise",
        ));
        tokio::time::sleep(Duration::from_millis(20)).await;
        state.event_bus.emit(inbound(
            DM_A,
            &dm_a(),
            "D",
            POST_A,
            PEER_A_ID,
            PEER_A,
            1_780_000_000_200,
            "real dm",
        ));
        let result = task.await.unwrap().expect("dm match");
        assert_eq!(result.matched_post_id, POST_A);
        assert_eq!(result.peer_username, PEER_A);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn new_typed_dm_after_arm_wakes() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_C}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": DM_C,
                "name": dm_c(),
                "type": "D",
                "last_post_at": 1_780_000_000_300i64
            })))
            .mount(&server)
            .await;
        let state = Arc::new(healthy_state(&server).await);
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        state.event_bus.emit(inbound(
            DM_C,
            &dm_c(),
            "D",
            POST_C,
            PEER_C_ID,
            PEER_C,
            1_780_000_000_300,
            "brand new",
        ));
        let result = task.await.unwrap().expect("new dm");
        assert_eq!(result.matched_post_id, POST_C);
        assert_eq!(result.peer_username, PEER_C);
        assert_eq!(result.dm_name, dm_c());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_during_catalog_is_caught_up() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        struct CatalogSeq(std::sync::atomic::AtomicUsize);
        impl wiremock::Respond for CatalogSeq {
            fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_millis(150))
                        .set_body_json(serde_json::json!([{
                            "id": DM_A,
                            "name": dm_a(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_050i64
                        }]))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!([
                        {
                            "id": DM_A,
                            "name": dm_a(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_050i64
                        },
                        {
                            "id": DM_C,
                            "name": dm_c(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_400i64
                        }
                    ]))
                }
            }
        }
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(CatalogSeq(std::sync::atomic::AtomicUsize::new(0)))
            .mount(&server)
            .await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        mount_posts(
            &server,
            DM_C,
            POST_C,
            PEER_C_ID,
            1_780_000_000_400,
            "after outage",
        )
        .await;
        let state = Arc::new(healthy_state(&server).await);
        let ws = state.ws_state_holder.lock().await.clone().unwrap();
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        ws.reconnect_count.fetch_add(1, Ordering::SeqCst);
        let result = task.await.unwrap().expect("catch-up");
        assert_eq!(result.matched_post_id, POST_C);
        assert_eq!(result.peer_username, PEER_C);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconnect_during_admission_snapshot_is_caught_up() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        struct CatalogSeq(std::sync::atomic::AtomicUsize);
        impl wiremock::Respond for CatalogSeq {
            fn respond(&self, _: &wiremock::Request) -> ResponseTemplate {
                if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                    ResponseTemplate::new(200)
                        .set_delay(Duration::from_millis(150))
                        .set_body_json(serde_json::json!([{
                            "id": DM_A,
                            "name": dm_a(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_050i64
                        }]))
                } else {
                    ResponseTemplate::new(200).set_body_json(serde_json::json!([
                        {
                            "id": DM_A,
                            "name": dm_a(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_050i64
                        },
                        {
                            "id": DM_C,
                            "name": dm_c(),
                            "type": "D",
                            "last_post_at": 1_780_000_000_400i64
                        }
                    ]))
                }
            }
        }
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(CatalogSeq(std::sync::atomic::AtomicUsize::new(0)))
            .mount(&server)
            .await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        mount_posts(
            &server,
            DM_C,
            POST_C,
            PEER_C_ID,
            1_780_000_000_400,
            "after outage",
        )
        .await;
        let state = Arc::new(healthy_state(&server).await);
        let ws = state.ws_state_holder.lock().await.clone().unwrap();
        let hold = ws.connection_state.lock().await;
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        ws.reconnect_count.fetch_add(1, Ordering::SeqCst);
        drop(hold);
        let result = task.await.unwrap().expect("catch-up");
        assert_eq!(result.matched_post_id, POST_C);
        assert_eq!(result.peer_username, PEER_C);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn mention_skips_ack_burst_and_wakes_on_bot_token() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "ACK",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    POST_C: {
                        "id": POST_C,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "@agent-bravo-devlead please",
                        "create_at": 1_780_000_000_200i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let result = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: true,
                after: Some(&cursor),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .expect("mention match");
        assert_eq!(result.matched_post_id, POST_C);
        let json = serde_json::to_value(&result.messages[0]).unwrap();
        assert!(
            json.get("mention_user_ids").is_none(),
            "public message must not leak mention ids: {json}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn future_watermark_cursor_is_uncertain() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {}
            })))
            .expect(0)
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let mut cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID);
        cursor.watermark = 9_000_000_000_000;
        let raw = cursor.encode().unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&raw),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("watermark")),
            "{err}"
        );
        assert!(
            !matches!(err, CoreError::WaitTimeout(_)),
            "future watermark must not be a clean deadman: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn fabricated_equal_watermark_id_is_uncertain() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_100, "real").await;
        let state = healthy_state(&server).await;
        let mut cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID);
        cursor.watermark = 1_780_000_000_100;
        cursor
            .observed_ids
            .insert("zzzzzz00000000000000000001".into());
        let raw = cursor.encode().unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&raw),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("observed")),
            "{err}"
        );
        assert!(
            !matches!(err, CoreError::WaitTimeout(_)),
            "unprovable observed id must not be a clean deadman: {err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn positive_empty_watermark_without_history_is_uncertain() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "id": DM_A,
                    "name": dm_a(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_300i64
                }])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "wake",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    POST_C: {
                        "id": POST_C,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "later",
                        "create_at": 1_780_000_000_300i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let mut cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID);
        cursor.watermark = 1_780_000_000_200;
        let raw = cursor.encode().unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: Some("wake"),
                pattern: None,
                mention: false,
                after: Some(&raw),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("empty") || msg.contains("watermark")),
            "{err}"
        );
        assert!(
            !matches!(err, CoreError::WaitTimeout(_)),
            "unprovable positive empty watermark must not be a clean deadman: {err}"
        );
        state
            .wait_owners
            .acquire_inbox(None, Duration::from_secs(2))
            .await
            .expect("inbox slot must still be free");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn positive_empty_watermark_with_witness_is_uncertain() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!([{
                    "id": DM_A,
                    "name": dm_a(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_300i64
                }])),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "wake",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    POST_C: {
                        "id": POST_C,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "later",
                        "create_at": 1_780_000_000_300i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let mut cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID);
        cursor.watermark = 1_780_000_000_300;
        let raw = cursor.encode().unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: Some("wake"),
                pattern: None,
                mention: false,
                after: Some(&raw),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("empty")),
            "{err}"
        );
        assert!(
            !matches!(err, CoreError::WaitTimeout(_)),
            "positive empty watermark with a witness must not be a clean deadman: {err}"
        );
        state
            .wait_owners
            .acquire_inbox(None, Duration::from_secs(2))
            .await
            .expect("inbox slot must still be free");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn two_peers_self_post_ignored() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([
                {
                    "id": DM_A,
                    "name": dm_a(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_100i64
                },
                {
                    "id": DM_C,
                    "name": dm_c(),
                    "type": "D",
                    "last_post_at": 1_780_000_000_200i64
                }
            ])))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    "postid0000000000000000000s": {
                        "id": "postid0000000000000000000s",
                        "channel_id": DM_A,
                        "user_id": BOT_ID,
                        "message": "own",
                        "create_at": 1_780_000_000_080i64,
                        "root_id": ""
                    },
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "peer a",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        mount_posts(
            &server,
            DM_C,
            POST_C,
            PEER_C_ID,
            1_780_000_000_200,
            "peer c",
        )
        .await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let result = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&cursor),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .expect("peer match");
        assert_eq!(result.matched_post_id, POST_A);
        assert_eq!(result.peer_username, PEER_A);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn same_ms_lower_id_is_delivered() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        let high = "zzzzzz00000000000000000001";
        let low = "aaaaaa00000000000000000001";
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    "zzzzzz00000000000000000001": {
                        "id": high,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "higher",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    "aaaaaa00000000000000000001": {
                        "id": low,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "lower",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let mut cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID);
        cursor = cursor.advance(1_780_000_000_100, high).unwrap();
        let raw = cursor.encode().unwrap();
        let result = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&raw),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .expect("same-ms");
        assert_eq!(result.matched_post_id, low);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_cap_513_fails_closed() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        let mut posts = serde_json::Map::new();
        for i in 0..513 {
            let pid = format!("a{i:025}");
            posts.insert(
                pid.clone(),
                serde_json::json!({
                    "id": pid,
                    "channel_id": DM_A,
                    "user_id": PEER_A_ID,
                    "message": "x",
                    "create_at": 1_780_000_000_000i64 + i,
                    "root_id": ""
                }),
            );
        }
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": posts
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&cursor),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("512") || msg.contains("exceeds")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbox_replace_is_inbox_to_inbox() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        let state = Arc::new(healthy_state(&server).await);
        let first_state = Arc::clone(&state);
        let first = tokio::spawn(async move {
            wait_with_params_inbox(
                &first_state,
                WaitInboxRequest {
                    timeout_secs: 4,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(4),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        let conflict = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 1,
                contains: None,
                pattern: None,
                mention: false,
                after: None,
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(1),
            },
        )
        .await
        .unwrap_err();
        let CoreError::WaitAlreadyActive {
            existing_wait_id, ..
        } = conflict
        else {
            panic!("expected already active, got {conflict}");
        };
        let replace_state = Arc::clone(&state);
        let replaced = tokio::spawn(async move {
            wait_with_params_inbox(
                &replace_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: Some(&existing_wait_id),
                    deadline: Instant::now() + Duration::from_secs(3),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        state.event_bus.emit(inbound(
            DM_A,
            &dm_a(),
            "D",
            POST_A,
            PEER_A_ID,
            PEER_A,
            1_780_000_000_400,
            "after replace",
        ));
        let result = replaced.await.unwrap().expect("replaced inbox");
        assert_eq!(result.peer_username, PEER_A);
        let _ = first.await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn substituted_channel_id_is_uncertain() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_C}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "dmid00000000000000000000zz",
                "name": dm_c(),
                "type": "D",
                "last_post_at": 1
            })))
            .mount(&server)
            .await;
        let state = Arc::new(healthy_state(&server).await);
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 2,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(2),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(40)).await;
        state.event_bus.emit(inbound(
            DM_C,
            &dm_c(),
            "D",
            POST_C,
            PEER_C_ID,
            PEER_C,
            1_780_000_000_300,
            "spoof",
        ));
        let err = task.await.unwrap().unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("id")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follow_sink_failure_keeps_last_acked_cursor() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "one",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    POST_C: {
                        "id": POST_C,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "two",
                        "create_at": 1_780_000_000_200i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let handle = tokio::spawn(async move {
            wait_with_params_inbox_follow(
                &state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: Some(&cursor),
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
                tx,
            )
            .await
        });
        let mut last_ok = None;
        for _ in 0..8 {
            let record = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("inbox follow emitted a record")
                .expect("follow channel open");
            let mode = record.event.mode();
            if matches!(mode, WaitFollowMode::Backlog | WaitFollowMode::Live) {
                if last_ok.is_none() {
                    last_ok = record.event.inbox_cursor().map(str::to_string);
                    let _ = record.written.send(Ok(()));
                } else {
                    let _ = record.written.send(Err("injected sink failure".into()));
                    break;
                }
            } else {
                let _ = record.written.send(Ok(()));
            }
        }
        drop(rx);
        let err = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("follow released after sink failure")
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitProviderDegraded { .. }),
            "{err}"
        );
        assert!(last_ok.is_some());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_paginates_past_a_full_page() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(CatalogPages { total: 201 })
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let catalog = state.client.list_direct_channels().await.expect("201");
        assert_eq!(catalog.len(), 201);
        assert_eq!(catalog[200].id, format!("dmid{n:022}", n = 201));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn catalog_1024_ok_1025_fails_closed() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(CatalogPages { total: 1024 })
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        assert_eq!(
            state
                .client
                .list_direct_channels()
                .await
                .expect("1024")
                .len(),
            1024
        );

        let server = MockServer::start().await;
        mount_baseline(&server).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/users/{BOT_ID}/channels")))
            .respond_with(CatalogPages { total: 1025 })
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: None,
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("1024") || msg.contains("exceeds")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_512_across_two_dms_succeeds() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_two_dm_catalog(&server).await;
        mount_history_pages(&server, DM_A, 300, PEER_A_ID, 'a').await;
        mount_history_pages(&server, DM_C, 212, PEER_C_ID, 'c').await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let result = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&cursor),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .expect("512 bound allows delivery");
        assert!(
            result.matched_post_id == "a0000000000000000000000000"
                || result.matched_post_id == "c0000000000000000000000000",
            "{}",
            result.matched_post_id
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn history_513_across_two_dms_fails_closed() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_two_dm_catalog(&server).await;
        mount_history_pages(&server, DM_A, 300, PEER_A_ID, 'a').await;
        mount_history_pages(&server, DM_C, 213, PEER_C_ID, 'c').await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&cursor),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("512") || msg.contains("exceeds")),
            "{err}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn direct_owner_blocks_inbox_and_replace() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_100, "x").await;
        let state = healthy_state(&server).await;
        let lease = state
            .wait_owners
            .acquire_direct(DM_A, "org", &dm_a(), None, Duration::from_secs(5))
            .await
            .expect("direct lease");
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: None,
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(matches!(err, CoreError::WaitAlreadyActive { .. }), "{err}");
        let err = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 2,
                contains: None,
                pattern: None,
                mention: false,
                after: None,
                replace_wait_id: Some(&lease.wait_id),
                deadline: Instant::now() + Duration::from_secs(2),
            },
        )
        .await
        .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitConflictChanged { .. }),
            "{err}"
        );
        drop(lease);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn inbox_owner_blocks_direct_not_channel() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        let state = Arc::new(healthy_state(&server).await);
        let wait_state = Arc::clone(&state);
        let task = tokio::spawn(async move {
            wait_with_params_inbox(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 4,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(4),
                },
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(80)).await;
        let blocked = match state
            .wait_owners
            .acquire_direct(DM_A, "org", &dm_a(), None, Duration::from_secs(2))
            .await
        {
            Ok(_) => panic!("direct should be blocked by inbox"),
            Err(err) => err,
        };
        assert!(
            matches!(blocked, CoreError::WaitAlreadyActive { .. }),
            "{blocked}"
        );
        let channel = state
            .wait_owners
            .acquire(
                "public00000000000000000001",
                "org",
                "town-square",
                None,
                Duration::from_secs(2),
            )
            .await
            .expect("channel wait is not dm-class");
        drop(channel);
        state.event_bus.emit(inbound(
            DM_A,
            &dm_a(),
            "D",
            POST_A,
            PEER_A_ID,
            PEER_A,
            1_780_000_000_400,
            "after owner check",
        ));
        let result = task.await.unwrap().expect("inbox match");
        assert_eq!(result.matched_post_id, POST_A);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follow_resume_from_acked_cursor_recovers_undelivered() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_A}/posts")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "posts": {
                    POST_A: {
                        "id": POST_A,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "one",
                        "create_at": 1_780_000_000_100i64,
                        "root_id": ""
                    },
                    POST_C: {
                        "id": POST_C,
                        "channel_id": DM_A,
                        "user_id": PEER_A_ID,
                        "message": "two",
                        "create_at": 1_780_000_000_200i64,
                        "root_id": ""
                    }
                }
            })))
            .mount(&server)
            .await;
        let state = healthy_state(&server).await;
        let cursor = InboxCursorV1::empty(&state.profile.name, BOT_ID)
            .encode()
            .unwrap();
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let handle = tokio::spawn({
            let state = healthy_state(&server).await;
            async move {
                wait_with_params_inbox_follow(
                    &state,
                    WaitInboxRequest {
                        timeout_secs: 3,
                        contains: None,
                        pattern: None,
                        mention: false,
                        after: Some(&cursor),
                        replace_wait_id: None,
                        deadline: Instant::now() + Duration::from_secs(3),
                    },
                    tx,
                )
                .await
            }
        });
        let mut last_ok = None;
        for _ in 0..8 {
            let record = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("inbox follow emitted a record")
                .expect("follow channel open");
            let mode = record.event.mode();
            if matches!(mode, WaitFollowMode::Backlog | WaitFollowMode::Live) {
                if last_ok.is_none() {
                    last_ok = record.event.inbox_cursor().map(str::to_string);
                    let _ = record.written.send(Ok(()));
                } else {
                    let _ = record.written.send(Err("injected sink failure".into()));
                    break;
                }
            } else {
                let _ = record.written.send(Ok(()));
            }
        }
        drop(rx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
        let after = last_ok.expect("acked cursor");
        let recovered = wait_with_params_inbox(
            &state,
            WaitInboxRequest {
                timeout_secs: 3,
                contains: None,
                pattern: None,
                mention: false,
                after: Some(&after),
                replace_wait_id: None,
                deadline: Instant::now() + Duration::from_secs(3),
            },
        )
        .await
        .expect("undelivered recovered");
        assert_eq!(recovered.matched_post_id, POST_C);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn follow_failed_terminal_keeps_last_acked_cursor() {
        let server = MockServer::start().await;
        mount_baseline(&server).await;
        mount_one_dm_catalog(&server, 0).await;
        mount_posts(&server, DM_A, POST_A, PEER_A_ID, 1_780_000_000_050, "old").await;
        Mock::given(method("GET"))
            .and(path(format!("/api/v4/channels/{DM_C}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "dmid00000000000000000000zz",
                "name": dm_c(),
                "type": "D",
                "last_post_at": 1
            })))
            .mount(&server)
            .await;
        let state = Arc::new(healthy_state(&server).await);
        let (tx, mut rx) = tokio::sync::mpsc::channel(4);
        let wait_state = Arc::clone(&state);
        let handle = tokio::spawn(async move {
            wait_with_params_inbox_follow(
                &wait_state,
                WaitInboxRequest {
                    timeout_secs: 3,
                    contains: None,
                    pattern: None,
                    mention: false,
                    after: None,
                    replace_wait_id: None,
                    deadline: Instant::now() + Duration::from_secs(3),
                },
                tx,
            )
            .await
        });
        let mut last_ok = None;
        let mut failed_cursor = None;
        for _ in 0..8 {
            let record = tokio::time::timeout(Duration::from_secs(2), rx.recv())
                .await
                .expect("inbox follow emitted a record")
                .expect("follow channel open");
            let mode = record.event.mode();
            if matches!(mode, WaitFollowMode::Armed) {
                let _ = record.written.send(Ok(()));
                tokio::time::sleep(Duration::from_millis(40)).await;
                state.event_bus.emit(inbound(
                    DM_A,
                    &dm_a(),
                    "D",
                    POST_A,
                    PEER_A_ID,
                    PEER_A,
                    1_780_000_000_400,
                    "first",
                ));
            } else if matches!(mode, WaitFollowMode::Backlog | WaitFollowMode::Live) {
                last_ok = record.event.inbox_cursor().map(str::to_string);
                let _ = record.written.send(Ok(()));
                state.event_bus.emit(inbound(
                    DM_C,
                    &dm_c(),
                    "D",
                    POST_C,
                    PEER_C_ID,
                    PEER_C,
                    1_780_000_000_500,
                    "spoof",
                ));
            } else if matches!(mode, WaitFollowMode::Failed) {
                failed_cursor = record.event.inbox_cursor().map(str::to_string);
                let _ = record.written.send(Ok(()));
                break;
            } else {
                let _ = record.written.send(Ok(()));
            }
        }
        let err = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("follow released after failed terminal")
            .unwrap()
            .unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(ref msg) if msg.contains("id")),
            "{err}"
        );
        assert_eq!(failed_cursor, last_ok);
        assert!(last_ok.is_some());
    }
}
