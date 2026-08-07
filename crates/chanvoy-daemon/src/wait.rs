//! PER-038 wait engine: content filter, exclusive baseline, absolute
//! deadman, one-message result, shared predicate, provider retry.
//!
//! Causal order (entarch F3 / pre-build Q3 / R2): subscribe → drain while
//! establishing anchor → backfill → merge bus+REST candidates under
//! first-match → live push with dual REST when unhealthy. Bare-wait
//! membership is bus-arrival (and REST posts after the tip id), never
//! `create_at` exclusivity.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chanvoy_core::{
    CoreError, DaemonEvent, DaemonEventPayloadInner, InboundEventPayload, Message, WaitResult,
    WsConnectionState,
};
use regex::RegexBuilder;
use reqwest::StatusCode;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout, Instant};

use crate::AppState;

/// Max UTF-8 source bytes for `--contains` and `--pattern` (product + secrev).
pub const FILTER_SOURCE_MAX_BYTES: usize = 256;
/// Compiled regex size limit (bytes) enforced at RegexBuilder.
pub const REGEX_COMPILED_SIZE_LIMIT: usize = 64 * 1024;
const BACKOFF_BASE: Duration = Duration::from_millis(500);
const BACKOFF_CAP: Duration = Duration::from_secs(8);
const REST_IDLE: Duration = Duration::from_secs(2);

/// Compiled body filter shared by backfill, push, REST poll, and lag recovery.
#[derive(Debug, Clone)]
pub struct WaitPredicate {
    my_user_id: String,
    channel_id: String,
    contains: Option<String>,
    pattern: Option<regex::Regex>,
}

impl WaitPredicate {
    /// Compile filter flags before any subscribe/provider wait loop.
    /// Empty values are refuse (not match-all). Invalid/oversize/over-complex
    /// patterns are hard errors (`WaitFilterInvalid`), never timeouts.
    pub fn compile(
        my_user_id: &str,
        channel_id: &str,
        contains: Option<&str>,
        pattern: Option<&str>,
    ) -> Result<Self, CoreError> {
        let contains = match contains {
            None => None,
            Some("") => {
                return Err(CoreError::WaitFilterInvalid(
                    "empty --contains is refused (not match-all)".into(),
                ));
            }
            Some(s) if s.len() > FILTER_SOURCE_MAX_BYTES => {
                return Err(CoreError::WaitFilterInvalid(format!(
                    "--contains exceeds {FILTER_SOURCE_MAX_BYTES} UTF-8 bytes"
                )));
            }
            Some(s) => Some(s.to_string()),
        };

        let pattern = match pattern {
            None => None,
            Some("") => {
                return Err(CoreError::WaitFilterInvalid(
                    "empty --pattern is refused (not match-all)".into(),
                ));
            }
            Some(s) if s.len() > FILTER_SOURCE_MAX_BYTES => {
                return Err(CoreError::WaitFilterInvalid(format!(
                    "--pattern exceeds {FILTER_SOURCE_MAX_BYTES} UTF-8 bytes"
                )));
            }
            Some(s) => {
                let re = RegexBuilder::new(s)
                    .size_limit(REGEX_COMPILED_SIZE_LIMIT)
                    .dfa_size_limit(REGEX_COMPILED_SIZE_LIMIT)
                    .build()
                    .map_err(|err| {
                        CoreError::WaitFilterInvalid(format!(
                            "invalid or over-complex --pattern: {err}"
                        ))
                    })?;
                Some(re)
            }
        };

        Ok(Self {
            my_user_id: my_user_id.to_string(),
            channel_id: channel_id.to_string(),
            contains,
            pattern,
        })
    }

    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    fn body_matches(&self, body: &str) -> bool {
        if let Some(needle) = &self.contains {
            if !body.contains(needle.as_str()) {
                return false;
            }
        }
        if let Some(re) = &self.pattern {
            if !re.is_match(body) {
                return false;
            }
        }
        true
    }

    pub fn matches_message(&self, message: &Message) -> bool {
        message.user_id != self.my_user_id && self.body_matches(&message.message)
    }

    pub fn matches_inbound(&self, payload: &InboundEventPayload) -> bool {
        payload.channel_id == self.channel_id
            && payload.sender_id != self.my_user_id
            && self.body_matches(&payload.message)
    }
}

pub fn inbound_to_message(payload: &InboundEventPayload) -> Message {
    Message {
        id: payload.post_id.clone(),
        user_id: payload.sender_id.clone(),
        username: payload.sender_username.clone(),
        message: payload.message.clone(),
        create_at: payload.create_at,
        root_id: payload.root_id.clone(),
    }
}

/// Deterministic first match: chronological `(create_at, id)`.
pub fn first_match<'a>(
    messages: impl IntoIterator<Item = &'a Message>,
    predicate: &WaitPredicate,
    exclude: &HashSet<String>,
) -> Option<Message> {
    let mut candidates: Vec<&Message> = messages
        .into_iter()
        .filter(|m| !exclude.contains(&m.id) && predicate.matches_message(m))
        .collect();
    candidates.sort_by(|a, b| a.create_at.cmp(&b.create_at).then_with(|| a.id.cmp(&b.id)));
    candidates.first().map(|m| (*m).clone())
}

/// Earliest among already-collected candidate messages (for bus+REST merge).
pub fn earliest_message(candidates: &[Message]) -> Option<Message> {
    candidates
        .iter()
        .min_by(|a, b| a.create_at.cmp(&b.create_at).then_with(|| a.id.cmp(&b.id)))
        .cloned()
}

fn is_retryable_provider(err: &CoreError) -> bool {
    match err {
        CoreError::Api { status, .. } => matches!(
            *status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::INTERNAL_SERVER_ERROR
                | StatusCode::BAD_GATEWAY
                | StatusCode::SERVICE_UNAVAILABLE
                | StatusCode::GATEWAY_TIMEOUT
        ),
        CoreError::Http(_) => true,
        _ => false,
    }
}

fn is_terminal_auth(err: &CoreError) -> bool {
    matches!(
        err,
        CoreError::Api {
            status: StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN,
            ..
        }
    )
}

/// Absolute deadline from RPC entry; covers resolve, anchor, backfill, retries, block.
pub async fn wait_with_params(
    state: &AppState,
    channel: &str,
    timeout_secs: u64,
    team: Option<&str>,
    contains: Option<&str>,
    pattern: Option<&str>,
    after: Option<&str>,
) -> Result<WaitResult, CoreError> {
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    WaitPredicate::compile("pending", "pending", contains, pattern)?;

    let channel_id = provider_retry(state, channel, deadline, || async {
        state
            .client
            .resolve_channel(channel, team)
            .await
            .map(|r| r.channel_id)
    })
    .await?;

    let predicate = WaitPredicate::compile(&state.my_user_id, &channel_id, contains, pattern)?;

    let is_monitored = state
        .profile
        .monitored_channels
        .iter()
        .any(|m| m.eq_ignore_ascii_case(channel));

    if is_monitored {
        wait_push_path(state, channel, &predicate, after, deadline).await
    } else {
        wait_rest_path(state, channel, &predicate, after, deadline).await
    }
}

async fn wait_push_path(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    after: Option<&str>,
    deadline: Instant,
) -> Result<WaitResult, CoreError> {
    let mut rx = state.event_bus.subscribe();
    let mut bus_buffer: VecDeque<Arc<DaemonEvent>> = VecDeque::new();

    drain_bus(&mut rx, &mut bus_buffer, channel)?;

    // Establish baseline while continuously capturing bus events (R2 finding 2).
    let (scan_after, rest_baseline) = {
        let baseline = establish_baseline(state, channel, predicate.channel_id(), after, deadline);
        tokio::pin!(baseline);
        loop {
            tokio::select! {
                res = &mut baseline => break res?,
                _ = sleep(Duration::from_millis(5)) => {
                    drain_bus(&mut rx, &mut bus_buffer, channel)?;
                }
            }
        }
    };

    let mut processed: HashSet<String> = HashSet::new();

    // Backfill while still draining the bus.
    let mut backfill_msgs: Vec<Message> = Vec::new();
    if let Some(ref anchor) = scan_after {
        let fetch = provider_retry(state, channel, deadline, || {
            let a = anchor.clone();
            async move {
                state
                    .client
                    .posts_after_by_channel_id(predicate.channel_id(), &a)
                    .await
            }
        });
        tokio::pin!(fetch);
        loop {
            tokio::select! {
                res = &mut fetch => {
                    backfill_msgs = res?;
                    break;
                }
                _ = sleep(Duration::from_millis(5)) => {
                    drain_bus(&mut rx, &mut bus_buffer, channel)?;
                }
            }
        }
    }

    // Final drain, then first-match across backfill + bus (causal order).
    drain_bus(&mut rx, &mut bus_buffer, channel)?;
    if let Some(hit) =
        first_match_across_rest_and_bus(&backfill_msgs, &bus_buffer, predicate, &processed)
    {
        return Ok(one_message_result(channel, hit));
    }
    for m in &backfill_msgs {
        processed.insert(m.id.clone());
    }
    mark_bus_processed(&bus_buffer, predicate, &mut processed);

    let mut scan_cursor = scan_after;
    let mut clean_observe = true;
    let mut saw_push_event = false;
    let mut saw_healthy_ws = ws_connection_healthy(state).await;

    loop {
        if Instant::now() >= deadline {
            return push_deadline_outcome(
                state,
                channel,
                clean_observe,
                saw_push_event,
                saw_healthy_ws,
            )
            .await;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let idle = REST_IDLE.min(remaining);
        // Dual-observe: REST poll on idle even when push is quiet / WS unhealthy.
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(event) => {
                        saw_push_event = true;
                        clean_observe = true;
                        if ws_connection_healthy(state).await {
                            saw_healthy_ws = true;
                        }
                        bus_buffer.push_back(event);
                        if let Some(hit) = first_match_across_rest_and_bus(
                            &[],
                            &bus_buffer,
                            predicate,
                            &processed,
                        ) {
                            return Ok(one_message_result(channel, hit));
                        }
                        mark_bus_processed(&bus_buffer, predicate, &mut processed);
                        bus_buffer.clear();
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        match lag_recover_page(
                            state,
                            channel,
                            predicate,
                            scan_cursor.as_deref(),
                            deadline,
                        )
                        .await
                        {
                            Ok(page) => {
                                clean_observe = true;
                                let mut exclude = rest_baseline.clone();
                                exclude.extend(processed.iter().cloned());
                                if let Some(hit) = first_match(&page, predicate, &exclude) {
                                    return Ok(one_message_result(channel, hit));
                                }
                                if let Some(last) = page.last() {
                                    scan_cursor = Some(last.id.clone());
                                    for m in page {
                                        processed.insert(m.id);
                                    }
                                }
                            }
                            Err(CoreError::WaitProviderDegraded { .. })
                            | Err(CoreError::WaitTimeout(_)) => {
                                clean_observe = false;
                            }
                            Err(other) => return Err(other),
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(CoreError::WaitProviderDegraded {
                            channel: channel.to_string(),
                            message: "event bus closed during wait".into(),
                        });
                    }
                }
            }
            _ = sleep(idle) => {
                // Dual REST observe with shared predicate / cursor / deadline.
                if Instant::now() >= deadline {
                    return push_deadline_outcome(
                        state,
                        channel,
                        clean_observe,
                        saw_push_event,
                        saw_healthy_ws,
                    )
                    .await;
                }
                if ws_connection_healthy(state).await {
                    saw_healthy_ws = true;
                }
                let page = if let Some(ref cursor) = scan_cursor {
                    provider_retry(state, channel, deadline, || {
                        let c = cursor.clone();
                        async move {
                            state
                                .client
                                .posts_after_by_channel_id(predicate.channel_id(), &c)
                                .await
                        }
                    })
                    .await
                } else {
                    // Empty-at-arm: page first non-empty observation to exhaustion.
                    empty_at_arm_observation(state, channel, predicate, deadline).await
                };
                match page {
                    Ok(page) => {
                        clean_observe = true;
                        let mut exclude = rest_baseline.clone();
                        exclude.extend(processed.iter().cloned());
                        if let Some(hit) = first_match(&page, predicate, &exclude) {
                            return Ok(one_message_result(channel, hit));
                        }
                        if let Some(last) = page.last() {
                            scan_cursor = Some(last.id.clone());
                            for m in page {
                                processed.insert(m.id);
                            }
                        }
                    }
                    Err(CoreError::WaitProviderDegraded { .. }) => {
                        clean_observe = false;
                    }
                    Err(CoreError::WaitTimeout(_)) => {
                        return push_deadline_outcome(
                            state,
                            channel,
                            clean_observe,
                            saw_push_event,
                            saw_healthy_ws,
                        )
                        .await;
                    }
                    Err(other) => return Err(other),
                }
            }
        }
    }
}

async fn wait_rest_path(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    after: Option<&str>,
    deadline: Instant,
) -> Result<WaitResult, CoreError> {
    let (mut scan_cursor, rest_baseline) =
        establish_baseline(state, channel, predicate.channel_id(), after, deadline).await?;
    let mut processed: HashSet<String> = HashSet::new();

    if let Some(ref anchor) = scan_cursor {
        let backfill = provider_retry(state, channel, deadline, || {
            let a = anchor.clone();
            async move {
                state
                    .client
                    .posts_after_by_channel_id(predicate.channel_id(), &a)
                    .await
            }
        })
        .await?;
        let mut exclude = rest_baseline.clone();
        exclude.extend(processed.iter().cloned());
        if let Some(hit) = first_match(&backfill, predicate, &exclude) {
            return Ok(one_message_result(channel, hit));
        }
        if let Some(last) = backfill.last() {
            scan_cursor = Some(last.id.clone());
        }
        for m in backfill {
            processed.insert(m.id);
        }
    }

    let mut clean_observe = true;
    let mut backoff = BACKOFF_BASE;

    loop {
        if Instant::now() >= deadline {
            if clean_observe {
                return Err(CoreError::WaitTimeout(channel.to_string()));
            }
            return Err(CoreError::WaitProviderDegraded {
                channel: channel.to_string(),
                message: "deadline reached while provider observation was failing".into(),
            });
        }

        let page_result = if let Some(ref cursor) = scan_cursor {
            provider_retry(state, channel, deadline, || {
                let c = cursor.clone();
                async move {
                    state
                        .client
                        .posts_after_by_channel_id(predicate.channel_id(), &c)
                        .await
                }
            })
            .await
        } else {
            empty_at_arm_observation(state, channel, predicate, deadline).await
        };

        match page_result {
            Ok(page) => {
                clean_observe = true;
                backoff = BACKOFF_BASE;
                let mut exclude = rest_baseline.clone();
                exclude.extend(processed.iter().cloned());
                if let Some(hit) = first_match(&page, predicate, &exclude) {
                    return Ok(one_message_result(channel, hit));
                }
                if let Some(last) = page.last() {
                    if scan_cursor.as_deref() != Some(last.id.as_str()) {
                        scan_cursor = Some(last.id.clone());
                    }
                    for m in page {
                        processed.insert(m.id);
                    }
                }
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(CoreError::WaitTimeout(channel.to_string()));
                }
                sleep(REST_IDLE.min(remaining)).await;
            }
            Err(CoreError::WaitProviderDegraded { .. }) => {
                clean_observe = false;
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(CoreError::WaitProviderDegraded {
                        channel: channel.to_string(),
                        message: "deadline reached while provider observation was failing".into(),
                    });
                }
                sleep(backoff.min(remaining)).await;
                backoff = (backoff * 2).min(BACKOFF_CAP);
            }
            Err(other) => return Err(other),
        }
    }
}

/// Empty-at-arm: page first non-empty observation to exhaustion, return
/// the full set for earliest-match selection (entarch/secrev R2 residual A).
async fn empty_at_arm_observation(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    deadline: Instant,
) -> Result<Vec<Message>, CoreError> {
    // Probe latest(1) first so we do not walk a still-empty channel forever
    // without yielding; then page all posts if non-empty.
    let probe = provider_retry(state, channel, deadline, || async {
        state
            .client
            .latest_channel_messages_by_id(predicate.channel_id(), 1)
            .await
    })
    .await?;
    if probe.is_empty() {
        return Ok(Vec::new());
    }
    provider_retry(state, channel, deadline, || async {
        state
            .client
            .all_channel_posts_by_id(predicate.channel_id())
            .await
    })
    .await
}

async fn establish_baseline(
    state: &AppState,
    channel: &str,
    channel_id: &str,
    after: Option<&str>,
    deadline: Instant,
) -> Result<(Option<String>, HashSet<String>), CoreError> {
    let mut rest_baseline = HashSet::new();

    if let Some(anchor_id) = after {
        if anchor_id.is_empty() {
            return Err(CoreError::WaitFilterInvalid(
                "empty --after is refused".into(),
            ));
        }
        provider_retry(state, channel, deadline, || async {
            state
                .client
                .assert_post_in_channel(channel_id, channel, anchor_id)
                .await
        })
        .await
        .map_err(|err| map_anchor_err(err, anchor_id))?;
        rest_baseline.insert(anchor_id.to_string());
        return Ok((Some(anchor_id.to_string()), rest_baseline));
    }

    let tip = provider_retry(state, channel, deadline, || async {
        state
            .client
            .latest_channel_messages_by_id(channel_id, 30)
            .await
    })
    .await?;

    if let Some(last) = tip.last() {
        for m in &tip {
            rest_baseline.insert(m.id.clone());
        }
        Ok((Some(last.id.clone()), rest_baseline))
    } else {
        Ok((None, rest_baseline))
    }
}

fn map_anchor_err(err: CoreError, anchor_id: &str) -> CoreError {
    match err {
        CoreError::AnchorNotFound(_) => {
            CoreError::WaitFilterInvalid(format!("wait --after post not found: {anchor_id}"))
        }
        CoreError::AnchorChannelMismatch { channel, .. } => CoreError::WaitFilterInvalid(format!(
            "wait --after post {anchor_id} is not in channel {channel}"
        )),
        other => other,
    }
}

fn first_match_across_rest_and_bus(
    rest: &[Message],
    bus: &VecDeque<Arc<DaemonEvent>>,
    predicate: &WaitPredicate,
    processed: &HashSet<String>,
) -> Option<Message> {
    let mut candidates: Vec<Message> = Vec::new();
    for m in rest {
        if !processed.contains(&m.id) && predicate.matches_message(m) {
            candidates.push(m.clone());
        }
    }
    for event in bus {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if !processed.contains(&p.post_id) && predicate.matches_inbound(p) {
                candidates.push(inbound_to_message(p));
            }
        }
    }
    earliest_message(&candidates)
}

fn mark_bus_processed(
    bus: &VecDeque<Arc<DaemonEvent>>,
    predicate: &WaitPredicate,
    processed: &mut HashSet<String>,
) {
    for event in bus {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if p.channel_id == predicate.channel_id() {
                processed.insert(p.post_id.clone());
            }
        }
    }
}

fn drain_bus(
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus_buffer: &mut VecDeque<Arc<DaemonEvent>>,
    channel: &str,
) -> Result<(), CoreError> {
    loop {
        match rx.try_recv() {
            Ok(ev) => bus_buffer.push_back(ev),
            Err(broadcast::error::TryRecvError::Empty) => return Ok(()),
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "event bus lagged during wait baseline".into(),
                });
            }
            Err(broadcast::error::TryRecvError::Closed) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "event bus closed".into(),
                });
            }
        }
    }
}

fn one_message_result(channel: &str, message: Message) -> WaitResult {
    WaitResult {
        channel: channel.to_string(),
        messages: vec![message],
    }
}

async fn lag_recover_page(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    scan_cursor: Option<&str>,
    deadline: Instant,
) -> Result<Vec<Message>, CoreError> {
    if let Some(cursor) = scan_cursor {
        provider_retry(state, channel, deadline, || {
            let c = cursor.to_string();
            async move {
                state
                    .client
                    .posts_after_by_channel_id(predicate.channel_id(), &c)
                    .await
            }
        })
        .await
    } else {
        empty_at_arm_observation(state, channel, predicate, deadline).await
    }
}

async fn ws_connection_healthy(state: &AppState) -> bool {
    let ws = {
        let guard = state.ws_state_holder.lock().await;
        guard.clone()
    };
    let Some(ws) = ws else {
        return false;
    };
    let state = *ws.connection_state.lock().await;
    matches!(state, WsConnectionState::Healthy)
}

/// Clean deadman only when observation actually worked (secrev/entarch B).
async fn push_deadline_outcome(
    state: &AppState,
    channel: &str,
    clean_observe: bool,
    saw_push_event: bool,
    saw_healthy_ws: bool,
) -> Result<WaitResult, CoreError> {
    if !clean_observe {
        return Err(CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "deadline reached while provider observation was failing".into(),
        });
    }
    let healthy_now = ws_connection_healthy(state).await;
    // Never healthy for the arm and never saw a push event → failed
    // observation, not honest silence.
    if !saw_push_event && !saw_healthy_ws && !healthy_now {
        return Err(CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "wait ended without a healthy push observation path".into(),
        });
    }
    Err(CoreError::WaitTimeout(channel.to_string()))
}

/// Pure deadline with no prior retryable failure is a clean deadman, not
/// provider degradation (devrev r1).
fn deadline_error(channel: &str, saw_retryable: bool, detail: Option<String>) -> CoreError {
    if saw_retryable {
        CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: detail.unwrap_or_else(|| {
                "deadline reached while provider observation was failing".into()
            }),
        }
    } else {
        CoreError::WaitTimeout(channel.to_string())
    }
}

async fn provider_retry<T, F, Fut>(
    state: &AppState,
    channel: &str,
    deadline: Instant,
    mut op: F,
) -> Result<T, CoreError>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    let _ = state;
    let mut delay = BACKOFF_BASE;
    let mut saw_retryable = false;
    loop {
        if Instant::now() >= deadline {
            return Err(deadline_error(
                channel,
                saw_retryable,
                Some("deadline reached before a successful observation".into()),
            ));
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        // Bound every attempt by remaining deadline (entarch R2 finding 1).
        match timeout(remaining, op()).await {
            Ok(Ok(v)) => return Ok(v),
            Ok(Err(err)) if is_terminal_auth(&err) => return Err(err),
            Ok(Err(err)) if is_retryable_provider(&err) => {
                saw_retryable = true;
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(deadline_error(
                        channel,
                        true,
                        Some(format!("provider error until deadline: {err}")),
                    ));
                }
                sleep(delay.min(remaining).min(BACKOFF_CAP)).await;
                delay = (delay * 2).min(BACKOFF_CAP);
            }
            Ok(Err(err)) => return Err(err),
            Err(_) => {
                // Attempt itself overran remaining budget — not clean silence.
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "provider call stalled past wait deadline".into(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chanvoy_core::{DaemonEventKind, InboundEventPayload, Provider};

    fn pred(contains: Option<&str>, pattern: Option<&str>) -> WaitPredicate {
        WaitPredicate::compile("bot", "ch-1", contains, pattern).expect("compile")
    }

    fn msg(id: &str, user: &str, body: &str, create_at: i64) -> Message {
        Message {
            id: id.into(),
            user_id: user.into(),
            username: user.into(),
            message: body.into(),
            create_at,
            root_id: id.into(),
        }
    }

    fn inbound_event(post_id: &str, body: &str, create_at: i64) -> Arc<DaemonEvent> {
        Arc::new(DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: "ch-1".into(),
                channel_name: "c".into(),
                post_id: post_id.into(),
                root_id: post_id.into(),
                sender_id: "u".into(),
                sender_username: "u".into(),
                message: body.into(),
                create_at,
                received_at: create_at,
                mentioned: false,
            }),
        })
    }

    #[test]
    fn contains_case_sensitive() {
        let p = pred(Some("ASSENT"), None);
        assert!(p.matches_message(&msg("1", "u", "panel ASSENT here", 1)));
        assert!(!p.matches_message(&msg("2", "u", "panel assent here", 1)));
    }

    #[test]
    fn pattern_case_insensitive_flag() {
        let p = pred(None, Some("(?i)assent"));
        assert!(p.matches_message(&msg("1", "u", "Assent granted", 1)));
    }

    #[test]
    fn contains_and_pattern() {
        let p = pred(Some("RATIFY"), Some("(?i)entarch"));
        assert!(p.matches_message(&msg("1", "u", "entarch: RATIFY", 1)));
        assert!(!p.matches_message(&msg("2", "u", "entarch: REVISE", 1)));
        assert!(!p.matches_message(&msg("3", "u", "other: RATIFY", 1)));
    }

    #[test]
    fn self_posts_ignored() {
        let p = pred(Some("hi"), None);
        assert!(!p.matches_message(&msg("1", "bot", "hi", 1)));
    }

    #[test]
    fn empty_contains_refused() {
        let err = WaitPredicate::compile("bot", "ch", Some(""), None).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(_)));
    }

    #[test]
    fn pattern_over_256_refused() {
        let big = "a".repeat(FILTER_SOURCE_MAX_BYTES + 1);
        let err = WaitPredicate::compile("bot", "ch", None, Some(&big)).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(_)));
    }

    #[test]
    fn contains_over_256_refused() {
        let big = "a".repeat(FILTER_SOURCE_MAX_BYTES + 1);
        let err = WaitPredicate::compile("bot", "ch", Some(&big), None).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(_)));
    }

    #[test]
    fn invalid_regex_refused() {
        let err = WaitPredicate::compile("bot", "ch", None, Some("(")).unwrap_err();
        assert!(matches!(err, CoreError::WaitFilterInvalid(_)));
    }

    #[test]
    fn motivating_case_insensitive_patterns_compile_under_size_cap() {
        for pat in ["(?i)assent", "(?i)ASSENT|RATIFY", "(?i)assent|ratify"] {
            WaitPredicate::compile("bot", "ch", None, Some(pat)).expect(pat);
        }
    }

    #[test]
    fn first_match_is_chronological() {
        let p = pred(Some("X"), None);
        let msgs = vec![msg("b", "u", "X late", 200), msg("a", "u", "X early", 100)];
        let hit = first_match(&msgs, &p, &HashSet::new()).unwrap();
        assert_eq!(hit.id, "a");
    }

    #[test]
    fn metacharacters_literal_under_contains() {
        let p = pred(Some("a+b"), None);
        assert!(p.matches_message(&msg("1", "u", "use a+b here", 1)));
        assert!(!p.matches_message(&msg("2", "u", "use aaab here", 1)));
    }

    #[test]
    fn inbound_channel_id_must_match() {
        let p = pred(None, None);
        let good = InboundEventPayload {
            profile: "t".into(),
            provider: Provider::Mattermost,
            channel_id: "ch-1".into(),
            channel_name: "general".into(),
            post_id: "p1".into(),
            root_id: "p1".into(),
            sender_id: "u".into(),
            sender_username: "alice".into(),
            message: "hello".into(),
            create_at: 1,
            received_at: 1,
            mentioned: false,
        };
        assert!(p.matches_inbound(&good));
        let mut other = good.clone();
        other.channel_id = "ch-other".into();
        assert!(!p.matches_inbound(&other));
    }

    #[test]
    fn pure_deadline_is_clean_deadman_not_provider() {
        let err = deadline_error("ops", false, None);
        assert!(matches!(err, CoreError::WaitTimeout(_)));
    }

    #[test]
    fn retryable_deadline_is_provider_degraded() {
        let err = deadline_error("ops", true, Some("500s".into()));
        match err {
            CoreError::WaitProviderDegraded { channel, message } => {
                assert_eq!(channel, "ops");
                assert!(message.contains("500"));
            }
            other => panic!("expected WaitProviderDegraded, got {other:?}"),
        }
    }

    /// Seam: earlier bus match wins over later REST backfill match (R2 #2).
    #[test]
    fn earlier_bus_match_wins_over_later_rest_backfill() {
        let p = pred(Some("X"), None);
        let rest = vec![msg("p2", "u", "X later", 200)];
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("p1", "X earlier", 100));
        let hit = first_match_across_rest_and_bus(&rest, &bus, &p, &HashSet::new()).unwrap();
        assert_eq!(
            hit.id, "p1",
            "causal first match must prefer earlier bus event"
        );
    }

    #[test]
    fn event_bus_has_no_replay_of_pre_subscribe_events() {
        use chanvoy_core::EventBus;

        let bus = EventBus::new(16);
        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: "ch".into(),
                channel_name: "c".into(),
                post_id: "pre".into(),
                root_id: "pre".into(),
                sender_id: "u".into(),
                sender_username: "u".into(),
                message: "pre".into(),
                create_at: 1,
                received_at: 1,
                mentioned: false,
            }),
        });
        let mut rx = bus.subscribe();
        assert!(matches!(
            rx.try_recv(),
            Err(broadcast::error::TryRecvError::Empty)
        ));
        bus.emit(DaemonEvent {
            seq: 0,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: "ch".into(),
                channel_name: "c".into(),
                post_id: "post".into(),
                root_id: "post".into(),
                sender_id: "u".into(),
                sender_username: "u".into(),
                message: "post".into(),
                create_at: 2,
                received_at: 2,
                mentioned: false,
            }),
        });
        let got = rx.try_recv().expect("post-sub event");
        match &got.payload {
            DaemonEventPayloadInner::Inbound(p) => assert_eq!(p.post_id, "post"),
            _ => panic!("expected inbound"),
        }
    }
}
