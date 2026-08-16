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
    validate_wait_channel_v3_strings, CoreError, DaemonEvent, DaemonEventPayloadInner,
    InboundEventPayload, Message, WaitResult, WsConnectionState,
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
pub(crate) const BACKOFF_BASE: Duration = Duration::from_millis(500);
pub(crate) const BACKOFF_CAP: Duration = Duration::from_secs(8);
pub(crate) const REST_IDLE: Duration = Duration::from_secs(2);

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

fn is_retryable_provider(err: &CoreError) -> bool {
    match err {
        // AC-W6 / devrev R3: all 5xx + 429 + transport, not a fixed 500/502/503/504 list.
        CoreError::Api { status, .. } => {
            *status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
        }
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

/// Refuse zero / empty wait windows before any subscribe or provider work.
pub(crate) fn validate_wait_timeout_secs(timeout_secs: u64) -> Result<(), CoreError> {
    if timeout_secs == 0 {
        return Err(CoreError::WaitFilterInvalid(
            "wait timeout must be greater than zero".into(),
        ));
    }
    Ok(())
}

pub struct WaitRequest<'a> {
    pub channel: &'a str,
    pub timeout_secs: u64,
    pub team: Option<&'a str>,
    pub contains: Option<&'a str>,
    pub pattern: Option<&'a str>,
    pub after: Option<&'a str>,
    pub replace_wait_id: Option<&'a str>,
    pub emit_wait_ids: bool,
}

/// Absolute deadline from RPC entry; covers resolve, anchor, backfill, retries, block.
pub async fn wait_with_params(
    state: &AppState,
    req: WaitRequest<'_>,
) -> Result<WaitResult, CoreError> {
    // Direct RPC and CLI share this path (devrev R3 #4): zero is input hard,
    // never a clean deadman / WaitTimeout.
    let WaitRequest {
        channel,
        timeout_secs,
        team,
        contains,
        pattern,
        after,
        replace_wait_id,
        emit_wait_ids,
    } = req;
    validate_wait_timeout_secs(timeout_secs)?;
    validate_wait_channel_v3_strings(channel, team, contains, pattern, after)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    // Pure filter compile only (no provider). Ownership acquire is after
    // resolve + explicit-after bind and before subscribe/backfill.
    WaitPredicate::compile("pending", "pending", contains, pattern)?;

    let resolved = provider_retry(state, channel, deadline, || async {
        state.client.resolve_channel(channel, team).await
    })
    .await?;

    if let Some(anchor) = after {
        if anchor.is_empty() {
            return Err(CoreError::WaitFilterInvalid(
                "empty --after is refused".into(),
            ));
        }
        establish_baseline(state, channel, &resolved.channel_id, Some(anchor), deadline).await?;
    }

    let remaining = deadline.saturating_duration_since(Instant::now());
    let lease = state
        .wait_owners
        .acquire(
            &resolved.channel_id,
            &resolved.team_name,
            &resolved.channel_name,
            replace_wait_id,
            remaining,
        )
        .await?;
    // Subscribe/backfill only after a successful acquire. Tests use
    // armed_count as the provider-I/O gate.
    state.wait_owners.note_arm();
    let (session, _guard) = lease.into_guard();

    let is_monitored = state
        .profile
        .monitored_channels
        .iter()
        .any(|m| m.eq_ignore_ascii_case(channel));

    let inner = async {
        if is_monitored {
            wait_push_path(state, channel, team, contains, pattern, after, deadline).await
        } else {
            let predicate =
                WaitPredicate::compile(&state.my_user_id, &resolved.channel_id, contains, pattern)?;
            wait_rest_path(state, channel, &predicate, after, deadline).await
        }
    };

    let result = tokio::select! {
        biased;
        _ = session.cancel.cancelled() => Err(CoreError::WaitReplaced {
            wait_id: session.wait_id.clone(),
            replaced_by_wait_id: session.replaced_by_id(),
        }),
        res = inner => res,
    };

    match result {
        Ok(mut wr) if emit_wait_ids => {
            wr.wait_id = Some(session.wait_id);
            wr.replaced_wait_id = session.replaced_wait_id;
            Ok(wr)
        }
        other => other,
    }
}

/// Whether this RPC method uses the A1 waitprims first-match engine.
/// Legacy `wait_channel` / `wait_channel_v2` stay on the established paths.
pub(crate) fn uses_first_match_engine(method: &str) -> bool {
    method == chanvoy_core::WAIT_CHANNEL_V3_METHOD
}

/// PER-040 v3 only: first-match hold. Legacy wait RPCs must not call this.
pub async fn wait_with_params_v3(
    state: &AppState,
    req: WaitRequest<'_>,
) -> Result<WaitResult, CoreError> {
    let WaitRequest {
        channel,
        timeout_secs,
        team,
        contains,
        pattern,
        after,
        replace_wait_id,
        emit_wait_ids,
    } = req;
    validate_wait_timeout_secs(timeout_secs)?;
    validate_wait_channel_v3_strings(channel, team, contains, pattern, after)?;
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);

    WaitPredicate::compile("pending", "pending", contains, pattern)?;

    let resolved = provider_retry(state, channel, deadline, || async {
        state.client.resolve_channel(channel, team).await
    })
    .await?;

    let remaining = deadline.saturating_duration_since(Instant::now());
    let lease = state
        .wait_owners
        .acquire(
            &resolved.channel_id,
            &resolved.team_name,
            &resolved.channel_name,
            replace_wait_id,
            remaining,
        )
        .await?;
    state.wait_owners.note_arm();
    let (session, guard) = lease.into_guard();

    let predicate =
        WaitPredicate::compile(&state.my_user_id, &resolved.channel_id, contains, pattern)?;

    let result = crate::waitprims_hold::run_single_channel_first_match(
        state,
        crate::waitprims_hold::FirstMatchWait {
            channel,
            after,
            predicate,
            deadline,
            session: &session,
            guard,
        },
    )
    .await;

    match result {
        Ok(mut wr) if emit_wait_ids => {
            wr.wait_id = Some(session.wait_id);
            wr.replaced_wait_id = session.replaced_wait_id;
            Ok(wr)
        }
        other => other,
    }
}

/// Monitored path: **subscribe first**, then resolve/compile/anchor/backfill
/// while draining the receiver (devrev D1 / AC-W3).
pub(crate) async fn wait_push_path(
    state: &AppState,
    channel: &str,
    team: Option<&str>,
    contains: Option<&str>,
    pattern: Option<&str>,
    after: Option<&str>,
    deadline: Instant,
) -> Result<WaitResult, CoreError> {
    state.wait_owners.note_provider_io();
    let mut rx = state.event_bus.subscribe();
    let mut bus_buffer: VecDeque<Arc<DaemonEvent>> = VecDeque::new();
    drain_bus(&mut rx, &mut bus_buffer, channel)?;

    // Resolve while continuously draining (subscribe already done).
    let channel_id = {
        let resolve = provider_retry(state, channel, deadline, || async {
            state
                .client
                .resolve_channel(channel, team)
                .await
                .map(|r| r.channel_id)
        });
        tokio::pin!(resolve);
        loop {
            tokio::select! {
                res = &mut resolve => break res?,
                _ = sleep(Duration::from_millis(5)) => {
                    drain_bus(&mut rx, &mut bus_buffer, channel)?;
                }
            }
        }
    };
    let predicate = WaitPredicate::compile(&state.my_user_id, &channel_id, contains, pattern)?;
    let pred_channel = predicate.channel_id().to_string();

    // Explicit --after: bus may only fire posts confirmed in the provider
    // `after` relation (devrev D2). None = bare wait (post-sub bus eligible).
    let mut after_eligible: Option<HashSet<String>> = after.map(|_| HashSet::new());

    let (scan_after, _rest_baseline) = {
        let baseline = establish_baseline(state, channel, &pred_channel, after, deadline);
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
    let mut backfill_msgs: Vec<Message> = Vec::new();
    if let Some(ref anchor) = scan_after {
        let fetch = provider_retry(state, channel, deadline, || {
            let a = anchor.clone();
            let ch = pred_channel.clone();
            async move { state.client.posts_after_by_channel_id(&ch, &a).await }
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
    note_after_eligible(&mut after_eligible, &backfill_msgs);

    drain_bus(&mut rx, &mut bus_buffer, channel)?;
    if let Some(hit) = first_match_bus_then_rest(
        &backfill_msgs,
        &bus_buffer,
        &predicate,
        &processed,
        after_eligible.as_ref(),
    ) {
        return Ok(one_message_result(channel, hit));
    }
    for m in &backfill_msgs {
        processed.insert(m.id.clone());
    }
    // F1: under explicit --after, retain body-matching bus candidates that
    // are not yet REST-confirmed; do not mark them processed.
    reconcile_bus_after_eval(
        &mut bus_buffer,
        &predicate,
        &mut processed,
        after_eligible.as_ref(),
    );
    // F2: initial posts_after(anchor) is exhaustive — drop proven non-members.
    // (Only when an explicit after anchor was used; bare wait has no set.)
    if after.is_some() {
        drop_pending_non_members_after_success(
            &mut bus_buffer,
            &predicate,
            after_eligible.as_ref(),
        );
    }

    // D5: advance exclusive cursor past initial backfill so live REST
    // does not re-page the entire backlog from the original anchor.
    let mut scan_cursor = if let Some(last) = backfill_msgs.last() {
        Some(last.id.clone())
    } else {
        scan_after
    };

    let mut clean_observe = true;
    let mut saw_healthy_inbound = false;
    let mut saw_successful_rest = false;

    loop {
        if Instant::now() >= deadline {
            return push_deadline_outcome(
                state,
                channel,
                clean_observe,
                saw_healthy_inbound,
                saw_successful_rest,
            )
            .await;
        }

        let remaining = deadline.saturating_duration_since(Instant::now());
        let idle = REST_IDLE.min(remaining);
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(event) => {
                        // D3: only healthy *inbound* observation counts.
                        if let DaemonEventPayloadInner::Inbound(_) = &event.payload {
                            if ws_connection_healthy(state).await {
                                saw_healthy_inbound = true;
                                clean_observe = true;
                            }
                        }
                        bus_buffer.push_back(event);
                        if let Some(hit) = first_match_bus_then_rest(
                            &[],
                            &bus_buffer,
                            &predicate,
                            &processed,
                            after_eligible.as_ref(),
                        ) {
                            return Ok(one_message_result(channel, hit));
                        }
                        reconcile_bus_after_eval(
                            &mut bus_buffer,
                            &predicate,
                            &mut processed,
                            after_eligible.as_ref(),
                        );
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        // Drain anything left, then REST recover while still
                        // draining the bus (devrev R3 #2).
                        let _ = drain_bus(&mut rx, &mut bus_buffer, channel);
                        let cursor_snap = scan_cursor.clone();
                        let page = {
                            let recover = lag_recover_page(
                                state,
                                channel,
                                &predicate,
                                cursor_snap.as_deref(),
                                deadline,
                            );
                            tokio::pin!(recover);
                            loop {
                                tokio::select! {
                                    res = &mut recover => break res,
                                    _ = sleep(Duration::from_millis(5)) => {
                                        drain_bus(&mut rx, &mut bus_buffer, channel)?;
                                        if let Some(hit) = first_match_bus_then_rest(
                                            &[],
                                            &bus_buffer,
                                            &predicate,
                                            &processed,
                                            after_eligible.as_ref(),
                                        ) {
                                            return Ok(one_message_result(channel, hit));
                                        }
                                        reconcile_bus_after_eval(
                                            &mut bus_buffer,
                                            &predicate,
                                            &mut processed,
                                            after_eligible.as_ref(),
                                        );
                                    }
                                }
                            }
                        };
                        match page {
                            Ok(page) => {
                                note_after_eligible(&mut after_eligible, &page);
                                saw_successful_rest = true;
                                clean_observe = true;
                                drain_bus(&mut rx, &mut bus_buffer, channel)?;
                                if let Some(hit) = first_match_bus_then_rest(
                                    &page,
                                    &bus_buffer,
                                    &predicate,
                                    &processed,
                                    after_eligible.as_ref(),
                                ) {
                                    return Ok(one_message_result(channel, hit));
                                }
                                reconcile_bus_after_eval(
                                    &mut bus_buffer,
                                    &predicate,
                                    &mut processed,
                                    after_eligible.as_ref(),
                                );
                                // F2: successful exhaustive posts_after — drop non-members.
                                drop_pending_non_members_after_success(
                                    &mut bus_buffer,
                                    &predicate,
                                    after_eligible.as_ref(),
                                );
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
                if Instant::now() >= deadline {
                    return push_deadline_outcome(
                        state,
                        channel,
                        clean_observe,
                        saw_healthy_inbound,
                        saw_successful_rest,
                    )
                    .await;
                }
                // Continuous bus drain through the REST future (devrev R3 #2).
                let cursor_snap = scan_cursor.clone();
                let page = {
                    let fetch_fut = async {
                        if let Some(ref cursor) = cursor_snap {
                            provider_retry(state, channel, deadline, || {
                                let c = cursor.clone();
                                let ch = pred_channel.clone();
                                async move {
                                    state
                                        .client
                                        .posts_after_by_channel_id(&ch, &c)
                                        .await
                                }
                            })
                            .await
                        } else {
                            empty_at_arm_observation(state, channel, &predicate, deadline)
                                .await
                        }
                    };
                    tokio::pin!(fetch_fut);
                    loop {
                        tokio::select! {
                            res = &mut fetch_fut => break res,
                            _ = sleep(Duration::from_millis(5)) => {
                                drain_bus(&mut rx, &mut bus_buffer, channel)?;
                                if let Some(hit) = first_match_bus_then_rest(
                                    &[],
                                    &bus_buffer,
                                    &predicate,
                                    &processed,
                                    after_eligible.as_ref(),
                                ) {
                                    return Ok(one_message_result(channel, hit));
                                }
                                reconcile_bus_after_eval(
                                    &mut bus_buffer,
                                    &predicate,
                                    &mut processed,
                                    after_eligible.as_ref(),
                                );
                            }
                        }
                    }
                };
                match page {
                    Ok(page) => {
                        note_after_eligible(&mut after_eligible, &page);
                        saw_successful_rest = true;
                        clean_observe = true;
                        // Live REST must drain bus before firing (devrev).
                        drain_bus(&mut rx, &mut bus_buffer, channel)?;
                        if let Some(hit) = first_match_bus_then_rest(
                            &page,
                            &bus_buffer,
                            &predicate,
                            &processed,
                            after_eligible.as_ref(),
                        ) {
                            return Ok(one_message_result(channel, hit));
                        }
                        reconcile_bus_after_eval(
                            &mut bus_buffer,
                            &predicate,
                            &mut processed,
                            after_eligible.as_ref(),
                        );
                        // F2: successful exhaustive posts_after — drop non-members.
                        drop_pending_non_members_after_success(
                            &mut bus_buffer,
                            &predicate,
                            after_eligible.as_ref(),
                        );
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
                            saw_healthy_inbound,
                            saw_successful_rest,
                        )
                        .await;
                    }
                    Err(other) => return Err(other),
                }
            }
        }
    }
}

pub(crate) async fn wait_rest_path(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    after: Option<&str>,
    deadline: Instant,
) -> Result<WaitResult, CoreError> {
    state.wait_owners.note_provider_io();
    let (scan_cursor, rest_baseline) =
        establish_baseline(state, channel, predicate.channel_id(), after, deadline).await?;
    wait_rest_from_cursor(
        state,
        channel,
        predicate,
        scan_cursor,
        rest_baseline,
        deadline,
    )
    .await
}

/// REST observe from an already-resolved exclusive cursor. Does not
/// re-establish a baseline (A1 bind cursor must be consumed as-is).
pub(crate) async fn wait_rest_from_cursor(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    mut scan_cursor: Option<String>,
    rest_baseline: HashSet<String>,
    deadline: Instant,
) -> Result<WaitResult, CoreError> {
    let mut processed: HashSet<String> = HashSet::new();

    if let Some(ref anchor) = scan_cursor {
        let backfill = provider_retry(state, channel, deadline, || {
            let a = anchor.clone();
            let ch = predicate.channel_id().to_string();
            async move { state.client.posts_after_by_channel_id(&ch, &a).await }
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
                let ch = predicate.channel_id().to_string();
                async move { state.client.posts_after_by_channel_id(&ch, &c).await }
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
pub(crate) async fn empty_at_arm_observation(
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

pub(crate) async fn establish_baseline(
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

pub(crate) fn note_after_eligible(after_eligible: &mut Option<HashSet<String>>, page: &[Message]) {
    if let Some(el) = after_eligible.as_mut() {
        for m in page {
            el.insert(m.id.clone());
        }
    }
}

fn bus_id_after_eligible(after_eligible: Option<&HashSet<String>>, post_id: &str) -> bool {
    match after_eligible {
        None => true,                     // bare wait: post-sub bus is eligible
        Some(el) => el.contains(post_id), // explicit --after: REST-confirmed only
    }
}

/// Prefer bus **arrival order**, then REST chronological first match.
/// Does not reorder bus events by provider timestamps (devrev R2).
pub(crate) fn first_match_bus_then_rest(
    rest: &[Message],
    bus: &VecDeque<Arc<DaemonEvent>>,
    predicate: &WaitPredicate,
    processed: &HashSet<String>,
    after_eligible: Option<&HashSet<String>>,
) -> Option<Message> {
    for event in bus {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if processed.contains(&p.post_id) {
                continue;
            }
            if !bus_id_after_eligible(after_eligible, &p.post_id) {
                continue;
            }
            if predicate.matches_inbound(p) {
                return Some(inbound_to_message(p));
            }
        }
    }
    first_match(rest, predicate, processed)
}

/// After a non-firing eval, mark terminal bus candidates processed and drop
/// them. Under explicit `--after`, body-matching posts that are not yet in
/// `after_eligible` stay pending so a later REST confirmation can fire them
/// exactly once (entarch/secrev F1). Bare wait treats all channel inbound as
/// immediately terminal (post-sub bus is eligible without REST).
pub(crate) fn reconcile_bus_after_eval(
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    predicate: &WaitPredicate,
    processed: &mut HashSet<String>,
    after_eligible: Option<&HashSet<String>>,
) {
    let mut keep = VecDeque::new();
    for event in bus.drain(..) {
        match &event.payload {
            DaemonEventPayloadInner::Inbound(p) if p.channel_id == predicate.channel_id() => {
                if processed.contains(&p.post_id) {
                    continue;
                }
                let eligible = bus_id_after_eligible(after_eligible, &p.post_id);
                if !eligible && predicate.matches_inbound(p) {
                    // Pending F1 candidate: match body, after-membership unknown.
                    keep.push_back(event);
                    continue;
                }
                processed.insert(p.post_id.clone());
            }
            _ => {
                // Control-plane / wrong-channel: drop without polluting processed.
            }
        }
    }
    *bus = keep;
}

/// F2: after a **successful** exhaustive `posts_after` scan, drop pending
/// after-gated body-matches whose ids are still absent from `after_eligible`.
/// Absence is authoritative non-membership for that scan. Does **not** mark
/// them `processed`, so a later REST page that does return the id can still
/// fire (concurrent race). Failed/timeout REST paths must not call this.
/// Bare wait (`after_eligible == None`) is a no-op.
pub(crate) fn drop_pending_non_members_after_success(
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    predicate: &WaitPredicate,
    after_eligible: Option<&HashSet<String>>,
) {
    let Some(eligible) = after_eligible else {
        return;
    };
    let mut keep = VecDeque::new();
    for event in bus.drain(..) {
        match &event.payload {
            DaemonEventPayloadInner::Inbound(p)
                if p.channel_id == predicate.channel_id() && predicate.matches_inbound(p) =>
            {
                if eligible.contains(&p.post_id) {
                    // Confirmed member still pending (should be rare after
                    // first_match); keep for the next eval.
                    keep.push_back(event);
                }
                // else: proven non-member for this complete scan — drop.
            }
            DaemonEventPayloadInner::Inbound(p) if p.channel_id == predicate.channel_id() => {
                // Non-matching channel inbound: drop (same as reconcile).
                let _ = p;
            }
            other => {
                // Preserve unexpected payloads only if they were kept before;
                // control-plane is not match material.
                let _ = other;
            }
        }
    }
    *bus = keep;
}

pub(crate) fn drain_bus(
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

pub(crate) fn one_message_result(channel: &str, message: Message) -> WaitResult {
    WaitResult {
        channel: channel.to_string(),
        messages: vec![message],
        wait_id: None,
        replaced_wait_id: None,
    }
}

pub(crate) async fn lag_recover_page(
    state: &AppState,
    channel: &str,
    predicate: &WaitPredicate,
    scan_cursor: Option<&str>,
    deadline: Instant,
) -> Result<Vec<Message>, CoreError> {
    if let Some(cursor) = scan_cursor {
        provider_retry(state, channel, deadline, || {
            let c = cursor.to_string();
            let ch = predicate.channel_id().to_string();
            async move { state.client.posts_after_by_channel_id(&ch, &c).await }
        })
        .await
    } else {
        empty_at_arm_observation(state, channel, predicate, deadline).await
    }
}

pub(crate) async fn ws_connection_healthy(state: &AppState) -> bool {
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

/// Clean deadman only when observation actually worked (secrev/entarch B,
/// devrev D3). Connection-state bus traffic is not evidence; a currently
/// healthy WS with zero successful observes is still provider/hard, not
/// clean silence (secrev D3 residual).
async fn push_deadline_outcome(
    _state: &AppState,
    channel: &str,
    clean_observe: bool,
    saw_healthy_inbound: bool,
    saw_successful_rest: bool,
) -> Result<WaitResult, CoreError> {
    if !clean_observe {
        return Err(CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "deadline reached while provider observation was failing".into(),
        });
    }
    // Honest silence requires a real observe path during the window:
    // healthy inbound and/or successful REST. Do not fall through on
    // "WS healthy *now*" without an observe success.
    if !saw_healthy_inbound && !saw_successful_rest {
        return Err(CoreError::WaitProviderDegraded {
            channel: channel.to_string(),
            message: "wait ended without a healthy push or REST observation".into(),
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

pub(crate) async fn provider_retry<T, F, Fut>(
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
    fn all_server_errors_and_429_are_retryable() {
        for code in [429u16, 500, 501, 502, 503, 504, 507] {
            let status = StatusCode::from_u16(code).expect("status");
            let err = CoreError::Api {
                status,
                message: format!("probe {code}"),
            };
            assert!(
                is_retryable_provider(&err),
                "status {code} must be retryable under AC-W6"
            );
        }
        let client_err = CoreError::Api {
            status: StatusCode::BAD_REQUEST,
            message: "no".into(),
        };
        assert!(!is_retryable_provider(&client_err));
    }

    #[test]
    fn first_match_engine_is_v3_only() {
        assert!(uses_first_match_engine(
            chanvoy_core::WAIT_CHANNEL_V3_METHOD
        ));
        assert!(!uses_first_match_engine("wait_channel"));
        assert!(!uses_first_match_engine("wait_channel_v2"));
        assert!(!uses_first_match_engine("wait_channels_v1"));
    }

    #[test]
    fn zero_timeout_is_filter_invalid_not_deadman() {
        let err = validate_wait_timeout_secs(0).unwrap_err();
        assert!(
            matches!(err, CoreError::WaitFilterInvalid(_)),
            "zero timeout must be input hard, got {err:?}"
        );
        assert!(validate_wait_timeout_secs(1).is_ok());
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
        let hit = first_match_bus_then_rest(&rest, &bus, &p, &HashSet::new(), None).unwrap();
        assert_eq!(
            hit.id, "p1",
            "must prefer bus arrival over REST timestamp order"
        );
    }

    #[test]
    fn explicit_after_blocks_bus_until_rest_confirms() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("old", "X pre-anchor", 50));
        let eligible = HashSet::new();
        assert!(
            first_match_bus_then_rest(&[], &bus, &p, &HashSet::new(), Some(&eligible)).is_none(),
            "pre-anchor bus must not fire under --after"
        );
        let mut eligible = HashSet::new();
        eligible.insert("new".into());
        bus.push_back(inbound_event("new", "X post-anchor", 150));
        let hit =
            first_match_bus_then_rest(&[], &bus, &p, &HashSet::new(), Some(&eligible)).unwrap();
        assert_eq!(hit.id, "new");
    }

    /// F1 live sequence: bus match before REST eligibility must not be
    /// marked processed / dropped; REST confirm then fires exactly once.
    #[test]
    fn after_gated_bus_match_survives_reconcile_until_rest_confirms() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("new", "X post-anchor", 150));
        let mut eligible = HashSet::new();
        let mut processed = HashSet::new();

        assert!(
            first_match_bus_then_rest(&[], &bus, &p, &processed, Some(&eligible)).is_none(),
            "unconfirmed after-gated match must not fire yet"
        );
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        assert!(
            !processed.contains("new"),
            "must not mark unconfirmed body-match processed"
        );
        assert_eq!(bus.len(), 1, "must retain pending candidate");

        eligible.insert("new".into());
        let hit = first_match_bus_then_rest(&[], &bus, &p, &processed, Some(&eligible))
            .expect("REST-confirmed bus match must fire once");
        assert_eq!(hit.id, "new");

        // Same id via REST page must not double-fire after processed.
        processed.insert(hit.id.clone());
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        assert!(bus.is_empty());
        assert!(
            first_match_bus_then_rest(
                &[msg("new", "u", "X post-anchor", 150)],
                &bus,
                &p,
                &processed,
                Some(&eligible),
            )
            .is_none(),
            "must not fire a second time"
        );
    }

    /// F1/F2: delayed pre-anchor matching bus event never fires; after a
    /// successful complete after-scan it is dropped (not retained forever).
    #[test]
    fn pre_anchor_bus_match_dropped_after_successful_scan() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("old", "X pre-anchor", 50));
        let mut eligible = HashSet::new();
        let mut processed = HashSet::new();

        assert!(first_match_bus_then_rest(&[], &bus, &p, &processed, Some(&eligible)).is_none());
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        assert!(!processed.contains("old"));
        assert_eq!(bus.len(), 1, "pending until a successful complete scan");

        // Successful exhaustive scan confirms only "other" — not "old".
        eligible.insert("other".into());
        let rest = vec![msg("other", "u", "no match body", 200)];
        assert!(
            first_match_bus_then_rest(&rest, &bus, &p, &processed, Some(&eligible)).is_none(),
            "pre-anchor bus must not fire when REST never confirms its id"
        );
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        drop_pending_non_members_after_success(&mut bus, &p, Some(&eligible));
        assert!(bus.is_empty(), "proven non-member must leave the buffer");
        assert!(
            !processed.contains("old"),
            "non-member drop must not mark processed (REST race safety)"
        );
    }

    /// F2: failed/timeout REST must not drop pending after-gated matches.
    #[test]
    fn failed_scan_retains_pending_after_gated_matches() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("maybe", "X candidate", 150));
        let eligible = HashSet::new();
        let mut processed = HashSet::new();
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        assert_eq!(bus.len(), 1);
        // Simulate failure path: reconcile only, no drop_pending_non_members.
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
        assert_eq!(bus.len(), 1, "must retain pending across failed REST");
    }

    /// F2: repeated delayed pre-anchor noise does not grow the buffer after
    /// each successful complete scan.
    #[test]
    fn repeated_pre_anchor_noise_stays_bounded_after_success() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        let mut eligible = HashSet::new();
        eligible.insert("real".into());
        let mut processed = HashSet::new();
        for i in 0..50 {
            bus.push_back(inbound_event(&format!("old{i}"), "X noise", i));
            reconcile_bus_after_eval(&mut bus, &p, &mut processed, Some(&eligible));
            drop_pending_non_members_after_success(&mut bus, &p, Some(&eligible));
        }
        assert!(
            bus.is_empty(),
            "buffer must not retain pre-anchor noise across successful scans"
        );
        assert!(bus.len() < 5, "bounded after repeated noise");
    }

    /// Bare wait: non-match channel inbound is terminal and dropped.
    #[test]
    fn bare_wait_reconcile_marks_channel_inbound_processed() {
        let p = pred(Some("X"), None);
        let mut bus = VecDeque::new();
        bus.push_back(inbound_event("p1", "nope", 1));
        let mut processed = HashSet::new();
        reconcile_bus_after_eval(&mut bus, &p, &mut processed, None);
        assert!(processed.contains("p1"));
        assert!(bus.is_empty());
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
