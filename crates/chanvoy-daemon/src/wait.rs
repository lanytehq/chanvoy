//! PER-038 wait engine: content filter, exclusive baseline, absolute
//! deadman, one-message result, shared predicate, provider retry.
//!
//! Causal order (entarch F3 / pre-build Q3): subscribe → establish
//! anchor/tip → backfill through shared predicate → consume push with
//! dedupe. Bare-wait membership is bus-arrival (and REST posts after the
//! tip id), never `create_at` exclusivity.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chanvoy_core::{
    CoreError, DaemonEvent, DaemonEventPayloadInner, InboundEventPayload, Message, WaitResult,
};
use regex::RegexBuilder;
use reqwest::StatusCode;
use tokio::sync::broadcast;
use tokio::time::{sleep, Instant};

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

    /// Shared message predicate: correct channel (when known), not self, body filter.
    pub fn matches_message(&self, message: &Message) -> bool {
        message.user_id != self.my_user_id && self.body_matches(&message.message)
    }

    /// Shared inbound-event predicate: resolved channel_id, not self, body filter.
    /// Membership (post-arm / after-anchor) is decided by the engine, not here.
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

    // Compile filters before channel resolution so bad patterns fail closed
    // without a Mattermost round-trip (channel_id filled in after resolve).
    // We compile against a placeholder channel_id then rebuild — cheaper than
    // two validation paths; source validation is pure.
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
    // 1. Subscribe first and buffer (no-replay EventBus contract).
    let mut rx = state.event_bus.subscribe();
    let mut bus_buffer: VecDeque<Arc<DaemonEvent>> = VecDeque::new();

    // Drain anything already queued (should be empty for a fresh sub).
    loop {
        match rx.try_recv() {
            Ok(ev) => bus_buffer.push_back(ev),
            Err(broadcast::error::TryRecvError::Empty) => break,
            Err(broadcast::error::TryRecvError::Lagged(_)) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "event bus lagged before wait baseline was established".into(),
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

    // 2. Resolve anchor / bare tip (after subscribe).
    // `rest_baseline` excludes pre-arm REST history only — never bus events
    // (entarch Q3: snapshot ids must not suppress queued causal events).
    let (scan_after, rest_baseline) =
        establish_baseline(state, channel, predicate.channel_id(), after, deadline).await?;
    let mut processed: HashSet<String> = HashSet::new();

    // 3. Backfill posts after baseline through shared predicate.
    if let Some(ref anchor) = scan_after {
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
        let mut rest_exclude = rest_baseline.clone();
        rest_exclude.extend(processed.iter().cloned());
        if let Some(hit) = first_match(&backfill, predicate, &rest_exclude) {
            return Ok(one_message_result(channel, hit));
        }
        for m in &backfill {
            processed.insert(m.id.clone());
        }
    }

    // 4. Evaluate buffered bus events (post-subscription by construction).
    while let Some(event) = bus_buffer.pop_front() {
        if let Some(hit) = evaluate_bus_event(&event, predicate, &processed) {
            return Ok(one_message_result(channel, hit));
        }
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if p.channel_id == predicate.channel_id() {
                processed.insert(p.post_id.clone());
            }
        }
    }

    // 5. Live loop: push + lag recovery via forward page from scan cursor.
    let mut scan_cursor = scan_after;
    let mut clean_observe = true;

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

        let remaining = deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Ok(event)) => {
                clean_observe = true;
                if let Some(hit) = evaluate_bus_event(&event, predicate, &processed) {
                    return Ok(one_message_result(channel, hit));
                }
                if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
                    if p.channel_id == predicate.channel_id() {
                        processed.insert(p.post_id.clone());
                    }
                }
            }
            Ok(Err(broadcast::error::RecvError::Lagged(_))) => {
                // Forward page through the shared predicate. Empty-cursor
                // lag must evaluate the recovered tip page (not only set
                // the cursor) or the post that arrived during lag is
                // permanently skipped by posts_after (devrev r1).
                match lag_recover_page(state, channel, predicate, scan_cursor.as_deref(), deadline)
                    .await
                {
                    Ok(page) => {
                        clean_observe = true;
                        let mut rest_exclude = rest_baseline.clone();
                        rest_exclude.extend(processed.iter().cloned());
                        if let Some(hit) = first_match(&page, predicate, &rest_exclude) {
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
                    Err(other) => return Err(other),
                }
            }
            Ok(Err(broadcast::error::RecvError::Closed)) => {
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "event bus closed during wait".into(),
                });
            }
            Err(_) => {
                if clean_observe {
                    return Err(CoreError::WaitTimeout(channel.to_string()));
                }
                return Err(CoreError::WaitProviderDegraded {
                    channel: channel.to_string(),
                    message: "deadline reached while provider observation was failing".into(),
                });
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

    // Initial backfill
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
            // Empty at arm: recover a wider latest window and evaluate it
            // before advancing the cursor (same class as push lag recovery).
            provider_retry(state, channel, deadline, || async {
                state
                    .client
                    .latest_channel_messages_by_id(predicate.channel_id(), 200)
                    .await
            })
            .await
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

/// Establish exclusive baseline after subscription (push path) or at arm (REST).
///
/// Returns `(scan_cursor, rest_baseline)`:
/// * `--after R`: prove R + channel bind; scan from R; baseline `{R}`.
/// * bare: tip snapshot; scan from tip id; baseline = tip-page ids (REST only).
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

fn evaluate_bus_event(
    event: &DaemonEvent,
    predicate: &WaitPredicate,
    processed: &HashSet<String>,
) -> Option<Message> {
    match &event.payload {
        DaemonEventPayloadInner::Inbound(p) if predicate.matches_inbound(p) => {
            // `processed` is post-evaluation dedupe (already returned / already
            // accepted from backfill). It is NOT the tip snapshot set — bus
            // events remain eligible even when the tip page also saw the id.
            if processed.contains(&p.post_id) {
                return None;
            }
            Some(inbound_to_message(p))
        }
        _ => None,
    }
}

fn one_message_result(channel: &str, message: Message) -> WaitResult {
    WaitResult {
        channel: channel.to_string(),
        messages: vec![message],
    }
}

/// Lag / empty-cursor recovery page: with a cursor, page after it; without,
/// take a latest window and evaluate it (must not only stamp the tip).
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
        provider_retry(state, channel, deadline, || async {
            state
                .client
                .latest_channel_messages_by_id(predicate.channel_id(), 200)
                .await
        })
        .await
    }
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
    let _ = state; // reserved for future Retry-After header plumbing
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
        match op().await {
            Ok(v) => return Ok(v),
            Err(err) if is_terminal_auth(&err) => return Err(err),
            Err(err) if is_retryable_provider(&err) => {
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
            Err(err) => return Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chanvoy_core::{InboundEventPayload, Provider};

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
        assert!(
            matches!(err, CoreError::WaitTimeout(_)),
            "no retryable failure → WaitTimeout, got {err:?}"
        );
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

    #[test]
    fn tip_id_in_rest_baseline_does_not_block_bus_eval() {
        // rest_baseline is not passed to evaluate_bus_event — only processed.
        let p = pred(Some("X"), None);
        let processed = HashSet::new();
        let event = DaemonEvent {
            seq: 1,
            kind: chanvoy_core::DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: "ch-1".into(),
                channel_name: "c".into(),
                post_id: "tip-also-on-bus".into(),
                root_id: "tip-also-on-bus".into(),
                sender_id: "u".into(),
                sender_username: "u".into(),
                message: "X here".into(),
                create_at: 1,
                received_at: 1,
                mentioned: false,
            }),
        };
        let hit = evaluate_bus_event(&event, &p, &processed).expect("bus eligible");
        assert_eq!(hit.id, "tip-also-on-bus");
    }

    #[test]
    fn event_bus_has_no_replay_of_pre_subscribe_events() {
        use chanvoy_core::{DaemonEvent, DaemonEventKind, EventBus};
        use std::sync::Arc;

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
        // Pre-subscribe event must not be delivered.
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
        let _ = Arc::strong_count(&got);
    }
}
