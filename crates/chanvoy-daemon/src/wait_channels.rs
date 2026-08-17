//! Multi-arm `wait_channels_v1` engine.
//!
//! Subscribe once, resolve and bind every arm, snapshot omitted
//! baselines, backfill, then consume the live edge. Any arm failure
//! cancels the whole operation. Events arriving after subscribe and
//! before an omitted-baseline snapshot are retained and evaluated at
//! most once per arm.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use chanvoy_core::{
    first_backfill_winner, validate_wait_channels_params, CoreError, DaemonEvent,
    DaemonEventPayloadInner, Message, WaitChannelSelector, WaitChannelsParams, WaitChannelsResult,
};
use tokio::sync::broadcast;
use tokio::time::{sleep, Instant};

use crate::wait::{
    drain_bus, empty_at_arm_observation, establish_baseline, first_match, inbound_to_message,
    lag_recover_page, note_after_eligible, provider_retry, ws_connection_healthy, WaitPredicate,
    REST_IDLE,
};
use crate::AppState;

struct ArmedArm {
    selector: WaitChannelSelector,
    channel_id: String,
    predicate: WaitPredicate,
    after: Option<String>,
    after_eligible: Option<HashSet<String>>,
    scan_cursor: Option<String>,
    processed: HashSet<String>,
    saw_successful_rest: bool,
    saw_healthy_inbound: bool,
    clean_observe: bool,
}

/// Bounded retain window used by hermetic seam hooks. Snapshot/backfill
/// must not erase it. Each `(arm, post_id)` pair is evaluated at most
/// once.
#[cfg(test)]
#[derive(Default)]
struct FanInRetain {
    events: Vec<Arc<DaemonEvent>>,
    evaluated: HashSet<(String, String)>,
}

#[cfg(test)]
impl FanInRetain {
    fn retain(&mut self, event: Arc<DaemonEvent>) {
        self.events.push(event);
    }

    fn len(&self) -> usize {
        self.events.len()
    }

    /// Snapshot / backfill must not drop retained events.
    fn snapshot_noop(&self) {}

    fn eval_arm(&mut self, channel_id: &str, predicate: &WaitPredicate) -> Option<Message> {
        for event in &self.events {
            let DaemonEventPayloadInner::Inbound(payload) = &event.payload else {
                continue;
            };
            if payload.channel_id != channel_id {
                continue;
            }
            let key = (channel_id.to_string(), payload.post_id.clone());
            if !self.evaluated.insert(key) {
                continue;
            }
            if predicate.matches_inbound(payload) {
                return Some(inbound_to_message(payload));
            }
        }
        None
    }
}

pub async fn wait_channels_with_params(
    state: &AppState,
    params: WaitChannelsParams,
) -> Result<
    (
        WaitChannelsResult,
        Option<crate::waitprims_fanin::StagedFanInConsume>,
    ),
    CoreError,
> {
    crate::waitprims_fanin::wait_channels_first_match(state, params).await
}

#[allow(dead_code)]
async fn wait_channels_legacy_unused(
    state: &AppState,
    params: WaitChannelsParams,
) -> Result<WaitChannelsResult, CoreError> {
    validate_wait_channels_params(&params)?;
    WaitPredicate::compile(
        "pending",
        "pending",
        params.contains.as_deref(),
        params.pattern.as_deref(),
    )?;

    let deadline = Instant::now() + Duration::from_secs(params.timeout_secs);
    let channels: Vec<WaitChannelSelector> = params
        .arms
        .iter()
        .map(chanvoy_core::WaitChannelArm::selector)
        .collect();

    let mut rx = state.event_bus.subscribe();
    let mut bus: VecDeque<Arc<DaemonEvent>> = VecDeque::new();
    drain_bus(&mut rx, &mut bus, "fan-in")?;

    let mut armed = Vec::with_capacity(params.arms.len());
    let mut seen_ids: HashSet<String> = HashSet::new();

    for arm in &params.arms {
        let selector = arm.selector();
        let qualified = selector.qualified();
        let resolve = provider_retry(state, &qualified, deadline, || async {
            state
                .client
                .resolve_channel(&qualified, None)
                .await
                .map(|r| r.channel_id)
        });
        let channel_id = match with_bus_drain(&mut rx, &mut bus, &qualified, resolve).await {
            Ok(id) => id,
            Err(err) => return Err(map_arm_err(&selector, err)),
        };
        if !seen_ids.insert(channel_id.clone()) {
            return Err(CoreError::WaitFilterInvalid(format!(
                "duplicate wait arm {qualified} (same canonical channel as another arm)"
            )));
        }
        let predicate = WaitPredicate::compile(
            &state.my_user_id,
            &channel_id,
            params.contains.as_deref(),
            params.pattern.as_deref(),
        )?;
        armed.push(ArmedArm {
            selector,
            channel_id,
            predicate,
            after: arm.after.clone(),
            after_eligible: arm.after.as_ref().map(|_| HashSet::new()),
            scan_cursor: None,
            processed: HashSet::new(),
            saw_successful_rest: false,
            saw_healthy_inbound: false,
            clean_observe: true,
        });
    }

    for arm in &mut armed {
        let qualified = arm.selector.qualified();
        let baseline = establish_baseline(
            state,
            &qualified,
            &arm.channel_id,
            arm.after.as_deref(),
            deadline,
        );
        let (scan_after, rest_baseline) =
            match with_bus_drain(&mut rx, &mut bus, &qualified, baseline).await {
                Ok(v) => v,
                Err(err) => return Err(map_arm_err(&arm.selector, err)),
            };
        let retained = retained_post_ids_for_channel(&bus, &arm.channel_id);
        merge_snapshot_into_processed(&mut arm.processed, rest_baseline, &retained);
        arm.scan_cursor = scan_after;
    }

    let mut backfill: Vec<(WaitChannelSelector, Message, String)> = Vec::new();
    for arm in &mut armed {
        let Some(anchor) = arm.scan_cursor.clone() else {
            continue;
        };
        let qualified = arm.selector.qualified();
        let fetch = provider_retry(state, &qualified, deadline, || {
            let a = anchor.clone();
            let ch = arm.channel_id.clone();
            async move { state.client.posts_after_by_channel_id(&ch, &a).await }
        });
        let page = match with_bus_drain(&mut rx, &mut bus, &qualified, fetch).await {
            Ok(page) => page,
            Err(err) => return Err(map_arm_err(&arm.selector, err)),
        };
        note_after_eligible(&mut arm.after_eligible, &page);
        arm.saw_successful_rest = true;
        if let Some(hit) = first_match(&page, &arm.predicate, &arm.processed) {
            backfill.push((arm.selector.clone(), hit, arm.channel_id.clone()));
        }
        if let Some(last) = page.last() {
            arm.scan_cursor = Some(last.id.clone());
        }
        for m in page {
            arm.processed.insert(m.id);
        }
    }

    drain_bus(&mut rx, &mut bus, "fan-in")?;
    if let Some((selector, message)) = first_live_match(&bus, &armed) {
        return Ok(WaitChannelsResult::match_one(channels, selector, message));
    }
    if let Some((selector, message, _)) = first_backfill_winner(&backfill) {
        return Ok(WaitChannelsResult::match_one(
            channels,
            selector.clone(),
            message.clone(),
        ));
    }
    for arm in &mut armed {
        reconcile_fan_in_arm(&mut bus, arm);
        if arm.after.is_some() {
            drop_pending_fan_in(&mut bus, arm);
        }
    }

    live_loop(state, &mut rx, &mut bus, &mut armed, channels, deadline).await
}

async fn live_loop(
    state: &AppState,
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    armed: &mut [ArmedArm],
    channels: Vec<WaitChannelSelector>,
    deadline: Instant,
) -> Result<WaitChannelsResult, CoreError> {
    loop {
        if Instant::now() >= deadline {
            return fan_in_deadline(armed);
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        let idle = REST_IDLE.min(remaining);
        tokio::select! {
            recv = rx.recv() => {
                match recv {
                    Ok(event) => {
                        if let DaemonEventPayloadInner::Inbound(payload) = &event.payload {
                            if ws_connection_healthy(state).await {
                                for arm in armed.iter_mut() {
                                    if payload.channel_id == arm.channel_id {
                                        arm.saw_healthy_inbound = true;
                                        arm.clean_observe = true;
                                    }
                                }
                            }
                        }
                        bus.push_back(event);
                        if let Some((selector, message)) = first_live_match(bus, armed) {
                            return Ok(WaitChannelsResult::match_one(channels, selector, message));
                        }
                        for arm in armed.iter_mut() {
                            reconcile_fan_in_arm(bus, arm);
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        let _ = drain_bus(rx, bus, "fan-in");
                        if let Some((selector, message)) = first_live_match(bus, armed) {
                            return Ok(WaitChannelsResult::match_one(channels, selector, message));
                        }
                        if let Some(hit) = recover_lagged(state, rx, bus, armed, deadline).await? {
                            return Ok(WaitChannelsResult::match_one(channels, hit.0, hit.1));
                        }
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        return Err(CoreError::WaitProviderDegraded {
                            channel: join_selectors(armed),
                            message: "event bus closed during wait".into(),
                        });
                    }
                }
            }
            _ = sleep(idle) => {
                if Instant::now() >= deadline {
                    return fan_in_deadline(armed);
                }
                if let Some(hit) = poll_rest_arms(state, rx, bus, armed, deadline).await? {
                    return Ok(WaitChannelsResult::match_one(channels, hit.0, hit.1));
                }
            }
        }
    }
}

async fn recover_lagged(
    state: &AppState,
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    armed: &mut [ArmedArm],
    deadline: Instant,
) -> Result<Option<(WaitChannelSelector, Message)>, CoreError> {
    for arm in armed.iter_mut() {
        let qualified = arm.selector.qualified();
        let recover = lag_recover_page(
            state,
            &qualified,
            &arm.predicate,
            arm.scan_cursor.as_deref(),
            deadline,
        );
        match with_bus_drain(rx, bus, &qualified, recover).await {
            Ok(page) => {
                note_after_eligible(&mut arm.after_eligible, &page);
                arm.saw_successful_rest = true;
                arm.clean_observe = true;
                drain_bus(rx, bus, &qualified)?;
                if let Some((selector, message)) = first_live_match(bus, std::slice::from_ref(arm))
                {
                    return Ok(Some((selector, message)));
                }
                if let Some(hit) = first_match(&page, &arm.predicate, &arm.processed) {
                    return Ok(Some((arm.selector.clone(), hit)));
                }
                if let Some(last) = page.last() {
                    arm.scan_cursor = Some(last.id.clone());
                    for m in page {
                        arm.processed.insert(m.id);
                    }
                }
            }
            Err(CoreError::WaitProviderDegraded { .. }) | Err(CoreError::WaitTimeout(_)) => {
                arm.clean_observe = false;
            }
            Err(other) => return Err(map_arm_err(&arm.selector, other)),
        }
    }
    Ok(None)
}

async fn poll_rest_arms(
    state: &AppState,
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    armed: &mut [ArmedArm],
    deadline: Instant,
) -> Result<Option<(WaitChannelSelector, Message)>, CoreError> {
    drain_bus(rx, bus, "fan-in")?;
    if let Some((selector, message)) = first_live_match(bus, armed) {
        return Ok(Some((selector, message)));
    }
    let mut deadline_hit = false;
    for i in 0..armed.len() {
        let qualified = armed[i].selector.qualified();
        let fetch = async {
            if let Some(cursor) = armed[i].scan_cursor.clone() {
                provider_retry(state, &qualified, deadline, || {
                    let c = cursor.clone();
                    let ch = armed[i].channel_id.clone();
                    async move { state.client.posts_after_by_channel_id(&ch, &c).await }
                })
                .await
            } else {
                empty_at_arm_observation(state, &qualified, &armed[i].predicate, deadline).await
            }
        };
        match with_bus_drain(rx, bus, &qualified, fetch).await {
            Ok(page) => {
                note_after_eligible(&mut armed[i].after_eligible, &page);
                armed[i].saw_successful_rest = true;
                armed[i].clean_observe = true;
                drain_bus(rx, bus, &qualified)?;
                if let Some((selector, message)) = first_live_match(bus, armed) {
                    return Ok(Some((selector, message)));
                }
                if let Some(hit) = first_match(&page, &armed[i].predicate, &armed[i].processed) {
                    return Ok(Some((armed[i].selector.clone(), hit)));
                }
                reconcile_fan_in_arm(bus, &mut armed[i]);
                if armed[i].after.is_some() {
                    drop_pending_fan_in(bus, &armed[i]);
                }
                if let Some(last) = page.last() {
                    if armed[i].scan_cursor.as_deref() != Some(last.id.as_str()) {
                        armed[i].scan_cursor = Some(last.id.clone());
                    }
                    for m in page {
                        armed[i].processed.insert(m.id);
                    }
                }
            }
            Err(CoreError::WaitProviderDegraded { .. }) => {
                armed[i].clean_observe = false;
            }
            Err(CoreError::WaitTimeout(_)) => {
                deadline_hit = true;
                break;
            }
            Err(other) => return Err(map_arm_err(&armed[i].selector, other)),
        }
    }
    if deadline_hit {
        return Err(fan_in_deadline(armed).unwrap_err());
    }
    Ok(None)
}

fn retained_post_ids_for_channel(
    bus: &VecDeque<Arc<DaemonEvent>>,
    channel_id: &str,
) -> HashSet<String> {
    bus.iter()
        .filter_map(|event| match &event.payload {
            DaemonEventPayloadInner::Inbound(payload) if payload.channel_id == channel_id => {
                Some(payload.post_id.clone())
            }
            _ => None,
        })
        .collect()
}

/// A later tip snapshot must not mark a retained post-sub event processed.
fn merge_snapshot_into_processed(
    processed: &mut HashSet<String>,
    snapshot_ids: impl IntoIterator<Item = String>,
    retained_ids: &HashSet<String>,
) {
    for id in snapshot_ids {
        if !retained_ids.contains(&id) {
            processed.insert(id);
        }
    }
}

/// Per-arm reconcile that keeps other arms' inbound events on the shared bus.
fn reconcile_fan_in_arm(bus: &mut VecDeque<Arc<DaemonEvent>>, arm: &mut ArmedArm) {
    let mut keep = VecDeque::new();
    for event in bus.drain(..) {
        match &event.payload {
            DaemonEventPayloadInner::Inbound(payload) if payload.channel_id == arm.channel_id => {
                if arm.processed.contains(&payload.post_id) {
                    continue;
                }
                let eligible = match arm.after_eligible.as_ref() {
                    None => true,
                    Some(set) => set.contains(&payload.post_id),
                };
                if !eligible && arm.predicate.matches_inbound(payload) {
                    keep.push_back(event);
                    continue;
                }
                arm.processed.insert(payload.post_id.clone());
            }
            DaemonEventPayloadInner::Inbound(_) => keep.push_back(event),
            _ => {}
        }
    }
    *bus = keep;
}

/// Drop this arm's proven non-members after a successful posts_after; keep
/// every other arm's events.
fn drop_pending_fan_in(bus: &mut VecDeque<Arc<DaemonEvent>>, arm: &ArmedArm) {
    let Some(eligible) = arm.after_eligible.as_ref() else {
        return;
    };
    let mut keep = VecDeque::new();
    for event in bus.drain(..) {
        match &event.payload {
            DaemonEventPayloadInner::Inbound(payload)
                if payload.channel_id == arm.channel_id
                    && arm.predicate.matches_inbound(payload) =>
            {
                if eligible.contains(&payload.post_id) {
                    keep.push_back(event);
                }
            }
            DaemonEventPayloadInner::Inbound(payload) if payload.channel_id == arm.channel_id => {}
            DaemonEventPayloadInner::Inbound(_) => keep.push_back(event),
            _ => {}
        }
    }
    *bus = keep;
}

fn first_live_match(
    bus: &VecDeque<Arc<DaemonEvent>>,
    armed: &[ArmedArm],
) -> Option<(WaitChannelSelector, Message)> {
    for event in bus {
        let DaemonEventPayloadInner::Inbound(payload) = &event.payload else {
            continue;
        };
        for arm in armed {
            if arm.processed.contains(&payload.post_id) {
                continue;
            }
            if payload.channel_id != arm.channel_id {
                continue;
            }
            let eligible = match arm.after_eligible.as_ref() {
                None => true,
                Some(set) => set.contains(&payload.post_id),
            };
            if !eligible {
                continue;
            }
            if arm.predicate.matches_inbound(payload) {
                return Some((arm.selector.clone(), inbound_to_message(payload)));
            }
        }
    }
    None
}

fn fan_in_deadline(armed: &[ArmedArm]) -> Result<WaitChannelsResult, CoreError> {
    let joined = join_selectors(armed);
    if armed.iter().any(|a| !a.clean_observe) {
        return Err(CoreError::WaitProviderDegraded {
            channel: joined,
            message: "deadline reached while provider observation was failing".into(),
        });
    }
    let proven = armed
        .iter()
        .all(|a| a.saw_successful_rest || a.saw_healthy_inbound);
    if !proven {
        return Err(CoreError::WaitProviderDegraded {
            channel: joined,
            message: "wait ended without a healthy push or REST observation".into(),
        });
    }
    Err(CoreError::WaitTimeout(joined))
}

fn join_selectors(armed: &[ArmedArm]) -> String {
    armed
        .iter()
        .map(|a| a.selector.qualified())
        .collect::<Vec<_>>()
        .join(",")
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
        other => CoreError::WaitFilterInvalid(format!(
            "wait arm {q}: {}",
            redact_arm_error(&other.to_string())
        )),
    }
}

fn redact_arm_error(message: &str) -> String {
    const MAX: usize = 240;
    if message.len() <= MAX {
        message.to_string()
    } else {
        format!("{}…", &message[..MAX])
    }
}

pub(crate) async fn with_bus_drain<T, Fut>(
    rx: &mut broadcast::Receiver<Arc<DaemonEvent>>,
    bus: &mut VecDeque<Arc<DaemonEvent>>,
    label: &str,
    fut: Fut,
) -> Result<T, CoreError>
where
    Fut: std::future::Future<Output = Result<T, CoreError>>,
{
    tokio::pin!(fut);
    loop {
        tokio::select! {
            res = &mut fut => return res,
            _ = sleep(Duration::from_millis(5)) => {
                drain_bus(rx, bus, label)?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chanvoy_core::{DaemonEventKind, InboundEventPayload, Provider};

    fn pred(channel_id: &str) -> WaitPredicate {
        WaitPredicate::compile("bot", channel_id, Some("ASSENT"), None).expect("pred")
    }

    fn inbound(channel_id: &str, post_id: &str, body: &str) -> Arc<DaemonEvent> {
        Arc::new(DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: channel_id.into(),
                channel_name: "c".into(),
                post_id: post_id.into(),
                root_id: post_id.into(),
                sender_id: "u".into(),
                sender_username: "u".into(),
                message: body.into(),
                create_at: 1,
                received_at: 1,
                mentioned: false,
            }),
        })
    }

    #[test]
    fn retain_survives_snapshot_and_evals_once_per_arm() {
        let mut seam = FanInRetain::default();
        seam.retain(inbound("ch-a", "p1", "ASSENT now"));
        assert_eq!(seam.len(), 1);
        seam.snapshot_noop();
        assert_eq!(seam.len(), 1);
        let a = pred("ch-a");
        let first = seam.eval_arm("ch-a", &a).expect("match");
        assert_eq!(first.id, "p1");
        assert!(seam.eval_arm("ch-a", &a).is_none(), "exactly once per arm");
        let b = pred("ch-b");
        assert!(
            seam.eval_arm("ch-b", &b).is_none(),
            "wrong-channel retain is not a match"
        );
    }

    #[test]
    fn inject_between_arms_is_retained_for_later_eval() {
        let mut seam = FanInRetain::default();
        // After arm A resolve, before arm B: event for B is retained.
        seam.retain(inbound("ch-b", "p-b", "ASSENT b"));
        let a = pred("ch-a");
        assert!(seam.eval_arm("ch-a", &a).is_none());
        let b = pred("ch-b");
        let hit = seam.eval_arm("ch-b", &b).expect("b match");
        assert_eq!(hit.id, "p-b");
    }

    #[test]
    fn self_posts_never_match_on_any_arm() {
        let p = WaitPredicate::compile("bot", "ch-a", Some("ASSENT"), None).unwrap();
        let ev = Arc::new(DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(InboundEventPayload {
                profile: "t".into(),
                provider: Provider::Mattermost,
                channel_id: "ch-a".into(),
                channel_name: "c".into(),
                post_id: "self".into(),
                root_id: "self".into(),
                sender_id: "bot".into(),
                sender_username: "bot".into(),
                message: "ASSENT".into(),
                create_at: 1,
                received_at: 1,
                mentioned: false,
            }),
        });
        let mut seam = FanInRetain::default();
        seam.retain(ev);
        assert!(seam.eval_arm("ch-a", &p).is_none());
    }

    fn arm(channel: &str, channel_id: &str, after: Option<&str>) -> ArmedArm {
        ArmedArm {
            selector: WaitChannelSelector::new("org", channel),
            channel_id: channel_id.into(),
            predicate: pred(channel_id),
            after: after.map(str::to_string),
            after_eligible: after.map(|_| HashSet::new()),
            scan_cursor: None,
            processed: HashSet::new(),
            saw_successful_rest: true,
            saw_healthy_inbound: false,
            clean_observe: true,
        }
    }

    #[test]
    fn snapshot_does_not_erase_retained_post_sub_event() {
        let mut bus = VecDeque::new();
        bus.push_back(inbound("ch-a", "p-live", "ASSENT now"));
        let retained = retained_post_ids_for_channel(&bus, "ch-a");
        assert!(retained.contains("p-live"));
        let mut processed = HashSet::new();
        merge_snapshot_into_processed(&mut processed, ["older".into(), "p-live".into()], &retained);
        assert!(processed.contains("older"));
        assert!(
            !processed.contains("p-live"),
            "retained post-sub id must survive the tip snapshot"
        );
        let armed = [arm("a", "ch-a", None)];
        let (sel, msg) = first_live_match(&bus, &armed).expect("retained event still matches");
        assert_eq!(sel.channel, "a");
        assert_eq!(msg.id, "p-live");
    }

    #[test]
    fn earlier_arm_reconcile_keeps_later_explicit_after_candidate() {
        let mut a = arm("a", "ch-a", None);
        let mut b = arm("b", "ch-b", Some("anchor-b"));
        let mut bus = VecDeque::new();
        bus.push_back(inbound("ch-b", "p-b", "ASSENT b"));
        reconcile_fan_in_arm(&mut bus, &mut a);
        assert_eq!(bus.len(), 1, "foreign inbound must stay on the shared bus");
        reconcile_fan_in_arm(&mut bus, &mut b);
        assert_eq!(
            bus.len(),
            1,
            "later explicit-after candidate stays pending until REST confirms"
        );
        b.after_eligible
            .as_mut()
            .expect("gated")
            .insert("p-b".into());
        let (sel, msg) = first_live_match(&bus, &[a, b]).expect("later arm can still fire");
        assert_eq!(sel.channel, "b");
        assert_eq!(msg.id, "p-b");
    }

    #[test]
    fn shared_deadline_with_one_degraded_arm_is_not_clean_timeout() {
        let mut a = arm("a", "ch-a", None);
        a.clean_observe = false;
        a.saw_successful_rest = false;
        let mut b = arm("b", "ch-b", None);
        b.clean_observe = true;
        b.saw_successful_rest = true;
        let err = fan_in_deadline(&[a, b]).unwrap_err();
        assert!(
            matches!(err, CoreError::WaitProviderDegraded { .. }),
            "mixed degraded + later deadline must stay hard, got {err:?}"
        );
    }
}
