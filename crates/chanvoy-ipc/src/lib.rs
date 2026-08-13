use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use chanvoy_core::{
    Channel, CoreError, DaemonEvent, DaemonEventKind, DaemonEventPayloadInner, EventBus,
    MattermostClient, Message, Profile, SubscriptionFilter,
};
use ipcprims::frame::Frame;
use ipcprims::peer::{async_connect_with_config, AsyncPeerTx, HandshakeConfig, PeerConfig};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

const CHANNEL_260: u16 = 260;
const CHANNEL_3: u16 = 3;

pub const CHAT_CHANNEL: u16 = CHANNEL_260;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ChatErrorCode {
    NotFound,
    PermissionDenied,
    DelegationExpired,
    ProviderError,
    RateLimited,
    InvalidRequest,
    SendBlocked,
    SubscriptionNotFound,
    UnsupportedOperation,
    ChannelNotJoined,
    /// PER-014: post-bind whoami probe (or any later daemon_status call)
    /// caught the bot identity diverging from the configured bot_username.
    /// Network-backed IPC requests refuse with this code while the drift
    /// bit is set; subscription event forwarding is suppressed; local
    /// socket and `daemon_status` remain queryable so operators can
    /// re-run `chanvoy auto-setup` to re-validate identity. Per
    /// @agent-entarch-lanytehq's PR #16 finding (2026-04-28).
    IdentityDrift,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type")]
pub enum ChatFrame {
    #[serde(rename = "chat_channel_list_request")]
    ChannelListRequest {
        request_id: String,
        delegation_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        team_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        include_archived: Option<bool>,
    },
    #[serde(rename = "chat_channel_list_response")]
    ChannelListResponse {
        request_id: String,
        channels: Vec<ChannelSummary>,
    },
    #[serde(rename = "chat_channel_get_request")]
    ChannelGetRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
    },
    #[serde(rename = "chat_channel_get_response")]
    ChannelGetResponse {
        request_id: String,
        channel: ChannelSummary,
    },
    #[serde(rename = "chat_read_request")]
    ReadRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_root_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u32>,
    },
    #[serde(rename = "chat_read_response")]
    ReadResponse {
        request_id: String,
        channel_id: String,
        posts: Vec<PostSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        has_more: Option<bool>,
    },
    #[serde(rename = "chat_post_request")]
    PostRequest {
        request_id: String,
        delegation_id: String,
        channel_id: String,
        message: String,
        gate_token: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        thread_root_id: Option<String>,
    },
    #[serde(rename = "chat_post_response")]
    PostResponse {
        request_id: String,
        post: PostSummary,
    },
    #[serde(rename = "chat_subscribe_request")]
    SubscribeRequest {
        request_id: String,
        delegation_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        filter: Option<IpcSubscriptionFilter>,
        #[serde(skip_serializing_if = "Option::is_none")]
        resume_after_seq: Option<u64>,
    },
    #[serde(rename = "chat_subscribe_response")]
    SubscribeResponse {
        request_id: String,
        subscription_id: String,
        starting_seq: u64,
    },
    #[serde(rename = "chat_unsubscribe_request")]
    UnsubscribeRequest {
        request_id: String,
        delegation_id: String,
        subscription_id: String,
    },
    #[serde(rename = "chat_unsubscribe_response")]
    UnsubscribeResponse { request_id: String, success: bool },
    #[serde(rename = "chat_event_notification")]
    EventNotification {
        subscription_id: String,
        seq: u64,
        event_kind: String,
        occurred_at: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        channel_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        post: Option<PostSummary>,
        #[serde(skip_serializing_if = "Option::is_none")]
        mentions_bot: Option<bool>,
        #[serde(skip_serializing_if = "Option::is_none")]
        actor: Option<String>,
    },
    #[serde(rename = "chat_subscription_gap")]
    SubscriptionGap {
        subscription_id: String,
        expected_seq: u64,
        next_seq: u64,
        reason: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        message: Option<String>,
    },
    #[serde(rename = "chat_error")]
    Error {
        request_id: String,
        error_code: ChatErrorCode,
        message: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        retryable: Option<bool>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ChannelSummary {
    pub channel_id: String,
    pub name: String,
    pub kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub team_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct PostSummary {
    pub post_id: String,
    pub channel_id: String,
    pub author: String,
    pub created_at: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thread_root_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IpcSubscriptionFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_ids: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_kinds: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mentions_only: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AuditEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub entry_id: String,
    pub peer_id: String,
    pub timestamp: String,
    pub action: String,
    pub actor: String,
    pub prev_hash: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub severity: Option<String>,
}

/// Rebuild a thread failure so that nothing of the provider's survives.
///
/// A thread read anchors on whatever post the caller named, then runs
/// against that post's thread root. When a reply was named, the root is
/// derived — so anything quoting it would hand the caller an identifier
/// they never supplied and could not otherwise obtain.
///
/// The identifier is not the only thing that changes. Modelled failures
/// keep their kind and are restated against the caller's post. Everything
/// else is deliberately *converted* to a constructed gateway-status
/// failure, because a provider's own error body, or a transport error
/// carrying the URL it was fetching, both quote that derived root. The
/// status is preserved where there is one, since that is what tells a
/// caller whether to retry; the provider's prose never is.
///
/// Nothing returned from here was received from anywhere: every arm
/// builds its value, including the fallthrough. Forwarding the original
/// is what leaked twice.
fn restate_against_requested_post(error: CoreError, requested_post_id: &str) -> CoreError {
    match error {
        CoreError::AnchorChannelMismatch { channel, .. } => CoreError::AnchorChannelMismatch {
            post_id: requested_post_id.to_string(),
            channel,
        },
        CoreError::AnchorNotFound(_) => CoreError::AnchorNotFound(requested_post_id.to_string()),
        CoreError::EmptyThread { .. } => CoreError::EmptyThread {
            root_id: requested_post_id.to_string(),
        },
        // Provider answered, but its body is its own text and may quote
        // the derived root. Keep the status — that is what decides
        // whether a caller should retry — and drop the body.
        CoreError::Api { status, .. } => CoreError::Api {
            status,
            message: format!("could not read the thread for post {requested_post_id}"),
        },
        // Everything else is discarded rather than forwarded.
        //
        // This arm exists because the previous version forwarded the
        // original error, and a transport failure renders the URL it was
        // fetching — which after the derivation is the derived root.
        // Enumerating the leaky variants is what produced that bug: the
        // surface ends up defined by the cases not thought of. Nothing
        // reaches a caller from here that was not built here.
        _ => CoreError::Api {
            status: reqwest::StatusCode::BAD_GATEWAY,
            message: format!("could not read the thread for post {requested_post_id}"),
        },
    }
}

pub fn core_error_to_chat(error: CoreError, request_id: &str) -> ChatFrame {
    let (code, retryable) = match &error {
        CoreError::WaitTimeout(_) => (ChatErrorCode::NotFound, Some(false)),
        CoreError::WaitFilterInvalid(_) => (ChatErrorCode::InvalidRequest, Some(false)),
        CoreError::WaitProviderDegraded { .. } => (ChatErrorCode::ProviderError, Some(true)),
        CoreError::WaitAlreadyActive { .. }
        | CoreError::WaitConflictChanged { .. }
        | CoreError::WaitReplaced { .. }
        | CoreError::WaitReplaceUnconfirmed { .. } => (ChatErrorCode::InvalidRequest, Some(false)),
        CoreError::ProfileNotFound(_) => (ChatErrorCode::NotFound, Some(false)),
        // Every cause of an empty thread body is permanent: the post was
        // deleted, it sits in a channel this identity cannot read, or the
        // id is not a post id. Falling through to the retryable default
        // would tell an automated caller to retry a read that can never
        // succeed — and this surface is consumed by agents, which honor
        // that flag literally.
        CoreError::EmptyThread { .. } => (ChatErrorCode::NotFound, Some(false)),
        // A post that does not exist, and a post that exists in some
        // other channel, are both permanent answers to the question
        // that was asked. Retrying changes neither.
        //
        // Both report not-found rather than distinguishing the two:
        // "this post is not in this channel" is the honest answer to
        // the caller's question, and it does not confirm the existence
        // of a post the caller named a channel it may not be able to
        // read.
        CoreError::AnchorNotFound(_) | CoreError::AnchorChannelMismatch { .. } => {
            (ChatErrorCode::NotFound, Some(false))
        }
        // (message is unified for these two below — see `message`)
        CoreError::RequiresElevatedCapability => (ChatErrorCode::PermissionDenied, Some(false)),
        CoreError::Api { status, .. } => match *status {
            s if s == reqwest::StatusCode::UNAUTHORIZED || s == reqwest::StatusCode::FORBIDDEN => {
                (ChatErrorCode::PermissionDenied, Some(false))
            }
            s if s == reqwest::StatusCode::NOT_FOUND => (ChatErrorCode::NotFound, Some(false)),
            s if s == reqwest::StatusCode::TOO_MANY_REQUESTS => {
                (ChatErrorCode::RateLimited, Some(true))
            }
            _ => (ChatErrorCode::ProviderError, Some(true)),
        },
        _ => (ChatErrorCode::ProviderError, Some(true)),
    };
    // A binding refusal reports the same text whether the post does not
    // exist or exists in another channel. The two underlying messages
    // differ, and a caller that could tell them apart would have an
    // existence oracle over channels it cannot read: ask about a post
    // id, and the wording says whether it is real. This surface is
    // reached by peers, so the answer to "is that post in this channel"
    // is a flat no either way.
    //
    // The operator-facing path deliberately does the opposite and names
    // the owning channel — an operator running the CLI already holds the
    // access that would make the distinction a leak.
    let message = match &error {
        CoreError::AnchorNotFound(post_id) | CoreError::AnchorChannelMismatch { post_id, .. } => {
            binding_refusal_message(post_id)
        }
        other => other.to_string(),
    };
    ChatFrame::Error {
        request_id: request_id.to_string(),
        error_code: code,
        message,
        retryable,
    }
}

/// The single wording used for every binding refusal on the peer
/// surface, so that "does not exist" and "exists elsewhere" cannot be
/// told apart.
///
/// The post id is included because the caller supplied it — echoing it
/// back reveals nothing and makes the refusal diagnosable. What is
/// withheld is the distinction between the two causes.
pub fn binding_refusal_message(post_id: &str) -> String {
    format!("post {post_id} not found in the requested channel")
}

pub fn translate_m2_filter_to_260(filter: &SubscriptionFilter) -> IpcSubscriptionFilter {
    match filter {
        SubscriptionFilter::AllMonitored => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string(), "mention".to_string()]),
            mentions_only: None,
        },
        SubscriptionFilter::ChannelByName(name) => IpcSubscriptionFilter {
            channel_ids: Some(vec![name.clone()]),
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        },
        SubscriptionFilter::MentionsOnly => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["mention".to_string()]),
            mentions_only: Some(true),
        },
        SubscriptionFilter::ConnectionState => IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["channel_updated".to_string()]),
            mentions_only: None,
        },
    }
}

pub fn translate_260_filter_to_m2(filter: &IpcSubscriptionFilter) -> SubscriptionFilter {
    if filter.mentions_only == Some(true) {
        return SubscriptionFilter::MentionsOnly;
    }
    if let Some(ids) = &filter.channel_ids {
        if ids.len() == 1 {
            return SubscriptionFilter::ChannelByName(ids[0].clone());
        }
    }
    SubscriptionFilter::AllMonitored
}

pub fn daemon_event_to_chat_notification(
    event: &DaemonEvent,
    subscription_id: &str,
) -> Option<ChatFrame> {
    match &event.payload {
        DaemonEventPayloadInner::Inbound(p) => Some(ChatFrame::EventNotification {
            subscription_id: subscription_id.to_string(),
            seq: event.seq,
            event_kind: match event.kind {
                DaemonEventKind::InboundMessage => "message_posted".to_string(),
                DaemonEventKind::InboundMention => "mention".to_string(),
                _ => return None,
            },
            occurred_at: format_rfc3339(p.create_at),
            channel_id: Some(p.channel_id.clone()),
            post: Some(PostSummary {
                post_id: p.post_id.clone(),
                channel_id: p.channel_id.clone(),
                author: p.sender_username.clone(),
                created_at: format_rfc3339(p.create_at),
                message: p.message.clone(),
                // Same non-empty rule as the read path. Push events are
                // how a subscribed caller learns a message exists, so
                // dropping the thread root here would leave it with no
                // way to reply without a second round trip — and no way
                // at all to reply correctly to a reply.
                thread_root_id: non_empty_root(&p.root_id),
            }),
            mentions_bot: Some(p.mentioned),
            actor: Some(p.sender_id.clone()),
        }),
        DaemonEventPayloadInner::ConnectionStateChanged(_) => Some(ChatFrame::EventNotification {
            subscription_id: subscription_id.to_string(),
            seq: event.seq,
            event_kind: "channel_updated".to_string(),
            occurred_at: format_rfc3339(chanvoy_core::now_unix_millis()),
            channel_id: None,
            post: None,
            mentions_bot: None,
            actor: None,
        }),
        DaemonEventPayloadInner::Gap(_) => None,
    }
}

fn event_matches_ipc_filter(event: &DaemonEvent, filter: &IpcSubscriptionFilter) -> bool {
    if let DaemonEventPayloadInner::Gap(_) = &event.payload {
        return true;
    }
    if let Some(kinds) = &filter.event_kinds {
        let kind_str = match event.kind {
            DaemonEventKind::InboundMessage => "message_posted",
            DaemonEventKind::InboundMention => "mention",
            DaemonEventKind::ConnectionStateChanged => "channel_updated",
            _ => return false,
        };
        if !kinds.iter().any(|k| k == kind_str) {
            return false;
        }
    }
    if filter.mentions_only == Some(true) {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if !p.mentioned {
                return false;
            }
        }
    }
    if let Some(channel_ids) = &filter.channel_ids {
        if let DaemonEventPayloadInner::Inbound(p) = &event.payload {
            if !channel_ids.iter().any(|c| c == &p.channel_id) {
                return false;
            }
        }
    }
    true
}

fn format_rfc3339(millis: i64) -> String {
    let secs = millis / 1000;
    let subsec_millis = (millis % 1000) as u32;
    chrono::DateTime::from_timestamp(secs, subsec_millis * 1_000_000)
        .map(|dt| dt.to_rfc3339())
        .unwrap_or_else(|| "1970-01-01T00:00:00.000Z".to_string())
}

fn channel_to_summary(ch: &Channel) -> ChannelSummary {
    ChannelSummary {
        channel_id: ch.id.clone(),
        name: ch.name.clone(),
        kind: "public".to_string(),
        display_name: Some(ch.display_name.clone()),
        team_id: None,
    }
}

fn message_to_post_summary(msg: &Message, channel_id: &str) -> PostSummary {
    PostSummary {
        post_id: msg.id.clone(),
        channel_id: channel_id.to_string(),
        author: msg.username.clone(),
        created_at: format_rfc3339(msg.create_at),
        message: msg.message.clone(),
        thread_root_id: thread_root_of(msg),
    }
}

/// The thread a message belongs to, or `None` when we genuinely do not
/// know. A message read from a current chanvoy always names its thread
/// (a top-level post names itself); an empty value only happens on a
/// message that came from an older daemon, and reporting that as
/// `None` is more honest than inventing a root.
fn thread_root_of(msg: &Message) -> Option<String> {
    non_empty_root(&msg.root_id)
}

/// A thread root is absent only when it is genuinely empty, which now
/// means the value came from an older daemon that did not report one.
/// Shared by the read and push paths so the two cannot disagree about
/// what "no thread root" means.
fn non_empty_root(root_id: &str) -> Option<String> {
    if root_id.is_empty() {
        None
    } else {
        Some(root_id.to_string())
    }
}

struct SubscriptionEntry {
    filter: IpcSubscriptionFilter,
}

#[derive(Debug, Clone)]
pub struct IpcPeerState {
    pub connected: bool,
    pub peer_id: Option<String>,
    pub reconnect_count: u64,
}

pub struct IpcPeer {
    client: MattermostClient,
    event_bus: Arc<EventBus>,
    #[allow(dead_code)]
    profile: Profile,
    gateway_socket: String,
    state: Arc<tokio::sync::Mutex<IpcPeerState>>,
    subscriptions: Arc<tokio::sync::Mutex<HashMap<String, SubscriptionEntry>>>,
    /// PER-014: shared drift signal from the daemon's `AppState`. When
    /// set, network-backed IPC requests refuse with `IdentityDrift` and
    /// event forwarding to subscribers is suppressed. Local control /
    /// audit / subscription-management responses still flow. Per
    /// @agent-entarch-lanytehq's PR #16 finding (2026-04-28).
    identity_drift: Arc<AtomicBool>,
}

impl IpcPeer {
    pub fn new(
        profile: &Profile,
        client: MattermostClient,
        event_bus: Arc<EventBus>,
        gateway_socket: String,
        identity_drift: Arc<AtomicBool>,
    ) -> Self {
        Self {
            client,
            event_bus,
            profile: profile.clone(),
            gateway_socket,
            state: Arc::new(tokio::sync::Mutex::new(IpcPeerState {
                connected: false,
                peer_id: None,
                reconnect_count: 0,
            })),
            subscriptions: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            identity_drift,
        }
    }

    /// PER-014: build a drift-refused error frame for a given request_id.
    /// Used by every network-backed IPC handler at the top of its body
    /// to short-circuit when the drift bit is set.
    fn drift_refusal(request_id: String) -> ChatFrame {
        ChatFrame::Error {
            request_id,
            error_code: ChatErrorCode::IdentityDrift,
            message: "identity drift detected: configured bot_username does not match the \
                Mattermost-returned username for this token; network-backed IPC requests \
                are refused. Inspect daemon_status.mattermost_identity_drift and re-run \
                `chanvoy auto-setup` to re-validate identity."
                .to_string(),
            retryable: Some(true),
        }
    }

    pub fn state(&self) -> Arc<tokio::sync::Mutex<IpcPeerState>> {
        Arc::clone(&self.state)
    }

    pub async fn run(self: Arc<Self>, cancel: CancellationToken) {
        let mut attempt: u64 = 0;
        loop {
            if cancel.is_cancelled() {
                break;
            }
            attempt += 1;

            match self.connect_and_serve(cancel.clone()).await {
                Ok(()) => {
                    info!("ipc peer session ended cleanly");
                }
                Err(e) => {
                    warn!(%e, "ipc peer session error");
                }
            }

            {
                let mut s = self.state.lock().await;
                s.connected = false;
                s.peer_id = None;
                s.reconnect_count += 1;
            }

            if cancel.is_cancelled() {
                break;
            }

            let delay = if attempt <= 3 {
                std::time::Duration::from_secs(1 << attempt.min(3))
            } else {
                std::time::Duration::from_secs(30)
            };

            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }
    }

    async fn connect_and_serve(
        &self,
        cancel: CancellationToken,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let handshake_config = HandshakeConfig {
            auth_token: None,
            ..Default::default()
        };
        let peer_config = PeerConfig {
            enable_any_delivery: false,
            ..Default::default()
        };

        let peer = async_connect_with_config(
            &self.gateway_socket,
            &[CHANNEL_260, CHANNEL_3],
            &handshake_config,
            None,
            Some(peer_config),
            None,
        )
        .await?;

        let peer_id = peer.id().to_string();
        info!(%peer_id, "ipc peer connected to gateway");

        {
            let mut s = self.state.lock().await;
            s.connected = true;
            s.peer_id = Some(peer_id.clone());
        }

        let (tx, mut rx) = peer.into_split();
        let mut channel_rx = rx.take_channel_receiver(CHANNEL_260);

        let event_bus = Arc::clone(&self.event_bus);
        let mut event_rx = event_bus.subscribe();
        let tx_clone = tx.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        let cancel_fwd = cancel.clone();
        let audit_peer_id = peer_id.clone();
        let drift_for_fwd = Arc::clone(&self.identity_drift);

        let forward_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    recv_result = event_rx.recv() => {
                        match recv_result {
                            Ok(event) => {
                                // PER-014: drop Mattermost-sourced events
                                // when identity drift is set. Same contract
                                // as the local UDS subscription path —
                                // operators query daemon_status to learn
                                // why events stopped, then re-run
                                // `chanvoy auto-setup`. Per
                                // @agent-entarch-lanytehq's PR #16
                                // finding (2026-04-28).
                                if drift_for_fwd.load(Ordering::Relaxed) {
                                    continue;
                                }
                                let subs = subscriptions.lock().await;
                                for (sub_id, entry) in subs.iter() {
                                    if event_matches_ipc_filter(&event, &entry.filter) {
                                        if let DaemonEventPayloadInner::Gap(g) = &event.payload {
                                            let gap = ChatFrame::SubscriptionGap {
                                                subscription_id: sub_id.clone(),
                                                expected_seq: g.missed_from_seq,
                                                next_seq: g.missed_to_seq,
                                                reason: "overflow".to_string(),
                                                message: Some(format!(
                                                    "missed {} events",
                                                    g.missed_to_seq.saturating_sub(g.missed_from_seq)
                                                )),
                                            };
                                            let payload = serde_json::to_vec(&gap).unwrap_or_default();
                                            if tx_clone.send(CHANNEL_260, &payload).await.is_err() {
                                                return;
                                            }
                                        } else if let Some(notification) = daemon_event_to_chat_notification(&event, sub_id) {
                                            let payload = serde_json::to_vec(&notification).unwrap_or_default();
                                            if tx_clone.send(CHANNEL_260, &payload).await.is_err() {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Err(_) => return,
                        }
                    }
                    _ = cancel_fwd.cancelled() => return,
                }
            }
        });

        emit_audit(&tx, &audit_peer_id, "chat.peer_connected", None, "info").await;

        loop {
            tokio::select! {
                frame_result = async {
                    match channel_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    match frame_result {
                        Ok(frame) => {
                            self.handle_frame(frame, &tx, &audit_peer_id).await;
                        }
                        Err(_) => {
                            break;
                        }
                    }
                }
                _ = cancel.cancelled() => {
                    break;
                }
            }
        }

        emit_audit(&tx, &audit_peer_id, "chat.peer_disconnected", None, "info").await;

        forward_handle.abort();
        let _ = tx.shutdown().await;
        Ok(())
    }

    async fn handle_frame(&self, frame: Frame, tx: &AsyncPeerTx, peer_id: &str) {
        let Ok(chat_frame) = serde_json::from_slice::<ChatFrame>(&frame.payload) else {
            return;
        };

        match chat_frame {
            ChatFrame::ChannelListRequest {
                request_id,
                delegation_id,
                team_id: _,
                include_archived,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let result = self.client.list_channels().await;
                let response = match result {
                    Ok(channels) => {
                        let channels: Vec<ChannelSummary> = channels
                            .iter()
                            .filter(|c| {
                                include_archived.unwrap_or(false) || !c.name.starts_with("__")
                            })
                            .map(channel_to_summary)
                            .collect();
                        ChatFrame::ChannelListResponse {
                            request_id,
                            channels,
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::ReadRequest {
                request_id,
                delegation_id,
                channel_id,
                thread_root_id,
                limit,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let response = if let Some(root_id) = &thread_root_id {
                    self.thread_read_response(request_id, channel_id, root_id, limit)
                        .await
                } else {
                    self.channel_read_response(request_id, channel_id, limit)
                        .await
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::PostRequest {
                request_id,
                delegation_id,
                channel_id,
                message,
                gate_token,
                thread_root_id,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                if gate_token.is_empty() {
                    let err = ChatFrame::Error {
                        request_id,
                        error_code: ChatErrorCode::PermissionDenied,
                        message: "gate_token is required for post operations".to_string(),
                        retryable: Some(false),
                    };
                    let _ = self.send_response(tx, &err).await;
                    return;
                }
                let result = if let Some(root_id) = &thread_root_id {
                    self.client
                        .post_threaded_reply(&channel_id, root_id, &message)
                        .await
                } else {
                    self.client.post_message_by_id(&channel_id, &message).await
                };
                let response = match result {
                    Ok(receipt) => {
                        emit_audit(
                            tx,
                            peer_id,
                            if thread_root_id.is_some() {
                                "chat.thread_reply"
                            } else {
                                "chat.post"
                            },
                            Some(serde_json::json!({
                                "channel_id": &channel_id,
                                "post_id": &receipt.id,
                                "has_thread_root": thread_root_id.is_some(),
                            })),
                            "notice",
                        )
                        .await;
                        ChatFrame::PostResponse {
                            request_id,
                            post: PostSummary {
                                post_id: receipt.id,
                                channel_id: channel_id.clone(),
                                author: String::new(),
                                created_at: format_rfc3339(chanvoy_core::now_unix_millis()),
                                message,
                                thread_root_id,
                            },
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::ChannelGetRequest {
                request_id,
                delegation_id,
                channel_id,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let result = self.client.list_channels().await;
                let response = match result {
                    Ok(channels) => {
                        let found = channels.iter().find(|c| c.id == channel_id);
                        match found {
                            Some(ch) => ChatFrame::ChannelGetResponse {
                                request_id,
                                channel: channel_to_summary(ch),
                            },
                            None => ChatFrame::Error {
                                request_id,
                                error_code: ChatErrorCode::NotFound,
                                message: format!("channel {channel_id} not found"),
                                retryable: Some(false),
                            },
                        }
                    }
                    Err(e) => core_error_to_chat(e, &request_id),
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::SubscribeRequest {
                request_id,
                delegation_id,
                filter,
                resume_after_seq,
            } => {
                if self.identity_drift.load(Ordering::Relaxed) {
                    let _ = self
                        .send_response(tx, &Self::drift_refusal(request_id))
                        .await;
                    return;
                }
                let _ = &delegation_id;
                let sub_id = uuid::Uuid::new_v4().to_string();
                let start_seq = self.event_bus.current_seq();
                let ipc_filter = filter.unwrap_or(IpcSubscriptionFilter {
                    channel_ids: None,
                    event_kinds: None,
                    mentions_only: None,
                });

                if let Some(resume_seq) = resume_after_seq {
                    if resume_seq < start_seq {
                        let gap = ChatFrame::SubscriptionGap {
                            subscription_id: sub_id.clone(),
                            expected_seq: resume_seq,
                            next_seq: start_seq,
                            reason: "history_unavailable".to_string(),
                            message: Some(format!(
                                "missed {} events between seq {} and {}",
                                start_seq.saturating_sub(resume_seq),
                                resume_seq,
                                start_seq
                            )),
                        };
                        let _ = self.send_response(tx, &gap).await;
                    }
                }

                {
                    let mut subs = self.subscriptions.lock().await;
                    subs.insert(sub_id.clone(), SubscriptionEntry { filter: ipc_filter });
                }

                emit_audit(
                    tx,
                    peer_id,
                    "chat.subscribe",
                    Some(serde_json::json!({ "subscription_id": &sub_id })),
                    "info",
                )
                .await;

                let response = ChatFrame::SubscribeResponse {
                    request_id,
                    subscription_id: sub_id,
                    starting_seq: start_seq,
                };
                let _ = self.send_response(tx, &response).await;
            }
            ChatFrame::UnsubscribeRequest {
                request_id,
                delegation_id,
                subscription_id,
            } => {
                let _ = &delegation_id;
                let removed = {
                    let mut subs = self.subscriptions.lock().await;
                    subs.remove(&subscription_id).is_some()
                };
                if removed {
                    emit_audit(
                        tx,
                        peer_id,
                        "chat.unsubscribe",
                        Some(serde_json::json!({ "subscription_id": &subscription_id })),
                        "info",
                    )
                    .await;
                }
                let response = ChatFrame::UnsubscribeResponse {
                    request_id,
                    success: removed,
                };
                let _ = self.send_response(tx, &response).await;
            }
            _ => {}
        }
    }

    /// The ordinary (non-thread) branch of a read request: a time
    /// window over one channel.
    ///
    /// Separate from `handle_frame` for the same reason
    /// `thread_read_response` is — the completeness contract below has
    /// to be exercisable without standing up a peer transport.
    async fn channel_read_response(
        &self,
        request_id: String,
        channel_id: String,
        limit: Option<u32>,
    ) -> ChatFrame {
        let since = chanvoy_core::now_unix_millis() - (30 * 60 * 1000);
        let result = self
            .client
            .read_channel_by_id_since_millis(&channel_id, since)
            .await;
        match result {
            Ok(messages) => {
                let limit = limit.unwrap_or(50) as usize;
                let total = messages.len();
                // Completeness here is three-valued, because the
                // provider has already had its say.
                //
                // A window read asks for one page. If it came back
                // full, posts may exist beyond it that this response
                // never saw — so `false` would be a claim we cannot
                // support, and the honest answer is that we do not
                // know. Reporting `Some(false)` off `total > limit`
                // alone was wrong for exactly this reason: with a limit
                // above the page size, a truncated window reported
                // itself complete.
                let has_more = if total > limit {
                    Some(true)
                } else if total >= chanvoy_core::CHANNEL_WINDOW_PAGE_SIZE {
                    None
                } else {
                    Some(false)
                };
                let posts: Vec<PostSummary> = messages
                    .into_iter()
                    .take(limit)
                    .map(|m| message_to_post_summary(&m, &channel_id))
                    .collect();
                ChatFrame::ReadResponse {
                    request_id,
                    channel_id,
                    posts,
                    has_more,
                }
            }
            Err(e) => core_error_to_chat(e, &request_id),
        }
    }

    /// The thread branch of a read request.
    ///
    /// The anchor point-fetch is what binds the thread to the channel
    /// the caller named. Without it the channel id on the request is
    /// decoration — the thread would be read on the strength of the
    /// post id alone, and every summary would then be stamped with
    /// whatever channel the caller claimed, which is a read of any
    /// thread the bot can see. A refusal here issues no thread request
    /// at all.
    ///
    /// Separate from `handle_frame` so that contract can be exercised
    /// without standing up a peer transport.
    async fn thread_read_response(
        &self,
        request_id: String,
        channel_id: String,
        root_id: &str,
        limit: Option<u32>,
    ) -> ChatFrame {
        // This peer speaks in channel ids, so the id doubles as the
        // operator-facing channel name in a refusal.
        let anchor = match self
            .client
            .get_post_in_channel(&channel_id, &channel_id, root_id)
            .await
        {
            Ok(anchor) => anchor,
            Err(e) => return core_error_to_chat(e, &request_id),
        };
        // The anchor's root is canonical, so naming any reply in the
        // thread reads the whole thread.
        // This peer speaks in channel ids on both sides of the call, so
        // the id doubles as the operator-facing name here too.
        let messages = match self
            .client
            .read_thread_in_channel(&channel_id, &channel_id, &anchor.root_id)
            .await
        {
            Ok(messages) => messages,
            // Re-stated against the id the caller sent. The root was
            // derived from it, so a caller that named a reply never
            // supplied the root and must not be handed it back.
            Err(e) => {
                return core_error_to_chat(restate_against_requested_post(e, root_id), &request_id)
            }
        };
        let limit = limit.unwrap_or(50) as usize;
        // Whether the response is the whole thread has to be answered
        // before the list is consumed. A truncated result that reports
        // nothing about the truncation is indistinguishable from a
        // complete one, and a caller reading a thread to decide what to
        // reply to would be reasoning about a conversation it has only
        // seen the front of.
        let total = messages.len();
        let posts: Vec<PostSummary> = messages
            .into_iter()
            .take(limit)
            .map(|m| PostSummary {
                post_id: m.id.clone(),
                channel_id: channel_id.clone(),
                author: m.username.clone(),
                created_at: format_rfc3339(m.create_at),
                message: m.message.clone(),
                thread_root_id: thread_root_of(&m),
            })
            .collect();
        ChatFrame::ReadResponse {
            request_id,
            channel_id,
            posts,
            has_more: Some(total > limit),
        }
    }

    async fn send_response(
        &self,
        tx: &AsyncPeerTx,
        frame: &ChatFrame,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let payload = serde_json::to_vec(frame)?;
        tx.send(CHANNEL_260, &payload).await?;
        Ok(())
    }
}

async fn emit_audit(
    tx: &AsyncPeerTx,
    peer_id: &str,
    action: &str,
    details: Option<serde_json::Value>,
    severity: &str,
) {
    let event = AuditEvent {
        event_type: "audit_event".to_string(),
        entry_id: uuid::Uuid::new_v4().to_string(),
        peer_id: peer_id.to_string(),
        timestamp: format_rfc3339(chanvoy_core::now_unix_millis()),
        action: action.to_string(),
        actor: "chanvoy".to_string(),
        prev_hash: "0".repeat(64),
        details,
        severity: Some(severity.to_string()),
    };
    if let Ok(payload) = serde_json::to_vec(&event) {
        let _ = tx.send(CHANNEL_3, &payload).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn core_error_maps_permission_denied() {
        let err = CoreError::RequiresElevatedCapability;
        let frame = core_error_to_chat(err, "test-1");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::PermissionDenied);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_not_found() {
        let err = CoreError::WaitTimeout("ch".to_string());
        let frame = core_error_to_chat(err, "test-2");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
                assert_eq!(retryable, Some(false));
            }
            _ => panic!("expected error frame"),
        }
    }

    /// A channel-binding refusal is permanent, and must not invite a
    /// retry. It is also reported as not-found rather than as a
    /// distinct mismatch, so a refusal does not confirm that the named
    /// post exists somewhere the caller cannot see.
    #[test]
    fn a_binding_refusal_is_terminal_and_indistinguishable_from_not_found() {
        let mut seen: Vec<String> = Vec::new();
        for err in [
            CoreError::AnchorNotFound("p-1".to_string()),
            CoreError::AnchorChannelMismatch {
                post_id: "p-1".to_string(),
                channel: "somewhere-else".to_string(),
            },
        ] {
            // The two underlying errors genuinely differ in wording; the
            // point of the test is that the peer surface does not pass
            // that difference on.
            let raw = err.to_string();
            let frame = core_error_to_chat(err, "test-bind");
            match frame {
                ChatFrame::Error {
                    error_code,
                    retryable,
                    message,
                    ..
                } => {
                    assert_eq!(error_code, ChatErrorCode::NotFound);
                    assert_eq!(
                        retryable,
                        Some(false),
                        "a post cannot move into the requested channel on a retry"
                    );
                    assert_ne!(
                        message, raw,
                        "the underlying wording must not reach the peer verbatim"
                    );
                    assert!(
                        !message.contains("somewhere-else"),
                        "a refusal must not name the channel the post really lives in"
                    );
                    seen.push(message);
                }
                _ => panic!("expected error frame"),
            }
        }
        assert_eq!(
            seen[0], seen[1],
            "a missing post and a post in another channel must be reported \
             identically, or the difference is an existence oracle"
        );
    }

    /// An empty thread body is permanent, so it must not be advertised
    /// as retryable. Automated callers honor that flag literally, and a
    /// read that can never succeed would be retried forever.
    #[test]
    fn an_empty_thread_is_a_terminal_not_found_not_a_retryable_provider_error() {
        let err = CoreError::EmptyThread {
            root_id: "root-1".to_string(),
        };
        let frame = core_error_to_chat(err, "test-empty-thread");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
                assert_eq!(
                    retryable,
                    Some(false),
                    "retrying an empty thread cannot ever change the answer"
                );
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_api_404() {
        let err = CoreError::Api {
            status: reqwest::StatusCode::NOT_FOUND,
            message: "not found".to_string(),
        };
        let frame = core_error_to_chat(err, "test-3");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::NotFound);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_api_429() {
        let err = CoreError::Api {
            status: reqwest::StatusCode::TOO_MANY_REQUESTS,
            message: "rate limited".to_string(),
        };
        let frame = core_error_to_chat(err, "test-4");
        match frame {
            ChatFrame::Error {
                error_code,
                retryable,
                ..
            } => {
                assert_eq!(error_code, ChatErrorCode::RateLimited);
                assert_eq!(retryable, Some(true));
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn core_error_maps_generic_as_provider_error() {
        let err = CoreError::Io(std::io::Error::new(std::io::ErrorKind::BrokenPipe, "pipe"));
        let frame = core_error_to_chat(err, "test-5");
        match frame {
            ChatFrame::Error { error_code, .. } => {
                assert_eq!(error_code, ChatErrorCode::ProviderError);
            }
            _ => panic!("expected error frame"),
        }
    }

    #[test]
    fn translate_m2_all_monitored() {
        let f = translate_m2_filter_to_260(&SubscriptionFilter::AllMonitored);
        assert!(f.event_kinds.is_some());
        assert!(f.mentions_only.is_none());
    }

    #[test]
    fn translate_m2_channel_by_name() {
        let f =
            translate_m2_filter_to_260(&SubscriptionFilter::ChannelByName("per-005".to_string()));
        assert_eq!(f.channel_ids, Some(vec!["per-005".to_string()]));
    }

    #[test]
    fn translate_m2_mentions_only() {
        let f = translate_m2_filter_to_260(&SubscriptionFilter::MentionsOnly);
        assert_eq!(f.mentions_only, Some(true));
    }

    #[test]
    fn translate_260_mentions_only_roundtrip() {
        let f = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: None,
            mentions_only: Some(true),
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::MentionsOnly);
    }

    #[test]
    fn translate_260_channel_by_name_roundtrip() {
        let f = IpcSubscriptionFilter {
            channel_ids: Some(vec!["per-005".to_string()]),
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::ChannelByName("per-005".to_string()));
    }

    #[test]
    fn translate_260_default_is_all_monitored() {
        let f = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        let m2 = translate_260_filter_to_m2(&f);
        assert_eq!(m2, SubscriptionFilter::AllMonitored);
    }

    #[test]
    fn chat_frame_serialization_roundtrip() {
        let frame = ChatFrame::ChannelListRequest {
            request_id: "abc-123".to_string(),
            delegation_id: "del-1".to_string(),
            team_id: None,
            include_archived: None,
        };
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("\"chat_channel_list_request\""));
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(frame, parsed);
    }

    /// A pushed reply must arrive carrying the thread it belongs to.
    ///
    /// The regression this guards is narrow and easy to reintroduce:
    /// the root is normalized upstream and carried correctly on the
    /// read path, then dropped here, at the one boundary a subscribed
    /// caller actually receives live events on. A caller that then
    /// replies using the post id is rejected by the provider, because
    /// a reply cannot be the target of another reply — so the failure
    /// surfaces at write time, far from this function.
    #[test]
    fn a_pushed_reply_carries_its_thread_root() {
        let event = DaemonEvent {
            seq: 7,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "reply-9".to_string(),
                root_id: "root-1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "a reply".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let frame = daemon_event_to_chat_notification(&event, "sub-1").expect("notification frame");
        let ChatFrame::EventNotification { post, .. } = frame else {
            panic!("expected an event notification");
        };
        let post = post.expect("notification carries a post summary");
        assert_eq!(
            post.thread_root_id,
            Some("root-1".to_string()),
            "a pushed reply must name the thread root, not its own id and not nothing"
        );
        assert_ne!(
            post.thread_root_id,
            Some(post.post_id.clone()),
            "a reply must not be reported as its own thread root"
        );
    }

    /// A pushed top-level post is the root of its own thread, so it
    /// still names a usable reply target rather than nothing.
    #[test]
    fn a_pushed_top_level_post_is_its_own_thread_root() {
        let event = DaemonEvent {
            seq: 8,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "post-1".to_string(),
                root_id: "post-1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "top level".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let frame = daemon_event_to_chat_notification(&event, "sub-1").expect("notification frame");
        let ChatFrame::EventNotification { post, .. } = frame else {
            panic!("expected an event notification");
        };
        assert_eq!(
            post.expect("post summary").thread_root_id,
            Some("post-1".to_string())
        );
    }

    #[test]
    fn ipc_filter_matches_all_event_kinds() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn ipc_filter_rejects_wrong_kind() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["mention".to_string()]),
            mentions_only: None,
        };
        assert!(!event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn ipc_filter_channel_id_scoping() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let matching = IpcSubscriptionFilter {
            channel_ids: Some(vec!["ch1".to_string()]),
            event_kinds: None,
            mentions_only: None,
        };
        let non_matching = IpcSubscriptionFilter {
            channel_ids: Some(vec!["ch2".to_string()]),
            event_kinds: None,
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &matching));
        assert!(!event_matches_ipc_filter(&event, &non_matching));
    }

    #[test]
    fn ipc_filter_mentions_only_rejects_non_mention() {
        let event = DaemonEvent {
            seq: 1,
            kind: DaemonEventKind::InboundMessage,
            payload: DaemonEventPayloadInner::Inbound(chanvoy_core::InboundEventPayload {
                profile: "test".to_string(),
                provider: chanvoy_core::Provider::Mattermost,
                channel_id: "ch1".to_string(),
                channel_name: "general".to_string(),
                post_id: "p1".to_string(),
                root_id: "p1".to_string(),
                sender_id: "u1".to_string(),
                sender_username: "alice".to_string(),
                message: "hi".to_string(),
                create_at: 1000,
                received_at: 1001,
                mentioned: false,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: None,
            mentions_only: Some(true),
        };
        assert!(!event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn audit_event_serialization() {
        let event = AuditEvent {
            event_type: "audit_event".to_string(),
            entry_id: uuid::Uuid::new_v4().to_string(),
            peer_id: "chanvoy-1".to_string(),
            timestamp: "2026-04-08T16:00:00+00:00".to_string(),
            action: "chat.post".to_string(),
            actor: "chanvoy".to_string(),
            prev_hash: "0".repeat(64),
            details: Some(serde_json::json!({"channel_id": "ch1"})),
            severity: Some("notice".to_string()),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"audit_event\""));
        let parsed: AuditEvent = serde_json::from_str(&json).unwrap();
        assert_eq!(event, parsed);
    }

    #[test]
    fn audit_actions_are_not_double_prefixed() {
        let action = "chat.post";
        let event = AuditEvent {
            event_type: "audit_event".to_string(),
            entry_id: uuid::Uuid::new_v4().to_string(),
            peer_id: "chanvoy-1".to_string(),
            timestamp: "2026-04-08T16:00:00+00:00".to_string(),
            action: action.to_string(),
            actor: "chanvoy".to_string(),
            prev_hash: "0".repeat(64),
            details: None,
            severity: Some("info".to_string()),
        };

        assert_eq!(event.action, action);
    }

    #[test]
    fn gap_events_pass_ipc_filter() {
        let event = DaemonEvent {
            seq: 10,
            kind: DaemonEventKind::Gap,
            payload: DaemonEventPayloadInner::Gap(chanvoy_core::GapPayload {
                subscription_id: "m2-sub".to_string(),
                missed_from_seq: 5,
                missed_to_seq: 10,
            }),
        };
        let filter = IpcSubscriptionFilter {
            channel_ids: None,
            event_kinds: Some(vec!["message_posted".to_string()]),
            mentions_only: None,
        };
        assert!(event_matches_ipc_filter(&event, &filter));
    }

    #[test]
    fn subscription_gap_frame_uses_ipc_sub_id() {
        let gap = ChatFrame::SubscriptionGap {
            subscription_id: "ipc-sub-1".to_string(),
            expected_seq: 5,
            next_seq: 10,
            reason: "history_unavailable".to_string(),
            message: Some("missed 5 events between seq 5 and 10".to_string()),
        };
        let json = serde_json::to_string(&gap).unwrap();
        assert!(json.contains("ipc-sub-1"));
        assert!(json.contains("history_unavailable"));
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(gap, parsed);
    }

    #[test]
    fn drift_refusal_frame_carries_identity_drift_code() {
        // PER-014 (entarch finding #1): IPC drift gate refuses
        // network-backed requests with the new IdentityDrift code.
        // Verify the frame shape, retryable hint, and roundtrip.
        let frame = IpcPeer::drift_refusal("req-123".to_string());
        match &frame {
            ChatFrame::Error {
                request_id,
                error_code,
                message,
                retryable,
            } => {
                assert_eq!(request_id, "req-123");
                assert_eq!(*error_code, ChatErrorCode::IdentityDrift);
                assert!(
                    message.contains("identity drift"),
                    "diagnostic should name the failure mode"
                );
                assert!(
                    message.contains("auto-setup"),
                    "diagnostic should point at the recovery action"
                );
                // retryable=Some(true) because re-running auto-setup
                // can clear the drift bit; this is not a permanent
                // permission denial.
                assert_eq!(*retryable, Some(true));
            }
            other => panic!("expected Error frame, got {other:?}"),
        }
        // Roundtrip the new error code through serde so wire-format
        // consumers see "identity_drift" snake_case.
        let json = serde_json::to_string(&frame).unwrap();
        assert!(json.contains("identity_drift"), "json={json}");
        let parsed: ChatFrame = serde_json::from_str(&json).unwrap();
        assert_eq!(frame, parsed);
    }

    // ------------------------------------------------------------------
    // Thread reads are bound to the channel the caller named
    // ------------------------------------------------------------------

    mod thread_read {
        use super::*;
        use chanvoy_core::{CapabilityClass, CredentialMode, Provider};
        use wiremock::matchers::{method as http_method, path as http_path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        pub(super) const CALLER_CHANNEL: &str = "channel-the-caller-named";
        const OTHER_CHANNEL: &str = "channel-the-caller-may-not-read";
        const ROOT_ID: &str = "thread-root";

        pub(super) fn peer_against(mock_url: &str) -> IpcPeer {
            let profile = Profile {
                name: "thread-read".to_string(),
                role: "bravo-devlead".to_string(),
                scope: "lanytehq".to_string(),
                provider: Provider::Mattermost,
                bot_username: "bot-stable".to_string(),
                team_name: "team-slug-stable".to_string(),
                server_url: mock_url.to_string(),
                env_name: "LANYTE_MM_TOKEN".to_string(),
                env_file: None,
                credential_mode: CredentialMode::EnvName,
                capability_class: CapabilityClass::Standard,
                monitored_channels: vec![],
                ipc: None,
                reduce: None,
            };
            let client = MattermostClient::new(&profile, "fixture-token".to_string())
                .expect("build MattermostClient");
            IpcPeer::new(
                &profile,
                client,
                Arc::new(EventBus::new(16)),
                "unused-gateway-socket".to_string(),
                Arc::new(AtomicBool::new(false)),
            )
        }

        /// One post in the shape the provider actually sends.
        pub(super) fn wire_post(
            id: &str,
            channel_id: &str,
            user_id: &str,
            create_at: i64,
            root_id: &str,
        ) -> serde_json::Value {
            serde_json::json!({
                "id": id,
                "channel_id": channel_id,
                "user_id": user_id,
                "message": format!("body of {id}"),
                "create_at": create_at,
                "root_id": root_id,
            })
        }

        pub(super) fn posts_envelope(posts: Vec<serde_json::Value>) -> serde_json::Value {
            let order: Vec<String> = posts
                .iter()
                .map(|p| p["id"].as_str().unwrap().to_string())
                .collect();
            let map: serde_json::Map<String, serde_json::Value> = posts
                .into_iter()
                .map(|p| (p["id"].as_str().unwrap().to_string(), p))
                .collect();
            serde_json::json!({ "order": order, "posts": map })
        }

        async fn mount_post(server: &MockServer, post: serde_json::Value) {
            let id = post["id"].as_str().unwrap().to_string();
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{id}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(post))
                .mount(server)
                .await;
        }

        async fn mount_thread(server: &MockServer, root_id: &str, body: serde_json::Value) {
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{root_id}/thread")))
                .respond_with(ResponseTemplate::new(200).set_body_json(body))
                .mount(server)
                .await;
        }

        pub(super) async fn mount_user(server: &MockServer, user_id: &str, username: &str) {
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/users/{user_id}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(serde_json::json!({"id": user_id, "username": username})),
                )
                .mount(server)
                .await;
        }

        async fn thread_requests(server: &MockServer) -> usize {
            server
                .received_requests()
                .await
                .expect("wiremock received_requests")
                .iter()
                .filter(|req| req.url.path().ends_with("/thread"))
                .count()
        }

        async fn author_lookups(server: &MockServer) -> usize {
            server
                .received_requests()
                .await
                .expect("wiremock received_requests")
                .iter()
                .filter(|req| req.url.path().starts_with("/api/v4/users/"))
                .count()
        }

        /// The gateway refuses a point response that is not the post it
        /// asked for.
        ///
        /// Composition through the shared core check makes this hold
        /// today; the point of pinning it here is that the gateway
        /// contract should not depend on that composition surviving a
        /// future extraction or reordering. A substituted post is worse
        /// here than a wrong-channel one: its root would be taken as
        /// canonical and an unrelated conversation fetched under the
        /// caller's channel label.
        #[tokio::test]
        async fn a_point_response_for_a_different_post_is_refused_by_the_gateway() {
            let server = MockServer::start().await;
            // Asked for ROOT_ID; the provider answers with a different
            // post, correctly in the caller's channel, rooted elsewhere.
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{ROOT_ID}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(wire_post(
                    "substituted-post",
                    CALLER_CHANNEL,
                    "user-b",
                    1_700_000_009_000,
                    "foreign-root",
                )))
                .mount(&server)
                .await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-substituted".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error {
                    error_code,
                    retryable,
                    message,
                    ..
                } => {
                    assert_eq!(*error_code, ChatErrorCode::NotFound);
                    assert_eq!(
                        *retryable,
                        Some(false),
                        "the provider will not answer differently on a retry"
                    );
                    assert_eq!(message, &binding_refusal_message(ROOT_ID));
                }
                other => panic!("expected Error frame, got {other:?}"),
            }

            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            for leaked in [
                "substituted-post",
                "foreign-root",
                "body of substituted-post",
            ] {
                assert!(
                    !rendered.contains(leaked),
                    "nothing of the substituted post may reach the caller: {leaked} in {rendered}"
                );
            }
            assert_eq!(
                thread_requests(&server).await,
                0,
                "a substituted point response must stop the read before any thread is fetched"
            );
            assert_eq!(
                author_lookups(&server).await,
                0,
                "and before any author is resolved"
            );
        }

        /// A thread that lives in the caller's channel reads normally,
        /// and every summary names the thread it belongs to.
        #[tokio::test]
        async fn a_thread_in_the_named_channel_reads_and_every_post_names_its_thread() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "reply-1",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-ok".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match frame {
                ChatFrame::ReadResponse {
                    request_id,
                    channel_id,
                    posts,
                    ..
                } => {
                    assert_eq!(request_id, "req-ok");
                    assert_eq!(channel_id, CALLER_CHANNEL);
                    assert_eq!(
                        posts.iter().map(|p| p.post_id.as_str()).collect::<Vec<_>>(),
                        vec![ROOT_ID, "reply-1"]
                    );
                    assert_eq!(
                        posts.iter().map(|p| p.author.as_str()).collect::<Vec<_>>(),
                        vec!["alice", "bob"]
                    );
                    for post in &posts {
                        assert_eq!(
                            post.thread_root_id.as_deref(),
                            Some(ROOT_ID),
                            "every summary names the thread it is part of: {post:?}"
                        );
                    }
                }
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// Naming a reply reads the same thread as naming the root — the
        /// anchor's root is what the thread request is made against.
        #[tokio::test]
        async fn naming_a_reply_reads_the_whole_thread() {
            let server = MockServer::start().await;
            let reply = wire_post(
                "reply-1",
                CALLER_CHANNEL,
                "user-b",
                1_700_000_001_000,
                ROOT_ID,
            );
            mount_post(&server, reply).await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "reply-1",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            // Deliberately NOT mounted: /posts/reply-1/thread. Asking
            // the provider for the thread of the id the caller passed,
            // rather than the canonical root, 404s and fails this test.
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-reply".to_string(),
                    CALLER_CHANNEL.to_string(),
                    "reply-1",
                    None,
                )
                .await;

            match frame {
                ChatFrame::ReadResponse { posts, .. } => assert_eq!(
                    posts.iter().map(|p| p.post_id.as_str()).collect::<Vec<_>>(),
                    vec![ROOT_ID, "reply-1"],
                    "a reply id reads the same thread its root does"
                ),
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// A post that lives in another channel is refused, and the
        /// thread is never asked for. Asserting only on the error would
        /// pass even if the bind ran after the fetch.
        #[tokio::test]
        async fn a_thread_in_another_channel_is_refused_before_any_thread_request() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, OTHER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            // Mounted and answerable on purpose: the point is that it is
            // never called, not that calling it would fail.
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![wire_post(
                    ROOT_ID,
                    OTHER_CHANNEL,
                    "user-a",
                    1_700_000_000_000,
                    "",
                )]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-mismatch".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                // The refusal echoes the caller's own post id and nothing
                // else. It must NOT reveal which channel the post really
                // lives in, and must read identically to a refusal for a
                // post that does not exist at all — otherwise a peer can
                // probe for posts in channels it cannot read.
                ChatFrame::Error { message, .. } => {
                    assert_eq!(message, &binding_refusal_message(ROOT_ID));
                    assert!(
                        !message.contains(OTHER_CHANNEL),
                        "refusal must not name the channel the post really lives in: {message}"
                    );
                }
                other => panic!("expected Error frame, got {other:?}"),
            }
            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            assert!(
                !rendered.contains("body of"),
                "no post body may leak on a refusal: {rendered}"
            );
            assert_eq!(
                thread_requests(&server).await,
                0,
                "a cross-channel thread read must issue no thread request at all"
            );
        }

        /// A post that does not exist is refused, and again nothing is
        /// asked of the thread endpoint.
        #[tokio::test]
        async fn a_missing_anchor_is_refused_before_any_thread_request() {
            let server = MockServer::start().await;
            Mock::given(http_method("GET"))
                .and(http_path(format!("/api/v4/posts/{ROOT_ID}")))
                .respond_with(
                    ResponseTemplate::new(404)
                        .set_body_json(serde_json::json!({"status_code": 404})),
                )
                .mount(&server)
                .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![wire_post(
                    ROOT_ID,
                    CALLER_CHANNEL,
                    "user-a",
                    1_700_000_000_000,
                    "",
                )]),
            )
            .await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-missing".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                // Code and retryability are whatever `core_error_to_chat`
                // already assigns an anchor failure; this test is about
                // the refusal happening at all, and happening first.
                ChatFrame::Error { message, .. } => assert!(
                    message.contains(ROOT_ID),
                    "refusal names the post it could not find: {message}"
                ),
                other => panic!("expected Error frame, got {other:?}"),
            }
            assert_eq!(
                thread_requests(&server).await,
                0,
                "a missing anchor must issue no thread request at all"
            );
        }

        /// A thread whose anchor is in the caller's channel but whose
        /// envelope also carries a post from elsewhere is refused, and
        /// no summary is returned at all.
        ///
        /// The anchor bind does not cover this: it passes, the thread
        /// request is genuinely made, and what comes back is a mix. The
        /// peer's channel is the narrower scope — the bot's credential
        /// reaches channels the peer never named — and every summary
        /// this function builds is stamped with the peer's channel id,
        /// so an unchecked post would be handed back labelled with a
        /// channel it is not in. The refusal reads the same as any
        /// other binding refusal, for the same reason: telling the two
        /// apart would be an existence oracle.
        #[tokio::test]
        async fn a_thread_carrying_an_out_of_channel_post_is_refused_whole() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "stray-reply",
                        OTHER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-mixed".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error { message, .. } => {
                    // The refusal names the root the caller asked for.
                    // The stray post's id is provider-supplied and is
                    // not the caller's to learn: echoing it back would
                    // confirm the existence and identity of a post
                    // outside the channel they named, which is the
                    // narrower disclosure form of the existence oracle
                    // this bind exists to prevent.
                    assert_eq!(message, &binding_refusal_message(ROOT_ID));
                    assert!(
                        !message.contains("stray-reply"),
                        "the offending post's id must not reach the caller: {message}"
                    );
                    assert!(
                        !message.contains(OTHER_CHANNEL),
                        "the refusal must not name where the stray post lives: {message}"
                    );
                }
                other => panic!("expected Error frame, got {other:?}"),
            }
            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            assert!(
                !rendered.contains("body of"),
                "no body may survive the refusal, including the in-channel \
                 root's: {rendered}"
            );
        }

        /// A caller who named a reply is refused in terms of the reply,
        /// never in terms of the root derived from it.
        ///
        /// The root is this function's own work: it comes off the
        /// anchor, and a caller who cited a reply neither supplied it
        /// nor has any way to obtain it. Quoting it in a refusal turns
        /// a failed read into a lookup — hand over a reply id, get back
        /// the id of the post that started the conversation, whether or
        /// not the read was allowed to proceed. The refusal reads
        /// exactly as it would for the id the caller actually sent.
        #[tokio::test]
        async fn a_malformed_thread_named_by_a_reply_is_refused_in_terms_of_the_reply() {
            let server = MockServer::start().await;
            let cited_reply = "the-reply-the-caller-cited";
            let derived_root = "the-root-the-caller-never-named";
            mount_post(
                &server,
                wire_post(
                    cited_reply,
                    CALLER_CHANNEL,
                    "user-b",
                    1_700_000_001_000,
                    derived_root,
                ),
            )
            .await;
            // The thread is fetched against the derived root and comes
            // back mixed, so the refusal is raised deep inside the
            // thread read — where the only id in scope is the derived
            // one.
            mount_thread(
                &server,
                derived_root,
                posts_envelope(vec![
                    wire_post(
                        derived_root,
                        CALLER_CHANNEL,
                        "user-a",
                        1_700_000_000_000,
                        "",
                    ),
                    wire_post(
                        "stray-reply",
                        OTHER_CHANNEL,
                        "user-c",
                        1_700_000_002_000,
                        derived_root,
                    ),
                ]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            mount_user(&server, "user-c", "carol").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-reply-refusal".to_string(),
                    CALLER_CHANNEL.to_string(),
                    cited_reply,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error { message, .. } => {
                    assert_eq!(message, &binding_refusal_message(cited_reply));
                    assert!(
                        !message.contains(derived_root),
                        "the derived root was never supplied by the caller and must \
                         not be handed back: {message}"
                    );
                }
                other => panic!("expected Error frame, got {other:?}"),
            }
            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            assert!(
                !rendered.contains(derived_root),
                "the derived root must not reach the caller anywhere in the frame: {rendered}"
            );
            assert!(
                !rendered.contains("stray-reply") && !rendered.contains(OTHER_CHANNEL),
                "no provider-supplied id or channel may reach the caller: {rendered}"
            );
            assert!(
                !rendered.contains("body of"),
                "no post body may survive the refusal: {rendered}"
            );
        }

        /// The same holds when the thread comes back empty: an empty
        /// thread reached through a reply is reported against the reply.
        ///
        /// This failure is raised on a different path from the binding
        /// refusal above and carries a different wording, so it is a
        /// separate chance to leak the same derived id.
        #[tokio::test]
        async fn an_empty_thread_named_by_a_reply_is_reported_against_the_reply() {
            let server = MockServer::start().await;
            let cited_reply = "the-reply-the-caller-cited";
            let derived_root = "the-root-the-caller-never-named";
            mount_post(
                &server,
                wire_post(
                    cited_reply,
                    CALLER_CHANNEL,
                    "user-b",
                    1_700_000_001_000,
                    derived_root,
                ),
            )
            .await;
            mount_thread(
                &server,
                derived_root,
                serde_json::json!({"order": [], "posts": {}}),
            )
            .await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            let frame = peer
                .thread_read_response(
                    "req-empty-by-reply".to_string(),
                    CALLER_CHANNEL.to_string(),
                    cited_reply,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error { message, .. } => {
                    assert!(
                        message.contains(cited_reply),
                        "the diagnostic names the id the caller supplied: {message}"
                    );
                    assert!(
                        !message.contains(derived_root),
                        "the derived root must not be disclosed: {message}"
                    );
                }
                other => panic!("expected Error frame, got {other:?}"),
            }
        }

        /// A truncated thread says so, and a complete one says so too.
        ///
        /// `has_more` is the only thing distinguishing "this is the
        /// conversation" from "this is the front of the conversation."
        /// A peer reads a thread to decide what to say next; if a
        /// truncated result is reported the same way as a complete one,
        /// the peer answers a conversation it has only seen part of and
        /// has no way to discover that. Both directions are asserted —
        /// a field hard-coded to `true` is as useless as one hard-coded
        /// to `false`.
        #[tokio::test]
        async fn a_truncated_thread_is_reported_as_truncated() {
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
            )
            .await;
            mount_thread(
                &server,
                ROOT_ID,
                posts_envelope(vec![
                    wire_post(ROOT_ID, CALLER_CHANNEL, "user-a", 1_700_000_000_000, ""),
                    wire_post(
                        "reply-1",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        ROOT_ID,
                    ),
                    wire_post(
                        "reply-2",
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_002_000,
                        ROOT_ID,
                    ),
                ]),
            )
            .await;
            mount_user(&server, "user-a", "alice").await;
            mount_user(&server, "user-b", "bob").await;
            let peer = peer_against(&server.uri());

            // A limit below the thread length: two of three posts.
            let frame = peer
                .thread_read_response(
                    "req-truncated".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    Some(2),
                )
                .await;
            match frame {
                ChatFrame::ReadResponse {
                    posts, has_more, ..
                } => {
                    assert_eq!(posts.len(), 2, "the limit is applied");
                    assert_eq!(
                        has_more,
                        Some(true),
                        "a thread cut short by the limit must not look complete"
                    );
                }
                other => panic!("expected ReadResponse, got {other:?}"),
            }

            // A limit at or above the thread length: nothing withheld.
            let frame = peer
                .thread_read_response(
                    "req-complete".to_string(),
                    CALLER_CHANNEL.to_string(),
                    ROOT_ID,
                    Some(3),
                )
                .await;
            match frame {
                ChatFrame::ReadResponse {
                    posts, has_more, ..
                } => {
                    assert_eq!(posts.len(), 3);
                    assert_eq!(
                        has_more,
                        Some(false),
                        "a complete thread must not be advertised as truncated"
                    );
                }
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// The provider refuses the derived thread and says so in its
        /// own words — which quote the id it was asked about.
        ///
        /// That body is the provider's text, not ours, and after the
        /// derivation the id it names is the root the caller never
        /// supplied. It is dropped. What survives is the status, and
        /// only the status: it is what tells an automated caller
        /// whether to try again, and a transient outage that came back
        /// as a terminal refusal would strand a caller that would have
        /// succeeded on a retry. Both directions are checked, because a
        /// sanitiser that collapsed every provider failure into one
        /// classification would satisfy the disclosure half of this
        /// test on its own.
        #[tokio::test]
        async fn a_provider_failure_on_the_derived_thread_is_reported_without_quoting_it() {
            let cited_reply = "the-reply-the-caller-cited";
            let derived_root = "the-root-the-caller-never-named";
            let provider_body = format!("upstream is unwell while reading thread {derived_root}");

            for (status, expected_code, expected_retryable) in [
                (500_u16, ChatErrorCode::ProviderError, true),
                (404_u16, ChatErrorCode::NotFound, false),
            ] {
                let server = MockServer::start().await;
                mount_post(
                    &server,
                    wire_post(
                        cited_reply,
                        CALLER_CHANNEL,
                        "user-b",
                        1_700_000_001_000,
                        derived_root,
                    ),
                )
                .await;
                mount_user(&server, "user-b", "bob").await;
                Mock::given(http_method("GET"))
                    .and(http_path(format!("/api/v4/posts/{derived_root}/thread")))
                    .respond_with(
                        ResponseTemplate::new(status).set_body_string(provider_body.clone()),
                    )
                    .mount(&server)
                    .await;
                let peer = peer_against(&server.uri());

                let frame = peer
                    .thread_read_response(
                        format!("req-provider-{status}"),
                        CALLER_CHANNEL.to_string(),
                        cited_reply,
                        None,
                    )
                    .await;

                match &frame {
                    ChatFrame::Error {
                        error_code,
                        retryable,
                        message,
                        ..
                    } => {
                        assert_eq!(
                            error_code, &expected_code,
                            "status {status} must keep its classification through the \
                             re-statement: {frame:?}"
                        );
                        assert_eq!(
                            *retryable,
                            Some(expected_retryable),
                            "status {status} must keep its retryability: withholding the \
                             provider's words must not turn a transient outage into a \
                             terminal refusal, nor the reverse: {frame:?}"
                        );
                        assert!(
                            message.contains(cited_reply),
                            "the failure is reported against the id the caller sent: {message}"
                        );
                    }
                    other => panic!("expected Error frame, got {other:?}"),
                }
                let rendered = serde_json::to_string(&frame).expect("serialize frame");
                assert!(
                    !rendered.contains(derived_root),
                    "the derived root must not reach the caller anywhere in the \
                     frame: {rendered}"
                );
                assert!(
                    !rendered.contains("upstream is unwell"),
                    "the provider's own body is its text about an id the caller never \
                     supplied, and must not be forwarded: {rendered}"
                );
                assert!(
                    !rendered.contains("/posts/"),
                    "no fragment of the URL the request was made against may be \
                     rendered: {rendered}"
                );
            }
        }

        /// The thread request fails at the transport layer rather than
        /// coming back with a status.
        ///
        /// This is the case a variant-by-variant sanitiser misses. A
        /// reqwest failure renders the URL it was fetching, and after
        /// the derivation that URL contains the derived root — so an
        /// error forwarded from here discloses the id through wording
        /// nobody wrote deliberately.
        #[tokio::test]
        async fn a_transport_failure_on_the_derived_thread_is_reported_without_naming_it() {
            let cited_reply = "the-reply-the-caller-cited";
            let derived_root = "the-root-the-caller-never-named";
            let server = MockServer::start().await;
            mount_post(
                &server,
                wire_post(
                    cited_reply,
                    CALLER_CHANNEL,
                    "user-b",
                    1_700_000_001_000,
                    derived_root,
                ),
            )
            .await;
            mount_user(&server, "user-b", "bob").await;
            // Mounted and answerable: the front end is what stops the
            // request, so the fixture is not quietly testing a 404.
            mount_thread(
                &server,
                derived_root,
                posts_envelope(vec![wire_post(
                    derived_root,
                    CALLER_CHANNEL,
                    "user-a",
                    1_700_000_000_000,
                    "",
                )]),
            )
            .await;
            let front_end = HangUpOnThread::in_front_of(*server.address());
            let peer = peer_against(&front_end.uri());

            let frame = peer
                .thread_read_response(
                    "req-transport".to_string(),
                    CALLER_CHANNEL.to_string(),
                    cited_reply,
                    None,
                )
                .await;

            match &frame {
                ChatFrame::Error {
                    error_code,
                    retryable,
                    message,
                    ..
                } => {
                    assert_eq!(
                        error_code,
                        &ChatErrorCode::ProviderError,
                        "a provider that hung up is a provider failure: {frame:?}"
                    );
                    assert_eq!(
                        *retryable,
                        Some(true),
                        "a connection that dropped may well succeed on a retry: {frame:?}"
                    );
                    assert!(
                        message.contains(cited_reply),
                        "the failure is reported against the id the caller sent: {message}"
                    );
                }
                other => panic!("expected Error frame, got {other:?}"),
            }
            let rendered = serde_json::to_string(&frame).expect("serialize frame");
            assert!(
                !rendered.contains(derived_root),
                "the derived root must not reach the caller anywhere in the frame: {rendered}"
            );
            assert!(
                !rendered.contains("/posts/"),
                "a transport error renders the URL it was fetching; no fragment of it \
                 may reach the caller: {rendered}"
            );
            assert!(
                !rendered.contains(&front_end.uri()),
                "nor may the provider address: {rendered}"
            );
        }

        /// A front end that answers everything the mock provider
        /// answers, except a thread request: that one it accepts and
        /// then hangs up on without writing a status line.
        ///
        /// Shutting the provider down instead would fail the anchor
        /// fetch too, and the derived root would never be reached —
        /// leaving nothing to disclose and a test that proves nothing.
        /// Standing in front of it lets the anchor succeed and only the
        /// thread request fail, which is the shape the caller-visible
        /// wording is being checked against.
        struct HangUpOnThread {
            addr: std::net::SocketAddr,
            shutdown: Arc<AtomicBool>,
            accept_loop: Option<std::thread::JoinHandle<()>>,
        }

        impl HangUpOnThread {
            fn in_front_of(upstream: std::net::SocketAddr) -> Self {
                let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind front end");
                let addr = listener.local_addr().expect("front end address");
                listener
                    .set_nonblocking(true)
                    .expect("front end accept loop must be interruptible");
                let shutdown = Arc::new(AtomicBool::new(false));
                let stop = Arc::clone(&shutdown);
                let accept_loop = std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        match listener.accept() {
                            Ok((stream, _)) => {
                                std::thread::spawn(move || forward_unless_thread(stream, upstream));
                            }
                            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                                std::thread::sleep(std::time::Duration::from_millis(5));
                            }
                            Err(_) => break,
                        }
                    }
                });
                Self {
                    addr,
                    shutdown,
                    accept_loop: Some(accept_loop),
                }
            }

            fn uri(&self) -> String {
                format!("http://{}", self.addr)
            }
        }

        impl Drop for HangUpOnThread {
            fn drop(&mut self) {
                self.shutdown.store(true, Ordering::Relaxed);
                if let Some(handle) = self.accept_loop.take() {
                    let _ = handle.join();
                }
            }
        }

        /// One connection: read the request head, hang up on a thread
        /// request, otherwise relay it upstream and stream the answer
        /// back verbatim.
        fn forward_unless_thread(mut client: std::net::TcpStream, upstream: std::net::SocketAddr) {
            use std::io::Write;

            // `accept` may hand back a non-blocking socket on some
            // platforms; the relay below is written blocking.
            let _ = client.set_nonblocking(false);
            let Some((head, path)) = read_request_head(&mut client) else {
                return;
            };
            if path.split('?').next().unwrap_or(&path).ends_with("/thread") {
                return;
            }
            let Ok(mut server) = std::net::TcpStream::connect(upstream) else {
                return;
            };
            // One request per connection: asking upstream to close means
            // the relay below terminates on EOF, and means the client
            // opens a fresh connection for the thread request rather
            // than reusing a pooled one.
            let relayed = head
                .lines()
                .filter(|line| {
                    !line.to_ascii_lowercase().starts_with("connection:")
                        && !line.to_ascii_lowercase().starts_with("proxy-connection:")
                })
                .collect::<Vec<_>>()
                .join("\r\n");
            if server
                .write_all(format!("{relayed}\r\nConnection: close\r\n\r\n").as_bytes())
                .is_err()
            {
                return;
            }
            let _ = std::io::copy(&mut server, &mut client);
            let _ = client.flush();
        }

        /// Read up to the blank line that ends an HTTP request head.
        /// Returns the head without its terminating blank line, and the
        /// request target. Requests with bodies are not relayed — the
        /// reads under test are all GETs.
        fn read_request_head(client: &mut std::net::TcpStream) -> Option<(String, String)> {
            use std::io::Read;

            let mut head: Vec<u8> = Vec::new();
            let mut byte = [0_u8; 1];
            while !head.ends_with(b"\r\n\r\n") {
                match client.read(&mut byte) {
                    Ok(0) | Err(_) => return None,
                    Ok(_) => head.push(byte[0]),
                }
                if head.len() > 16 * 1024 {
                    return None;
                }
            }
            let text = String::from_utf8_lossy(&head).to_string();
            let path = text.lines().next()?.split_whitespace().nth(1)?.to_string();
            Some((text.trim_end().to_string(), path))
        }
    }

    // ------------------------------------------------------------------
    // An ordinary channel read says what it does not know
    // ------------------------------------------------------------------

    mod channel_window_read {
        use super::thread_read::{
            mount_user, peer_against, posts_envelope, wire_post, CALLER_CHANNEL,
        };
        use super::*;
        use wiremock::matchers::{
            method as http_method, path as http_path, query_param as http_query_param,
        };
        use wiremock::{Mock, MockServer, ResponseTemplate};

        /// A window of `count` posts, all by one author, in the caller's
        /// channel, ordered in time.
        ///
        /// The mock binds `per_page`, not just the path. The
        /// completeness rule below decides "full page" by comparing the
        /// count it received against `CHANNEL_WINDOW_PAGE_SIZE`, which
        /// is only meaningful if that constant is also the page size the
        /// transport asked the provider for. Matching on the path alone
        /// leaves the two free to drift apart while every assertion
        /// still passes: a transport asking for a different page size —
        /// or none at all — would be answered by this mock regardless.
        /// Bound here, that drift stops matching and the tests fail.
        async fn mount_window(server: &MockServer, count: usize) {
            let posts: Vec<serde_json::Value> = (0..count)
                .map(|n| {
                    wire_post(
                        &format!("post-{n:02}"),
                        CALLER_CHANNEL,
                        "user-a",
                        1_700_000_000_000 + n as i64 * 1_000,
                        "",
                    )
                })
                .collect();
            Mock::given(http_method("GET"))
                .and(http_path(format!(
                    "/api/v4/channels/{CALLER_CHANNEL}/posts"
                )))
                .and(http_query_param(
                    "per_page",
                    chanvoy_core::CHANNEL_WINDOW_PAGE_SIZE.to_string(),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(posts_envelope(posts)))
                .mount(server)
                .await;
            mount_user(server, "user-a", "alice").await;
        }

        async fn read_window(count: usize, limit: Option<u32>) -> ChatFrame {
            let server = MockServer::start().await;
            mount_window(&server, count).await;
            let peer = peer_against(&server.uri());
            peer.channel_read_response("req-window".to_string(), CALLER_CHANNEL.to_string(), limit)
                .await
        }

        fn completeness_of(frame: ChatFrame) -> (usize, Option<bool>) {
            match frame {
                ChatFrame::ReadResponse {
                    posts, has_more, ..
                } => (posts.len(), has_more),
                other => panic!("expected ReadResponse, got {other:?}"),
            }
        }

        /// A window that comes back exactly full reports its
        /// completeness as unknown, not as complete.
        ///
        /// One read asks the provider for one page. A page that arrives
        /// full is the one case where the count carries no information:
        /// a channel with exactly that many posts in the window and a
        /// channel with hundreds look identical from here. The limit is
        /// above the page size, so nothing was withheld by this layer —
        /// but "we withheld nothing" is not "there is nothing more," and
        /// answering `false` states the second while only knowing the
        /// first. A peer that reads a channel to decide what to say
        /// next would treat the front of a busy channel as the whole of
        /// a quiet one, with no way to find out.
        #[tokio::test]
        async fn a_window_that_came_back_full_reports_completeness_as_unknown() {
            let (returned, has_more) =
                completeness_of(read_window(chanvoy_core::CHANNEL_WINDOW_PAGE_SIZE, None).await);

            assert_eq!(
                returned,
                chanvoy_core::CHANNEL_WINDOW_PAGE_SIZE,
                "the default limit is above the page size, so nothing is dropped here"
            );
            assert_eq!(
                has_more, None,
                "a full page cannot be told from a truncated one, and must not \
                 be reported as complete"
            );
        }

        /// A window that came back short of a page, and short of the
        /// limit, genuinely is everything there was.
        ///
        /// The provider stopped before its page size, which is the one
        /// piece of evidence that there was nothing more to send.
        #[tokio::test]
        async fn a_window_short_of_a_page_is_reported_complete() {
            let (returned, has_more) = completeness_of(read_window(5, None).await);

            assert_eq!(returned, 5);
            assert_eq!(
                has_more,
                Some(false),
                "a short page under the limit is the whole window"
            );
        }

        /// A window cut short by the caller's own limit says so.
        #[tokio::test]
        async fn a_window_cut_short_by_the_limit_is_reported_truncated() {
            let (returned, has_more) = completeness_of(read_window(5, Some(2)).await);

            assert_eq!(returned, 2, "the limit is applied");
            assert_eq!(
                has_more,
                Some(true),
                "posts this layer withheld itself must be declared"
            );
        }
    }
}
